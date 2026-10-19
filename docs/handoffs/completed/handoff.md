# fred-bronze-to-gold-pipeline
Full ETL Fred Pipeline

# FRED ETL Pipeline --- Databricks Handoff + LLM Build Spec

## Goal

Build a production-grade FRED API ingestion pipeline that will
eventually live in **Databricks** and can be handed either to:

1.  An engineering/database team
2.  An LLM/code agent for implementation

The system should be manifest-driven, scalable, auditable,
metadata-rich, and suitable for quant research, dashboards, optimizer
inputs, and point-in-time macro features.

------------------------------------------------------------------------

# Implementation Status (current)

The original FRED-only spec below is fully realized and the pipeline has been
generalized well past it. Current state:

-   **Multi-source (8):** FRED, BLS, EIA, US Treasury, World Bank, BEA, Census,
    SEC — a pluggable `SourceClient` layer (adding a source = one client module +
    one registry entry). See **Multi-Source Ingestion**.
-   **~2,380 series** across the FRED domain manifests + inactive demo manifests
    for the seven other sources (incl. the NSA + SA **CPI-U baskets**).
-   **Medallion with lineage:** `source` is part of the Silver natural key
    `(source, series_id, observation_date, realtime_start)`; Bronze records
    source + endpoint; replay is source-aware.
-   **Loading:** full-on-first-run then restate-last-N incremental;
    replay-from-Bronze; point-in-time (ALFRED) vintages.
-   **Gold layer complete:** latest / point-in-time / daily feature matrix;
    feature transforms; config-driven curve spreads; **frequency-aware N-leg
    cross-series features** + a **leak-free point-in-time variant**; **governance**
    (coverage/freshness view + cross-source reconciliation); **SEC company
    financials** (standardized statements, ratios, cross-company ranks, with
    quarterly/annual duration disambiguation incl. Q4 de-cumulation). All seven
    Gold-roadmap items are implemented (see the roadmap section).
-   **Engineering:** pure-core + lazy-Spark shell; Databricks/Delta **and** local
    SQLite backends in parity; layered config (file/env/args/secret scope);
    audit + data-quality profiles; Unity Catalog DDL; Asset Bundle (main +
    per-source jobs); **~289 unit tests + a Spark/Delta integration job in CI**.
-   **Docs:** this handoff, `README.md`, `docs/architecture.md`,
    `data_dictionary.md`, `validation.md`, `adding_a_source.md`,
    `deployment_runbook.md`, `incremental_loading.md`, `etl_build_spec.md`.

**Open (non-blocking, documented):** go-live provisioning + live series-ID
verification (`docs/deployment_runbook.md`); per-source metadata reconciliation
for non-FRED sources (`reconcile`/`discover` are FRED-only); optional new sources
(markets/OHLCV, the SDMX bundle) and a full SEC manifest generator.

## Performance Improvement Pickup (2026-10-19)

Spec 010 Phase 0's fixture-backed benchmark harness is implemented and its
SQLite smoke acceptance passes. Phase 1's `gold.equity_total_return_index`
checkpoint pilot is integrated for SQLite and PostgreSQL. Focused tests cover
no-op skipping, forward append, backdated dividend restatement, config-hash
invalidation, forced full rebuild, and interrupted-build rollback/recovery on
both backends. This establishes correctness only; a large-seed speedup has not
been measured or claimed.

**Next pickup:** run the Gold-only baseline against an isolated copy of the
large SQLite/PostgreSQL seed when the host has enough memory. Compare the
equity stage and end-to-end Gold medians, peak RSS, and row parity against a
full rebuild. Keep the incremental path only if it improves the stage
materially (Spec 010 suggests a 20% median gate) without increasing peak RSS or
changing output. After that gate, continue with the `gold.recession_probability_daily`
checkpoint integration. See `specs/spec010/README.md` and
`docs/handoffs/pipeline_performance.md` for implementation details and caveats.

------------------------------------------------------------------------

# Target Platform

Use Databricks as the long-term home:

-   Unity Catalog for governance, access control, lineage, and discovery
-   Delta Lake (Bronze / Silver / Gold)
-   Databricks Workflows / Jobs
-   Databricks Secrets
-   Lakeflow Spark Declarative Pipelines (where appropriate)
-   Databricks Asset Bundles for CI/CD and deployment

------------------------------------------------------------------------

# High-Level Architecture

``` text
Source APIs (FRED · BLS · EIA · Treasury · World Bank · BEA · Census · SEC)
    ↓
Python Ingestion Package (pluggable source clients)
    ↓
Unity Catalog Volume / Raw Archive
    ↓
Bronze Delta Tables
    ↓
Silver Normalized Tables
    ↓
Gold Feature Store
    ↓
Dashboards / SQL Warehouse / Optimizers / ML
```

Recommended Unity Catalog layout:

``` text
macro_dev
macro_test
macro_prod

Schemas:
- meta
- audit
- bronze
- silver
- gold
- sandbox
```

------------------------------------------------------------------------

# Repository Structure

``` text
fred-databricks-etl/
├── manifests/
├── resources/
├── sql/
├── src/
├── notebooks/
├── tests/
└── docs/
```

------------------------------------------------------------------------

# Core Design Principles

-   Manifest-driven ingestion
-   Pluggable multi-source ingestion (FRED, BLS, EIA, ...)
-   Metadata-driven orchestration
-   Delta MERGE for idempotent loads
-   Raw payload retention
-   Point-in-time (vintage) support
-   Complete auditability
-   Reusable feature store
-   Infrastructure-as-code
-   Environment promotion (Dev → Test → Prod)

------------------------------------------------------------------------

# Core Delta Tables

## Meta

-   meta.fred_series
-   meta.fred_manifest
-   meta.fred_series_manifest_map
-   meta.fred_series_lifecycle   (FRED metadata reconciliation)
-   meta.fred_series_drift       (manifest-vs-source drift)

## Audit

-   audit.etl_run
-   audit.etl_series_run
-   audit.data_quality_result

## Bronze

-   bronze.fred_api_response   (multi-source: carries a `source` column + the endpoint actually called)

## Silver

-   silver.fred_observation   (natural / MERGE key: source, series_id, observation_date, realtime_start)

## Gold

-   gold.fred_latest_observation
-   gold.fred_point_in_time
-   gold.fred_macro_feature_daily
-   gold.fred_feature_transforms
-   gold.fred_curve_spread
-   gold.fred_cross_series_feature
-   gold.fred_cross_series_feature_pit
-   gold.fred_source_reconciliation
-   gold.fred_company_fundamentals
-   gold.fred_company_ratios
-   gold.fred_revision_stats

------------------------------------------------------------------------

# Databricks Workflow

1.  Validate Manifest
2.  Initialize Run
3.  Read Manifest
4.  Extract Source Data (per series `source`: FRED / BLS / EIA / Treasury /
    World Bank / BEA / Census / SEC)
5.  Write Bronze
6.  Transform to Silver
7.  Run Data Quality
8.  Build Gold Features
9.  Close Audit
10. Notify

------------------------------------------------------------------------

# Manifest Requirements

Each manifest should define:

-   series_id
-   title
-   category
-   frequency
-   units
-   active
-   source            (upstream API: fred (default) / bls / eia)
-   load_type
-   expected_update_frequency
-   vintage_enabled
-   validation_profile
-   business_owner
-   technical_owner
-   downstream_use_case
-   priority

------------------------------------------------------------------------

# Point-in-Time Support

Persisted per Silver observation (see `silver.fred_observation` /
`transform.SILVER_COLUMNS`):

-   source
-   series_id
-   observation_date
-   realtime_start
-   realtime_end
-   value
-   revision_number   (1..N per (series_id, observation_date), by realtime_start)
-   is_missing
-   ingested_at

Vintage history is captured as the set of `realtime_start` rows per
observation_date rather than separate `first_seen_at` / `latest_seen_at`
columns: the earliest `realtime_start` is the first print and the latest is the
current revision. `ingested_at` records when each row was loaded.

Maintain two analytical views (implemented as `gold.v_latest_revised` /
`gold.v_point_in_time`):

-   latest_revised
-   point_in_time

------------------------------------------------------------------------

# Security

-   Store API keys in Databricks Secret Scopes (fred_api_key, plus optional
    bls_api_key / eia_api_key in the same scope)
-   Never hardcode credentials
-   Parameterize catalog/schema names
-   Support Dev/Test/Prod deployments

------------------------------------------------------------------------

# LLM Build Specification

The implementation should:

1.  Read YAML manifests.
2.  Validate manifests.
3.  Retrieve FRED observations.
4.  Store raw payloads in Bronze.
5.  Transform to Silver.
6.  Perform Delta MERGE.
7.  Record audit metadata.
8.  Execute validation rules.
9.  Build Gold feature tables.
10. Refresh analytical views.
11. Use Databricks Secrets.
12. Include unit tests.
13. Include SQL DDL.
14. Include Databricks Asset Bundle configuration.

------------------------------------------------------------------------

# Suggested Initial Series

> **Historical seed set.** These 27 FRED series were the original hand-picked
> universe. The live pipeline has since grown to **~2,380 series across 8
> sources** — the FRED domain manifests (rates, inflation, labor, growth,
> money/banking, prices, production/housing, international) plus the BLS/EIA/
> Treasury/World Bank/BEA/Census/SEC demo manifests (see **Multi-Source
> Ingestion** below and `manifests/`). This section is kept for provenance; it is
> not the current universe.

## Rates

-   DGS1MO
-   DGS3MO
-   DGS6MO
-   DGS1
-   DGS2
-   DGS5
-   DGS10
-   DGS30
-   FEDFUNDS
-   SOFR

## Inflation

-   CPIAUCSL
-   CPILFESL
-   PCEPI
-   PCEPILFE
-   T5YIE
-   T10YIE

## Labor

-   UNRATE
-   PAYEMS
-   ICSA
-   CIVPART
-   JTSJOL

## Growth

-   GDP
-   GDPC1
-   INDPRO
-   RSAFS
-   HOUST
-   PERMIT

## BLS CPI Basket (`source: bls`, `manifests/bls_cpi_basket.yml`)

The full CPI-U item hierarchy from BLS (FRED mirrors only a subset). CPI-U, NSA,
U.S. city average — series id `CUUR0000<item>`. A seasonally-adjusted companion
(`manifests/bls_cpi_basket_sa.yml`, `CUSR0000<item>`) mirrors the SA-published
aggregates/majors/common sub-items. Inactive by default; **item codes to be
verified against the live BLS series directory before activating.**

- **Headline / special aggregates:** SA0 (All items), SA0L1E (Core), SA0E
  (Energy), SAC (Commodities), SAS (Services), SACL1E (Core goods), SASLE (Core
  services)
- **8 major groups:** SAF (Food & beverages), SAH (Housing), SAA (Apparel), SAT
  (Transportation), SAM (Medical care), SAR (Recreation), SAE (Education &
  communication), SAG (Other goods & services)
- **Food:** SAF1 (Food), SAF11 (Food at home), SEFV (Food away from home), SAF116
  (Alcoholic beverages)
- **Housing:** SAH1 (Shelter), SEHA (Rent of primary residence), SEHC (Owners'
  equivalent rent), SAH2 (Fuels & utilities), SEHF01 (Electricity), SEHF02
  (Utility/piped gas)
- **Transportation:** SETA01 (New vehicles), SETA02 (Used cars & trucks), SETB01
  (Gasoline, all types)
- **Medical:** SAM1 (Medical care commodities), SAM2 (Medical care services)

SA (seasonally adjusted) variants use the `CUSR0000<item>` prefix and are shipped
as `manifests/bls_cpi_basket_sa.yml` (29 series — SA is published for the
aggregates/majors/common sub-items, not the full stratum set).

------------------------------------------------------------------------

# Gold Layer Feature Engineering Roadmap

Identified once the series universe grew beyond the initial seed set (the 27
FRED "Suggested Initial Series" → **~2,380 series across 8 sources**: FRED
[rates, inflation, labor, growth, money/banking, prices, production/housing,
international], plus BLS, EIA, US Treasury, World Bank, BEA, Census, and SEC). All
seven roadmap items below are now **implemented** (see each item's status); this
section is retained as the design record of how they came to be.

## 1. Point-in-time-safe (rolling) z-score --- correctness fix

**Status: implemented** (`features.py::compute_feature_transforms` /
`gold_polars.py::compute_feature_transforms_frame`, expanding z-score).

`gold.fred_feature_transforms.zscore` was previously computed from the
**full-sample** mean/std of each series (past *and* future observations
relative to any given row's date). That leaks future information into
historical rows, which conflicts with the pipeline's stated leak-free,
point-in-time design goal. Replace with a rolling or expanding-as-of-date
z-score (mean/std computed only from observations at-or-before each row's
date) so every row only reflects what was knowable as of that date.

## 2. Revision-magnitude / vintage-volatility Gold table

**Status: implemented** (`gold.fred_revision_stats` table +
`gold.v_series_revision_summary` view; `features.py::compute_revision_stats`
/ `gold_polars.py::compute_revision_stats_frame` / `gold.py::
_revision_stats_sql`).

The pipeline already retains full point-in-time vintage history
(`gold.fred_point_in_time`) but doesn't surface *how much* a series
typically gets revised. A new Gold table --- e.g. `gold.fred_revision_stats`
--- could capture, per series (and optionally per observation_date):

-   first-release value vs. latest-revised value (revision delta / % change)
-   number of revisions per observation_date
-   average / max revision magnitude over a trailing window

This is uniquely cheap to produce here (the vintage data is already
captured) and is valuable for quant researchers assessing how much to trust
a series' initial print (e.g. GDP/payrolls are heavily revised; market
series are not).

## 3. Config-driven spreads/ratios (generalize `curve_spread`)

**Status: implemented** (`config/spreads.yml` +
`fred_pipeline.spread_config.load_spread_defs`; wired into
`features.py::compute_curve_spreads`, `gold_polars.py::
compute_curve_spreads_frame`, and `gold.py::_curve_spread_sql` / the
static `sql/50_gold.sql` mirror).

`gold.fred_curve_spread` used to hardcode 4 Treasury curve pairs
(`DEFAULT_CURVE_SPREADS` in `features.py`). It's now driven by a reviewable
YAML file, `config/spreads.yml`, with each entry a `(name, long_leg,
short_leg, op)` definition — `op: spread` (long − short) or `op: ratio`
(long / short, guarded against a zero short leg). Ships with the same 4
Treasury pairs as before (no output change until new entries are added).
Now that the series universe spans rates, prices, labor, production/housing,
international, and national accounts, new cross-series features (e.g. real
yields = nominal minus breakeven inflation, credit spreads = corporate yield
minus Treasury, PCE vs. CPI divergence) can be added by editing that YAML —
no Python or SQL changes needed for the Python-driven backends (local SQLite
and Databricks/Spark). **Still needs quant sign-off on which new pairs to
add** — the mechanism is built, but no new pairs beyond the original 4 have
been added, since that's a domain judgment call, not an engineering one.

## 4. Frequency-aware, N-leg cross-series features

**Status: implemented** (`config/cross_series.yml` +
`fred_pipeline.cross_series_config`; engine
`fred_pipeline.features.compute_cross_series_features`, reused by both backends;
new table `gold.fred_cross_series_feature`).

`fred_curve_spread` (item 3) only combines **same-frequency, 2-leg** series on a
shared date grid. With the multi-source universe (daily Treasury, quarterly BEA,
annual World Bank, monthly Census…), useful features cross both **frequency** and
**source**. This adds an as-of alignment step — each leg is downsampled to a
target `frequency` (the last observation within each period) — plus **N-leg**
combinations: `spread` (a−b), `ratio` (a/b), and `composite` (Σ weightᵢ·legᵢ).
Ships three illustrative examples (real 10y yield; debt-to-GDP across
Treasury÷BEA; a monthly activity composite). **Same governance note as item 3**:
the mechanism is built; which features to compute (and their weights) is a quant
sign-off.

## 5. Governance Gold objects (coverage + cross-source reconciliation)

**Status: implemented.** Two governance views/tables over the multi-source data:

-   **`gold.v_source_coverage`** (pure view; `sql/60_views.sql` +
    `local_store.py`) — per `(source, series_id)`: latest observation, count,
    days-since, and an `is_stale` verdict from the manifest cadence. Operational
    visibility across the 8 sources.
-   **`gold.fred_source_reconciliation`** (config-driven table;
    `config/reconciliations.yml` + `fred_pipeline.reconciliation_config`; engine
    `fred_pipeline.features.compute_source_reconciliation`, reused by both
    backends) — same-concept series from different sources (FRED `UNRATE` vs BLS
    `LNS14000000`; FRED vs BEA GDP) aligned and compared with a `diverged` flag.
    A pure view can't do this (series ids differ by source with no join key), so
    the concept pairs are declared in YAML.

## 6. Point-in-time cross-series features (leak-free)

**Status: implemented** (`fred_pipeline.features.compute_cross_series_features_pit`;
new table `gold.fred_cross_series_feature_pit`).

The cross-series features (item 4) use *latest-revised* values — fine for
dashboards, but they inject later revisions into historical points (look-ahead
bias). This adds a **`realtime_start`-aligned** variant: each leg contributes the
value that was actually known (as-first-reported by default, or as-of any date),
so the feature series is leak-free for backtests. Reuses the same alignment/
combine logic (both backends share the one Python engine), reads raw Silver for
vintages. Identical to latest-revised for non-vintage series.

## 7. SEC statement standardization + company financials

**Status: implemented** (`config/sec_concepts.yml` + `config/sec_ratios.yml` +
`fred_pipeline.sec_standardization`; tables `gold.fred_company_fundamentals` /
`gold.fred_company_ratios`; view `gold.v_company_ratio_ranks`).

The SEC source lands one raw XBRL concept per series; companies use different
tags for the same line item. This standardizes raw tags → canonical concepts via
a priority-ordered mapping, then computes derived ratios, then ranks companies
cross-sectionally. **Restatement analytics come free** from
`gold.fred_revision_stats` (SEC filings carry `filed`-date vintages, which already
flow through it). Both backends share the one Python engine (SQL can't do the
priority tag-coalescing cleanly).

**Duration disambiguation: implemented.** Income-statement facts carry a
duration; a single 10-Q reports both the quarterly (~3-month) and YTD (~9-month)
figure for the same period end, which collided on the natural key. The SEC
normalizer now keeps only facts matching a target duration — `SEC_PERIOD`
(default `quarterly`, or `annual`) — so income concepts land as a consistent
series and ratios like net margin use matching durations. Instant balance-sheet
facts are always kept; Bronze replay resolves `SEC_PERIOD` identically. A 10-K
reports the FY (12-month) figure, not Q4, so **Q4 is synthesized by
de-cumulation** (`Q4 = FY − 9-month YTD`, dated at the FY end, known as of the
10-K filing) — giving a complete quarterly series. This is the last item on the
Gold roadmap; the remaining open work is non-Gold (per-source metadata
reconciliation for non-FRED sources; optional new sources).

------------------------------------------------------------------------

# Market Terminal Analytical Views (Power BI Gold plan)

**Status: ALL PHASES (0–6) implemented** (branch
`EconGoldTerminalViews`; 3c = rolling-window stats companions
`gold.curve_spread_rolling` / `credit_spread_rolling` /
`treasury_curve_rolling`, windows 1/5/10/21/63/126/252 obs; 5 = the regime
playbook `gold.macro_regime_daily` [five pillar scores from
`config/regime.yml`, ordered rule table, official conditions indices driving
liquidity/credit] plus the statistical lab `gold.series_correlation` /
`gold.series_lead_lag` [curated pairs in `config/stats_pairs.yml`,
rolling/expanding Pearson, ±12-lag CCF, two-direction Granger F with
pure-Python p-values]; 6 = the global tables `gold.global_inflation` /
`gold.global_policy_rates` [GCPI/GPOL, `config/global_series.yml`, US rows
live off active series, World Bank/ECB entries inactive verify-first] plus
`gold.powerbi_catalog`, the report author's manifest of every Gold object,
kept current by a test) —
full spec + per-phase status in `docs/market_terminal_gold_views.md`; column
semantics in `docs/data_dictionary.md`.

Implemented: `gold.dim_series` + `gold.dim_date` (star-schema dimensions;
`config/series_catalog.yml` tags ~65 already-ingested series with category/
polarity/transform), the ECON macro dashboard
(`gold.macro_indicator_dashboard` / `macro_indicator_sparkline` /
`macro_category_summary`), and the Treasury Curve Lab (`gold.treasury_curve` /
`treasury_curve_metrics` / `curve_spread_daily`, plus
`gold.spread_inversion_episode` — one row per unique inversion period per
spread, opening on the first negative print and closing when the spread turns
non-negative), and the Phase-4 rates complex — the BMRK benchmark board
(`gold.benchmark_rate_board`: change/trend/spread-to-benchmark/regime per
configured rate), the FUND funding tape + 0–100 stress gauge
(`gold.funding_tape_daily` / `funding_stress_daily`), and CRDT credit spreads
(`gold.credit_spread_daily`: ICE BofA OAS with percentile-threshold stress
episodes) — all via the shared pure-Python engines in
`src/fred_pipeline/terminal_views.py` wired into both backends, configured by
`config/benchmark_rates.yml` / `funding.yml` / `credit.yml`. New manifests
ship inactive pending live-FRED verification: `macro_flags.yml`
(`USREC`/`USRECD` — recession overlays are NULL until activated),
`fed_funding.yml` (EFFR/IORB/OBFR/BGCR/TGCR/RRPONTSYD/SOFR30DAYAVG),
`ice_credit.yml` (9 OAS indices), and `DGS3/DGS7/DGS20` + `DPRIME`/
`MORTGAGE30US` in `rates.yml`; absent series emit no Gold rows until
activated, so the tables populate progressively. Phase 2's Inflation
Explorer (`gold.inflation_explorer` / `inflation_contribution`,
`config/inflation_items.yml`) ships three item trees — the SA tree is rooted
at already-active `CPIAUCSL`/`PCEPI` so headline rows appear immediately; the
CUUR/CUSR item drill-down fills in when the CPI basket manifests are
activated, and the shipped group weights are approximate (refresh from the
BLS relative-importance table). The build plan is complete; what remains is
operational — verify + activate the inactive manifests (USREC, funding
corridor, OAS, CPI baskets, DGS3/7/20, World Bank CPI, ECB rates), refresh
the CPI weights, tune the regime thresholds once real history is loaded —
plus two deferred code items: PCE item-level via BEA, and the optional
starter `.pbix` (plan doc open question #4).

A separate project, `market_terminal` (a Bloomberg-style quant terminal),
renders its macro analytics (ECON macro dashboard, INFL inflation explorer, CURV
treasury-curve lab, BMRK benchmark rates, FUND funding tape, CRDT credit spreads,
REGIME regime playbook, STAT/EDA stats) on the fly in TypeScript from a
166-series FRED subset. The plan recreates those surfaces as **precomputed
Gold-layer tables/views for Power BI**, gaining point-in-time correctness the
terminal lacks.

Key facts for whoever picks this up:
- **Uses the existing catalog first.** The pipeline already ingests ~2,380
  series — a superset of the terminal's 166 — including the official
  financial-conditions/stress indices (`NFCI`, `ANFCI`, `STLFSI4`), sticky/
  flexible/trimmed CPI, breakevens, the Fed balance sheet, and the SOFR complex.
  Every planned Gold object draws from these; a new `config/series_catalog.yml`
  (+ `gold.dim_series`) tags each into an `econ_category`, polarity, and default
  transform.
- **~15 free-FRED gaps only:** `DGS3/DGS7/DGS20`, `USREC`, the funding corridor
  (`IORB/EFFR/OBFR/BGCR/TGCR/RRPONTSYD`), ICE BofA OAS (`BAMLH0A0HYM2`,
  `BAMLC0A0CM`, rating/sector), `DPRIME/MORTGAGE30US`. PCE item level via the
  already-wired BEA API; global CPI/policy via the free World Bank/OECD path. **No
  paid feeds.**
- **Same engineering pattern:** one pure-Python engine per metric shared by both
  backends, YAML-driven config, PIT-safe rolling stats, provenance/staleness on
  every row. New Gold objects are all additive (see the doc's §3 table).
- Delivered in 6 phases (dimensions → ECON → INFL → curve → rates complex →
  regime/stats → global + Power BI catalog). Four open questions for the user are
  listed at the end of the doc (PCE scope, regime rule table, correlation scope,
  `.pbix` delivery).

------------------------------------------------------------------------

# Multi-Source Ingestion

**Status: implemented** (`src/fred_pipeline/sources/`,
`pipeline.SOURCE_FACTORIES`; full guide in `docs/adding_a_source.md`).

The pipeline began FRED-only, but the FRED-specific surface is small and
isolated, so it was generalized to ingest additional public APIs through the
*same* Bronze → Silver → DQ → Gold → audit path. A manifest series declares its
upstream API with a `source:` field (default `fred`); everything downstream is
source-agnostic.

## Implemented sources

-   **FRED** — refactored onto a shared HTTP transport; behavior unchanged.
-   **BLS** (Bureau of Labor Statistics) — `source: bls`. Key optional (keyless
    works at a lower quota). Demo manifests (inactive): `manifests/bls_labor.yml`
    (unemployment); `manifests/bls_cpi_basket.yml` — the full **CPI-U item
    hierarchy** (30 series: headline + special aggregates, the 8 major groups,
    and key sub-strata), **NSA** (`CUUR0000<item>`); and
    `manifests/bls_cpi_basket_sa.yml` — the **seasonally-adjusted** companion (29
    series, `CUSR0000<item>`), covering the SA-published aggregates/majors/common
    sub-items (SA is narrower than NSA). FRED mirrors only a subset of BLS's CPI
    series, so the whole consumer basket lives here. **Item codes were assembled
    from the documented CPI structure and must be verified against the live BLS
    series directory before activating.**
-   **EIA** (Energy Information Administration) — `source: eia`. Key required.
    Demo manifest `manifests/eia_energy.yml` (inactive).
-   **US Treasury** (Fiscal Data) — `source: treasury`. **Keyless.** series_id
    encodes `<dataset_path>:<field>`. Demo `manifests/treasury_fiscal.yml`.
-   **World Bank** (Indicators) — `source: worldbank`. **Keyless.** series_id
    encodes `<country>:<indicator>`. Demo `manifests/worldbank_global.yml`.
-   **BEA** (National accounts) — `source: bea`. Key required. series_id encodes
    `<dataset>:<table>:<line>:<freq>`. Demo `manifests/bea_national_accounts.yml`.
-   **Census** (economic time series) — `source: census`. **Keyless** (key
    optional). series_id encodes `<dataset_path>:<predicate=value,...>`. Demo
    `manifests/census_indicators.yml`.
-   **SEC** (company financials) — `source: sec`. **Keyless** (needs a
    descriptive User-Agent). series_id encodes `<CIK>:<taxonomy>/<tag>:<unit>`;
    filings are captured as point-in-time vintages. Demo
    `manifests/sec_financials.yml`; generate at scale via
    `sources.sec.build_sec_manifest`.
-   **Stooq** (equity daily OHLCV) — `source: stooq`. **Keyless.** CSV, split-
    adjusted close → price return. series_id encodes `<ticker>:<field>` (field
    default `close`). Demo `manifests/equity_stooq.yml` (inactive). See the
    equity sub-plan above.
-   **iShares/State Street** (ETF holdings) — `source: ishares`. **Keyless.**
    Fetches a fund's daily holdings CSV and explodes it into per-constituent
    weight series `<ETF>:<constituent>`; URL resolved from
    `sources.ishares.HOLDINGS_URLS`. Demo `manifests/etf_holdings.yml`
    (inactive). Also the symbol-universe generator
    (`sources.ishares.build_equity_manifest`).
-   **Tiingo** (equity total return) — `source: tiingo`. **Key required**
    (free tier, personal use). series_id is the bare ticker; one fetch
    explodes into `<ticker>:close/divCash/splitFactor/adjClose`. Feeds
    `gold.equity_total_return_index`. Demo `manifests/equity_tiingo.yml`
    (inactive).

## How it works

-   `sources/base.py::HTTPSource` holds the shared rate limiter, retry/backoff,
    and request engine. The `SourceClient` protocol (`get_observations` +
    `normalize`) is the entire surface the orchestrator depends on, so a new
    source is **one client module plus one `SOURCE_FACTORIES` entry** — nothing
    in Bronze/Silver/Gold moves.
-   **`source` is part of the Silver natural key**:
    `(source, series_id, observation_date, realtime_start)`. Bronze also records
    `source` and the endpoint actually called, and the Bronze→Silver replay
    re-derives each payload with the correct per-source normalizer.
-   **Keys/secrets**: `bls_api_key` / `eia_api_key` config settings, `BLS_API_KEY`
    / `EIA_API_KEY` env vars, or the same Databricks secret scope as FRED
    (`secrets/<scope>/bls_api_key`, `.../eia_api_key`). The CLI and notebook
    entrypoints require only the keys the *active* sources need.
-   **Deployment**: run everything in one job, or one job per source
    (`resources/source_jobs.yml` — paused `bls_ingestion` / `eia_ingestion`
    templates scoped to their manifest files).

## Still requires (not engineering)

-   Quant sign-off on which non-FRED series to activate.
-   A live check of the demo series IDs once API keys are configured (blocked in
    the build environment by egress policy; IDs are structurally verified).

------------------------------------------------------------------------

# Equity Price & Total Return — Two-Source Sub-Plan (Stooq + Tiingo)

**Status: implemented (both slices).** The equity asset class is built end to
end, reusing the existing Bronze → Silver → DQ → Gold → audit path unchanged:
- **Stooq price return + constituents** — `sources/stooq.py`,
  `sources/ishares.py`, `build_equity_manifest` → `gold.equity_return_daily`
  + `gold.index_constituents`; manifests `equity_stooq.yml` / `etf_holdings.yml`.
- **Tiingo total return** — `sources/tiingo.py` (keyed) →
  `gold.equity_total_return_index`, dividends reinvested, reconstructed from
  the **raw** `close` + `divCash` + `splitFactor` (not Tiingo's `adjClose`), so
  it survives dividend restatements; manifest `equity_tiingo.yml`. Config key
  `tiingo_api_key` / `TIINGO_API_KEY` (gated only when a `tiingo` series is
  active).

Both backends, `equity_views.py`, DDL, Power BI catalog rows, and tests are in
place; all manifests ship inactive (verify-first). **Source isolation:** Stooq
and Tiingo both name a `<ticker>:close` series, so the Gold equity builders
read **source-filtered** Silver (Stooq→price, Tiingo→total, iShares→
constituents) instead of the merged latest table, which would otherwise
collapse the two `:close` sources onto one row.

**Ownership decided: this repo owns the equity pipeline.** Equities are folded
into *this* medallion pipeline (gaining PIT / DQ / audit / total-return-from-
raw-inputs); the `market_terminal` project and its `market_data_pipeline`
(Yahoo/DuckDB) stay a **separate** system. No dependency runs between them —
if `market_terminal` ever needs these series it reads the published Gold
tables, exactly as it would any other consumer.

> ⚠️ **Verify before building.** Free-tier terms in market data shift often
> and could not be checked live from this environment. Confirm current quotas
> **and licensing** on each provider's site first. In particular, **Tiingo's
> free tier is personal / non-commercial** — commercial use needs a paid
> plan; that is a licensing decision, not an engineering one.

## Why two sources

No single free API gives both breadth *and* the dividend data needed for
total return. Split the job:

-   **Stooq — breadth (price return).** Keyless, no account, effectively
    unlimited; publishes a **bulk EOD ZIP of the entire US market** plus
    per-ticker daily CSV. **Split-adjusted close only — no dividends**, so it
    yields *price* return for thousands of tickers at near-zero API cost. This
    is the wide net.
-   **Tiingo — depth (total return).** Free with a (free) account + key;
    metered (~1,000 req/day, ~50/hr, and the binding limit **~500 unique
    symbols/month**). Its daily endpoint returns, per ticker, `close`,
    **`adjClose`, `divCash`, and `splitFactor` in one call** — the only free
    tier that hands you dividend cash amounts, so Gold can compute true total
    return. Reserve it for the core list: S&P 500 constituents + the broad
    ETFs (SPY/VTI/QQQ/IWM/EFA/EEM/AGG/TLT/HYG/GLD…). ~530 symbols brushes the
    500/month cap — trim to top weights + ETFs, or let Yahoo (fragile,
    unofficial) cover overflow.

**Design bias — store inputs, derive returns.** Prefer the raw
`close`/`divCash`/`splitFactor` over a pre-adjusted number: computing total
return in Gold from raw inputs (below) is reproducible and survives dividend
restatements, exactly the Bronze-verbatim → Gold-derived principle the rest
of the pipeline follows. Pre-adjusted feeds (Yahoo's `Adj Close`) can't be
rebuilt when a dividend is corrected.

## Constituent list — daily ETF-holdings CSV (the hard part, solved freely)

Index *membership* is licensed data with no good free API. The free,
defensible workaround: **iShares and State Street publish full ETF holdings
as daily CSV/XLSX** (e.g. iShares **IVV** "Detailed Holdings and Analytics"
CSV; State Street **SPY** holdings file). Ingest that file to get, per day,
the constituent tickers **and their weights**. It serves two roles:

1.  It *is* the symbol universe — `build_equity_manifest(holdings_csv)`
    generates the ticker manifest Stooq/Tiingo pull (mirrors
    `sources.sec.build_sec_manifest`), so the pull list tracks index changes
    automatically instead of being hand-maintained.
2.  It feeds `gold.index_constituents` (ticker × weight × as-of × index),
    the weight source for any index-reconstruction or attribution Gold table.

This is a scraper/CSV-puller, not a JSON API, but it still fits `HTTPSource`
(fetch bytes → normalize); the daily holdings file is captured verbatim in
Bronze and exploded in Silver.

## How it fits the existing abstraction

-   **New `source:` values `stooq`, `tiingo`, `ishares` (holdings).** Each is
    **one client module + one `SOURCE_FACTORIES` entry** (`sources/stooq.py`
    ~ the size of `census.py` since it's CSV-over-HTTP; `sources/tiingo.py` ~
    `eia.py`; `sources/ishares.py` for holdings). Nothing in Bronze/Silver/
    Gold moves. `source` is already in the Silver natural key.
-   **Scalar-explode to fit the existing single-`value` Silver, recommended.**
    Silver is a scalar `(source, series_id, observation_date, value, …)`
    model; a stock bar is multi-field. Encode `series_id = <ticker>:<field>`
    (e.g. `AAPL:close`, `AAPL:adjClose`, `AAPL:divCash`, `AAPL:splitFactor`,
    `AAPL:volume`) — the same composite-id convention Treasury
    (`<dataset>:<field>`) and World Bank (`<country>:<indicator>`) already
    use. This keeps **every** downstream mechanism (DQ, replay, PIT, the Gold
    transforms) working with zero schema change; the trade-off is more series
    rows. *Alternative:* a dedicated wide `silver_equity_bar` table — more
    natural for OHLCV but breaks the one-path principle and needs its own
    Gold; only adopt it if true bar semantics are needed downstream.
-   **Not revised → `vintage_enabled: false`, blank `realtime_start`** (same
    as `rates.yml` market data). Incremental restate-last-N still applies:
    pull the last ~5 observations each run to catch late split/dividend
    corrections.
-   **Keys**: `tiingo_api_key` config setting / `TIINGO_API_KEY` env var /
    Databricks secret scope, exactly like `eia_api_key`; Stooq and the
    holdings CSV are keyless. The CLI only demands the key when a `tiingo`
    series is active.

## New Gold objects

Computed by the shared-Python-engine pattern (one function, both backends),
like every other Gold table:

-   **`gold.equity_total_return_index`** — the payoff of this whole plan.
    `TR_t = TR_{t−1} × (P_t + D_t) / P_{t−1}` from raw `close` + `divCash`
    (dividends reinvested), indexed to 100 at each series' start. One row per
    ticker × date. Rebuildable when a dividend is restated.
-   **`gold.equity_return_daily`** — price return from `adjClose` (already
    split-adjusted) and total return day-over-day, per ticker × date; MoM/YoY/
    trailing-window horizons reuse the existing transform + rolling-window
    engines directly.
-   **`gold.index_constituents`** — ticker × weight × shares × as-of × index,
    exploded from the holdings CSV.
-   **Free reuse:** point Stooq's and Tiingo's `close` for the same ticker at
    the existing **cross-source reconciliation** engine
    (`config/reconciliations.yml`) to flag price disagreements — no new code.

## Open decisions (for the user)

1.  **Which repo owns equities.** ✅ **Decided: this repo.** Equities live in
    this medallion pipeline; `market_terminal` stays separate (see the
    Ownership note above).
2.  **Scalar-explode vs. a wide `silver_equity_bar`** (recommendation:
    scalar-explode — see above).
3.  **Tiingo symbol budget.** ✅ **Resolved:** `manifests/equity_tiingo.yml`
    sits at 85 tickers, comfortably under the ~500/month cap — no Yahoo
    overflow needed.
4.  **Commercial use** — ✅ **Decided: personal use.** Tiingo's free tier
    covers this, so the total-return path is unblocked and no paid plan is
    needed. (If usage ever turns commercial, revisit — the free tier would no
    longer apply and the total-return path would need paid Tiingo or a
    different dividend source.)

------------------------------------------------------------------------

# ML Extensions Sub-Plan

**Status: ALL PHASES IMPLEMENTED** (ML-0 through ML-6, plus the ML-3b and
ML-5b enhancements). This section was originally the build spec for an
ML/statistical-inference tier on top of the existing Gold layer; it's kept
below as the design record (principles, dependency graph, per-phase
rationale) since the tables it describes now ship in Gold: `gold.
ml_feature_matrix` (ML-0), `gold.yield_curve_ns_factors` (ML-1), `gold.
macro_factor_scores`/`macro_factor_loadings` (ML-2), `gold.
recession_probability_daily` (ML-3, incl. the ML-3b horizon forecasts),
`gold.macro_anomaly_scores` (ML-4), `gold.equity_factor_attribution` (ML-5)
and `gold.equity_factor_implied_return` (ML-5b), and `gold.inflation_forecast`
(ML-6, marked done below).

## Design principles

These carry over from the rest of the pipeline and must not be relaxed:

1. **Train on point-in-time data only.** Every model reads
   `gold.fred_point_in_time` or the expanding-z variants, never
   `gold.fred_latest_observation`. Look-ahead bias via future revisions is
   not acceptable even in development.

2. **Expanding estimation window.** Parameters are re-estimated with all
   data available through each date — same discipline as the existing
   expanding z-score in `gold.fred_feature_transforms`. No look-forward
   calibration, no train/test split that crosses time.

3. **Pure Python; scipy-optional.** Match the `regime_stats.py` pattern
   (its own F-distribution, OLS, and CCF — all pure Python). Where scipy
   is genuinely needed for speed (e.g. curve fitting at startup over 5,000
   dates), wrap behind `try/except ImportError` with a pure-Python fallback.
   Do not make scipy a hard dependency.

4. **Config-driven inputs.** Each model's input series list and
   hyperparameters live in a YAML config file (like `config/stats_pairs.yml`
   or `config/regime.yml`). Adding a feature = editing YAML, not Python.

5. **Model metadata on every row.** `model_vintage` (date parameters were
   last estimated), `n_obs` (training sample size), and `is_backfilled`
   (True when fewer than `min_obs` points are available — early-history rows
   that a consumer should treat cautiously). These belong in the output table
   as columns, not as separate audit tables.

6. **Power BI catalog guard stays green.** Add a `gold.powerbi_catalog` entry
   for each new Gold table at implementation time — the
   `test_powerbi_catalog_covers_gold_tables` test will fail otherwise.

7. **Additive only.** No existing Gold tables are renamed or schema-changed.

---

## Dependency graph

```
ML-0  Feature Matrix ──► ML-2  PCA Factor Scores ──► ML-5  Equity Factor Attribution
                                                  ──► ML-4  Macro Anomaly Detection
ML-1  Nelson-Siegel  ──► ML-3  Recession Probability
                     ──► ML-6  Inflation Forecasting (auxiliary input)
```

ML-0 and ML-1 are the foundation. Everything else layers on top of one or
both. ML-5 additionally requires `gold.equity_return_daily` (already built).

---

## Phase ML-0: ML Feature Matrix (~1 day)

**New table: `gold.ml_feature_matrix`**
**New config: `config/ml_features.yml`**

A single wide table aligned to a daily date grid: one row per date, one
column per selected feature, sourced from the existing Gold views. All
downstream ML phases read from this rather than joining multiple tables
themselves.

### What it pulls together

| Source Gold table | Features |
|---|---|
| `gold.fred_feature_transforms` | Expanding z-scores for DGS10, UNRATE, PAYEMS, CPIAUCSL, PCEPILFE, INDPRO, RSAFS, HOUST, PERMIT, GDP, T10YIE |
| `gold.treasury_curve_metrics` | `level`, `slope_10y2y`, `slope_10y3m`, `curvature_2_5_10`, `butterfly_2_10_30` |
| `gold.credit_spread_daily` | OAS z-score (IG and HY, separately) |
| `gold.funding_stress_daily` | Composite stress gauge (0–100) |
| `gold.macro_regime_daily` | `composite_score` and per-pillar scores |
| `gold.equity_return_daily` | SPY and QQQ trailing-21d return (optional; set aside if equity data inactive) |

### Config schema (`config/ml_features.yml`)

```yaml
ml_features:
  date_grid: daily
  forward_fill_days: 31     # max gap to fill; beyond this a row is NULL
  min_features_required: 10  # rows with fewer live features are excluded

  features:
    - name: dgs10_z
      source_table: fred_feature_transforms
      series_id: DGS10
      column: zscore
    - name: unrate_z
      source_table: fred_feature_transforms
      series_id: UNRATE
      column: zscore
    # ... (quant sign-off required on full list — see Open Decisions)
```

### Output columns

`observation_date`, `[feature_name]` × N, `n_features_live`
(count of features sourced directly, not filled), `n_features_filled`
(count forward-filled), `n_features_null` (count still null after filling).

### Engineering notes

The main complexity is mixed-frequency alignment: treasury curve and
macro z-scores are daily; CPI/PCE and GDP are monthly/quarterly. Use the
same `_downsample_asof` logic already in `features.py` — forward-fill
to daily within the `forward_fill_days` window, carry the last known
quarterly value forward up to 31 days. Beyond that, the slot is NULL
(don't impute). The `fill_pct` column on each row tells the consumer what
fraction of features are live vs. carried forward.

---

## Phase ML-1: Nelson-Siegel Yield Curve Fitting (~2 days)

**New table: `gold.yield_curve_ns_factors`**
**New module: `src/fred_pipeline/ns_model.py`**

Fits the Nelson-Siegel (1987) parametric model to the Treasury curve each
day. Goes beyond the discrete metrics already in `gold.treasury_curve_metrics`
(ad-hoc level/slope/curvature differences) by giving a *parametric,
internally-consistent* description of the full curve:

```
y(τ) = β₀ + β₁ · L(τ,λ) + β₂ · C(τ,λ)

where:
  L(τ,λ) = (1 − e^{−τ/λ}) / (τ/λ)          [slope loading]
  C(τ,λ) = L(τ,λ) − e^{−τ/λ}               [curvature loading]
```

**Economic meaning of the three factors:**

- **β₀** — long-run level (all maturities converge here as τ → ∞)
- **β₁** — slope / short-rate spread (negative when normal curve; β₁ < 0 means 10y > 1m)
- **β₂** — curvature / hump (positive when the medium is richer than the extremes)
- **λ** — decay speed (set to a fixed value ≈ 1.6–2.5 for US Treasuries, or grid-searched)

### Input

`gold.treasury_curve` — the 8 observed tenors: `1m`, `3m`, `6m`, `1y`,
`2y`, `5y`, `10y`, `30y`. Requires ≥ 4 non-null tenors per date for a valid
fit (`fit_valid = True`).

### Implementation approach

Two-step: (1) fix λ at 1.7 (the Diebold-Li value that maximises the
loadings' variance at medium tenors — a standard calibration; no estimation
needed), then (2) run a closed-form linear regression for (β₀, β₁, β₂) as
a function of the loading matrix. This makes the fit trivially pure-Python —
just 3×3 OLS per date. The optional step: grid-search λ ∈ [0.5, 5.0] (step
0.1) for the daily λ that minimises RMSE, if the fixed-λ RMSE exceeds a
threshold. Emit `lambda_estimated = True/False`.

### Output columns

`observation_date`, `beta0`, `beta1`, `beta2`, `lambda`, `lambda_estimated`,
`fit_rmse`, `n_tenors`, `fit_valid`.

### Why it matters downstream

- **ML-2 (PCA)**: β₀/β₁/β₂ are compact, orthogonal-ish curve features that
  replace the 8 raw tenors in the feature matrix.
- **ML-3 (Recession)**: β₁ (slope) is the Estrella-Mishkin recession
  predictor — cleaner than the ad-hoc 10y–3m spread.
- **Power BI**: the NS factors can be plotted as a 3-panel time series; the
  fitted curve can be visualised at any tenor not directly observed.

---

## Phase ML-2: Macro PCA Factor Scores (~3 days)

**New tables: `gold.macro_factor_scores`, `gold.macro_factor_loadings`**
**New module: `src/fred_pipeline/macro_pca.py`**

Extracts latent macro factors from the standardized feature matrix via
principal-component analysis. Produces 3–5 orthogonal factors with
established economic interpretations:

- **PC1 ("growth cycle")**: typically loads positively on activity
  (INDPRO, PAYEMS) and negatively on credit spreads and unemployment
- **PC2 ("monetary / inflation")**: loads on rates, breakeven inflation, CPI
- **PC3 ("liquidity / stress")**: loads on HY OAS, funding spreads, NFCI

This replicates the well-documented macro factor decomposition (Bernanke,
Boivin & Eliasz 2005; Ang & Piazzesi 2003 for the rate curve) but derived
from the live pipeline data with PIT-safe expanding estimation.

### Input

`gold.ml_feature_matrix` (Phase ML-0). The ~20 standardized features
(already z-scored) make PCA scale-invariant without an extra step.

### Implementation approach

**Expanding PCA via incremental eigendecomposition.** At each date:
1. Maintain the expanding sample covariance matrix Σ_t (rank update: O(p²)
   per new observation, where p = number of features).
2. Extract top-K eigenvectors via power iteration (K=5, up to 50 iterations
   to convergence — typically ~10). Pure NumPy, ~30 lines.
3. Sign-anchor: ensure the loading on the first feature in each PC is always
   positive (swap sign if not). This prevents the arbitrary sign flips that
   occur when the eigenspace shifts.
4. Project the current feature vector onto the K eigenvectors → PC scores.

For the **loadings table**: re-estimate monthly (not daily) — daily
re-estimation creates noise in the loadings without adding information.
Emit a `model_vintage` column so consumers can tell when the factor
definition last changed.

### Output columns

`gold.macro_factor_scores`: `observation_date`, `pc1`..`pc5`,
`pc1_var_explained`..`pc5_var_explained`, `cumulative_var_5pc`,
`n_features`, `model_vintage`.

`gold.macro_factor_loadings`: `model_vintage`, `feature_name`,
`pc1_loading`..`pc5_loading`. Updated monthly.

### Decision needed

**Factor rotation.** Raw PCA factors can be hard to interpret because each
eigenvector loads on many features. Options: (a) sign-anchoring only
[recommended — simplest, no ambiguity in direction], (b) varimax rotation
[more interpretable but harder to implement incrementally and breaks
orthogonality], (c) promax [oblique, allows correlated factors]. Recommend
(a) for the first implementation; revisit if the loadings are noisy.

---

## Phase ML-3: Recession Probability Model (~3 days)

**New table: `gold.recession_probability_daily`**
**New module: `src/fred_pipeline/recession_model.py`**

Estimates the real-time probability of being in an NBER recession at each
date using logistic regression on leading macro indicators. The Estrella-Mishkin
(1998) and Wright (2006) curve-slope models are the standard benchmarks; this
extends them with additional predictors available in the pipeline.

### Input features

| Feature | Source | Economic rationale |
|---|---|---|
| NS slope factor (β₁) | `gold.yield_curve_ns_factors` | Estrella-Mishkin core predictor |
| Unemployment 3m change | `gold.fred_feature_transforms` (mom) | Leading indicator of slowdown |
| INDPRO 6m change | same | Manufacturing cycle |
| HY OAS z-score | `gold.credit_spread_daily` | Credit stress / financing conditions |
| Funding stress gauge | `gold.funding_stress_daily` | Systemic risk |
| Composite regime score | `gold.macro_regime_daily` | Multi-pillar signal |

### Target variable

`USREC` from FRED (binary: 1 = NBER recession month, 0 = expansion). The
`macro_flags.yml` manifest ships it inactive — **must be activated before
this phase can produce real estimates**. Development and testing can use
synthetic labels (e.g. 1 for dates matching known recessions from the NBER
website, hardcoded in the test fixture).

### Implementation approach

**Expanding logistic regression via IRLS (Iteratively Reweighted Least
Squares).** IRLS converges to the MLE of the logistic model in ~10–20
iterations and is implementable in ~40 lines of NumPy — no sklearn or
scipy needed. Re-estimate at each date using all available labeled history
through that date.

Regularization: `L2` ridge with λ = 0.01 (prevents divergence on small
samples and near-multicollinear features). Tune on the first 20 years of
USREC history; hold out the rest.

Early history rows (fewer than `min_obs = 60` labeled months) get
`is_backfilled = True` — the logit is estimated but the consumer should
treat these as unreliable. Emit the training sample size (`n_obs`) so the
consumer can apply their own threshold.

### Output columns

`observation_date`, `recession_prob` (0–1), `logit_score` (the raw
log-odds), `model_vintage`, `n_obs_training`, `is_backfilled`.

### Enhancement (Phase ML-3b): Horizon forecasts

Separate logistic models for P(recession starting in next 3m / 6m / 12m),
using the same features lagged by the horizon. The 12m-ahead model is the
most useful for asset allocation. Adds three extra columns to the same
table: `prob_recession_3m`, `prob_recession_6m`, `prob_recession_12m`.

---

## Phase ML-4: Macro Anomaly Detection (~2 days)

**New table: `gold.macro_anomaly_scores`**
**New module: `src/fred_pipeline/anomaly.py`**

Computes a multivariate outlier score for each date: the Mahalanobis
distance of the factor-score vector from the expanding-window distribution.
Complements the rule-based regime system — flags dates where the overall
macro state is statistically unusual regardless of which named regime applies.

Known anomaly dates should show the highest scores: GFC 2008–2009, COVID
March 2020, 1987 Black Monday (equity factor), 1994 bond market crash
(curve factor).

### Input

`gold.macro_factor_scores` PC1..PC3 (a 3D space is sufficient and
numerically stable; Σ is 3×3, trivially invertible). Alternatively, use the
full `gold.ml_feature_matrix` for a higher-dimensional version (more
sensitive but numerically noisier — use the ridge stabilizer from
`regime_stats.py`).

### Implementation

```
D²_t = (x_t − μ_t)ᵀ Σ_t⁻¹ (x_t − μ_t)
```

where μ_t and Σ_t are the expanding mean and covariance through date t.
Invert Σ with the same ridge fallback used in the Granger OLS
(`λ = 1e-8 × trace/k` — already tested in `regime_stats.py`). Under
multivariate normality, D² ~ χ²(k); the p-value from the χ² CDF
gives the anomaly probability. A pure-Python regularized incomplete gamma
function (same approach as the F-distribution in `regime_stats.py`) avoids
any scipy dependency.

### Output columns

`observation_date`, `mahal_distance`, `chi2_pct` (χ²(k) percentile of D²,
0–1), `is_anomaly` (True when `chi2_pct > 0.99`), `n_features_used`,
`model_vintage`.

### Downstream uses

1. **Governance gate**: flag the current date in a Power BI alert tile if
   `is_anomaly = True` — something statistically unusual is happening.
2. **Research**: anomaly dates are natural breakpoints for regime-conditional
   backtests.
3. **Data quality**: a sudden spike on a date with no macro news likely
   indicates a feed error (especially useful for equity data).

---

## Phase ML-5: Equity Factor Attribution (~3 days)

**New table: `gold.equity_factor_attribution`**
**New module: `src/fred_pipeline/equity_factor.py`**

Decomposes each equity ticker's realized daily returns into exposures to
the macro factors from Phase ML-2, using rolling OLS. Produces a
Fama-French-style attribution but driven by macro factors rather than
style — appropriate for understanding _why_ a ticker moved in the context of
the macro cycle.

### Input

- `gold.equity_return_daily` — the daily `price_return` per ticker
- `gold.macro_factor_scores` — PC1..PC3 as the explanatory variables
  (frequency-aligned to daily; factor scores are already at daily grain)

### Implementation

Rolling OLS over a configurable window (default 252 trading days) per
ticker. For each window ending at date t:

```
r_ticker = α + β₁·PC1 + β₂·PC2 + β₃·PC3 + ε
```

The OLS normal equations (XᵀX)β = Xᵀy are solved in pure Python with
partial-pivoting Gaussian elimination (already proven in `regime_stats.py`
for the Granger test). Add the same ridge regularizer for near-singular
windows.

Emit one row per `(ticker, window_end_date)` for each configured window
length (default: [63, 126, 252] days).

### Output columns

`ticker`, `window_end_date`, `window_days`, `alpha`, `beta_pc1`, `beta_pc2`,
`beta_pc3`, `r_squared`, `residual_vol`, `information_ratio`
(`alpha / residual_vol × √252`), `n_obs`.

### Enhancement (Phase ML-5b): Factor-implied return

Add a `gold.equity_factor_implied_return` table: for each date × ticker,
multiply the current factor scores by the ticker's trailing betas → factor-
implied return. Compare to realized → residual "idiosyncratic alpha."
One row per ticker per date (forward-filled betas, current factor scores).

---

## Phase ML-6: Short-Horizon Inflation Forecasting ✅ DONE

**New table: `gold.inflation_forecast`**
**New module: `src/fred_pipeline/inflation_model.py`**

Generates 1m / 3m / 6m / 12m ahead CPI and PCE forecasts from
autoregressive and VAR models estimated on the existing inflation data.
The first deliverable for "forward-looking" macro analytics — complements
the backward-looking `gold.inflation_explorer`.

### Input

- `gold.inflation_explorer` — per-item MoM changes for CPI-U SA, CPI-U
  NSA, and PCE (component breakdown available when BLS basket is activated)
- `gold.fred_feature_transforms` — energy prices (EIA), PPI (if active),
  and the breakeven inflation series (T5YIE / T10YIE)
- `gold.macro_factor_scores` (from ML-2) — PC2 (inflation/monetary factor)
  as an exogenous predictor

### Implementation

**AR(p) per series.** Select lag order p by BIC (expanding window, from
p=1 to p=12 monthly lags). Forecast h months ahead by recursive
substitution. Confidence intervals from bootstrapped residuals
(500 draws, pure Python `random` module — no scipy needed for this).

**VAR(p) for joint CPI + PCE.** Treating them as a bivariate system captures
the common monetary policy signal. Same expanding estimation; same BIC lag
selection. The 2×2 companion matrix is invertible in pure Python.

Both models emit a `model_vintage` column (date of last re-estimation;
re-estimate monthly to avoid daily refitting cost).

### Output columns

`series_id` (e.g. `CPIAUCSL`, `PCEPI`), `forecast_date` (as-of),
`horizon_months` (1/3/6/12), `forecast_value`, `lower_80`, `upper_80`,
`lower_95`, `upper_95`, `model_type` (`ar` / `var`), `lag_order`,
`model_vintage`, `n_obs_training`.

**Implemented:**
- `src/fred_pipeline/inflation_model.py` — AR(p)/VAR(p) engines with BIC lag selection,
  recursive h-step forecasting, and fixed-coefficient residual bootstrap CIs (500 draws).
- `config/inflation_forecast.yml` — default config (CPIAUCSL + PCEPI, horizons 1/3/6/12m).
- `gold_inflation_forecast` DDL in `local_store.py`; wired into `build_gold()` for both
  the SQLite and Spark/Delta backends.
- Catalog entry in `global_views.py` (POWERBI_CATALOG).
- `tests/test_inflation_model.py` — 46 tests covering all primitives and the main engine.

---

## Open decisions

1. **USREC activation (blocks ML-3 live estimates).** ✅ **Resolved:**
   `USREC`/`USRECD` are `active: true` in `manifests/macro_flags.yml` —
   ML-3 trains on real recession labels, not synthetic ones.

2. **scipy as optional dependency.** The plan keeps scipy entirely optional.
   If the pure-Python NS fitting (ML-1) proves too slow over the full
   historical range at startup, allow `scipy.optimize.minimize` with a hard
   `try/except` wrapper. Do not add scipy to `requirements.txt` without
   confirming it doesn't conflict with Databricks Runtime version.

3. **Feature selection sign-off (config/ml_features.yml).** The ~20-series
   starting set above is a suggestion. A quant should approve the list before
   ML-2 factor loadings are considered interpretable. The config-driven design
   means this is a YAML edit, not a code change.

4. **Factor rotation convention (ML-2).** ✅ **Resolved:** implemented as
   recommended — sign-anchoring (the max-abs loading component is always
   positive). Revisit with varimax if the raw PCA factors prove hard to
   interpret with real data.

5. **Equity universe for ML-5.** Rolling OLS over 252 days × N tickers ×
   3 factors runs in ~1ms per ticker; at 500 tickers that's ~30s per build.
   Acceptable, but set `active: false` on equity manifests and gate ML-5
   behind an `equity_factors_enabled` config flag until the Tiingo key is set.

6. **Databricks MLflow integration (optional).** The pure-Python model
   outputs land in Gold Delta tables — no MLflow dependency. If the team
   wants experiment tracking or registered models, the same engine functions
   can be wrapped with `mlflow.log_params` / `mlflow.log_metrics` calls added
   _around_ the engine (not inside it), maintaining the separation between
   pure analytics and infrastructure.

------------------------------------------------------------------------

# Engineering Handoff Checklist

> Actionable, checkbox form with commands and per-item owners:
> **`docs/deployment_runbook.md`** (workspace provisioning + quant/ops decisions).

## Quant

-   Define series universe
-   Approve validation rules
-   Identify vintage-sensitive series
-   Validate outputs

## Engineering

-   Create Unity Catalog objects
-   Configure Delta tables
-   Configure workflows
-   Configure Asset Bundles (incl. per-source jobs in resources/source_jobs.yml)
-   Configure secret scopes (fred_api_key + optional bls_api_key / eia_api_key)
-   **Spark/Delta backend parity pass** ✅ **implemented** — `gold.py` now calls
    all new pure-Python engines: ML-1 NS factors (`_build_terminal_views`),
    equity price reconciliation (`_build_equity_views`), new `_build_zscore_views`
    (rolling z-score + heatmap), and new `_build_ml_pipeline` (ML-0/2/4/3/5 in
    sequence). The Databricks path is now output-identical to
    `local_store.build_gold()`.
-   **Historical backfill CLI command** ✅ **implemented** — `fred-pipeline backfill`
    generates point-in-time Gold snapshots over a date range (monthly/weekly/daily
    cadence). For each snapshot date D, Silver is filtered to rows with
    `realtime_start <= D` (non-vintage series always included), collapsed to the
    latest revision, and a focused set of Gold engines is run: feature transforms,
    ML-0 feature matrix, ML-2 PCA factor scores/loadings, ML-4 anomaly scores,
    regime playbook, and ML-3 recession probability. All output rows are tagged
    with `as_of_date = D` and written to a separate SQLite database (default
    `fred_backfill.db`) with `pit_*`-prefixed tables. Resume support (skips
    already-computed dates via `pit_backfill_log`) and a `--no-resume` override
    are included. 22 new unit tests in `tests/test_backfill.py`.
-   **Cross-series structural break detection** ✅ **implemented** —
    `compute_series_structural_breaks` in `regime_stats.py` adds two rows per
    configured pair to `gold.series_structural_breaks`:
    * **Chow** (`test_type='chow'`): scans all candidate break dates (15% trim from
      each end) under `series_b ~ 1 + series_a`; reports the date with the highest
      F-statistic and its p-value.
    * **CUSUM** (`test_type='cusum'`): Brown-Durbin-Evans cumulative-residual test;
      reports the first 5%-boundary crossing date and a KS-based p-value approximation.
    Both tests report pre/post segment means, observation counts, and an
    `is_significant` flag. Wired into `local_store.build_gold()` and the Spark
    `_build_regime_stats()` path; added to the Power BI catalog under EDA.
    20 new unit tests in `tests/test_structural_breaks.py`.
-   **PCE item-level via BEA API** ✅ **implemented** — `config/inflation_items.yml`
    PCE/SA tree expanded from headline + core only to a full 23-item hierarchy using
    BEA NIPA Table 2.4.4 (`NIPA:T20404:LINE:M`) series IDs:
    * **Level 0** (FRED-sourced): `PCEPI` (headline)
    * **Level 1** (FRED-sourced): `PCEPILFE` (core); BEA: Goods (line 2), Services (line 13)
    * **Level 2** (BEA): Durable Goods, Nondurable Goods, Household Services, NPISHs
    * **Level 3** (BEA): 4 durable + 4 nondurable + 7 services spending categories
    * **Waterfall** (16 items): Motor Vehicles, Furnishings, Recreational Goods, Other Durable,
      Food Off-Premises, Clothing, Energy Goods, Other Nondurable, Housing & Utilities,
      Health Care, Transportation Svcs, Recreation Svcs, Food Services & Accommodations,
      Financial Svcs & Insurance, Other Svcs, NPISHs. Weights are approximate 2023-era nominal
      PCE expenditure shares (sum = 100%); refresh from BEA Section 2 Underlying Detail.
    * **Manifest**: `manifests/bea_pce_items.yml` (21 BEA series, all `active: false`).
      Activate by setting `BEA_API_KEY` and flipping `active: true`.
    * `compute_inflation_explorer` requires no engine changes — the function already handles
      mixed FRED/BEA `series_id` formats by exact matching; BEA price index levels flow
      through the same MoM/YoY/acceleration/contribution path as CPI items.
    * Power BI catalog entries for `inflation_explorer` and `inflation_contribution` updated
      to describe PCE drill-down and the 16-item waterfall.
    * 28 new unit tests in `tests/test_pce_items.py`.

## Platform

-   Configure permissions
-   Configure monitoring
-   Configure alerts
-   Configure cost controls

------------------------------------------------------------------------

# First Sprint

Deliver:

-   Databricks project
-   YAML manifests
-   Python package
-   Bronze/Silver/Gold tables
-   Audit framework
-   Data quality checks
-   Initial Databricks Workflow
-   Documentation

The objective is a production-shaped MVP that can scale to hundreds of
FRED series while remaining governed, auditable, and reusable for quant
research and optimization workflows.
