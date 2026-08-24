# fred-bronze-to-gold-pipeline

A production-grade, **manifest-driven**, **multi-source** economic-data
ingestion pipeline that lands data in a **Bronze → Silver → Gold** medallion
architecture on **Databricks + Delta Lake**. Built for quant research,
dashboards, optimizer inputs, and **point-in-time** macro features.

Started FRED-only; the source layer is now pluggable, so a series declares its
upstream API (`source:` in the manifest) and flows through the same path.
**Thirteen sources are wired**: FRED, BLS, EIA, ECB, US Treasury, World Bank,
BIS, BEA, Census, SEC (company financials), Tiingo, Stooq, and iShares — see
[Data sources](#data-sources).

> 📄 The original product spec / engineering handoff lives in
> [`docs/handoffs/completed/handoff.md`](./docs/handoffs/completed/handoff.md).
> This README is the practical guide to the implementation. Architecture
> rationale is in
> [`docs/deployment/architecture.md`](./docs/deployment/architecture.md); every
> table/column is in
> [`docs/dictionary/data_dictionary.md`](./docs/dictionary/data_dictionary.md);
> the data-quality rules and where to review/change them are in
> [`docs/validation/validation.md`](./docs/validation/validation.md). A
> dataset-agnostic, reusable build spec (usable as a prompt for standing up a
> *new* ETL like this one) is in
> [`docs/instructions/etl_build_spec.md`](./docs/instructions/etl_build_spec.md).
> To run with the non-FRED sources (which API keys, activating series, exact
> commands), see
> [`docs/instructions/running_multi_source.md`](./docs/instructions/running_multi_source.md).
> The Power BI report suite built on the Gold layer is specified in
> [`docs/handoffs/powerbi_report_suite.md`](./docs/handoffs/powerbi_report_suite.md),
> with per-report source queries, data models and readiness in
> [`docs/handoffs/powerbi_report_build_plan.md`](./docs/handoffs/powerbi_report_build_plan.md).
> Run alerting (who gets emailed when a stage fails) is in
> [`docs/handoffs/run_alerting.md`](./docs/handoffs/run_alerting.md).

## What it does

```
manifests/*.yml → source API (FRED / BLS / EIA / ECB / Treasury /
                              World Bank / BEA / Census / SEC) → Bronze (raw JSON)
     → Silver (normalized, MERGE) → Data Quality
     → Gold (latest / point-in-time / daily feature matrix)
     → full audit trail (runs, series, DQ results)
```

* **Manifest-driven** — the series universe and per-series policy (source, load
  type, validation profile, vintage tracking, ownership) live in reviewable YAML.
* **Multi-source** — a pluggable `SourceClient` layer; adding a source is one
  client module + one registry entry. Bronze/Silver/Gold are source-agnostic.
* **Idempotent** — Silver is a Delta `MERGE` on
  `(source, series_id, observation_date, realtime_start)`; re-runs never
  duplicate, and each row is tagged with its origin.
* **Point-in-time** — vintage-enabled series retain every revision, enabling
  leak-free backtests ("what was known on date X").
* **Auditable** — every run, series-run, and data-quality check is persisted.
* **Testable** — the business logic is pure Python (no Spark/network), covered
  by a fast unit-test suite; PySpark is imported lazily only for I/O.
* **Promotable** — one codebase targets `macro_dev` / `macro_test` /
  `macro_prod` via config + a Databricks Asset Bundle.

## Repository layout

```
fred-bronze-to-gold-pipeline/
├── config/               # config.example.yaml template (real config.yaml git-ignored)
├── manifests/            # YAML series universe (per-domain + per-source) + JSON schema
├── src/fred_pipeline/    # the Python package (pure core + Spark I/O)
│   ├── config.py, pipeline.py, cli.py, backfill.py, replay.py
│   │                     #   entrypoints/foundation — stay at the top level
│   ├── sources/          # pluggable source clients (base + fred/bls/eia/ecb/…)
│   ├── catalogs/         # manifest model, API-driven discovery, meta-table sync
│   ├── gold_config/          # config-driven Gold-feature loaders (spreads, curve
│   │                     #   tenors, regime playbook, cross-series, …)
│   ├── io/               # storage backends: Warehouse abstraction, local SQLite,
│   │                     #   Spark/Delta I/O
│   ├── data/                 # Bronze/Silver: raw payload retention, normalization
│   ├── ml/                # statistical/ML models (PCA, Nelson-Siegel, recession
│   │                     #   probability, anomaly detection, inflation forecast)
│   ├── governance/        # audit/lineage, metadata drift+lifecycle, licensing,
│   │                     #   stage tracking + run alerting (email/webhook)
│   ├── validation/        # data-quality rules
│   └── writer/            # Gold-layer builder engines (feature tables, quant
│                          #   transforms, terminal/equity/regime/z-score views)
├── sql/                  # Unity Catalog DDL (00..60, parameterized by {{catalog}})
├── resources/            # Databricks Asset Bundle jobs (main + per-source templates)
├── databricks.yml        # Asset Bundle (dev/test/prod targets)
├── notebooks/            # Databricks job entrypoint
├── .github/workflows/    # CI: unit matrix + Spark/Delta integration job
├── tests/                # pytest suite (Spark tests auto-skip if PySpark absent)
└── docs/                 # architecture, data dictionary, validation, handoffs
                          #   (Power BI report suite + build plan, run alerting,
                          #   FOMC calendar scraper), instructions, deployment
```

## Quickstart (local)

The pipeline has three storage backends, chosen at run time:

| Mode | Flag | Writes to | Needs Spark? |
|---|---|---|---|
| Dry run | `--dry-run` | nothing (extract + DQ only) | no |
| **Local** | `--local` | a **SQLite `.db` file** | **no** |
| Databricks | *(default)* | Delta / Unity Catalog | yes |

```bash
# 1. Install dev dependencies
pip install -r requirements-dev.txt

# 2. Validate the manifests (no network, no Spark)
PYTHONPATH=src python -m fred_pipeline validate --manifests manifests

# 3. Run the fast unit-test suite (no Spark needed; Spark tests auto-skip)
python -m pytest

# 4. Dry-run against the real FRED API (extract + DQ, no writes)
export FRED_API_KEY=your_key_here
PYTHONPATH=src python -m fred_pipeline run --env dev --dry-run
```

Get a free FRED API key at <https://fredaccount.stlouisfed.org/apikeys>.

### Run fully locally and save to a database file

No Databricks, no Spark — the whole Bronze → Silver → Gold → audit flow runs on
your machine and persists to a single SQLite file you can open with any SQLite
tool, `pandas.read_sql`, DBeaver, DuckDB, etc.

```bash
export FRED_API_KEY=your_key_here
PYTHONPATH=src python -m fred_pipeline run --local --db-path fred_local.db
```

This creates `fred_local.db` with all layers as `{schema}_{table}` tables:

```
meta_fred_series              gold_fred_latest_observation
bronze_fred_api_response      gold_fred_point_in_time
silver_fred_observation       gold_fred_macro_feature_daily
audit_etl_run / _series_run   audit_data_quality_result
```

Inspect it, e.g.:

```bash
python - <<'PY'
import sqlite3, pandas as pd
con = sqlite3.connect("fred_local.db")
print(pd.read_sql("SELECT series_id, observation_date, value "
                  "FROM gold_fred_latest_observation "
                  "ORDER BY observation_date DESC LIMIT 10", con))
PY
```

Re-running is idempotent (Silver upserts on the same natural key the Delta MERGE
uses), so you can run it repeatedly against the same file without duplicates.
The same code path, pointed at a `SparkWarehouse` instead, is what runs on
Databricks — so local results match production semantics.

## Operations reference

Every `python -m fred_pipeline <command>` the CLI supports, grouped by what
it does. All commands accept `--env dev|test|prod` and `--config <path>`;
only the flags that differ in meaning are listed per command. (Full detail:
[`docs/instructions/running_multi_source.md`](docs/instructions/running_multi_source.md).)

### Build / extract (talks to external APIs)

| Command | What it does | Key flags |
|---|---|---|
| `run` | Full Bronze → Silver → Gold: extracts active series from every source with an active manifest entry, then rebuilds Gold (unless `--no-gold`) | `--local --db-path`, `--series`, `--source` / `--exclude-source`, `--full`, `--dry-run`, `--no-gold`, `--extract-workers`, `--source-workers`, `--rate-limit-per-minute`, `--source-rate-limits` |
| `price-constituents` | Dynamic Tiingo pricing batch: reads current ETF membership from `gold_index_constituents`, prices only missing/stale tickers (by weight rank), stops on a Tiingo quota hit | `--index-etf`, `--max-symbols`, `--stale-days`, `--rate-limit-per-minute`, `--dry-run`, `--rebuild-gold` |
| `discover` | Generates a new manifest from a FRED category/release/search — not a refresh, a discovery/authoring tool | `--category-id` / `--release-id` / `--search`, `--frequencies`, `--min-popularity`, `--max`, `--out`, `--dry-run` |
| `discover-ecb` | Lists ECB SDMX dataflows for candidate manifest authoring | `--list-flows`, `--search`, `--max`, `--json` |

### Refresh (rebuilds from already-ingested data — no external API calls)

| Command | What it does | Key flags |
|---|---|---|
| `gold` | Rebuilds every Gold table from already-persisted Bronze/Silver | `--local --db-path` |
| `replay` | Rebuilds Silver (+ Gold, unless `--no-gold`) from archived Bronze payloads — reprocesses raw data already on disk, e.g. after fixing a normalization/DQ bug | `--series`, `--no-gold`, `--local --db-path` |

### Maintenance / governance (metadata health, not observation data)

| Command | What it does | Key flags |
|---|---|---|
| `validate` | Schema/duplicate-id check on the manifests — no network, no backend; `--commercial` also fails on any active source whose license doesn't clear commercial use (`config/data_licensing.yml`) | `--manifests`, `--commercial` |
| `reconcile` | FRED-only metadata drift (title/frequency/units, discontinued, not-found) + all-source staleness (any source, via ingested data) | `--series`, `--local --db-path`, `--no-persist`, `--fail-on-drift` |
| `backfill` | Generates point-in-time Gold snapshots over a historical date range into a separate output DB — for backtesting, not a live refresh | `--from`, `--to` (required), `--step monthly\|weekly\|daily`, `--tables`, `--db-path`, `--backfill-db`, `--no-resume` |

## Common workflows

| Goal | Sequence |
|---|---|
| First-ever local run | `validate` → `run --dry-run --series <one id>` (confirm a key/id works cheaply) → `run --local --db-path fred_local.db` |
| Routine daily refresh | `run --local --db-path fred_local.db` (incremental restate-last-N + full Gold rebuild + release calendar, all in one) |
| Large first-time load, rate-limit-safe | `run --local --db-path f.db --full --no-gold --source-workers fred=8,tiingo=1 --source-rate-limits fred=60,tiingo=5` → `gold --local --db-path f.db` (separates slow extraction retries from the one-shot Gold rebuild) |
| Add a brand-new source/series | edit the manifest (`active: true`) → `validate` → `run --dry-run --series <new id>` → `run --local --db-path fred_local.db` |
| Grow ETF constituent coverage | `run --local --db-path f.db --series <ETF ticker>` (refresh holdings **and** rebuild Gold, so `gold_index_constituents` reflects today's snapshot before you plan against it) → `price-constituents --db-path f.db --index-etf <ETF> --dry-run` (preview the next batch) → `price-constituents --db-path f.db --index-etf <ETF> --rate-limit-per-minute 5` (pull it) → `gold --local --db-path f.db` (rebuild once more so price-derived tables pick up the new prices) |
| Health-check the series universe | `reconcile --local --db-path fred_local.db` (safe to run anytime — reads ingested data + FRED metadata only, doesn't touch other sources' quotas) |
| Recover from a bad DQ/normalization bug | fix the code → `replay --local --db-path fred_local.db` (reprocesses already-archived Bronze payloads, no re-fetch, no API quota spent) |
| Backtest "what did Gold look like historically" | `run --local --db-path fred_local.db` (ensure Silver is current) → `backfill --db-path fred_local.db --backfill-db fred_backfill.db --from 2015-01-01 --to 2026-01-01 --step monthly` |
| Onboard a new FRED category/release wholesale | `discover --name <manifest_name> --category-id <id> --dry-run` (preview) → re-run with `--out manifests/<name>.yml` → hand-review the generated YAML → `validate` → `run --dry-run --series <a few ids>` → activate for real |

## Deploy to Databricks

> Full go-live checklist (provisioning steps + the quant/ops decisions, with
> owners): [`docs/deployment/deployment_runbook.md`](docs/deployment/deployment_runbook.md).

```bash
# One-time per environment: create catalog/schemas/tables and the secret scope
#   (run each sql/*.sql file with {{catalog}} replaced by macro_dev/test/prod)
databricks secrets create-scope fred
databricks secrets put-secret   fred api_key

# Deploy + run the job via Asset Bundles
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run fred_ingestion -t dev
```

The job resolves the API key from the `fred/api_key` secret scope, syncs
manifests into the `meta` schema, ingests each series, runs data quality, and
rebuilds the Gold layer — recording a complete audit trail.

## Configuration

Settings can come from a **YAML config file**, environment variables, CLI
arguments, or a Databricks secret scope. Precedence (highest wins):

```
explicit CLI/arg  >  environment variable  >  config file  >  built-in default
```

API keys additionally fall back to a Databricks secret scope when not set by any
of the above — `fred_api_key` from `secrets/<scope>/api_key`, and the other
source keys from `secrets/<scope>/<field>` (e.g. `secrets/fred/eia_api_key`).

### Config file

Copy the template and edit it (the real file is git-ignored so your key never
gets committed):

```bash
cp config/config.example.yaml config/config.yaml
# edit config/config.yaml — set fred_api_key and any HTTP knobs
PYTHONPATH=src python -m fred_pipeline run --local   # auto-reads config/config.yaml
```

`config/config.yaml` is picked up automatically. Point elsewhere with
`--config path/to/file.yaml` or `FRED_CONFIG_FILE=...`. It supports a flat
mapping or a `default:` block with per-environment overrides:

```yaml
default:
  fred_api_key: ""            # prefer env var / secret scope for real keys
  rate_limit_per_minute: 120
  max_retries: 5
  secret_scope: fred          # Databricks secret scope name
  secret_key: api_key
environments:
  prod:
    rate_limit_per_minute: 60 # merged on top of default for --env prod
```

### Where each setting can come from

| Setting | Config key | Env var | CLI |
|---|---|---|---|
| FRED API key | `fred_api_key` | `FRED_API_KEY` | `--fred-api-key`* / secret scope |
| BLS API key (optional) | `bls_api_key` | `BLS_API_KEY` | keyless works at a lower quota |
| EIA API key | `eia_api_key` | `EIA_API_KEY` | required to activate `source: eia` series |
| ECB API key | — | — | keyless |
| ECB API base URL | `ecb_base_url` | `ECB_BASE_URL` | optional proxy/mirror override; default is official ECB endpoint |
| BEA API key | `bea_api_key` | `BEA_API_KEY` | required to activate `source: bea` series |
| Census API key (optional) | `census_api_key` | `CENSUS_API_KEY` | keyless works at a lower quota |
| SEC User-Agent | `sec_user_agent` | `SEC_USER_AGENT` | set to your contact; SEC 403s without a descriptive UA |
| SEC income duration | — | `SEC_PERIOD` | `quarterly` (default) or `annual` — target duration for income-statement facts |
| API base URL | `fred_base_url` | `FRED_BASE_URL` | |
| Request timeout | `request_timeout_seconds` | `FRED_REQUEST_TIMEOUT_SECONDS` | |
| Max retries | `max_retries` | `FRED_MAX_RETRIES` | |
| Rate limit / min | `rate_limit_per_minute` | `FRED_RATE_LIMIT_PER_MINUTE` | |
| Secret scope / key | `secret_scope` / `secret_key` | `FRED_SECRET_SCOPE` / `FRED_SECRET_KEY` | |
| Raw archive volume | `raw_volume_path` | `FRED_RAW_VOLUME_PATH` | |
| Target catalog | — | — | `--env {dev,test,prod}` → `macro_{env}` |
| Series universe | — | — | `--manifests DIR` (`manifests/*.yml`) |
| Config file path | — | `FRED_CONFIG_FILE` | `--config FILE` |

*The API key is passed programmatically (`PipelineConfig.resolve(fred_api_key=…)`);
on the CLI, use the config file, `FRED_API_KEY`, or a Databricks secret scope.

## Data sources

A series' `source:` selects its upstream API and its client; every source lands
in the same tables, tagged by `source` in the natural key. **Thirteen source
clients are wired**, twelve of which have active series today.

Counts below are the currently-active series per source
(`active: true` in `manifests/*.yml`) — **2,840 active of 2,941 declared**.

| Source | `source:` | API key | Active series | Manifests |
|---|---|---|---|---|
| FRED | `fred` | required | **2,570** | the domain manifests |
| Tiingo | `tiingo` | **required** | 85 | `equity_tiingo.yml` |
| BLS | `bls` | optional (keyless) | 60 | `bls_cpi_basket.yml`, `bls_cpi_basket_sa.yml`, `bls_labor.yml` |
| World Bank | `worldbank` | none | 37 | `worldbank_global.yml` |
| BIS | `bis` | none | 36 | `bis_policy_rates.yml` |
| BEA | `bea` | **required** | 23 | `bea_pce_items.yml`, `bea_national_accounts.yml` |
| SEC (company financials) | `sec` | none (User-Agent) | 3 | `sec_financials.yml` |
| EIA | `eia` | **required** | 2 | `eia_energy.yml` |
| US Treasury | `treasury` | none | 2 | `treasury_fiscal.yml` |
| Census | `census` | optional (keyless) | 1 | `census_indicators.yml` |
| iShares | `ishares` | none | 1 | `etf_holdings.yml` |
| ECB | `ecb` | none | 20 | `ecb_rates.yml` |
| Stooq | `stooq` | none | **0** (manifest inactive) | `equity_stooq.yml` |

SEC is the one that exercises the point-in-time machinery — each filing's `filed`
date becomes a vintage. ECB ships with 20 verified active FX and rates series.
Stooq ships inactive: its 89 entries are the price-return counterpart to the
Tiingo total-return series, activated when you want the cross-source price
reconciliation (`gold.equity_price_reconciliation`).

Before activating anything for redistribution, check
[`config/data_licensing.yml`](config/data_licensing.yml) — Tiingo, Stooq, and
iShares are all non-redistributable, and `validate --commercial` gates on it.
To add a source, see [`docs/instructions/adding_a_source.md`](docs/instructions/adding_a_source.md);
for keys, egress, and activation decisions see the register in
[`docs/deployment/deployment_runbook.md`](docs/deployment/deployment_runbook.md).

## The series universe

The FRED universe spans **2,570 active series** across the domain manifests
(rates, inflation, labor, growth, money/banking, prices, production/housing,
international, national accounts, regional/state). It grew from the handoff's
27-series seed via **API-driven discovery** (below). It is a deliberate,
reviewed set — not all of FRED (~800k series). Grow it three ways:

> **Ingestion and presentation are separate layers.** A manifest entry decides
> what gets *pulled*; [`config/series_catalog.yml`](config/series_catalog.yml)
> decides what gets *presentation semantics* (`econ_category`, `polarity`,
> `default_transform`, `geo`). The catalog currently covers **290** of the
> active series — those are the ones `gold.dim_series`,
> `gold.macro_indicator_dashboard`, and `gold.macro_category_summary` are built
> from. Everything else is still fully queryable via
> `gold.fred_latest_observation` / `gold.fred_feature_transforms` /
> `gold.zscore_heatmap`, just without a category or polarity.

### 1. Add a series by hand

Add an entry to the appropriate manifest under `manifests/` (fields validated
against `manifests/manifest.schema.json`), open a PR, and the next run picks it
up — including syncing its metadata into `meta.fred_series`.

```yaml
- series_id: DGS10
  title: 10-Year Treasury Constant Maturity Rate
  category: rates
  frequency: d
  units: Percent
  # vintage_enabled defaults to true (point-in-time safe). Set false only for
  # provably non-revised market/price series if you want a leaner pull.
  validation_profile: standard
  downstream_use_case: yield_curve
  priority: 1
  tags: [rates, curve, treasury]
```

**Revision-sensitivity is on by default.** `vintage_enabled` defaults to `true`,
so every series captures its full point-in-time (ALFRED) history unless you
opt out. This is the leakage-safe default for backtests: you can always collapse
vintages to "latest revised" (the `gold.v_latest_revised` view), but you cannot
recover vintages a run never captured. For never-revised market series (yields,
SOFR, breakevens) it's a cheap no-op — one vintage per date.

### 2. Add a series from another source

Series aren't limited to FRED. A manifest entry can set `source:` to `bls`,
`eia`, `ecb`, `treasury`, `worldbank`, `bea`, `census`, or `sec` and it flows
through the same Bronze/Silver/Gold path — each row is tagged with its `source`
in the natural key. ECB, Treasury, World Bank, Census, and SEC are keyless
(SEC needs a descriptive User-Agent); EIA and BEA require a key. **SEC** brings
company financials (fundamentals from EDGAR XBRL) in as point-in-time series.
`ecb_rates.yml` ships active with a verified starter exchange-rate series. See
the inactive demos under `manifests/` (`bls_labor.yml`, `bls_cpi_basket.yml` —
the full CPI-U item hierarchy, more complete than FRED's partial mirror —
`eia_energy.yml`, `treasury_fiscal.yml`, `worldbank_global.yml`,
`bea_national_accounts.yml`, `census_indicators.yml`, `sec_financials.yml`), and
[`docs/instructions/adding_a_source.md`](docs/instructions/adding_a_source.md) for how to add a new source
(one client module + one registry entry).

## Metadata governance (drift + lifecycle + all-source staleness)

Manifests declare *intent*; FRED is the source of *truth*, and they drift over
time. `reconcile` runs two independent checks:

- **FRED-only drift + lifecycle** — fetches FRED's `/series` metadata for each
  FRED-sourced series and reports `frequency_mismatch` (error), `discontinued`
  (warning), `units_changed` (info), `not_found` (error), plus a lifecycle
  snapshot (observation range, `last_updated`, popularity, staleness vs. FRED's
  own reported latest date), appended to `meta.fred_series_lifecycle`/
  `meta.fred_series_drift`. FRED-only because it diffs against FRED's own
  metadata catalog — a Tiingo ticker or BLS series id isn't in it.
- **All-source staleness** — compares every series' manifest cadence against
  the latest observation *this pipeline has already ingested* into Silver, no
  live upstream API call required. Because it only reads already-ingested
  data, it covers every source (FRED, Tiingo, BLS, EIA, ...), not just FRED.
  Appended to `meta.series_staleness`, one row per `(source, series_id)`.

```bash
# report only
PYTHONPATH=src python -m fred_pipeline reconcile --no-persist

# persist lifecycle + drift + all-source staleness to a local SQLite file
PYTHONPATH=src python -m fred_pipeline reconcile --local --db-path fred_local.db

# gate CI: exit non-zero on any error-level FRED drift
PYTHONPATH=src python -m fred_pipeline reconcile --fail-on-drift
```

Findings land in `meta.fred_series_drift`, `meta.fred_series_lifecycle`, and
`meta.series_staleness` (Delta or SQLite). Use `--fail-on-drift` in CI to catch
a FRED series changing frequency or being discontinued before it silently
corrupts downstream features; query `meta.series_staleness` to see which
series across *any* source need a refresh.

## Incremental loads (full-on-first-run, then restate last N)

Each run decides a load window per series against whatever backend it's writing
to (Delta or local SQLite):

- **Series has no data yet → full history.** The first ever load pulls the
  complete series (and, for `vintage_enabled`, its full vintage history).
- **Series already loaded → restate the last N observations.** Subsequent runs
  re-pull only the most recent `N` observation dates (`observation_start` set to
  the N-th most recent) and `MERGE` them, so **revisions to recent points are
  restated and new points are inserted** — idempotently, no duplicates.
- **`load_type: full`** on a series forces a full re-pull every run.

`N` is `restate_last_n` (default **90**, set via config/env/CLI), overridable
per series in a manifest with `restate_records:` — tune it higher for series
with deep benchmark revisions (GDP, payrolls) and lower for high-frequency
series. The effective strategy per series is recorded in
`audit.etl_series_run.load_type` (`full` or `restate_last_<n>`).

```yaml
# manifest entry: restate the last 24 observations for a heavily-revised series
- series_id: GDPC1
  title: Real Gross Domestic Product
  category: growth
  frequency: q
  load_type: incremental
  restate_records: 24        # ~6 years of quarters, to catch annual revisions
```

> Because "restate last N" only re-pulls recent observations, revisions to
> points *older* than the window won't be re-captured until a `full` run. Size
> `restate_records` to each series' revision behavior, or schedule a periodic
> full refresh for the deeply-revised ones.

### 3. Discover series from the FRED API

Generate a whole manifest from a FRED **category**, **release**, or **search**
instead of hand-listing ids. The generator maps FRED metadata → validated specs
(popularity → priority), drops `DISCONTINUED` series, dedupes against your
existing manifests, and writes YAML that's guaranteed to load.

```bash
# Preview series in FRED category 22 (Treasury constant maturities), no write:
PYTHONPATH=src python -m fred_pipeline discover --name rates_extra \
    --category-id 22 --frequencies d --dry-run

# Write a manifest from a release, keeping only popular monthly/quarterly series:
PYTHONPATH=src python -m fred_pipeline discover --name jolts \
    --release-id 192 --frequencies m,q --min-popularity 20 \
    --out manifests/jolts.yml

# Or from a search:
PYTHONPATH=src python -m fred_pipeline discover --name inflation_breakevens \
    --search "breakeven inflation" --max 25 --out manifests/breakevens.yml
```

Useful flags: `--max N`, `--min-popularity 0-100`, `--frequencies d,w,m,q`,
`--include-discontinued`, `--include-existing` (skip dedupe), `--dry-run`.
Find category/release ids on the FRED website (the id is in the page URL) or via
the API. Review the generated YAML, set `vintage_enabled` / `validation_profile`
where it matters, and commit it like any other manifest.

### 4. Discover ECB dataflows

ECB discovery is keyless. The first helper lists SDMX dataflows so you can pick
which ECB domains to inspect before generating candidate manifests.

```bash
PYTHONPATH=src python -m fred_pipeline discover-ecb --list-flows --search exchange
PYTHONPATH=src python -m fred_pipeline discover-ecb --list-flows --search rates --json
```

Follow `specs/spec002` for the planned next slices: flow structure inspection,
bounded key expansion, and inactive candidate manifest generation.

## Open decisions (before non-FRED go-live)

The code is complete; what's left is provisioning + domain calls, tracked as a
checkboxed decision register in
[`docs/deployment/deployment_runbook.md`](docs/deployment/deployment_runbook.md). In short:

- **Which sources/series to activate** — the remaining non-FRED demos are inactive;
  turn on what you want (and, for SEC at scale, generate the manifest with
  `fred_pipeline.sources.sec.build_sec_manifest`).
- **Keys & secrets** — provision EIA/BEA keys and (optional) BLS/Census keys in
  the secret scope; set `SEC_USER_AGENT`. ECB is keyless.
- **Egress** — allow the source hosts the active sources need.
- **Per-series data policy** — `vintage_enabled`, `validation_profile`, value
  bounds, `restate_records`; plus any new `config/spreads.yml` pairs.
- **Verify remaining demo IDs live** — aside from the active ECB starter, the
  demo series IDs (and Census predicate codes) were built to the documented API
  shapes but not verified against the live APIs.
- **Known follow-ons** — SEC statement standardization (canonical tags + duration
  disambiguation); per-source **drift** reconciliation against each source's own
  metadata catalog (today's `reconcile` diffs FRED-only against FRED's `/series`
  metadata — staleness, unlike drift, already covers every source since it's
  computed from ingested data rather than a live per-source API call).

## Status

Implemented and tested (**984 non-Spark tests + a Spark/Delta integration suite in
CI**, green on the latest commit). Highlights: **thirteen pluggable sources**
(twelve with active series; Stooq ships inactive) with
`source` in the natural key and source-aware Bronze lineage + replay;
**API-driven FRED discovery**; **metadata governance** (drift + lifecycle vs.
live FRED); **incremental loads** (full-on-first-run, then restate last N);
**replay-from-Bronze** rebuild; **stage tracking + run-summary email alerting** (Outlook/Microsoft 365 SMTP, Microsoft Graph, or a Slack-compatible webhook — reports *which phase* failed, so a run whose Gold rebuild broke is no longer reported as a success); richer **data quality**
(freshness + value bounds); **quant Gold features** (MoM/YoY/diff/z-score, curve
spreads, **frequency-aware N-leg cross-series features** — `config/cross_series.yml`,
as-of alignment for cross-source/cross-frequency spreads/ratios/composites,
plus a **point-in-time (`realtime_start`-aligned) leak-free variant** for
backtests, as-of-date point-in-time snapshots); **SEC company financials**
(XBRL tags standardized to canonical statements + derived ratios +
cross-company ranks); **governance Gold** (multi-source
coverage/freshness view + config-driven cross-source reconciliation with a
divergence flag); a pluggable storage backend
(**Databricks/Delta or local SQLite**); layered configuration (**YAML file / env
vars / args / secret scope**); Unity Catalog DDL; the audit framework; a
**GitHub Actions CI**; and the Databricks Asset Bundle (main + per-source jobs).
