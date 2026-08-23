# Power BI Report Suite — Specification & Engineering Handoff

**Status:** approved scope, ready to build
**Owner:** BI / report authoring
**Upstream:** `fred-bronze-to-gold-pipeline` Gold layer (Databricks Unity Catalog `macro_{env}` / local SQLite mirror)
**Build layer:** [`powerbi_report_build_plan.md`](powerbi_report_build_plan.md) —
the source query and model specification for every table in every report, plus a
verified per-report readiness verdict. Read this document for *what* to build
and that one for *how*.

**Companion docs:** [`docs/dictionary/data_dictionary.md`](../dictionary/data_dictionary.md) ·
[`docs/handoffs/completed/market_terminal_gold_views.md`](completed/market_terminal_gold_views.md) ·
[`docs/deployment/architecture.md`](../deployment/architecture.md) ·
[`docs/validation/validation.md`](../validation/validation.md)

---

## 0. How to read this document

This is a **build spec**, not a design exploration. Sections 1–4 are binding
conventions every report must follow. Section 5 is fourteen self-contained
report specifications — a report author can build any one of them from its
section alone, without reading the other thirteen. Sections 6–9 cover delivery,
governance, and the prerequisites that block specific reports.

Every table and column named here exists in the pipeline today unless it appears
in **§8 Prerequisites & gaps**, which lists the handful of things that must be
built or activated before a specific report can be populated.

The pipeline ships a machine-readable manifest of its own Gold objects at
`gold.powerbi_catalog` — 54 rows, including the `PIT` and `GOV` modules that
Reports 13 and 14 bind to (source of truth:
`fred_pipeline.writer.global_views.POWERBI_CATALOG`). Appendix A reproduces it.
**When this document and `gold.powerbi_catalog` disagree, the table wins** — it
is generated from the code that builds the data.

Two tests keep it honest: `test_powerbi_catalog_covers_gold_tables` fails when a
Gold table *or view* has no catalog row, and
`test_powerbi_catalog_has_no_phantom_entries` fails when the catalog lists an
object that no longer exists.

---

## 1. Decisions register

These were settled before authoring began. Each has a consequence the build must
honour.

| # | Decision | Consequence for the build |
|---|---|---|
| D1 | **Model-per-report.** Fourteen independent PBIP projects, each with its own semantic model. | No shared dataset bottleneck; each report refreshes and deploys independently. **Cost:** `dim_date`, `dim_series`, `market_calendar` and the core measure set are duplicated fourteen times. §4.1 defines a *model kernel* — a versioned set of shared M queries and DAX measures copied into every model — to keep the duplication mechanical rather than divergent. |
| D2 | **Fourteen topics** (§5). SEC Company Fundamentals is explicitly **out of scope** for v1 (`manifests/sec_financials.yml` carries three demo companies). | `gold.fred_company_fundamentals`, `gold.fred_company_ratios`, `gold.v_company_ratio_ranks` remain unbound. Revisit when the SEC manifest is generated at scale. |
| D3 | **Three audiences in one suite:** PM/CIO executive, quant researcher, and external/client-facing. | Every report uses the same three-tier page structure (§4.4): page 1 executive, pages 2–n analyst, hidden drillthrough pages. External distribution is gated per-report by source licensing (§4.6). |
| D4 | **Dual-bound connectivity:** local SQLite mirror for development, Databricks SQL Warehouse for production. | Every model uses the parameterised connection pattern in §3.3. A report author builds against `fred_local.db` and repoints to Unity Catalog with a parameter change and no query rewrites. Table naming differs between the two backends — §3.2. |

---

## 2. What the Gold layer already does for you

The single most important thing for a report author to internalise:

> **The analytics are already computed. Z-scores, percentiles, contributions,
> streaks, inversion runs, breadth, stress scores, regime names, rolling stats,
> and model outputs are all stored columns — not measures you need to write.**

Consequences:

- **DAX stays thin.** Most measures are `SELECTEDVALUE`, `LASTNONBLANK`, or a
  plain `AVERAGE` over a pre-computed column. Do not re-derive a z-score in DAX;
  the Gold column is point-in-time safe and the DAX one would not be.
- **Point-in-time safety is a property of the data, not the report.** Expanding
  and rolling statistics in Gold use trailing-only windows
  (`UNBOUNDED PRECEDING .. CURRENT ROW`). A DAX measure that computes a
  statistic over `ALL(dim_date)` silently leaks future information into
  historical rows and must not be used for anything a researcher will act on.
- **Long/tidy facts are the norm.** One visual serves many series through a
  slicer. Resist the temptation to pivot into wide tables in Power Query.
- **Recession shading is a column.** `dim_date[is_recession]`,
  `treasury_curve_metrics[is_recession]`, `curve_spread_daily[is_recession]`,
  `credit_spread_daily[is_recession]`. It is a nullable boolean where **NULL
  means "unknown / `USREC` not yet ingested"** — never render NULL as "no
  recession".

---

## 3. Connectivity and data binding

### 3.1 Environments

| Environment | Backend | Location | Use |
|---|---|---|---|
| Dev | SQLite | `fred_local.db` (from `python -m fred_pipeline run --local --db-path fred_local.db`) | Report authoring, visual iteration, DAX development |
| Test | Databricks SQL Warehouse | catalog `macro_test` | Integration validation before promotion |
| Prod | Databricks SQL Warehouse | catalog `macro_prod` | Published reports |

### 3.2 Table naming differs between backends

This is the one real friction point of D4 and must be handled in the parameter
layer, not by hand-editing queries.

| Object | Databricks (prod) | SQLite mirror (dev) |
|---|---|---|
| Gold fact | `macro_prod.gold.macro_indicator_dashboard` | `gold_macro_indicator_dashboard` |
| Gold view | `macro_prod.gold.v_source_coverage` | `gold_v_source_coverage` |
| Meta | `macro_prod.meta.series_staleness` | `meta_series_staleness` |
| Audit | `macro_prod.audit.etl_run` | `audit_etl_run` |
| Silver | `macro_prod.silver.fred_observation` | `silver_fred_observation` |

The SQLite mirror flattens `{schema}.{table}` to `{schema}_{table}` in a single
file (see `src/fred_pipeline/io/local_store.py`).

### 3.3 The parameterised connection pattern (required)

Every model declares these parameters:

| Parameter | Type | Dev value | Prod value |
|---|---|---|---|
| `p_Backend` | Text | `"SQLite"` | `"Databricks"` |
| `p_SqlitePath` | Text | `C:\data\fred_local.db` | *(unused)* |
| `p_DbxHost` | Text | *(unused)* | `adb-xxxx.azuredatabricks.net` |
| `p_DbxHttpPath` | Text | *(unused)* | `/sql/1.0/warehouses/xxxx` |
| `p_Catalog` | Text | *(unused)* | `macro_prod` |

And a single shared function `fnGold`, copied verbatim into every model:

```m
// fnGold: resolve one Gold-layer object across both backends.
// Usage:  fnGold("gold", "macro_indicator_dashboard")
(schema as text, table as text) as table =>
let
    Source =
        if p_Backend = "SQLite" then
            let
                Db   = Odbc.DataSource("driver={SQLite3 ODBC Driver};database=" & p_SqlitePath),
                Tbl  = Db{[Name = schema & "_" & table]}[Data]
            in  Tbl
        else
            let
                Db   = Databricks.Catalogs(p_DbxHost, p_DbxHttpPath, [Catalog = p_Catalog, Database = schema]),
                Tbl  = Db{[Name = table, Kind = "Table"]}[Data]
            in  Tbl
in
    Source
```

Rules:

- **No hard-coded table references anywhere else in the model.** Every query
  starts `= fnGold("gold", "<table>")`.
- Repointing dev → prod is a parameter edit in the Power BI Service dataset
  settings or a deployment-pipeline rule. No query changes, no refactor.
- SQLite is **dev only**. It is not a supported production source and must never
  be the binding of a published report.

### 3.4 Storage mode

**Import mode for every report in v1.** Rationale: the Gold layer is
pre-aggregated and the largest objects in scope are comfortably importable
(§6.1). DirectQuery is a later optimisation for the two deep-history reports
(#9 Statistical Lab, #13 PIT & Revisions Lab) if their models outgrow the
refresh window — flagged in each spec.

### 3.5 Refresh

- **Schedule:** one refresh per weekday, ~90 minutes after the pipeline's daily
  `run` completes. Confirm the pipeline's actual completion time from
  `audit.etl_run.ended_at` before fixing the schedule.
- **Failure handling:** a report whose refresh fails must not silently serve
  stale data to an executive audience. Every report's executive page carries the
  freshness banner defined in §4.5.
- **Incremental refresh:** only for the deep-history facts listed in §6.2.
  Everything else is a full refresh; the tables are small enough that the added
  complexity is not worth it.

---

## 4. Shared conventions (the model kernel)

Because of D1, these are **copied into every model**. Keep them in a versioned
folder (`/powerbi/_kernel/`) and treat drift between a report's copy and the
kernel as a defect.

### 4.1 Kernel contents

```
/powerbi/_kernel/
├── m/
│   ├── fnGold.m                 # §3.3
│   ├── qDimDate.m               # gold.dim_date, typed + marked as date table
│   ├── qDimSeries.m             # gold.dim_series
│   ├── qMarketCalendar.m        # gold.market_calendar
│   └── qPowerBiCatalog.m        # gold.powerbi_catalog (documentation page)
├── dax/
│   ├── core_measures.dax        # §4.3
│   └── formatting_rules.dax     # conditional-formatting helper measures
├── theme/
│   └── macro_theme.json         # §4.7
└── KERNEL_VERSION               # semver; stamped into each model's About page
```

### 4.2 Star schema

Every model is a star. Facts join to dimensions; facts never join to facts.

```
                 ┌──────────────┐
                 │  dim_date    │  (marked as date table on [date])
                 └──────┬───────┘
                        │ 1:*  dim_date[date] → fact[observation_date]
                        │
   ┌──────────────┐     ▼      ┌──────────────────┐
   │ dim_series   │──1:*──▶ [ FACT TABLE(S) ]     │
   └──────────────┘            └──────────────────┘
      dim_series[series_id] → fact[series_id]
                        ▲
                        │ 1:*  (report-local, filtered to one calendar_name)
                 ┌──────┴───────┐
                 │market_calendar│
                 └──────────────┘
```

Rules:

- `dim_date` is **marked as the date table** on `[date]` in every model. Without
  this, DAX time intelligence silently misbehaves.
- All relationships are **single-direction** (dimension filters fact). Enable
  bi-directional filtering only where a spec explicitly calls for it, and record
  why.
- `market_calendar` is long/tidy across three calendars (`NYSE`, `SIFMA`,
  `FEDWIRE`). A model that joins it must filter to exactly one `calendar_name`
  in Power Query, or the join fans out 3×. Default to **NYSE** for equity
  reports and **FEDWIRE** for rates/funding settlement math (FEDWIRE does not
  observe Good Friday, which is why it is the correct calendar for T+1/T+2
  against a Fed-cleared instrument).
- `dim_series` covers the **279 curated series** in `config/series_catalog.yml`,
  not the 2,829 active series in the manifests. These are two independent
  layers: the manifests decide what gets *ingested*, `series_catalog.yml`
  decides what gets *presentation semantics*. Reports that need the wider
  universe bind to `gold.fred_latest_observation` /
  `gold.fred_feature_transforms` and accept a thinner dimension. See §8-G1.
- `dim_series[geo]` carries a USPS state code (`"CA"`) or census-region name
  (`"Midwest"`) for `econ_category = 'REGIONAL'` rows, and is blank for
  national series. Use it as the map key in Report 12 — never parse the title.
- `dim_series[metric]` names the measure where one `econ_category` holds several
  incompatible ones: REGIONAL spans `Unemployment Rate`, `Coincident Activity`,
  and `House Price Index`. Slice on it rather than deriving the split from the
  series-id suffix, and never put two of them on one axis.

### 4.3 Core measure library

Copied into every model. Names are stable across the suite — an executive
reading two reports sees the same measure mean the same thing.

```dax
-- Latest observation of the selected series, respecting all filters.
Latest Value =
VAR _d = MAX ( 'fact'[observation_date] )
RETURN CALCULATE ( SELECTEDVALUE ( 'fact'[value] ), 'fact'[observation_date] = _d )

-- As-of date actually shown (for the freshness banner).
As Of Date = MAX ( 'fact'[observation_date] )

-- Data age in days vs today. Drives the staleness badge.
Staleness Days = DATEDIFF ( [As Of Date], TODAY (), DAY )

-- Freshness verdict. Thresholds are deliberately generous for monthly data;
-- override per report where the cadence is daily.
Freshness Badge =
SWITCH (
    TRUE (),
    ISBLANK ( [As Of Date] ), "⚫ No data",
    [Staleness Days] <= 3,    "🟢 Current",
    [Staleness Days] <= 10,   "🟡 Aging",
    "🔴 Stale"
)

-- Pre-computed z-score passthrough. NEVER re-derive this in DAX.
Z Score = SELECTEDVALUE ( 'fact'[zscore] )

-- Percentile rendered 0–100 for display (Gold stores 0–1).
Percentile Pct = SELECTEDVALUE ( 'fact'[percentile] ) * 100

-- Polarity-aware direction: is the latest move good or bad for the economy?
-- polarity: +1 a rise is bullish, -1 bearish, 0 neutral (dim_series).
Direction Verdict =
VAR _chg = SELECTEDVALUE ( 'fact'[change_abs] )
VAR _pol = SELECTEDVALUE ( 'dim_series'[polarity] )
RETURN
    SWITCH (
        TRUE (),
        _pol = 0,            "Neutral",
        _chg * _pol > 0,     "Improving",
        _chg * _pol < 0,     "Deteriorating",
        "Unchanged"
    )

-- Conditional-formatting colour for the above. Colour-blind safe pair (§4.7).
Direction Colour =
SWITCH ( [Direction Verdict],
    "Improving",     "#1B7F79",
    "Deteriorating", "#B4441F",
    "#6B7280"
)

-- Recession shading. TRUE/FALSE/BLANK are three distinct states; BLANK means
-- USREC has not been ingested and must not be rendered as "no recession".
Recession Shade =
VAR _r = SELECTEDVALUE ( 'dim_date'[is_recession] )
RETURN IF ( ISBLANK ( _r ), BLANK (), IF ( _r, "#E5E7EB", BLANK () ) )
```

### 4.4 Page structure (all fourteen reports)

| Page | Audience | Content |
|---|---|---|
| **1. Overview** | PM / CIO | ≤ 8 visuals. Headline KPI cards, one hero chart, the plain-language verdict (regime name, stress bucket, trend word). Freshness banner top-right. No jargon in visual titles. |
| **2–n. Analysis** | Quant researcher | Dense. Full slicer set, z-scores, percentiles, windows, model diagnostics, statistical significance flags. |
| **Hidden. Drillthrough** | Both | Single-entity deep dive reached by right-click from any page. Always includes the entity's full history, provenance (`source`, `realtime_start`), and staleness. |
| **Hidden. About** | All | Data lineage, refresh time, kernel version, source licensing notice (§4.6), link to `gold.powerbi_catalog` documentation page. |

Every report has an **About** page. It is the single place an external recipient
can see where the numbers came from.

### 4.5 The freshness banner (required, top-right of every Overview page)

A card visual bound to `[Freshness Badge]` and `[As Of Date]`, formatted:

```
Data as of 2026-08-11  🟢 Current
```

For reports with mixed-cadence sources (#1 Macro Cockpit, #10 Global Macro),
show the **worst** source's verdict — "worst-source aggregation", matching the
pipeline's own provenance convention.

### 4.6 Licensing and external distribution (binding)

`config/data_licensing.yml` is the register. Its current state:

| Source | License | Redistribution | Commercial | Attribution |
|---|---|---|---|---|
| fred, bls, treasury, census, bea, sec, eia | public-domain | ✅ | ✅ | not required |
| worldbank | open-data (CC BY 4.0) | ✅ | ✅ | **required** |
| **bis** | **attribution-noncommercial** | ✅ non-commercial only | ❌ | **required** |
| tiingo | free-tier personal-use | ❌ | ❌ | — |
| stooq | requires-agreement | ❌ | ❌ | — |
| ishares | requires-agreement | ❌ | ❌ | — |

This produces two distribution tiers:

- **Tier A — externally distributable.** Reports sourced entirely from
  public-domain and CC BY sources. Reports #1–#9, #12, #13, #14. Where World Bank
  data appears (#10), the About page **must** carry the CC BY 4.0 attribution.
- **Tier B — internal only.** Any report touching Tiingo, Stooq, or iShares
  data: **#11 Equity & Factor Analytics**. This report must be published to an
  internal-only workspace, must not be shared to external Entra guests, and must
  not use publish-to-web. The restriction is on *redistribution of the data*, not
  on internal analysis.
- **#10 Global Macro Monitor is Tier A for non-commercial distribution only.**
  `gold.global_policy_rates` is largely BIS-sourced, and the BIS permits
  redistribution without written permission **for non-commercial purposes**
  while retaining copyright; commercial use needs BIS permission. Attribution to
  the BIS is a copyright condition, so the About page must carry it alongside
  the World Bank CC BY notice. If this report ever becomes part of a
  commercially distributed product, clear it with the BIS first.

Enforcement is workspace placement plus a sensitivity label, not report logic.
`python -m fred_pipeline validate --commercial` is the upstream guardrail that
fails a build if an active source does not clear commercial use.

### 4.7 Visual design standard

- **Theme file:** `macro_theme.json` in the kernel. One theme across all
  fourteen so the suite reads as one product.
- **Sequential/diverging palettes:** diverging for z-scores and changes
  (centred on zero), sequential for levels and percentiles. Never a rainbow.
- **The red/green pair must be colour-blind safe.** Use the teal/rust pair in
  `[Direction Colour]` above, not pure `#FF0000`/`#00FF00`. Always pair colour
  with a second channel — an arrow glyph, a sign, or position.
- **Units are explicit in every axis and card title.** The Gold layer mixes
  percent, percentage points, basis points, index levels, and decimal fractions
  in adjacent tables. Ambiguity here produces wrong decisions. Specific traps:
  - `curve_spread_daily` carries **both** `value` (percent) and `value_bps`.
  - `credit_spread_daily` carries **both** `oas_pct` and `oas_bps`; the rolling
    companion `credit_spread_rolling` is **bps only**.
  - `inflation_explorer[mom_pct]` / `[yoy_pct]` are **fractions** (0.003 = 0.3%),
    while `contribution_pp` is in **percentage points**.
  - `inflation_forecast[forecast_value]` and its CI bounds are **MoM decimal
    fractions**.
  - `treasury_curve_rolling[change]` is a **percentage-point** move (×100 for bps).
- **Sparklines** come from `gold.macro_indicator_sparkline` (last 36 points,
  `point_index` 0 = oldest), not from a truncated fact table.
- **Accessibility:** all visuals have alt text; tab order set on every page; no
  information conveyed by colour alone.

### 4.8 Source control

Reports are authored as **PBIP (Power BI Project)** format and committed to
`/powerbi/<report_folder>/`. This makes semantic-model changes reviewable in a
PR diff. `.pbix` binaries are not committed.

---

## 5. The fourteen reports

Each spec is self-contained. Column names are the contract — they match
`sql/50_gold.sql`.

---

### Report 1 — Macro Cockpit (ECON)

**Purpose.** The daily "what is the economy doing" board. Every curated
indicator at its latest print, grouped by category, with breadth and a forward
calendar of what prints next.

**Audience.** Executive-first. This is the report a CIO opens at 7am.
**Tier.** A (externally distributable).

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.macro_indicator_dashboard` | 1 / series (latest) | primary fact |
| `gold.macro_indicator_sparkline` | 1 / series × point (36) | sparkline fact |
| `gold.macro_category_summary` | 1 / category | breadth fact |
| `gold.release_calendar` | 1 / release × date | forward calendar |
| `gold.fred_feature_transforms` | 1 / series × date | drillthrough history |
| `gold.dim_series`, `gold.dim_date` | — | dimensions |

**Model notes.** The catalog now carries 254 series, 120 of which are
`econ_category = 'REGIONAL'` (the state panel, Report 12). **Filter
`econ_category <> 'REGIONAL'` in this report's Power Query**, or the national
board is swamped by 50 states and the breadth visual double-counts labour-market
signal. Report 12 owns the REGIONAL rows.

`macro_indicator_dashboard` is a *snapshot* fact — one row per
series, not a time series. Join it to `dim_series` on `series_id`; do **not**
join it to `dim_date` (its `latest_date` varies per series and a date filter
would blank the board). Join `fred_feature_transforms` to `dim_date` for the
drillthrough history instead.

**Pages**

1. **Overview.** Category breadth bar (`macro_category_summary[breadth_pct]`,
   REGIONAL row excluded),
   surprise index cards (`surprise_index`), 8 headline KPI cards (UNRATE, PAYEMS,
   CPIAUCSL, GDPC1, DGS10, NFCI, INDPRO, RSAFS) each with sparkline and
   direction arrow, next-7-days release list.
2. **Indicator board.** Full table of the 134 national catalog series: latest, prior,
   change, YoY, z-score, percentile, surprise, staleness. Conditional formatting
   on `direction_is_good` (pre-computed — use it rather than recomputing
   polarity in DAX) and on `staleness_days`.
3. **Category detail.** Small multiples by `econ_category` — the ten national
   buckets (ACTIVITY 8, CONSUMER 8, CREDIT 15, FX 15, GROWTH 9, HOUSING 10,
   INFLATION 17, LABOR 21, MONEY 9, RATES 22).
4. **Release calendar.** Agenda view over `release_calendar`, filtered
   `is_future = TRUE`, coloured by `importance`. Note this table is
   **intentionally not point-in-time** — it is a re-fetched schedule;
   `fetched_at` stamps its staleness.
5. *(hidden)* **Series drillthrough** on `series_id`.

**Key measures**

```dax
Breadth % = SELECTEDVALUE ( 'macro_category_summary'[breadth_pct] )

Surprise Index = SELECTEDVALUE ( 'macro_category_summary'[surprise_index] )

-- The surprise proxy is (latest − trailing-window mean); there is no consensus
-- forecast in this pipeline. Label it "vs trend", never "vs consensus".
Surprise vs Trend = SELECTEDVALUE ( 'macro_indicator_dashboard'[surprise_z] )

Improving Count = SUM ( 'macro_category_summary'[n_improving] )

Next Release =
CALCULATE (
    MIN ( 'release_calendar'[release_date] ),
    'release_calendar'[is_future] = TRUE ()
)
```

**Acceptance criteria**
- Board shows all 134 national catalog series; none blank; no REGIONAL rows.
- `direction_is_good` formatting matches `dim_series[polarity]` sign convention
  on a hand-checked sample of 5 series (must include one `polarity = -1` series
  such as UNRATE and one `polarity = 0` such as DTWEXBGS).
- Release calendar shows only future dates on the calendar page.
- Freshness banner reports the **worst** staleness across the board.

---

### Report 2 — Inflation Explorer (INFL)

**Purpose.** Decompose headline CPI/PCE to item level: what is driving the
print, by how much, and where it is heading.

**Audience.** Analyst-first, with an executive summary page.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.inflation_explorer` | 1 / item × month | primary fact |
| `gold.inflation_contribution` | 1 / item × month | waterfall fact |
| `gold.inflation_forecast` | 1 / series × horizon × model | forecast fact |
| `gold.global_inflation` | 1 / country × print | optional context strip |
| `gold.dim_date` | — | dimension |

**Model notes.** `inflation_explorer` carries a self-referencing hierarchy via
`parent_item` / `hierarchy_level`. Build a Power BI **decomposition tree** on it
rather than flattening. `basket` (CPI/PCE) and `sa_nsa` are the two top-level
slicers and must be **single-select with a default** — mixing SA and NSA in one
visual produces nonsense.

**Units trap.** `mom_pct` / `yoy_pct` are fractions; `contribution_pp` is
percentage points; `three_month_annualized` is an annualised rate. Format each
explicitly.

**Pages**

1. **Overview.** Headline vs core, MoM and YoY, 3m-annualised, acceleration
   verdict (`mom_accel`, `yoy_accel` sign), and the top-5 contributor list.
2. **Contribution waterfall.** `inflation_contribution` for the selected month:
   items ranked by `rank_in_month`, stacked against the `is_headline_total` bar.
   The `is_headline_total = TRUE` row is the total bar, not a contributor —
   exclude it from the item stack or it double-counts.
3. **Item tree.** Decomposition tree on `item_label` / `parent_item`, with
   `weight` and `contribution_pp` as the analysed measure.
4. **Item drill.** Line chart of any selected item's index, MoM, YoY, and
   3m-annualised over 24 months.
5. **Forecast.** Fan chart from `inflation_forecast`: `forecast_value` with the
   80% and 95% bands (`lower_80`/`upper_80`, `lower_95`/`upper_95`), split by
   `model_type` (AR vs VAR) and `horizon_months`. Label `model_vintage`
   prominently — this is a model output, not an observation.
6. *(hidden)* **Item drillthrough** on `series_id`.

**Key measures**

```dax
-- Gold stores fractions; render as percent.
MoM % = SELECTEDVALUE ( 'inflation_explorer'[mom_pct] ) * 100
YoY % = SELECTEDVALUE ( 'inflation_explorer'[yoy_pct] ) * 100

Contribution (pp) = SUM ( 'inflation_contribution'[contribution_pp] )

Headline Total (pp) =
CALCULATE (
    SUM ( 'inflation_contribution'[contribution_pp] ),
    'inflation_contribution'[is_headline_total] = TRUE ()
)

Acceleration Verdict =
VAR _a = SELECTEDVALUE ( 'inflation_explorer'[mom_accel] )
RETURN SWITCH ( TRUE (), _a > 0, "Accelerating", _a < 0, "Cooling", "Flat" )

-- Forecast values are MoM decimal fractions.
Forecast MoM % = SELECTEDVALUE ( 'inflation_forecast'[forecast_value] ) * 100
```

**Prerequisites.** The CPI item trees depend on `bls_cpi_basket.yml` /
`bls_cpi_basket_sa.yml`; the PCE sub-item tree depends on `bea_pce_items.yml`
(21 BEA NIPA 2.4.4 items) and a BEA API key. Without these the tree renders
headline-and-core only. See §8-G2.

**Acceptance criteria**
- Item contributions for a given month sum to within rounding of the
  `is_headline_total` bar.
- SA/NSA and CPI/PCE slicers are single-select and default to CPI/SA.
- Forecast bands are monotonic: `lower_95 ≤ lower_80 ≤ forecast_value ≤ upper_80 ≤ upper_95`.

---

### Report 3 — Treasury Curve Lab (CURV)

**Purpose.** The shape of the Treasury curve today, how it got here, and what
its level/slope/curvature factors are doing.

**Audience.** Analyst-first.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.treasury_curve` | 1 / as-of date × tenor | curve fact |
| `gold.treasury_curve_metrics` | 1 / as-of date | metrics fact |
| `gold.treasury_curve_rolling` | 1 / tenor × date × window | momentum fact |
| `gold.yield_curve_ns_factors` | 1 / date | Nelson-Siegel fact |
| `gold.dim_date`, `gold.market_calendar` (FEDWIRE) | — | dimensions |

**Pages**

1. **Overview.** Today's curve (line over `tenor_months`), yesterday/1m/1y ago
   overlays, the four metric cards (`level`, `slope_10y2y`, `slope_10y3m`,
   `curvature_2_5_10`), and the `curve_move` verdict rendered as a plain-language
   sentence: *"Bear flattener — yields rose, curve flattened."*
2. **Curve evolution.** Animated curve (play axis on `as_of_date`), plus a
   3D-free heatmap of `yield_pct` by tenor × date.
3. **Metrics history.** `level` / `slope` / `curvature` / `butterfly_2_10_30`
   time series with recession shading from `is_recession` and inversion flags
   (`is_inverted_10y2y`, `is_inverted_10y3m`) as markers.
4. **Rolling momentum.** `treasury_curve_rolling` with a window slicer
   (1/5/10/21/63/126/252 observations). Remember `change` is in percentage
   points.
5. **Nelson-Siegel factors.** β₀ (level) / β₁ (slope) / β₂ (curvature) three-panel
   time series, plus `fit_rmse` and `lambda_estimated` as diagnostics. **Filter
   `fit_valid = TRUE`** — invalid fits must never reach a chart.

**Key measures**

```dax
Slope 10s2s (bps) = SELECTEDVALUE ( 'treasury_curve_metrics'[slope_10y2y] ) * 100

Curve Move = SELECTEDVALUE ( 'treasury_curve_metrics'[curve_move] )

Curve Move Narrative =
SWITCH ( [Curve Move],
    "bull-steepener",  "Yields fell, curve steepened",
    "bull-flattener",  "Yields fell, curve flattened",
    "bear-steepener",  "Yields rose, curve steepened",
    "bear-flattener",  "Yields rose, curve flattened",
    "parallel-up",     "Yields rose across the curve",
    "parallel-down",   "Yields fell across the curve",
    [Curve Move]
)

NS Fit Quality =
VAR _r = SELECTEDVALUE ( 'yield_curve_ns_factors'[fit_rmse] )
RETURN IF ( SELECTEDVALUE ( 'yield_curve_ns_factors'[fit_valid] ), FORMAT ( _r, "0.000" ), "invalid fit" )
```

**Acceptance criteria**
- Curve page renders every tenor present in `config/curve.yml` with data.
- No chart displays a row where `fit_valid = FALSE`.
- Recession shading appears for known NBER periods and is absent (not white-
  filled) where `is_recession` is NULL.

---

### Report 4 — Spreads & Inversions (CURV)

**Purpose.** The nine configured curve spreads, their valuation percentiles, and
the complete history of inversion episodes cross-referenced with recessions.

**Audience.** Analyst-first, with an executive inversion-status card.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.curve_spread_daily` | 1 / spread × date | primary fact |
| `gold.spread_inversion_episode` | 1 / spread × episode | episode fact |
| `gold.curve_spread_rolling` | 1 / spread × date × window | momentum fact |
| `gold.dim_date` | — | dimension |

**Spreads configured** (`config/spreads.yml`): T10Y2Y, T10Y3M, T2Y3M, T30Y10Y,
T10Y5Y, T5Y2Y, T10Y1Y, T30Y2Y, T5Y3M.

**Pages**

1. **Overview.** Inversion status board — one card per spread showing
   `value_bps`, `is_inverted`, and `inversion_run` (consecutive inverted
   observations) — plus the count of currently-ongoing episodes.
2. **Spread history.** Selected spread over time with a zero reference line,
   recession shading, and the `percentile` band as context.
3. **Inversion episodes.** Gantt-style bands from `spread_inversion_episode`:
   `start_date` → `end_date` (NULL = ongoing, measured to `last_inverted_date`),
   with `trough_bps`, `trough_date`, `calendar_days`, and `recession_overlap`.
4. **Valuation.** Cross-spread z-score/percentile heatmap at the selected date.
5. **Rolling momentum.** Window slicer over `curve_spread_rolling`.

**Model notes.** `spread_inversion_episode` has an *episodic* grain — it does not
join cleanly to `dim_date`. Keep it unrelated to `dim_date` and drive it from the
`spread_name` slicer only, or model it with an inactive relationship activated by
`USERELATIONSHIP` in a dedicated measure. The simpler unrelated-table approach is
preferred.

**Key measures**

```dax
Spread (bps) = SELECTEDVALUE ( 'curve_spread_daily'[value_bps] )

Inversion Run = SELECTEDVALUE ( 'curve_spread_daily'[inversion_run] )

Inversion Status =
VAR _inv = SELECTEDVALUE ( 'curve_spread_daily'[is_inverted] )
VAR _run = [Inversion Run]
RETURN
    SWITCH ( TRUE (),
        ISBLANK ( _inv ),  "No data",
        _inv,              "Inverted (" & _run & " obs)",
        "Normal"
    )

Ongoing Episodes =
CALCULATE (
    COUNTROWS ( 'spread_inversion_episode' ),
    'spread_inversion_episode'[is_ongoing] = TRUE ()
)

Episode Duration (days) =
SELECTEDVALUE ( 'spread_inversion_episode'[calendar_days] )
```

**Note on ratios.** `config/spreads.yml` supports `op: ratio` as well as
`spread`. Inversion logic (`is_inverted`, `inversion_run`,
`spread_inversion_episode`) applies to **spreads only** — a ratio has no zero
line. Filter ratio rows out of the inversion visuals.

**Acceptance criteria**
- Episode count per spread matches a manual scan of sign changes in
  `curve_spread_daily` for one spread.
- Ongoing episodes render with an open-ended bar, not a bar ending today.
- Ratio-type entries never appear on inversion visuals.

---

### Report 5 — Funding & Liquidity (FUND / BMRK)

**Purpose.** The funding corridor, the Fed's balance sheet, funding spreads, and
a single 0–100 stress gauge — plus the benchmark rate board.

**Audience.** Executive gauge + analyst tape.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.funding_stress_daily` | 1 / date | gauge fact |
| `gold.funding_tape_daily` | 1 / metric × date | tape fact |
| `gold.benchmark_rate_board` | 1 / rate (latest) | board fact |
| `gold.dim_date`, `gold.market_calendar` (FEDWIRE) | — | dimensions |

**Pages**

1. **Overview.** The stress gauge: `stress_score` (0–100) as a gauge visual with
   bucket bands — calm < 40, normal < 60, elevated < 80, stressed ≥ 80 — plus
   `stress_bucket` as a text verdict and the score's 1-year history as an area
   chart with the same bands.
2. **Funding tape.** `funding_tape_daily` faceted by `metric_type`
   (`rate` / `balance` / `spread`), each panel with its own axis. Corridor rates
   (IORB, EFFR, OBFR, SOFR, BGCR, TGCR) on one panel makes the corridor visible.
3. **Benchmark board.** `benchmark_rate_board` as a table with trend arrows
   (`trend`), `change_bps`, `spread_to_benchmark_bps`, `zscore`, `percentile`,
   and the `regime` tag (tightening / easing / stable), grouped by
   `rate_category` (policy, treasury, secured_overnight, unsecured_overnight,
   term_reference, real, inflation_linked, lending).
4. **Component detail.** The stress gauge's component spreads with their
   individual z-scores, so a user can see *which* component is driving the score.

**Model notes.** `funding_stress_daily` emits a row **only on dates where every
component spread has a value** (`n_components`). Gaps in the gauge line are
correct behaviour, not a defect — do not interpolate. Surface `n_components` on
the Overview page so a user can see coverage.

**Key measures**

```dax
Stress Score = SELECTEDVALUE ( 'funding_stress_daily'[stress_score] )

Stress Bucket = SELECTEDVALUE ( 'funding_stress_daily'[stress_bucket] )

Stress Colour =
SWITCH ( [Stress Bucket],
    "calm",     "#1B7F79",
    "normal",   "#4B8FA6",
    "elevated", "#D08C34",
    "stressed", "#B4441F",
    "#6B7280"
)

Rate Change (bps) = SELECTEDVALUE ( 'benchmark_rate_board'[change_bps] )

Trend Arrow =
SWITCH ( SELECTEDVALUE ( 'benchmark_rate_board'[trend] ),
    "rising", "▲", "falling", "▼", "—"
)
```

**Acceptance criteria**
- Gauge bands match the documented thresholds exactly.
- Days with an incomplete component set show a gap, not a carried-forward value.
- Board covers every rate in `config/benchmark_rates.yml`.

---

### Report 6 — Fed Policy Watch (FOMC)

**Purpose.** Market-implied policy path: what the curve says the Fed will do at
each upcoming meeting.

**Audience.** Executive-first.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.fomc_probability` | 1 / meeting × 25bp outcome bucket | probability fact |
| `gold.fomc_meeting_path` | 1 / meeting | path fact |
| `gold.benchmark_rate_board` | 1 / rate | current-policy context |
| `gold.dim_date` | — | dimension |

**Pages**

1. **Overview.** Next meeting's probability bars (`outcome_bps` × `probability`),
   the current target range (`target_lower_bps` / `target_upper_bps`) as a card,
   and the implied path line from `fomc_meeting_path[implied_rate]` across all
   upcoming meetings.
2. **Probability grid.** Meeting × outcome-bucket heatmap, `probability` as
   colour intensity. Meetings on the x-axis, 25bp rungs on the y.
3. **Implied path detail.** `implied_move_bps` per meeting and
   `cumulative_move_bps` since `model_vintage`.

**Model notes — read this before building.** These probabilities are **derived
from the short end of the Treasury curve via a forward-rate bootstrap**, not
from CME fed funds futures. They are a model output, not a market quote. The
report must:
- Display `model_vintage` (the curve snapshot date) prominently on every page.
- Carry a standing methodology note on the Overview page and the About page:
  *"Derived from Treasury short-rate forwards (DFEDTARL/DFEDTARU/EFFR,
  DGS1MO–DGS1). Not CME FedWatch. Recomputed each pipeline run."*
- Never label a value "market-implied probability" without that qualifier.

Only meetings on or after `model_vintage` are present in the data.

**Key measures**

```dax
Probability = SUM ( 'fomc_probability'[probability] )

Modal Outcome (bps) =
VAR _t = TOPN ( 1, 'fomc_probability', 'fomc_probability'[probability], DESC )
RETURN MAXX ( _t, 'fomc_probability'[outcome_bps] )

Implied Rate = SELECTEDVALUE ( 'fomc_meeting_path'[implied_rate] )

Cumulative Move (bps) = SELECTEDVALUE ( 'fomc_meeting_path'[cumulative_move_bps] )

Model Vintage = "Model vintage: " & FORMAT ( MAX ( 'fomc_probability'[model_vintage] ), "yyyy-mm-dd" )
```

**Acceptance criteria**
- Probabilities sum to ~1.00 (±0.01) per `meeting_date`.
- `model_vintage` visible on every page.
- Methodology qualifier present on Overview and About.

---

### Report 7 — Credit Conditions (CRDT)

**Purpose.** IG and HY option-adjusted spreads, the credit curve by rating, and
where spreads sit against their own history.

**Audience.** Analyst-first with an executive stress card.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.credit_spread_daily` | 1 / instrument × date | primary fact |
| `gold.credit_spread_rolling` | 1 / instrument × date × window | momentum fact |
| `gold.dim_date` | — | dimension |

**Instruments** (`config/credit.yml`): IG_OAS, HY_OAS (headline); AAA/AA/A/BBB
(rating_ig); BB/B/CCC (rating_hy).

**Pages**

1. **Overview.** IG and HY OAS cards in bps with `change_bps`, percentile gauge,
   `is_stress_episode` flag, and the HY–IG gap.
2. **Rating curve.** OAS by rating rung (AAA → CCC) at the selected date, with
   a prior-date overlay. This is where credit-curve steepening shows up.
3. **Spread history.** Selected instrument over time with recession shading and
   stress-episode markers (`is_stress_episode` = expanding percentile ≥ the
   configured `stress_percentile`).
4. **Valuation.** Percentile/z-score heatmap across instruments.
5. **Rolling momentum.** Window slicer over `credit_spread_rolling` — **bps
   only** in this table.

**Key measures**

```dax
OAS (bps) = SELECTEDVALUE ( 'credit_spread_daily'[oas_bps] )

HY-IG Gap (bps) =
VAR _hy = CALCULATE ( [OAS (bps)], 'credit_spread_daily'[instrument] = "HY_OAS" )
VAR _ig = CALCULATE ( [OAS (bps)], 'credit_spread_daily'[instrument] = "IG_OAS" )
RETURN _hy - _ig

In Stress Episode =
IF ( SELECTEDVALUE ( 'credit_spread_daily'[is_stress_episode] ), "Stress", "Normal" )

Valuation Verdict =
VAR _p = SELECTEDVALUE ( 'credit_spread_daily'[percentile] )
RETURN
    SWITCH ( TRUE (),
        ISBLANK ( _p ), "No data",
        _p >= 0.9,      "Very wide (top decile)",
        _p >= 0.7,      "Wide",
        _p <= 0.1,      "Very tight (bottom decile)",
        _p <= 0.3,      "Tight",
        "Mid-range"
    )
```

**Licensing note.** The BAML/ICE OAS series arrive via FRED and are covered by
FRED's redistribution terms, but they originate from ICE Data Indices. The
`config/data_licensing.yml` note on `fred` flags this. Keep the report Tier A but
mention ICE as the index originator on the About page.

**Acceptance criteria**
- HY OAS > IG OAS on every date (a violation indicates a data defect worth
  raising, not a report bug to hide).
- Rating curve is monotonic AAA → CCC on a spot-checked date, or the exception
  is explainable.

---

### Report 8 — Regime & Recession Risk (REGIME / ML)

**Purpose.** The single "what macro regime are we in" answer, its five component
pillars, recession probability at three horizons, and statistical anomaly
detection.

**Audience.** Executive-first; this is the report that produces a headline verdict.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.macro_regime_daily` | 1 / date | regime fact |
| `gold.recession_probability_daily` | 1 / date | probit fact |
| `gold.macro_factor_scores` | 1 / date × factor | PCA fact |
| `gold.macro_factor_loadings` | 1 / date × factor × feature | PCA loadings |
| `gold.macro_anomaly_scores` | 1 / date | anomaly fact |
| `gold.zscore_heatmap` | 1 / series × date | cross-series context |
| `gold.dim_date`, `gold.dim_series` | — | dimensions |

**Pages**

1. **Overview.** The regime name as the hero element (`regime_name`:
   Goldilocks / Reflation / Stagflation / Growth-Scare / Liquidity-Squeeze /
   Policy-Easing / Neutral), `regime_confidence`, the five pillar scores as a
   radar or bullet chart, `composite_score`, and the three recession
   probabilities as gauges.
2. **Regime history.** Regime ribbon (regime name as a coloured band over time)
   with recession shading beneath, plus the five pillar time series as small
   multiples.
3. **Recession probability.** `prob_recession_3m` / `_6m` / `_12m` fan chart with
   NBER shading. **Filter or visually distinguish `is_backfilled = TRUE`** —
   these are early rows fit on fewer than `min_obs` training examples and are not
   comparable to mature estimates.
4. **PCA factors.** `macro_factor_scores` over time with
   `explained_variance_ratio`, plus a loadings heatmap
   (`macro_factor_loadings[loading]` by factor × feature) at the selected date.
5. **Anomalies.** `mahalanobis_d2` over time with `is_anomaly` markers
   (p < 0.01), annotated against known events.
6. **Cross-series z-score heatmap.** `zscore_heatmap` filtered to a date for a
   category-level heat grid.

**Model notes.** `macro_regime_daily` emits only once every pillar has at least
one live input within `max_staleness_days` (200). A pillar whose inputs are all
stale drops out and the row does not emit — early history will be sparse. The
regime rules are **ordered, first-match-wins**, so `regime_confidence` is the
smallest z-margin by which the matched rule cleared; it is **NULL for the default
`Neutral` regime** and must render as "n/a", not as zero confidence.

**Key measures**

```dax
Current Regime = SELECTEDVALUE ( 'macro_regime_daily'[regime_name] )

Regime Confidence =
VAR _c = SELECTEDVALUE ( 'macro_regime_daily'[regime_confidence] )
RETURN IF ( ISBLANK ( _c ), "n/a (default regime)", FORMAT ( _c, "0.00" ) )

Composite Score = SELECTEDVALUE ( 'macro_regime_daily'[composite_score] )

-- Higher composite = friendlier (risk-on) macro mix.
Composite Verdict =
SWITCH ( TRUE (),
    [Composite Score] > 0.5,  "Supportive",
    [Composite Score] < -0.5, "Hostile",
    "Mixed"
)

Recession Prob 12m = SELECTEDVALUE ( 'recession_probability_daily'[prob_recession_12m] )

Mature Estimates Only =
CALCULATE (
    [Recession Prob 12m],
    'recession_probability_daily'[is_backfilled] = FALSE ()
)

Explained Variance (top 3) =
CALCULATE (
    MAX ( 'macro_factor_scores'[cumulative_variance_ratio] ),
    'macro_factor_scores'[factor] = 3
)
```

**Acceptance criteria**
- Regime ribbon has no unexplained gaps after the date all five pillars go live.
- `is_backfilled` rows are visually distinguished on the probability page.
- `regime_confidence` renders "n/a" for `Neutral`, never `0`.
- PCA loading signs are stable across adjacent dates (the engine anchors the
  max-abs component positive; a sign flip in the chart indicates a binding error).

---

### Report 9 — Statistical Lab (STAT / EDA)

**Purpose.** The relationship toolkit: correlation, lead/lag, Granger causality,
and structural breaks between configured series pairs.

**Audience.** Quant researcher only. No executive page — this report's Overview
is a *methodology* page instead.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.series_correlation` | 1 / pair × window × date | correlation fact |
| `gold.series_lead_lag` | 1 / pair × lag | CCF/Granger fact |
| `gold.series_structural_breaks` | 1 / pair × test_type | break fact |
| `gold.fred_series_zscore_rolling` | 1 / series × date × window | multi-window z |
| `gold.fred_feature_transforms` | 1 / series × date | underlying transforms |
| `gold.dim_series`, `gold.dim_date` | — | dimensions |

**Pages**

1. **Methodology.** What each test does, what `transform_a`/`transform_b` mean
   (default first-differenced), what `window = 0` means (expanding full sample
   to date, from the 3rd common observation), and the significance conventions.
   This page exists because misreading these outputs is the main risk.
2. **Correlation matrix.** Pair × correlation heatmap at the selected date and
   window, with `n_obs` surfaced — a correlation on 8 observations must not look
   like one on 800.
3. **Rolling correlation.** Selected pair's `correlation` over time by window,
   with the expanding (`window = 0`) line as a reference.
4. **Lead/lag.** CCF bar chart across `lag` (**positive lag = series_a LEADS
   series_b** — label the axis explicitly), with `best_lag` highlighted and the
   two Granger directions as cards (`granger_f_ab`/`granger_p_ab` = does A help
   predict B?, and the reverse).
5. **Structural breaks.** `break_date` scatter by pair, split by `test_type`
   (`chow` / `cusum`), with `f_stat`, `p_value`, `is_significant`, and the
   pre/post means (`pre_mean_a`, `post_mean_a`, `pre_mean_b`, `post_mean_b`).

**Model notes.** `series_lead_lag` denormalises `best_lag` and the Granger
statistics onto **every lag row** of a pair. Aggregating them with `SUM` will
multiply them by the lag count — always use `SELECTEDVALUE` or `MAX`.
`series_structural_breaks[break_date]` is NULL when the test could not run (too
few observations); render that as "not testable", not as a missing date.

**Key measures**

```dax
Correlation = SELECTEDVALUE ( 'series_correlation'[correlation] )

Sample Size = SELECTEDVALUE ( 'series_correlation'[n_obs] )

-- Guard against reading a correlation off a tiny sample.
Correlation (guarded) =
IF ( [Sample Size] < 30, BLANK (), [Correlation] )

Best Lag = SELECTEDVALUE ( 'series_lead_lag'[best_lag] )

Lead Lag Narrative =
VAR _l = [Best Lag]
VAR _a = SELECTEDVALUE ( 'series_lead_lag'[series_a] )
VAR _b = SELECTEDVALUE ( 'series_lead_lag'[series_b] )
RETURN
    SWITCH ( TRUE (),
        ISBLANK ( _l ), "No data",
        _l > 0, _a & " leads " & _b & " by " & _l & " periods",
        _l < 0, _b & " leads " & _a & " by " & ABS ( _l ) & " periods",
        "Contemporaneous"
    )

Granger A→B p =
VAR _p = SELECTEDVALUE ( 'series_lead_lag'[granger_p_ab] )
RETURN IF ( ISBLANK ( _p ), "n/a", FORMAT ( _p, "0.0000" ) )

Break Verdict =
VAR _d = SELECTEDVALUE ( 'series_structural_breaks'[break_date] )
RETURN
    IF ( ISBLANK ( _d ), "Not testable (too few obs)",
        FORMAT ( _d, "yyyy-mm-dd" ) &
        IF ( SELECTEDVALUE ( 'series_structural_breaks'[is_significant] ), " (significant)", " (not significant)" ) )
```

**Storage-mode note.** `fred_feature_transforms` spans the full series universe
and deep history. If the import exceeds the refresh window, either restrict the
model to the pairs in `config/stats_pairs.yml` (the only ones the other three
tables cover anyway) or move this one table to DirectQuery. Restricting is
preferred.

**Acceptance criteria**
- Positive-lag interpretation is labelled on the CCF axis.
- `n_obs` is visible wherever a correlation is displayed.
- Denormalised Granger columns aggregate correctly (no lag-count multiplication).

---

### Report 10 — Global Macro Monitor (GCPI / GPOL)

**Purpose.** Inflation and policy rates across ~76 countries: who is above
target, who is hiking, who is cutting.

**Audience.** Executive-first (map and board), analyst detail behind.
**Tier.** **A for non-commercial distribution, with BIS + World Bank
attribution.** Commercial distribution needs BIS permission first (§8-G4).

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.global_inflation` | 1 / country × print | inflation fact |
| `gold.global_policy_rates` | 1 / country × print | policy fact |
| `gold.dim_date` | — | dimension |

**Pages**

1. **Overview.** Choropleth map of `cpi_yoy_pct` by `iso3`, region cards
   (AMER / EMEA / APAC), and counts of countries `hiking` / `cutting` / `on-hold`
   from `stance`.
2. **Inflation board.** Table by country: `cpi_yoy_pct`, `change_pp`, `trend`
   (accelerating / cooling / flat, ±0.05pp dead-band), `streak` (signed
   consecutive-print run), `target_pct`, `vs_target_pp`. Conditional formatting
   on `vs_target_pp` — diverging palette centred on zero.
3. **Policy board.** Table by country: `policy_rate_pct`, `change_bps`,
   `last_move_bps`, `stance`, `real_rate_pct`.
4. **Real rates.** Scatter of `policy_rate_pct` vs `cpi_yoy_pct` by country,
   with the zero-real-rate diagonal drawn — the clearest single view of who is
   actually restrictive.
5. **Country detail.** Selected country's inflation and policy history together.

**Model notes — mixed frequency is the central hazard.** World Bank
`FP.CPI.TOTL.ZG` entries are **annual**; the US runs off **monthly** `CPIAUCSL`
(`yoy_from_index`); several majors run off monthly OECD-style FRED series
(`JPNCPIALLMINMEI`, `DEUCPIALLMINMEI`, `GBRCPIALLMINMEI`,
`CP0000EZ19M086NEST`, …). **A map that renders "latest print" across a
mixed-frequency panel silently compares a 2025 annual figure to a July 2026
monthly one.** Required mitigations:
- Show `observation_date` per country in every tooltip.
- Add an explicit "as-of spread" indicator on the map page: min and max
  `observation_date` across the rendered countries.
- Provide a frequency slicer so an analyst can restrict to monthly reporters.

`real_rate_pct` is **ex-post** (policy rate − latest CPI YoY print on or before
the date) and is populated only where configured and fresh. Label it "ex-post
real rate"; blanks are expected.

**Key measures**

```dax
CPI YoY % = SELECTEDVALUE ( 'global_inflation'[cpi_yoy_pct] )

vs Target (pp) = SELECTEDVALUE ( 'global_inflation'[vs_target_pp] )

Trend Streak =
VAR _s = SELECTEDVALUE ( 'global_inflation'[streak] )
RETURN
    SWITCH ( TRUE (),
        ISBLANK ( _s ), "",
        _s > 0, _s & " prints accelerating",
        _s < 0, ABS ( _s ) & " prints cooling",
        "flat"
    )

Hiking Count =
CALCULATE ( DISTINCTCOUNT ( 'global_policy_rates'[country] ),
            'global_policy_rates'[stance] = "hiking" )

As-Of Spread =
"Prints from " & FORMAT ( MIN ( 'global_inflation'[observation_date] ), "yyyy-mm" ) &
" to "        & FORMAT ( MAX ( 'global_inflation'[observation_date] ), "yyyy-mm" )
```

**Acceptance criteria**
- Every map tooltip shows the country's own `observation_date`.
- As-of spread indicator present on the map page.
- CC BY 4.0 attribution for World Bank data on the About page.
- Report placed in an internal workspace until §8-G4 clears.

---

### Report 11 — Equity & Factor Analytics

**Purpose.** Total vs price return, realized volatility, index membership, and
macro-factor attribution for the covered equity universe.

**Audience.** Analyst-first.
**Tier.** **B — internal only.** Tiingo, Stooq, and iShares are all
`redistribution_allowed: false`. No external sharing, no publish-to-web.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.equity_total_return_index` | 1 / ticker × date | TR fact (Tiingo) |
| `gold.equity_return_daily` | 1 / ticker × date | PR fact (Stooq) |
| `gold.realized_volatility` | 1 / ticker × date × window | vol fact |
| `gold.index_constituents` | 1 / ETF × constituent × snapshot | membership fact |
| `gold.equity_factor_attribution` | 1 / ticker × factor × window × date | beta fact |
| `gold.equity_factor_implied_return` | 1 / ticker × window × month | decomposition fact |
| `gold.equity_price_reconciliation` | 1 / ticker × date | QA reference |
| `gold.dim_date`, `gold.market_calendar` (NYSE) | — | dimensions |

**Pages**

1. **Overview.** Total-return vs price-return index lines for the selected
   ticker (their gap **is** reinvested income), `dividend_yield_pct`,
   `trailing_12m_dividend`, and 21/63/252-day realized vol cards.
2. **Returns.** Return heatmap (ticker × month), cumulative index chart,
   drawdown.
3. **Volatility.** `realized_volatility` by window with a window slicer; vol
   cone (percentile bands of each window's history) and current reading against
   it. Note rows appear only once a window is fully populated — no partial-window
   stats.
4. **Index membership.** `index_constituents` treemap by `weight_pct`, filtered
   `is_latest_snapshot = TRUE` for current membership; weight-drift chart across
   snapshots for a selected constituent.
5. **Factor attribution.** Rolling `beta` per PCA factor with `t_stat`
   significance shading, `r_squared`, and `alpha`. Remember `alpha` / `r_squared`
   / `n_obs` are **repeated across every factor row** for the same
   (ticker, window, date) — use `SELECTEDVALUE`, never `SUM`.
6. **Return decomposition.** `equity_factor_implied_return`: `factor_return`
   (systematic) + `alpha_return` + `residual_return` vs `realized_return`, as a
   stacked bar per month.
7. *(hidden)* **Data quality.** `equity_price_reconciliation`: Stooq vs Tiingo
   `pct_diff` scatter with `diverged` flagged. ~1% divergence is expected
   (Stooq adjusts splits only; Tiingo `adjClose` adjusts splits + dividends).

**Model notes.** `manifests/equity_stooq.yml` is currently **fully inactive**
(89 series, all `active: false`), so `equity_return_daily` and
`equity_price_reconciliation` will be empty until it is activated.
`equity_tiingo.yml` is active (85 series), so total return, realized vol, and
factor attribution populate. Build pages 1–3 and 5–6 first; page 4 depends on
`etf_holdings.yml` (active, 1 ETF) and page 7 on Stooq activation. See §8-G3.

**Key measures**

```dax
Total Return Index = SELECTEDVALUE ( 'equity_total_return_index'[total_return_index] )
Price Return Index = SELECTEDVALUE ( 'equity_total_return_index'[price_return_index] )

-- The gap between the two indices is cumulative reinvested income.
Income Contribution = [Total Return Index] - [Price Return Index]

Dividend Yield % = SELECTEDVALUE ( 'equity_total_return_index'[dividend_yield_pct] )

Realized Vol % = SELECTEDVALUE ( 'realized_volatility'[realized_vol_pct] )

Factor Beta = SELECTEDVALUE ( 'equity_factor_attribution'[beta] )

-- Repeated across factor rows: SELECTEDVALUE, not SUM.
Model R² = SELECTEDVALUE ( 'equity_factor_attribution'[r_squared] )

Beta Significant =
VAR _t = SELECTEDVALUE ( 'equity_factor_attribution'[t_stat] )
RETURN IF ( ABS ( _t ) >= 1.96, "significant", "not significant" )

Residual Return = SELECTEDVALUE ( 'equity_factor_implied_return'[residual_return] )
```

**Acceptance criteria**
- Total-return index ≥ price-return index for every dividend-paying ticker.
- Vol cone uses only fully-populated windows.
- Report deployed to an internal-only workspace with a sensitivity label; the
  About page states the redistribution restriction.

---

### Report 12 — Regional & State Economic Map

**Purpose.** State-level economic conditions: unemployment, house prices, and
the Philadelphia Fed coincident index, mapped and ranked.

**Audience.** Executive-first (map), analyst ranking behind.
**Tier.** A.
**Status.** ✅ **Unblocked.** `config/series_catalog.yml` now carries a
`REGIONAL` bucket of 120 state / census-region series, each with a `geo` code
surfaced on `gold.dim_series[geo]`. No report-local state mapping is needed.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.dim_series` (`econ_category = 'REGIONAL'`) | 1 / series | state dimension via `geo` |
| `gold.macro_indicator_dashboard` | 1 / series (latest) | latest state readings |
| `gold.macro_category_summary` (`REGIONAL` row) | 1 / category | state diffusion index |
| `gold.fred_latest_observation` | 1 / series × date | history fact |
| `gold.fred_feature_transforms` | 1 / series × date | YoY / z-score fact |
| `gold.zscore_heatmap` | 1 / series × date | cross-state z-scores |
| `gold.dim_date` | — | dimension |

**Series in scope** (cataloged as `REGIONAL`, 120 total):

| Source manifest | Cataloged | Content | Polarity | Transform |
|---|---|---|---|---|
| `state_unemployment.yml` | 52 | State unemployment rates (50 states + DC + PR) | −1 | level |
| `state_coincident_index.yml` | 50 | Philadelphia Fed state coincident activity indexes | +1 | pc1 |
| `state_house_price.yml` | 14 | FHFA all-transactions state house price indexes | 0 | pc1 |
| `regional_aggregates.yml` | 4 | Census-region unemployment rates | −1 | level |

**The state dimension is `dim_series` itself.** Filter it to
`econ_category = 'REGIONAL'` and use `geo` as the map key — a USPS code for
states (`CA`, `NY`, `DC`, `PR`) and a region name for the four census
aggregates (`Midwest`, `Northeast`, `South`, `West`). Because the census-region
rows share the table with state rows, **filter `geo` to two-character codes for
the choropleth** or the four regions will fail to map.

Split the three metrics apart with a Power Query grouping on the `series_id`
suffix (`*UR`, `*PHCI`, `*STHPI`), or add a report-local metric column derived
from `notes`. A metric field on the catalog would be cleaner and is worth
raising if a second regional report appears.

**The diffusion index is free.** `macro_category_summary` computes breadth per
category, so its `REGIONAL` row is the share of state series improving
(polarity-adjusted) — exactly what page 4 needs. Note it blends unemployment,
activity, and house prices; for a pure activity diffusion count, compute it off
the `*PHCI` subset instead.

**Pages**

1. **Overview.** Choropleth of state unemployment rate, region cards, and the
   best/worst five states.
2. **Rankings.** State table: level, YoY change, z-score vs own history,
   percentile vs all states.
3. **House prices.** Choropleth and ranking on the 14 covered states, clearly
   labelled as partial coverage.
4. **Coincident index.** Diffusion view — how many states are expanding vs
   contracting, over time. This is the report's most decision-relevant page.
5. *(hidden)* **State drillthrough** — all metrics for one state.

**Key measures**

```dax
-- dim_series filtered to REGIONAL; geo is the map key.
State Code = SELECTEDVALUE ( 'dim_series'[geo] )

-- Census-region rows carry a name, not a 2-letter code; keep them off the map.
Is Mappable State = IF ( LEN ( [State Code] ) = 2, TRUE (), FALSE () )

State Diffusion % =
CALCULATE (
    SELECTEDVALUE ( 'macro_category_summary'[breadth_pct] ),
    'macro_category_summary'[econ_category] = "REGIONAL"
)
```

**Acceptance criteria**
- Map covers all 50 states + DC for unemployment; PR and the four census
  regions are excluded from the choropleth but present in the rankings table.
- Partial-coverage pages (house prices, 14 states) label their coverage
  explicitly.
- Every rendered state resolves from `geo` — no title parsing anywhere in the
  model.
- Diffusion count reconciles against a manual count for one date.

---

### Report 13 — Point-in-Time & Revisions Lab

**Purpose.** Answer "what was known on date X" and "how much does this series get
revised" — the report backtest authors need before trusting a series.

**Audience.** Quant researcher only. Methodology page instead of an executive page.
**Tier.** A.

**Tables**

| Table | Grain | Role |
|---|---|---|
| `gold.fred_revision_stats` | 1 / series × observation | revision fact |
| `gold.v_series_revision_summary` | 1 / series | revision summary view |
| `gold.fred_point_in_time` | 1 / series × obs × vintage | vintage fact |
| `gold.v_point_in_time` | 1 / vintage row | vintage view (Silver-backed) |
| `gold.fred_cross_series_feature` | 1 / feature × date | latest-revised features |
| `gold.fred_cross_series_feature_pit` | 1 / feature × date | PIT-aligned features |
| `gold.dim_series`, `gold.dim_date` | — | dimensions |

**Pages**

1. **Methodology.** What a vintage is, what `realtime_start` / `realtime_end`
   mean, why `vintage_enabled` defaults to true, and the difference between the
   latest-revised and PIT cross-series features. Includes the standing warning:
   *a backtest built on latest-revised data overstates its own performance.*
2. **Revision profile.** `v_series_revision_summary` ranked:
   `avg_abs_revision_pct`, `max_abs_revision_pct`, `avg_revision_count`. The
   headline output is a "trust the first print?" ranking — GDP and payrolls at
   one end, market/price series at the other.
3. **Revision detail.** For a selected series: first print vs current value per
   observation date, revision magnitude over time, and `revision_count`
   distribution.
4. **Vintage explorer.** An as-of date slicer over `fred_point_in_time` /
   `v_point_in_time`: the series as it appeared on that date, overlaid with the
   series as it appears today. This is the report's centrepiece.
5. **PIT vs latest-revised features.** Same cross-series feature computed both
   ways (`fred_cross_series_feature` vs `_pit`), with the gap charted — a direct
   measure of how much leakage a naive backtest would absorb.

**Model notes.** `fred_point_in_time` is the largest object in the suite (every
vintage of every vintage-enabled series). Mitigations, in order:
1. Restrict the model to the series a researcher actually backtests (a
   parameterised series list), not the full universe.
2. Apply incremental refresh partitioned on `observation_date`.
3. Move this one table to DirectQuery if 1–2 are insufficient.

The as-of filter is a range predicate, not an equality:
`realtime_start <= AsOf AND (realtime_end >= AsOf OR realtime_end IS NULL)`.
Implement it as a disconnected as-of date parameter table plus a measure —
**not** as a relationship, which cannot express it.

**Key measures**

```dax
-- Disconnected parameter table 'AsOf'[Date] drives the vintage filter.
As Of Selected = SELECTEDVALUE ( 'AsOf'[Date], MAX ( 'dim_date'[date] ) )

Value As Known =
VAR _asof = [As Of Selected]
RETURN
CALCULATE (
    SELECTEDVALUE ( 'fred_point_in_time'[value] ),
    FILTER (
        'fred_point_in_time',
        'fred_point_in_time'[realtime_start] <= _asof
            && ( ISBLANK ( 'fred_point_in_time'[realtime_end] )
                 || 'fred_point_in_time'[realtime_end] >= _asof )
    )
)

Avg Abs Revision % = SELECTEDVALUE ( 'v_series_revision_summary'[avg_abs_revision_pct] )

First Print Trust =
VAR _r = [Avg Abs Revision %]
RETURN
    SWITCH ( TRUE (),
        ISBLANK ( _r ), "Unknown",
        _r < 0.1,  "High — first print ≈ final",
        _r < 1.0,  "Moderate",
        "Low — heavily revised"
    )

PIT vs Revised Gap =
SELECTEDVALUE ( 'fred_cross_series_feature'[value] )
    - SELECTEDVALUE ( 'fred_cross_series_feature_pit'[value] )
```

**Acceptance criteria**
- Vintage explorer reproduces a known historical print (spot-check one GDP
  vintage against ALFRED).
- Non-vintage series correctly show `revision_count = 1` and are excluded from
  the revision ranking rather than ranked as "never revised".
- Model refreshes inside the scheduled window.

---

### Report 14 — Pipeline Health & Data Governance

**Purpose.** Is the data trustworthy today? Run status, data-quality outcomes,
freshness by source, metadata drift, and cross-source reconciliation.

**Audience.** Data operations / platform team.
**Tier.** A (but the natural audience is internal).

**Tables**

| Table | Grain | Role |
|---|---|---|
| `audit.etl_run` | 1 / run | run fact |
| `audit.etl_series_run` | 1 / run × series | series-run fact |
| `audit.data_quality_result` | 1 / run × series × check | DQ fact |
| `meta.series_staleness` | 1 / source × series | freshness fact |
| `meta.fred_series_drift` | 1 / finding | drift fact |
| `meta.fred_series_lifecycle` | 1 / series × snapshot | lifecycle fact |
| `meta.fred_series` | 1 / series | series reference |
| `gold.v_source_coverage` | 1 / source × series | coverage view |
| `gold.fred_source_reconciliation` | 1 / concept × date | reconciliation fact |
| `gold.powerbi_catalog` | 1 / Gold object | documentation |

**Pages**

1. **Overview.** Last run status (`status`: running / succeeded / failed /
   partial), `duration_seconds`, `series_succeeded` / `series_failed`, DQ pass
   rate, count of stale series, count of error-severity drift findings. Five
   numbers that answer "can I trust today's data".
2. **Run history.** `etl_run` over time: duration trend, success rate,
   series counts. A duration trend is the earliest warning of an upstream
   problem.
3. **Series-level failures.** `etl_series_run` filtered to `status <> 'succeeded'`
   or `dq_passed = FALSE`, with `error_message`, `load_type`
   (`full` / `restate_last_<n>`), and rows written to Bronze/Silver.
4. **Data quality.** `data_quality_result` by `check_name` and `severity`, with
   pass rate per check and the failing series list. Error-severity failures are
   the actionable set.
5. **Freshness.** `meta.series_staleness` by `source`: stale counts,
   `days_since_last_observation` distribution, and the stale-series list. This
   covers **every** source (it is computed from ingested data, not a live API
   call), unlike drift.
6. **Metadata drift.** `fred_series_drift` by `kind` (`frequency_mismatch`,
   `discontinued`, `units_changed`, `not_found`) and `severity`, with
   manifest-vs-FRED values side by side. FRED-only by design.
7. **Cross-source reconciliation.** `fred_source_reconciliation` — same-concept
   series from different sources with the divergence flag; plus
   `equity_price_reconciliation` if equity sources are active.
8. **Gold object catalog.** `gold.powerbi_catalog` rendered as a searchable
   table: `object_name`, `object_type`, `module`, `grain`, `intended_visual`,
   `description`. This page is the suite's own documentation and should be
   linked from every other report's About page.

**Model notes.** The audit and meta tables are **append-only and grow with every
run**. Apply an incremental-refresh policy on `etl_series_run` and
`data_quality_result` (partition on run date) rather than importing full history
— these are the two highest-row-count tables in the report.

Timestamps in the meta tables (`checked_at`, `detected_at`,
`latest_observation_date`) are stored as **STRING** in several places
(`meta.fred_series_lifecycle`, `meta.fred_series_drift`,
`meta.series_staleness`). Convert them to dates in Power Query, do not rely on
implicit conversion.

**Key measures**

```dax
Last Run Status =
VAR _last = MAX ( 'etl_run'[started_at] )
RETURN CALCULATE ( SELECTEDVALUE ( 'etl_run'[status] ), 'etl_run'[started_at] = _last )

DQ Pass Rate =
DIVIDE (
    CALCULATE ( COUNTROWS ( 'data_quality_result' ), 'data_quality_result'[passed] = TRUE () ),
    COUNTROWS ( 'data_quality_result' )
)

Error Findings =
CALCULATE ( COUNTROWS ( 'data_quality_result' ), 'data_quality_result'[severity] = "error" )

Stale Series =
CALCULATE ( COUNTROWS ( 'series_staleness' ), 'series_staleness'[is_stale] = TRUE () )

Drift Errors =
CALCULATE ( COUNTROWS ( 'fred_series_drift' ), 'fred_series_drift'[severity] = "error" )

Trust Verdict =
SWITCH ( TRUE (),
    [Last Run Status] = "failed",           "🔴 Last run failed",
    [Error Findings] > 0,                   "🔴 " & [Error Findings] & " DQ errors",
    [Last Run Status] = "partial",          "🟡 Partial run",
    [Drift Errors] > 0,                     "🟡 " & [Drift Errors] & " drift errors",
    [Stale Series] > 0,                     "🟡 " & [Stale Series] & " stale series",
    "🟢 Healthy"
)
```

**Acceptance criteria**
- `[Trust Verdict]` reflects a deliberately failed test run correctly.
- Freshness page covers all sources present in `meta.series_staleness`, not
  just FRED.
- Catalog page row count matches `gold.powerbi_catalog`.

---

## 6. Performance and refresh engineering

### 6.1 Model sizing

Sizes are driven by the series universe (2,829 active series across the
manifests) and history depth. Before building, run the row-count query in
Appendix C against the target environment — **do not** size from this table
alone.

| Class | Objects | Expected size | Approach |
|---|---|---|---|
| Snapshot facts | `macro_indicator_dashboard`, `benchmark_rate_board`, `macro_category_summary`, `funding_stress_daily` | tiny (10²–10³ rows) | full import |
| Curated daily facts | `treasury_curve*`, `curve_spread*`, `credit_spread*`, `funding_tape_daily`, `macro_regime_daily` | small (10⁴–10⁵) | full import |
| Windowed facts | `curve_spread_rolling`, `credit_spread_rolling`, `treasury_curve_rolling`, `realized_volatility` | medium (10⁵–10⁶; ×7 windows) | full import; window slicer defaults to one window |
| **Universe-scope z-score facts** | `zscore_heatmap`, `fred_series_zscore_rolling` | **large** — built from *all* 2,829 active series, not the 279 cataloged ones | **restrict on load** (see below) |
| Universe facts | `fred_latest_observation`, `fred_feature_transforms`, `ml_feature_matrix` | large | restrict series; incremental refresh |
| Vintage facts | `fred_point_in_time`, `silver.fred_observation` | largest | restrict series + incremental refresh; DirectQuery last resort |
| Audit facts | `etl_series_run`, `data_quality_result` | grows every run | incremental refresh on run date |

**The z-score tables are a sizing trap.** `_build_zscore_views` reads the whole
of `gold.fred_feature_transforms`, so `zscore_heatmap` is one row per
(series, date) across the **entire active universe** and
`fred_series_zscore_rolling` multiplies that by four observation windows.
Neither is restricted to `config/series_catalog.yml`. Binding either one
unfiltered will dominate a model.

The fix is a load-time inner join to `dim_series`, which restricts to the 254
cataloged series as a side effect of the join:

```sql
FROM   ${p_Catalog}.gold.zscore_heatmap z
JOIN   ${p_Catalog}.gold.dim_series d ON d.series_id = z.series_id
```

That join is load-bearing, not cosmetic — drop it and the query returns the full
universe. The build plan's Report 8 and Report 9 queries already carry it.

### 6.2 Incremental refresh policy

Apply to: `fred_point_in_time`, `fred_feature_transforms`,
`fred_latest_observation`, `etl_series_run`, `data_quality_result`.

- `RangeStart` / `RangeEnd` parameters on `observation_date` (facts) or run date
  (audit).
- Store 10 years, refresh the last 90 days. The 90-day window matches the
  pipeline's default `restate_last_n = 90`, so restated revisions inside that
  window are picked up. **If a series overrides `restate_records` higher than 90
  (e.g. GDPC1 at 24 quarters), its older restatements will not be re-imported
  by an incremental refresh** — schedule a periodic full refresh for models
  containing such series, or widen the refresh window to match the largest
  configured `restate_records`.
- Do not enable "detect data changes" against Delta — the pipeline rewrites Gold
  tables wholesale (`CREATE OR REPLACE`), which defeats the detection.

### 6.3 Query folding

The `fnGold` pattern preserves folding on Databricks. Verify with "View Native
Query" after adding any transformation step. Filters, column removal, and type
changes fold; custom-column logic and index columns generally do not — push
those into the Gold layer via a pipeline PR instead, where every consumer
benefits.

---

## 7. Delivery plan

Phases are ordered by dependency and value. Each phase is independently
shippable.

| Phase | Reports | Depends on | Notes |
|---|---|---|---|
| **0. Kernel** | — | — | `fnGold`, dimensions, core measures, theme, PBIP scaffold, dev SQLite build. **Nothing else starts until this is done.** |
| **1. Operations** | **#14 Pipeline Health** | Phase 0 | **Built first, on its own.** No data prerequisites, and it is the only report that says whether the other thirteen can be trusted. |
| **2. Core macro** | #1 Macro Cockpit, #3 Curve Lab | Phase 1 | Highest-value pair for the executive audience; fully-populated data. |
| **3. Rates & credit** | #4 Spreads & Inversions, #6 Fed Policy Watch, #7 Credit Conditions | Phase 2 | Same shapes as phase 2; fast follow. |
| **4. Verdict layer** | #8 Regime & Recession Risk, #2 Inflation Explorer | Phase 2 | Both fully ready — §8-G2's "inactive manifests" claim was wrong; all three inflation trees resolve. |
| **5. Research** | #9 Statistical Lab, #13 PIT & Revisions Lab | Phase 2 | Both need the sizing work in §6. |
| **6. Breadth** | #12 Regional Map, #11 Equity & Factor, #10 Global Macro | G3, G4 | #12 is unblocked (§8-G1); #11 loses only its reconciliation page (G3); #10 is non-commercial-only (G4). |
| **7. Funding** | #5 Funding & Liquidity | Phase 3 | G7 fixed, so the stress gauge populates. Confirm `BGCRRATE` ingested after the first run. |

**Report 14 is built first, and finished, before phase 2 starts.** It is the
cheapest report here and the only one that answers "can I trust what the other
thirteen are showing me". It has no data prerequisites — the audit and meta
tables are written on every run regardless of which sources are active — and it
doubles as the acceptance test for the pipeline itself: which series failed, the
DQ pass rate, per-source staleness, and whether the Gold layer actually contains
what this document claims. Finding that out through a blank KPI card on the
Macro Cockpit is a much worse day.

Phases 3–7 are ordered by shape and audience rather than dependency, so they can
be resequenced freely to suit whoever is waiting.

---

## 8. Prerequisites and known gaps

These are the things that must change outside Power BI. Each is a pipeline PR,
not a report-authoring task.

**G1 — `dim_series` covers 279 of 2,829 active series.** *(Partially resolved.)*
Ingestion and presentation are separate layers. The manifests activate **2,829
series** (of 2,930 declared) — fred 2,570, tiingo 85, bls 60, worldbank 37,
bis 36, bea 23, ecb 9, sec 3, eia 2, treasury 2, census 1, ishares 1 — and all
of them reach Gold. Separately, `config/series_catalog.yml` gives some of them
presentation semantics (`econ_category`, `polarity`, `default_transform`,
`geo`, `scale`, `decimals`), which is what `dim_series` and the ECON objects
(`macro_indicator_dashboard`, `_sparkline`, `macro_category_summary`) iterate.

The catalog was expanded from 67 to **279** entries:

| Bucket | Entries | |
|---|---|---|
| LABOR | 21 | RATES 35, INFLATION 18, CREDIT 17, FX 22 |
| HOUSING | 10 | GROWTH 10, MONEY 9, ACTIVITY 9, CONSUMER 8 |
| **REGIONAL** | **120** | new bucket: 52 state unemployment, 50 coincident indexes, 14 state HPI, 4 census regions |

Every entry is verified active in a manifest (enforced by
`test_repo_series_catalog_entries_are_all_ingested`), and every REGIONAL entry
carries a `geo` code (enforced by
`test_repo_series_catalog_regional_entries_carry_geo`). This unblocked Report 12
and roughly doubled Report 1's national coverage.

**What remains.** The other 2,550 active series are fully ingested and queryable
via `fred_latest_observation`, `fred_point_in_time`, `fred_feature_transforms`,
`fred_series_zscore_rolling`, and `zscore_heatmap` — but still arrive with no
category, polarity, or formatting metadata. That is a deliberate curation
boundary, not an oversight: the bulk of the remainder is the long tail of
`prices_extra` (800), `money_banking` (366), `production_housing` (361),
`labor_extra` (356), and `national_accounts_extra` (332), which exist to feed
features and models rather than a dashboard. Extend the catalog when a specific
report needs a specific series.

This gap never affected the domain reports, which are driven by their own
configs rather than the catalog: `curve.yml` (#3), `spreads.yml` (#4),
`benchmark_rates.yml`/`funding.yml` (#5), `credit.yml` (#7), `regime.yml` (#8),
`stats_pairs.yml` (#9), `global_series.yml` (#10).

**Migration note.** `gold.dim_series` gained `geo` and `metric` columns. Both
backends pick them up without manual DDL:

- **SQLite** — a database file created before either column is upgraded on open
  by `LocalWarehouse._apply_additive_migrations`.
- **Databricks** — the Gold builder writes `dim_series` with
  `.mode("overwrite").option("overwriteSchema", "true")`, so the next `gold`
  build replaces the table's schema in place. **No `ALTER TABLE` is needed**
  (an earlier revision of this document said otherwise — that was wrong).

What *does* need to stay in step is the Spark `StructType` in
`fred_pipeline.writer.gold`: a row key with no matching `StructField` is
dropped on the Databricks write while the SQLite path stays fine, which is
invisible wherever the Spark tests skip. `tests/test_dim_series_schema_parity.py`
now compares all four declarations of this table.
*Blocks:* #12 Regional Map (fully), and limits #1 Macro Cockpit's coverage.
*Fix:* extend `series_catalog.yml`. For #12 specifically, add the state series
with a new `econ_category: REGIONAL` plus a state identifier.
*Owner:* pipeline. *Interim:* report-local `dim_state` (§5, Report 12).

**G2 — Inflation item trees.** ~~Depend on inactive manifests.~~ **Resolved —
the claim was wrong.** `bls_cpi_basket.yml` (30), `bls_cpi_basket_sa.yml` (29),
and `bea_pce_items.yml` (21) are all active, and all three trees resolve
completely: CPI/NSA 30/30, CPI/SA 29/29, PCE/SA 23/23. Report #2 is fully ready.
The original claim came from the README's stale "seven inactive demo manifests"
text rather than from the manifests themselves.

One caveat survives, as a labelling requirement rather than a gate: the PCE
contribution weights are **approximate nominal-PCE expenditure shares**, not
BEA's published relative importances. Refresh from BEA Section 2 Underlying
Detail for precise contributions, and say which is in use on the page.

**G3 — `equity_stooq.yml` is fully inactive (89 series).** *Narrower than
originally stated.* `select_canonical_equity_price_rows` converts Tiingo
`adjClose` into `<ticker>:close` rows for any ticker with no Stooq close, so
`equity_return_daily`, `realized_volatility`, `equity_factor_attribution`, and
`equity_factor_implied_return` **all populate from Tiingo's 85 active series**.
*Blocks:* only `gold.equity_price_reconciliation` (#11's hidden QA page), which
by construction needs both sources on the same (ticker, date).
*Fix:* activate the manifest when you want the cross-source price check;
otherwise mark that one page pending and ship the rest of #11.

**G4 — `bis` licensing.** ✅ **Registered** (the answer is a constraint, not a
clearance). `bis` now has a `config/data_licensing.yml` entry recording that the
BIS retains copyright in its statistical data, permits redistribution **without
written permission for non-commercial purposes**, requires permission for
commercial use, and treats attribution as a copyright condition.

*Effect on #10 Global Macro Monitor:* internal and non-commercial distribution
is fine with a BIS attribution on the About page; commercial distribution needs
BIS permission first. `validate --commercial` still flags `bis` — correctly, and
now with a real reason (`license_type='attribution-noncommercial'`) instead of
"unreviewed".

*Caveat — now tracked, not just noted:* the entry was written from secondary
reporting of the BIS terms. The primary page could not be read (egress to
bis.org is blocked by the authoring environment's network policy), so it carries
`review_status: provisional` and a `source_of_truth` saying exactly that.

`fred_pipeline validate --licensing-review` **fails** while any source that
permits redistribution is unverified. Today that is `bis` and `worldbank`:

```
ERROR: redistribution-review check failed -- these sources permit
redistribution on unverified authority:
  - bis (36 active series): permits redistribution but
    review_status='provisional' -- nobody has confirmed this against
    https://www.bis.org/terms_conditions.htm
  - worldbank (37 active series): ...
```

U.S. federal sources are exempt: they are public domain by statute
(17 U.S.C. 105), not by a terms page, so requiring a read there would be noise
that trains people to ignore the check.

**To clear it:** read `terms_url`, then set `review_status: verified` and
`reviewed_by: <name>` in `config/data_licensing.yml`. A `verified` entry with no
name is rejected, and a verification older than two years is flagged stale.
Run this check before publishing anything derived from the data outside the
organisation.

**G7 — Funding stress gauge emitted zero rows (`TGCR` id mismatch).** ✅ **Fixed.**
`config/funding.yml` and `config/benchmark_rates.yml` referenced `TGCR` while
`manifests/fed_funding.yml` declared FRED's real id, `TGCRRATE`. Nothing joined
them, so the `SOFR_TGCR` spread never computed — and because
`funding_stress_daily` emits only on dates where *every* component spread has a
value, the whole 0–100 gauge produced nothing, and #8's recession probit
silently lost its configured `funding_stress` feature.

The configs were wrong, not the manifest: FRED publishes the repo reference
rates with a `RATE` suffix (bare `TGCR`/`BGCR` are prefixes of the percentile
and volume companions). Both configs now point at `TGCRRATE`; gauge components
resolve 4/4. `BGCR` is now declared as `BGCRRATE` but ships `active: false`
pending a live id check — it is a tape/board row, not a gauge component.

Guarded by `tests/test_config_manifest_integrity.py`, which fails on any Gold
config referencing a series id no manifest declares. Full detail in
[`powerbi_report_build_plan.md`](powerbi_report_build_plan.md) §2 G-A.

**G5 — FOMC probabilities are curve-derived, not futures-derived.**
Not a defect — a documented design decision (option A, no CME connector). But it
is a *communication* risk: users will assume CME FedWatch.
*Mitigation:* the standing methodology note required in Report #6. No pipeline
change needed.

**G6 — Per-source metadata drift is FRED-only.**
`reconcile` diffs manifest intent against FRED's `/series` metadata; other
sources have no drift check. Staleness, by contrast, covers every source.
*Impact:* #14 page 6 is correctly labelled FRED-only. A non-FRED series that
changes frequency upstream will not be caught until it shows up as staleness or
a DQ failure.

---

## 9. Definition of done

A report is done when **all** of the following hold.

**Data**
- [ ] Every visual binds to a Gold object named in this spec (or in
      `gold.powerbi_catalog`); no ad-hoc SQL in the model.
- [ ] No DAX measure re-derives a statistic that exists as a Gold column.
- [ ] No measure computes a statistic over an unbounded window that would leak
      future data into historical rows.
- [ ] Units are labelled on every axis, card, and tooltip (§4.7 traps checked).
- [ ] NULL is distinguished from FALSE wherever `is_recession`,
      `regime_confidence`, `real_rate_pct`, or `break_date` appear.

**Model**
- [ ] Kernel version stamped on the About page and matching `KERNEL_VERSION`.
- [ ] `dim_date` marked as the date table.
- [ ] All relationships single-direction unless documented otherwise.
- [ ] `market_calendar` filtered to exactly one `calendar_name` if present.
- [ ] All table references go through `fnGold`; parameters resolve against both
      backends.

**Report**
- [ ] Page structure follows §4.4 (Overview / Analysis / Drillthrough / About).
- [ ] Freshness banner present and reflecting worst-source staleness.
- [ ] Alt text on every visual; tab order set; no colour-only encoding.
- [ ] Report-specific acceptance criteria in §5 all pass.

**Governance**
- [ ] Distribution tier assigned (§4.6) and the report deployed to the matching
      workspace with the matching sensitivity label.
- [ ] Attribution present where required (World Bank CC BY 4.0).
- [ ] PBIP committed to `/powerbi/<report_folder>/`.
- [ ] Refresh scheduled and a failure alert routed to a named owner.

---

## Appendix A — Gold object inventory

Reproduced from `gold.powerbi_catalog` (source of truth:
`fred_pipeline.writer.global_views.POWERBI_CATALOG`). The **Report** column maps
each object to its consumer in this suite.

| Object | Type | Module | Grain | Intended visual | Report |
|---|---|---|---|---|---|
| `dim_series` | dimension | ALL | 1 / series | slicers + relationships (`geo` = map key for REGIONAL) | all |
| `dim_date` | dimension | ALL | 1 / calendar day | date table + recession shading | all |
| `market_calendar` | dimension | ALL | 1 / calendar × day | date table per market | 3, 5, 11 |
| `macro_indicator_dashboard` | fact | ECON | 1 / series (latest) | KPI grid | 1 |
| `macro_indicator_sparkline` | fact | ECON | 1 / series × point | sparkline small multiples | 1 |
| `macro_category_summary` | fact | ECON | 1 / category | breadth bar / cards | 1 |
| `inflation_explorer` | fact | INFL | 1 / item × month | decomposition tree | 2 |
| `inflation_contribution` | fact | INFL | 1 / item × month | waterfall | 2 |
| `treasury_curve` | fact | CURV | 1 / date × tenor | line over tenor | 3 |
| `treasury_curve_metrics` | fact | CURV | 1 / date | line + recession shading | 3 |
| `curve_spread_daily` | fact | CURV | 1 / spread × date | line + zero line | 4 |
| `spread_inversion_episode` | fact | CURV | 1 / spread × episode | Gantt bands | 4 |
| `curve_spread_rolling` | fact | CURV | 1 / spread × date × window | line + window slicer | 4 |
| `treasury_curve_rolling` | fact | CURV | 1 / tenor × date × window | line + window slicer | 3 |
| `yield_curve_ns_factors` | fact | CURV | 1 / date | 3-panel factor series | 3 |
| `benchmark_rate_board` | fact | BMRK | 1 / rate (latest) | board with trend arrows | 5, 6 |
| `funding_tape_daily` | fact | FUND | 1 / metric × date | faceted lines | 5 |
| `funding_stress_daily` | fact | FUND | 1 / date | gauge + bucket bands | 5 |
| `credit_spread_daily` | fact | CRDT | 1 / instrument × date | line + stress markers | 7 |
| `credit_spread_rolling` | fact | CRDT | 1 / instrument × date × window | line + window slicer | 7 |
| `macro_regime_daily` | fact | REGIME | 1 / date | regime ribbon + pillars | 8 |
| `fred_series_zscore_rolling` | fact | STAT | 1 / series × date × window | multi-window fan chart | 9 |
| `zscore_heatmap` | fact | STAT | 1 / series × date | heatmap / fan chart | 8, 12 |
| `series_correlation` | fact | STAT | 1 / pair × window × date | heatmap / rolling line | 9 |
| `series_lead_lag` | fact | EDA | 1 / pair × lag | CCF bars + Granger cards | 9 |
| `series_structural_breaks` | fact | EDA | 1 / pair × test_type | break scatter | 9 |
| `fomc_probability` | fact | FOMC | 1 / meeting × bucket | stacked probability bars | 6 |
| `fomc_meeting_path` | fact | FOMC | 1 / meeting | implied-path line | 6 |
| `global_inflation` | fact | GCPI | 1 / country × print | map / heat table | 10 |
| `global_policy_rates` | fact | GPOL | 1 / country × print | board by region | 10 |
| `equity_return_daily` | fact | EQUITY | 1 / ticker × date | line / return heatmap | 11 |
| `index_constituents` | fact | EQUITY | 1 / ETF × constituent × snapshot | weight treemap | 11 |
| `equity_total_return_index` | fact | EQUITY | 1 / ticker × date | TR vs PR line | 11 |
| `equity_price_reconciliation` | reference | EQUITY | 1 / ticker × date | divergence scatter | 11 |
| `realized_volatility` | fact | EQUITY | 1 / ticker × date × window | line + window slicer | 11 |
| `equity_factor_attribution` | fact | EQUITY | 1 / ticker × factor × window × date | exposure heatmap | 11 |
| `fred_latest_observation` | fact | CORE | 1 / series × date | generic line | 12 |
| `fred_feature_transforms` | fact | CORE | 1 / series × date | generic line | 1, 9, 12 |
| `powerbi_catalog` | reference | ALL | 1 / Gold object | documentation page | 14 |
| `release_calendar` | fact | CAL | 1 / release × date | forward calendar | 1 |
| `ml_feature_matrix` | fact | ML | 1 / date × feature | table / heatmap | 8 |
| `macro_factor_scores` | fact | ML | 1 / date × factor | factor score lines | 8 |
| `macro_factor_loadings` | fact | ML | 1 / date × factor × feature | loadings heatmap | 8 |
| `macro_anomaly_scores` | fact | ML | 1 / date | line + anomaly markers | 8 |
| `recession_probability_daily` | fact | ML | 1 / date | probability fan chart | 8 |
| `equity_factor_implied_return` | fact | ML | 1 / ticker × window × month | implied vs realized scatter | 11 |
| `inflation_forecast` | fact | ML | 1 / series × horizon × model | fan chart | 2 |

**Objects deliberately unbound in v1:** `fred_company_fundamentals`,
`fred_company_ratios`, `v_company_ratio_ranks` (D2 — SEC out of scope);
`fred_macro_feature_daily`, `fred_curve_spread`, `fred_cross_series_feature*`
(superseded for reporting by the terminal views, except in #13);
`v_latest_revised`, `v_series_latest_value` (Silver-backed convenience views).

---

## Appendix B — Units and semantics quick reference

The single highest-risk area for a report author. Check against this before
formatting any measure.

| Column pattern | Unit | Notes |
|---|---|---|
| `*_pct` on `curve_spread_daily[value]`, `treasury_curve[yield_pct]`, `credit_spread_daily[oas_pct]` | percent (4.25 = 4.25%) | |
| `*_bps` | basis points | `value_bps`, `oas_bps`, `change_bps`, `trough_bps` |
| `inflation_explorer[mom_pct]` / `[yoy_pct]` | **decimal fraction** (0.003 = 0.3%) | multiply by 100 to display |
| `inflation_*[contribution_pp]` | percentage points | already display-ready |
| `inflation_forecast[forecast_value]`, `lower_*`, `upper_*` | **MoM decimal fraction** | |
| `global_inflation[cpi_yoy_pct]`, `[vs_target_pp]` | percent / percentage points | |
| `treasury_curve_rolling[change]` | **percentage points** | ×100 for bps |
| `credit_spread_rolling[change_bps]` | basis points | this table is bps-only |
| `curve_spread_rolling[change]` | parent table's native units | percent for spreads |
| `equity_*[price_return]`, `[total_return]` | decimal fraction | |
| `realized_volatility[realized_vol_pct]` | percent, annualised | from **log** returns |
| `*[percentile]` | 0–1 | ×100 to display |
| `*[zscore]` | standard deviations | expanding = PIT-safe; rolling = trailing-w |
| `macro_regime_daily[*_score]` | z-score units | higher composite = friendlier |
| `funding_stress_daily[stress_score]` | 0–100 | `clamp(50 + 20 × composite_z, 0, 100)` |
| `*[is_recession]`, `[recession_overlap]` | nullable boolean | **NULL ≠ FALSE** |
| `window` columns | **observations**, not calendar days | 1/5/10/21/63/126/252 ≈ day…year |

---

## Appendix C — Environment sizing query

Run before building any model, against the target environment.

```sql
-- Databricks. Swap to gold_<table> names for the SQLite mirror.
SELECT 'macro_indicator_dashboard' AS obj, COUNT(*) AS rows FROM macro_prod.gold.macro_indicator_dashboard
UNION ALL SELECT 'inflation_explorer',        COUNT(*) FROM macro_prod.gold.inflation_explorer
UNION ALL SELECT 'treasury_curve',            COUNT(*) FROM macro_prod.gold.treasury_curve
UNION ALL SELECT 'curve_spread_daily',        COUNT(*) FROM macro_prod.gold.curve_spread_daily
UNION ALL SELECT 'curve_spread_rolling',      COUNT(*) FROM macro_prod.gold.curve_spread_rolling
UNION ALL SELECT 'credit_spread_daily',       COUNT(*) FROM macro_prod.gold.credit_spread_daily
UNION ALL SELECT 'funding_tape_daily',        COUNT(*) FROM macro_prod.gold.funding_tape_daily
UNION ALL SELECT 'macro_regime_daily',        COUNT(*) FROM macro_prod.gold.macro_regime_daily
UNION ALL SELECT 'series_correlation',        COUNT(*) FROM macro_prod.gold.series_correlation
UNION ALL SELECT 'series_lead_lag',           COUNT(*) FROM macro_prod.gold.series_lead_lag
UNION ALL SELECT 'global_inflation',          COUNT(*) FROM macro_prod.gold.global_inflation
UNION ALL SELECT 'equity_total_return_index', COUNT(*) FROM macro_prod.gold.equity_total_return_index
UNION ALL SELECT 'realized_volatility',       COUNT(*) FROM macro_prod.gold.realized_volatility
UNION ALL SELECT 'fred_feature_transforms',   COUNT(*) FROM macro_prod.gold.fred_feature_transforms
UNION ALL SELECT 'fred_latest_observation',   COUNT(*) FROM macro_prod.gold.fred_latest_observation
UNION ALL SELECT 'fred_point_in_time',        COUNT(*) FROM macro_prod.gold.fred_point_in_time
ORDER BY rows DESC;
```

Anything above ~10M rows gets incremental refresh or a series restriction before
a model is built on it.

---

## Appendix D — Workspace and deployment layout

```
Workspace: Macro Analytics [DEV]     ← authoring, dev SQLite or macro_dev
Workspace: Macro Analytics [TEST]    ← macro_test, validation
Workspace: Macro Analytics [PROD]    ← macro_prod, Tier A reports
Workspace: Macro Analytics [PROD-INTERNAL]  ← Tier B: #11 Equity, #10 Global (until G4)
```

- Deployment pipeline DEV → TEST → PROD with a parameter rule swapping
  `p_Backend` / `p_Catalog` per stage.
- Tier B reports deploy to PROD-INTERNAL, which has external sharing disabled at
  the workspace level and a sensitivity label applied.
- Report naming: `MACRO — <Report Name>` (e.g. `MACRO — Treasury Curve Lab`) so
  the suite groups alphabetically in the Service.
- Every report's About page links to #14's catalog page.

---

## Appendix E — Open questions for the report author

Answer these during phase 0; they do not block the spec.

1. **Refresh timing** — what time does the daily pipeline run actually finish?
   Read `audit.etl_run.ended_at` over a fortnight before fixing the schedule.
2. **Row-level security** — none is specified. If any audience should see only a
   subset (e.g. external recipients restricted to a country or category), that
   is a model-level RLS design and needs to be decided before phase 5.
3. **Series restriction lists** — reports #9 and #13 need an explicit list of
   which series a researcher backtests. Without one, both models import the full
   universe.
4. **Alerting** — should #14's `[Trust Verdict]` drive a Power BI data alert or
   a Teams/email notification? The pipeline already has its own notification
   path (`fred_pipeline.governance.notify`); decide which owns operational
   alerting so it does not happen twice.
5. **Historical depth** — 10 years is assumed in §6.2. Confirm against what
   researchers actually use; several series have far longer history and the
   import cost is real.
