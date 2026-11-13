# Power BI Report Suite — Build Plan, Source Queries & Readiness

**Companion to** [`powerbi_report_suite.md`](powerbi_report_suite.md), which
defines the suite's conventions, page structures, and DAX. This document is the
buildable layer: **the source query for every table in every model, the model
specification (relationships, storage, hidden fields, sort-by), and a verified
readiness verdict per report.**

**Readiness is evidence-based.** Every "ready" and "gated" verdict below was
produced by resolving each Gold table's driving config against the series
actually active in `manifests/*.yml`. Appendix A is the audit script — re-run it
after any manifest or config change and this document's verdicts with it.

---

## 1. Readiness at a glance

| # | Report | Status | Gate |
|---|---|---|---|
| 1 | Macro Cockpit | 🟢 **Ready** | — (release calendar needs a networked `run`) |
| 2 | Inflation Explorer | 🟢 **Ready** | — all three item trees fully ingested |
| 3 | Treasury Curve Lab | 🟢 **Ready** | — 11/11 tenors |
| 4 | Spreads & Inversions | 🟢 **Ready** | — 9 spreads, all legs present |
| 5 | Funding & Liquidity | 🟢 **Ready** | G-A fixed; gauge components 4/4, tape 10/10, board 17/17 |
| 6 | Fed Policy Watch | 🟢 **Ready** | — anchors + 4 forward tenors present |
| 7 | Credit Conditions | 🟢 **Ready** | — 9/9 OAS instruments |
| 8 | Regime & Recession Risk | 🟢 **Ready** | probit regains its `funding_stress` feature now G-A is fixed |
| 9 | Statistical Lab | 🟢 **Ready** | — 11/11 pair series |
| 10 | Global Macro Monitor | 🟡 **Ready, non-commercial** | data complete (38/38 + 38/38); **G-B**: BIS terms permit non-commercial redistribution with attribution, not commercial |
| 11 | Equity & Factor Analytics | 🟡 **Ready, one page gated** | **G-C**: reconciliation page needs Stooq; all other pages populate from Tiingo |
| 12 | Regional & State Map | 🟢 **Ready** | — 120 REGIONAL catalog entries with `geo` |
| 13 | PIT & Revisions Lab | 🟢 **Ready** | — vintage capture on by default |
| 14 | Pipeline Health | 🟢 **Ready** | — audit/meta populate on every run |

**Twelve of fourteen fully ready. Two constrained, neither by data:** Report 10
by BIS licensing (G-B) and Report 11's single QA page by Stooq being inactive
(G-C). Nothing is blocked.

### Corrections to the earlier spec's gap list

Auditing turned up three claims in `powerbi_report_suite.md` §8 that were
inherited from the README rather than checked against the manifests:

| Claim | Reality |
|---|---|
| G2: "CPI/PCE item trees depend on inactive manifests" | **Wrong.** `bls_cpi_basket.yml` (30), `bls_cpi_basket_sa.yml` (29), and `bea_pce_items.yml` (21) are all active. All three trees resolve 100%: CPI/NSA 30/30, CPI/SA 29/29, PCE/SA 23/23. Report 2 is fully ready. |
| G3: "Stooq inactive blocks `gold.equity_return_daily`" | **Wrong.** `select_canonical_equity_price_rows` falls back to Tiingo `adjClose` for any ticker with no Stooq close, so `equity_return_daily`, `realized_volatility`, and factor attribution all populate from Tiingo's 85 series. Only `equity_price_reconciliation` genuinely needs both sources. |
| G1: "`dim_series` covers 67 series" | **Superseded.** Now 254, including 120 REGIONAL with `geo`. |

Those corrections are applied in this document; the spec's §8 has been updated
to match.

---

## 2. The three real gates

### G-A — Funding stress gauge emitted zero rows (`TGCR` id mismatch) ✅ Fixed

**The defect.** `config/funding.yml` and `config/benchmark_rates.yml` referenced
a series called **`TGCR`**, while `manifests/fed_funding.yml` declared the
Tri-Party General Collateral Rate under FRED's actual id, **`TGCRRATE`**.
Nothing joined them, so the `TGCR` tape row was empty, the `SOFR_TGCR` spread
never computed, and because `funding_stress_daily` emits a row **only on dates
where every component spread has a value**, the entire 0–100 stress gauge
produced no rows. `funding_stress` is also a configured feature of the recession
probit (`config/recession_model.yml`), so Report 8 silently ran on a reduced
feature set.

**The resolution.** The *manifest* was right and the *configs* were wrong —
the opposite of this document's first guess. FRED publishes the repo reference
rates with a `RATE` suffix: the bare `TGCR`/`BGCR` strings are prefixes of the
percentile and volume companions (`TGCR25THPERCENTILE`, `TGCRVOLUME`), not the
rate series. Fixed by pointing both configs at `TGCRRATE`:

- `config/funding.yml` — the `TGCR` metric's `series_id`, and the `SOFR_TGCR`
  spread's `short_leg` (spread legs are series ids, not the `name` labels).
- `config/benchmark_rates.yml` — the `TGCR` board row.

Stress-gauge components now resolve **4/4** and `funding_stress_daily` emits.

**`BGCR` — declared, still inactive.** It was referenced by both configs with no
manifest entry at any status. It is now declared and **active** as
**`BGCRRATE`** in `manifests/fed_funding.yml`, completing the funding tape at
10/10 metrics and the benchmark board at 17/17 rates.

The id follows the same RATE-suffix convention as the confirmed `TGCRRATE` but
could not be checked live (egress to `fred.stlouisfed.org` is blocked from the
authoring environment). If it is wrong, the first run records one failed series
in `audit.etl_series_run` and affects nothing else — **check there after the
next run**. `BGCR` is not a stress-gauge component, so the gauge is unaffected
either way.

**Prevention — shipped.** `tests/test_config_manifest_integrity.py` now
cross-checks every series id referenced by a Gold config against the manifests:

- `test_every_config_series_is_declared_in_a_manifest` fails on an id no
  manifest declares (the typo/rename case), across curve, spreads, funding,
  benchmark rates, credit, regime, stats pairs, ML features, inflation items,
  and FOMC configs.
- `test_funding_stress_gauge_components_are_resolvable` guards the gauge's
  components specifically, and fails on *inactive* legs too — one unresolvable
  leg empties the whole gauge, so "declared but not ingested" is also a failure
  there.

Both were confirmed to fail when the `TGCR` spelling is reintroduced. The tests
deliberately tolerate declared-but-inactive ids elsewhere: shipping a series
inactive is a coverage decision, while naming an id that exists nowhere is
always a defect.

### G-B — BIS data is non-commercial 🟡 Registered

`bis` now has a `config/data_licensing.yml` entry, so the register no longer
reports it as unreviewed. The answer it records is a real constraint rather than
a clean bill of health:

- The BIS **retains copyright** in its statistical data.
- Its published terms permit download, display and **redistribution without
  written permission for non-commercial purposes**.
- **Commercial use is not covered by that grant** and needs BIS permission.
- **Attribution is a copyright condition**, not a courtesy — unlike the US
  federal sources, closer to World Bank CC BY.

So Report 10 is fine for internal and non-commercial use with a BIS attribution
on the About page, and needs permission before it becomes part of anything
commercially distributed. `validate --commercial` still flags it — correctly
now, with `license_type='attribution-noncommercial'` rather than "unreviewed",
alongside tiingo and ishares.

The entry was written from the BIS's published terms page, which could not be
fetched directly (egress to bis.org is blocked from the authoring environment).
Re-read `terms_url` and bump `last_reviewed_date` before making a real
compliance decision, particularly before any commercial distribution.

### G-C — Stooq inactive 🟡

`manifests/equity_stooq.yml` ships fully inactive (89 entries). Its only
report-visible consequence is Report 11's hidden reconciliation page:
`equity_price_reconciliation` emits rows only where both Stooq and Tiingo carry
a value for the same (ticker, date). Every other Report 11 page populates from
Tiingo. Activate Stooq when you want the cross-source price check.

---

## 3. Query conventions

Every query below is written for **Databricks**. Two mechanical substitutions
cover the other backend and the parameter layer:

| Databricks | SQLite mirror |
|---|---|
| `${p_Catalog}.gold.macro_indicator_dashboard` | `gold_macro_indicator_dashboard` |
| `${p_Catalog}.meta.series_staleness` | `meta_series_staleness` |
| `${p_Catalog}.audit.etl_run` | `audit_etl_run` |

In the model these are not written as raw SQL strings — they go through
`fnGold(schema, table)` from the spec's §3.3, which resolves both backends. The
SQL here is the **contract**: the columns, filters, and grain each query must
deliver. Where a query is a plain projection, implement it as `fnGold` plus
Power Query column selection so it folds; where it has a `WHERE` clause, add the
filter as a folded Power Query step.

**Two rules that apply to every query:**

1. **Never `SELECT *`.** Import only the columns the report binds. Unused
   columns cost memory in every model and this suite has fourteen of them.
2. **Filter at the source, not in a visual.** Every `WHERE` below is load-time.

---

## 4. Per-report build specifications

---

### Report 1 — Macro Cockpit 🟢 Ready

**Readiness detail.** Catalog resolves 254 entries, all active. All 10 configured
releases in `config/release_calendar.yml` have a present representative series.
One operational caveat: `gold.release_calendar` is written by
`FredPipeline.run()` against a **live FRED releases/dates fetch**, not by
`build_gold()` — a `gold`-only rebuild will not refresh it, and an offline run
leaves it stale. `fetched_at` stamps staleness; surface it on page 4.

**Source queries**

```sql
-- qMacroDashboard : 1 row per national catalog series (latest observation)
SELECT series_id, econ_category, polarity, default_transform,
       as_of_date, latest_date, latest_value, prior_date, prior_value,
       change_abs, change_pct, yoy_pct, zscore, percentile,
       surprise, surprise_z, direction_is_good,
       spark_min, spark_max, staleness_days, realtime_start
FROM   ${p_Catalog}.gold.macro_indicator_dashboard
WHERE  econ_category <> 'REGIONAL';   -- states belong to Report 12

-- qSparkline : last 36 points per series, national only
SELECT s.series_id, s.point_index, s.observation_date, s.value
FROM   ${p_Catalog}.gold.macro_indicator_sparkline s
JOIN   ${p_Catalog}.gold.dim_series d ON d.series_id = s.series_id
WHERE  d.econ_category <> 'REGIONAL';

-- qCategorySummary : breadth + surprise index per national category
SELECT econ_category, as_of_date, n_series, n_improving, n_deteriorating,
       breadth_pct, avg_zscore, surprise_index
FROM   ${p_Catalog}.gold.macro_category_summary
WHERE  econ_category <> 'REGIONAL';

-- qReleaseCalendar : forward-looking economic calendar
SELECT release_id, release_name, release_date, importance, econ_category,
       representative_series_id, is_future, fetched_at
FROM   ${p_Catalog}.gold.release_calendar;

-- qDimSeries : national slice of the star-schema hub
SELECT series_id, title, source, frequency, units, econ_category,
       polarity, default_transform, scale, decimals, notes
FROM   ${p_Catalog}.gold.dim_series
WHERE  econ_category <> 'REGIONAL';

-- qDimDate : kernel query, unfiltered
SELECT date, date_key, year, quarter, quarter_label, year_quarter,
       year_quarter_sort, month, month_name, month_short_name, year_month,
       year_month_sort, week_of_year, year_week, day_name, day_short_name,
       is_weekday, is_month_end, is_quarter_end, is_year_end,
       fiscal_year, fiscal_year_label, fiscal_quarter, fiscal_quarter_label,
       is_recession
FROM   ${p_Catalog}.gold.dim_date;
```

**Data model**

| Query | Grain | Storage | Role |
|---|---|---|---|
| `qDimSeries` | 1 / series (134) | Import | dimension |
| `qDimDate` | 1 / day | Import | date table |
| `qMacroDashboard` | 1 / series | Import | snapshot fact |
| `qSparkline` | 1 / series × point | Import | detail fact |
| `qCategorySummary` | 1 / category (10) | Import | aggregate fact |
| `qReleaseCalendar` | 1 / release × date | Import | calendar fact |

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimSeries[series_id]` | `qMacroDashboard[series_id]` | 1:1 | single | ✅ |
| `qDimSeries[series_id]` | `qSparkline[series_id]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qReleaseCalendar[release_date]` | 1:* | single | ✅ |

**Deliberately unrelated:** `qMacroDashboard` is *not* joined to `qDimDate`. Its
`latest_date` differs per series (daily yields vs quarterly GDP); a date filter
would blank most of the board. `qCategorySummary` joins to nothing — it is
filtered by its own `econ_category` slicer.

**Model settings**
- Mark `qDimDate` as the date table on `[date]`.
- Sort-by: `month_short_name` → `month`, `quarter_label` → `quarter`,
  `year_month` → `year_month_sort`, `day_short_name` → `day_of_week_iso`.
- Hide from report view: all `*_sort` columns, `polarity`, `default_transform`,
  `realtime_start`, `point_index`.
- Format: `change_pct` / `yoy_pct` / `breadth_pct` as percent; `zscore` 2dp;
  `percentile` as percent; `staleness_days` integer.

---

### Report 2 — Inflation Explorer 🟢 Ready

**Readiness detail.** All three configured item trees resolve completely —
**CPI/NSA 30/30, CPI/SA 29/29, PCE/SA 23/23** — from the active
`bls_cpi_basket.yml`, `bls_cpi_basket_sa.yml`, and `bea_pce_items.yml`. Both
forecast series (`CPIAUCSL`, `PCEPI`) are present. This supersedes the earlier
"depends on inactive manifests" claim.

One caveat that is a labelling requirement, not a gate: the PCE waterfall's
contribution weights are **approximate nominal-PCE expenditure shares**, not
BEA's published relative importances. Say so on the page.

**Source queries**

```sql
-- qInflationExplorer : item × month, full hierarchy
SELECT series_id, item_label, parent_item, hierarchy_level, basket, sa_nsa,
       observation_date, index_value, mom_pct, yoy_pct, mom_accel, yoy_accel,
       three_month_annualized, weight, contribution_pp
FROM   ${p_Catalog}.gold.inflation_explorer;

-- qInflationContribution : ranked waterfall items + the headline total bar
SELECT observation_date, basket, sa_nsa, series_id, item_label,
       contribution_pp, rank_in_month, is_headline_total
FROM   ${p_Catalog}.gold.inflation_contribution;

-- qInflationForecast : AR/VAR fan chart inputs
SELECT series_id, forecast_date, horizon_months, forecast_value,
       lower_80, upper_80, lower_95, upper_95,
       model_type, lag_order, model_vintage, n_obs_training
FROM   ${p_Catalog}.gold.inflation_forecast;

-- qItemTree : disconnected hierarchy helper for the decomposition tree
SELECT DISTINCT series_id, item_label, parent_item, hierarchy_level,
       basket, sa_nsa
FROM   ${p_Catalog}.gold.inflation_explorer;
```

**Data model**

| Query | Grain | Storage | Role |
|---|---|---|---|
| `qInflationExplorer` | 1 / item × month | Import | primary fact |
| `qInflationContribution` | 1 / item × month | Import | waterfall fact |
| `qInflationForecast` | 1 / series × horizon × model | Import | forecast fact |
| `qItemTree` | 1 / item | Import | hierarchy dimension |
| `qDimDate` | 1 / day | Import | date table |

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimDate[date]` | `qInflationExplorer[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qInflationContribution[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qInflationForecast[forecast_date]` | 1:* | single | ✅ |
| `qItemTree[series_id]` | `qInflationExplorer[series_id]` | 1:* | single | ✅ |
| `qItemTree[series_id]` | `qInflationContribution[series_id]` | 1:* | single | ✅ |

**Model settings**
- `basket` and `sa_nsa` slicers are **single-select with defaults** (CPI, SA).
  Mixing baskets or SA/NSA in one visual is meaningless.
- On the waterfall, filter `is_headline_total = FALSE` for the item stack and
  read the total separately — including it in the stack double-counts.
- Format `mom_pct` / `yoy_pct` / `forecast_value` and CI bounds as **percent
  from a decimal fraction** (Gold stores 0.003 = 0.3%); `contribution_pp` as a
  plain 2dp number labelled "pp".
- Hide `hierarchy_level`, `rank_in_month`, `lag_order`, `n_obs_training`.

---

### Report 3 — Treasury Curve Lab 🟢 Ready

**Readiness detail.** All **11/11** tenors in `config/curve.yml` resolve
(`DGS1MO` … `DGS30`, including `DGS3`, `DGS7`, `DGS20`, which the config comment
described as pending). The curve renders complete.

**Source queries**

```sql
-- qTreasuryCurve : tidy curve, one row per date × tenor
SELECT as_of_date, tenor_label, tenor_months, series_id, yield_pct
FROM   ${p_Catalog}.gold.treasury_curve;

-- qCurveMetrics : per-date level/slope/curvature + move classification
SELECT as_of_date, level, slope_10y2y, slope_10y3m, curvature_2_5_10,
       butterfly_2_10_30, is_inverted_10y2y, is_inverted_10y3m,
       is_recession, curve_move
FROM   ${p_Catalog}.gold.treasury_curve_metrics;

-- qCurveRolling : multi-horizon momentum (change is in PERCENTAGE POINTS)
SELECT tenor_label, tenor_months, series_id, observation_date, window,
       yield_pct, change, pct_change, zscore
FROM   ${p_Catalog}.gold.treasury_curve_rolling;

-- qNSFactors : Nelson-Siegel fit -- valid fits only
SELECT observation_date, beta0, beta1, beta2, lambda, lambda_estimated,
       fit_rmse, n_tenors, fit_valid
FROM   ${p_Catalog}.gold.yield_curve_ns_factors
WHERE  fit_valid = true;

-- qMarketCalendar : FEDWIRE business-day dimension, not a settlement schedule
SELECT calendar_date, is_business_day, day_type, holiday_name,
       prior_business_day, next_business_day,
       is_last_business_day_of_month, is_last_business_day_of_quarter
FROM   ${p_Catalog}.gold.market_calendar
WHERE  calendar_name = 'FEDWIRE';
```

The legacy `t2_settle_date` field is intentionally excluded: it is calendar
arithmetic and is not a verified settlement date. See the
[settlement-evidence contract](../dictionary/settlement_evidence_contract.md)
for its narrow source-backed historical scope and unavailable states.

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimDate[date]` | `qTreasuryCurve[as_of_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qCurveMetrics[as_of_date]` | 1:1 | single | ✅ |
| `qDimDate[date]` | `qCurveRolling[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qNSFactors[observation_date]` | 1:1 | single | ✅ |
| `qDimDate[date]` | `qMarketCalendar[calendar_date]` | 1:1 | single | ✅ |

**Model settings**
- **`calendar_name` filter is mandatory.** `market_calendar` is long across three
  calendars; without the filter the join fans out 3× and every measure triples.
- Sort-by: `tenor_label` → `tenor_months` (otherwise the curve axis reads
  "10Y, 1M, 1Y, 20Y…" alphabetically — the single most common defect in a curve
  report).
- `window` slicer defaults to a single value (21) to keep the rolling visual
  readable.
- Label `change` as percentage points; ×100 only where a bps axis is intended.

---

### Report 4 — Spreads & Inversions 🟢 Ready

**Readiness detail.** 9 spreads configured in `config/spreads.yml`
(T10Y2Y, T10Y3M, T2Y3M, T30Y10Y, T10Y5Y, T5Y2Y, T10Y1Y, T30Y2Y, T5Y3M); all 6
distinct legs are active.

**Source queries**

```sql
-- qCurveSpreadDaily : configured spreads with PIT z-score/percentile
SELECT spread_name, observation_date, long_leg, short_leg, value, value_bps,
       zscore, percentile, is_inverted, inversion_run, is_recession
FROM   ${p_Catalog}.gold.curve_spread_daily;

-- qInversionEpisode : one row per spread × episode
SELECT spread_name, long_leg, short_leg, episode_number, start_date, end_date,
       last_inverted_date, observation_count, calendar_days,
       trough_value, trough_bps, trough_date, is_ongoing, recession_overlap
FROM   ${p_Catalog}.gold.spread_inversion_episode;

-- qSpreadRolling : multi-horizon momentum
SELECT spread_name, observation_date, window, value, change, pct_change, zscore
FROM   ${p_Catalog}.gold.curve_spread_rolling;

-- qSpreadDim : disconnected spread dimension (drives slicers)
SELECT DISTINCT spread_name, long_leg, short_leg
FROM   ${p_Catalog}.gold.curve_spread_daily;
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimDate[date]` | `qCurveSpreadDaily[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qSpreadRolling[observation_date]` | 1:* | single | ✅ |
| `qSpreadDim[spread_name]` | `qCurveSpreadDaily[spread_name]` | 1:* | single | ✅ |
| `qSpreadDim[spread_name]` | `qSpreadRolling[spread_name]` | 1:* | single | ✅ |
| `qSpreadDim[spread_name]` | `qInversionEpisode[spread_name]` | 1:* | single | ✅ |

**`qInversionEpisode` is deliberately not joined to `qDimDate`.** Its grain is an
episode spanning a date range, not a point in time; a date relationship on
`start_date` would silently filter out every episode that began before the
selected window. Drive it from the `spread_name` slicer and render
`start_date` → `end_date` as a bar.

**Model settings**
- If any `config/spreads.yml` entry uses `op: ratio`, exclude it from the
  inversion visuals — a ratio has no zero line, and `is_inverted` /
  `inversion_run` / episodes are spread-only by construction.
- `end_date` is NULL for ongoing episodes; render as an open-ended bar to
  `last_inverted_date`, never as "ends today".
- `recession_overlap` is nullable — NULL means USREC absent, not "no overlap".

---

### Report 5 — Funding & Liquidity 🟢 Ready

**Readiness detail.**

| Component | Status |
|---|---|
| Funding tape metrics | 10/10 |
| Funding spread legs | 4/4 |
| **Stress gauge components** | **4/4 — all resolvable** |
| **`funding_stress_daily`** | **emits** |
| Benchmark rate board | 17/17 |

G-A is fixed (§2): the configs now reference `TGCRRATE`, FRED's real id for the
Tri-Party General Collateral Rate, so the `SOFR_TGCR` spread computes and the
gauge populates.

`BGCRRATE` is now active, completing the tape at 10/10 and the board at 17/17.
Its id was inferred from the confirmed `TGCRRATE` convention rather than checked
live (egress blocked), so **confirm it in `audit.etl_series_run` after the first
run** — a wrong id shows up there as one failed series and nothing else.

**Source queries**

```sql
-- qFundingTape : corridor rates, balances, spreads (metric_type discriminates)
SELECT metric_name, metric_type, observation_date, value, zscore, percentile
FROM   ${p_Catalog}.gold.funding_tape_daily;

-- qFundingStress : the 0-100 gauge. Populates now that G-A is fixed.
SELECT observation_date, composite_z, stress_score, stress_bucket, n_components
FROM   ${p_Catalog}.gold.funding_stress_daily;

-- qBenchmarkBoard : one row per configured rate at its latest observation
SELECT series_id, rate_label, rate_category, benchmark_series, as_of_date,
       latest_date, latest_value, prior_value, change_bps, trend,
       spread_to_benchmark_bps, zscore, percentile, regime, staleness_days
FROM   ${p_Catalog}.gold.benchmark_rate_board;

-- qMarketCalendar : FEDWIRE
SELECT calendar_date, is_business_day, day_type, holiday_name
FROM   ${p_Catalog}.gold.market_calendar
WHERE  calendar_name = 'FEDWIRE';
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimDate[date]` | `qFundingTape[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qFundingStress[observation_date]` | 1:1 | single | ✅ |
| `qDimDate[date]` | `qMarketCalendar[calendar_date]` | 1:1 | single | ✅ |

`qBenchmarkBoard` is a latest-snapshot fact — unrelated to `qDimDate`, same
reasoning as Report 1's dashboard.

**Model settings**
- Facet the tape by `metric_type`; rates (percent) and balances ($ billions/
  trillions) must never share an axis.
- Surface `n_components` next to the gauge — it is the honest indicator of
  whether the composite is complete, and post-fix it should read 3.
- Gauge bands: <40 calm, 40–60 normal, 60–80 elevated, ≥80 stressed.
- Gaps in the gauge line are correct (incomplete component set on that date).
  Never interpolate.

---

### Report 6 — Fed Policy Watch 🟢 Ready

**Readiness detail.** All three anchors present (`DFEDTARL`, `DFEDTARU`, `EFFR`)
plus all four forward tenors (`DGS1MO`, `DGS3MO`, `DGS6MO`, `DGS1`).
`config/fomc.yml` carries 12 meeting dates verified against the Fed's calendar
as of 2026-07-17 — **refresh that list as the Fed publishes further out**, or
the implied path silently runs out of meetings.

**Source queries**

```sql
-- qFomcProbability : meeting × 25bp outcome bucket
SELECT meeting_date, target_lower_bps, target_upper_bps, outcome_bps,
       probability, model_vintage, n_inputs
FROM   ${p_Catalog}.gold.fomc_probability;

-- qFomcPath : probability-weighted expected rate per meeting
SELECT meeting_date, implied_rate, implied_move_bps, cumulative_move_bps,
       model_vintage
FROM   ${p_Catalog}.gold.fomc_meeting_path;

-- qPolicyContext : current policy rates for the header cards
SELECT series_id, rate_label, latest_date, latest_value, change_bps, trend
FROM   ${p_Catalog}.gold.benchmark_rate_board
WHERE  rate_category = 'policy';

-- qMeetingDim : disconnected meeting dimension (meetings are not calendar days
-- in any useful sense for slicing -- there are 8 a year)
SELECT DISTINCT meeting_date FROM ${p_Catalog}.gold.fomc_meeting_path;
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qMeetingDim[meeting_date]` | `qFomcProbability[meeting_date]` | 1:* | single | ✅ |
| `qMeetingDim[meeting_date]` | `qFomcPath[meeting_date]` | 1:1 | single | ✅ |

**No `dim_date` relationship.** Meetings are 8 discrete events a year; a full
date table adds nothing and makes the axis mostly empty. `qMeetingDim` is the
axis.

**Model settings**
- `model_vintage` on every page — this is a model output recomputed each run.
- Standing methodology note (Overview + About): *"Derived from Treasury
  short-rate forwards (DFEDTARL/DFEDTARU/EFFR, DGS1MO–DGS1). Not CME FedWatch."*
- `outcome_bps` sorted ascending on the y-axis so the ladder reads in rate order.
- Validate `SUM(probability) ≈ 1.00` per `meeting_date` as a load-time check.

---

### Report 7 — Credit Conditions 🟢 Ready

**Readiness detail.** All **9/9** instruments in `config/credit.yml` are active
via `manifests/ice_credit.yml` (IG, HY, AAA, AA, A, BBB, BB, B, CCC).

**Source queries**

```sql
-- qCreditSpreadDaily : OAS history per instrument, percent AND bps
SELECT instrument, series_id, category, observation_date, oas_pct, oas_bps,
       change_bps, zscore, percentile, is_stress_episode, is_recession
FROM   ${p_Catalog}.gold.credit_spread_daily;

-- qCreditRolling : multi-horizon momentum -- BPS ONLY in this table
SELECT instrument, series_id, observation_date, window,
       oas_bps, change_bps, pct_change, zscore
FROM   ${p_Catalog}.gold.credit_spread_rolling;

-- qInstrumentDim : rating-ordered dimension for the credit curve axis
SELECT DISTINCT instrument, series_id, category
FROM   ${p_Catalog}.gold.credit_spread_daily;
```

Add a **rating sort order** column in Power Query on `qInstrumentDim` — the
credit curve must render AAA → AA → A → BBB → BB → B → CCC, which is neither
alphabetical nor the config order:

```
IG_OAS 0, HY_OAS 1, AAA_OAS 2, AA_OAS 3, A_OAS 4, BBB_OAS 5,
BB_OAS 6, B_OAS 7, CCC_OAS 8
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimDate[date]` | `qCreditSpreadDaily[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qCreditRolling[observation_date]` | 1:* | single | ✅ |
| `qInstrumentDim[instrument]` | `qCreditSpreadDaily[instrument]` | 1:* | single | ✅ |
| `qInstrumentDim[instrument]` | `qCreditRolling[instrument]` | 1:* | single | ✅ |

**Model settings**
- Sort-by: `instrument` → `rating_sort`.
- Use `oas_bps` everywhere; keep `oas_pct` hidden. Mixing the two on one page is
  the most likely unit error in this report.
- Load-time sanity check: HY_OAS > IG_OAS on every date. A violation is a data
  defect worth raising upstream, not something to filter away.

---

### Report 8 — Regime & Recession Risk 🟢 Ready

**Readiness detail.** All five regime pillars resolve **3/3** inputs
(growth, inflation, liquidity, credit, policy) — the config's "(inactive)"
comments on `BAMLH0A0HYM2` and `EFFR` are stale; both are active now.
`ml_features.yml` resolves **12/12**. `USREC` and `USRECD` are active, so
recession shading and the probit's label both work.

**The degradation:** `config/recession_model.yml` enables a `funding_stress`
feature, which reads `gold.funding_stress_daily`. That table was empty before
G-A was fixed, and the model silently dropped the feature — it fits on whatever
features resolve and records the count in `n_features`. With G-A fixed the
feature is back. **Surface `n_features` on the probability page anyway**: it is
the only visible signal that the probit is running on fewer inputs than
configured, and it costs one card.

**Source queries**

```sql
-- qRegimeDaily : five pillars + composite + named regime
SELECT observation_date, growth_score, inflation_score, liquidity_score,
       credit_score, policy_score, composite_score, regime_name,
       regime_confidence
FROM   ${p_Catalog}.gold.macro_regime_daily;

-- qRecessionProb : 3/6/12-month forward probabilities
SELECT observation_date, recession_prob, prob_recession_3m, prob_recession_6m,
       prob_recession_12m, logit_score, n_features, n_obs_training,
       model_vintage, is_backfilled
FROM   ${p_Catalog}.gold.recession_probability_daily;

-- qFactorScores : expanding monthly PCA
SELECT observation_date, factor, score, explained_variance_ratio,
       cumulative_variance_ratio, n_obs
FROM   ${p_Catalog}.gold.macro_factor_scores;

-- qFactorLoadings : latest-date loadings heatmap
SELECT observation_date, factor, feature_name, loading
FROM   ${p_Catalog}.gold.macro_factor_loadings;

-- qAnomalyScores : Mahalanobis D2 in factor space
SELECT observation_date, mahalanobis_d2, chi2_df, p_value, is_anomaly,
       n_factors_used
FROM   ${p_Catalog}.gold.macro_anomaly_scores;

-- qZScoreHeatmap : cross-series context, national only.
-- Windows are trailing OBSERVATION counts: 12≈1y / 36≈3y / 60≈5y / 120≈10y
-- at monthly cadence. Expanding = PIT-safe full sample to date.
SELECT z.series_id, z.observation_date, z.value,
       z.zscore_expanding, z.percentile_expanding,
       z.zscore_12,  z.percentile_12,
       z.zscore_36,  z.percentile_36,
       z.zscore_60,  z.percentile_60,
       z.zscore_120, z.percentile_120
FROM   ${p_Catalog}.gold.zscore_heatmap z
JOIN   ${p_Catalog}.gold.dim_series d ON d.series_id = z.series_id
WHERE  d.econ_category <> 'REGIONAL';
```

`zscore_heatmap` is wide by window. For a fan chart, unpivot the eight
`zscore_*`/`percentile_*` columns into `window` / `zscore` / `percentile` in
Power Query; for a single-date heatmap, bind the wide columns directly.

**The `dim_series` join above is load-bearing.** `zscore_heatmap` is built from
the whole of `fred_feature_transforms` — one row per (series, date) across all
2,820 active series, not the 254 cataloged ones. The inner join restricts it to
the catalog as a side effect. Remove the join "to simplify the query" and the
model imports the entire universe.

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimDate[date]` | `qRegimeDaily[observation_date]` | 1:1 | single | ✅ |
| `qDimDate[date]` | `qRecessionProb[observation_date]` | 1:1 | single | ✅ |
| `qDimDate[date]` | `qFactorScores[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qFactorLoadings[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qAnomalyScores[observation_date]` | 1:1 | single | ✅ |
| `qDimSeries[series_id]` | `qZScoreHeatmap[series_id]` | 1:* | single | ✅ |

**Model settings**
- `regime_confidence` is **NULL for the default `Neutral` regime** — render
  "n/a", never 0.
- Distinguish `is_backfilled = TRUE` rows visually on the probability page; they
  were fit on fewer than `min_obs` examples.
- A regime ribbon needs a stable colour per `regime_name` — define it as a
  measure-driven colour, not visual-level formatting, so it stays consistent
  across pages.
- Sparse early history is expected: a row emits only once every pillar has a
  live input within `max_staleness_days` (200).

---

### Report 9 — Statistical Lab 🟢 Ready

**Readiness detail.** All **11/11** distinct series across
`config/stats_pairs.yml` pairs are active. Windows configured: 63, 252, and 0
(expanding).

**Source queries**

```sql
-- qCorrelation : pair × window × date
SELECT series_a, series_b, transform_a, transform_b, window,
       observation_date, correlation, n_obs
FROM   ${p_Catalog}.gold.series_correlation;

-- qLeadLag : pair × lag, with best_lag + Granger denormalised onto every row
SELECT series_a, series_b, transform_a, transform_b, lag, cross_correlation,
       n_obs, best_lag, granger_f_ab, granger_p_ab, granger_f_ba, granger_p_ba,
       as_of_date
FROM   ${p_Catalog}.gold.series_lead_lag;

-- qStructuralBreaks : chow + cusum per pair
SELECT series_a, series_b, transform_a, transform_b, test_type, break_date,
       f_stat, p_value, pre_n, post_n, pre_mean_a, post_mean_a,
       pre_mean_b, post_mean_b, cusum_max, is_significant, as_of_date
FROM   ${p_Catalog}.gold.series_structural_breaks;

-- qPairDim : the pair is the entity in this report, not the series
SELECT DISTINCT series_a, series_b,
       CONCAT(series_a, ' / ', series_b) AS pair_label
FROM   ${p_Catalog}.gold.series_correlation;

-- qTransforms : restricted to the configured pair series only (see below).
-- `zscore` here is the EXPANDING, point-in-time-safe z-score (the window frame
-- is UNBOUNDED PRECEDING..CURRENT ROW); mom/yoy are fractions, diff is native.
SELECT series_id, observation_date, value, mom, diff, yoy, zscore
FROM   ${p_Catalog}.gold.fred_feature_transforms
WHERE  series_id IN (
    'DGS2','DGS10','DTWEXBGS','T10YIE','CFNAI','UNRATE'
    /* … complete from config/stats_pairs.yml … */
);
```

**The `qTransforms` restriction is not optional.** Unrestricted,
`fred_feature_transforms` spans all 2,820 active series over full history and
will dominate the model. The only series this report needs are the ones in
`config/stats_pairs.yml`.

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qPairDim[pair_label]` | `qCorrelation[pair_label]`¹ | 1:* | single | ✅ |
| `qPairDim[pair_label]` | `qLeadLag[pair_label]`¹ | 1:* | single | ✅ |
| `qPairDim[pair_label]` | `qStructuralBreaks[pair_label]`¹ | 1:* | single | ✅ |
| `qDimDate[date]` | `qCorrelation[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qTransforms[observation_date]` | 1:* | single | ✅ |

¹ Add a matching `pair_label` computed column to each fact in Power Query —
a two-column composite key cannot be a Power BI relationship.

**Model settings**
- `qLeadLag` denormalises `best_lag` and all four Granger statistics onto every
  lag row. **Aggregate them with `SELECTEDVALUE`/`MAX`, never `SUM`** — summing
  multiplies by the lag count (25 rows at `max_lag: 12`).
- Label the CCF axis explicitly: **positive lag = `series_a` leads `series_b`**.
- Surface `n_obs` next to every correlation; the guarded measure blanks below 30.
- `break_date` NULL = "not testable (too few obs)", not a missing value.
- No executive page. Page 1 is the methodology page — misreading these outputs
  is this report's principal risk.

---

### Report 10 — Global Macro Monitor 🟡 Ready, non-commercial (G-B)

**Readiness detail.** Data is **complete**: 38/38 configured inflation series
and 38/38 policy-rate series resolve across 76 countries. The constraint is
licensing, not data — see §2 G-B.

**Source queries**

```sql
-- qGlobalInflation : CPI YoY by country × print
SELECT country, iso3, region, series_id, observation_date, cpi_yoy_pct,
       change_pp, trend, streak, target_pct, vs_target_pp
FROM   ${p_Catalog}.gold.global_inflation;

-- qGlobalPolicy : policy rate by country × print
SELECT country, iso3, region, series_id, observation_date, policy_rate_pct,
       change_bps, last_move_bps, stance, real_rate_pct
FROM   ${p_Catalog}.gold.global_policy_rates;

-- qCountryDim : the shared dimension across both facts
SELECT DISTINCT country, iso3, region FROM (
    SELECT country, iso3, region FROM ${p_Catalog}.gold.global_inflation
    UNION
    SELECT country, iso3, region FROM ${p_Catalog}.gold.global_policy_rates
);

-- qLatestByCountry : the map layer -- latest print per country, with its date
SELECT country, iso3, region, observation_date, cpi_yoy_pct, vs_target_pp
FROM (
    SELECT g.*, ROW_NUMBER() OVER (
               PARTITION BY country ORDER BY observation_date DESC) AS rn
    FROM   ${p_Catalog}.gold.global_inflation g
) WHERE rn = 1;
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qCountryDim[iso3]` | `qGlobalInflation[iso3]` | 1:* | single | ✅ |
| `qCountryDim[iso3]` | `qGlobalPolicy[iso3]` | 1:* | single | ✅ |
| `qCountryDim[iso3]` | `qLatestByCountry[iso3]` | 1:1 | single | ✅ |
| `qDimDate[date]` | `qGlobalInflation[observation_date]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qGlobalPolicy[observation_date]` | 1:* | single | ✅ |

**Model settings — mixed frequency is the hazard.** World Bank
`FP.CPI.TOTL.ZG` entries are **annual**; the US, Japan, Germany, UK, France,
Italy, Canada and the euro area run **monthly**. A "latest print" map compares a
2025 annual figure with a 2026 monthly one unless you show the asymmetry:
- `observation_date` in every map tooltip (that is why `qLatestByCountry`
  carries it).
- An as-of spread indicator on the map page (min and max rendered date).
- A frequency slicer so an analyst can restrict to monthly reporters.

Also:
- `real_rate_pct` is **ex-post** (policy − latest CPI YoY on or before the date)
  and blank where unconfigured or stale. Label it as such.
- `iso3` is the map key; `country` is the display name.
- Diverging palette centred on zero for `vs_target_pp`.
- About page: **World Bank CC BY 4.0 attribution is required.**
- Workspace: internal or non-commercial distribution is permitted with BIS
  attribution; commercial distribution needs BIS permission first (§2 G-B).

---

### Report 11 — Equity & Factor Analytics 🟡 Ready, one page gated (G-C)

**Readiness detail.** 85 Tiingo series active, 1 iShares ETF holdings feed, 0
Stooq. Because `select_canonical_equity_price_rows` falls back to Tiingo
`adjClose` where Stooq has no close, **`equity_return_daily`,
`realized_volatility`, `equity_factor_attribution`, and
`equity_factor_implied_return` all populate**. `equity_factor_attribution.yml`
has `tickers: []`, which means *all* tickers, not none.

Only the hidden reconciliation page is gated: `equity_price_reconciliation`
needs both sources on the same (ticker, date). Its 11 configured tickers (SPY,
QQQ, IWM, EFA, AGG, AAPL, MSFT, NVDA, AMZN, GOOGL, META) are Tiingo-covered but
have no Stooq counterpart until that manifest is activated.

**Tier B — internal only.** Tiingo, Stooq, and iShares are all
`redistribution_allowed: false`.

**Source queries**

```sql
-- qTotalReturn : TR vs PR index, dividends, yield
SELECT ticker, observation_date, close, dividend, split_factor,
       price_return, total_return, price_return_index, total_return_index,
       trailing_12m_dividend, dividend_yield_pct
FROM   ${p_Catalog}.gold.equity_total_return_index;

-- qReturnDaily : canonical price return (Tiingo-backed while Stooq is inactive)
SELECT ticker, observation_date, close, price_change, price_return,
       price_return_index
FROM   ${p_Catalog}.gold.equity_return_daily;

-- qRealizedVol : annualised stdev of daily LOG returns, per window
SELECT ticker, observation_date, window, realized_vol_pct
FROM   ${p_Catalog}.gold.realized_volatility;

-- qConstituents : current membership + snapshot history
SELECT index_etf, constituent, observation_date, weight_pct, weight_rank,
       is_latest_snapshot
FROM   ${p_Catalog}.gold.index_constituents;

-- qFactorAttribution : rolling betas on PCA macro factors
SELECT ticker, observation_date, window, factor, beta, t_stat,
       alpha, r_squared, n_obs
FROM   ${p_Catalog}.gold.equity_factor_attribution;

-- qImpliedReturn : systematic / alpha / residual decomposition
SELECT ticker, observation_date, window, implied_return, factor_return,
       alpha_return, realized_return, residual_return
FROM   ${p_Catalog}.gold.equity_factor_implied_return;

-- qPriceReconciliation : EMPTY until Stooq is activated (G-C)
SELECT ticker, observation_date, stooq_close, tiingo_adj_close,
       abs_diff, pct_diff, diverged
FROM   ${p_Catalog}.gold.equity_price_reconciliation;

-- qTickerDim
SELECT DISTINCT ticker FROM ${p_Catalog}.gold.equity_total_return_index;

-- qMarketCalendar : NYSE for equities
SELECT calendar_date, is_business_day, day_type, holiday_name
FROM   ${p_Catalog}.gold.market_calendar
WHERE  calendar_name = 'NYSE';
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qTickerDim[ticker]` | `qTotalReturn[ticker]` | 1:* | single | ✅ |
| `qTickerDim[ticker]` | `qReturnDaily[ticker]` | 1:* | single | ✅ |
| `qTickerDim[ticker]` | `qRealizedVol[ticker]` | 1:* | single | ✅ |
| `qTickerDim[ticker]` | `qFactorAttribution[ticker]` | 1:* | single | ✅ |
| `qTickerDim[ticker]` | `qImpliedReturn[ticker]` | 1:* | single | ✅ |
| `qTickerDim[ticker]` | `qPriceReconciliation[ticker]` | 1:* | single | ✅ |
| `qDimDate[date]` | each fact's date column | 1:* | single | ✅ |
| `qMarketCalendar[calendar_date]` | — | — | — | join to `qDimDate[date]` |

`qConstituents[constituent]` is **not** related to `qTickerDim` — ETF
constituents are a much larger universe than the priced tickers, and an enforced
relationship would drop unpriced holdings from the treemap. Keep it standalone.

**Model settings**
- `alpha`, `r_squared`, `n_obs` are **repeated across every factor row** for the
  same (ticker, window, date). `SELECTEDVALUE`, never `SUM`.
- `is_latest_snapshot = TRUE` for "current membership"; the unfiltered table is
  snapshot history.
- Vol windows (21/63/126/252) emit only once fully populated — the vol cone
  should use only complete windows.
- Realized vol is from **log** returns; `equity_return_daily[price_return]` is
  simple. Do not mix them in one calculation.
- Sanity check: `total_return_index ≥ price_return_index` for dividend payers.

---

### Report 12 — Regional & State Economic Map 🟢 Ready

**Readiness detail.** Unblocked by the catalog expansion: 120 `REGIONAL`
entries on `gold.dim_series`, each carrying both a `geo` code and a `metric`
name — 52 state unemployment rates, 50 coincident activity indexes, 14 state
house price indexes, 4 census regions. No report-local state mapping table and
no series-id parsing is needed: the map keys off `geo`, the measure slicer off
`metric`.

**Source queries**

```sql
-- qStateDim : dim_series is the state dimension. `geo` is the map key and
-- `metric` names the measure -- both are catalog columns now, so nothing here
-- parses a series id or a title.
SELECT series_id, title, geo, metric, polarity, default_transform, notes,
       CASE WHEN LENGTH(geo) = 2 THEN true ELSE false END AS is_state
FROM   ${p_Catalog}.gold.dim_series
WHERE  econ_category = 'REGIONAL';

-- qRegionalLatest : latest reading per regional series
SELECT m.series_id, m.latest_date, m.latest_value, m.prior_value,
       m.change_abs, m.change_pct, m.yoy_pct, m.zscore, m.percentile,
       m.direction_is_good, m.staleness_days
FROM   ${p_Catalog}.gold.macro_indicator_dashboard m
JOIN   ${p_Catalog}.gold.dim_series d ON d.series_id = m.series_id
WHERE  d.econ_category = 'REGIONAL';

-- qRegionalHistory : full history for the ranking + drillthrough pages
SELECT o.series_id, o.observation_date, o.value
FROM   ${p_Catalog}.gold.fred_latest_observation o
JOIN   ${p_Catalog}.gold.dim_series d ON d.series_id = o.series_id
WHERE  d.econ_category = 'REGIONAL'
  AND  o.is_missing = false;

-- qStateDiffusion : the REGIONAL breadth row = % of state series improving
SELECT econ_category, as_of_date, n_series, n_improving, n_deteriorating,
       breadth_pct, avg_zscore
FROM   ${p_Catalog}.gold.macro_category_summary
WHERE  econ_category = 'REGIONAL';
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qStateDim[series_id]` | `qRegionalLatest[series_id]` | 1:1 | single | ✅ |
| `qStateDim[series_id]` | `qRegionalHistory[series_id]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qRegionalHistory[observation_date]` | 1:* | single | ✅ |

**Model settings**
- **Filter `is_state = TRUE` on the choropleth.** The four census-region rows
  carry a name (`Midwest`) not a two-letter code and will fail to map; keep them
  in the rankings table instead. Puerto Rico (`PR`) is a state-level row that
  most US state maps will not render — decide explicitly whether to include it
  and say which on the page.
- `geo` is the map location field, categorised as **State or Province**.
- `metric` is a single-select slicer; the three metrics have different units
  (percent, index, index) and must not share an axis.
- House prices cover **14 of 52** — label partial coverage on that page rather
  than showing an ambiguously empty map.
- `qStateDiffusion` blends unemployment, activity, and house prices. For a pure
  activity diffusion count, compute it from the `*PHCI` subset of
  `qRegionalLatest` instead of using `breadth_pct` directly.

---

### Report 13 — Point-in-Time & Revisions Lab 🟢 Ready

**Readiness detail.** `vintage_enabled` defaults to true, so the whole active
universe captures vintages; `gold.fred_point_in_time` and
`gold.fred_revision_stats` populate on every run. Readiness is unconstrained by
data — the constraint is **model size**, which is why the series restriction
below is mandatory rather than advisory.

**Source queries**

```sql
-- qRevisionSummary : per-series revision profile (the "trust the first print?"
-- ranking). Small -- one row per series.
SELECT series_id, observation_count, avg_revision_count, max_revision_count,
       avg_abs_revision_pct, max_abs_revision_pct
FROM   ${p_Catalog}.gold.v_series_revision_summary;

-- qRevisionStats : per-observation revision magnitude, restricted to the same
-- series list as qPointInTime below.
SELECT series_id, observation_date, revision_count,
       first_value, first_realtime_start,
       latest_value, latest_realtime_start,
       revision_delta, revision_pct
FROM   ${p_Catalog}.gold.fred_revision_stats
WHERE  series_id IN ('GDPC1','PAYEMS','CPIAUCSL','UNRATE','INDPRO','PCEPI');

-- qPointInTime : every vintage. THE LARGEST TABLE IN THE SUITE -- restrict.
SELECT series_id, observation_date, realtime_start, realtime_end, value,
       revision_number
FROM   ${p_Catalog}.gold.fred_point_in_time
WHERE  series_id IN ('GDPC1','PAYEMS','CPIAUCSL','UNRATE','INDPRO','PCEPI')
  AND  observation_date >= '2010-01-01';

-- qCrossSeriesRevised / qCrossSeriesPIT : leakage comparison.
-- The PIT variant carries an extra `basis` column recording how each leg was
-- aligned; the revised variant has no equivalent.
SELECT feature_name, op, observation_date, value
FROM   ${p_Catalog}.gold.fred_cross_series_feature;

SELECT feature_name, op, observation_date, value, basis
FROM   ${p_Catalog}.gold.fred_cross_series_feature_pit;

-- qAsOf : disconnected as-of parameter table driving the vintage filter.
-- Built from realtime_start (the vintage axis), NOT observation_date -- the
-- question is "what was known on date X", and X is a publication date.
SELECT DISTINCT realtime_start AS as_of_date
FROM   ${p_Catalog}.gold.fred_point_in_time
WHERE  realtime_start IS NOT NULL AND realtime_start <> '';
```

Only three of the nine `fred_revision_stats` columns are obvious from the table
name: it is built by `CREATE OR REPLACE ... AS SELECT`, so its shape comes from
the query, not a DDL column list. The magnitude column is **`revision_delta`**
(`latest_value − first_value`), not `revision_abs`.

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qDimSeries[series_id]` | `qRevisionSummary[series_id]` | 1:1 | single | ✅ |
| `qDimSeries[series_id]` | `qRevisionStats[series_id]` | 1:* | single | ✅ |
| `qDimSeries[series_id]` | `qPointInTime[series_id]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qPointInTime[observation_date]` | 1:* | single | ✅ |
| `qAsOf[as_of_date]` | — | — | **disconnected** | ❌ |

**`qAsOf` must stay disconnected.** The vintage filter is a range predicate —
`realtime_start <= AsOf AND (realtime_end >= AsOf OR realtime_end IS NULL)` —
which no Power BI relationship can express. It is implemented in the
`[Value As Known]` measure (spec §5, Report 13).

**Model settings**
- Incremental refresh on `qPointInTime`, partitioned on `observation_date`.
- Non-vintage series show `revision_count = 1`; exclude them from the revision
  ranking rather than ranking them as "never revised" — they were never
  *eligible* to be revised.
- Consider DirectQuery for `qPointInTime` only if restriction + incremental
  refresh still miss the refresh window.

---

### Report 14 — Pipeline Health & Governance 🟢 Ready

**Readiness detail.** Audit and meta tables are written on every run regardless
of which sources are active, so this report has no data prerequisite.

**This is wave 1 — build it first and finish it before any other report
starts** (§5). Every other report's credibility depends on someone noticing when
the pipeline breaks, and this is also the acceptance test for the first live
pipeline run: it is where you confirm `BGCRRATE` resolved, see which series
failed, read the DQ pass rate, and check per-source freshness.

**Source queries**

```sql
-- qEtlRun : one row per pipeline invocation
SELECT run_id, environment, triggered_by, status, started_at, ended_at,
       duration_seconds, series_total, series_succeeded, series_failed,
       error_message
FROM   ${p_Catalog}.audit.etl_run;

-- qSeriesRun : per (run, series) -- incremental refresh on run date
SELECT run_id, series_id, status, load_type, started_at, duration_seconds,
       observations_extracted, rows_written_bronze, rows_merged_silver,
       dq_passed, error_message
FROM   ${p_Catalog}.audit.etl_series_run;

-- qDataQuality : check outcomes -- incremental refresh on run date
SELECT run_id, series_id, check_name, passed, severity, message, metric_value
FROM   ${p_Catalog}.audit.data_quality_result;

-- qStaleness : all-source freshness (NOTE: string dates -- convert in PQ)
SELECT source, series_id, frequency, latest_observation_date,
       days_since_last_observation, is_stale, has_data, checked_at
FROM   ${p_Catalog}.meta.series_staleness;

-- qDrift : FRED-only manifest-vs-live metadata drift
SELECT series_id, field, manifest_value, fred_value, kind, severity, detected_at
FROM   ${p_Catalog}.meta.fred_series_drift;

-- qLifecycle : FRED-reported series health snapshots
SELECT series_id, fred_title, fred_frequency, observation_start, observation_end,
       last_updated, popularity, discontinued, days_since_last_observation,
       is_stale, checked_at
FROM   ${p_Catalog}.meta.fred_series_lifecycle;

-- qSourceCoverage : coverage + freshness verdict per (source, series)
SELECT source, series_id, category, frequency, latest_observation_date,
       observation_count, days_since_last, is_stale
FROM   ${p_Catalog}.gold.v_source_coverage;

-- qReconciliation : cross-source same-concept divergence
SELECT name, observation_date, series_a, value_a, series_b, value_b,
       abs_diff, pct_diff, diverged
FROM   ${p_Catalog}.gold.fred_source_reconciliation;

-- qCatalog : the suite's own documentation page
SELECT object_name, object_type, module, grain, intended_visual, description
FROM   ${p_Catalog}.gold.powerbi_catalog;

-- qSeriesMeta : manifest intent, for joining ownership onto failures
SELECT series_id, title, category, frequency, units, active, load_type,
       vintage_enabled, validation_profile, business_owner, technical_owner,
       priority
FROM   ${p_Catalog}.meta.fred_series;
```

**Data model**

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qEtlRun[run_id]` | `qSeriesRun[run_id]` | 1:* | single | ✅ |
| `qEtlRun[run_id]` | `qDataQuality[run_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qSeriesRun[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qDataQuality[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qStaleness[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qDrift[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qLifecycle[series_id]` | 1:* | single | ✅ |
| `qDimDate[date]` | `qEtlRun[run_date]`¹ | 1:* | single | ✅ |

¹ Derive `run_date` from `started_at` in Power Query; relationships cannot be
built on a timestamp against a date dimension without truncation.

`qCatalog` is standalone documentation — related to nothing.

**Model settings**
- **String timestamps.** `checked_at`, `detected_at`, and
  `latest_observation_date` are `STRING` in `meta.fred_series_lifecycle`,
  `meta.fred_series_drift`, and `meta.series_staleness`. Convert explicitly in
  Power Query; do not rely on implicit conversion.
- `qStaleness` is one row per (source, series) per check — if it accumulates
  snapshots over time, take the latest `checked_at` per key in Power Query or
  the stale-count measure will multiply.
- Incremental refresh on `qSeriesRun` and `qDataQuality` (partition on run
  date); these are the two highest-row-count tables and they grow every run.
- Drift is **FRED-only** by design; label the page so nobody reads an empty
  drift table as "no drift anywhere". Staleness covers every source.

---

## 5. Recommended build order (revised for readiness)

| Wave | Reports | Rationale |
|---|---|---|
| **0** | Kernel (`fnGold`, dimensions, measures, theme) | Nothing starts without it |
| **1** | **14 Pipeline Health — on its own** | Build it first and finish it before anything else starts. See below. |
| **2** | **1** Macro Cockpit, **3** Curve Lab | All ready; the highest-value pair for the executive audience |
| **3** | **4** Spreads, **7** Credit, **6** Fed Policy | All ready, same shapes as wave 2 |
| **4** | **2** Inflation, **8** Regime (note `n_features`), **12** Regional Map | All ready; #2 and #12 both moved earlier once auditing corrected their status |
| **5** | **9** Statistical Lab, **13** PIT Lab | Ready but need the sizing work in the spec's §6 |
| **6** | **11** Equity (skip recon page), **10** Global (non-commercial workspace) | Ready with documented constraints |
| **7** | **5** Funding & Liquidity | Ready (G-A fixed, tape 10/10, board 17/17). Confirm `BGCRRATE` ingested after the first run |

### Why Report 14 goes first, alone

It is the cheapest report in the suite and the only one that answers *"can I
trust what the other thirteen are showing me?"*. Three concrete reasons it comes
before, not alongside, the rest:

1. **No prerequisites.** The audit and meta tables are written on every run
   regardless of which sources are active, so it needs nothing that could
   delay it.
2. **It is the acceptance test for the pipeline itself.** Every readiness
   verdict in §1 is static analysis of configs against manifests — no report in
   this suite has yet been pointed at a populated warehouse. Report 14 is how
   you find out whether the run actually succeeded, which series failed, what
   the DQ pass rate is, and how stale each source is. Discovering that through
   a blank KPI card on the Macro Cockpit is a much worse day.
3. **It de-risks specific open items.** After the first run it immediately
   shows whether `BGCRRATE` resolved (§2 G-A), whether any series is failing
   quietly, and — via `gold.powerbi_catalog` on page 8 — what the Gold layer
   actually contains versus what this document claims.

Build it, run it against real data, and only then start wave 2. If the numbers
on Report 14 look wrong, every downstream report would have inherited the same
problem invisibly.

**Everything after wave 2 is ordered by shape and audience, not dependency.**
G-A is fixed, so Report 5 can move earlier than wave 7 if it is wanted sooner,
and waves 3–7 can be resequenced freely to suit whoever is waiting.

---

## Appendix A — Readiness audit script

Re-run after any manifest or config change. It resolves every Gold-table config
against the active series universe and prints missing ids per surface. This is
the check that found G-A. It is now enforced by `tests/test_config_manifest_integrity.py`.

```python
# scripts/audit_report_readiness.py
import glob
import yaml

active = set()
for path in glob.glob('manifests/*.yml'):
    doc = yaml.safe_load(open(path)) or {}
    for spec in doc.get('series') or []:
        if spec.get('active', True):
            active.add(spec['series_id'])

def cfg(name):
    return yaml.safe_load(open(f'config/{name}')) or {}

def report(label, ids):
    ids = [i for i in ids if i]
    missing = sorted({i for i in ids if i not in active})
    total = len(set(ids))
    status = f"MISSING: {' '.join(missing)}" if missing else "ok"
    print(f"{label:34s} {total - len(missing):3d}/{total:3d}  {status}")
    return missing

report('curve.yml tenors', [t['series_id'] for t in cfg('curve.yml')['tenors']])
report('spreads.yml legs',
       [x for s in cfg('spreads.yml')['spreads']
          for x in (s['long_leg'], s['short_leg'])])

fund = cfg('funding.yml')
report('funding metrics', [m['series_id'] for m in fund['metrics']])
spreads = {s['name']: s for s in fund['spreads']}
needed = {leg
          for c in fund['stress']['components']
          for leg in (spreads[c['spread']]['long_leg'],
                      spreads[c['spread']]['short_leg'])}
gauge_missing = report('STRESS GAUGE components', sorted(needed))
print(f"  -> funding_stress_daily emits rows: "
      f"{'NO' if gauge_missing else 'YES'}")

rates = cfg('benchmark_rates.yml')['rates']
report('benchmark_rates.yml',
       [r['series_id'] for r in rates] + [r.get('benchmark') for r in rates])
report('credit.yml', [c['series_id'] for c in cfg('credit.yml')['instruments']])
for pillar, conf in cfg('regime.yml')['pillars'].items():
    report(f'regime pillar {pillar}', [i['series_id'] for i in conf['inputs']])
report('stats_pairs.yml',
       [x for p in cfg('stats_pairs.yml')['pairs']
          for x in (p['series_a'], p['series_b'])])
report('ml_features.yml',
       [f['series_id'] for f in cfg('ml_features.yml')['features']])
report('inflation_items.yml',
       [i['series_id'] for i in cfg('inflation_items.yml')['items']])
```

**Now enforced in CI.** `tests/test_config_manifest_integrity.py` runs this
cross-check on every commit:

- `test_every_config_series_is_declared_in_a_manifest` — fails on any id no
  manifest declares, across all ten Gold configs.
- `test_funding_stress_gauge_components_are_resolvable` — fails if any gauge
  component leg is missing *or inactive*, since one bad leg empties the gauge.

Keep the script above for ad-hoc coverage reporting (it shows *inactive*
declared series, which the tests deliberately tolerate); rely on the tests to
catch the defect class.

---

## Appendix B — Query-to-report index

| Gold object | Consumed by |
|---|---|
| `dim_series` | all (Report 12 uses `geo` as the map key) |
| `dim_date` | all |
| `market_calendar` | 3 (FEDWIRE), 5 (FEDWIRE), 11 (NYSE) |
| `macro_indicator_dashboard` | 1 (national), 12 (regional) |
| `macro_indicator_sparkline` | 1 |
| `macro_category_summary` | 1 (national), 12 (REGIONAL diffusion) |
| `inflation_explorer`, `inflation_contribution` | 2 |
| `inflation_forecast` | 2 |
| `treasury_curve`, `treasury_curve_metrics`, `treasury_curve_rolling` | 3 |
| `yield_curve_ns_factors` | 3 |
| `curve_spread_daily`, `spread_inversion_episode`, `curve_spread_rolling` | 4 |
| `funding_tape_daily`, `funding_stress_daily` | 5 |
| `benchmark_rate_board` | 5, 6 |
| `fomc_probability`, `fomc_meeting_path` | 6 |
| `credit_spread_daily`, `credit_spread_rolling` | 7 |
| `macro_regime_daily`, `recession_probability_daily` | 8 |
| `macro_factor_scores`, `macro_factor_loadings`, `macro_anomaly_scores` | 8 |
| `zscore_heatmap` | 8 |
| `series_correlation`, `series_lead_lag`, `series_structural_breaks` | 9 |
| `fred_feature_transforms` | 9 (restricted) |
| `global_inflation`, `global_policy_rates` | 10 |
| `equity_total_return_index`, `equity_return_daily`, `realized_volatility` | 11 |
| `index_constituents`, `equity_factor_attribution`, `equity_factor_implied_return` | 11 |
| `equity_price_reconciliation` | 11 (gated) |
| `fred_latest_observation` | 12 |
| `fred_point_in_time`, `fred_revision_stats`, `v_series_revision_summary` | 13 |
| `fred_cross_series_feature`, `fred_cross_series_feature_pit` | 13 |
| `audit.*`, `meta.*`, `v_source_coverage`, `fred_source_reconciliation` | 14 |
| `powerbi_catalog` | 14 |

**Unbound in v1:** `fred_company_fundamentals`, `fred_company_ratios`,
`v_company_ratio_ranks` (SEC out of scope), `fred_macro_feature_daily`,
`fred_curve_spread`, `ml_feature_matrix`, `v_latest_revised`,
`v_series_latest_value`.
