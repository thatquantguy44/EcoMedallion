"""Local SQLite backend — run the whole pipeline on a laptop, no Spark.

``LocalWarehouse`` implements the same :class:`fred_pipeline.warehouse.Warehouse`
surface as the Spark/Delta backend but persists to a single SQLite ``.db`` file.
It is intended for local development, demos, CI, and quickly inspecting results
without a Databricks workspace.

Design notes
------------
* Delta schemas/catalogs don't exist in SQLite, so tables are named
  ``{schema}_{name}`` (e.g. ``silver_fred_observation``) in one file.
* Silver upserts use ``INSERT ... ON CONFLICT`` on the same natural key the
  Delta MERGE uses, so re-runs are idempotent here too.
* Gold is rebuilt with the same semantics as the pure-Python spec functions in
  :mod:`fred_pipeline.transform` / :mod:`fred_pipeline.features`. When
  ``polars`` is installed (``pip install -e ".[local]"``), the vectorized
  implementations in :mod:`fred_pipeline.gold_polars` are used instead — they
  are output-identical (see ``tests/test_gold_polars_parity.py``) but far
  faster once the series universe grows past a few dozen series, since the
  daily feature matrix is a dense ``series x calendar_day`` panel. Falls back
  to the pure-Python versions automatically if polars isn't installed.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import os
import sqlite3
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from fred_pipeline.audit import EtlRun, EtlSeriesRun
from fred_pipeline.config import PipelineConfig
from fred_pipeline.manifest import Manifest
from fred_pipeline.meta import build_meta_rows
from fred_pipeline.quality import QualityReport
from fred_pipeline.transform import latest_by_observation

log = logging.getLogger(__name__)
from fred_pipeline.warehouse import dq_rows


def _gold_feature_impls():
    """("polars" | "python", daily_feature_matrix, compute_feature_transforms,
    compute_curve_spreads, compute_revision_stats).

    Prefers the polars-accelerated implementations (output-identical to the
    pure-Python spec — see tests/test_gold_polars_parity.py) when polars is
    installed; falls back to the pure-Python versions otherwise, exactly as
    Spark is an optional, lazily-imported dependency elsewhere in this repo.
    In "polars" mode the functions return DataFrames (see
    ``LocalWarehouse._insert_frame``); in "python" mode they return
    ``list[dict]`` (see ``LocalWarehouse._insert``).
    """
    try:
        from fred_pipeline.gold_polars import (
            compute_curve_spreads_frame,
            compute_feature_transforms_frame,
            compute_revision_stats_frame,
            daily_feature_matrix_frame,
        )

        return (
            "polars",
            daily_feature_matrix_frame,
            compute_feature_transforms_frame,
            compute_curve_spreads_frame,
            compute_revision_stats_frame,
        )
    except ImportError:
        from fred_pipeline.features import (
            compute_curve_spreads,
            compute_feature_transforms,
            compute_revision_stats,
        )
        from fred_pipeline.transform import daily_feature_matrix

        return (
            "python",
            daily_feature_matrix,
            compute_feature_transforms,
            compute_curve_spreads,
            compute_revision_stats,
        )


def _compute_parallel(tasks: dict[str, Callable[[], Any]]) -> dict[str, Any]:
    """Run independent Gold computations in parallel, returning by task name.

    SQLite writes stay serial in ``build_gold``; this helper is only for pure
    dict/list-in → dict/list-out engines that do not touch the connection.
    """
    if not tasks:
        return {}
    if len(tasks) == 1:
        name, fn = next(iter(tasks.items()))
        return {name: fn()}
    max_workers = min(len(tasks), os.cpu_count() or 1)
    out: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fn): name for name, fn in tasks.items()}
        for fut in as_completed(futures):
            out[futures[fut]] = fut.result()
    return out


# DDL for the SQLite mirror of the Delta tables. Kept in one place for clarity.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta_fred_series (
    series_id TEXT PRIMARY KEY, title TEXT, category TEXT, frequency TEXT,
    units TEXT, active INTEGER, load_type TEXT, expected_update_frequency TEXT,
    vintage_enabled INTEGER, validation_profile TEXT, business_owner TEXT,
    technical_owner TEXT, downstream_use_case TEXT, priority INTEGER,
    restate_records INTEGER, min_value REAL, max_value REAL,
    tags TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS meta_fred_manifest (
    manifest_name TEXT PRIMARY KEY, description TEXT, version INTEGER,
    source_path TEXT, series_count INTEGER, loaded_at TEXT
);
CREATE TABLE IF NOT EXISTS meta_fred_series_manifest_map (
    series_id TEXT, manifest_name TEXT, updated_at TEXT,
    PRIMARY KEY (series_id, manifest_name)
);
CREATE TABLE IF NOT EXISTS bronze_fred_api_response (
    run_id TEXT, source TEXT NOT NULL DEFAULT 'fred', series_id TEXT,
    endpoint TEXT, request_params TEXT,
    response_payload TEXT, observation_count INTEGER, payload_bytes INTEGER,
    ingested_at TEXT
);
CREATE TABLE IF NOT EXISTS silver_fred_observation (
    source TEXT NOT NULL DEFAULT 'fred',
    series_id TEXT, observation_date TEXT, realtime_start TEXT,
    realtime_end TEXT, value REAL, raw_value TEXT, is_missing INTEGER,
    row_hash TEXT, revision_number INTEGER, ingested_at TEXT, run_id TEXT,
    PRIMARY KEY (source, series_id, observation_date, realtime_start)
);
CREATE TABLE IF NOT EXISTS gold_fred_latest_observation (
    series_id TEXT, observation_date TEXT, value REAL, realtime_start TEXT,
    realtime_end TEXT, is_missing INTEGER, revision_number INTEGER, ingested_at TEXT
);
CREATE TABLE IF NOT EXISTS gold_fred_point_in_time (
    series_id TEXT, observation_date TEXT, realtime_start TEXT, realtime_end TEXT,
    value REAL, revision_number INTEGER, is_missing INTEGER, ingested_at TEXT
);
CREATE TABLE IF NOT EXISTS gold_fred_macro_feature_daily (
    as_of_date TEXT, series_id TEXT, raw_value REAL, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_feature_transforms (
    series_id TEXT, observation_date TEXT, value REAL,
    mom REAL, diff REAL, yoy REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_series_zscore_rolling (
    series_id TEXT, observation_date TEXT, window INTEGER,
    value REAL, change REAL, pct_change REAL, zscore REAL, percentile REAL
);
CREATE TABLE IF NOT EXISTS gold_zscore_heatmap (
    series_id TEXT, observation_date TEXT, value REAL,
    zscore_expanding REAL, percentile_expanding REAL,
    zscore_12 REAL, percentile_12 REAL,
    zscore_36 REAL, percentile_36 REAL,
    zscore_60 REAL, percentile_60 REAL,
    zscore_120 REAL, percentile_120 REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_curve_spread (
    spread_name TEXT, observation_date TEXT, long_leg TEXT, short_leg TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_cross_series_feature (
    feature_name TEXT, op TEXT, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_cross_series_feature_pit (
    feature_name TEXT, op TEXT, observation_date TEXT, value REAL, basis TEXT
);
CREATE TABLE IF NOT EXISTS gold_fred_source_reconciliation (
    name TEXT, observation_date TEXT, series_a TEXT, value_a REAL,
    series_b TEXT, value_b REAL, abs_diff REAL, pct_diff REAL, diverged INTEGER
);
CREATE TABLE IF NOT EXISTS gold_fred_company_fundamentals (
    cik TEXT, concept TEXT, statement TEXT, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_company_ratios (
    cik TEXT, ratio_name TEXT, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_revision_stats (
    series_id TEXT, observation_date TEXT, revision_count INTEGER,
    first_value REAL, first_realtime_start TEXT,
    latest_value REAL, latest_realtime_start TEXT,
    revision_delta REAL, revision_pct REAL
);

-- Market-terminal analytical views (docs/market_terminal_gold_views.md):
-- star-schema dimensions + the ECON macro dashboard + the Treasury Curve Lab,
-- shaped for Power BI. Built by fred_pipeline.terminal_views (pure Python,
-- shared with the Spark backend).
CREATE TABLE IF NOT EXISTS gold_dim_series (
    series_id TEXT PRIMARY KEY, title TEXT, source TEXT, frequency TEXT,
    units TEXT, econ_category TEXT, polarity INTEGER, default_transform TEXT,
    scale TEXT, decimals INTEGER, geo TEXT, metric TEXT, notes TEXT
);
CREATE TABLE IF NOT EXISTS gold_dim_date (
    -- date identifiers
    date TEXT PRIMARY KEY, date_key INTEGER,
    -- calendar year
    year INTEGER, year_label TEXT,
    year_start_date TEXT, year_end_date TEXT,
    is_year_start INTEGER, is_year_end INTEGER, is_leap_year INTEGER,
    -- calendar quarter
    quarter INTEGER, quarter_label TEXT,
    year_quarter TEXT, year_quarter_sort INTEGER,
    quarter_start_date TEXT, quarter_end_date TEXT,
    is_quarter_start INTEGER, is_quarter_end INTEGER,
    -- calendar month
    month INTEGER, month_name TEXT, month_short_name TEXT,
    year_month TEXT, year_month_sort INTEGER,
    month_start_date TEXT, month_end_date TEXT,
    is_month_start INTEGER, is_month_end INTEGER, days_in_month INTEGER,
    -- ISO week
    iso_year INTEGER, week_of_year INTEGER, year_week TEXT,
    week_start_date TEXT, week_end_date TEXT,
    is_week_start INTEGER, is_week_end INTEGER,
    -- day
    day_of_month INTEGER, day_of_year INTEGER,
    day_name TEXT, day_short_name TEXT,
    day_of_week_iso INTEGER, day_of_week_sun INTEGER,
    is_weekday INTEGER, is_weekend INTEGER,
    -- US Federal fiscal year (October start)
    fiscal_year INTEGER, fiscal_year_label TEXT,
    fiscal_quarter INTEGER, fiscal_quarter_label TEXT, fiscal_month INTEGER,
    fiscal_year_quarter_sort INTEGER,
    fiscal_year_start_date TEXT, fiscal_year_end_date TEXT,
    fiscal_quarter_start_date TEXT, fiscal_quarter_end_date TEXT,
    is_fiscal_year_start INTEGER, is_fiscal_year_end INTEGER,
    is_fiscal_quarter_start INTEGER, is_fiscal_quarter_end INTEGER,
    -- NBER recession (NULL = unknown / not yet ingested)
    is_recession INTEGER,
    -- quant/derivatives marker dates (calendar-agnostic)
    is_imm_date INTEGER, is_monthly_option_expiry INTEGER, is_triple_witching INTEGER
);
CREATE TABLE IF NOT EXISTS gold_market_calendar (
    calendar_name TEXT, calendar_date TEXT, is_weekend INTEGER,
    is_holiday INTEGER, holiday_name TEXT, day_type TEXT,
    is_business_day INTEGER, prior_business_day TEXT, next_business_day TEXT,
    t2_settle_date TEXT, business_day_of_month INTEGER,
    business_days_in_month INTEGER, is_first_business_day_of_month INTEGER,
    is_last_business_day_of_month INTEGER, is_last_business_day_of_quarter INTEGER,
    is_last_business_day_of_year INTEGER
);
CREATE TABLE IF NOT EXISTS gold_macro_indicator_dashboard (
    series_id TEXT, econ_category TEXT, polarity INTEGER,
    default_transform TEXT, as_of_date TEXT, latest_date TEXT,
    latest_value REAL, prior_date TEXT, prior_value REAL, change_abs REAL,
    change_pct REAL, yoy_pct REAL, zscore REAL, percentile REAL,
    surprise REAL, surprise_z REAL, direction_is_good INTEGER,
    spark_min REAL, spark_max REAL, staleness_days INTEGER, realtime_start TEXT
);
CREATE TABLE IF NOT EXISTS gold_macro_indicator_sparkline (
    series_id TEXT, point_index INTEGER, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_macro_category_summary (
    econ_category TEXT, as_of_date TEXT, n_series INTEGER,
    n_improving INTEGER, n_deteriorating INTEGER, breadth_pct REAL,
    avg_zscore REAL, surprise_index REAL
);
CREATE TABLE IF NOT EXISTS gold_treasury_curve (
    as_of_date TEXT, tenor_label TEXT, tenor_months INTEGER,
    series_id TEXT, yield_pct REAL
);
CREATE TABLE IF NOT EXISTS gold_treasury_curve_metrics (
    as_of_date TEXT, level REAL, slope_10y2y REAL, slope_10y3m REAL,
    curvature_2_5_10 REAL, butterfly_2_10_30 REAL,
    is_inverted_10y2y INTEGER, is_inverted_10y3m INTEGER,
    is_recession INTEGER, curve_move TEXT
);
CREATE TABLE IF NOT EXISTS gold_yield_curve_ns_factors (
    observation_date TEXT, beta0 REAL, beta1 REAL, beta2 REAL,
    lambda REAL, lambda_estimated INTEGER, fit_rmse REAL,
    n_tenors INTEGER, fit_valid INTEGER
);
CREATE TABLE IF NOT EXISTS gold_curve_spread_daily (
    spread_name TEXT, observation_date TEXT, long_leg TEXT, short_leg TEXT,
    value REAL, value_bps REAL, zscore REAL, percentile REAL,
    is_inverted INTEGER, inversion_run INTEGER, is_recession INTEGER
);
CREATE TABLE IF NOT EXISTS gold_spread_inversion_episode (
    spread_name TEXT, long_leg TEXT, short_leg TEXT, episode_number INTEGER,
    start_date TEXT, end_date TEXT, last_inverted_date TEXT,
    observation_count INTEGER, calendar_days INTEGER,
    trough_value REAL, trough_bps REAL, trough_date TEXT,
    is_ongoing INTEGER, recession_overlap INTEGER
);
CREATE TABLE IF NOT EXISTS gold_benchmark_rate_board (
    series_id TEXT, rate_label TEXT, rate_category TEXT,
    benchmark_series TEXT, as_of_date TEXT, latest_date TEXT,
    latest_value REAL, prior_value REAL, change_bps REAL, trend TEXT,
    spread_to_benchmark_bps REAL, zscore REAL, percentile REAL,
    regime TEXT, staleness_days INTEGER
);
CREATE TABLE IF NOT EXISTS gold_funding_tape_daily (
    metric_name TEXT, metric_type TEXT, observation_date TEXT,
    value REAL, zscore REAL, percentile REAL
);
CREATE TABLE IF NOT EXISTS gold_funding_stress_daily (
    observation_date TEXT, composite_z REAL, stress_score REAL,
    stress_bucket TEXT, n_components INTEGER
);
CREATE TABLE IF NOT EXISTS gold_credit_spread_daily (
    instrument TEXT, series_id TEXT, category TEXT, observation_date TEXT,
    oas_pct REAL, oas_bps REAL, change_bps REAL, zscore REAL,
    percentile REAL, is_stress_episode INTEGER, is_recession INTEGER
);
CREATE TABLE IF NOT EXISTS gold_inflation_explorer (
    series_id TEXT, item_label TEXT, parent_item TEXT,
    hierarchy_level INTEGER, basket TEXT, sa_nsa TEXT, observation_date TEXT,
    index_value REAL, mom_pct REAL, yoy_pct REAL, mom_accel REAL,
    yoy_accel REAL, three_month_annualized REAL, weight REAL,
    contribution_pp REAL
);
CREATE TABLE IF NOT EXISTS gold_inflation_contribution (
    observation_date TEXT, basket TEXT, sa_nsa TEXT, series_id TEXT,
    item_label TEXT, contribution_pp REAL, rank_in_month INTEGER,
    is_headline_total INTEGER
);
CREATE TABLE IF NOT EXISTS gold_curve_spread_rolling (
    spread_name TEXT, observation_date TEXT, window INTEGER,
    value REAL, change REAL, pct_change REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_credit_spread_rolling (
    instrument TEXT, series_id TEXT, observation_date TEXT, window INTEGER,
    oas_bps REAL, change_bps REAL, pct_change REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_treasury_curve_rolling (
    tenor_label TEXT, tenor_months INTEGER, series_id TEXT,
    observation_date TEXT, window INTEGER,
    yield_pct REAL, change REAL, pct_change REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_macro_regime_daily (
    observation_date TEXT, growth_score REAL, inflation_score REAL,
    liquidity_score REAL, credit_score REAL, policy_score REAL,
    composite_score REAL, regime_name TEXT, regime_confidence REAL
);
CREATE TABLE IF NOT EXISTS gold_series_correlation (
    series_a TEXT, series_b TEXT, transform_a TEXT, transform_b TEXT,
    window INTEGER, observation_date TEXT, correlation REAL, n_obs INTEGER
);
CREATE TABLE IF NOT EXISTS gold_series_lead_lag (
    series_a TEXT, series_b TEXT, transform_a TEXT, transform_b TEXT,
    lag INTEGER, cross_correlation REAL, n_obs INTEGER, best_lag INTEGER,
    granger_f_ab REAL, granger_p_ab REAL, granger_f_ba REAL,
    granger_p_ba REAL, as_of_date TEXT
);
CREATE TABLE IF NOT EXISTS gold_series_structural_breaks (
    series_a TEXT, series_b TEXT, transform_a TEXT, transform_b TEXT,
    test_type TEXT, break_date TEXT,
    f_stat REAL, p_value REAL, pre_n INTEGER, post_n INTEGER,
    pre_mean_a REAL, post_mean_a REAL, pre_mean_b REAL, post_mean_b REAL,
    cusum_max REAL, is_significant INTEGER NOT NULL DEFAULT 0,
    as_of_date TEXT
);
-- FOMC rate probabilities (option A, no CME connector; config/fomc.yml).
CREATE TABLE IF NOT EXISTS gold_fomc_probability (
    meeting_date TEXT, target_lower_bps INTEGER, target_upper_bps INTEGER,
    outcome_bps INTEGER, probability REAL, model_vintage TEXT, n_inputs INTEGER
);
CREATE TABLE IF NOT EXISTS gold_fomc_meeting_path (
    meeting_date TEXT, implied_rate REAL, implied_move_bps REAL,
    cumulative_move_bps REAL, model_vintage TEXT
);
CREATE TABLE IF NOT EXISTS gold_global_inflation (
    country TEXT, iso3 TEXT, region TEXT, series_id TEXT,
    observation_date TEXT, cpi_yoy_pct REAL, change_pp REAL, trend TEXT,
    streak INTEGER, target_pct REAL, vs_target_pp REAL
);
CREATE TABLE IF NOT EXISTS gold_global_policy_rates (
    country TEXT, iso3 TEXT, region TEXT, series_id TEXT,
    observation_date TEXT, policy_rate_pct REAL, change_bps REAL,
    last_move_bps REAL, stance TEXT, real_rate_pct REAL
);
CREATE TABLE IF NOT EXISTS gold_powerbi_catalog (
    object_name TEXT PRIMARY KEY, object_type TEXT, module TEXT,
    grain TEXT, intended_visual TEXT, description TEXT
);
CREATE TABLE IF NOT EXISTS gold_equity_return_daily (
    ticker TEXT, observation_date TEXT, close REAL, price_change REAL,
    price_return REAL, price_return_index REAL
);
CREATE TABLE IF NOT EXISTS gold_index_constituents (
    index_etf TEXT, constituent TEXT, observation_date TEXT,
    weight_pct REAL, weight_rank INTEGER, is_latest_snapshot INTEGER
);
CREATE TABLE IF NOT EXISTS gold_equity_total_return_index (
    ticker TEXT, observation_date TEXT, close REAL, dividend REAL,
    split_factor REAL, price_return REAL, total_return REAL,
    price_return_index REAL, total_return_index REAL,
    trailing_12m_dividend REAL, dividend_yield_pct REAL
);
CREATE TABLE IF NOT EXISTS gold_equity_price_reconciliation (
    ticker TEXT, observation_date TEXT,
    stooq_close REAL, tiingo_adj_close REAL,
    abs_diff REAL, pct_diff REAL, diverged INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS gold_realized_volatility (
    ticker TEXT, observation_date TEXT, window INTEGER, realized_vol_pct REAL
);

-- ML pipeline (handoff.md "ML Extensions Sub-Plan"):
-- ML-0 feature matrix, ML-2 PCA factor scores/loadings, ML-4 anomaly scores.
CREATE TABLE IF NOT EXISTS gold_ml_feature_matrix (
    observation_date TEXT, feature_name TEXT, series_id TEXT,
    transform TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_macro_factor_scores (
    observation_date TEXT, factor INTEGER, score REAL,
    explained_variance_ratio REAL, cumulative_variance_ratio REAL, n_obs INTEGER
);
CREATE TABLE IF NOT EXISTS gold_macro_factor_loadings (
    observation_date TEXT, factor INTEGER, feature_name TEXT, loading REAL
);
CREATE TABLE IF NOT EXISTS gold_macro_anomaly_scores (
    observation_date TEXT, mahalanobis_d2 REAL, chi2_df INTEGER,
    p_value REAL, is_anomaly INTEGER NOT NULL DEFAULT 0, n_factors_used INTEGER
);
CREATE TABLE IF NOT EXISTS gold_equity_factor_attribution (
    ticker TEXT, observation_date TEXT, window INTEGER, factor INTEGER,
    beta REAL, t_stat REAL, alpha REAL, r_squared REAL, n_obs INTEGER
);
CREATE TABLE IF NOT EXISTS gold_equity_factor_implied_return (
    ticker TEXT, observation_date TEXT, window INTEGER,
    implied_return REAL, factor_return REAL, alpha_return REAL,
    realized_return REAL, residual_return REAL
);
CREATE TABLE IF NOT EXISTS gold_recession_probability_daily (
    observation_date TEXT, recession_prob REAL, prob_recession_3m REAL,
    prob_recession_6m REAL, prob_recession_12m REAL, logit_score REAL,
    n_features INTEGER, n_obs_training INTEGER, model_vintage TEXT,
    is_backfilled INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS gold_inflation_forecast (
    series_id TEXT, forecast_date TEXT, horizon_months INTEGER,
    forecast_value REAL, lower_80 REAL, upper_80 REAL, lower_95 REAL,
    upper_95 REAL, model_type TEXT, lag_order INTEGER,
    model_vintage TEXT, n_obs_training INTEGER
);

-- Forward-looking economic release calendar (terminal module CAL). Not
-- point-in-time (it's a re-fetched schedule, not a revised observation) —
-- fetched_at stamps staleness. Written directly by FredPipeline.run() via
-- Warehouse.write_release_calendar(), not by build_gold().
CREATE TABLE IF NOT EXISTS gold_release_calendar (
    release_id INTEGER, release_name TEXT, release_date TEXT,
    importance TEXT, econ_category TEXT, representative_series_id TEXT,
    is_future INTEGER, fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_etl_run (
    run_id TEXT PRIMARY KEY, environment TEXT, manifest_path TEXT,
    triggered_by TEXT, status TEXT, started_at TEXT, ended_at TEXT,
    duration_seconds REAL, series_total INTEGER, series_succeeded INTEGER,
    series_failed INTEGER, error_message TEXT
);
CREATE TABLE IF NOT EXISTS audit_etl_series_run (
    run_id TEXT, series_id TEXT, status TEXT, load_type TEXT, started_at TEXT,
    ended_at TEXT, duration_seconds REAL, observations_extracted INTEGER,
    rows_written_bronze INTEGER, rows_merged_silver INTEGER, dq_passed INTEGER,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS audit_data_quality_result (
    run_id TEXT, series_id TEXT, check_name TEXT, passed INTEGER,
    severity TEXT, message TEXT, metric_value REAL
);
CREATE TABLE IF NOT EXISTS audit_query_log (
    queried_at TEXT, query_text_hash TEXT, caller TEXT
);
CREATE TABLE IF NOT EXISTS meta_fred_series_lifecycle (
    series_id TEXT, fred_title TEXT, fred_frequency TEXT, fred_units TEXT,
    seasonal_adjustment TEXT, observation_start TEXT, observation_end TEXT,
    last_updated TEXT, popularity INTEGER, discontinued INTEGER,
    days_since_last_observation INTEGER, is_stale INTEGER, checked_at TEXT
);
CREATE TABLE IF NOT EXISTS meta_fred_series_drift (
    series_id TEXT, field TEXT, manifest_value TEXT, fred_value TEXT,
    kind TEXT, severity TEXT, detected_at TEXT
);
CREATE TABLE IF NOT EXISTS meta_series_staleness (
    source TEXT, series_id TEXT, frequency TEXT, latest_observation_date TEXT,
    days_since_last_observation INTEGER, is_stale INTEGER, has_data INTEGER,
    checked_at TEXT
);

-- SQLite equivalents of the Delta-only gold.v_* views in sql/60_views.sql
-- (SQLite has no CREATE OR REPLACE VIEW, so these use IF NOT EXISTS and are
-- expected to stay in sync with that file by hand).
CREATE VIEW IF NOT EXISTS gold_v_latest_revised AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY series_id, observation_date
        ORDER BY realtime_start DESC
    ) AS rn
    FROM silver_fred_observation
)
SELECT series_id, observation_date, value, realtime_start, realtime_end,
       is_missing, revision_number, ingested_at
FROM ranked
WHERE rn = 1;

CREATE VIEW IF NOT EXISTS gold_v_point_in_time AS
SELECT series_id, observation_date, realtime_start, realtime_end, value,
       revision_number, is_missing, ingested_at
FROM silver_fred_observation;

CREATE VIEW IF NOT EXISTS gold_v_series_latest_value AS
WITH latest AS (
    SELECT series_id, observation_date, value,
        ROW_NUMBER() OVER (
            PARTITION BY series_id ORDER BY observation_date DESC
        ) AS rn
    FROM gold_v_latest_revised
    WHERE is_missing = false
)
SELECT series_id, observation_date, value
FROM latest
WHERE rn = 1;

CREATE VIEW IF NOT EXISTS gold_v_series_revision_summary AS
SELECT series_id,
    COUNT(*)               AS observation_count,
    AVG(revision_count)    AS avg_revision_count,
    MAX(revision_count)    AS max_revision_count,
    AVG(ABS(revision_pct)) AS avg_abs_revision_pct,
    MAX(ABS(revision_pct)) AS max_abs_revision_pct
FROM gold_fred_revision_stats
GROUP BY series_id;

-- Multi-source coverage & freshness dashboard: latest observation, count, and a
-- staleness verdict per (source, series_id), using the manifest cadence from
-- meta. Mirrors gold.v_source_coverage in sql/60_views.sql.
CREATE VIEW IF NOT EXISTS gold_v_source_coverage AS
WITH per_series AS (
    SELECT source, series_id,
           MAX(observation_date)            AS latest_observation_date,
           COUNT(DISTINCT observation_date) AS observation_count
    FROM silver_fred_observation
    GROUP BY source, series_id
),
aged AS (
    SELECT p.source, p.series_id, m.category, m.frequency,
           p.latest_observation_date, p.observation_count,
           CAST(julianday('now') - julianday(p.latest_observation_date) AS INTEGER)
               AS days_since_last
    FROM per_series p
    LEFT JOIN meta_fred_series m ON m.series_id = p.series_id
)
SELECT source, series_id, category, frequency, latest_observation_date,
       observation_count, days_since_last,
       CASE
         WHEN frequency IN ('d','daily')       AND days_since_last > 10  THEN 1
         WHEN frequency IN ('w','weekly')      AND days_since_last > 21  THEN 1
         WHEN frequency IN ('bw','biweekly')   AND days_since_last > 30  THEN 1
         WHEN frequency IN ('m','monthly')     AND days_since_last > 75  THEN 1
         WHEN frequency IN ('q','quarterly')   AND days_since_last > 200 THEN 1
         WHEN frequency IN ('sa','semiannual') AND days_since_last > 380 THEN 1
         WHEN frequency IN ('a','annual')      AND days_since_last > 550 THEN 1
         ELSE 0
       END AS is_stale
FROM aged;

-- Cross-company ranks/percentiles of each SEC-derived ratio within each period.
-- Mirrors gold.v_company_ratio_ranks in sql/60_views.sql.
CREATE VIEW IF NOT EXISTS gold_v_company_ratio_ranks AS
SELECT cik, ratio_name, observation_date, value,
       PERCENT_RANK() OVER (
           PARTITION BY ratio_name, observation_date ORDER BY value
       ) AS pct_rank,
       ROW_NUMBER() OVER (
           PARTITION BY ratio_name, observation_date ORDER BY value DESC
       ) AS rank_desc
FROM gold_fred_company_ratios;

-- Tier-7: indexes on the core query paths.
--
-- gold_v_latest_revised: ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY realtime_start DESC)
--   → covering index lets SQLite sort each partition without a full table scan.
-- read_silver / merge_silver: filter + sort on (series_id, observation_date).
-- restate_start: DISTINCT observation_date WHERE series_id = ? ORDER BY DESC LIMIT N.
-- gold_fred_latest_observation: read by series_id in Gold rebuild + downstream joins.
-- gold_macro_factor_scores: queried by observation_date in anomaly + attribution.
-- gold_equity_factor_attribution: queried by (ticker, window) in implied-return.
CREATE INDEX IF NOT EXISTS ix_silver_obs_sid_rt
    ON silver_fred_observation(series_id, realtime_start DESC);
CREATE INDEX IF NOT EXISTS ix_silver_obs_sid_date
    ON silver_fred_observation(series_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_gold_latest_sid
    ON gold_fred_latest_observation(series_id);
CREATE INDEX IF NOT EXISTS ix_factor_scores_date
    ON gold_macro_factor_scores(observation_date);
CREATE INDEX IF NOT EXISTS ix_equity_attr_ticker_window
    ON gold_equity_factor_attribution(ticker, window);
"""


def _encode(value: Any) -> Any:
    """Coerce a Python value into something SQLite can store."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return value


class LocalWarehouse:
    supports_incremental_audit = True

    """A SQLite-file implementation of the Warehouse protocol."""

    def __init__(self, config: PipelineConfig, db_path: str = "fred_local.db"):
        self.config = config
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        # Tier-6 SQLite performance tuning: WAL journal, relaxed fsync,
        # 64 MB page cache, 256 MB mmap.  These survive reconnects via the
        # journal_mode pragma (WAL is persisted); the others are session-level.
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-65536")  # 64 MB
        self.conn.execute("PRAGMA mmap_size=268435456")  # 256 MB
        self.conn.executescript(_SCHEMA)
        self._apply_additive_migrations()
        self._defer_commits = False
        self.conn.commit()

    # ---- schema migrations ----------------------------------------------

    # Columns added to an existing table after that table shipped. ``_SCHEMA``
    # uses CREATE TABLE IF NOT EXISTS, which is a no-op against a database file
    # created by an earlier version -- so a new column would be missing there
    # and every insert would fail with "table X has no column named Y". SQLite
    # supports ADD COLUMN cheaply (metadata-only), so replay the additions on
    # open. Additive only: renames and drops still need a rebuild.
    _ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (
        # (table, column, type) -- geo landed with the REGIONAL series catalog.
        ("gold_dim_series", "geo", "TEXT"),
        # metric disambiguates measures that share an econ_category (REGIONAL
        # spans unemployment, activity and house prices).
        ("gold_dim_series", "metric", "TEXT"),
    )

    def _apply_additive_migrations(self) -> None:
        for table, column, coltype in self._ADDED_COLUMNS:
            existing = {
                row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")
            }
            if not existing:
                continue  # table absent entirely; _SCHEMA just created it
            if column not in existing:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")

    # ---- low-level helpers ---------------------------------------------

    def _insert(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        upsert_keys: Sequence[str] | None = None,
    ) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        collist = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders})"
        if upsert_keys:
            updates = ", ".join(
                f"{c}=excluded.{c}" for c in cols if c not in upsert_keys
            )
            conflict = ", ".join(upsert_keys)
            sql += f" ON CONFLICT({conflict}) DO UPDATE SET {updates}"
        data = [tuple(_encode(r.get(c)) for c in cols) for r in rows]
        self.conn.executemany(sql, data)
        if not self._defer_commits:
            self.conn.commit()
        return len(rows)

    def _insert_frame(
        self,
        table: str,
        df: Any,
        upsert_keys: Sequence[str] | None = None,
    ) -> int:
        """Like :meth:`_insert` but for a polars DataFrame.

        Inserts straight from ``df.iter_rows()`` (plain tuples), skipping the
        per-row dict allocation ``_insert`` does — that dict-building step is
        what dominates wall-clock time at large row counts (see the
        gold_polars module docstring). Callers must ensure the DataFrame has
        no bool/date/datetime columns (cast to Utf8/int first), since this
        path skips ``_encode``.
        """
        if df.is_empty():
            return 0
        cols = df.columns
        collist = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders})"
        if upsert_keys:
            updates = ", ".join(
                f"{c}=excluded.{c}" for c in cols if c not in upsert_keys
            )
            conflict = ", ".join(upsert_keys)
            sql += f" ON CONFLICT({conflict}) DO UPDATE SET {updates}"
        n = df.height
        self.conn.executemany(sql, df.iter_rows())
        if not self._defer_commits:
            self.conn.commit()
        return n

    def _read(self, table: str) -> list[dict[str, Any]]:
        cur = self.conn.execute(f"SELECT * FROM {table}")
        return [dict(row) for row in cur.fetchall()]

    # ---- Warehouse surface ---------------------------------------------

    def sync_meta(self, manifests: Iterable[Manifest]) -> dict[str, int]:
        rows = build_meta_rows(list(manifests))
        counts = {}
        counts["fred_series"] = self._insert(
            "meta_fred_series", rows["fred_series"], upsert_keys=["series_id"]
        )
        counts["fred_manifest"] = self._insert(
            "meta_fred_manifest", rows["fred_manifest"], upsert_keys=["manifest_name"]
        )
        counts["fred_series_manifest_map"] = self._insert(
            "meta_fred_series_manifest_map",
            rows["fred_series_manifest_map"],
            upsert_keys=["series_id", "manifest_name"],
        )
        return counts

    def restate_start(self, series_id: str, n: int) -> str | None:
        """Earliest observation_date among the N most recent for this series.

        Returns ``None`` when the series has no rows yet (→ full load).
        """
        cur = self.conn.execute(
            """
            SELECT MIN(observation_date) FROM (
                SELECT DISTINCT observation_date FROM silver_fred_observation
                WHERE series_id = ?
                ORDER BY observation_date DESC
                LIMIT ?
            )
            """,
            (series_id, int(n)),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def write_bronze(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("bronze_fred_api_response", rows)

    def read_bronze(self, series_ids: list[str] | None = None) -> list[dict[str, Any]]:
        sql = (
            "SELECT source, series_id, response_payload, run_id, ingested_at "
            "FROM bronze_fred_api_response"
        )
        params: tuple = ()
        if series_ids:
            placeholders = ", ".join("?" * len(series_ids))
            sql += f" WHERE series_id IN ({placeholders})"
            params = tuple(series_ids)
        sql += " ORDER BY ingested_at"
        return self.query(sql, params)

    def merge_silver(self, rows: list[dict[str, Any]]) -> int:
        return self._insert(
            "silver_fred_observation",
            rows,
            upsert_keys=["source", "series_id", "observation_date", "realtime_start"],
        )

    def build_gold(self) -> dict[str, str]:
        self.conn.execute("BEGIN")
        self._defer_commits = True
        try:
            result = self._build_gold_inner()
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            return result
        finally:
            self._defer_commits = False

    def _build_gold_inner(self) -> dict[str, str]:
        silver = self._read("silver_fred_observation")
        for r in silver:
            r["is_missing"] = bool(r.get("is_missing"))

        # point-in-time = every vintage row
        self.conn.execute("DELETE FROM gold_fred_point_in_time")
        pit = [
            {
                "series_id": r["series_id"],
                "observation_date": r["observation_date"],
                "realtime_start": r["realtime_start"],
                "realtime_end": r["realtime_end"],
                "value": r["value"],
                "revision_number": r["revision_number"],
                "is_missing": r["is_missing"],
                "ingested_at": r["ingested_at"],
            }
            for r in silver
        ]
        self._insert("gold_fred_point_in_time", pit)

        # latest revision per (series, date)
        latest = latest_by_observation(silver)
        self.conn.execute("DELETE FROM gold_fred_latest_observation")
        latest_rows = [
            {
                "series_id": r["series_id"],
                "observation_date": r["observation_date"],
                "value": r["value"],
                "realtime_start": r["realtime_start"],
                "realtime_end": r["realtime_end"],
                "is_missing": r["is_missing"],
                "revision_number": r.get("revision_number"),
                "ingested_at": r["ingested_at"],
            }
            for r in latest
        ]
        self._insert("gold_fred_latest_observation", latest_rows)

        # daily forward-filled feature matrix; quant transforms (mom/yoy/diff/
        # zscore); curve spreads; revision-magnitude stats. Prefers the
        # polars-accelerated versions (output-identical, see gold_polars
        # module docstring) when available, inserting straight from the
        # DataFrame to skip Python dict overhead.
        (
            mode,
            build_daily_matrix,
            build_transforms,
            build_spreads,
            build_revision_stats,
        ) = _gold_feature_impls()
        insert = self._insert_frame if mode == "polars" else self._insert

        self.conn.execute("DELETE FROM gold_fred_macro_feature_daily")
        insert("gold_fred_macro_feature_daily", build_daily_matrix(latest))

        self.conn.execute("DELETE FROM gold_fred_feature_transforms")
        _ft_result = build_transforms(latest)
        insert("gold_fred_feature_transforms", _ft_result)
        # Capture as list[dict] for the ML feature-matrix engine (ML-0).
        feature_transform_rows: list[dict] = (
            _ft_result.to_dicts() if mode == "polars" else list(_ft_result)
        )

        # Historical z-score analysis (ML-adjacent: reads feature_transform_rows).
        from fred_pipeline.zscore_views import (
            compute_fred_series_zscore_rolling,
            compute_zscore_heatmap,
        )

        self.conn.execute("DELETE FROM gold_fred_series_zscore_rolling")
        self._insert(
            "gold_fred_series_zscore_rolling",
            compute_fred_series_zscore_rolling(feature_transform_rows),
        )
        self.conn.execute("DELETE FROM gold_zscore_heatmap")
        self._insert(
            "gold_zscore_heatmap", compute_zscore_heatmap(feature_transform_rows)
        )

        self.conn.execute("DELETE FROM gold_fred_curve_spread")
        insert("gold_fred_curve_spread", build_spreads(latest))

        # cross-series features (frequency-aware, N-leg): a small output, so use
        # the pure-Python reference directly (same function the Spark path reuses).
        from fred_pipeline.features import (
            compute_cross_series_features,
            compute_cross_series_features_pit,
            compute_source_reconciliation,
        )

        self.conn.execute("DELETE FROM gold_fred_cross_series_feature")
        self._insert(
            "gold_fred_cross_series_feature", compute_cross_series_features(latest)
        )
        # Point-in-time (as-first-reported) variant: leak-free, reads raw Silver
        # (all vintages), not latest-revision rows.
        self.conn.execute("DELETE FROM gold_fred_cross_series_feature_pit")
        self._insert(
            "gold_fred_cross_series_feature_pit",
            compute_cross_series_features_pit(silver),
        )
        self.conn.execute("DELETE FROM gold_fred_source_reconciliation")
        self._insert(
            "gold_fred_source_reconciliation", compute_source_reconciliation(latest)
        )

        # SEC company financials: standardize raw XBRL tags into canonical line
        # items, then derived ratios (reads raw Silver for source='sec' rows).
        from fred_pipeline.sec_standardization import (
            compute_sec_ratios,
            standardize_sec_statements,
        )

        fundamentals = standardize_sec_statements(silver)
        self.conn.execute("DELETE FROM gold_fred_company_fundamentals")
        self._insert("gold_fred_company_fundamentals", fundamentals)
        self.conn.execute("DELETE FROM gold_fred_company_ratios")
        self._insert("gold_fred_company_ratios", compute_sec_ratios(fundamentals))

        # revision stats read raw Silver (every vintage), not latest-revision
        # rows — they exist to measure how much observations get revised.
        self.conn.execute("DELETE FROM gold_fred_revision_stats")
        insert("gold_fred_revision_stats", build_revision_stats(silver))

        # Market-terminal analytical views (docs/market_terminal_gold_views.md):
        # dimensions, the ECON macro dashboard, and the Treasury Curve Lab.
        # All pure-Python engines shared verbatim with the Spark backend.
        from fred_pipeline.terminal_views import (
            build_dim_date,
            build_dim_series,
            compute_benchmark_rate_board,
            compute_credit_spread_daily,
            compute_credit_spread_rolling,
            compute_curve_spread_daily,
            compute_curve_spread_rolling,
            compute_fomc_probability,
            compute_funding_features,
            compute_inflation_explorer,
            compute_macro_dashboard,
            compute_market_calendar,
            compute_spread_inversion_episodes,
            compute_treasury_curve,
            compute_treasury_curve_rolling,
        )

        meta_rows = self.query(
            "SELECT series_id, title, frequency, units FROM meta_fred_series"
        )
        self.conn.execute("DELETE FROM gold_dim_series")
        self._insert("gold_dim_series", build_dim_series(meta_rows=meta_rows))

        obs_dates = [
            r["observation_date"]
            for r in latest
            if not r["is_missing"] and r.get("observation_date")
        ]
        usrec = [r for r in latest if r["series_id"] == "USREC"]
        self.conn.execute("DELETE FROM gold_dim_date")
        self.conn.execute("DELETE FROM gold_market_calendar")
        if obs_dates:
            self._insert(
                "gold_dim_date",
                build_dim_date(min(obs_dates), max(obs_dates), usrec),
            )
            self._insert(
                "gold_market_calendar",
                compute_market_calendar(min(obs_dates), max(obs_dates)),
            )

        # Independent pure-Python Gold engines can compute in parallel; writes
        # remain serial below because SQLite has a single writer.
        from fred_pipeline.global_views import (
            compute_global_inflation,
            compute_global_policy_rates,
            powerbi_catalog_rows,
        )
        from fred_pipeline.regime_stats import (
            compute_macro_regime,
            compute_series_correlation,
            compute_series_lead_lag,
            compute_series_structural_breaks,
        )

        computed = _compute_parallel(
            {
                "dashboard": lambda: compute_macro_dashboard(latest),
                "curve": lambda: compute_treasury_curve(latest),
                "curve_spread_daily": lambda: compute_curve_spread_daily(latest),
                "spread_inversion_episode": lambda: compute_spread_inversion_episodes(
                    latest
                ),
                "benchmark_rate_board": lambda: compute_benchmark_rate_board(latest),
                "funding": lambda: compute_funding_features(latest),
                "credit_spread_daily": lambda: compute_credit_spread_daily(latest),
                "inflation": lambda: compute_inflation_explorer(latest),
                "curve_spread_rolling": lambda: compute_curve_spread_rolling(latest),
                "credit_spread_rolling": lambda: compute_credit_spread_rolling(latest),
                "treasury_curve_rolling": lambda: compute_treasury_curve_rolling(
                    latest
                ),
                "macro_regime_daily": lambda: compute_macro_regime(latest),
                "series_correlation": lambda: compute_series_correlation(latest),
                "series_lead_lag": lambda: compute_series_lead_lag(latest),
                "series_structural_breaks": lambda: compute_series_structural_breaks(
                    latest
                ),
                "fomc": lambda: compute_fomc_probability(latest),
                "global_inflation": lambda: compute_global_inflation(latest),
                "global_policy_rates": lambda: compute_global_policy_rates(latest),
            }
        )

        dash = computed["dashboard"]
        self.conn.execute("DELETE FROM gold_macro_indicator_dashboard")
        self._insert("gold_macro_indicator_dashboard", dash["dashboard"])
        self.conn.execute("DELETE FROM gold_macro_indicator_sparkline")
        self._insert("gold_macro_indicator_sparkline", dash["sparkline"])
        self.conn.execute("DELETE FROM gold_macro_category_summary")
        self._insert("gold_macro_category_summary", dash["category_summary"])

        curve = computed["curve"]
        self.conn.execute("DELETE FROM gold_treasury_curve")
        self._insert("gold_treasury_curve", curve["curve"])
        self.conn.execute("DELETE FROM gold_treasury_curve_metrics")
        self._insert("gold_treasury_curve_metrics", curve["metrics"])
        from fred_pipeline.ns_model import compute_yield_curve_ns_factors

        ns_factor_rows = compute_yield_curve_ns_factors(curve["curve"])
        self.conn.execute("DELETE FROM gold_yield_curve_ns_factors")
        self._insert("gold_yield_curve_ns_factors", ns_factor_rows)
        self.conn.execute("DELETE FROM gold_curve_spread_daily")
        self._insert("gold_curve_spread_daily", computed["curve_spread_daily"])
        self.conn.execute("DELETE FROM gold_spread_inversion_episode")
        self._insert(
            "gold_spread_inversion_episode", computed["spread_inversion_episode"]
        )

        # Phase 4 rates complex: BMRK benchmark board, FUND funding tape +
        # stress gauge, CRDT credit spreads (configs under config/).
        self.conn.execute("DELETE FROM gold_benchmark_rate_board")
        self._insert("gold_benchmark_rate_board", computed["benchmark_rate_board"])
        funding = computed["funding"]
        self.conn.execute("DELETE FROM gold_funding_tape_daily")
        self._insert("gold_funding_tape_daily", funding["tape"])
        self.conn.execute("DELETE FROM gold_funding_stress_daily")
        self._insert("gold_funding_stress_daily", funding["stress"])
        credit_rows = computed["credit_spread_daily"]
        self.conn.execute("DELETE FROM gold_credit_spread_daily")
        self._insert("gold_credit_spread_daily", credit_rows)

        # Phase 2 Inflation Explorer (config/inflation_items.yml).
        inflation = computed["inflation"]
        self.conn.execute("DELETE FROM gold_inflation_explorer")
        self._insert("gold_inflation_explorer", inflation["explorer"])
        self.conn.execute("DELETE FROM gold_inflation_contribution")
        self._insert("gold_inflation_contribution", inflation["contribution"])

        # Rolling-window stats companions (windows 1/5/10/21/63/126/252 obs)
        # for the spread, credit, and curve daily tables.
        self.conn.execute("DELETE FROM gold_curve_spread_rolling")
        self._insert("gold_curve_spread_rolling", computed["curve_spread_rolling"])
        self.conn.execute("DELETE FROM gold_credit_spread_rolling")
        self._insert("gold_credit_spread_rolling", computed["credit_spread_rolling"])
        self.conn.execute("DELETE FROM gold_treasury_curve_rolling")
        self._insert("gold_treasury_curve_rolling", computed["treasury_curve_rolling"])

        # Phase 5: regime playbook + statistical lab (config/regime.yml,
        # config/stats_pairs.yml).
        regime_rows = computed["macro_regime_daily"]
        self.conn.execute("DELETE FROM gold_macro_regime_daily")
        self._insert("gold_macro_regime_daily", regime_rows)
        self.conn.execute("DELETE FROM gold_series_correlation")
        self._insert("gold_series_correlation", computed["series_correlation"])
        self.conn.execute("DELETE FROM gold_series_lead_lag")
        self._insert("gold_series_lead_lag", computed["series_lead_lag"])
        self.conn.execute("DELETE FROM gold_series_structural_breaks")
        self._insert(
            "gold_series_structural_breaks", computed["series_structural_breaks"]
        )

        # docs/handoffs/terminal_phase0_gaps.md item 3: FOMC rate
        # probabilities (config/fomc.yml) — option A, no CME connector;
        # derived from already-ingested FRED short-rate/target series.
        fomc = computed["fomc"]
        self.conn.execute("DELETE FROM gold_fomc_probability")
        self._insert("gold_fomc_probability", fomc["probability"])
        self.conn.execute("DELETE FROM gold_fomc_meeting_path")
        self._insert("gold_fomc_meeting_path", fomc["meeting_path"])

        # Phase 6: global inflation / policy rates + the Power BI catalog
        # (config/global_series.yml; catalog from global_views.POWERBI_CATALOG).
        self.conn.execute("DELETE FROM gold_global_inflation")
        self._insert("gold_global_inflation", computed["global_inflation"])
        self.conn.execute("DELETE FROM gold_global_policy_rates")
        self._insert("gold_global_policy_rates", computed["global_policy_rates"])
        self.conn.execute("DELETE FROM gold_powerbi_catalog")
        self._insert("gold_powerbi_catalog", powerbi_catalog_rows())

        # Equity slice: canonical price return, constituents (iShares), total
        # return (Tiingo). Feed source-filtered Silver rows so the shared
        # <ticker>:close namespace can't collapse vendor rows before canonical
        # selection (equity data is non-vintage → one row per source/id/date).
        from fred_pipeline.equity_views import (
            compute_equity_price_reconciliation,
            compute_equity_return_daily,
            compute_equity_total_return_index,
            compute_index_constituents,
            compute_realized_volatility,
            select_canonical_equity_price_rows,
        )

        stooq_rows = [r for r in silver if r.get("source") == "stooq"]
        ishares_rows = [r for r in silver if r.get("source") == "ishares"]
        tiingo_rows = [r for r in silver if r.get("source") == "tiingo"]
        canonical_price_rows = select_canonical_equity_price_rows(
            stooq_rows, tiingo_rows
        )
        eq_return_rows = compute_equity_return_daily(canonical_price_rows)
        self.conn.execute("DELETE FROM gold_equity_return_daily")
        self._insert("gold_equity_return_daily", eq_return_rows)
        self.conn.execute("DELETE FROM gold_index_constituents")
        self._insert(
            "gold_index_constituents", compute_index_constituents(ishares_rows)
        )
        self.conn.execute("DELETE FROM gold_equity_total_return_index")
        self._insert(
            "gold_equity_total_return_index",
            compute_equity_total_return_index(tiingo_rows),
        )
        self.conn.execute("DELETE FROM gold_equity_price_reconciliation")
        self._insert(
            "gold_equity_price_reconciliation",
            compute_equity_price_reconciliation(stooq_rows, tiingo_rows),
        )
        self.conn.execute("DELETE FROM gold_realized_volatility")
        self._insert(
            "gold_realized_volatility",
            compute_realized_volatility(canonical_price_rows),
        )

        # ML pipeline: ML-0 feature matrix → ML-2 PCA scores/loadings → ML-4 anomaly.
        from fred_pipeline.anomaly import compute_macro_anomaly_scores
        from fred_pipeline.macro_pca import compute_macro_factor_scores
        from fred_pipeline.ml_features import compute_ml_feature_matrix

        ml_cfg = None  # load from repo config/ml_features.yml
        try:
            from fred_pipeline.ml_features import load_ml_feature_config

            ml_cfg = load_ml_feature_config()
        except (OSError, ValueError) as exc:
            log.warning("Using default ML feature config: %s", exc)
        ml_matrix = compute_ml_feature_matrix(feature_transform_rows, ml_cfg)
        self.conn.execute("DELETE FROM gold_ml_feature_matrix")
        self._insert("gold_ml_feature_matrix", ml_matrix)

        n_comp = ml_cfg.n_components if ml_cfg else 5
        pca = compute_macro_factor_scores(ml_matrix, n_components=n_comp)
        self.conn.execute("DELETE FROM gold_macro_factor_scores")
        self._insert("gold_macro_factor_scores", pca["scores"])
        self.conn.execute("DELETE FROM gold_macro_factor_loadings")
        self._insert("gold_macro_factor_loadings", pca["loadings"])

        anom_thresh = ml_cfg.anomaly_threshold if ml_cfg else 0.99
        self.conn.execute("DELETE FROM gold_macro_anomaly_scores")
        self._insert(
            "gold_macro_anomaly_scores",
            compute_macro_anomaly_scores(pca["scores"], anomaly_threshold=anom_thresh),
        )

        # ML-5: Equity factor attribution (rolling OLS vs PCA macro factors).
        from fred_pipeline.equity_factor_attribution import (
            compute_equity_factor_attribution,
            compute_equity_factor_implied_return,
            load_equity_factor_config,
        )

        ef_cfg = None
        try:
            ef_cfg = load_equity_factor_config()
        except (OSError, ValueError) as exc:
            log.warning("Using default equity factor config: %s", exc)
        attribution_rows = compute_equity_factor_attribution(
            eq_return_rows, pca["scores"], cfg=ef_cfg
        )
        self.conn.execute("DELETE FROM gold_equity_factor_attribution")
        self._insert("gold_equity_factor_attribution", attribution_rows)

        # ML-5b: Factor-implied return decomposition.
        self.conn.execute("DELETE FROM gold_equity_factor_implied_return")
        self._insert(
            "gold_equity_factor_implied_return",
            compute_equity_factor_implied_return(
                attribution_rows, pca["scores"], eq_return_rows, cfg=ef_cfg
            ),
        )

        # ML-3: Expanding IRLS logistic recession probability model.
        from fred_pipeline.recession_model import (
            compute_recession_probability,
            load_recession_model_config,
        )

        rec_cfg = None
        try:
            rec_cfg = load_recession_model_config()
        except (OSError, ValueError) as exc:
            log.warning("Using default recession model config: %s", exc)
        self.conn.execute("DELETE FROM gold_recession_probability_daily")
        self._insert(
            "gold_recession_probability_daily",
            compute_recession_probability(
                latest,
                ns_factor_rows=ns_factor_rows,
                feature_transform_rows=feature_transform_rows,
                credit_spread_rows=credit_rows,
                funding_stress_rows=funding["stress"],
                regime_rows=regime_rows,
                cfg=rec_cfg,
            ),
        )

        # ML-6: Short-horizon inflation forecasting (AR + VAR on CPI/PCE MoM).
        from fred_pipeline.inflation_model import (
            compute_inflation_forecast,
            load_inflation_forecast_config,
        )

        inf_cfg = None
        try:
            inf_cfg = load_inflation_forecast_config()
        except (OSError, ValueError) as exc:
            log.warning("Using default inflation forecast config: %s", exc)
        self.conn.execute("DELETE FROM gold_inflation_forecast")
        self._insert(
            "gold_inflation_forecast",
            compute_inflation_forecast(latest, cfg=inf_cfg),
        )

        return {
            k: "ok"
            for k in (
                "fred_point_in_time",
                "fred_latest_observation",
                "fred_macro_feature_daily",
                "fred_feature_transforms",
                "fred_series_zscore_rolling",
                "zscore_heatmap",
                "fred_curve_spread",
                "fred_cross_series_feature",
                "fred_cross_series_feature_pit",
                "fred_source_reconciliation",
                "fred_company_fundamentals",
                "fred_company_ratios",
                "fred_revision_stats",
                "dim_series",
                "dim_date",
                "market_calendar",
                "macro_indicator_dashboard",
                "macro_indicator_sparkline",
                "macro_category_summary",
                "treasury_curve",
                "treasury_curve_metrics",
                "yield_curve_ns_factors",
                "curve_spread_daily",
                "spread_inversion_episode",
                "benchmark_rate_board",
                "funding_tape_daily",
                "funding_stress_daily",
                "credit_spread_daily",
                "inflation_explorer",
                "inflation_contribution",
                "curve_spread_rolling",
                "credit_spread_rolling",
                "treasury_curve_rolling",
                "macro_regime_daily",
                "series_correlation",
                "series_lead_lag",
                "series_structural_breaks",
                "global_inflation",
                "global_policy_rates",
                "powerbi_catalog",
                "equity_return_daily",
                "index_constituents",
                "equity_total_return_index",
                "equity_price_reconciliation",
                "realized_volatility",
                "ml_feature_matrix",
                "macro_factor_scores",
                "macro_factor_loadings",
                "macro_anomaly_scores",
                "equity_factor_attribution",
                "equity_factor_implied_return",
                "recession_probability_daily",
                "inflation_forecast",
            )
        }

    def point_in_time_features(self, as_of: str) -> list[dict[str, Any]]:
        """Each series' value as known on ``as_of`` (leakage-free snapshot)."""
        from fred_pipeline.features import point_in_time_snapshot

        silver = self._read("silver_fred_observation")
        for r in silver:
            r["is_missing"] = bool(r.get("is_missing"))
        return point_in_time_snapshot(silver, as_of)

    def write_lifecycle(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_fred_series_lifecycle", rows)

    def write_drift(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_fred_series_drift", rows)

    def latest_observation_dates(
        self, series_ids: Sequence[str] | None = None
    ) -> dict[str, str]:
        """Most recent ingested ``observation_date`` per series, any source.

        Reads already-ingested Silver rows rather than calling a live API, so
        it works uniformly across FRED, Tiingo, BLS, EIA, etc. Used by
        source-agnostic staleness checks (see ``governance/reconcile.py``).
        """
        if series_ids is not None and not series_ids:
            return {}
        sql = (
            "SELECT series_id, MAX(observation_date) FROM silver_fred_observation "
            "WHERE is_missing = 0"
        )
        params: list[Any] = []
        if series_ids:
            placeholders = ",".join("?" * len(series_ids))
            sql += f" AND series_id IN ({placeholders})"
            params.extend(series_ids)
        sql += " GROUP BY series_id"
        cur = self.conn.execute(sql, params)
        return {row[0]: row[1] for row in cur.fetchall() if row[1] is not None}

    def write_staleness(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_series_staleness", rows)

    def write_release_calendar(self, rows: list[dict[str, Any]]) -> int:
        # Full-refresh (not append): it's a re-fetched forward schedule, not
        # an accumulating observation history, so overwrite each run.
        self.conn.execute("DELETE FROM gold_release_calendar")
        self.conn.commit()
        return self._insert("gold_release_calendar", rows)

    def persist_run_state(self, run: EtlRun) -> None:
        self._insert("audit_etl_run", [run.to_row()], upsert_keys=["run_id"])

    def persist_series_run(self, series_run: EtlSeriesRun) -> None:
        self.conn.execute(
            "DELETE FROM audit_etl_series_run WHERE run_id = ? AND series_id = ?",
            (series_run.run_id, series_run.series_id),
        )
        self._insert("audit_etl_series_run", [series_run.to_row()])

    def persist_run(self, run: EtlRun) -> None:
        self.persist_run_state(run)
        for series_run in run.series_runs:
            self.persist_series_run(series_run)

    def persist_dq(self, run_id: str, report: QualityReport) -> None:
        self._insert("audit_data_quality_result", dq_rows(run_id, report))

    def close(self) -> None:
        self.conn.close()

    # ---- convenience for interactive/local use -------------------------

    def query(
        self,
        sql: str,
        params: Sequence[Any] = (),
        *,
        caller: str = "",
    ) -> list[dict[str, Any]]:
        """Run an ad-hoc SQL query and return rows as dicts.

        Logs to ``audit_query_log`` (docs/handoffs/
        governance_and_access_control.md item 2) -- a query-level access log
        for this backend, which has no Unity-Catalog-style built-in one.
        ``caller`` is an optional free-text label for who/what issued the
        query (e.g. a notebook name); left blank when unknown. The query
        text itself isn't stored, only a hash, so the log can't leak
        sensitive literals embedded in ad-hoc SQL.
        """
        self._log_query(sql, caller=caller)
        cur = self.conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def _log_query(self, sql: str, *, caller: str = "") -> None:
        query_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        self.conn.execute(
            "INSERT INTO audit_query_log (queried_at, query_text_hash, caller) "
            "VALUES (?, ?, ?)",
            (_dt.datetime.now(_dt.timezone.utc).isoformat(), query_hash, caller),
        )
        self.conn.commit()

    def tables(self) -> list[str]:
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]
