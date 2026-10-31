"""Gold layer: analytics-ready feature tables and point-in-time views.

Gold is rebuilt from Silver with pure SQL so it stays reproducible and cheap to
reason about:

  * ``gold.fred_latest_observation`` — one row per (series, date) at its latest
    revision (the "as revised today" view).
  * ``gold.fred_point_in_time``       — full vintage history for ALFRED-style
    "what was known on date X" queries.
  * ``gold.fred_macro_feature_daily``  — a daily, forward-filled feature matrix
    suitable for optimizer inputs and ML.

The SQL templates live here so the orchestrator can refresh Gold in one call;
equivalent DDL/DML also ships in ``sql/50_gold.sql`` and ``sql/60_views.sql``.
"""

from __future__ import annotations

from typing import Any

from fred_pipeline.config import PipelineConfig
from fred_pipeline.spark_io import get_spark


def _latest_observation_sql(config: PipelineConfig) -> str:
    silver = config.table("silver", "fred_observation")
    gold = config.table("gold", "fred_latest_observation")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY series_id, observation_date
                ORDER BY realtime_start DESC
            ) AS rn
        FROM {silver}
    )
    SELECT
        series_id,
        observation_date,
        value,
        realtime_start,
        realtime_end,
        is_missing,
        revision_number,
        ingested_at
    FROM ranked
    WHERE rn = 1
    """


def _point_in_time_sql(config: PipelineConfig) -> str:
    silver = config.table("silver", "fred_observation")
    gold = config.table("gold", "fred_point_in_time")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    SELECT
        series_id,
        observation_date,
        realtime_start,
        realtime_end,
        value,
        revision_number,
        is_missing,
        ingested_at
    FROM {silver}
    """


def _macro_feature_daily_sql(config: PipelineConfig) -> str:
    """Daily, forward-filled feature matrix built from latest observations."""
    latest = config.table("gold", "fred_latest_observation")
    gold = config.table("gold", "fred_macro_feature_daily")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH bounds AS (
        SELECT MIN(observation_date) AS min_d, MAX(observation_date) AS max_d
        FROM {latest}
    ),
    calendar AS (
        SELECT explode(sequence(
            (SELECT min_d FROM bounds),
            (SELECT max_d FROM bounds),
            INTERVAL 1 DAY
        )) AS as_of_date
    ),
    series_list AS (
        SELECT DISTINCT series_id FROM {latest}
    ),
    grid AS (
        SELECT c.as_of_date, s.series_id
        FROM calendar c CROSS JOIN series_list s
    ),
    joined AS (
        SELECT
            g.as_of_date,
            g.series_id,
            l.value,
            last_value(l.value, true) OVER (
                PARTITION BY g.series_id ORDER BY g.as_of_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS value_ffill
        FROM grid g
        LEFT JOIN {latest} l
          ON g.series_id = l.series_id AND g.as_of_date = l.observation_date
    )
    SELECT as_of_date, series_id, value AS raw_value, value_ffill AS value
    FROM joined
    """


def _feature_transforms_sql(config: PipelineConfig) -> str:
    """MoM / diff / YoY / z-score per series from latest observations.

    Note: YoY here matches the exact −12-month observation (works for monthly/
    quarterly month-end series). The Local/SQLite backend uses nearest-on-or-
    before within a tolerance; results agree for regular monthly/quarterly data.

    z-score is an *expanding* (point-in-time safe) mean/std — the window frame
    is bounded to UNBOUNDED PRECEDING..CURRENT ROW, so each row's mean/std only
    reflects observations at-or-before its own date, never later ones. A plain
    ``PARTITION BY series_id`` (whole-partition) aggregate would leak future
    values into every historical row.
    """
    latest = config.table("gold", "fred_latest_observation")
    gold = config.table("gold", "fred_feature_transforms")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH base AS (
        SELECT series_id, observation_date, value
        FROM {latest} WHERE is_missing = false
    ),
    w AS (
        SELECT series_id, observation_date, value,
            LAG(value) OVER (PARTITION BY series_id ORDER BY observation_date) AS prev_value,
            AVG(value) OVER (
                PARTITION BY series_id ORDER BY observation_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS mean_v,
            STDDEV_POP(value) OVER (
                PARTITION BY series_id ORDER BY observation_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS std_v
        FROM base
    ),
    ya AS (
        SELECT a.*, b.value AS year_ago
        FROM w a
        LEFT JOIN base b
          ON b.series_id = a.series_id
         AND b.observation_date = add_months(a.observation_date, -12)
    )
    SELECT series_id, observation_date, value,
        CASE WHEN prev_value IS NOT NULL AND prev_value <> 0
             THEN (value - prev_value) / prev_value END AS mom,
        CASE WHEN prev_value IS NOT NULL THEN value - prev_value END AS diff,
        CASE WHEN year_ago IS NOT NULL AND year_ago <> 0
             THEN (value - year_ago) / year_ago END AS yoy,
        CASE WHEN std_v IS NOT NULL AND std_v <> 0
             THEN (value - mean_v) / std_v END AS zscore
    FROM ya
    """


def _curve_spread_sql(config: PipelineConfig) -> str:
    """Cross-series spreads/ratios, defined in ``config/spreads.yml`` (see
    :func:`fred_pipeline.spread_config.load_spread_defs`) rather than
    hardcoded, so a reviewer can add new pairs without touching this file."""
    from fred_pipeline.spread_config import load_spread_defs

    latest = config.table("gold", "fred_latest_observation")
    gold = config.table("gold", "fred_curve_spread")
    legs = []
    for sd in load_spread_defs():
        if sd.op == "ratio":
            value_sql = "a.value / b.value"
            zero_guard = "\n          AND b.value <> 0"
        else:
            value_sql = "a.value - b.value"
            zero_guard = ""
        legs.append(f"""
        SELECT '{sd.name}' AS spread_name, a.observation_date,
               '{sd.long_leg}' AS long_leg, '{sd.short_leg}' AS short_leg,
               {value_sql} AS value
        FROM {latest} a JOIN {latest} b ON a.observation_date = b.observation_date
        WHERE a.series_id = '{sd.long_leg}' AND b.series_id = '{sd.short_leg}'
          AND a.is_missing = false AND b.is_missing = false{zero_guard}
        """)
    union = "\nUNION ALL\n".join(legs)
    return f"CREATE OR REPLACE TABLE {gold} AS\n{union}"


def _revision_stats_sql(config: PipelineConfig) -> str:
    """How much each observation moved between its first print and today.

    Unlike the other Gold tables (built from ``gold.fred_latest_observation``),
    this reads raw Silver (every vintage), since it exists to measure revision
    behavior itself. Non-vintage series (blank ``realtime_start``) always have
    ``revision_count = 1`` — no vintage history is tracked for them.
    """
    silver = config.table("silver", "fred_observation")
    gold = config.table("gold", "fred_revision_stats")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH base AS (
        SELECT series_id, observation_date, realtime_start, value, revision_number
        FROM {silver}
        WHERE is_missing = false AND value IS NOT NULL
    ),
    bounds AS (
        SELECT series_id, observation_date,
            MIN(revision_number) AS min_rev, MAX(revision_number) AS max_rev,
            COUNT(*) AS revision_count
        FROM base
        GROUP BY series_id, observation_date
    )
    SELECT b.series_id, b.observation_date, b.revision_count,
        f.value AS first_value, f.realtime_start AS first_realtime_start,
        l.value AS latest_value, l.realtime_start AS latest_realtime_start,
        l.value - f.value AS revision_delta,
        CASE WHEN f.value <> 0 THEN (l.value - f.value) / f.value END AS revision_pct
    FROM bounds b
    JOIN base f ON f.series_id = b.series_id AND f.observation_date = b.observation_date
              AND f.revision_number = b.min_rev
    JOIN base l ON l.series_id = b.series_id AND l.observation_date = b.observation_date
              AND l.revision_number = b.max_rev
    """


def point_in_time_features_sql(config: PipelineConfig, as_of: str) -> str:
    """Ad-hoc SQL: each series' value as known on ``as_of`` (leakage-free)."""
    silver = config.table("silver", "fred_observation")
    return f"""
    WITH known AS (
        SELECT series_id, observation_date, value, realtime_start
        FROM {silver}
        WHERE is_missing = false
          AND realtime_start <= DATE '{as_of}'
          AND (realtime_end IS NULL OR realtime_end > DATE '{as_of}')
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY series_id
            ORDER BY observation_date DESC, realtime_start DESC
        ) AS rn
        FROM known
    )
    SELECT DATE '{as_of}' AS as_of_date, series_id, observation_date, value
    FROM ranked WHERE rn = 1
    """


def _build_ecb_fx_reference_rates(config: PipelineConfig, spark: Any) -> None:
    """Write exact, non-forward-filled ECB native/inverse/cross FX rates."""
    from pyspark.sql.types import StringType, StructField, StructType

    from fred_pipeline.writer.features import compute_ecb_fx_reference_rates

    silver = config.table("silver", "fred_observation")
    gold = config.table("gold", "ecb_fx_reference_rate")
    input_rows = [
        row.asDict()
        for row in spark.sql(
            f"""
            SELECT source, series_id, CAST(observation_date AS STRING) AS observation_date,
                   CAST(realtime_start AS STRING) AS realtime_start, value, raw_value,
                   exact_value, is_missing, is_deleted, provider_valid_from,
                   base_currency, quote_currency, observation_event_id
            FROM {silver}
            WHERE source = 'ecb'
            """
        ).collect()
    ]
    rows = compute_ecb_fx_reference_rates(input_rows)
    schema = StructType(
        [
            StructField("observation_date", StringType()),
            StructField("base_currency", StringType()),
            StructField("quote_currency", StringType()),
            StructField("orientation", StringType()),
            StructField("rate_decimal", StringType()),
            StructField("rounded_rate_decimal", StringType()),
            StructField("rounding_policy_version", StringType()),
            StructField("native_series_ids", StringType()),
            StructField("input_event_ids", StringType()),
            StructField("availability_state", StringType()),
            StructField("quality_reason", StringType()),
        ]
    )
    (
        spark.createDataFrame(rows, schema=schema)
        .selectExpr(
            "CAST(observation_date AS DATE) AS observation_date",
            *schema.fieldNames()[1:],
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold)
    )


def _build_cross_series(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_cross_series_feature`` on Spark by reusing the pure-Python
    reference (:func:`fred_pipeline.features.compute_cross_series_features`).

    Cross-series features involve only the handful of leg series named in
    ``config/cross_series.yml``, so we read just those from the (already-built)
    latest-observation table, collect them, compute in Python — guaranteeing
    parity with the local backend — and overwrite the table.
    """
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.cross_series_config import load_cross_series_defs
    from fred_pipeline.features import compute_cross_series_features

    gold = config.table("gold", "fred_cross_series_feature")
    defs = load_cross_series_defs()
    leg_ids = sorted({sid for d in defs for (sid, _w) in d.legs})

    feats: list[dict[str, Any]] = []
    if leg_ids:
        latest = config.table("gold", "fred_latest_observation")
        in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in leg_ids)
        df = spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"value, is_missing FROM {latest} WHERE series_id IN ({in_list})"
        )
        rows = [r.asDict() for r in df.collect()]
        feats = compute_cross_series_features(rows, defs)

    # Explicit schema so the empty case still creates a well-typed table.
    schema = StructType(
        [
            StructField("feature_name", StringType()),
            StructField("op", StringType()),
            StructField("observation_date", StringType()),
            StructField("value", DoubleType()),
        ]
    )
    out = spark.createDataFrame(feats, schema=schema).selectExpr(
        "feature_name",
        "op",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value",
    )
    out.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(gold)


def _build_cross_series_pit(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_cross_series_feature_pit`` — the point-in-time
    (as-first-reported) cross-series features — by reusing the pure-Python
    reference (:func:`fred_pipeline.features.compute_cross_series_features_pit`).

    Reads the configured leg series' **full vintage history** from Silver (this
    variant needs ``realtime_start``), collects them, computes in Python, and
    overwrites the table."""
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.cross_series_config import load_cross_series_defs
    from fred_pipeline.features import compute_cross_series_features_pit

    gold = config.table("gold", "fred_cross_series_feature_pit")
    defs = load_cross_series_defs()
    leg_ids = sorted({sid for d in defs for (sid, _w) in d.legs})

    rows_out: list[dict[str, Any]] = []
    if leg_ids:
        silver = config.table("silver", "fred_observation")
        in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in leg_ids)
        df = spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"CAST(realtime_start AS STRING) AS realtime_start, value, is_missing "
            f"FROM {silver} WHERE series_id IN ({in_list})"
        )
        rows = [r.asDict() for r in df.collect()]
        rows_out = compute_cross_series_features_pit(rows, defs)

    schema = StructType(
        [
            StructField("feature_name", StringType()),
            StructField("op", StringType()),
            StructField("observation_date", StringType()),
            StructField("value", DoubleType()),
            StructField("basis", StringType()),
        ]
    )
    out = spark.createDataFrame(rows_out, schema=schema).selectExpr(
        "feature_name",
        "op",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value",
        "basis",
    )
    out.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(gold)


def _build_company_financials(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_company_fundamentals`` + ``gold.fred_company_ratios`` by
    reusing the pure-Python standardizer (:mod:`fred_pipeline.sec_standardization`).

    Reads the ``source='sec'`` slice of Silver (bounded by the active SEC series),
    standardizes raw XBRL tags into canonical concepts, computes ratios, and
    overwrites both tables. Priority tag-coalescing is impractical in SQL, so —
    like the other cross-cutting Gold tables — both backends share the one
    Python engine."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    from fred_pipeline.sec_standardization import (
        compute_sec_ratios,
        standardize_sec_statements,
    )

    silver = config.table("silver", "fred_observation")
    df = spark.sql(
        f"SELECT source, series_id, "
        f"CAST(observation_date AS STRING) AS observation_date, "
        f"CAST(realtime_start AS STRING) AS realtime_start, value, is_missing "
        f"FROM {silver} WHERE source = 'sec'"
    )
    rows = [r.asDict() for r in df.collect()]
    fundamentals = standardize_sec_statements(rows)
    ratios = compute_sec_ratios(fundamentals)

    fund_schema = StructType(
        [
            StructField("cik", StringType()),
            StructField("concept", StringType()),
            StructField("statement", StringType()),
            StructField("observation_date", StringType()),
            StructField("value", DoubleType()),
        ]
    )
    spark.createDataFrame(fundamentals, schema=fund_schema).selectExpr(
        "cik",
        "concept",
        "statement",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "fred_company_fundamentals"))

    ratio_schema = StructType(
        [
            StructField("cik", StringType()),
            StructField("ratio_name", StringType()),
            StructField("observation_date", StringType()),
            StructField("value", DoubleType()),
        ]
    )
    spark.createDataFrame(ratios, schema=ratio_schema).selectExpr(
        "cik",
        "ratio_name",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "fred_company_ratios"))


def _build_source_reconciliation(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_source_reconciliation`` on Spark by reusing the pure-Python
    reference (:func:`fred_pipeline.features.compute_source_reconciliation`) — same
    collect-legs-and-compute pattern as :func:`_build_cross_series`, so both
    backends stay in parity."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.features import compute_source_reconciliation
    from fred_pipeline.reconciliation_config import load_reconciliation_defs

    gold = config.table("gold", "fred_source_reconciliation")
    defs = load_reconciliation_defs()
    series = sorted({s for d in defs for s in (d.series_a, d.series_b)})

    rows_out: list[dict[str, Any]] = []
    if series:
        latest = config.table("gold", "fred_latest_observation")
        in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in series)
        df = spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"value, is_missing FROM {latest} WHERE series_id IN ({in_list})"
        )
        rows = [r.asDict() for r in df.collect()]
        rows_out = compute_source_reconciliation(rows, defs)

    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("observation_date", StringType()),
            StructField("series_a", StringType()),
            StructField("value_a", DoubleType()),
            StructField("series_b", StringType()),
            StructField("value_b", DoubleType()),
            StructField("abs_diff", DoubleType()),
            StructField("pct_diff", DoubleType()),
            StructField("diverged", BooleanType()),
        ]
    )
    out = spark.createDataFrame(rows_out, schema=schema).selectExpr(
        "name",
        "CAST(observation_date AS DATE) AS observation_date",
        "series_a",
        "CAST(value_a AS DOUBLE) AS value_a",
        "series_b",
        "CAST(value_b AS DOUBLE) AS value_b",
        "CAST(abs_diff AS DOUBLE) AS abs_diff",
        "CAST(pct_diff AS DOUBLE) AS pct_diff",
        "diverged",
    )
    out.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(gold)


def _collect_latest(
    config: PipelineConfig, spark: Any, series_ids: list[str]
) -> list[dict[str, Any]]:
    """Collect the named series from ``gold.fred_latest_observation`` as plain
    dicts (dates as ISO strings) — the input shape the pure-Python
    terminal-view engines expect."""
    if not series_ids:
        return []
    latest = config.table("gold", "fred_latest_observation")
    in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in series_ids)
    df = spark.sql(
        f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
        f"CAST(realtime_start AS STRING) AS realtime_start, value, is_missing "
        f"FROM {latest} WHERE series_id IN ({in_list})"
    )
    return [r.asDict() for r in df.collect()]


def _build_terminal_views(config: PipelineConfig, spark: Any) -> None:
    """Build the market-terminal analytical views (dimensions + ECON macro
    dashboard + Treasury Curve Lab; see ``docs/market_terminal_gold_views.md``)
    by reusing the pure-Python engines in :mod:`fred_pipeline.terminal_views` —
    the same collect-and-compute pattern as :func:`_build_cross_series`, so
    both backends stay in parity. Inputs are bounded: only cataloged series,
    curve tenors, spread legs, and USREC are collected."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.catalog_config import load_series_catalog
    from fred_pipeline.curve_config import load_curve_defs
    from fred_pipeline.spread_config import load_spread_defs
    from fred_pipeline.inflation_config import load_inflation_items
    from fred_pipeline.gold_config.mmkt_config import load_mmkt_config
    from fred_pipeline.rates_complex_config import (
        load_benchmark_board,
        load_credit_config,
        load_funding_config,
    )
    from fred_pipeline.writer.mmkt_views import compute_mmkt_rate_snapshot
    from fred_pipeline.terminal_views import (
        RECESSION_SERIES,
        build_dim_date,
        build_dim_series,
        compute_benchmark_rate_board,
        compute_credit_spread_daily,
        compute_credit_spread_rolling,
        compute_curve_spread_daily,
        compute_curve_spread_rolling,
        compute_funding_features,
        compute_inflation_explorer,
        compute_macro_dashboard,
        compute_market_calendar,
        compute_spread_inversion_episodes,
        compute_treasury_curve,
        compute_treasury_curve_rolling,
    )

    def _write(
        name: str, rows: list[dict[str, Any]], schema: Any, casts: list[str]
    ) -> None:
        spark.createDataFrame(rows, schema=schema).selectExpr(*casts).write.format(
            "delta"
        ).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            config.table("gold", name)
        )

    catalog = load_series_catalog()
    tenors = load_curve_defs()
    spreads = load_spread_defs()

    # dim_series: catalog semantics + title/frequency/units from meta.
    meta_df = spark.sql(
        f"SELECT series_id, title, frequency, units FROM "
        f"{config.table('meta', 'fred_series')}"
    )
    dim_series = build_dim_series(catalog, [r.asDict() for r in meta_df.collect()])
    _write(
        "dim_series",
        dim_series,
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("title", StringType()),
                StructField("source", StringType()),
                StructField("frequency", StringType()),
                StructField("units", StringType()),
                StructField("econ_category", StringType()),
                StructField("polarity", IntegerType()),
                StructField("default_transform", StringType()),
                StructField("scale", StringType()),
                StructField("decimals", IntegerType()),
                # geo/metric: the REGIONAL panel's map key and measure name. Must stay
                # in step with build_dim_series' output keys, the gold_dim_series DDL in
                # local_store, and sql/50_gold.sql -- guarded by
                # tests/test_dim_series_schema_parity.py.
                StructField("geo", StringType()),
                StructField("metric", StringType()),
                StructField("notes", StringType()),
            ]
        ),
        ["*"],
    )

    # dim_date: calendar bounds from latest observations + the USREC overlay.
    latest = config.table("gold", "fred_latest_observation")
    bounds = spark.sql(
        f"SELECT CAST(MIN(observation_date) AS STRING) AS lo, "
        f"CAST(MAX(observation_date) AS STRING) AS hi FROM {latest} "
        f"WHERE is_missing = false"
    ).collect()[0]
    usrec_rows = _collect_latest(config, spark, [RECESSION_SERIES])
    dim_date = (
        build_dim_date(bounds["lo"], bounds["hi"], usrec_rows) if bounds["lo"] else []
    )
    # All 60 calendar columns. build_dim_date emits every one and
    # sql/50_gold.sql declares every one; an abbreviated StructType here
    # silently drops the remainder on the Delta write while the SQLite
    # mirror stays complete -- which is exactly what happened, costing
    # Databricks the entire time-intelligence surface (date_key, ISO week,
    # the fiscal calendar, every sort-by column Power BI needs). Guarded by
    # test_terminal_view_schema_matches_shipped_ddl in the Spark suite.
    _write(
        "dim_date",
        dim_date,
        StructType(
            [
                StructField("date", StringType()),
                StructField("date_key", IntegerType()),
                StructField("year", IntegerType()),
                StructField("year_label", StringType()),
                StructField("year_start_date", StringType()),
                StructField("year_end_date", StringType()),
                StructField("is_year_start", BooleanType()),
                StructField("is_year_end", BooleanType()),
                StructField("is_leap_year", BooleanType()),
                StructField("quarter", IntegerType()),
                StructField("quarter_label", StringType()),
                StructField("year_quarter", StringType()),
                StructField("year_quarter_sort", IntegerType()),
                StructField("quarter_start_date", StringType()),
                StructField("quarter_end_date", StringType()),
                StructField("is_quarter_start", BooleanType()),
                StructField("is_quarter_end", BooleanType()),
                StructField("month", IntegerType()),
                StructField("month_name", StringType()),
                StructField("month_short_name", StringType()),
                StructField("year_month", StringType()),
                StructField("year_month_sort", IntegerType()),
                StructField("month_start_date", StringType()),
                StructField("month_end_date", StringType()),
                StructField("is_month_start", BooleanType()),
                StructField("is_month_end", BooleanType()),
                StructField("days_in_month", IntegerType()),
                StructField("iso_year", IntegerType()),
                StructField("week_of_year", IntegerType()),
                StructField("year_week", StringType()),
                StructField("week_start_date", StringType()),
                StructField("week_end_date", StringType()),
                StructField("is_week_start", BooleanType()),
                StructField("is_week_end", BooleanType()),
                StructField("day_of_month", IntegerType()),
                StructField("day_of_year", IntegerType()),
                StructField("day_name", StringType()),
                StructField("day_short_name", StringType()),
                StructField("day_of_week_iso", IntegerType()),
                StructField("day_of_week_sun", IntegerType()),
                StructField("is_weekday", BooleanType()),
                StructField("is_weekend", BooleanType()),
                StructField("fiscal_year", IntegerType()),
                StructField("fiscal_year_label", StringType()),
                StructField("fiscal_quarter", IntegerType()),
                StructField("fiscal_quarter_label", StringType()),
                StructField("fiscal_month", IntegerType()),
                StructField("fiscal_year_quarter_sort", IntegerType()),
                StructField("fiscal_year_start_date", StringType()),
                StructField("fiscal_year_end_date", StringType()),
                StructField("fiscal_quarter_start_date", StringType()),
                StructField("fiscal_quarter_end_date", StringType()),
                StructField("is_fiscal_year_start", BooleanType()),
                StructField("is_fiscal_year_end", BooleanType()),
                StructField("is_fiscal_quarter_start", BooleanType()),
                StructField("is_fiscal_quarter_end", BooleanType()),
                StructField("is_recession", BooleanType()),
                StructField("is_imm_date", BooleanType()),
                StructField("is_monthly_option_expiry", BooleanType()),
                StructField("is_triple_witching", BooleanType()),
            ]
        ),
        [
            "CAST(date AS DATE) AS date",
            "date_key",
            "year",
            "year_label",
            "CAST(year_start_date AS DATE) AS year_start_date",
            "CAST(year_end_date AS DATE) AS year_end_date",
            "is_year_start",
            "is_year_end",
            "is_leap_year",
            "quarter",
            "quarter_label",
            "year_quarter",
            "year_quarter_sort",
            "CAST(quarter_start_date AS DATE) AS quarter_start_date",
            "CAST(quarter_end_date AS DATE) AS quarter_end_date",
            "is_quarter_start",
            "is_quarter_end",
            "month",
            "month_name",
            "month_short_name",
            "year_month",
            "year_month_sort",
            "CAST(month_start_date AS DATE) AS month_start_date",
            "CAST(month_end_date AS DATE) AS month_end_date",
            "is_month_start",
            "is_month_end",
            "days_in_month",
            "iso_year",
            "week_of_year",
            "year_week",
            "CAST(week_start_date AS DATE) AS week_start_date",
            "CAST(week_end_date AS DATE) AS week_end_date",
            "is_week_start",
            "is_week_end",
            "day_of_month",
            "day_of_year",
            "day_name",
            "day_short_name",
            "day_of_week_iso",
            "day_of_week_sun",
            "is_weekday",
            "is_weekend",
            "fiscal_year",
            "fiscal_year_label",
            "fiscal_quarter",
            "fiscal_quarter_label",
            "fiscal_month",
            "fiscal_year_quarter_sort",
            "CAST(fiscal_year_start_date AS DATE) AS fiscal_year_start_date",
            "CAST(fiscal_year_end_date AS DATE) AS fiscal_year_end_date",
            "CAST(fiscal_quarter_start_date AS DATE) AS fiscal_quarter_start_date",
            "CAST(fiscal_quarter_end_date AS DATE) AS fiscal_quarter_end_date",
            "is_fiscal_year_start",
            "is_fiscal_year_end",
            "is_fiscal_quarter_start",
            "is_fiscal_quarter_end",
            "is_recession",
            "is_imm_date",
            "is_monthly_option_expiry",
            "is_triple_witching",
        ],
    )

    # Market calendars (NYSE/SIFMA/FEDWIRE): holiday-aware business-day logic,
    # long/tidy by calendar_name, over the same date bounds as dim_date.
    market_calendar = (
        compute_market_calendar(bounds["lo"], bounds["hi"]) if bounds["lo"] else []
    )
    _write(
        "market_calendar",
        market_calendar,
        StructType(
            [
                StructField("calendar_name", StringType()),
                StructField("calendar_date", StringType()),
                StructField("is_weekend", BooleanType()),
                StructField("is_holiday", BooleanType()),
                StructField("holiday_name", StringType()),
                StructField("day_type", StringType()),
                StructField("is_business_day", BooleanType()),
                StructField("prior_business_day", StringType()),
                StructField("next_business_day", StringType()),
                StructField("t2_settle_date", StringType()),
                StructField("business_day_of_month", IntegerType()),
                StructField("business_days_in_month", IntegerType()),
                StructField("is_first_business_day_of_month", BooleanType()),
                StructField("is_last_business_day_of_month", BooleanType()),
                StructField("is_last_business_day_of_quarter", BooleanType()),
                StructField("is_last_business_day_of_year", BooleanType()),
            ]
        ),
        [
            "calendar_name",
            "CAST(calendar_date AS DATE) AS calendar_date",
            "is_weekend",
            "is_holiday",
            "holiday_name",
            "day_type",
            "is_business_day",
            "CAST(prior_business_day AS DATE) AS prior_business_day",
            "CAST(next_business_day AS DATE) AS next_business_day",
            "CAST(t2_settle_date AS DATE) AS t2_settle_date",
            "business_day_of_month",
            "business_days_in_month",
            "is_first_business_day_of_month",
            "is_last_business_day_of_month",
            "is_last_business_day_of_quarter",
            "is_last_business_day_of_year",
        ],
    )

    # ECON macro dashboard (+ sparkline + category summary) over the catalog.
    dash = compute_macro_dashboard(
        _collect_latest(config, spark, sorted(e.series_id for e in catalog)),
        catalog,
    )
    _write(
        "macro_indicator_dashboard",
        dash["dashboard"],
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("econ_category", StringType()),
                StructField("polarity", IntegerType()),
                StructField("default_transform", StringType()),
                StructField("as_of_date", StringType()),
                StructField("latest_date", StringType()),
                StructField("latest_value", DoubleType()),
                StructField("prior_date", StringType()),
                StructField("prior_value", DoubleType()),
                StructField("change_abs", DoubleType()),
                StructField("change_pct", DoubleType()),
                StructField("yoy_pct", DoubleType()),
                StructField("zscore", DoubleType()),
                StructField("percentile", DoubleType()),
                StructField("surprise", DoubleType()),
                StructField("surprise_z", DoubleType()),
                StructField("direction_is_good", BooleanType()),
                StructField("spark_min", DoubleType()),
                StructField("spark_max", DoubleType()),
                StructField("staleness_days", IntegerType()),
                StructField("realtime_start", StringType()),
            ]
        ),
        [
            "series_id",
            "econ_category",
            "polarity",
            "default_transform",
            "CAST(as_of_date AS DATE) AS as_of_date",
            "CAST(latest_date AS DATE) AS latest_date",
            "latest_value",
            "CAST(prior_date AS DATE) AS prior_date",
            "prior_value",
            "change_abs",
            "change_pct",
            "yoy_pct",
            "zscore",
            "percentile",
            "surprise",
            "surprise_z",
            "direction_is_good",
            "spark_min",
            "spark_max",
            "staleness_days",
            "realtime_start",
        ],
    )
    _write(
        "macro_indicator_sparkline",
        dash["sparkline"],
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("point_index", IntegerType()),
                StructField("observation_date", StringType()),
                StructField("value", DoubleType()),
            ]
        ),
        [
            "series_id",
            "point_index",
            "CAST(observation_date AS DATE) AS observation_date",
            "value",
        ],
    )
    _write(
        "macro_category_summary",
        dash["category_summary"],
        StructType(
            [
                StructField("econ_category", StringType()),
                StructField("as_of_date", StringType()),
                StructField("n_series", IntegerType()),
                StructField("n_improving", IntegerType()),
                StructField("n_deteriorating", IntegerType()),
                StructField("breadth_pct", DoubleType()),
                StructField("avg_zscore", DoubleType()),
                StructField("surprise_index", DoubleType()),
            ]
        ),
        [
            "econ_category",
            "CAST(as_of_date AS DATE) AS as_of_date",
            "n_series",
            "n_improving",
            "n_deteriorating",
            "breadth_pct",
            "avg_zscore",
            "surprise_index",
        ],
    )

    # Treasury Curve Lab: tenor series + USREC.
    curve_rows = _collect_latest(
        config,
        spark,
        sorted({t.series_id for t in tenors} | {RECESSION_SERIES}),
    )
    curve = compute_treasury_curve(curve_rows, tenors)
    _write(
        "treasury_curve",
        curve["curve"],
        StructType(
            [
                StructField("as_of_date", StringType()),
                StructField("tenor_label", StringType()),
                StructField("tenor_months", IntegerType()),
                StructField("series_id", StringType()),
                StructField("yield_pct", DoubleType()),
            ]
        ),
        [
            "CAST(as_of_date AS DATE) AS as_of_date",
            "tenor_label",
            "tenor_months",
            "series_id",
            "yield_pct",
        ],
    )
    _write(
        "treasury_curve_metrics",
        curve["metrics"],
        StructType(
            [
                StructField("as_of_date", StringType()),
                StructField("level", DoubleType()),
                StructField("slope_10y2y", DoubleType()),
                StructField("slope_10y3m", DoubleType()),
                StructField("curvature_2_5_10", DoubleType()),
                StructField("butterfly_2_10_30", DoubleType()),
                StructField("is_inverted_10y2y", BooleanType()),
                StructField("is_inverted_10y3m", BooleanType()),
                StructField("is_recession", BooleanType()),
                StructField("curve_move", StringType()),
            ]
        ),
        [
            "CAST(as_of_date AS DATE) AS as_of_date",
            "level",
            "slope_10y2y",
            "slope_10y3m",
            "curvature_2_5_10",
            "butterfly_2_10_30",
            "is_inverted_10y2y",
            "is_inverted_10y3m",
            "is_recession",
            "curve_move",
        ],
    )
    _write(
        "treasury_curve_rolling",
        compute_treasury_curve_rolling(curve_rows, tenors),
        StructType(
            [
                StructField("tenor_label", StringType()),
                StructField("tenor_months", IntegerType()),
                StructField("series_id", StringType()),
                StructField("observation_date", StringType()),
                StructField("window", IntegerType()),
                StructField("yield_pct", DoubleType()),
                StructField("change", DoubleType()),
                StructField("pct_change", DoubleType()),
                StructField("zscore", DoubleType()),
            ]
        ),
        [
            "tenor_label",
            "tenor_months",
            "series_id",
            "CAST(observation_date AS DATE) AS observation_date",
            "window",
            "yield_pct",
            "change",
            "pct_change",
            "zscore",
        ],
    )

    # ML-1: Nelson-Siegel three-factor fit to the daily Treasury curve.
    from fred_pipeline.ns_model import compute_yield_curve_ns_factors

    _write(
        "yield_curve_ns_factors",
        compute_yield_curve_ns_factors(curve["curve"]),
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("beta0", DoubleType()),
                StructField("beta1", DoubleType()),
                StructField("beta2", DoubleType()),
                StructField("lambda", DoubleType()),
                StructField("lambda_estimated", BooleanType()),
                StructField("fit_rmse", DoubleType()),
                StructField("n_tenors", IntegerType()),
                StructField("fit_valid", BooleanType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "beta0",
            "beta1",
            "beta2",
            "`lambda`",
            "lambda_estimated",
            "fit_rmse",
            "n_tenors",
            "fit_valid",
        ],
    )

    # Enriched spread history + inversion episodes: spread legs + USREC.
    leg_ids = sorted(
        {s for sd in spreads for s in (sd.long_leg, sd.short_leg)} | {RECESSION_SERIES}
    )
    leg_rows = _collect_latest(config, spark, leg_ids)
    spread_daily = compute_curve_spread_daily(leg_rows, spreads)
    _write(
        "curve_spread_daily",
        spread_daily,
        StructType(
            [
                StructField("spread_name", StringType()),
                StructField("observation_date", StringType()),
                StructField("long_leg", StringType()),
                StructField("short_leg", StringType()),
                StructField("value", DoubleType()),
                StructField("value_bps", DoubleType()),
                StructField("zscore", DoubleType()),
                StructField("percentile", DoubleType()),
                StructField("is_inverted", BooleanType()),
                StructField("inversion_run", IntegerType()),
                StructField("is_recession", BooleanType()),
            ]
        ),
        [
            "spread_name",
            "CAST(observation_date AS DATE) AS observation_date",
            "long_leg",
            "short_leg",
            "value",
            "value_bps",
            "zscore",
            "percentile",
            "is_inverted",
            "inversion_run",
            "is_recession",
        ],
    )
    episodes = compute_spread_inversion_episodes(leg_rows, spreads)
    _write(
        "spread_inversion_episode",
        episodes,
        StructType(
            [
                StructField("spread_name", StringType()),
                StructField("long_leg", StringType()),
                StructField("short_leg", StringType()),
                StructField("episode_number", IntegerType()),
                StructField("start_date", StringType()),
                StructField("end_date", StringType()),
                StructField("last_inverted_date", StringType()),
                StructField("observation_count", IntegerType()),
                StructField("calendar_days", IntegerType()),
                StructField("trough_value", DoubleType()),
                StructField("trough_bps", DoubleType()),
                StructField("trough_date", StringType()),
                StructField("is_ongoing", BooleanType()),
                StructField("recession_overlap", BooleanType()),
            ]
        ),
        [
            "spread_name",
            "long_leg",
            "short_leg",
            "episode_number",
            "CAST(start_date AS DATE) AS start_date",
            "CAST(end_date AS DATE) AS end_date",
            "CAST(last_inverted_date AS DATE) AS last_inverted_date",
            "observation_count",
            "calendar_days",
            "trough_value",
            "trough_bps",
            "CAST(trough_date AS DATE) AS trough_date",
            "is_ongoing",
            "recession_overlap",
        ],
    )
    _write(
        "curve_spread_rolling",
        compute_curve_spread_rolling(leg_rows, spreads),
        StructType(
            [
                StructField("spread_name", StringType()),
                StructField("observation_date", StringType()),
                StructField("window", IntegerType()),
                StructField("value", DoubleType()),
                StructField("change", DoubleType()),
                StructField("pct_change", DoubleType()),
                StructField("zscore", DoubleType()),
            ]
        ),
        [
            "spread_name",
            "CAST(observation_date AS DATE) AS observation_date",
            "window",
            "value",
            "change",
            "pct_change",
            "zscore",
        ],
    )

    # Phase 4 rates complex: BMRK benchmark board, FUND funding tape + stress
    # gauge, CRDT credit spreads (configs under config/).
    board = load_benchmark_board()
    board_ids = sorted(
        {rd.series_id for rd in board.rates}
        | {rd.benchmark for rd in board.rates if rd.benchmark}
    )
    board_rows = compute_benchmark_rate_board(
        _collect_latest(config, spark, board_ids), board
    )
    _write(
        "benchmark_rate_board",
        board_rows,
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("rate_label", StringType()),
                StructField("rate_category", StringType()),
                StructField("benchmark_series", StringType()),
                StructField("as_of_date", StringType()),
                StructField("latest_date", StringType()),
                StructField("latest_value", DoubleType()),
                StructField("prior_value", DoubleType()),
                StructField("change_bps", DoubleType()),
                StructField("trend", StringType()),
                StructField("spread_to_benchmark_bps", DoubleType()),
                StructField("zscore", DoubleType()),
                StructField("percentile", DoubleType()),
                StructField("regime", StringType()),
                StructField("staleness_days", IntegerType()),
            ]
        ),
        [
            "series_id",
            "rate_label",
            "rate_category",
            "benchmark_series",
            "CAST(as_of_date AS DATE) AS as_of_date",
            "CAST(latest_date AS DATE) AS latest_date",
            "latest_value",
            "prior_value",
            "change_bps",
            "trend",
            "spread_to_benchmark_bps",
            "zscore",
            "percentile",
            "regime",
            "staleness_days",
        ],
    )

    funding_cfg = load_funding_config()
    funding_ids = sorted(
        {m.series_id for m in funding_cfg.metrics}
        | {s for sp in funding_cfg.spreads for s in (sp.long_leg, sp.short_leg)}
    )
    funding = compute_funding_features(
        _collect_latest(config, spark, funding_ids), funding_cfg
    )
    _write(
        "funding_tape_daily",
        funding["tape"],
        StructType(
            [
                StructField("metric_name", StringType()),
                StructField("metric_type", StringType()),
                StructField("observation_date", StringType()),
                StructField("value", DoubleType()),
                StructField("zscore", DoubleType()),
                StructField("percentile", DoubleType()),
            ]
        ),
        [
            "metric_name",
            "metric_type",
            "CAST(observation_date AS DATE) AS observation_date",
            "value",
            "zscore",
            "percentile",
        ],
    )
    _write(
        "funding_stress_daily",
        funding["stress"],
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("composite_z", DoubleType()),
                StructField("stress_score", DoubleType()),
                StructField("stress_bucket", StringType()),
                StructField("n_components", IntegerType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "composite_z",
            "stress_score",
            "stress_bucket",
            "n_components",
        ],
    )

    mmkt_cfg = load_mmkt_config()
    mmkt_ids = sorted({instrument.series_id for instrument in mmkt_cfg.instruments})
    mmkt_rows = compute_mmkt_rate_snapshot(
        _collect_latest(config, spark, mmkt_ids), mmkt_cfg
    )
    _write(
        "mmkt_rate_snapshot",
        mmkt_rows,
        StructType(
            [
                StructField("instrument_id", StringType()),
                StructField("label", StringType()),
                StructField("instrument_class", StringType()),
                StructField("tenor_days", IntegerType()),
                StructField("series_id", StringType()),
                StructField("quoted_basis", StringType()),
                StructField("quoted_rate_pct", DoubleType()),
                StructField("normalized_basis", StringType()),
                StructField("mm_yield_pct", DoubleType()),
                StructField("conversion", StringType()),
                StructField("observation_date", StringType()),
                StructField("as_of_date", StringType()),
                StructField("staleness_days", IntegerType()),
                StructField("max_staleness_days", IntegerType()),
                StructField("status", StringType()),
                StructField("status_detail", StringType()),
            ]
        ),
        [
            "instrument_id",
            "label",
            "instrument_class",
            "tenor_days",
            "series_id",
            "quoted_basis",
            "quoted_rate_pct",
            "normalized_basis",
            "mm_yield_pct",
            "conversion",
            "CAST(observation_date AS DATE) AS observation_date",
            "CAST(as_of_date AS DATE) AS as_of_date",
            "staleness_days",
            "max_staleness_days",
            "status",
            "status_detail",
        ],
    )

    credit_cfg = load_credit_config()
    credit_ids = sorted(
        {cd.series_id for cd in credit_cfg.instruments} | {RECESSION_SERIES}
    )
    credit_input = _collect_latest(config, spark, credit_ids)
    credit_rows = compute_credit_spread_daily(credit_input, credit_cfg)
    _write(
        "credit_spread_daily",
        credit_rows,
        StructType(
            [
                StructField("instrument", StringType()),
                StructField("series_id", StringType()),
                StructField("category", StringType()),
                StructField("observation_date", StringType()),
                StructField("oas_pct", DoubleType()),
                StructField("oas_bps", DoubleType()),
                StructField("change_bps", DoubleType()),
                StructField("zscore", DoubleType()),
                StructField("percentile", DoubleType()),
                StructField("is_stress_episode", BooleanType()),
                StructField("is_recession", BooleanType()),
            ]
        ),
        [
            "instrument",
            "series_id",
            "category",
            "CAST(observation_date AS DATE) AS observation_date",
            "oas_pct",
            "oas_bps",
            "change_bps",
            "zscore",
            "percentile",
            "is_stress_episode",
            "is_recession",
        ],
    )
    _write(
        "credit_spread_rolling",
        compute_credit_spread_rolling(credit_input, credit_cfg),
        StructType(
            [
                StructField("instrument", StringType()),
                StructField("series_id", StringType()),
                StructField("observation_date", StringType()),
                StructField("window", IntegerType()),
                StructField("oas_bps", DoubleType()),
                StructField("change_bps", DoubleType()),
                StructField("pct_change", DoubleType()),
                StructField("zscore", DoubleType()),
            ]
        ),
        [
            "instrument",
            "series_id",
            "CAST(observation_date AS DATE) AS observation_date",
            "window",
            "oas_bps",
            "change_bps",
            "pct_change",
            "zscore",
        ],
    )

    # Phase 2 Inflation Explorer (config/inflation_items.yml).
    infl_items = load_inflation_items()
    inflation = compute_inflation_explorer(
        _collect_latest(config, spark, sorted({i.series_id for i in infl_items})),
        infl_items,
    )
    _write(
        "inflation_explorer",
        inflation["explorer"],
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("item_label", StringType()),
                StructField("parent_item", StringType()),
                StructField("hierarchy_level", IntegerType()),
                StructField("basket", StringType()),
                StructField("sa_nsa", StringType()),
                StructField("observation_date", StringType()),
                StructField("index_value", DoubleType()),
                StructField("mom_pct", DoubleType()),
                StructField("yoy_pct", DoubleType()),
                StructField("mom_accel", DoubleType()),
                StructField("yoy_accel", DoubleType()),
                StructField("three_month_annualized", DoubleType()),
                StructField("weight", DoubleType()),
                StructField("contribution_pp", DoubleType()),
            ]
        ),
        [
            "series_id",
            "item_label",
            "parent_item",
            "hierarchy_level",
            "basket",
            "sa_nsa",
            "CAST(observation_date AS DATE) AS observation_date",
            "index_value",
            "mom_pct",
            "yoy_pct",
            "mom_accel",
            "yoy_accel",
            "three_month_annualized",
            "weight",
            "contribution_pp",
        ],
    )
    _write(
        "inflation_contribution",
        inflation["contribution"],
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("basket", StringType()),
                StructField("sa_nsa", StringType()),
                StructField("series_id", StringType()),
                StructField("item_label", StringType()),
                StructField("contribution_pp", DoubleType()),
                StructField("rank_in_month", IntegerType()),
                StructField("is_headline_total", BooleanType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "basket",
            "sa_nsa",
            "series_id",
            "item_label",
            "contribution_pp",
            "rank_in_month",
            "is_headline_total",
        ],
    )


def _build_regime_stats(config: PipelineConfig, spark: Any) -> None:
    """Build the Phase-5 regime playbook + statistical lab tables
    (``gold.macro_regime_daily``, ``series_correlation``, ``series_lead_lag``,
    ``series_structural_breaks``) by reusing the pure-Python engines in
    :mod:`fred_pipeline.regime_stats` — same collect-and-compute pattern as
    :func:`_build_terminal_views`. Input is bounded to pillar/pair series."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.regime_stats import (
        compute_macro_regime,
        compute_series_correlation,
        compute_series_lead_lag,
        compute_series_structural_breaks,
    )
    from fred_pipeline.regime_stats_config import (
        load_regime_config,
        load_stats_config,
    )

    def _write(
        name: str, rows: list[dict[str, Any]], schema: Any, casts: list[str]
    ) -> None:
        spark.createDataFrame(rows, schema=schema).selectExpr(*casts).write.format(
            "delta"
        ).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            config.table("gold", name)
        )

    regime_cfg = load_regime_config()
    regime_ids = sorted({i.series_id for p in regime_cfg.pillars for i in p.inputs})
    regime_rows = compute_macro_regime(
        _collect_latest(config, spark, regime_ids), regime_cfg
    )
    _write(
        "macro_regime_daily",
        regime_rows,
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("growth_score", DoubleType()),
                StructField("inflation_score", DoubleType()),
                StructField("liquidity_score", DoubleType()),
                StructField("credit_score", DoubleType()),
                StructField("policy_score", DoubleType()),
                StructField("composite_score", DoubleType()),
                StructField("regime_name", StringType()),
                StructField("regime_confidence", DoubleType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "growth_score",
            "inflation_score",
            "liquidity_score",
            "credit_score",
            "policy_score",
            "composite_score",
            "regime_name",
            "regime_confidence",
        ],
    )

    stats_cfg = load_stats_config()
    stats_ids = sorted({s for p in stats_cfg.pairs for s in (p.series_a, p.series_b)})
    stats_input = _collect_latest(config, spark, stats_ids)
    _write(
        "series_correlation",
        compute_series_correlation(stats_input, stats_cfg),
        StructType(
            [
                StructField("series_a", StringType()),
                StructField("series_b", StringType()),
                StructField("transform_a", StringType()),
                StructField("transform_b", StringType()),
                StructField("window", IntegerType()),
                StructField("observation_date", StringType()),
                StructField("correlation", DoubleType()),
                StructField("n_obs", IntegerType()),
            ]
        ),
        [
            "series_a",
            "series_b",
            "transform_a",
            "transform_b",
            "window",
            "CAST(observation_date AS DATE) AS observation_date",
            "correlation",
            "n_obs",
        ],
    )
    _write(
        "series_lead_lag",
        compute_series_lead_lag(stats_input, stats_cfg),
        StructType(
            [
                StructField("series_a", StringType()),
                StructField("series_b", StringType()),
                StructField("transform_a", StringType()),
                StructField("transform_b", StringType()),
                StructField("lag", IntegerType()),
                StructField("cross_correlation", DoubleType()),
                StructField("n_obs", IntegerType()),
                StructField("best_lag", IntegerType()),
                StructField("granger_f_ab", DoubleType()),
                StructField("granger_p_ab", DoubleType()),
                StructField("granger_f_ba", DoubleType()),
                StructField("granger_p_ba", DoubleType()),
                StructField("as_of_date", StringType()),
            ]
        ),
        [
            "series_a",
            "series_b",
            "transform_a",
            "transform_b",
            "lag",
            "cross_correlation",
            "n_obs",
            "best_lag",
            "granger_f_ab",
            "granger_p_ab",
            "granger_f_ba",
            "granger_p_ba",
            "CAST(as_of_date AS DATE) AS as_of_date",
        ],
    )
    _write(
        "series_structural_breaks",
        compute_series_structural_breaks(stats_input, stats_cfg),
        StructType(
            [
                StructField("series_a", StringType()),
                StructField("series_b", StringType()),
                StructField("transform_a", StringType()),
                StructField("transform_b", StringType()),
                StructField("test_type", StringType()),
                StructField("break_date", StringType()),
                StructField("f_stat", DoubleType()),
                StructField("p_value", DoubleType()),
                StructField("pre_n", IntegerType()),
                StructField("post_n", IntegerType()),
                StructField("pre_mean_a", DoubleType()),
                StructField("post_mean_a", DoubleType()),
                StructField("pre_mean_b", DoubleType()),
                StructField("post_mean_b", DoubleType()),
                StructField("cusum_max", DoubleType()),
                StructField("is_significant", IntegerType()),
                StructField("as_of_date", StringType()),
            ]
        ),
        [
            "series_a",
            "series_b",
            "transform_a",
            "transform_b",
            "test_type",
            "CAST(break_date AS DATE) AS break_date",
            "f_stat",
            "p_value",
            "pre_n",
            "post_n",
            "pre_mean_a",
            "post_mean_a",
            "pre_mean_b",
            "post_mean_b",
            "cusum_max",
            "is_significant",
            "CAST(as_of_date AS DATE) AS as_of_date",
        ],
    )

    # docs/handoffs/terminal_phase0_gaps.md item 3: FOMC rate probabilities
    # (config/fomc.yml) — option A, no CME connector; derived from
    # already-ingested FRED short-rate/target series.
    from fred_pipeline.gold_config.fomc_config import load_fomc_config
    from fred_pipeline.terminal_views import compute_fomc_probability

    fomc_cfg = load_fomc_config()
    if fomc_cfg is not None:
        fomc_ids = sorted(
            {t.series_id for t in fomc_cfg.tenors}
            | {
                fomc_cfg.target_low_series,
                fomc_cfg.target_high_series,
                fomc_cfg.effective_rate_series,
            }
        )
        fomc = compute_fomc_probability(
            _collect_latest(config, spark, fomc_ids), fomc_cfg
        )
        _write(
            "fomc_probability",
            fomc["probability"],
            StructType(
                [
                    StructField("meeting_date", StringType()),
                    StructField("target_lower_bps", IntegerType()),
                    StructField("target_upper_bps", IntegerType()),
                    StructField("outcome_bps", IntegerType()),
                    StructField("probability", DoubleType()),
                    StructField("model_vintage", StringType()),
                    StructField("n_inputs", IntegerType()),
                ]
            ),
            [
                "CAST(meeting_date AS DATE) AS meeting_date",
                "target_lower_bps",
                "target_upper_bps",
                "outcome_bps",
                "probability",
                "CAST(model_vintage AS DATE) AS model_vintage",
                "n_inputs",
            ],
        )
        _write(
            "fomc_meeting_path",
            fomc["meeting_path"],
            StructType(
                [
                    StructField("meeting_date", StringType()),
                    StructField("implied_rate", DoubleType()),
                    StructField("implied_move_bps", DoubleType()),
                    StructField("cumulative_move_bps", DoubleType()),
                    StructField("model_vintage", StringType()),
                ]
            ),
            [
                "CAST(meeting_date AS DATE) AS meeting_date",
                "implied_rate",
                "implied_move_bps",
                "cumulative_move_bps",
                "CAST(model_vintage AS DATE) AS model_vintage",
            ],
        )


def _build_global_views(config: PipelineConfig, spark: Any) -> None:
    """Build the Phase-6 global tables + the Power BI catalog
    (``gold.global_inflation``, ``global_policy_rates``, ``powerbi_catalog``)
    from the pure-Python engines in :mod:`fred_pipeline.global_views` — same
    collect-and-compute pattern as the other terminal-view builders."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.global_config import load_global_config
    from fred_pipeline.global_views import (
        compute_global_inflation,
        compute_global_policy_rates,
        powerbi_catalog_rows,
    )

    def _write(
        name: str, rows: list[dict[str, Any]], schema: Any, casts: list[str]
    ) -> None:
        spark.createDataFrame(rows, schema=schema).selectExpr(*casts).write.format(
            "delta"
        ).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            config.table("gold", name)
        )

    cfg = load_global_config()
    ids = sorted(
        {d.series_id for d in cfg.inflation} | {d.series_id for d in cfg.policy_rates}
    )
    rows_in = _collect_latest(config, spark, ids)
    _write(
        "global_inflation",
        compute_global_inflation(rows_in, cfg),
        StructType(
            [
                StructField("country", StringType()),
                StructField("iso3", StringType()),
                StructField("region", StringType()),
                StructField("series_id", StringType()),
                StructField("observation_date", StringType()),
                StructField("cpi_yoy_pct", DoubleType()),
                StructField("change_pp", DoubleType()),
                StructField("trend", StringType()),
                StructField("streak", IntegerType()),
                StructField("target_pct", DoubleType()),
                StructField("vs_target_pp", DoubleType()),
            ]
        ),
        [
            "country",
            "iso3",
            "region",
            "series_id",
            "CAST(observation_date AS DATE) AS observation_date",
            "cpi_yoy_pct",
            "change_pp",
            "trend",
            "streak",
            "target_pct",
            "vs_target_pp",
        ],
    )
    _write(
        "global_policy_rates",
        compute_global_policy_rates(rows_in, cfg),
        StructType(
            [
                StructField("country", StringType()),
                StructField("iso3", StringType()),
                StructField("region", StringType()),
                StructField("series_id", StringType()),
                StructField("observation_date", StringType()),
                StructField("policy_rate_pct", DoubleType()),
                StructField("change_bps", DoubleType()),
                StructField("last_move_bps", DoubleType()),
                StructField("stance", StringType()),
                StructField("real_rate_pct", DoubleType()),
            ]
        ),
        [
            "country",
            "iso3",
            "region",
            "series_id",
            "CAST(observation_date AS DATE) AS observation_date",
            "policy_rate_pct",
            "change_bps",
            "last_move_bps",
            "stance",
            "real_rate_pct",
        ],
    )
    _write(
        "powerbi_catalog",
        powerbi_catalog_rows(),
        StructType(
            [
                StructField("object_name", StringType()),
                StructField("object_type", StringType()),
                StructField("module", StringType()),
                StructField("grain", StringType()),
                StructField("intended_visual", StringType()),
                StructField("description", StringType()),
            ]
        ),
        [
            "object_name",
            "object_type",
            "module",
            "grain",
            "intended_visual",
            "description",
        ],
    )


def _build_equity_views(config: PipelineConfig, spark: Any) -> None:
    """Build the equity slice from the pure-Python engines in
    :mod:`fred_pipeline.equity_views`. Collected input is source-filtered so
    Stooq and Tiingo rows can be selected without collapsing shared series ids."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.equity_views import (
        compute_equity_price_reconciliation,
        compute_equity_return_daily,
        compute_equity_total_return_index,
        compute_index_constituents,
        compute_realized_volatility,
        select_canonical_equity_price_rows,
    )

    silver = config.table("silver", "fred_observation")

    def _rows(source: str) -> list[dict[str, Any]]:
        # Read raw Silver by source — equity data is non-vintage (one row per
        # (series_id, date)), so no latest-revision reduction is needed, and
        # source-filtering keeps Stooq's and Tiingo's shared <ticker>:close
        # namespaces from colliding (which the merged latest table would).
        return [
            r.asDict()
            for r in spark.sql(
                f"SELECT series_id, "
                f"CAST(observation_date AS STRING) AS observation_date, "
                f"value, is_missing FROM {silver} WHERE source = '{source}'"
            ).collect()
        ]

    stooq_rows = _rows("stooq")
    holdings_rows = _rows("ishares")
    tiingo_rows = _rows("tiingo")
    canonical_price_rows = select_canonical_equity_price_rows(stooq_rows, tiingo_rows)

    spark.createDataFrame(
        compute_equity_return_daily(canonical_price_rows),
        schema=StructType(
            [
                StructField("ticker", StringType()),
                StructField("observation_date", StringType()),
                StructField("close", DoubleType()),
                StructField("price_change", DoubleType()),
                StructField("price_return", DoubleType()),
                StructField("price_return_index", DoubleType()),
            ]
        ),
    ).selectExpr(
        "ticker",
        "CAST(observation_date AS DATE) AS observation_date",
        "close",
        "price_change",
        "price_return",
        "price_return_index",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "equity_return_daily"))

    spark.createDataFrame(
        compute_index_constituents(holdings_rows),
        schema=StructType(
            [
                StructField("index_etf", StringType()),
                StructField("constituent", StringType()),
                StructField("observation_date", StringType()),
                StructField("weight_pct", DoubleType()),
                StructField("weight_rank", IntegerType()),
                StructField("is_latest_snapshot", BooleanType()),
            ]
        ),
    ).selectExpr(
        "index_etf",
        "constituent",
        "CAST(observation_date AS DATE) AS observation_date",
        "weight_pct",
        "weight_rank",
        "is_latest_snapshot",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "index_constituents"))

    spark.createDataFrame(
        compute_equity_total_return_index(tiingo_rows),
        schema=StructType(
            [
                StructField("ticker", StringType()),
                StructField("observation_date", StringType()),
                StructField("close", DoubleType()),
                StructField("dividend", DoubleType()),
                StructField("split_factor", DoubleType()),
                StructField("price_return", DoubleType()),
                StructField("total_return", DoubleType()),
                StructField("price_return_index", DoubleType()),
                StructField("total_return_index", DoubleType()),
                StructField("trailing_12m_dividend", DoubleType()),
                StructField("dividend_yield_pct", DoubleType()),
            ]
        ),
    ).selectExpr(
        "ticker",
        "CAST(observation_date AS DATE) AS observation_date",
        "close",
        "dividend",
        "split_factor",
        "price_return",
        "total_return",
        "price_return_index",
        "total_return_index",
        "trailing_12m_dividend",
        "dividend_yield_pct",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "equity_total_return_index"))

    spark.createDataFrame(
        compute_equity_price_reconciliation(stooq_rows, tiingo_rows),
        schema=StructType(
            [
                StructField("ticker", StringType()),
                StructField("observation_date", StringType()),
                StructField("stooq_close", DoubleType()),
                StructField("tiingo_adj_close", DoubleType()),
                StructField("abs_diff", DoubleType()),
                StructField("pct_diff", DoubleType()),
                StructField("diverged", BooleanType()),
            ]
        ),
    ).selectExpr(
        "ticker",
        "CAST(observation_date AS DATE) AS observation_date",
        "stooq_close",
        "tiingo_adj_close",
        "abs_diff",
        "pct_diff",
        "diverged",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "equity_price_reconciliation"))

    spark.createDataFrame(
        compute_realized_volatility(canonical_price_rows),
        schema=StructType(
            [
                StructField("ticker", StringType()),
                StructField("observation_date", StringType()),
                StructField("window", IntegerType()),
                StructField("realized_vol_pct", DoubleType()),
            ]
        ),
    ).selectExpr(
        "ticker",
        "CAST(observation_date AS DATE) AS observation_date",
        "window",
        "realized_vol_pct",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "realized_volatility"))


def _build_zscore_views(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_series_zscore_rolling`` and ``gold.zscore_heatmap``
    from the pure-Python engines in :mod:`fred_pipeline.zscore_views`.
    Reads ``gold.fred_feature_transforms`` (already written by SQL step) and
    produces rolling/expanding z-score analytics per FRED series."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.zscore_views import (
        compute_fred_series_zscore_rolling,
        compute_zscore_heatmap,
    )

    def _write(
        name: str, rows: list[dict[str, Any]], schema: Any, casts: list[str]
    ) -> None:
        spark.createDataFrame(rows, schema=schema).selectExpr(*casts).write.format(
            "delta"
        ).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            config.table("gold", name)
        )

    ft_table = config.table("gold", "fred_feature_transforms")
    feature_transform_rows = [
        r.asDict()
        for r in spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"value, zscore FROM {ft_table}"
        ).collect()
    ]

    _write(
        "fred_series_zscore_rolling",
        compute_fred_series_zscore_rolling(feature_transform_rows),
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("observation_date", StringType()),
                StructField("window", IntegerType()),
                StructField("value", DoubleType()),
                StructField("change", DoubleType()),
                StructField("pct_change", DoubleType()),
                StructField("zscore", DoubleType()),
                StructField("percentile", DoubleType()),
            ]
        ),
        [
            "series_id",
            "CAST(observation_date AS DATE) AS observation_date",
            "window",
            "value",
            "change",
            "pct_change",
            "zscore",
            "percentile",
        ],
    )

    _write(
        "zscore_heatmap",
        compute_zscore_heatmap(feature_transform_rows),
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("observation_date", StringType()),
                StructField("value", DoubleType()),
                StructField("zscore_expanding", DoubleType()),
                StructField("percentile_expanding", DoubleType()),
                StructField("zscore_12", DoubleType()),
                StructField("percentile_12", DoubleType()),
                StructField("zscore_36", DoubleType()),
                StructField("percentile_36", DoubleType()),
                StructField("zscore_60", DoubleType()),
                StructField("percentile_60", DoubleType()),
                StructField("zscore_120", DoubleType()),
                StructField("percentile_120", DoubleType()),
            ]
        ),
        [
            "series_id",
            "CAST(observation_date AS DATE) AS observation_date",
            "value",
            "zscore_expanding",
            "percentile_expanding",
            "zscore_12",
            "percentile_12",
            "zscore_36",
            "percentile_36",
            "zscore_60",
            "percentile_60",
            "zscore_120",
            "percentile_120",
        ],
    )


def _build_ml_pipeline(config: PipelineConfig, spark: Any) -> None:
    """Build ML pipeline Gold tables (ML-0/2/4/3/5) by reusing the pure-Python
    engines, same collect-and-compute pattern as the other builders.

    Execution order:
      ML-0  feature matrix (reads ``gold.fred_feature_transforms``)
      ML-2  PCA factor scores + loadings (reads ML-0 output in memory)
      ML-4  Mahalanobis anomaly scores (reads ML-2 scores in memory)
      ML-3  recession probability (reads NS factors + credit/funding/regime from Gold)
      ML-5  equity factor attribution (reads equity returns + ML-2 scores in memory)
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from fred_pipeline.anomaly import compute_macro_anomaly_scores
    from fred_pipeline.equity_factor_attribution import (
        compute_equity_factor_attribution,
        compute_equity_factor_implied_return,
        load_equity_factor_config,
    )
    from fred_pipeline.macro_pca import compute_macro_factor_scores
    from fred_pipeline.ml_features import (
        compute_ml_feature_matrix,
        load_ml_feature_config,
    )
    from fred_pipeline.recession_model import (
        compute_recession_probability,
        load_recession_model_config,
    )
    from fred_pipeline.terminal_views import RECESSION_SERIES

    def _write(
        name: str, rows: list[dict[str, Any]], schema: Any, casts: list[str]
    ) -> None:
        spark.createDataFrame(rows, schema=schema).selectExpr(*casts).write.format(
            "delta"
        ).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            config.table("gold", name)
        )

    # Collect feature transforms (shared by ML-0 and ML-3).
    ft_table = config.table("gold", "fred_feature_transforms")
    feature_transform_rows = [
        r.asDict()
        for r in spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"value, mom, diff, yoy, zscore FROM {ft_table}"
        ).collect()
    ]

    # ML-0: feature matrix.
    ml_cfg = None
    try:
        ml_cfg = load_ml_feature_config()
    except Exception:
        pass
    ml_matrix = compute_ml_feature_matrix(feature_transform_rows, ml_cfg)
    _write(
        "ml_feature_matrix",
        ml_matrix,
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("feature_name", StringType()),
                StructField("series_id", StringType()),
                StructField("transform", StringType()),
                StructField("value", DoubleType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "feature_name",
            "series_id",
            "transform",
            "value",
        ],
    )

    # ML-2: PCA factor scores + loadings (keep pca in memory for ML-4 and ML-5).
    n_comp = ml_cfg.n_components if ml_cfg else 5
    pca = compute_macro_factor_scores(ml_matrix, n_components=n_comp)
    _write(
        "macro_factor_scores",
        pca["scores"],
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("factor", IntegerType()),
                StructField("score", DoubleType()),
                StructField("explained_variance_ratio", DoubleType()),
                StructField("cumulative_variance_ratio", DoubleType()),
                StructField("n_obs", IntegerType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "factor",
            "score",
            "explained_variance_ratio",
            "cumulative_variance_ratio",
            "n_obs",
        ],
    )
    _write(
        "macro_factor_loadings",
        pca["loadings"],
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("factor", IntegerType()),
                StructField("feature_name", StringType()),
                StructField("loading", DoubleType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "factor",
            "feature_name",
            "loading",
        ],
    )

    # ML-4: Mahalanobis anomaly scores.
    anom_thresh = ml_cfg.anomaly_threshold if ml_cfg else 0.99
    _write(
        "macro_anomaly_scores",
        compute_macro_anomaly_scores(pca["scores"], anomaly_threshold=anom_thresh),
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("mahalanobis_d2", DoubleType()),
                StructField("chi2_df", IntegerType()),
                StructField("p_value", DoubleType()),
                StructField("is_anomaly", BooleanType()),
                StructField("n_factors_used", IntegerType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "mahalanobis_d2",
            "chi2_df",
            "p_value",
            "is_anomaly",
            "n_factors_used",
        ],
    )

    # ML-3: recession probability — reads from already-written Gold tables.
    # lambda is a Spark SQL reserved keyword; backtick-quote it in the SELECT.
    ns_table = config.table("gold", "yield_curve_ns_factors")
    ns_factor_rows = [
        r.asDict()
        for r in spark.sql(
            f"SELECT CAST(observation_date AS STRING) AS observation_date, "
            f"beta0, beta1, beta2, `lambda`, lambda_estimated, fit_rmse, n_tenors, fit_valid "
            f"FROM {ns_table}"
        ).collect()
    ]
    credit_table = config.table("gold", "credit_spread_daily")
    credit_spread_rows = [
        r.asDict()
        for r in spark.sql(
            f"SELECT instrument, CAST(observation_date AS STRING) AS observation_date, "
            f"zscore FROM {credit_table}"
        ).collect()
    ]
    funding_table = config.table("gold", "funding_stress_daily")
    funding_stress_rows = [
        r.asDict()
        for r in spark.sql(
            f"SELECT CAST(observation_date AS STRING) AS observation_date, stress_score "
            f"FROM {funding_table}"
        ).collect()
    ]
    regime_table = config.table("gold", "macro_regime_daily")
    regime_rows = [
        r.asDict()
        for r in spark.sql(
            f"SELECT CAST(observation_date AS STRING) AS observation_date, composite_score "
            f"FROM {regime_table}"
        ).collect()
    ]
    rec_cfg = None
    try:
        rec_cfg = load_recession_model_config()
    except Exception:
        pass
    _write(
        "recession_probability_daily",
        compute_recession_probability(
            _collect_latest(config, spark, [RECESSION_SERIES]),
            ns_factor_rows=ns_factor_rows,
            feature_transform_rows=feature_transform_rows,
            credit_spread_rows=credit_spread_rows,
            funding_stress_rows=funding_stress_rows,
            regime_rows=regime_rows,
            cfg=rec_cfg,
        ),
        StructType(
            [
                StructField("observation_date", StringType()),
                StructField("recession_prob", DoubleType()),
                StructField("prob_recession_3m", DoubleType()),
                StructField("prob_recession_6m", DoubleType()),
                StructField("prob_recession_12m", DoubleType()),
                StructField("logit_score", DoubleType()),
                StructField("n_features", IntegerType()),
                StructField("n_obs_training", IntegerType()),
                StructField("model_vintage", StringType()),
                StructField("is_backfilled", BooleanType()),
            ]
        ),
        [
            "CAST(observation_date AS DATE) AS observation_date",
            "recession_prob",
            "prob_recession_3m",
            "prob_recession_6m",
            "prob_recession_12m",
            "logit_score",
            "n_features",
            "n_obs_training",
            "CAST(model_vintage AS DATE) AS model_vintage",
            "is_backfilled",
        ],
    )

    # ML-6: Short-horizon inflation forecasting (AR + VAR on CPI/PCE MoM).
    from fred_pipeline.inflation_model import (
        compute_inflation_forecast,
        load_inflation_forecast_config,
    )

    inf_cfg = None
    try:
        inf_cfg = load_inflation_forecast_config()
    except Exception:
        pass
    inf_series = list(inf_cfg.series if inf_cfg else ("CPIAUCSL", "PCEPI"))
    inf_pairs = list(inf_cfg.var_pairs if inf_cfg else [("CPIAUCSL", "PCEPI")])
    all_inf_ids = list(
        {s for s in inf_series} | {s for pair in inf_pairs for s in pair}
    )
    _write(
        "inflation_forecast",
        compute_inflation_forecast(
            _collect_latest(config, spark, all_inf_ids), cfg=inf_cfg
        ),
        StructType(
            [
                StructField("series_id", StringType()),
                StructField("forecast_date", StringType()),
                StructField("horizon_months", IntegerType()),
                StructField("forecast_value", DoubleType()),
                StructField("lower_80", DoubleType()),
                StructField("upper_80", DoubleType()),
                StructField("lower_95", DoubleType()),
                StructField("upper_95", DoubleType()),
                StructField("model_type", StringType()),
                StructField("lag_order", IntegerType()),
                StructField("model_vintage", StringType()),
                StructField("n_obs_training", IntegerType()),
            ]
        ),
        [
            "series_id",
            "CAST(forecast_date AS DATE) AS forecast_date",
            "horizon_months",
            "forecast_value",
            "lower_80",
            "upper_80",
            "lower_95",
            "upper_95",
            "model_type",
            "lag_order",
            "CAST(model_vintage AS DATE) AS model_vintage",
            "n_obs_training",
        ],
    )

    # ML-5: equity factor attribution — uses in-memory pca["scores"].
    eq_return_table = config.table("gold", "equity_return_daily")
    eq_return_rows = [
        r.asDict()
        for r in spark.sql(
            f"SELECT ticker, CAST(observation_date AS STRING) AS observation_date, "
            f"price_return FROM {eq_return_table}"
        ).collect()
    ]
    ef_cfg = None
    try:
        ef_cfg = load_equity_factor_config()
    except Exception:
        pass
    eq_attribution_rows = compute_equity_factor_attribution(
        eq_return_rows, pca["scores"], cfg=ef_cfg
    )
    _write(
        "equity_factor_attribution",
        eq_attribution_rows,
        StructType(
            [
                StructField("ticker", StringType()),
                StructField("observation_date", StringType()),
                StructField("window", IntegerType()),
                StructField("factor", IntegerType()),
                StructField("beta", DoubleType()),
                StructField("t_stat", DoubleType()),
                StructField("alpha", DoubleType()),
                StructField("r_squared", DoubleType()),
                StructField("n_obs", IntegerType()),
            ]
        ),
        [
            "ticker",
            "CAST(observation_date AS DATE) AS observation_date",
            "window",
            "factor",
            "beta",
            "t_stat",
            "alpha",
            "r_squared",
            "n_obs",
        ],
    )

    # ML-5b: Factor-implied return decomposition.
    _write(
        "equity_factor_implied_return",
        compute_equity_factor_implied_return(
            eq_attribution_rows, pca["scores"], eq_return_rows, cfg=ef_cfg
        ),
        StructType(
            [
                StructField("ticker", StringType()),
                StructField("observation_date", StringType()),
                StructField("window", IntegerType()),
                StructField("implied_return", DoubleType()),
                StructField("factor_return", DoubleType()),
                StructField("alpha_return", DoubleType()),
                StructField("realized_return", DoubleType()),
                StructField("residual_return", DoubleType()),
            ]
        ),
        [
            "ticker",
            "CAST(observation_date AS DATE) AS observation_date",
            "window",
            "implied_return",
            "factor_return",
            "alpha_return",
            "realized_return",
            "residual_return",
        ],
    )


def build_gold(config: PipelineConfig, *, spark: Any = None) -> dict[str, str]:
    """Rebuild all Gold tables from Silver. Returns table -> 'ok' map."""
    spark = get_spark(spark)
    steps = {
        "fred_point_in_time": _point_in_time_sql(config),
        "fred_latest_observation": _latest_observation_sql(config),
        "fred_macro_feature_daily": _macro_feature_daily_sql(config),
        "fred_feature_transforms": _feature_transforms_sql(config),
        "fred_curve_spread": _curve_spread_sql(config),
        "fred_revision_stats": _revision_stats_sql(config),
    }
    results: dict[str, str] = {}
    for name, sql in steps.items():
        spark.sql(sql)
        results[name] = "ok"
    # Cross-series features + source reconciliation are computed in Python (reused
    # by both backends) and written after latest_observation exists.
    _build_cross_series(config, spark)
    results["fred_cross_series_feature"] = "ok"
    _build_cross_series_pit(config, spark)
    results["fred_cross_series_feature_pit"] = "ok"
    _build_source_reconciliation(config, spark)
    results["fred_source_reconciliation"] = "ok"
    _build_ecb_fx_reference_rates(config, spark)
    results["ecb_fx_reference_rate"] = "ok"
    _build_company_financials(config, spark)
    results["fred_company_fundamentals"] = "ok"
    results["fred_company_ratios"] = "ok"
    _build_terminal_views(config, spark)
    for name in (
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
        "mmkt_rate_snapshot",
        "credit_spread_daily",
        "inflation_explorer",
        "inflation_contribution",
        "curve_spread_rolling",
        "credit_spread_rolling",
        "treasury_curve_rolling",
    ):
        results[name] = "ok"
    _build_regime_stats(config, spark)
    for name in (
        "macro_regime_daily",
        "series_correlation",
        "series_lead_lag",
        "series_structural_breaks",
    ):
        results[name] = "ok"
    _build_global_views(config, spark)
    for name in (
        "global_inflation",
        "global_policy_rates",
        "powerbi_catalog",
    ):
        results[name] = "ok"
    _build_equity_views(config, spark)
    for name in (
        "equity_return_daily",
        "index_constituents",
        "equity_total_return_index",
        "equity_price_reconciliation",
        "realized_volatility",
    ):
        results[name] = "ok"
    _build_zscore_views(config, spark)
    for name in ("fred_series_zscore_rolling", "zscore_heatmap"):
        results[name] = "ok"
    _build_ml_pipeline(config, spark)
    for name in (
        "ml_feature_matrix",
        "macro_factor_scores",
        "macro_factor_loadings",
        "macro_anomaly_scores",
        "recession_probability_daily",
        "equity_factor_attribution",
        "equity_factor_implied_return",
        "inflation_forecast",
    ):
        results[name] = "ok"
    return results
