"""Silver layer: normalized, deduplicated observations.

Silver is the clean, queryable form of every observation. Loads are idempotent
via a Delta MERGE on (series_id, observation_date, realtime_start), so re-runs
never create duplicates and revisions land as new real-time rows.
"""

from __future__ import annotations

from typing import Any

from fred_pipeline.config import PipelineConfig
from fred_pipeline.spark_io import (
    cluster_dataframe_by_series,
    get_spark,
    merge_delta,
    table_exists,
    target_partitions_for_rows,
)
from fred_pipeline.transform import (
    SILVER_COLUMNS,
    assign_revision_numbers,
    normalize_observations,
)
from fred_pipeline.data.observation_history import (
    observation_event,
    with_observation_event,
)

# Natural key that makes a silver row unique. ``source`` is the leading key so
# the same series_id could, in principle, be sourced from more than one upstream
# API without colliding (manifests still enforce globally-unique series_ids).
SILVER_MERGE_KEYS = ("source", "series_id", "observation_date", "realtime_start")
HISTORY_MERGE_KEYS = ("observation_event_id",)


def _normalize_for_source(
    source: str,
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: str,
    ingested_at: str | None,
    track_vintage: bool,
) -> list[dict[str, Any]]:
    """Dispatch to the right source normalizer (module-level, no client needed).

    Kept out of the client objects so Bronze replay can re-derive Silver for any
    source without constructing a client (EIA, for instance, requires an API key
    just to instantiate).
    """
    if source == "bls":
        from fred_pipeline.sources.bls import normalize_bls_observations

        return normalize_bls_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )
    if source == "eia":
        from fred_pipeline.sources.eia import normalize_eia_observations

        return normalize_eia_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )
    if source == "ecb":
        from fred_pipeline.sources.ecb import normalize_ecb_observations

        return normalize_ecb_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            track_vintage=track_vintage,
            source=source,
        )
    if source == "treasury":
        from fred_pipeline.sources.treasury import normalize_treasury_observations

        return normalize_treasury_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )
    if source == "worldbank":
        from fred_pipeline.sources.worldbank import normalize_worldbank_observations

        return normalize_worldbank_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )
    if source == "bea":
        from fred_pipeline.sources.bea import normalize_bea_observations

        return normalize_bea_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )
    if source == "census":
        from fred_pipeline.sources.census import normalize_census_observations

        return normalize_census_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )
    if source == "sec":
        from fred_pipeline.sources.sec import (
            normalize_sec_observations,
            resolve_sec_period,
        )

        # Replay honors the same SEC_PERIOD as ingestion (Bronze keeps every
        # duration verbatim, so the period filter must be re-applied here).
        return normalize_sec_observations(
            series_id,
            payload,
            run_id=run_id,
            ingested_at=ingested_at,
            track_vintage=track_vintage,
            source=source,
            period=resolve_sec_period(),
        )
    # default: FRED (and any unknown source, which stays FRED-shaped)
    return normalize_observations(
        series_id,
        payload,
        run_id=run_id,
        ingested_at=ingested_at,
        track_vintage=track_vintage,
        source=source or "fred",
    )


def build_silver_rows(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: str,
    ingested_at: str | None = None,
    track_vintage: bool = True,
    source: str = "fred",
) -> list[dict[str, Any]]:
    """Normalize a raw payload into silver rows with revision numbers.

    ``source`` selects the normalizer, so this works for FRED, BLS, and EIA
    payloads alike (used by the Bronze→Silver replay path).
    """
    rows = _normalize_for_source(
        source,
        series_id,
        payload,
        run_id=run_id,
        ingested_at=ingested_at,
        track_vintage=track_vintage,
    )
    return assign_revision_numbers(rows)


# Source rows carry ISO strings; cast to the Delta column types before MERGE.
_SILVER_CASTS = {
    "observation_date": "date",
    "realtime_start": "date",
    "realtime_end": "date",
    "ingested_at": "timestamp",
}


def merge_silver(
    config: PipelineConfig,
    rows: list[dict[str, Any]],
    *,
    spark: Any = None,
) -> dict[str, int]:
    """MERGE normalized rows into ``<catalog>.silver.fred_observation``."""
    if not rows:
        return {"source_rows": 0}
    spark = get_spark(spark)
    table = config.table("silver", "fred_observation")
    prepared = [with_observation_event(row) for row in rows]
    history_rows = [observation_event(row) for row in prepared]
    # Persist the immutable evidence before changing compact current state. A
    # retry is idempotent on observation_event_id if the later compact MERGE
    # fails after this independent Delta commit.
    history_table = config.table("silver", "fred_observation_history")
    history_df = spark.createDataFrame(history_rows)
    history_df = history_df.selectExpr(
        *[
            f"CAST(NULLIF({column}, '') AS {cast}) AS {column}"
            if (cast := _SILVER_CASTS.get(column)) in ("date", "timestamp")
            else column
            for column in history_df.columns
        ]
    )
    if table_exists(spark, table):
        existing = spark.table(table).select(*SILVER_MERGE_KEYS, "row_hash")
        join_condition = " AND ".join(
            f"t.{column} <=> s.{column}" for column in SILVER_MERGE_KEYS
        )
        history_df = (
            history_df.alias("s")
            .join(existing.alias("t"), join_condition, "left")
            .where("t.row_hash IS NULL OR NOT (t.row_hash <=> s.row_hash)")
            .select("s.*")
        )
    merge_delta(
        spark,
        history_df,
        history_table,
        HISTORY_MERGE_KEYS,
        update=False,
    )
    df = spark.createDataFrame(prepared)
    # Empty strings (e.g. open-ended realtime windows) must become NULL dates.
    exprs = []
    for col in df.columns:
        cast = _SILVER_CASTS.get(col)
        if cast in ("date", "timestamp"):
            exprs.append(f"CAST(NULLIF({col}, '') AS {cast}) AS {col}")
        else:
            exprs.append(col)
    df = df.selectExpr(*exprs)
    if getattr(config, "persistence_write_mode", "per_series") == "run_batched":
        df = cluster_dataframe_by_series(
            df,
            target_partitions=target_partitions_for_rows(
                prepared,
                target_file_size_mb=getattr(
                    config, "persistence_delta_target_file_size_mb", 256
                ),
                target_partitions=getattr(
                    config, "persistence_delta_target_partitions", "auto"
                ),
            ),
            sort_columns=("source", "series_id", "observation_date", "realtime_start"),
        )
    # Leave ingested_at untouched when a restated observation has identical
    # content. Due-date gating and incremental Gold both use it as a change
    # watermark, matching the SQLite and PostgreSQL merge paths.
    result = merge_delta(
        spark,
        df,
        table,
        SILVER_MERGE_KEYS,
        match_update_condition="NOT (t.row_hash <=> s.row_hash)",
    )
    return result


__all__ = [
    "HISTORY_MERGE_KEYS",
    "SILVER_COLUMNS",
    "SILVER_MERGE_KEYS",
    "build_silver_rows",
    "merge_silver",
]
