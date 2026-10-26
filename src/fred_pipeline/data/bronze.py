"""Bronze layer: raw payload retention.

Bronze stores the FRED API response *verbatim* (as a JSON string) plus minimal
ingestion metadata. Nothing is parsed or reshaped here — Bronze is the system
of record we can always re-derive Silver/Gold from.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from fred_pipeline.config import PipelineConfig
from fred_pipeline.spark_io import append_rows, get_spark, target_partitions_for_rows
from fred_pipeline.transform import payload_summary


def build_bronze_row(
    series_id: str,
    endpoint: str,
    payload: dict[str, Any],
    *,
    run_id: str,
    source: str = "fred",
    request_params: Optional[dict[str, Any]] = None,
    observation_count: Optional[int] = None,
) -> dict[str, Any]:
    """Assemble a single ``bronze.fred_api_response`` row from a raw payload.

    ``source``/``endpoint`` record which upstream API produced the payload.
    ``observation_count`` may be passed explicitly (the count of rows actually
    normalized) so it is accurate for sources that don't expose a top-level
    ``observations`` array; when omitted it falls back to the FRED-shaped
    payload summary.
    """
    summary = payload_summary(payload)
    if observation_count is None:
        observation_count = summary["observation_count"]
    return {
        "run_id": run_id,
        "source": source,
        "series_id": series_id,
        "endpoint": endpoint,
        "request_params": json.dumps(request_params or {}, sort_keys=True),
        "response_payload": json.dumps(payload),
        "observation_count": observation_count,
        "payload_bytes": summary["payload_bytes"],
        "ingested_at": datetime.now(timezone.utc),
    }


def write_bronze(
    config: PipelineConfig,
    rows: list[dict[str, Any]],
    *,
    spark: Any = None,
) -> int:
    """Append bronze rows to ``<catalog>.bronze.fred_api_response``."""
    if not rows:
        return 0
    spark = get_spark(spark)
    table = config.table("bronze", "fred_api_response")
    target_partitions = None
    if getattr(config, "persistence_write_mode", "per_series") == "run_batched":
        target_partitions = target_partitions_for_rows(
            rows,
            target_file_size_mb=getattr(
                config, "persistence_delta_target_file_size_mb", 256
            ),
            target_partitions=getattr(
                config, "persistence_delta_target_partitions", "auto"
            ),
        )
    return append_rows(
        spark,
        rows,
        table,
        target_partitions=target_partitions,
        sort_columns=("source", "series_id", "ingested_at"),
    )
