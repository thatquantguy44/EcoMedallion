"""Additive, idempotent performance audit storage for the existing backends."""

from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from typing import Any

from fred_pipeline.performance import PerformanceCollector

RUN_COLUMNS = "run_id STRING, started_at STRING, ended_at STRING, status STRING, processing_seconds DOUBLE, comparison_key STRING, metadata_json STRING, summary_json STRING, delivery_json STRING"
SPAN_COLUMNS = "span_id STRING, run_id STRING, parent_span_id STRING, name STRING, source STRING, series_id STRING, table_name STRING, status STRING, started_at STRING, ended_at STRING, offset_seconds DOUBLE, duration_seconds DOUBLE, attributes_json STRING, error_type STRING"


def comparison_key(metadata: dict[str, Any]) -> str:
    # Revision identifies the code but is deliberately not a comparison filter:
    # comparing a new revision with the preceding implementation is useful.
    comparable = {key: value for key, value in metadata.items() if key != "revision"}
    return hashlib.sha256(json.dumps(comparable, sort_keys=True).encode()).hexdigest()


class PerformanceStore:
    def __init__(self, warehouse: Any):
        self.warehouse = warehouse
        self.spark = getattr(warehouse, "spark", None)
        if self.spark is not None:
            for table, columns in (
                ("run_performance", RUN_COLUMNS),
                ("run_span", SPAN_COLUMNS),
            ):
                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {self._table(table)} ({columns}) USING DELTA"
                )

    def _table(self, name: str) -> str:
        return self.warehouse.config.table("audit", name)

    def _write(self, table: str, rows: list[dict[str, Any]], key: str) -> None:
        if not rows:
            return
        if self.spark is None:
            self.warehouse._insert(f"audit_{table}", rows, upsert_keys=[key])
            return
        temporary = f"performance_{uuid.uuid4().hex}"
        schema = RUN_COLUMNS if table == "run_performance" else SPAN_COLUMNS
        frame = self.spark.createDataFrame(rows, schema=schema)
        frame.createOrReplaceTempView(temporary)
        try:
            self.spark.sql(
                f"MERGE INTO {self._table(table)} AS t USING {temporary} AS s ON t.{key} = s.{key} "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
        finally:
            self.spark.catalog.dropTempView(temporary)

    def write_spans(self, spans: list[dict[str, Any]]) -> None:
        rows = []
        for record in spans:
            attributes = record["attributes"]
            rows.append(
                {
                    key: record[key]
                    for key in (
                        "span_id",
                        "run_id",
                        "parent_span_id",
                        "name",
                        "status",
                        "started_at",
                        "ended_at",
                        "offset_seconds",
                        "duration_seconds",
                        "error_type",
                    )
                }
                | {
                    "source": attributes.get("source", ""),
                    "series_id": attributes.get("series_id", ""),
                    "table_name": attributes.get("table", ""),
                    "attributes_json": json.dumps(attributes, sort_keys=True),
                }
            )
        self._write("run_span", rows, "span_id")

    def baseline(self, collector: PerformanceCollector) -> dict[str, Any]:
        key = comparison_key(collector.metadata)
        if self.spark is None:
            rows = self.warehouse.query(
                "SELECT processing_seconds FROM audit_run_performance "
                "WHERE comparison_key = ? AND run_id <> ? AND status = 'success' "
                "ORDER BY started_at DESC LIMIT 30",
                (key, collector.run_id),
            )
        else:
            # Only generated hexadecimal identifiers are interpolated here.
            assert all(c in "0123456789abcdef" for c in key + collector.run_id)
            rows = [
                r.asDict()
                for r in self.spark.sql(
                    f"SELECT processing_seconds FROM {self._table('run_performance')} "
                    f"WHERE comparison_key = '{key}' AND run_id <> '{collector.run_id}' "
                    "AND status = 'success' ORDER BY started_at DESC LIMIT 30"
                ).collect()
            ]
        seconds = [float(row["processing_seconds"]) for row in reversed(rows)]
        baseline: dict[str, Any] = {"count": len(seconds), "previous_seconds": seconds}
        if len(seconds) >= 3:
            baseline["median_seconds"] = statistics.median(seconds)
        return baseline

    def save(
        self, collector: PerformanceCollector, *, delivery: dict[str, Any] | None = None
    ) -> None:
        report = collector.snapshot()
        self.write_spans(report["spans"])
        self._write(
            "run_performance",
            [
                {
                    "run_id": collector.run_id,
                    "started_at": collector.started_at,
                    "ended_at": collector.ended_at,
                    "status": collector.status,
                    "processing_seconds": report["processing_seconds"],
                    "comparison_key": comparison_key(collector.metadata),
                    "metadata_json": json.dumps(collector.metadata, sort_keys=True),
                    "summary_json": json.dumps(report, sort_keys=True),
                    "delivery_json": json.dumps(delivery or {}, sort_keys=True),
                }
            ],
            "run_id",
        )
