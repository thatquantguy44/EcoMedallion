"""Replay the same saved journal against a disposable native PostgreSQL DB."""

import json
from pathlib import Path

from fred_pipeline.config import PipelineConfig
from fred_pipeline.io.postgres_store import PostgresWarehouse
from fred_pipeline.performance_store import PerformanceStore
from tests.test_postgres_warehouse import postgres_dsn as postgres_dsn


def test_postgres_replays_and_amends_saved_span_events_idempotently(postgres_dsn):
    source = (
        Path(__file__).parents[1] / "fixtures" / "performance" / "span_events.jsonl"
    )
    events = [json.loads(line) for line in source.read_text().splitlines()]
    warehouse = PostgresWarehouse(PipelineConfig(fred_api_key=""), dsn=postgres_dsn)
    try:
        store = PerformanceStore(warehouse)
        store.write_spans([row for row in events if row["status"] == "running"])
        assert {
            row["status"]
            for row in warehouse.query("SELECT status FROM audit.run_span")
        } == {"running"}
        store.write_spans(events)
        store.write_spans(events)
        amended = dict(events[-1], status="warned", event="span_updated")
        store.write_spans([amended])
        rows = warehouse.query(
            "SELECT span_id, status FROM audit.run_span ORDER BY span_id"
        )
        assert rows == [
            {"span_id": "fetch-a", "status": "warned"},
            {"span_id": "request-a", "status": "succeeded"},
        ]
    finally:
        warehouse.close()
