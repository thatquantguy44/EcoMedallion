"""Replay fixture journals and select history using the native SQLite store."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from fred_pipeline.config import PipelineConfig
from fred_pipeline.io.local_store import LocalWarehouse
from fred_pipeline.performance import PerformanceCollector
from fred_pipeline.performance_store import PerformanceStore, comparison_key


def test_saved_journal_replay_is_idempotent_and_preserves_parentage(tmp_path):
    source = (
        Path(__file__).parents[1] / "fixtures" / "performance" / "span_events.jsonl"
    )
    events = [json.loads(line) for line in source.read_text().splitlines()]
    warehouse = LocalWarehouse(
        PipelineConfig(fred_api_key=""), db_path=str(tmp_path / "replay.db")
    )
    try:
        store = PerformanceStore(warehouse)
        store.write_spans([row for row in events if row["status"] == "running"])
        assert {
            row["status"]
            for row in warehouse.query("SELECT status FROM audit_run_span")
        } == {"running"}
        store.write_spans(events)
        store.write_spans(events)
        rows = warehouse.query(
            "SELECT span_id, parent_span_id, status, duration_seconds FROM audit_run_span ORDER BY span_id"
        )
        assert rows == [
            {
                "span_id": "fetch-a",
                "parent_span_id": None,
                "status": "succeeded",
                "duration_seconds": 3,
            },
            {
                "span_id": "request-a",
                "parent_span_id": "fetch-a",
                "status": "succeeded",
                "duration_seconds": 1,
            },
        ]
    finally:
        warehouse.close()


def test_history_uses_latest_thirty_successes_excludes_self_and_ignores_revision(
    tmp_path,
    manual_clock,
):
    warehouse = LocalWarehouse(
        PipelineConfig(fred_api_key=""), db_path=str(tmp_path / "history.db")
    )
    store = PerformanceStore(warehouse)
    metadata = {
        "backend": "local",
        "due_series": 2,
        "ecb_assets_workload": {"mode": "poll", "releases": 0},
    }
    epoch = datetime(2026, 1, 1, tzinfo=timezone.utc)
    try:
        for index in range(36):
            collector = PerformanceCollector(clock=manual_clock, output_dir=tmp_path)
            collector.metadata = metadata | {"revision": f"revision-{index}"}
            collector.started_at = (epoch + timedelta(days=index)).isoformat()
            manual_clock.advance(index)
            collector.finish("partial" if index == 35 else "success")
            store.save(collector)
        current = PerformanceCollector(clock=manual_clock, output_dir=tmp_path)
        current.metadata = metadata | {"revision": "new-code"}
        manual_clock.advance(1000)
        current.finish("success")
        store.save(current)
        baseline = store.baseline(current)
        assert baseline["count"] == 30
        assert baseline["previous_seconds"] == list(range(5, 35))
        assert baseline["median_seconds"] == 19.5
        current.metadata["ecb_assets_workload"] = {"mode": "poll", "releases": 12}
        assert store.baseline(current)["count"] == 0
    finally:
        warehouse.close()


@pytest.mark.parametrize(
    "change",
    [
        {"backend": "postgres"},
        {"due_series": 3},
        {"force_full": True},
        {"workers": {"fred": 4}},
        {"row_volume_band": 12},
        {"ecb_assets_workload": {"mode": "skipped", "releases": 0}},
    ],
)
def test_comparison_key_separates_actual_workload_changes(change):
    original = {
        "backend": "local",
        "due_series": 2,
        "force_full": False,
        "workers": {"fred": 2},
        "row_volume_band": 8,
        "ecb_assets_workload": {"mode": "poll", "releases": 0},
    }
    assert comparison_key(original) != comparison_key(original | change)
    assert comparison_key(original | {"revision": "before"}) == comparison_key(
        original | {"revision": "after"}
    )


def test_delta_merge_failure_drops_temporary_view_and_preserves_original_error(
    tmp_path,
):
    error = RuntimeError("fixture Delta failure")

    class Spark:
        def __init__(self):
            self.registered = []
            self.dropped = []
            self.catalog = SimpleNamespace(dropTempView=self.dropped.append)

        def sql(self, sql):
            if sql.startswith("MERGE"):
                raise error

        def createDataFrame(self, rows, schema):
            assert len({row["run_id"] for row in rows}) == 1
            assert "processing_seconds DOUBLE" in schema
            return SimpleNamespace(createOrReplaceTempView=self.registered.append)

    spark = Spark()
    store = PerformanceStore(
        SimpleNamespace(spark=spark, config=PipelineConfig(fred_api_key=""))
    )
    collector = PerformanceCollector(output_dir=tmp_path)
    with pytest.raises(RuntimeError) as caught:
        store.save(collector)
    assert caught.value is error
    assert spark.dropped == spark.registered and len(spark.dropped) == 1


def test_delta_history_rows_become_chronological_baseline(tmp_path):
    history = [
        SimpleNamespace(asDict=lambda value=value: {"processing_seconds": value})
        for value in (120, 100, 80)
    ]

    class Spark:
        def sql(self, _sql):
            return SimpleNamespace(collect=lambda: history)

    store = PerformanceStore(
        SimpleNamespace(spark=Spark(), config=PipelineConfig(fred_api_key=""))
    )
    baseline = store.baseline(PerformanceCollector(output_dir=tmp_path))
    assert baseline == {
        "count": 3,
        "previous_seconds": [80, 100, 120],
        "median_seconds": 100,
    }
