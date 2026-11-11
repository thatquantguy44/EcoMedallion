"""Behavioral contracts for shared, bounded run performance measurements."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from fred_pipeline.performance import PerformanceCollector, span, submit_with_context
from fred_pipeline.timing import timed


class Clock:
    value = 0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += int(seconds * 1_000_000_000)


def test_nested_repeated_spans_preserve_identity_and_elapsed_time(tmp_path):
    clock = Clock()
    collector = PerformanceCollector(clock=clock, output_dir=tmp_path)
    with collector.activate():
        with span("extract"):
            for _ in range(2):
                with span("extract.fetch", source="bea", token="private"):
                    clock.advance(2)
    report = collector.finish()
    children = [s for s in report["spans"] if s["name"] == "extract.fetch"]
    parent = next(s for s in report["spans"] if s["name"] == "extract")
    assert report["processing_seconds"] == 4
    assert len({s["span_id"] for s in children}) == 2
    assert all(s["parent_span_id"] == parent["span_id"] for s in children)
    assert "private" not in str(report)


def test_decorator_failure_and_callback_failure_preserve_original_behavior(tmp_path):
    collector = PerformanceCollector(output_dir=tmp_path)

    @timed("failing")
    def failing():
        raise ValueError("sensitive provider message")

    @timed("working", progress_callback=lambda *_: 1 / 0)
    def working():
        return 42

    with collector.activate():
        with pytest.raises(ValueError, match="sensitive provider message"):
            failing()
        assert working() == 42
        with pytest.raises(KeyboardInterrupt):
            with span("interrupted"):
                raise KeyboardInterrupt()
    report = collector.finish()
    statuses = {s["name"]: s["status"] for s in report["spans"]}
    assert statuses == {
        "failing": "failed",
        "working": "succeeded",
        "interrupted": "cancelled",
    }
    assert "sensitive provider message" not in str(report)


def test_worker_context_and_overlapping_children_do_not_inflate_run_total(tmp_path):
    collector = PerformanceCollector(output_dir=tmp_path)
    with collector.activate():
        with span("gold") as parent:

            def work():
                with span("gold.engine"):
                    return 7

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [submit_with_context(pool, work) for _ in range(20)]
                assert [f.result() for f in futures] == [7] * 20
    report = collector.finish()
    children = [s for s in report["spans"] if s["name"] == "gold.engine"]
    assert len(children) == 20
    assert len({s["span_id"] for s in children}) == 20
    assert all(s["parent_span_id"] == parent.span_id for s in children)
    assert all(s["run_id"] == collector.run_id for s in children)


def test_bounded_samples_retain_aggregate_counts_and_disclose_loss(tmp_path):
    collector = PerformanceCollector(output_dir=tmp_path, max_spans=4)
    with collector.activate():
        for _ in range(20):
            with span("http.request", source="bea"):
                pass
    report = collector.finish()
    assert len(report["spans"]) <= 4
    assert report["aggregates"][0]["count"] == 20
    assert report["dropped_events"] > 0


def test_async_decorator_times_await_and_cancellation(tmp_path):
    collector = PerformanceCollector(output_dir=tmp_path)

    @timed("async.work")
    async def work():
        await asyncio.sleep(0)
        return 3

    @timed("async.cancel")
    async def cancel():
        raise asyncio.CancelledError()

    async def run():
        with collector.activate():
            assert await work() == 3
            with pytest.raises(asyncio.CancelledError):
                await cancel()

    asyncio.run(run())
    statuses = {s["name"]: s["status"] for s in collector.finish()["spans"]}
    assert statuses == {"async.work": "succeeded", "async.cancel": "cancelled"}


def test_storage_is_idempotent_and_baselines_exclude_different_workloads(tmp_path):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.io.local_store import LocalWarehouse
    from fred_pipeline.performance_store import PerformanceStore

    warehouse = LocalWarehouse(
        PipelineConfig(fred_api_key=""), db_path=str(tmp_path / "audit.db")
    )
    store = PerformanceStore(warehouse)
    try:
        for seconds, workload in (
            (10, "same"),
            (20, "same"),
            (30, "same"),
            (999, "other"),
        ):
            clock = Clock()
            collector = PerformanceCollector(clock=clock, output_dir=tmp_path)
            collector.metadata = {"backend": "local", "selection": workload}
            with collector.activate():
                with span("extract"):
                    clock.advance(seconds)
            collector.finish("success")
            store.save(collector)
            store.save(collector)
        assert (
            warehouse.query("SELECT count(*) AS n FROM audit_run_performance")[0]["n"]
            == 4
        )
        assert warehouse.query("SELECT count(*) AS n FROM audit_run_span")[0]["n"] == 4
        current = PerformanceCollector(output_dir=tmp_path)
        current.metadata = {"backend": "local", "selection": "same"}
        baseline = store.baseline(current)
        assert baseline["count"] == 3
        assert baseline["median_seconds"] == 20
        assert baseline["previous_seconds"] == [10, 20, 30]
        current.metadata["selection"] = "new"
        assert store.baseline(current)["count"] == 0
    finally:
        warehouse.close()


def test_failed_storage_and_unwritable_artifact_do_not_fail_processing(tmp_path):
    collector = PerformanceCollector(output_dir=tmp_path)
    collector.attach_sink(lambda _: (_ for _ in ()).throw(RuntimeError("private dsn")))
    with collector.activate():
        with span("gold"):
            pass
    report = collector.finish("success")
    assert report["status"] == "success"
    assert report["telemetry_errors"] == ["storage"]
    assert (tmp_path / f"{collector.run_id}.jsonl").read_text()
    assert "private dsn" not in (tmp_path / f"{collector.run_id}.json").read_text()
