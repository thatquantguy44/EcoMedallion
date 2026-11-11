"""Deterministic edge contracts for the collector, outside live pipeline runs."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from fred_pipeline.performance import (
    PerformanceCollector,
    current_performance,
    span,
    submit_with_context,
    suspend_performance,
)


def test_parallel_worker_seconds_are_not_added_to_elapsed_time(tmp_path, manual_clock):
    collector = PerformanceCollector(clock=manual_clock, output_dir=tmp_path)
    ready = threading.Barrier(3)
    release = threading.Event()

    def work():
        with span("gold.engine"):
            ready.wait(timeout=5)
            assert release.wait(timeout=5)

    with collector.activate(), ThreadPoolExecutor(max_workers=2) as pool:
        with span("gold"):
            jobs = [submit_with_context(pool, work) for _ in range(2)]
            ready.wait(timeout=5)
            manual_clock.advance(2)
            release.set()
            for job in jobs:
                job.result(timeout=5)
    report = collector.finish("success")
    engine = next(row for row in report["aggregates"] if row["name"] == "gold.engine")
    assert report["processing_seconds"] == 2
    assert engine["worker_seconds"] == 4
    assert engine["count"] == 2


def test_progress_flush_uses_batch_and_time_thresholds_then_forces_completion(
    tmp_path, manual_clock
):
    collector = PerformanceCollector(clock=manual_clock, output_dir=tmp_path)
    collector.flush_batch_size = 4
    batches = []
    collector.attach_sink(lambda rows: batches.append(rows))
    with collector.activate():
        with span("first"):
            pass
        collector.flush(force=False)
        assert not batches
        manual_clock.advance(5)
        collector.flush(force=False)
        assert len(batches) == 1
        for name in ("second", "third"):
            with span(name):
                pass
        collector.flush(force=False)
        assert len(batches) == 2
        with span("last"):
            pass
        collector.flush(force=False)
        assert len(batches) == 2
    collector.finish("success")
    assert len(batches) == 3
    assert sum(len(batch) for batch in batches) == 8


def test_journal_keeps_start_and_final_outcome_after_exception(tmp_path, manual_clock):
    collector = PerformanceCollector(clock=manual_clock, output_dir=tmp_path)
    original = ValueError("fixture-private-provider-message")
    with collector.activate(), pytest.raises(ValueError) as caught:
        with span("gold.engine"):
            manual_clock.advance(3)
            raise original
    collector.finish("failure")
    rows = [
        json.loads(line)
        for line in (tmp_path / f"{collector.run_id}.jsonl").read_text().splitlines()
    ]
    assert caught.value is original
    assert [row["event"] for row in rows] == ["span_started", "span_finished"]
    assert [row["status"] for row in rows] == ["running", "failed"]
    assert rows[0]["span_id"] == rows[1]["span_id"]
    assert rows[1]["duration_seconds"] == 3
    assert rows[1]["error_type"] == "ValueError"
    assert "fixture-private-provider-message" not in str(rows)


def test_detailed_measurements_still_aggregate_when_samples_are_disabled(tmp_path):
    collector = PerformanceCollector(output_dir=tmp_path, detailed=False)
    with collector.activate():
        with span("extract.fetch", source="ECB MID", series_id="EXAMPLE") as parent:
            with span("http.request", detailed=True):
                pass
    report = collector.finish("success")
    assert [row["span_id"] for row in report["spans"]] == [parent.span_id]
    assert {row["name"] for row in report["aggregates"]} == {
        "http.request",
        "extract.fetch",
    }
    assert {row["source"] for row in report["aggregates"]} == {"ecb_assets"}


def test_percentiles_use_disclosed_recent_bounded_sample(tmp_path, manual_clock):
    collector = PerformanceCollector(
        clock=manual_clock, output_dir=tmp_path, max_spans=8
    )
    with collector.activate():
        for seconds in range(1, 2050):
            with span("http.request", source="bea", detailed=True):
                manual_clock.advance(seconds)
    aggregate = collector.finish()["aggregates"][0]
    assert aggregate["count"] == 2049
    assert aggregate["sample_count"] == 2048
    assert aggregate["sampled"] is True
    assert aggregate["p50_seconds"] == 1025
    assert aggregate["p95_seconds"] == 1947


def test_aggregate_limit_does_not_leave_completed_events_running(tmp_path):
    collector = PerformanceCollector(output_dir=tmp_path, max_spans=2048)
    with collector.activate():
        for index in range(513):
            with span(f"fixture.operation.{index}"):
                pass
    report = collector.finish()
    assert len(report["aggregates"]) == 512
    assert "aggregate_limit" in report["telemetry_errors"]
    rows = [
        json.loads(line)
        for line in (tmp_path / f"{collector.run_id}.jsonl").read_text().splitlines()
    ]
    latest = {row["span_id"]: row for row in rows}
    assert len(latest) == 513
    assert all(row["status"] == "succeeded" for row in latest.values())


def test_unwritable_journal_and_artifact_do_not_fail_the_run(tmp_path):
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("fixture")
    collector = PerformanceCollector(output_dir=blocked)
    with collector.activate():
        with span("gold.engine"):
            pass
    report = collector.finish("success")
    assert report["status"] == "success"
    assert set(report["telemetry_errors"]) == {"journal", "artifact"}
    assert not (tmp_path / "not-a-directory.json").exists()


def test_suppressed_and_nested_collectors_restore_parent_context(tmp_path):
    outer = PerformanceCollector(output_dir=tmp_path / "outer")
    inner = PerformanceCollector(output_dir=tmp_path / "inner")
    with outer.activate():
        with span("outer") as parent:
            with suspend_performance():
                assert current_performance() is None
                with span("not-recorded"):
                    pass
            with inner.activate():
                with span("inner"):
                    pass
            assert current_performance() is outer
            with span("child"):
                pass
    outer_report = outer.finish()
    assert current_performance() is None
    child = next(row for row in outer_report["spans"] if row["name"] == "child")
    assert child["parent_span_id"] == parent.span_id
    assert {row["name"] for row in outer_report["spans"]} == {"outer", "child"}
    assert inner.finish()["spans"][0]["parent_span_id"] is None
