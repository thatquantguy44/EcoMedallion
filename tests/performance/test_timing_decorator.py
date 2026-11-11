"""The timing API must preserve callable behavior even when telemetry fails."""

import asyncio
import inspect
import json

import pytest

from fred_pipeline.performance import PerformanceCollector, performance_run
from fred_pipeline.timing import timed


@pytest.mark.parametrize(
    "bad_tags", [None, ["source", "fred"], {1: "invalid-key"}, {"name": "collision"}]
)
def test_invalid_attribute_callback_result_does_not_replace_success(tmp_path, bad_tags):
    collector = PerformanceCollector(output_dir=tmp_path)

    @timed("fixture.operation", attributes=lambda: bad_tags)
    def work():
        return 42

    with collector.activate():
        assert work() == 42
    assert collector.finish()["spans"][0]["status"] == "succeeded"


def test_attribute_callback_exception_does_not_mask_original_operation_error(tmp_path):
    def unavailable():
        raise RuntimeError("fixture attribute failure")

    original = ValueError("fixture operation failure")

    @timed("fixture.operation", attributes=unavailable)
    def work():
        raise original

    collector = PerformanceCollector(output_dir=tmp_path)
    with collector.activate(), pytest.raises(ValueError) as caught:
        work()
    assert caught.value is original
    assert collector.finish()["spans"][0]["error_type"] == "ValueError"


def test_sync_and_async_decorators_preserve_metadata_results_and_row_counts(tmp_path):
    @timed(count_result="rows", log_completion=False)
    def sync_work(value: int = 3) -> list[int]:
        """Fixture docstring."""
        return list(range(value))

    @timed("async.rows", count_result="rows", log_completion=False)
    async def async_work(value):
        await asyncio.sleep(0)
        return list(range(value))

    assert sync_work.__name__ == "sync_work"
    assert sync_work.__doc__ == "Fixture docstring."
    assert str(inspect.signature(sync_work)) == "(value: int = 3) -> list[int]"
    collector = PerformanceCollector(output_dir=tmp_path)
    with collector.activate():
        assert sync_work() == [0, 1, 2]
        assert asyncio.run(async_work(2)) == [0, 1]
    assert sorted(
        row["counters"]["rows"] for row in collector.finish()["aggregates"]
    ) == [2, 3]


def test_log_only_decorator_does_not_create_spans_but_calls_progress(tmp_path):
    progress = []

    @timed(
        "log-only",
        collect=False,
        progress_callback=lambda label, seconds: progress.append((label, seconds)),
    )
    def work():
        return "result"

    collector = PerformanceCollector(output_dir=tmp_path)
    with collector.activate():
        assert work() == "result"
    assert collector.finish()["spans"] == []
    assert progress[0][0] == "log-only" and progress[0][1] >= 0


def test_collection_failure_does_not_fail_wrapped_work(tmp_path, monkeypatch):
    collector = PerformanceCollector(output_dir=tmp_path)

    def unavailable(*_args, **_kwargs):
        raise OSError("fixture private storage detail")

    monkeypatch.setattr(collector, "_record", unavailable)

    @timed("work")
    def work():
        return 7

    with collector.activate():
        assert work() == 7
    report = collector.finish("success")
    assert report["telemetry_errors"] == ["collection"]
    assert "fixture private storage detail" not in str(report)


def test_outer_run_scope_saves_failure_and_nested_calls_share_one_file(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path))

    @performance_run
    def inner():
        raise LookupError("fixture-original-error")

    @performance_run
    def outer():
        return inner()

    with pytest.raises(LookupError, match="fixture-original-error"):
        outer()
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    report = json.loads(files[0].read_text())
    assert report["status"] == "failure"
    assert "fixture-original-error" not in str(report)
