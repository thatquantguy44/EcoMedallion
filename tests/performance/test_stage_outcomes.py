"""Verdict and persistence contracts for completed, skipped and failed work."""

import pytest

from fred_pipeline.config import PipelineConfig
from fred_pipeline.governance.stages import (
    RunStageTracker,
    StageRecord,
    StageStatus,
    overall_verdict,
)
from fred_pipeline.io.local_store import LocalWarehouse
from fred_pipeline.performance import PerformanceCollector
from fred_pipeline.performance_store import PerformanceStore


@pytest.mark.parametrize(
    "status,problem",
    [
        (StageStatus.FAILED, True),
        (StageStatus.NOT_RUN, True),
        (StageStatus.WARNED, False),
        (StageStatus.SUCCEEDED, False),
        (StageStatus.SKIPPED, False),
    ],
)
def test_problem_classification_distinguishes_warnings_and_deliberate_skips(
    status, problem
):
    record = StageRecord("gold", status)
    assert record.is_problem is problem
    assert record.to_dict()["status"] == status.value


def test_warn_does_not_create_a_stage_or_downgrade_an_existing_failure():
    tracker = RunStageTracker()
    assert tracker.warn("unknown", "fixture") is None
    assert tracker.records(include_not_run=False) == []
    with tracker.stage("gold", swallow=True):
        raise ValueError("fixture failure")
    tracker.warn("gold", "additional warning")
    assert tracker.get("gold").status is StageStatus.FAILED
    assert tracker.get("gold").detail["warnings"] == ["additional warning"]
    assert [record.name for record in tracker.failed_stages()] == ["gold"]


@pytest.mark.parametrize(
    "failed,total,calendar,extract,expected,reason",
    [
        (
            2,
            10,
            StageStatus.FAILED,
            StageStatus.SUCCEEDED,
            "partial",
            "optional stage(s) failed",
        ),
        (
            2,
            10,
            StageStatus.SUCCEEDED,
            StageStatus.SUCCEEDED,
            "partial",
            "2 of 10 series failed",
        ),
        (
            10,
            10,
            StageStatus.SUCCEEDED,
            StageStatus.SUCCEEDED,
            "failure",
            "all 10 series failed",
        ),
        (0, 10, StageStatus.WARNED, StageStatus.SUCCEEDED, "partial", "warnings"),
        (0, 0, StageStatus.SKIPPED, StageStatus.SKIPPED, "success", "nothing to do"),
    ],
)
def test_overall_verdict_preserves_degraded_series_and_optional_stage_context(
    failed,
    total,
    calendar,
    extract,
    expected,
    reason,
):
    records = [
        StageRecord(name, StageStatus.SUCCEEDED) for name in ("plan", "gold", "persist")
    ]
    records.extend(
        [StageRecord("extract", extract), StageRecord("release_calendar", calendar)]
    )
    verdict, detail = overall_verdict(records, series_failed=failed, series_total=total)
    assert verdict == expected
    assert reason in detail


def test_completed_stage_warning_is_persisted_on_the_next_flush(tmp_path):
    warehouse = LocalWarehouse(
        PipelineConfig(fred_api_key=""), db_path=str(tmp_path / "warn.db")
    )
    collector = PerformanceCollector(output_dir=tmp_path)
    store = PerformanceStore(warehouse)
    collector.attach_sink(store.write_spans)
    try:
        with collector.activate():
            tracker = RunStageTracker()
            with tracker.stage("fomc_calendar"):
                pass
            assert warehouse.query("SELECT status FROM audit_run_span") == [
                {"status": "succeeded"}
            ]
            tracker.warn("fomc_calendar", "fixture existing calendar retained")
            collector.flush()
        assert warehouse.query("SELECT status FROM audit_run_span") == [
            {"status": "warned"}
        ]
        assert collector.finish()["stages"][0]["status"] == "warned"
    finally:
        warehouse.close()
