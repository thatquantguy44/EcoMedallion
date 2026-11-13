"""Dagster-independent workload invocation and sanitized outcome reports."""

from dataclasses import dataclass, field
from typing import Any

from fred_pipeline.governance.stages import StageStatus, overall_verdict


@dataclass(frozen=True)
class JobReport:
    verdict: str
    details: dict[str, Any] = field(default_factory=dict)
    asset_key: tuple[str, ...] = ()
    materializations: tuple[dict[str, Any], ...] = ()


def pipeline_report(pipeline, run, *, durable: bool) -> JobReport:
    tracker = pipeline._stage_tracker
    verdict, _ = overall_verdict(
        tracker.records(),
        series_failed=run.series_failed,
        series_total=run.series_total,
        required_stages=tracker.required_stages,
    )
    details = {
        "pipeline_run_id": run.run_id,
        "series_total": run.series_total,
        "series_succeeded": run.series_succeeded,
        "series_failed": run.series_failed,
        "stages": {record.name: record.status.value for record in tracker.records()},
    }
    delivery = getattr(pipeline, "_last_alert_result", None)
    if delivery is not None:
        details["notification_status"] = delivery.status
    gold = tracker.get("gold")
    if gold and gold.detail.get("tables_not_ok"):
        verdict = "failure"
    updated = bool(
        durable
        and verdict == "success"
        and gold
        and gold.status is StageStatus.SUCCEEDED
        and not gold.detail.get("unchanged")
    )
    return JobReport(
        verdict,
        details,
        ("macro", "pipeline"),
        ({"pipeline_run_id": run.run_id},) if updated else (),
    )


def gold_result_succeeded(result: dict[str, Any]) -> bool:
    """Gold builders return per-table statuses even when no exception escaped."""
    return bool(result) and all(str(value).lower() == "ok" for value in result.values())
