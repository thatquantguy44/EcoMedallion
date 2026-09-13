"""Audit framework: run-level and series-level lineage records.

Every pipeline invocation opens an :class:`EtlRun`; every series processed gets
an :class:`EtlSeriesRun`. These are plain dataclasses (pure Python) that the
Spark layer persists into ``audit.etl_run`` / ``audit.etl_series_run`` and DQ
results into ``audit.data_quality_result``. Keeping them Spark-free means the
orchestration logic is testable and the audit trail is deterministic.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def new_run_id() -> str:
    return uuid.uuid4().hex


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"  # some series succeeded, some failed
    # spec007: series-level only, never a top-level EtlRun.status -- a series
    # judged not due for extraction never ran, so it's neither a success nor
    # a failure. Distinct from both so a run's own output can show why a
    # series doesn't appear in the extraction summary instead of looking
    # silently dropped.
    SKIPPED_NOT_DUE = "skipped_not_due"


@dataclass
class EtlSeriesRun:
    run_id: str
    series_id: str
    status: RunStatus = RunStatus.RUNNING
    load_type: str = ""
    started_at: datetime = field(default_factory=_now)
    ended_at: Optional[datetime] = None
    observations_extracted: int = 0
    rows_written_bronze: int = 0
    rows_merged_silver: int = 0
    dq_passed: Optional[bool] = None
    error_message: str = ""

    def complete(
        self,
        status: RunStatus,
        *,
        observations_extracted: int = 0,
        rows_written_bronze: int = 0,
        rows_merged_silver: int = 0,
        dq_passed: Optional[bool] = None,
        error_message: str = "",
    ) -> "EtlSeriesRun":
        self.status = status
        self.ended_at = _now()
        self.observations_extracted = observations_extracted
        self.rows_written_bronze = rows_written_bronze
        self.rows_merged_silver = rows_merged_silver
        self.dq_passed = dq_passed
        self.error_message = error_message
        return self

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    def to_row(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        d["duration_seconds"] = self.duration_seconds
        return d


@dataclass
class EtlRun:
    run_id: str = field(default_factory=new_run_id)
    environment: str = "dev"
    manifest_path: str = ""
    triggered_by: str = ""
    status: RunStatus = RunStatus.RUNNING
    started_at: datetime = field(default_factory=_now)
    ended_at: Optional[datetime] = None
    series_total: int = 0
    series_succeeded: int = 0
    series_failed: int = 0
    series_skipped_not_due: int = 0
    error_message: str = ""
    series_runs: list[EtlSeriesRun] = field(default_factory=list)

    def start_series(self, series_id: str, load_type: str = "") -> EtlSeriesRun:
        sr = EtlSeriesRun(run_id=self.run_id, series_id=series_id, load_type=load_type)
        self.series_runs.append(sr)
        return sr

    def finalize(self) -> "EtlRun":
        self.ended_at = _now()
        self.series_total = len(self.series_runs)
        self.series_succeeded = sum(
            1 for s in self.series_runs if s.status == RunStatus.SUCCEEDED
        )
        self.series_failed = sum(
            1 for s in self.series_runs if s.status == RunStatus.FAILED
        )
        if self.series_failed == 0 and self.series_total > 0:
            self.status = RunStatus.SUCCEEDED
        elif self.series_succeeded == 0:
            self.status = RunStatus.FAILED
        else:
            self.status = RunStatus.PARTIAL
        return self

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    def to_row(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "environment": self.environment,
            "manifest_path": self.manifest_path,
            "triggered_by": self.triggered_by,
            "status": self.status.value,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_seconds": self.duration_seconds,
            "series_total": self.series_total,
            "series_succeeded": self.series_succeeded,
            "series_failed": self.series_failed,
            "series_skipped_not_due": self.series_skipped_not_due,
            "error_message": self.error_message,
        }
