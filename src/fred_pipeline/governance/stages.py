"""Stage-level tracking for a pipeline run.

``audit.etl_run`` records a run's *series* outcome — how many series succeeded,
how many failed. That is not the same thing as whether the run as a whole did
its job, and the difference has teeth:

``FredPipeline.run()`` refreshes the Gold layer inside a ``try/except`` that
logs and swallows. ``run.finalize()`` has already computed ``RunStatus`` from
series outcomes alone, so a run whose Gold build raised still reports
``SUCCEEDED`` — and every Power BI report downstream quietly serves yesterday's
tables. Nothing in the audit trail says otherwise.

This module records what actually happened, stage by stage, so a run summary can
say "series fine, Gold FAILED" instead of a green tick. It is deliberately
dependency-free and pure: :class:`RunStageTracker` is a plain object a caller
drives with a context manager, and :func:`overall_verdict` is a function of the
records.

Stages are *declared* (``EXPECTED_STAGES``) rather than discovered, so a stage
that never ran at all is visible as ``NOT_RUN`` rather than simply absent — the
failure mode where a whole phase is skipped by a bad flag looks identical to
success if you only look at the stages that reported.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Iterator, Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


class StageStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    #: Deliberately not run (a flag turned it off). Not a problem.
    SKIPPED = "skipped"
    #: Ran but produced a degraded result the caller wants surfaced.
    WARNED = "warned"
    #: Declared but never reached — usually means the run died earlier.
    NOT_RUN = "not_run"


#: Whether a stage failing should fail the whole run. A stage marked
#: ``required=False`` failing degrades the verdict to PARTIAL instead.
@dataclass(frozen=True)
class StageSpec:
    name: str
    description: str
    required: bool = True


# The stages FredPipeline.run() actually has. Order is execution order, which
# is also the order they are rendered in the summary email.
EXPECTED_STAGES: tuple[StageSpec, ...] = (
    StageSpec("plan", "Resolve each series' load window against the warehouse"),
    StageSpec("extract", "Fetch from source APIs; write Bronze, Silver and DQ"),
    StageSpec(
        "fomc_calendar",
        "Check config/fomc.yml has enough runway for the FOMC Gold tables "
        "(and, with --refresh-fomc-calendar, add newly published meetings)",
        required=False,
    ),
    StageSpec("gold", "Rebuild the Gold analytical layer"),
    StageSpec(
        "release_calendar",
        "Re-fetch the forward economic release calendar",
        required=False,
    ),
    StageSpec("persist", "Write the run and series audit rows"),
)

_STAGE_INDEX = {spec.name: i for i, spec in enumerate(EXPECTED_STAGES)}


@dataclass
class StageRecord:
    """One stage's outcome. ``detail`` carries stage-specific counts that the
    summary renders as a sub-line (rows written, tables built, and so on)."""

    name: str
    status: StageStatus
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    detail: dict[str, Any] = field(default_factory=dict)
    error_type: str = ""
    error_message: str = ""

    @property
    def is_problem(self) -> bool:
        return self.status in (StageStatus.FAILED, StageStatus.NOT_RUN)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "detail": dict(self.detail),
            "error_type": self.error_type,
            "error_message": self.error_message,
        }


class RunStageTracker:
    """Records stage outcomes for one run.

    Usage mirrors how the pipeline is written — a context manager per stage,
    which records duration and captures an exception without changing whether
    it propagates::

        tracker = RunStageTracker()
        with tracker.stage("gold") as st:
            warehouse.build_gold()
            st.detail["tables"] = 60

    ``swallow=True`` records the failure and suppresses the exception, which is
    what the Gold and release-calendar stages want: a Gold failure should be
    *reported*, not abort a run whose ingestion succeeded.
    """

    def __init__(self) -> None:
        self._records: dict[str, StageRecord] = {}

    @contextmanager
    def stage(self, name: str, *, swallow: bool = False) -> Iterator[StageRecord]:
        record = StageRecord(name=name, status=StageStatus.SUCCEEDED, started_at=_now())
        self._records[name] = record
        started = time.monotonic()
        try:
            yield record
        except BaseException as exc:  # noqa: BLE001 - re-raised unless swallowed
            record.status = StageStatus.FAILED
            record.error_type = type(exc).__name__
            record.error_message = str(exc)[:2000]
            if not swallow:
                raise
        finally:
            record.ended_at = _now()
            record.duration_seconds = time.monotonic() - started

    def skip(self, name: str, reason: str = "") -> StageRecord:
        """Record a stage that was deliberately not run."""
        record = StageRecord(name=name, status=StageStatus.SKIPPED)
        if reason:
            record.detail["reason"] = reason
        self._records[name] = record
        return record

    def warn(self, name: str, message: str) -> Optional[StageRecord]:
        """Downgrade an already-succeeded stage to WARNED."""
        record = self._records.get(name)
        if record is None:
            return None
        if record.status is StageStatus.SUCCEEDED:
            record.status = StageStatus.WARNED
        record.detail.setdefault("warnings", []).append(message)
        return record

    def get(self, name: str) -> Optional[StageRecord]:
        return self._records.get(name)

    def records(self, *, include_not_run: bool = True) -> list[StageRecord]:
        """Every stage in execution order.

        Declared stages that never reported come back as ``NOT_RUN`` so a phase
        skipped by accident is visible, rather than merely missing.
        """
        out: list[StageRecord] = []
        for spec in EXPECTED_STAGES:
            record = self._records.get(spec.name)
            if record is None:
                if not include_not_run:
                    continue
                record = StageRecord(name=spec.name, status=StageStatus.NOT_RUN)
            out.append(record)
        # Any ad-hoc stage the caller tracked that is not declared, appended in
        # insertion order so nothing is lost.
        for name, record in self._records.items():
            if name not in _STAGE_INDEX:
                out.append(record)
        return out

    def failed_stages(self) -> list[StageRecord]:
        return [r for r in self.records() if r.status is StageStatus.FAILED]

    def to_dicts(self) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self.records()]


def overall_verdict(
    records: Iterable[StageRecord],
    *,
    series_failed: int = 0,
    series_total: int = 0,
) -> tuple[str, str]:
    """Reduce stages (and series counts) to one verdict and a plain reason.

    Returns ``(verdict, reason)`` where verdict is ``"success"``, ``"partial"``
    or ``"failure"``. This is the single line a reader should be able to act on,
    so the reason names the specific stage rather than saying "see above".

    A required stage failing is a **failure** even when every series ingested
    cleanly — that is the Gold case this module exists for.
    """
    records = list(records)
    by_name = {r.name: r for r in records}
    required = {spec.name for spec in EXPECTED_STAGES if spec.required}

    hard_failures = [
        r for r in records if r.status is StageStatus.FAILED and r.name in required
    ]
    if hard_failures:
        names = ", ".join(r.name for r in hard_failures)
        return "failure", f"required stage(s) failed: {names}"

    not_run = [
        r for r in records if r.status is StageStatus.NOT_RUN and r.name in required
    ]
    if not_run:
        names = ", ".join(r.name for r in not_run)
        return "failure", f"required stage(s) never ran: {names}"

    soft_failures = [
        r for r in records if r.status is StageStatus.FAILED and r.name not in required
    ]
    if series_total and series_failed >= series_total:
        return "failure", f"all {series_total} series failed"
    if series_failed:
        detail = f"{series_failed} of {series_total} series failed"
        if soft_failures:
            names = ", ".join(r.name for r in soft_failures)
            detail += f"; optional stage(s) failed: {names}"
        return "partial", detail
    if soft_failures:
        names = ", ".join(r.name for r in soft_failures)
        return "partial", f"optional stage(s) failed: {names}"

    warned = [r for r in records if r.status is StageStatus.WARNED]
    if warned:
        names = ", ".join(r.name for r in warned)
        return "partial", f"stage(s) reported warnings: {names}"

    if by_name.get("extract") and by_name["extract"].status is StageStatus.SKIPPED:
        return "success", "nothing to do (extract skipped)"
    return "success", "all stages completed"
