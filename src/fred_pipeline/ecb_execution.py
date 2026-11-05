"""Shared, Dagster-independent ECB forward-poll execution boundary.

The caller owns the warehouse and any injected HTTP session. This initial
boundary centralizes authority checks and polling; writer exclusion and the
complete health/notification report are separate prerequisites for Dagster.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from fred_pipeline.config import PipelineConfig


class ECBExecutionError(RuntimeError):
    """The shared ECB execution boundary could not run."""


class ECBAuthorityError(ECBExecutionError):
    """The selected warehouse is not the configured ECB writer."""


@dataclass(frozen=True)
class ECBPollReport:
    operation_id: str
    selected_backend: str
    outcome: str
    releases: tuple[dict[str, Any], ...]
    started_at: str
    finished_at: str
    repository_status: dict[str, Any]
    health_verdict: str
    notification_status: str


class ECBPollFailure(ECBExecutionError):
    """A poll failed; the report retains results committed before the failure."""

    def __init__(self, message: str, report: ECBPollReport):
        super().__init__(message)
        self.report = report


def execute_ecb_poll(
    config: PipelineConfig,
    *,
    warehouse: Any,
    selected_backend: str,
    authoritative_backend: str,
    allow_gap: bool = False,
    session: Any = None,
    client: Any = None,
    pipeline_factory: Callable[..., Any] | None = None,
    clock: Callable[[], datetime] | None = None,
    health_settings: Any = None,
    notify: Callable[[list[dict[str, Any]], dict[str, Any], str], Any] | None = None,
) -> ECBPollReport:
    """Poll a borrowed warehouse and optionally notify with the raw health inputs.

    Inject the configured ECB alert settings when reporting health. Without a
    notifier, the report records that delivery was not attempted; callers that
    still own alerting must not treat that as a delivery decision.
    """
    if authoritative_backend == "disabled":
        raise ECBAuthorityError("ECB eligible-assets writes are disabled")
    if selected_backend != authoritative_backend:
        raise ECBAuthorityError("selected backend is not the authoritative ECB backend")
    if selected_backend not in {"local", "postgres"} or not (
        hasattr(warehouse, "conn") and hasattr(warehouse, "_insert")
    ):
        raise ECBAuthorityError("the authoritative backend has no supported ECB writer")
    if config.network.mode != "env" and client is None and session is None:
        raise ECBExecutionError("managed ECB polling requires a startup-selected route")

    from fred_pipeline.ecb_eligible_assets import (
        ECBEligibleAssetRepository,
        ECBEligibleAssetsPipeline,
        ECBMIDClient,
    )
    from fred_pipeline.governance.ecb_alerting import assess_ecb_assets

    now = clock or (lambda: datetime.now(timezone.utc))
    started_at = now().isoformat()
    operation_id = uuid.uuid4().hex
    if client is None:
        client = ECBMIDClient(
            base_url=config.ecb_mid_base_url,
            timeout=config.request_timeout_seconds,
            max_retries=config.max_retries,
            rate_limit_per_minute=min(config.rate_limit_per_minute, 60),
            session=session,
        )
    factory = pipeline_factory or (lambda w, c: ECBEligibleAssetsPipeline(w, client=c))
    releases: list[dict[str, Any]] = []
    error: Exception | None = None
    try:
        factory(warehouse, client).poll(
            allow_gap=allow_gap, on_result=lambda row: releases.append(row.to_dict())
        )
    except Exception as exc:
        error = exc

    status: dict[str, Any] = {}
    health_error = str(error) if error is not None else ""
    health_verdict = "unknown"
    try:
        status = ECBEligibleAssetRepository(warehouse).status()
        health_verdict = assess_ecb_assets(
            action="poll",
            results=releases,
            status=status,
            error=health_error,
            now=now(),
            settings=health_settings,
        ).verdict
    except Exception:  # health reporting must not undo a committed release
        if not health_error:
            health_error = "could not assess ECB health status"

    notification_status = "not_attempted"
    if notify is not None:
        try:
            delivery = notify(list(releases), status, health_error)
            notification_status = getattr(delivery, "status", "unknown")
            if notification_status not in {
                "accepted",
                "disabled",
                "failed",
                "previewed",
                "suppressed",
                "unknown",
            }:
                notification_status = "unknown"
        except Exception:  # delivery is nonfatal and may contain credentials
            notification_status = "failed"

    latest = status.get("latest_accepted") or {}
    safe_status = {
        "latest_accepted": {
            key: latest.get(key)
            for key in (
                "release_id",
                "publication_at_utc",
                "coverage_status",
                "reconciliation_status",
            )
        }
        if latest
        else None,
        "current_eligible_count": status.get("current_eligible_count"),
        "quarantined_count": len(status.get("quarantined") or []),
    }
    report = ECBPollReport(
        operation_id=operation_id,
        selected_backend=selected_backend,
        outcome="failed"
        if error is not None
        else "accepted"
        if any(row.get("status") == "accepted" for row in releases)
        else "no_new_release",
        releases=tuple(releases),
        started_at=started_at,
        finished_at=now().isoformat(),
        repository_status=safe_status,
        health_verdict=health_verdict,
        notification_status=notification_status,
    )
    if error is not None:
        # Preserve CLI diagnostics; orchestration metadata must use the report.
        raise ECBPollFailure(str(error), report) from error
    return report
