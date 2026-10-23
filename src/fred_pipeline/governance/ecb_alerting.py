"""Operational health alerts for the ECB eligible-assets pipeline.

The observation pipeline's stage alert is emitted before the separate MID poll,
so ECB failures need their own assessment and message.  This module reuses the
existing alert routes and transports while keeping ECB freshness and data-
quality rules explicit and independently testable.
"""

from __future__ import annotations

import html
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from fred_pipeline.governance.alerting_config import (
    AlertingConfig,
    ECBAssetsAlertConfig,
    load_alerting_config,
)
from fred_pipeline.governance.email_transport import (
    EmailMessageSpec,
    EmailSendError,
    EmailTransport,
    build_transport,
)

log = logging.getLogger("fred_pipeline.ecb_alerting")

_FRANKFURT = ZoneInfo("Europe/Berlin")


@dataclass(frozen=True)
class ECBAssetsAssessment:
    verdict: str
    reason: str
    details: Mapping[str, Any] = field(default_factory=dict)


def _easter_sunday(year: int) -> date:
    """Return Gregorian Easter Sunday without an optional calendar package."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    weekday_adjustment = (32 + 2 * e + 2 * i - h - k) % 7
    correction = (a + 11 * h + 22 * weekday_adjustment) // 451
    shifted = h + weekday_adjustment - 7 * correction + 114
    month = shifted // 31
    day = shifted % 31 + 1
    return date(year, month, day)


def is_target_operating_day(day: date) -> bool:
    """Whether TARGET is normally open on ``day``.

    ECB eligible-assets publication is Monday-Friday excluding the six TARGET
    closing days: New Year, Good Friday, Easter Monday, 1 May, and 25/26 Dec.
    """
    if day.weekday() >= 5:
        return False
    easter = _easter_sunday(day.year)
    holidays = {
        date(day.year, 1, 1),
        easter - timedelta(days=2),
        easter + timedelta(days=1),
        date(day.year, 5, 1),
        date(day.year, 12, 25),
        date(day.year, 12, 26),
    }
    return day not in holidays


def expected_release_date(now: datetime, settings: ECBAssetsAlertConfig) -> date:
    """Latest ECB operating date whose release should have arrived by now."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(_FRANKFURT)
    hour, minute = (
        int(part) for part in settings.expected_release_time_local.split(":")
    )
    cutoff = datetime.combine(local.date(), time(hour, minute), _FRANKFURT)
    cutoff += timedelta(minutes=settings.grace_period_minutes)
    candidate = local.date()
    if local < cutoff or not is_target_operating_day(candidate):
        candidate -= timedelta(days=1)
    while not is_target_operating_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def assess_ecb_assets(
    *,
    action: str,
    results: Sequence[Mapping[str, Any]] = (),
    status: Mapping[str, Any] | None = None,
    error: str = "",
    now: datetime | None = None,
    settings: ECBAssetsAlertConfig | None = None,
) -> ECBAssetsAssessment:
    """Classify one ECB operation as success, partial, or failure."""
    settings = settings or ECBAssetsAlertConfig()
    status = status or {}
    now = now or datetime.now(timezone.utc)
    latest = status.get("latest_accepted") or {}
    current_count = int(status.get("current_eligible_count") or 0)
    details: dict[str, Any] = {
        "action": action,
        "releases_processed": len(results),
        "current_eligible_count": current_count,
    }

    if error:
        details["error"] = error
        return ECBAssetsAssessment("failure", f"ECB {action} failed", details)

    invalid = [row for row in results if row.get("valid") is False]
    failed = [
        row
        for row in results
        if str(row.get("status", "")).lower() in {"failed", "quarantined"}
    ]
    if invalid or failed:
        details["invalid_artifacts"] = len(invalid)
        details["failed_releases"] = len(failed)
        return ECBAssetsAssessment(
            "failure", "ECB validation or signature verification failed", details
        )

    latest_at = _parse_timestamp(latest.get("publication_at_utc"))
    quarantined = list(status.get("quarantined") or [])
    unresolved_quarantine = []
    for row in quarantined:
        quarantined_at = _parse_timestamp(row.get("publication_at_utc"))
        if latest_at is None or quarantined_at is None or quarantined_at > latest_at:
            unresolved_quarantine.append(row)
    if settings.alert_on_quarantine and unresolved_quarantine:
        details["unresolved_quarantined_releases"] = len(unresolved_quarantine)
        details["latest_quarantine_error"] = unresolved_quarantine[0].get(
            "error_detail", ""
        )
        return ECBAssetsAssessment(
            "failure", "ECB has an unresolved quarantined release", details
        )

    if not latest:
        return ECBAssetsAssessment(
            "failure", "ECB has no accepted eligible-assets release", details
        )

    expected = expected_release_date(now, settings)
    latest_local_date = latest_at.astimezone(_FRANKFURT).date() if latest_at else None
    details.update(
        {
            "latest_release_id": latest.get("release_id", ""),
            "latest_publication_at_utc": latest.get("publication_at_utc", ""),
            "expected_release_date": expected.isoformat(),
            "coverage_status": latest.get("coverage_status", "unknown"),
            "reconciliation_status": latest.get("reconciliation_status", "unknown"),
        }
    )
    if latest_local_date is None or latest_local_date < expected:
        return ECBAssetsAssessment(
            "partial",
            f"ECB eligible-assets release is late; expected {expected.isoformat()}",
            details,
        )

    coverage = str(latest.get("coverage_status", "")).lower()
    if settings.alert_on_gap and coverage == "gapped":
        return ECBAssetsAssessment("partial", "ECB release coverage is gapped", details)

    for row in results:
        if str(row.get("coverage_status", "")).lower() == "starts_here":
            continue
        changes = sum(
            int(row.get(key) or 0)
            for key in ("new_count", "updated_count", "deleted_count")
        )
        prior_count = max(
            1,
            int(row.get("full_count") or current_count)
            - int(row.get("new_count") or 0)
            + int(row.get("deleted_count") or 0),
        )
        change_percent = changes * 100.0 / prior_count
        if change_percent > settings.max_change_percent:
            details["change_percent"] = round(change_percent, 4)
            details["change_threshold_percent"] = settings.max_change_percent
            return ECBAssetsAssessment(
                "partial",
                "ECB release change volume exceeded its alert threshold",
                details,
            )

    reason = (
        f"ECB accepted {len(results)} eligible-assets release(s)"
        if results
        else "ECB eligible-assets feed is current; no new release"
    )
    return ECBAssetsAssessment("success", reason, details)


def _build_message(
    assessment: ECBAssetsAssessment,
    config: AlertingConfig,
    *,
    environment: str,
) -> EmailMessageSpec | None:
    route = config.route_for(assessment.verdict)
    if not route.has_recipients:
        return None
    label = assessment.verdict.upper()
    env = f"[{environment}]" if environment else ""
    prefix = f"{config.subject_prefix}[ECB]"
    subject = f"{prefix}{env} {label} — {assessment.reason}"
    detail_lines = [f"{key}: {value}" for key, value in assessment.details.items()]
    text_body = "\n".join(
        [
            f"{label} — {assessment.reason}",
            "",
            f"environment: {environment or 'n/a'}",
            *detail_lines,
        ]
    )
    detail_html = "".join(
        f"<tr><td><strong>{html.escape(str(key))}</strong></td>"
        f"<td>{html.escape(str(value))}</td></tr>"
        for key, value in assessment.details.items()
    )
    html_body = (
        "<!doctype html><html><body>"
        f"<h2>ECB eligible-assets {html.escape(label)}</h2>"
        f"<p>{html.escape(assessment.reason)}</p>"
        f"<p>Environment: {html.escape(environment or 'n/a')}</p>"
        f"<table>{detail_html}</table>"
        "</body></html>"
    )
    attachments: tuple[tuple[str, bytes, str], ...] = ()
    if config.attach_run_json:
        payload = {
            "environment": environment,
            "verdict": assessment.verdict,
            "reason": assessment.reason,
            "details": dict(assessment.details),
        }
        attachments = (
            (
                "ecb-assets-alert.json",
                json.dumps(payload, indent=2, default=str).encode("utf-8"),
                "application/json",
            ),
        )
    return EmailMessageSpec(
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        to=route.to,
        cc=route.cc,
        bcc=route.bcc,
        from_address=config.from_address,
        from_name=config.from_name,
        attachments=attachments,
    )


def send_ecb_assets_alert(
    *,
    action: str,
    results: Sequence[Mapping[str, Any]] = (),
    status: Mapping[str, Any] | None = None,
    error: str = "",
    environment: str = "",
    config: AlertingConfig | None = None,
    transport: EmailTransport | None = None,
    now: datetime | None = None,
) -> bool:
    """Assess and send one ECB alert, without ever failing the pipeline."""
    try:
        config = config or load_alerting_config()
        if not config.ecb_assets.enabled:
            return False
        assessment = assess_ecb_assets(
            action=action,
            results=results,
            status=status,
            error=error,
            now=now,
            settings=config.ecb_assets,
        )
        if not config.should_send(assessment.verdict, environment):
            if assessment.verdict != "success":
                log.warning(
                    "%s — no ECB alert sent (policy=%s, enabled=%s)",
                    assessment.reason,
                    config.policy,
                    config.enabled,
                )
            return False
        message = _build_message(assessment, config, environment=environment)
        if message is None:
            log.warning("ECB alert has no recipients for %s", assessment.verdict)
            return False
        (transport or build_transport(config)).send(message)
        log.info("ECB alert sent to %s", ", ".join(message.all_recipients))
        return True
    except EmailSendError:
        log.exception("ECB alert could not be delivered")
        return False
    except Exception:
        log.exception("Unexpected error while sending ECB alert")
        return False
