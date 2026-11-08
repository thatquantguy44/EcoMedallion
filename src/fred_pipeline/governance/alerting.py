"""Send the stage-tracking summary email for a run.

The entry point is :func:`send_run_alert`, which the pipeline calls once at the
end of a run. It decides whether to send (policy + environment + recipients),
renders both body parts, and hands the result to whichever transport the config
names.

**Alerting never fails a run.** Every exception is caught and logged. A pipeline
that ingested 2,800 series correctly must not be reported as failed because a
mail server was briefly unreachable — and the operator still gets the summary in
the log via the console transport.

The webhook notifier in :mod:`fred_pipeline.governance.notify` is unchanged and
still fires independently; this is an additional channel with a richer payload,
not a replacement.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional, Sequence

from fred_pipeline.governance.alerting_config import (
    AlertingConfig,
    load_alerting_config,
)
from fred_pipeline.governance.email_report import (
    RunSummary,
    build_summary,
    render_html,
    render_subject,
    render_text,
)
from fred_pipeline.governance.email_transport import (
    EmailMessageSpec,
    EmailTransport,
)
from fred_pipeline.governance.email_delivery import (
    DeliveryResult,
    deliver_message,
    new_delivery,
)
from fred_pipeline.governance.stages import (
    RunStageTracker,
    StageRecord,
    overall_verdict,
)

log = logging.getLogger("fred_pipeline.alerting")


def build_message(
    summary: RunSummary, config: AlertingConfig
) -> Optional[EmailMessageSpec]:
    """Render a summary into a sendable message, or None if nobody is routed."""
    route = config.route_for(summary.verdict)
    if not route.has_recipients:
        return None

    attachments: list[tuple[str, bytes, str]] = []
    if config.attach_run_json:
        payload = {
            "run_id": summary.run_id,
            "environment": summary.environment,
            "verdict": summary.verdict,
            "reason": summary.reason,
            "series": {
                "total": summary.series_total,
                "succeeded": summary.series_succeeded,
                "failed": summary.series_failed,
            },
            "stages": [record.to_dict() for record in summary.stages],
        }
        attachments.append(
            (
                f"run-{summary.run_id[:8]}.json",
                json.dumps(payload, indent=2).encode("utf-8"),
                "application/json",
            )
        )

    profile = config.profile_for(summary.verdict)
    return EmailMessageSpec(
        subject=render_subject(summary, config.subject_prefix),
        text_body=render_text(summary, max_failures=config.max_failures_listed),
        html_body=render_html(summary, max_failures=config.max_failures_listed),
        to=route.to,
        cc=route.cc,
        bcc=route.bcc,
        from_address=profile.from_address,
        from_name=profile.from_name,
        attachments=attachments,
    )


def send_run_alert_result(
    run: Any,
    tracker: RunStageTracker,
    *,
    config: Optional[AlertingConfig] = None,
    environment: str = "",
    transport: Optional[EmailTransport] = None,
    config_path: Optional[str] = None,
    http_client: Any = None,
    smtp_routes: Any = None,
) -> DeliveryResult:
    """Render and attempt the run summary, returning a structured outcome.

    ``transport`` is injectable so tests never touch a network or a mailbox;
    when omitted it is built from the config.
    """
    try:
        config = config or load_alerting_config(config_path)
    except Exception:
        log.exception("Could not load alerting config; falling back to no email alert")
        return new_delivery(AlertingConfig(), status="failed", category="configuration")

    try:
        stages: Sequence[StageRecord] = tracker.records()
        verdict, reason = overall_verdict(
            stages,
            series_failed=getattr(run, "series_failed", 0) or 0,
            series_total=getattr(run, "series_total", 0) or 0,
        )
        summary = build_summary(
            run,
            stages,
            verdict=verdict,
            reason=reason,
            environment=environment,
            max_failures=config.max_failures_listed,
        )

        if not config.should_send(verdict, environment):
            # Still surface a bad outcome somewhere, even when email is off --
            # a silent failure is the thing this module exists to prevent.
            if verdict != "success":
                log.warning(
                    "Run %s finished %s (%s) — no email sent (policy=%s, enabled=%s)",
                    summary.run_id,
                    verdict,
                    reason,
                    config.policy,
                    config.enabled,
                )
            return new_delivery(
                config,
                verdict,
                status="disabled" if not config.enabled else "suppressed",
            )

        message = build_message(summary, config)
        if message is None:
            log.warning(
                "Run %s finished %s but no recipients are routed for that "
                "verdict in config/alerting.yml",
                summary.run_id,
                verdict,
            )
            return new_delivery(config, verdict, status="suppressed")

        result = deliver_message(
            message,
            config,
            verdict,
            transport=transport,
            http_client=http_client,
            smtp_routes=smtp_routes,
        )
        log.info(
            "Run alert %s (verdict=%s, correlation=%s)",
            result.status,
            verdict,
            result.correlation_id,
        )
        return result
    except Exception as exc:
        log.error("Unexpected error while preparing run alert (%s)", type(exc).__name__)
        return new_delivery(config, status="failed", category="preparation")


def send_run_alert(run: Any, tracker: RunStageTracker, **kwargs: Any) -> bool:
    """Preserve the legacy boolean API; use send_run_alert_result for detail."""
    return send_run_alert_result(run, tracker, **kwargs).handled
