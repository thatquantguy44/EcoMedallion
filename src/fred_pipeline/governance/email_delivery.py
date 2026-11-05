"""Structured, nonfatal outcomes for a single alert delivery attempt."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, replace
from typing import Any

from fred_pipeline.governance.email_transport import (
    ConsoleEmailTransport,
    EmailMessageSpec,
    EmailSendError,
    FileEmailTransport,
    build_transport,
)

log = logging.getLogger("fred_pipeline.alerting")


@dataclass(frozen=True)
class DeliveryResult:
    status: str
    profile_id: str
    correlation_id: str
    error_category: str = ""

    @property
    def handled(self) -> bool:
        """Preserve the legacy boolean contract for accepted and previewed alerts."""
        return self.status in {"accepted", "previewed"}


def new_delivery(
    config: Any, verdict: str = "", *, status: str = "suppressed", category: str = ""
) -> DeliveryResult:
    profile = config.route_for(verdict).profile or config.default_profile or "legacy"
    return DeliveryResult(status, profile, uuid.uuid4().hex, category)


def deliver_message(
    message: EmailMessageSpec,
    config: Any,
    verdict: str,
    *,
    transport: Any = None,
    http_client: Any = None,
    smtp_routes: Any = None,
    result: DeliveryResult | None = None,
) -> DeliveryResult:
    result = result or new_delivery(config, verdict)
    try:
        sender = transport or build_transport(
            config, verdict=verdict, http_client=http_client, smtp_routes=smtp_routes
        )
        sender.send(replace(message, correlation_id=result.correlation_id))
        status = (
            "previewed"
            if isinstance(sender, (ConsoleEmailTransport, FileEmailTransport))
            else "accepted"
        )
        return replace(result, status=status)
    except EmailSendError as exc:
        log.warning(
            "Alert delivery %s (%s; correlation %s)",
            "unknown" if exc.uncertain else "failed",
            exc.category,
            result.correlation_id,
        )
        return replace(
            result,
            status="unknown" if exc.uncertain else "failed",
            error_category=exc.category,
        )
    except Exception as exc:
        log.error(
            "Alert delivery outcome unknown (%s; correlation %s)",
            type(exc).__name__,
            result.correlation_id,
        )
        return replace(result, status="unknown", error_category="unexpected")
