"""Collect and plan outbound HTTP destinations for configured email alerts."""

from __future__ import annotations

import logging
import os
from typing import Any

from fred_pipeline.governance.alerting_config import load_alerting_config
from fred_pipeline.network import (
    NetworkPreflightError,
    RunSessions,
    _origin,
    select_route,
)

log = logging.getLogger("fred_pipeline.alerting")

IDENTITY_URL = "https://login.microsoftonline.com/"
GRAPH_URL = "https://graph.microsoft.com/v1.0/"


def _alert_transports(environment: str) -> list[Any]:
    try:
        config = load_alerting_config()
    except Exception:
        log.warning("Alert configuration could not be loaded for networking")
        return []
    if (
        not config.enabled
        or config.policy == "never"
        or (config.environments and environment not in config.environments)
    ):
        return []
    transports = []
    for verdict in ("failure", "partial", "success"):
        if not config.should_send(verdict, environment):
            continue
        transport = config.profile_for(verdict).transport
        if transport not in transports:
            transports.append(transport)
    return transports


def alert_http_targets(environment: str) -> list[tuple[str, str]]:
    targets: dict[str, str] = {}
    for transport in _alert_transports(environment):
        if transport.kind == "microsoft_graph":
            targets[IDENTITY_URL] = "Microsoft identity"
            targets[GRAPH_URL] = "Microsoft Graph"
        elif (
            transport.kind in ("smtp", "outlook_smtp", "outlook_com_smtp")
            and transport.smtp_auth == "oauth2"
        ):
            targets[IDENTITY_URL] = "Microsoft identity"
    return list(targets.items())


def alert_smtp_targets(environment: str) -> list[Any]:
    return [
        transport
        for transport in _alert_transports(environment)
        if transport.kind in ("smtp", "outlook_smtp", "outlook_com_smtp")
        and transport.network is not None
    ]


def plan_alert_smtp(environment: str) -> dict[Any, Any]:
    from fred_pipeline.governance.email_transport import EmailSendError
    from fred_pipeline.governance.smtp_network import plan_smtp_route

    routes = {}
    for transport in alert_smtp_targets(environment):
        try:
            routes[transport] = plan_smtp_route(transport)
        except EmailSendError:
            log.warning("SMTP alert preflight failed")
            routes[transport] = None
    return routes


def plan_alert_http(settings: Any, environment: str) -> RunSessions:
    """Build an advisory plan; absent origins stay unavailable, never unmanaged."""
    routes = {}
    environment_snapshot = dict(os.environ)
    for url, label in alert_http_targets(environment):
        try:
            route = select_route(
                settings, url, label=label, environ=environment_snapshot
            )
            routes[_origin(url)] = route
        except NetworkPreflightError:
            log.warning("%s alert preflight failed", label)
    return RunSessions(routes)


def plan_profile_http(settings: Any, transport: Any) -> RunSessions:
    targets = []
    if transport.kind == "microsoft_graph":
        targets = [(IDENTITY_URL, "Microsoft identity"), (GRAPH_URL, "Microsoft Graph")]
    elif (
        transport.kind in ("smtp", "outlook_smtp", "outlook_com_smtp")
        and transport.smtp_auth == "oauth2"
    ):
        targets = [(IDENTITY_URL, "Microsoft identity")]
    routes = {}
    environment_snapshot = dict(os.environ)
    for url, label in targets:
        route = select_route(settings, url, label=label, environ=environment_snapshot)
        routes[_origin(url)] = route
    return RunSessions(routes)
