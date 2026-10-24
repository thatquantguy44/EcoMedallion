"""Repeatable file and live delivery drills for ECB eligible-assets alerts.

The drill exercises assessment, routing, rendering, and the file transport for
the four operational failure modes required by spec011. It never opens a
warehouse or calls the ECB. Network delivery is only enabled by the explicit
``live_delivery`` option and a fully enabled non-console transport.
"""

from __future__ import annotations

import json
from hashlib import sha256
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fred_pipeline.governance.alerting_config import (
    AlertingConfig,
    RouteConfig,
    TransportConfig,
)
from fred_pipeline.governance.ecb_alerting import (
    assess_ecb_assets,
    send_ecb_assets_alert,
)
from fred_pipeline.governance.email_transport import (
    EmailTransport,
    FileEmailTransport,
    build_transport,
)


_FIXTURE_NOW = datetime(2026, 10, 23, 21, 30, tzinfo=timezone.utc)
_CURRENT_STATUS: dict[str, Any] = {
    "latest_accepted": {
        "release_id": "drill-current-release",
        "publication_at_utc": "2026-10-23T15:56:07+00:00",
        "coverage_status": "complete",
        "reconciliation_status": "matched",
    },
    "current_eligible_count": 31_331,
    "quarantined": [],
}


def _drill_config(config: AlertingConfig, output_dir: Path) -> AlertingConfig:
    """Enable only file delivery while retaining the configured routes."""
    routes = dict(config.routes)
    for verdict in ("partial", "failure"):
        if not config.route_for(verdict).has_recipients:
            routes[verdict] = RouteConfig(to=("ecb-alert-drill@example.invalid",))
    return replace(
        config,
        enabled=True,
        policy="failure",
        from_address=config.from_address or "ecb-alert-drill@example.invalid",
        subject_prefix=f"{config.subject_prefix}[DRILL]",
        environments=(),
        transport=TransportConfig(kind="file", output_dir=str(output_dir)),
        routes=routes,
        ecb_assets=replace(config.ecb_assets, enabled=True),
    )


def _live_drill_config(config: AlertingConfig, *, environment: str) -> AlertingConfig:
    """Validate that the configured transport can prove external delivery."""
    if not config.enabled:
        raise ValueError("live alert drill requires alerting.enabled: true")
    if not config.ecb_assets.enabled:
        raise ValueError("live alert drill requires ecb_assets.enabled: true")
    if config.transport.kind in {"console", "file"}:
        raise ValueError(
            "live alert drill requires smtp, outlook_smtp, or microsoft_graph; "
            f"configured transport is {config.transport.kind!r}"
        )
    if config.policy == "never":
        raise ValueError("live alert drill cannot run with policy: never")
    for verdict in ("partial", "failure"):
        if not config.should_send(verdict, environment):
            raise ValueError(
                f"live alert drill has no enabled {verdict} route for "
                f"environment {environment!r}"
            )
    return replace(config, subject_prefix=f"{config.subject_prefix}[DRILL]")


def run_ecb_alert_drill(
    config: AlertingConfig,
    *,
    output_dir: str | Path,
    live_delivery: bool = False,
    environment: str = "prod",
    transport: EmailTransport | None = None,
) -> dict[str, Any]:
    """Deliver four synthetic alerts and return an evidence manifest.

    A unique subdirectory makes every execution independently auditable and
    avoids overwriting a prior drill.  Raising on a missing or misclassified
    alert makes the command suitable for deployment acceptance checks. File
    delivery is the safe default; live delivery requires an explicit flag and
    enabled SMTP or Microsoft Graph configuration.
    """
    root = Path(output_dir)
    run_at = datetime.now(timezone.utc)
    run_dir = root / run_at.strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True, exist_ok=False)
    if live_delivery:
        drill_config = _live_drill_config(config, environment=environment)
        delivery_transport = transport or build_transport(drill_config)
        delivery_environment = environment
    else:
        drill_config = _drill_config(config, run_dir)
        delivery_transport = transport or FileEmailTransport(drill_config.transport)
        delivery_environment = "drill"

    scenarios: list[dict[str, Any]] = [
        {
            "name": "late_release",
            "expected_verdict": "partial",
            "action": "poll",
            "status": {
                **_CURRENT_STATUS,
                "latest_accepted": {
                    **_CURRENT_STATUS["latest_accepted"],
                    "release_id": "drill-stale-release",
                    "publication_at_utc": "2026-10-22T15:56:07+00:00",
                },
            },
        },
        {
            "name": "quarantined_release",
            "expected_verdict": "failure",
            "action": "poll",
            "status": {
                **_CURRENT_STATUS,
                "quarantined": [
                    {
                        "release_id": "drill-quarantined-release",
                        "publication_at_utc": "2026-10-23T16:00:00+00:00",
                        "error_detail": "synthetic quarantine drill",
                    }
                ],
            },
        },
        {
            "name": "signature_failure",
            "expected_verdict": "failure",
            "action": "verify",
            "results": [
                {
                    "release_id": "drill-invalid-signature",
                    "valid": False,
                    "status": "quarantined",
                }
            ],
            "status": _CURRENT_STATUS,
        },
        {
            "name": "ingestion_failure",
            "expected_verdict": "failure",
            "action": "poll",
            "error": "synthetic ECB MID ingestion failure",
        },
    ]

    evidence: list[dict[str, Any]] = []
    for scenario in scenarios:
        call = {
            key: scenario[key]
            for key in ("action", "results", "status", "error")
            if key in scenario
        }
        assessment = assess_ecb_assets(
            **call,
            now=_FIXTURE_NOW,
            settings=drill_config.ecb_assets,
        )
        expected = scenario["expected_verdict"]
        if assessment.verdict != expected:
            raise RuntimeError(
                f"{scenario['name']} classified {assessment.verdict!r}; "
                f"expected {expected!r}"
            )
        before = {path.name for path in run_dir.iterdir()}
        sent = send_ecb_assets_alert(
            **call,
            environment=delivery_environment,
            config=drill_config,
            transport=delivery_transport,
            now=_FIXTURE_NOW,
        )
        after = {path.name for path in run_dir.iterdir()}
        files = sorted(after - before)
        if not sent:
            raise RuntimeError(
                f"{scenario['name']} was not accepted by the alert transport"
            )
        if not live_delivery:
            if not any(name.endswith(".html") for name in files):
                raise RuntimeError(f"{scenario['name']} did not produce an HTML alert")
            if not any(name.endswith(".txt") for name in files):
                raise RuntimeError(f"{scenario['name']} did not produce a text alert")
        route = drill_config.route_for(assessment.verdict)
        artifact_hashes = {
            name: sha256((run_dir / name).read_bytes()).hexdigest() for name in files
        }
        evidence.append(
            {
                "scenario": scenario["name"],
                "verdict": assessment.verdict,
                "reason": assessment.reason,
                "delivered": True,
                "transport_accepted": True,
                "recipients": list(route.to),
                "cc": list(route.cc),
                "bcc": list(route.bcc),
                "files": files,
                "sha256": artifact_hashes,
            }
        )

    manifest: dict[str, Any] = {
        "schema_version": 1,
        "run_at_utc": run_at.isoformat(),
        "fixture_clock_utc": _FIXTURE_NOW.isoformat(),
        "mode": "live" if live_delivery else "file",
        "non_mutating": True,
        "warehouse_opened": False,
        "network_used": live_delivery,
        "output_directory": str(run_dir),
        "configured_transport_kind": config.transport.kind,
        "configured_alerting_enabled": config.enabled,
        "delivery_environment": delivery_environment,
        "scenarios": evidence,
        "passed": len(evidence) == 4 and all(row["delivered"] for row in evidence),
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    manifest["manifest"] = str(manifest_path)
    return manifest
