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
) -> ECBPollReport:
    """Poll one authoritative SQL warehouse without taking ownership of it."""
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
        ECBEligibleAssetsPipeline,
        ECBMIDClient,
    )

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
    releases = tuple(
        row.to_dict() for row in factory(warehouse, client).poll(allow_gap=allow_gap)
    )
    return ECBPollReport(
        operation_id=operation_id,
        selected_backend=selected_backend,
        outcome="accepted"
        if any(row.get("status") == "accepted" for row in releases)
        else "no_new_release",
        releases=releases,
        started_at=started_at,
        finished_at=now().isoformat(),
    )
