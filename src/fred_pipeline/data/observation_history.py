"""Immutable observed-state helpers for normalized observations.

Compact Silver remains the current state used by legacy macro consumers. This
module produces the append-only event row used where capture-time lineage is
required (notably ECB FX and captured-as-of queries).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


HISTORY_COLUMNS = (
    "observation_event_id",
    "source",
    "series_id",
    "observation_date",
    "realtime_start",
    "realtime_end",
    "value",
    "raw_value",
    "exact_value",
    "is_missing",
    "row_hash",
    "revision_number",
    "provider_valid_from",
    "provider_valid_to",
    "base_currency",
    "quote_currency",
    "unit_multiplier",
    "observation_status",
    "observation_action",
    "is_deleted",
    "ingested_at",
    "run_id",
)


def observation_event(row: dict[str, Any]) -> dict[str, Any]:
    """Return a schema-complete, deterministically identified observed event."""
    event = {column: row.get(column) for column in HISTORY_COLUMNS}
    event["source"] = event["source"] or "fred"
    event["realtime_start"] = event["realtime_start"] or ""
    event["realtime_end"] = event["realtime_end"] or ""
    event["is_deleted"] = bool(event["is_deleted"])
    if not event["observation_event_id"]:
        identity = {
            "ingested_at": event["ingested_at"],
            "provider_valid_from": event["provider_valid_from"],
            "row_hash": event["row_hash"],
            "run_id": event["run_id"],
            "source": event["source"],
            "series_id": event["series_id"],
            "observation_date": event["observation_date"],
            "realtime_start": event["realtime_start"],
        }
        event["observation_event_id"] = hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    return event


def with_observation_event(row: dict[str, Any]) -> dict[str, Any]:
    """Add the immutable event identifier to the compact current-state row."""
    event = observation_event(row)
    result = dict(row)
    result["observation_event_id"] = event["observation_event_id"]
    return result
