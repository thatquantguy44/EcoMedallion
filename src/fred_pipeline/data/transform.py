"""Pure-Python transformation of raw FRED payloads into normalized rows.

Kept separate from the Spark writers so the trickiest logic — parsing FRED's
string values, handling the ``"."`` missing-value sentinel, computing revision
numbers, and deriving point-in-time keys — is fully unit-testable without a
SparkSession.

A "silver row" here is a plain ``dict`` with a stable schema. The Spark layer
(:mod:`fred_pipeline.silver`) simply wraps these into a DataFrame and MERGEs.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

# FRED encodes "no value for this date" as a literal single period.
MISSING_VALUE = "."

# Sentinel FRED uses for open-ended real-time windows.
REALTIME_OPEN_END = "9999-12-31"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_value(raw: Any) -> Optional[float]:
    """Parse a FRED observation value string into a float or ``None``.

    FRED returns values as strings, using ``"."`` for missing data. Anything
    that cannot be parsed becomes ``None`` (recorded as a DQ concern later)
    rather than raising, so one bad cell never fails an entire load.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if s == "" or s == MISSING_VALUE:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def parse_decimal_value(raw: Any) -> Optional[Decimal]:
    """Parse a provider number without losing its Decimal precision.

    ``raw_value`` remains the provider's lexical value. This helper supplies
    the separately stored exact numeric representation for sources, such as
    ECB FX, whose downstream arithmetic must not start from a binary float.
    """
    if raw is None:
        return None
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, bool):
        return None
    text = str(raw).strip()
    if text in ("", MISSING_VALUE):
        return None
    try:
        return Decimal(text.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _row_hash(
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: Any,
    *,
    semantics: dict[str, Any] | None = None,
) -> str:
    """Deterministic hash used to detect genuine changes across loads."""
    payload = f"{series_id}|{observation_date}|{realtime_start}|{value}"
    if semantics:
        payload += "|" + json.dumps(semantics, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    ingested_at: Optional[str] = None,
    run_id: Optional[str] = None,
    track_vintage: bool = True,
    source: str = "fred",
) -> list[dict[str, Any]]:
    """Convert a raw FRED observations payload into normalized silver rows.

    Parameters
    ----------
    series_id:
        The series the payload belongs to (FRED does not echo it per-row).
    payload:
        The exact JSON dict returned by ``series/observations``.
    ingested_at / run_id:
        Audit stamps threaded through from the pipeline run.
    track_vintage:
        When ``False`` (a non-vintage series), ``realtime_start`` / ``realtime_end``
        are blanked. Without realtime params FRED stamps every row with
        ``realtime_start = today``; keeping that in the MERGE key would insert a
        fresh row on every run. Blanking it collapses the key to
        ``(series_id, observation_date)`` so re-runs update in place. ``row_hash``
        still reflects the value, so genuine changes are detected.

    Returns
    -------
    A list of dicts with a stable schema (see :data:`SILVER_COLUMNS`).
    """
    ingested_at = ingested_at or _utc_now_iso()
    observations = payload.get("observations")
    if observations is None:
        raise ValueError(
            f"Payload for {series_id!r} has no 'observations' key "
            f"(keys: {sorted(payload.keys())})"
        )

    rows: list[dict[str, Any]] = []
    for obs in observations:
        obs_date = obs.get("date")
        if not obs_date:
            continue  # skip structurally broken rows
        if track_vintage:
            rt_start = obs.get("realtime_start", "")
            rt_end = obs.get("realtime_end", "")
        else:
            # Non-vintage: don't let FRED's per-request realtime dates create
            # duplicate rows across runs. Vintage history is intentionally
            # not tracked for this series.
            rt_start = ""
            rt_end = ""
        raw_value = obs.get("value")
        value = parse_value(raw_value)

        rows.append(
            {
                "source": source,
                "series_id": series_id,
                "observation_date": obs_date,
                "realtime_start": rt_start,
                "realtime_end": rt_end,
                "value": value,
                "raw_value": None if raw_value is None else str(raw_value),
                "is_missing": value is None,
                "row_hash": _row_hash(series_id, obs_date, rt_start, raw_value),
                "ingested_at": ingested_at,
                "run_id": run_id,
            }
        )
    return rows


# Canonical silver column order (also documented in docs/data_dictionary.md).
SILVER_COLUMNS = (
    "source",
    "series_id",
    "observation_date",
    "realtime_start",
    "realtime_end",
    "value",
    "raw_value",
    "is_missing",
    "row_hash",
    "ingested_at",
    "run_id",
)


def assign_revision_numbers(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add a monotonically increasing ``revision_number`` per observation_date.

    For vintage-enabled series, FRED returns multiple rows per
    ``observation_date`` (one per real-time window). Ordering by
    ``realtime_start`` gives revision 1, 2, 3, ... — the point-in-time history
    of how a given data point was revised over time.
    """
    rows = list(rows)
    by_date: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        by_date.setdefault((r["series_id"], r["observation_date"]), []).append(r)

    for group in by_date.values():
        group.sort(key=lambda r: r.get("realtime_start") or "")
        for i, r in enumerate(group, start=1):
            r["revision_number"] = i
    return rows


def latest_by_observation(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse vintage rows to the single latest revision per observation_date.

    This is the transformation behind ``gold.fred_latest_observation`` /
    the ``latest_revised`` view.
    """
    rows = list(rows)
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r["series_id"], r["observation_date"])
        cur = latest.get(key)
        if cur is None or (r.get("realtime_start") or "") >= (
            cur.get("realtime_start") or ""
        ):
            latest[key] = r
    return sorted(
        latest.values(), key=lambda r: (r["series_id"], r["observation_date"])
    )


def daily_feature_matrix(latest_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build a daily, forward-filled feature matrix (pure-Python parity of Gold).

    Given *latest-revision* rows (one per series/observation_date), produce one
    row per (calendar_day, series) between the global min and max observation
    date, forward-filling each series' most recent value. This mirrors
    ``gold.fred_macro_feature_daily`` for the local/SQLite backend where Spark's
    ``sequence``/``last_value(...) ignore nulls`` are unavailable.
    """
    from datetime import timedelta

    rows = [r for r in latest_rows if not r.get("is_missing", False)]
    parsed: list[tuple[str, date, Optional[float]]] = []
    for r in rows:
        try:
            d = datetime.strptime(str(r["observation_date"])[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError, KeyError):
            continue
        parsed.append((r["series_id"], d, r.get("value")))
    if not parsed:
        return []

    max_d = max(p[1] for p in parsed)
    series_ids = sorted({p[0] for p in parsed})

    # Per-series first observation: start each series' daily calendar at its
    # own first obs, not the global min. A global min cross-joined a
    # centuries-long calendar onto every series (one 1600s-era series forces
    # ~300y of daily rows), producing hundreds of millions of all-NULL leading
    # rows. Bounding per series is output-identical for every non-leading row.
    series_start: dict[str, date] = {}
    for s, d, _v in parsed:
        if s not in series_start or d < series_start[s]:
            series_start[s] = d

    # native[(series, date)] = value on that series' actual release date
    native: dict[tuple[str, date], Optional[float]] = {(s, d): v for s, d, v in parsed}

    out: list[dict[str, Any]] = []
    for sid in series_ids:
        last_val: Optional[float] = None
        cur = series_start[sid]
        while cur <= max_d:
            raw = native.get((sid, cur), None)
            if (sid, cur) in native:
                last_val = raw
            out.append(
                {
                    "as_of_date": cur.isoformat(),
                    "series_id": sid,
                    "raw_value": raw,
                    "value": last_val,
                }
            )
            cur += timedelta(days=1)
    return out


def payload_summary(payload: Any) -> dict[str, Any]:
    """Extract lightweight metadata about a payload for Bronze/audit rows.

    Tolerant of non-dict payloads (e.g. World Bank returns a top-level JSON
    array): only ``payload_bytes`` is meaningful then, and the pipeline supplies
    an accurate ``observation_count`` from the normalized rows.
    """
    d = payload if isinstance(payload, dict) else {}
    obs = d.get("observations") or []
    return {
        "observation_count": len(obs),
        "response_realtime_start": d.get("realtime_start"),
        "response_realtime_end": d.get("realtime_end"),
        "response_count": d.get("count"),
        "payload_bytes": len(json.dumps(payload)),
    }
