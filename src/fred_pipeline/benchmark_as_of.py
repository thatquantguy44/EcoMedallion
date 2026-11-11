"""Calendar cutoff for the US benchmark-rate board."""

from datetime import date, datetime
from zoneinfo import ZoneInfo


def benchmark_as_of_date(as_of: date | None = None) -> date:
    """Resolve one New York calendar date for the entire benchmark build."""
    today = datetime.now(ZoneInfo("America/New_York")).date()
    if as_of is None:
        return today
    if not isinstance(as_of, date) or isinstance(as_of, datetime):
        raise ValueError("benchmark as-of must be a calendar date")
    if as_of > today:
        raise ValueError("benchmark as-of cannot be in the future")
    return as_of


def select_benchmark_vintages(rows, as_of: date):
    """Select captured FRED revisions without inferring a missing vintage date.

    A blank realtime_start is retained only when the row's actual capture time
    was no later than the cutoff; it remains blank in the selected record.
    """
    from datetime import time, timedelta, timezone

    end = datetime.combine(
        as_of + timedelta(days=1), time.min, ZoneInfo("America/New_York")
    )
    capture_cutoff = min(end.astimezone(timezone.utc), datetime.now(timezone.utc))
    selected = {}
    for row in rows:
        if row.get("source") != "fred":
            continue
        try:
            observed = date.fromisoformat(str(row["observation_date"]))
            start_raw = row.get("realtime_start")
            start = date.fromisoformat(str(start_raw)) if start_raw else None
            if observed > as_of or (start and start > as_of):
                continue
            captured = datetime.fromisoformat(
                str(row["ingested_at"]).replace("Z", "+00:00")
            )
            if captured.tzinfo is None or captured.utcoffset() is None:
                raise ValueError("capture timestamp has no timezone")
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError(
                f"Invalid benchmark date or capture timestamp for {row.get('series_id')}"
            ) from exc
        if captured.astimezone(timezone.utc) >= capture_cutoff:
            continue
        key = (row["series_id"], observed)
        current = selected.get(key)
        if current is None or (start_raw or "") > (current.get("realtime_start") or ""):
            selected[key] = row
        elif (start_raw or "") == (current.get("realtime_start") or ""):
            raise ValueError(
                f"Ambiguous benchmark vintage for {row['series_id']} on {observed}"
            )
    return [selected[key] for key in sorted(selected)]
