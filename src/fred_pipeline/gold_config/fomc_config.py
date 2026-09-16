"""Config for the FOMC rate-probability engine (option A: no CME connector).

``config/fomc.yml`` declares the scheduled FOMC meeting decision dates, the
25bp outcome-bucket step, the anchor series for the current target
range/effective rate, and the short end of the Treasury curve used to
bootstrap the implied forward-rate path between meetings. See
:mod:`fred_pipeline.writer.terminal_views` (``compute_fomc_probability``)
and ``docs/handoffs/terminal_phase0_gaps.md`` item 3 for the full design.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import yaml

DEFAULT_FOMC_PATH = "config/fomc.yml"


class FOMCConfigError(ValueError):
    """Raised when the FOMC config file is malformed."""


@dataclass(frozen=True)
class FOMCTenorDef:
    """One point on the short end of the Treasury curve used for bootstrap."""

    series_id: str
    tenor_months: int

    def __post_init__(self) -> None:
        if not self.series_id:
            raise FOMCConfigError("FOMC tenor entry is missing series_id")
        if self.tenor_months <= 0:
            raise FOMCConfigError(
                f"FOMC tenor {self.series_id!r} must have tenor_months > 0"
            )


VERIFIED_BY_VALUES = ("manual", "scraper")


@dataclass(frozen=True)
class CalendarProvenance:
    """Where ``meeting_dates`` came from and how current it is.

    The file has always carried this in prose comments, which nothing can read.
    That left two very different states looking identical from inside the repo:
    "the Fed has not published further out yet" and "nobody has checked
    lately". ``published_through`` is what separates them -- it records the
    Fed's own horizon, not merely our last row.
    """

    source_url: str = ""
    last_verified: date | None = None
    verified_by: str = ""
    published_through: date | None = None

    def __post_init__(self) -> None:
        if self.verified_by and self.verified_by not in VERIFIED_BY_VALUES:
            raise FOMCConfigError(
                f"fomc.yml calendar_provenance.verified_by must be one of "
                f"{list(VERIFIED_BY_VALUES)}, got {self.verified_by!r}"
            )


@dataclass(frozen=True)
class FOMCConfig:
    meeting_dates: tuple[date, ...]
    bucket_step_bps: int
    target_low_series: str
    target_high_series: str
    effective_rate_series: str
    tenors: tuple[FOMCTenorDef, ...]
    # Optional: configs written before spec008 have no such block, and must
    # keep loading unchanged.
    calendar_provenance: CalendarProvenance | None = None

    def __post_init__(self) -> None:
        if not self.meeting_dates:
            raise FOMCConfigError("fomc.yml must declare at least one meeting_date")
        if list(self.meeting_dates) != sorted(self.meeting_dates):
            raise FOMCConfigError("fomc.yml meeting_dates must be ascending")
        if self.bucket_step_bps <= 0:
            raise FOMCConfigError("fomc.yml bucket_step_bps must be > 0")
        if len(self.tenors) < 2:
            raise FOMCConfigError(
                "fomc.yml must declare at least 2 tenors to bootstrap a forward rate"
            )
        months = [t.tenor_months for t in self.tenors]
        if months != sorted(months):
            raise FOMCConfigError("fomc.yml tenors must be ascending by tenor_months")


def _parse_fomc(raw: dict[str, Any], *, source: str) -> FOMCConfig:
    known = {
        "meeting_dates",
        "bucket_step_bps",
        "target_low_series",
        "target_high_series",
        "effective_rate_series",
        "tenors",
        "calendar_provenance",
    }
    unknown = set(raw) - known
    if unknown:
        raise FOMCConfigError(
            f"{source} has unknown top-level field(s): {sorted(unknown)}. "
            f"Allowed: {sorted(known)}"
        )

    raw_dates = raw.get("meeting_dates") or []
    try:
        meeting_dates = tuple(
            d if isinstance(d, date) else date.fromisoformat(str(d)) for d in raw_dates
        )
    except ValueError as exc:
        raise FOMCConfigError(f"{source} has an invalid meeting_date: {exc}") from exc

    raw_tenors = raw.get("tenors") or []
    if not isinstance(raw_tenors, list):
        raise FOMCConfigError(f"{source} 'tenors' must be a list")
    tenors = tuple(
        FOMCTenorDef(
            series_id=t.get("series_id", ""),
            tenor_months=int(t.get("tenor_months", 0)),
        )
        for t in raw_tenors
    )

    return FOMCConfig(
        meeting_dates=meeting_dates,
        bucket_step_bps=int(raw.get("bucket_step_bps", 25)),
        target_low_series=raw.get("target_low_series", ""),
        target_high_series=raw.get("target_high_series", ""),
        effective_rate_series=raw.get("effective_rate_series", ""),
        tenors=tenors,
        calendar_provenance=_parse_provenance(
            raw.get("calendar_provenance"), source=source
        ),
    )


def _parse_provenance(raw: Any, *, source: str) -> CalendarProvenance | None:
    """Parse the optional ``calendar_provenance`` block. Absent -> ``None``."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise FOMCConfigError(f"{source} 'calendar_provenance' must be a mapping")

    known = {"source_url", "last_verified", "verified_by", "published_through"}
    unknown = set(raw) - known
    if unknown:
        raise FOMCConfigError(
            f"{source} calendar_provenance has unknown field(s): "
            f"{sorted(unknown)}. Allowed: {sorted(known)}"
        )

    def _date(field: str) -> date | None:
        value = raw.get(field)
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError as exc:
            raise FOMCConfigError(
                f"{source} calendar_provenance.{field} is not a date: {exc}"
            ) from exc

    return CalendarProvenance(
        source_url=str(raw.get("source_url", "")),
        last_verified=_date("last_verified"),
        verified_by=str(raw.get("verified_by", "")),
        published_through=_date("published_through"),
    )


def load_fomc_config(path: Optional[str] = None) -> Optional[FOMCConfig]:
    """Load the FOMC config from YAML.

    Resolution: explicit ``path``, else ``FRED_FOMC_CONFIG_FILE`` env var,
    else ``config/fomc.yml``. A missing file returns ``None`` (the FOMC
    tables are then simply empty); a malformed file raises.
    """
    resolved = path or os.environ.get("FRED_FOMC_CONFIG_FILE") or DEFAULT_FOMC_PATH
    if not resolved or not os.path.isfile(resolved):
        return None
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise FOMCConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_fomc(data, source=resolved)
