"""Config-driven cross-series features for ``gold.fred_cross_series_feature``.

Where ``config/spreads.yml`` computes two-leg spreads/ratios on series that
already share a date grid, this adds **frequency-aware, N-leg** features so the
expanded, multi-source universe can be combined: a daily debt series ÷ a
quarterly GDP series, a weighted composite index across monthly indicators, etc.

Each feature declares a target ``frequency``; every leg is aligned to that grid
**as-of** (the last observation within each period — downsampling a finer leg to
a coarser target), then combined:

* ``spread``    — ``leg0 - leg1``      (exactly 2 legs)
* ``ratio``     — ``leg0 / leg1``      (exactly 2 legs; short leg ≠ 0)
* ``composite`` — ``Σ weightᵢ · legᵢ`` (1+ legs, weights default 1.0)
* ``four_quarter_change`` — quarterly leg minus the same quarter one year ago

Defined in a reviewable YAML file so features are added without code changes —
the same pattern as manifests and ``spreads.yml``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

DEFAULT_CROSS_SERIES_PATH = "config/cross_series.yml"

VALID_OPS = {"spread", "ratio", "composite", "four_quarter_change"}

# Target frequencies for alignment, normalized to short codes.
_FREQ_ALIASES = {
    "d": "d", "daily": "d",
    "w": "w", "weekly": "w",
    "m": "m", "monthly": "m",
    "q": "q", "quarterly": "q",
    "a": "a", "annual": "a",
}


class CrossSeriesConfigError(ValueError):
    """Raised when a cross-series config file is missing/malformed."""


@dataclass(frozen=True)
class CrossSeriesDef:
    """One cross-series feature. ``legs`` is a tuple of ``(series_id, weight)``."""

    name: str
    op: str
    frequency: str
    legs: tuple[tuple[str, float], ...]
    description: str = ""

    def __post_init__(self) -> None:
        if not self.name or not self.op or not self.legs:
            raise CrossSeriesConfigError(
                f"Cross-series definition missing required field(s): {self}"
            )
        if self.op not in VALID_OPS:
            raise CrossSeriesConfigError(
                f"Feature {self.name!r} has invalid op {self.op!r}; "
                f"expected one of {sorted(VALID_OPS)}"
            )
        if self.frequency not in _FREQ_ALIASES.values():
            raise CrossSeriesConfigError(
                f"Feature {self.name!r} has invalid frequency {self.frequency!r}; "
                f"expected one of {sorted(set(_FREQ_ALIASES.values()))}"
            )
        if self.op in ("spread", "ratio") and len(self.legs) != 2:
            raise CrossSeriesConfigError(
                f"Feature {self.name!r} op {self.op!r} needs exactly 2 legs, "
                f"got {len(self.legs)}"
            )
        if self.op == "four_quarter_change" and (
            self.frequency != "q" or len(self.legs) != 1
        ):
            raise CrossSeriesConfigError(
                f"Feature {self.name!r} four_quarter_change needs one quarterly leg"
            )


def _parse_legs(raw: Any, *, name: str) -> tuple[tuple[str, float], ...]:
    if not isinstance(raw, list) or not raw:
        raise CrossSeriesConfigError(f"Feature {name!r} must have a non-empty 'legs' list")
    legs: list[tuple[str, float]] = []
    for leg in raw:
        if isinstance(leg, str):
            legs.append((leg, 1.0))
        elif isinstance(leg, dict):
            sid = leg.get("series_id")
            if not sid:
                raise CrossSeriesConfigError(
                    f"Feature {name!r} has a leg missing 'series_id': {leg!r}"
                )
            legs.append((str(sid), float(leg.get("weight", 1.0))))
        else:
            raise CrossSeriesConfigError(
                f"Feature {name!r} leg must be a string or mapping, got {leg!r}"
            )
    return tuple(legs)


def _parse_defs(raw: Any, *, source: str) -> list[CrossSeriesDef]:
    if not isinstance(raw, list):
        raise CrossSeriesConfigError(f"{source} must contain a top-level 'features' list")
    defs: list[CrossSeriesDef] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise CrossSeriesConfigError(
                f"Each feature entry in {source} must be a mapping, got {item!r}"
            )
        known = {"name", "op", "frequency", "legs", "description"}
        unknown = set(item) - known
        if unknown:
            raise CrossSeriesConfigError(
                f"Feature {item.get('name')!r} in {source} has unknown field(s): "
                f"{sorted(unknown)}. Allowed: {sorted(known)}"
            )
        name = item.get("name", "")
        freq = _FREQ_ALIASES.get(str(item.get("frequency", "")).strip().lower())
        if freq is None:
            raise CrossSeriesConfigError(
                f"Feature {name!r} in {source} has invalid/missing frequency "
                f"{item.get('frequency')!r}"
            )
        sd = CrossSeriesDef(
            name=name,
            op=str(item.get("op", "")).strip().lower(),
            frequency=freq,
            legs=_parse_legs(item.get("legs"), name=name),
            description=item.get("description", ""),
        )
        if sd.name in seen:
            raise CrossSeriesConfigError(f"Duplicate feature name {sd.name!r} in {source}")
        seen.add(sd.name)
        defs.append(sd)
    return defs


def load_cross_series_defs(path: Optional[str] = None) -> list[CrossSeriesDef]:
    """Load cross-series feature definitions from YAML.

    Resolution: explicit ``path`` → ``FRED_CROSS_SERIES_FILE`` env → default
    ``config/cross_series.yml``. A missing file yields **no features** (this is a
    purely additive Gold table); a *malformed* file raises.
    """
    resolved = path or os.environ.get("FRED_CROSS_SERIES_FILE") or DEFAULT_CROSS_SERIES_PATH
    if not resolved or not os.path.isfile(resolved):
        return []
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise CrossSeriesConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_defs(data.get("features") or [], source=resolved)
