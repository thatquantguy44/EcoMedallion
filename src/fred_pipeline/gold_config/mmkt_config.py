"""Config loader for the money-market instrument map (specs/spec009).

``config/mmkt_instruments.yml`` → ``gold.mmkt_rate_snapshot``: which series
carries each instrument's rate, and the quoting basis that series uses. The
basis is the whole reason this is its own config rather than another row of
``config/benchmark_rates.yml``: bills and commercial paper are quoted on a
discount basis, constant-maturity yields on a bond-equivalent basis, and
overnight/repo rates on a simple ACT/360 basis, so a raw comparison ranks by
quoting convention. See ``fred_pipeline.writer.mmkt_views`` for the conversions.

A missing file loads as an empty config (the snapshot is then empty -- nothing
has been configured); a malformed file raises. Same convention as
``rates_complex_config``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fred_pipeline.gold_config.rates_complex_config import _entries, _load_yaml

DEFAULT_MMKT_PATH = "config/mmkt_instruments.yml"

VALID_BASES = ("money_market", "discount", "bond_equivalent")
VALID_CLASSES = ("overnight", "policy", "repo", "rrp", "tbill", "treasury_cmt", "cp")

# Simple-yield conversion from an ACT/365 investment basis to ACT/360 only holds
# while the instrument is inside its first coupon period (<= half a year); a
# longer constant maturity needs the compounded formula, which this file does
# not implement and so refuses rather than approximates.
MAX_BOND_EQUIVALENT_TENOR_DAYS = 182


class MmktConfigError(ValueError):
    """Raised when ``config/mmkt_instruments.yml`` is malformed."""


@dataclass(frozen=True)
class InstrumentDef:
    instrument_id: str
    label: str
    instrument_class: str
    series_id: str
    tenor_days: int
    basis: str
    max_staleness_days: int

    def __post_init__(self) -> None:
        if not (self.instrument_id and self.label and self.series_id):
            raise MmktConfigError(f"Instrument missing required field(s): {self}")
        if self.instrument_class not in VALID_CLASSES:
            raise MmktConfigError(
                f"Instrument {self.instrument_id!r} has invalid instrument_class "
                f"{self.instrument_class!r}; expected one of {list(VALID_CLASSES)}"
            )
        if self.basis not in VALID_BASES:
            raise MmktConfigError(
                f"Instrument {self.instrument_id!r} has invalid basis {self.basis!r}; "
                f"expected one of {list(VALID_BASES)}"
            )
        if self.tenor_days < 1:
            raise MmktConfigError(
                f"Instrument {self.instrument_id!r} tenor_days must be >= 1, got {self.tenor_days}"
            )
        if self.basis == "bond_equivalent" and self.tenor_days > MAX_BOND_EQUIVALENT_TENOR_DAYS:
            raise MmktConfigError(
                f"Instrument {self.instrument_id!r}: bond_equivalent conversion is only "
                f"defined for tenor_days <= {MAX_BOND_EQUIVALENT_TENOR_DAYS} (got "
                f"{self.tenor_days}); a longer maturity needs the compounded formula"
            )
        if self.max_staleness_days < 1:
            raise MmktConfigError(
                f"Instrument {self.instrument_id!r} max_staleness_days must be >= 1"
            )


@dataclass(frozen=True)
class MmktConfig:
    instruments: tuple[InstrumentDef, ...] = ()


def load_mmkt_config(path: Optional[str] = None) -> MmktConfig:
    data, resolved = _load_yaml(path, "FRED_MMKT_INSTRUMENTS_FILE", DEFAULT_MMKT_PATH)
    if data is None:
        return MmktConfig()
    default_staleness = int(data.get("default_max_staleness_days", 5))
    known = {
        "instrument_id", "label", "instrument_class", "series_id",
        "tenor_days", "basis", "max_staleness_days",
    }
    instruments: list[InstrumentDef] = []
    seen: set[str] = set()
    for item in _entries(data.get("instruments"), known, resolved, "instruments"):
        try:
            tenor_days = int(item.get("tenor_days", 0))
            staleness = int(item.get("max_staleness_days", default_staleness))
        except (TypeError, ValueError) as exc:
            raise MmktConfigError(f"Non-integer tenor/staleness in {resolved}: {item!r}") from exc
        inst = InstrumentDef(
            instrument_id=item.get("instrument_id", ""),
            label=item.get("label", ""),
            instrument_class=item.get("instrument_class", ""),
            series_id=item.get("series_id", ""),
            tenor_days=tenor_days,
            basis=item.get("basis", ""),
            max_staleness_days=staleness,
        )
        if inst.instrument_id in seen:
            raise MmktConfigError(f"Duplicate instrument {inst.instrument_id!r} in {resolved}")
        seen.add(inst.instrument_id)
        instruments.append(inst)
    return MmktConfig(instruments=tuple(instruments))
