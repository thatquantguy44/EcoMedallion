"""``gold.mmkt_rate_snapshot``: the money-market rate snapshot (specs/spec009).

One row per configured instrument (``config/mmkt_instruments.yml``), carrying
the latest rate **as quoted**, the same rate **normalized to one basis**, and
the provenance a consumer needs to display it honestly (source series, the
observation date, staleness, and a status).

Two design rules, both learned the hard way in this repo and the terminal:

* **One basis.** FRED's ``DTB*`` bill series and the Fed's commercial-paper
  series are quoted on a *discount* basis; ``DGS*`` constant-maturity yields on a
  *bond-equivalent* (investment) basis; SOFR/EFFR/repo on a simple ACT/360
  money-market basis. Comparing them raw ranks instruments by a quoting
  convention. Every row therefore carries ``mm_yield_pct``: the simple ACT/360
  money-market yield, the basis the overnight benchmarks are already on.

* **No silent omission.** ``compute_benchmark_rate_board`` skips a rate whose
  series isn't ingested, which is how a mistyped id once emptied a whole Gold
  table without an error. A consumer of *this* table is an optimizer: a missing
  or stale rate must reach it as ``status = "missing" | "stale"`` with a reason,
  so the terminal can show ``ERR`` (QuantMeridian's Gold policy: no silent
  fallback) instead of pricing off a hole.

Pure Python, no I/O: takes the same silver-row dicts the other terminal views
take. Point-in-time safe: pass ``as_of`` and only observations on or before it
are read. Market rates are not revised (``vintage_enabled: false`` in the
manifests), so vintages are not consulted.

Conversions (``tenor`` in days, rates as fractions inside, percent outside)::

    discount         y = d / (1 - d * tenor / 360)
    bond_equivalent  y = y_bey * 360 / 365            (tenor <= 182 days)
    money_market     y = y

They agree: a bill quoted at discount ``d`` has bond-equivalent yield
``365 d / (360 - d tenor)``, and ``y_bey * 360/365`` is exactly the discount
formula above -- ``tests/test_mmkt_rate_snapshot.py`` checks that identity.
The ACT/365 versus actual/actual difference in a leap year is ignored (it is
well under a basis point at these tenors) and is disclosed in ``conversion``.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from fred_pipeline.gold_config.mmkt_config import (
    MAX_BOND_EQUIVALENT_TENOR_DAYS,
    InstrumentDef,
    MmktConfig,
    load_mmkt_config,
)
from fred_pipeline.writer.features import _group_sorted

STATUS_OK = "ok"
STATUS_STALE = "stale"
STATUS_MISSING = "missing"

NORMALIZED_BASIS = "money_market_act360"


class MmktConversionError(ValueError):
    """Raised when a quoted rate cannot be converted (nonsensical discount rate,
    or a basis/tenor combination the formula does not cover)."""


def to_money_market_yield_pct(value_pct: float, basis: str, tenor_days: int) -> float:
    """Convert a quoted rate (percent) to the simple ACT/360 money-market yield
    (percent). See the module docstring for the formulas."""
    if basis == "money_market":
        return value_pct
    if basis == "discount":
        d = value_pct / 100.0
        denominator = 1.0 - d * tenor_days / 360.0
        if denominator <= 0.0:
            raise MmktConversionError(
                f"discount rate {value_pct}% over {tenor_days} days implies a non-positive "
                "price; refusing to convert"
            )
        return 100.0 * d / denominator
    if basis == "bond_equivalent":
        if tenor_days > MAX_BOND_EQUIVALENT_TENOR_DAYS:
            raise MmktConversionError(
                f"bond_equivalent conversion is only defined for tenor <= "
                f"{MAX_BOND_EQUIVALENT_TENOR_DAYS} days (got {tenor_days})"
            )
        return value_pct * 360.0 / 365.0
    raise MmktConversionError(f"unknown basis {basis!r}")


def _conversion_note(inst: InstrumentDef) -> str:
    if inst.basis == "money_market":
        return "none: already simple ACT/360"
    if inst.basis == "discount":
        return f"discount to money-market: d / (1 - d * {inst.tenor_days} / 360)"
    return "bond-equivalent (ACT/365) to money-market: y * 360 / 365; leap-year effect ignored"


def _row(
    inst: InstrumentDef,
    *,
    status: str,
    detail: str | None,
    snapshot_date: date | None,
    latest: tuple[date, float] | None,
    mm_yield_pct: float | None,
) -> dict[str, Any]:
    return {
        "instrument_id": inst.instrument_id,
        "label": inst.label,
        "instrument_class": inst.instrument_class,
        "tenor_days": inst.tenor_days,
        "series_id": inst.series_id,
        "quoted_basis": inst.basis,
        "quoted_rate_pct": latest[1] if latest else None,
        "normalized_basis": NORMALIZED_BASIS,
        "mm_yield_pct": mm_yield_pct,
        "conversion": _conversion_note(inst),
        "observation_date": latest[0].isoformat() if latest else None,
        "as_of_date": snapshot_date.isoformat() if snapshot_date else None,
        "staleness_days": (snapshot_date - latest[0]).days if latest and snapshot_date else None,
        "max_staleness_days": inst.max_staleness_days,
        "status": status,
        "status_detail": detail,
    }


def compute_mmkt_rate_snapshot(
    silver_rows: Iterable[dict[str, Any]],
    config: MmktConfig | None = None,
    *,
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    """``gold.mmkt_rate_snapshot``: one row for **every** configured instrument.

    ``as_of`` defaults to the latest observation date across the configured
    series. Passing it makes the snapshot reproducible and point-in-time safe:
    observations after ``as_of`` are ignored. An empty config yields no rows
    (nothing configured); a configured instrument is never omitted.
    """
    if config is None:
        config = load_mmkt_config()
    if not config.instruments:
        return []

    wanted = {inst.series_id for inst in config.instruments}
    by_series = _group_sorted(r for r in silver_rows if r.get("series_id") in wanted)
    if as_of is not None:
        by_series = {
            sid: [(d, v) for d, v in series if d <= as_of] for sid, series in by_series.items()
        }
        by_series = {sid: series for sid, series in by_series.items() if series}

    snapshot_date = as_of or max(
        (series[-1][0] for series in by_series.values()), default=None
    )

    out: list[dict[str, Any]] = []
    for inst in config.instruments:
        series = by_series.get(inst.series_id)
        if not series:
            out.append(
                _row(
                    inst,
                    status=STATUS_MISSING,
                    detail=(
                        f"no observation for series {inst.series_id!r}"
                        + (f" on or before {as_of.isoformat()}" if as_of else "")
                        + " -- the series is not ingested, inactive, or empty"
                    ),
                    snapshot_date=snapshot_date,
                    latest=None,
                    mm_yield_pct=None,
                )
            )
            continue

        latest = series[-1]
        try:
            mm_yield = to_money_market_yield_pct(latest[1], inst.basis, inst.tenor_days)
        except MmktConversionError as exc:
            out.append(
                _row(
                    inst,
                    status=STATUS_MISSING,
                    detail=f"unconvertible: {exc}",
                    snapshot_date=snapshot_date,
                    latest=latest,
                    mm_yield_pct=None,
                )
            )
            continue

        staleness = (snapshot_date - latest[0]).days if snapshot_date else 0
        stale = staleness > inst.max_staleness_days
        out.append(
            _row(
                inst,
                status=STATUS_STALE if stale else STATUS_OK,
                detail=(
                    f"latest observation is {staleness} days old (limit {inst.max_staleness_days})"
                    if stale
                    else None
                ),
                snapshot_date=snapshot_date,
                latest=latest,
                mm_yield_pct=mm_yield,
            )
        )
    return out
