"""``gold.mmkt_rate_snapshot`` engine and its instrument config (specs/spec009).

The conversion tests are pinned to numbers worked by hand with exact fractions,
not to the module's own output: the point of the table is that a 91-day bill at
a 5.00% discount rate and a 3-month constant maturity at the matching
bond-equivalent yield are *the same instrument*, and must normalize to the same
number.
"""

from __future__ import annotations

import glob
import os
from datetime import date
from fractions import Fraction

import pytest
import yaml

from fred_pipeline.gold_config.mmkt_config import (
    InstrumentDef,
    MmktConfig,
    MmktConfigError,
    load_mmkt_config,
)
from fred_pipeline.writer.mmkt_views import (
    NORMALIZED_BASIS,
    STATUS_MISSING,
    STATUS_OK,
    STATUS_STALE,
    MmktConversionError,
    compute_mmkt_rate_snapshot,
    to_money_market_yield_pct,
)


def _row(series_id, day, value, is_missing=False):
    return {
        "series_id": series_id,
        "observation_date": day,
        "value": value,
        "realtime_start": "2024-01-01",
        "is_missing": is_missing,
    }


def _inst(**overrides):
    fields = dict(
        instrument_id="TBILL_3M",
        label="3-Month T-Bill",
        instrument_class="tbill",
        series_id="DTB3",
        tenor_days=91,
        basis="discount",
        max_staleness_days=5,
    )
    fields.update(overrides)
    return InstrumentDef(**fields)


# --- conversions, against hand-worked values -----------------------------------------------


def _exact_discount_to_mm(d_pct: str, tenor: int) -> float:
    d = Fraction(d_pct) / 100
    return float(100 * d / (1 - d * tenor / 360))


def test_discount_to_money_market_matches_the_hand_worked_value() -> None:
    # 91-day bill at a 5.00% discount: 5 / (1 - 0.05*91/360) = 36000/7109 % exactly = 5.0640034...%
    assert to_money_market_yield_pct(5.0, "discount", 91) == pytest.approx(
        float(Fraction(36000, 7109)), rel=1e-12
    )
    assert to_money_market_yield_pct(5.0, "discount", 91) == pytest.approx(5.0640034, abs=1e-6)
    # 28-day at 4.00%: 4 / (1 - 0.04*28/360) = 9000/2243 % exactly = 4.0124833...%
    assert to_money_market_yield_pct(4.0, "discount", 28) == pytest.approx(
        float(Fraction(9000, 2243)), rel=1e-12
    )
    assert to_money_market_yield_pct(4.0, "discount", 28) == pytest.approx(4.0124833, abs=1e-6)
    # and the helper used below agrees with the pinned fractions
    assert _exact_discount_to_mm("5", 91) == pytest.approx(float(Fraction(36000, 7109)), rel=1e-12)
    # a discount rate of zero is a yield of zero
    assert to_money_market_yield_pct(0.0, "discount", 91) == 0.0


def test_discount_and_bond_equivalent_quotes_of_one_bill_normalize_identically() -> None:
    """A bill quoted at discount d has bond-equivalent yield 365 d / (360 - d t). Both
    quotes describe one instrument, so they must land on one money-market yield."""
    for d_pct, tenor in (("5", 91), ("4.25", 182), ("5.5", 28), ("0.35", 91)):
        d = Fraction(d_pct) / 100
        bey_pct = float(100 * 365 * d / (360 - d * tenor))
        via_discount = to_money_market_yield_pct(float(d_pct), "discount", tenor)
        via_bey = to_money_market_yield_pct(bey_pct, "bond_equivalent", tenor)
        assert via_bey == pytest.approx(via_discount, rel=1e-12), (d_pct, tenor)


def test_money_market_is_passed_through_unchanged() -> None:
    assert to_money_market_yield_pct(4.31, "money_market", 1) == 4.31


def test_the_raw_ranking_flips_once_normalized() -> None:
    """The reason the table exists. A 3-month bill at a 5.00% discount quotes *lower* than
    SOFR at 5.05%, yet yields more once both are on one basis."""
    sofr_quoted, bill_quoted = 5.05, 5.00
    assert bill_quoted < sofr_quoted
    assert to_money_market_yield_pct(bill_quoted, "discount", 91) > sofr_quoted


def test_a_nonsensical_discount_rate_is_refused() -> None:
    with pytest.raises(MmktConversionError, match="non-positive price"):
        to_money_market_yield_pct(400.0, "discount", 91)  # 400% over 91 days


def test_bond_equivalent_beyond_its_domain_is_refused() -> None:
    with pytest.raises(MmktConversionError, match="tenor <="):
        to_money_market_yield_pct(5.0, "bond_equivalent", 365)
    with pytest.raises(MmktConversionError, match="unknown basis"):
        to_money_market_yield_pct(5.0, "simple_annual", 91)


# --- config validation ---------------------------------------------------------------------


def test_config_rejects_bad_basis_class_tenor_and_staleness() -> None:
    for overrides, message in (
        ({"basis": "annualized"}, "invalid basis"),
        ({"instrument_class": "crypto"}, "invalid instrument_class"),
        ({"tenor_days": 0}, "tenor_days must be >= 1"),
        ({"max_staleness_days": 0}, "max_staleness_days"),
        ({"basis": "bond_equivalent", "tenor_days": 365}, "only defined for tenor_days"),
        ({"series_id": ""}, "missing required"),
    ):
        with pytest.raises(MmktConfigError, match=message):
            _inst(**overrides)


def test_loader_rejects_duplicates_unknown_fields_and_applies_the_default_staleness(
    tmp_path,
) -> None:
    def write(body: str) -> str:
        path = tmp_path / "mmkt.yml"
        path.write_text(body, encoding="utf-8")
        return str(path)

    base = (
        "- {instrument_id: A, label: A, instrument_class: tbill, series_id: DTB3, "
        "tenor_days: 91, basis: discount}\n"
    )
    with pytest.raises(MmktConfigError, match="Duplicate instrument"):
        load_mmkt_config(write("instruments:\n  " + base + "  " + base))
    with pytest.raises(Exception, match="unknown field"):
        load_mmkt_config(write("instruments:\n  - {instrument_id: A, label: A, wat: 1}\n"))

    loaded = load_mmkt_config(write("default_max_staleness_days: 9\ninstruments:\n  " + base))
    assert loaded.instruments[0].max_staleness_days == 9
    assert load_mmkt_config(str(tmp_path / "absent.yml")).instruments == ()


# --- the engine ----------------------------------------------------------------------------


def test_every_configured_instrument_gets_a_row_with_explicit_status() -> None:
    config = MmktConfig(
        instruments=(
            _inst(),
            _inst(instrument_id="ON_SOFR", label="SOFR", instrument_class="overnight",
                  series_id="SOFR", tenor_days=1, basis="money_market"),
            _inst(instrument_id="CP_NONFIN_3M", label="CP", instrument_class="cp",
                  series_id="DCPN3M", tenor_days=90),
        )
    )
    rows = [_row("DTB3", "2026-10-15", 5.0), _row("SOFR", "2026-10-15", 5.05)]

    snapshot = compute_mmkt_rate_snapshot(rows, config)

    assert [r["instrument_id"] for r in snapshot] == ["TBILL_3M", "ON_SOFR", "CP_NONFIN_3M"]
    by_id = {r["instrument_id"]: r for r in snapshot}
    assert by_id["TBILL_3M"]["status"] == STATUS_OK
    assert by_id["TBILL_3M"]["quoted_rate_pct"] == 5.0
    assert by_id["TBILL_3M"]["mm_yield_pct"] == pytest.approx(5.0640034, abs=1e-6)
    assert by_id["TBILL_3M"]["normalized_basis"] == NORMALIZED_BASIS
    assert by_id["ON_SOFR"]["mm_yield_pct"] == 5.05
    # The missing series is reported, not dropped -- the failure mode that once emptied a table.
    missing = by_id["CP_NONFIN_3M"]
    assert missing["status"] == STATUS_MISSING
    assert missing["mm_yield_pct"] is None and missing["quoted_rate_pct"] is None
    assert "DCPN3M" in missing["status_detail"]


def test_a_stale_series_is_flagged_with_its_age_and_limit() -> None:
    config = MmktConfig(instruments=(_inst(max_staleness_days=5), _inst(
        instrument_id="ON_SOFR", label="SOFR", instrument_class="overnight",
        series_id="SOFR", tenor_days=1, basis="money_market")))
    rows = [_row("DTB3", "2026-10-02", 5.0), _row("SOFR", "2026-10-15", 5.05)]

    by_id = {r["instrument_id"]: r for r in compute_mmkt_rate_snapshot(rows, config)}

    stale = by_id["TBILL_3M"]
    assert stale["status"] == STATUS_STALE
    assert stale["staleness_days"] == 13
    assert "13 days old (limit 5)" in stale["status_detail"]
    assert stale["mm_yield_pct"] is not None  # the value is shown; the status says do not price off it
    assert by_id["ON_SOFR"]["status"] == STATUS_OK and by_id["ON_SOFR"]["staleness_days"] == 0


def test_as_of_makes_the_snapshot_point_in_time_safe() -> None:
    config = MmktConfig(instruments=(_inst(),))
    rows = [
        _row("DTB3", "2026-10-01", 5.00),
        _row("DTB3", "2026-10-08", 4.90),
        _row("DTB3", "2026-10-15", 4.50),  # after as_of: must not be seen
    ]

    (row,) = compute_mmkt_rate_snapshot(rows, config, as_of=date(2026, 10, 9))

    assert row["quoted_rate_pct"] == 4.90
    assert row["observation_date"] == "2026-10-08"
    assert row["as_of_date"] == "2026-10-09" and row["staleness_days"] == 1

    (before_any,) = compute_mmkt_rate_snapshot(rows, config, as_of=date(2026, 9, 1))
    assert before_any["status"] == STATUS_MISSING
    assert "on or before 2026-09-01" in before_any["status_detail"]


def test_missing_flagged_observations_are_ignored() -> None:
    config = MmktConfig(instruments=(_inst(),))
    rows = [_row("DTB3", "2026-10-14", 5.0), _row("DTB3", "2026-10-15", 99.0, is_missing=True)]
    (row,) = compute_mmkt_rate_snapshot(rows, config)
    assert row["quoted_rate_pct"] == 5.0 and row["observation_date"] == "2026-10-14"


def test_an_unconvertible_value_is_reported_missing_not_raised() -> None:
    config = MmktConfig(instruments=(_inst(),))
    (row,) = compute_mmkt_rate_snapshot([_row("DTB3", "2026-10-15", 400.0)], config)
    assert row["status"] == STATUS_MISSING
    assert row["status_detail"].startswith("unconvertible")
    assert row["quoted_rate_pct"] == 400.0 and row["mm_yield_pct"] is None


def test_an_empty_config_yields_no_rows() -> None:
    assert compute_mmkt_rate_snapshot([_row("DTB3", "2026-10-15", 5.0)], MmktConfig()) == []


# --- the committed config ------------------------------------------------------------------


def _declared_series() -> dict[str, bool]:
    declared: dict[str, bool] = {}
    for path in glob.glob("manifests/*.yml"):
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        if not isinstance(doc, dict):
            continue
        for spec in doc.get("series") or []:
            if isinstance(spec, dict) and spec.get("series_id"):
                declared[spec["series_id"]] = bool(spec.get("active", True))
    return declared


def test_the_committed_config_loads_and_every_series_is_declared_in_a_manifest() -> None:
    """The TGCR/TGCRRATE lesson: an id no manifest declares is always a defect. Declared but
    inactive is a coverage decision, and the engine reports it as `missing`."""
    if not os.path.isfile("config/mmkt_instruments.yml"):
        pytest.skip("config/mmkt_instruments.yml not present")
    config = load_mmkt_config()
    assert len(config.instruments) >= 10
    declared = _declared_series()
    unknown = sorted(i.series_id for i in config.instruments if i.series_id not in declared)
    assert unknown == [], f"series named by config/mmkt_instruments.yml but declared in no manifest: {unknown}"


def test_the_committed_config_uses_each_series_once_and_the_right_bases() -> None:
    config = load_mmkt_config()
    series = [i.series_id for i in config.instruments]
    assert len(series) == len(set(series))
    bases = {i.series_id: i.basis for i in config.instruments}
    # FRED titles these "Discount Basis"; the CMT series are on the Treasury's investment basis.
    for sid in ("DTB4WK", "DTB3", "DTB6", "DCPF3M", "DCPN3M"):
        assert bases[sid] == "discount", sid
    for sid in ("DGS1MO", "DGS3MO", "DGS6MO"):
        assert bases[sid] == "bond_equivalent", sid
    for sid in ("SOFR", "EFFR", "OBFR", "IORB", "TGCRRATE", "RRPONTSYAWARD"):
        assert bases[sid] == "money_market", sid


def test_the_committed_snapshot_flags_the_not_yet_ingested_series() -> None:
    """With only some series ingested, the rest are `missing` rows, in config order."""
    config = load_mmkt_config()
    snapshot = compute_mmkt_rate_snapshot([_row("SOFR", "2026-10-15", 5.05)], config)
    assert [r["instrument_id"] for r in snapshot] == [i.instrument_id for i in config.instruments]
    statuses = {r["series_id"]: r["status"] for r in snapshot}
    assert statuses["SOFR"] == STATUS_OK
    assert all(s == STATUS_MISSING for sid, s in statuses.items() if sid != "SOFR")
