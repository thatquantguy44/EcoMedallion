"""Pure-logic tests for scripts/probe_mmkt_series.py (specs/spec009).

The script's network half cannot run in the build environment. What *can* be
pinned here is the part that decides pass/fail: reading the quoting basis out of
FRED's text, and classifying a fetched series against the config. The metadata
dicts below are hand-written to FRED's documented ``/fred/series`` shape; the
title strings are the ones FRED publishes. They are **not live captures** --
a real capture belongs in this directory once the probe has been run.
"""

from __future__ import annotations

import importlib.util
import pathlib
from datetime import date

import pytest

_SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "probe_mmkt_series.py"
_spec = importlib.util.spec_from_file_location("probe_mmkt_series", _SCRIPT)
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)

TODAY = date(2026, 10, 17)


def _meta(title, units="Percent", frequency_short="D", notes=""):
    return {"title": title, "units": units, "frequency_short": frequency_short, "notes": notes}


def _classify(meta, *, expected="discount", last=date(2026, 10, 16), max_staleness=5):
    return probe.classify_series(
        instrument_id="X",
        series_id="S",
        expected_basis=expected,
        max_staleness_days=max_staleness,
        meta=meta,
        last_observation_date=last,
        today=TODAY,
    )


def test_stated_basis_reads_discount_and_investment_text() -> None:
    assert probe.stated_basis("3-Month Treasury Bill Secondary Market Rate, Discount Basis") == "discount"
    assert probe.stated_basis(
        "Market Yield on U.S. Treasury Securities at 3-Month Constant Maturity, "
        "Quoted on an Investment Basis"
    ) == "bond_equivalent"
    assert probe.stated_basis("90-Day AA Financial Commercial Paper Interest Rate", "Discount Basis.") == "discount"


def test_silence_is_not_agreement() -> None:
    assert probe.stated_basis("Secured Overnight Financing Rate") is None
    assert probe.stated_basis("Discount Basis and Investment Basis") is None  # ambiguous


def test_a_matching_series_passes() -> None:
    record = _classify(_meta("3-Month Treasury Bill Secondary Market Rate, Discount Basis"))
    assert record["ok"] and record["basis_check"] == "match" and record["problems"] == []


def test_a_basis_mismatch_fails_and_says_which_way() -> None:
    record = _classify(
        _meta("Market Yield on U.S. Treasury Securities at 3-Month Constant Maturity, "
              "Quoted on an Investment Basis"),
        expected="discount",
    )
    assert not record["ok"] and record["basis_check"] == "MISMATCH"
    assert "config says 'discount' but FRED states 'bond_equivalent'" in record["problems"][0]


def test_overnight_rates_with_no_stated_basis_pass_as_money_market_only() -> None:
    sofr = _classify(_meta("Secured Overnight Financing Rate"), expected="money_market")
    assert sofr["ok"] and sofr["basis_check"] == "unverifiable"
    wrongly_configured = _classify(_meta("Secured Overnight Financing Rate"), expected="discount")
    assert not wrongly_configured["ok"]
    assert "states no basis -- verify by hand" in wrongly_configured["problems"][0]


def test_unresolved_wrong_units_wrong_frequency_and_stale_all_fail() -> None:
    assert _classify(None)["problems"] == ["id did not resolve on FRED"]
    assert "units" in " ".join(
        _classify(_meta("... Discount Basis", units="Index"))["problems"]
    )
    assert "frequency" in " ".join(
        _classify(_meta("... Discount Basis", frequency_short="M"))["problems"]
    )
    stale = _classify(_meta("... Discount Basis"), last=date(2026, 10, 1))
    assert not stale["ok"] and stale["staleness_days"] == 16
    assert _classify(_meta("... Discount Basis"), last=None)["problems"] == ["no observations returned"]


def test_auction_summary_reports_fields_without_asserting_a_shape() -> None:
    summary = probe.summarize_auction_fields(
        [{"auction_date": "2026-10-14", "high_discnt_rate": "4.9"}, {"auction_date": "x", "cusip": "y"}]
    )
    assert summary["date_like_fields"] == ["auction_date"]
    assert summary["rate_like_fields"] == ["high_discnt_rate"]
    assert summary["has_record_date"] is False
    assert probe.summarize_auction_fields([])["sample_size"] == 0
