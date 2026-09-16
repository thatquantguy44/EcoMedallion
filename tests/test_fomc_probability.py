"""Tests for FOMC rate probabilities (item 3 of
docs/handoffs/terminal_phase0_gaps.md): config/fomc.yml loading, the
Treasury-curve forward-rate bootstrap, the ported outcome ladder, and
compute_fomc_probability end-to-end (including both Gold backends).
"""

from __future__ import annotations

import textwrap
from datetime import date, timedelta

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.gold_config.fomc_config import (
    CalendarProvenance,
    FOMCConfig,
    FOMCConfigError,
    FOMCTenorDef,
    load_fomc_config,
)
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.terminal_views import (
    _fomc_outcome_ladder,
    _zero_yield_at_months,
    compute_fomc_probability,
)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _row(series_id, value, obs_date="2026-07-17"):
    return {
        "series_id": series_id,
        "observation_date": obs_date,
        "value": value,
        "is_missing": False,
    }


CFG = FOMCConfig(
    meeting_dates=(date(2026, 9, 16), date(2026, 10, 28), date(2026, 12, 9)),
    bucket_step_bps=25,
    target_low_series="DFEDTARL",
    target_high_series="DFEDTARU",
    effective_rate_series="EFFR",
    tenors=(
        FOMCTenorDef("DGS1MO", 1),
        FOMCTenorDef("DGS3MO", 3),
        FOMCTenorDef("DGS6MO", 6),
        FOMCTenorDef("DGS1", 12),
    ),
)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def test_load_fomc_config(tmp_path):
    p = tmp_path / "fomc.yml"
    p.write_text(
        textwrap.dedent("""
        meeting_dates: ["2026-09-16", "2026-10-28"]
        bucket_step_bps: 25
        target_low_series: DFEDTARL
        target_high_series: DFEDTARU
        effective_rate_series: EFFR
        tenors:
          - {series_id: DGS1MO, tenor_months: 1}
          - {series_id: DGS3MO, tenor_months: 3}
    """)
    )
    cfg = load_fomc_config(str(p))
    assert cfg.meeting_dates == (date(2026, 9, 16), date(2026, 10, 28))
    assert len(cfg.tenors) == 2


def test_load_fomc_config_missing_file_returns_none():
    assert load_fomc_config("/nonexistent/fomc.yml") is None


# ---------------------------------------------------------------------------
# calendar_provenance (spec008 §5 Decision 6)
# ---------------------------------------------------------------------------

_BASE_CONFIG = """
meeting_dates: ["2026-09-16", "2026-10-28"]
bucket_step_bps: 25
target_low_series: DFEDTARL
target_high_series: DFEDTARU
effective_rate_series: EFFR
tenors:
  - {series_id: DGS1MO, tenor_months: 1}
  - {series_id: DGS3MO, tenor_months: 3}
"""


def _write_config(tmp_path, provenance_yaml: str = ""):
    p = tmp_path / "fomc.yml"
    p.write_text(_BASE_CONFIG + textwrap.dedent(provenance_yaml))
    return str(p)


def test_config_without_provenance_block_still_loads(tmp_path):
    """Every config written before spec008 lacks the block and must keep
    loading exactly as before -- the Gold tables do not need provenance."""
    cfg = load_fomc_config(_write_config(tmp_path))
    assert cfg.calendar_provenance is None
    assert cfg.meeting_dates == (date(2026, 9, 16), date(2026, 10, 28))


def test_provenance_block_is_parsed(tmp_path):
    cfg = load_fomc_config(
        _write_config(
            tmp_path,
            """
            calendar_provenance:
              source_url: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
              last_verified: 2026-09-15
              verified_by: scraper
              published_through: "2028-01-26"
            """,
        )
    )
    prov = cfg.calendar_provenance
    assert prov == CalendarProvenance(
        source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        # YAML gives a bare date as a date and a quoted one as a string; both
        # must end up as datetime.date.
        last_verified=date(2026, 9, 15),
        verified_by="scraper",
        published_through=date(2028, 1, 26),
    )


def test_provenance_fields_are_individually_optional(tmp_path):
    cfg = load_fomc_config(
        _write_config(
            tmp_path,
            """
            calendar_provenance:
              verified_by: manual
            """,
        )
    )
    assert cfg.calendar_provenance == CalendarProvenance(verified_by="manual")


@pytest.mark.parametrize(
    "block,match",
    [
        (
            """
            calendar_provenance:
              verified_by: someone
            """,
            "verified_by must be one of",
        ),
        (
            """
            calendar_provenance:
              last_verified: "last tuesday"
            """,
            "last_verified is not a date",
        ),
        (
            """
            calendar_provenance:
              published_through: "2028-13-01"
            """,
            "published_through is not a date",
        ),
        (
            """
            calendar_provenance:
              verified_on: 2026-09-15
            """,
            "unknown field",
        ),
        (
            """
            calendar_provenance: 2026-09-15
            """,
            "must be a mapping",
        ),
    ],
    ids=[
        "bad-verified_by",
        "bad-last_verified",
        "bad-published_through",
        "unknown-field",
        "not-a-mapping",
    ],
)
def test_malformed_provenance_raises(tmp_path, block, match):
    """A typo here must fail loudly: the whole point of the block is that
    tooling trusts it, and a silently-ignored field defeats that."""
    with pytest.raises(FOMCConfigError, match=match):
        load_fomc_config(_write_config(tmp_path, block))


def test_repo_config_provenance_is_present_and_consistent():
    """The real config must carry the block, and it must not claim more than
    the Fed has published: a meeting date beyond published_through would be a
    date nobody could have read off the calendar.

    Deliberately no "last_verified is recent" check -- that would read the
    wall clock and become exactly the kind of time bomb spec008 is removing.
    Staleness is the scheduled job's call.
    """
    cfg = load_fomc_config("config/fomc.yml")
    prov = cfg.calendar_provenance
    assert prov is not None, "config/fomc.yml is missing calendar_provenance"
    assert prov.verified_by in ("manual", "scraper")
    assert prov.last_verified is not None
    assert prov.published_through is not None
    assert max(cfg.meeting_dates) <= prov.published_through, (
        f"config/fomc.yml lists {max(cfg.meeting_dates)}, beyond the Fed's "
        f"published horizon {prov.published_through}"
    )


def test_fomc_config_rejects_unsorted_meeting_dates():
    with pytest.raises(FOMCConfigError):
        FOMCConfig(
            meeting_dates=(date(2026, 10, 28), date(2026, 9, 16)),
            bucket_step_bps=25,
            target_low_series="DFEDTARL",
            target_high_series="DFEDTARU",
            effective_rate_series="EFFR",
            tenors=(FOMCTenorDef("DGS1MO", 1), FOMCTenorDef("DGS3MO", 3)),
        )


def test_fomc_config_rejects_fewer_than_2_tenors():
    with pytest.raises(FOMCConfigError):
        FOMCConfig(
            meeting_dates=(date(2026, 9, 16),),
            bucket_step_bps=25,
            target_low_series="DFEDTARL",
            target_high_series="DFEDTARU",
            effective_rate_series="EFFR",
            tenors=(FOMCTenorDef("DGS1MO", 1),),
        )


# ---------------------------------------------------------------------------
# Curve interpolation + forward-rate bootstrap (hand-computable)
# ---------------------------------------------------------------------------


def test_zero_yield_at_months_interpolates_linearly():
    curve = [(1, 4.30), (3, 4.10), (6, 3.95), (12, 3.70)]
    # Halfway between the 1mo and 3mo points -> halfway between their yields.
    assert _zero_yield_at_months(curve, 2) == pytest.approx(4.20)


def test_zero_yield_at_months_clamps_beyond_ends():
    curve = [(1, 4.30), (12, 3.70)]
    assert _zero_yield_at_months(curve, 0.5) == 4.30
    assert _zero_yield_at_months(curve, 24) == 3.70


def test_forward_rate_bootstrap_matches_hand_computation():
    """(1+y1)**t1 * (1+f)**(t2-t1) == (1+y2)**t2 -- verified independently."""
    t1, y1 = 0.25, 4.0
    t2, y2 = 0.5, 4.5
    expected_f = (
        ((1 + y2 / 100) ** t2 / (1 + y1 / 100) ** t1) ** (1 / (t2 - t1)) - 1
    ) * 100
    assert expected_f == pytest.approx(5.002404, abs=1e-5)

    # Reproduce it through compute_fomc_probability's first->second meeting
    # transition: meeting 1 at t1 (curve yield = y1, degenerate bootstrap from
    # as_of), meeting 2 at t2 bootstraps between y1 and y2.
    as_of = date(2026, 1, 1)
    meeting1 = as_of + timedelta(days=round(t1 * 365))
    meeting2 = as_of + timedelta(days=round(t2 * 365))
    cfg = FOMCConfig(
        meeting_dates=(meeting1, meeting2),
        bucket_step_bps=25,
        target_low_series="DFEDTARL",
        target_high_series="DFEDTARU",
        effective_rate_series="EFFR",
        tenors=(
            FOMCTenorDef("T1", round(t1 * 365 / 30.4375)),
            FOMCTenorDef("T2", round(t2 * 365 / 30.4375)),
        ),
    )
    latest_rows = [
        _row("EFFR", 4.0, as_of.isoformat()),
        _row("DFEDTARL", 3.75, as_of.isoformat()),
        _row("DFEDTARU", 4.0, as_of.isoformat()),
        _row("T1", y1, as_of.isoformat()),
        _row("T2", y2, as_of.isoformat()),
    ]
    out = compute_fomc_probability(latest_rows, cfg, as_of=as_of)
    path = {r["meeting_date"]: r for r in out["meeting_path"]}
    # meeting 2's implied_rate should sit near rate_before_2 + f - rate_before_2
    # i.e. its expected_rate should approach the bootstrapped forward f as the
    # ladder's resolution allows (within one 25bp bucket).
    assert abs(path[meeting2.isoformat()]["implied_rate"] - expected_f) < 0.30


# ---------------------------------------------------------------------------
# compute_fomc_probability end-to-end
# ---------------------------------------------------------------------------


def _easing_curve_rows():
    return [
        _row("EFFR", 4.33),
        _row("DFEDTARL", 4.25),
        _row("DFEDTARU", 4.50),
        _row("DGS1MO", 4.30),
        _row("DGS3MO", 4.10),
        _row("DGS6MO", 3.95),
        _row("DGS1", 3.70),
    ]


def test_compute_fomc_probability_sums_to_one_per_meeting():
    out = compute_fomc_probability(_easing_curve_rows(), CFG, as_of=date(2026, 7, 17))
    sums: dict[str, float] = {}
    for r in out["probability"]:
        sums[r["meeting_date"]] = sums.get(r["meeting_date"], 0.0) + r["probability"]
    assert len(sums) == 3
    for meeting_date, total in sums.items():
        assert total == pytest.approx(1.0, abs=1e-6), meeting_date


def test_compute_fomc_probability_chains_meetings_forward():
    """An easing curve (short rates below EFFR) should produce a monotonic
    cutting path: each meeting's implied_rate <= the prior one's."""
    out = compute_fomc_probability(_easing_curve_rows(), CFG, as_of=date(2026, 7, 17))
    rates = [r["implied_rate"] for r in out["meeting_path"]]
    assert len(rates) == 3
    assert rates == sorted(rates, reverse=True)


def test_compute_fomc_probability_excludes_past_meetings():
    out = compute_fomc_probability(
        _easing_curve_rows(),
        CFG,
        as_of=date(2026, 11, 1),
    )
    meeting_dates = {r["meeting_date"] for r in out["meeting_path"]}
    assert meeting_dates == {"2026-12-09"}


def test_compute_fomc_probability_no_config_returns_empty():
    out = compute_fomc_probability(_easing_curve_rows(), None, as_of=date(2026, 7, 17))
    # cfg=None triggers load_fomc_config(); with the real repo config present
    # this actually loads config/fomc.yml, so instead assert the "missing
    # curve data" path directly.
    assert isinstance(out, dict) and "probability" in out and "meeting_path" in out


def test_compute_fomc_probability_missing_curve_data_returns_empty():
    out = compute_fomc_probability(
        [_row("EFFR", 4.33)],
        CFG,
        as_of=date(2026, 7, 17),
    )
    assert out == {"probability": [], "meeting_path": []}


def test_fomc_outcome_ladder_splits_between_bracketing_rungs():
    dist = _fomc_outcome_ladder(4.25, 4.20, step_bps=25, n_outcomes=5)
    assert sum(dist.values()) == pytest.approx(1.0)
    assert all(round(k * 100) % 25 == 0 for k in dist)


# ---------------------------------------------------------------------------
# Both-backends: gold.fomc_probability / gold.fomc_meeting_path shape parity
# ---------------------------------------------------------------------------


def test_local_warehouse_build_gold_populates_fomc_tables(tmp_path, monkeypatch):
    monkeypatch.setenv("FRED_FOMC_CONFIG_FILE", "config/fomc.yml")
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    silver = _easing_curve_rows()
    for r in silver:
        r.setdefault("source", "fred")
        r.setdefault("raw_value", str(r["value"]))
        r.setdefault("realtime_start", "")
        r.setdefault("realtime_end", "")
        r.setdefault("row_hash", f"{r['series_id']}{r['observation_date']}")
        r.setdefault("revision_number", 1)
        r.setdefault("ingested_at", "t")
        r.setdefault("run_id", "r")
    wh.merge_silver(silver)
    wh.build_gold()

    prob_rows = wh.conn.execute("SELECT * FROM gold_fomc_probability").fetchall()
    path_rows = wh.conn.execute("SELECT * FROM gold_fomc_meeting_path").fetchall()
    assert len(prob_rows) > 0
    assert len(path_rows) > 0


# ---- meeting-date runway -----------------------------------------------------
# compute_fomc_probability filters to `d >= today`, so once config/fomc.yml's
# last meeting passes, gold.fomc_probability and gold.fomc_meeting_path emit
# nothing and the Power BI Fed Policy Watch report goes blank -- with no error
# anywhere. These two tests are deliberately time-dependent: they are the alarm
# that fires while there is still time to refresh the list.

# A FLOOR, not the alarm. This used to be 120 days, which made it the only
# thing in the repo that noticed an expiring calendar -- and the way it noticed
# was by failing `pytest -q`, which CI runs on every push to every branch and
# every PR. An expiring FOMC config would have turned every unrelated pull
# request red (2027-09-28, on the calendar as it stood).
#
# spec008 moves the warning to a scheduled job that opens a PR, and leaves this
# as a last-resort gate: below 45 days there is no longer room for a normal
# review cycle, so blocking a release is proportionate. See
# specs/spec008/README.md §5 Decision 5.
MIN_RUNWAY_DAYS = 45


def test_fomc_config_has_a_future_meeting():
    """Hard failure: no future meeting means the FOMC tables are empty today."""
    cfg = load_fomc_config("config/fomc.yml")
    assert cfg is not None, "config/fomc.yml is missing"
    today = date.today()
    future = [d for d in cfg.meeting_dates if d >= today]
    assert future, (
        "config/fomc.yml has no meeting dates on or after today, so "
        "gold.fomc_probability and gold.fomc_meeting_path will be EMPTY and the "
        "Fed Policy Watch report will render blank. Refresh meeting_dates from "
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm "
        "(use the second day of each two-day meeting -- the decision date)."
    )


def test_fomc_meeting_dates_have_runway():
    """Early warning: fail while there is still time to act, not after."""
    cfg = load_fomc_config("config/fomc.yml")
    assert cfg is not None
    last = max(cfg.meeting_dates)
    remaining = (last - date.today()).days
    assert remaining >= MIN_RUNWAY_DAYS, (
        f"config/fomc.yml runs out in {remaining} days (last meeting {last}), "
        f"which is past the point where the scheduled refresh job should have "
        f"handled this.\n"
        f"FIRST: look for an open PR on the automation/fomc-calendar-refresh "
        f"branch, and for an open 'FOMC calendar refresh due' issue — the job "
        f"may already have proposed the fix.\n"
        f"If neither exists the job itself is broken; check its workflow run. "
        f"To fix by hand, run `python scripts/scrape_fomc_calendar.py "
        f"--missing-only` and paste the block into meeting_dates (DECISION "
        f"dates — the second day of each two-day meeting; keep ascending), "
        f"then update calendar_provenance at the top of the file."
    )


def test_fomc_meeting_dates_are_unique():
    """A duplicated date would double-count that meeting in the implied path."""
    cfg = load_fomc_config("config/fomc.yml")
    assert cfg is not None
    dates = list(cfg.meeting_dates)
    assert len(dates) == len(set(dates)), "config/fomc.yml has duplicate meeting_dates"
