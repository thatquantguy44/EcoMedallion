"""Market-terminal analytical views: catalog/curve configs, the pure-Python
engines (dimensions, ECON dashboard, Curve Lab, enriched spreads), and their
integration into the local backend's build_gold."""

from __future__ import annotations

import pytest

from fred_pipeline.catalog_config import (
    VALID_CATEGORIES,
    CatalogConfigError,
    CatalogEntry,
    load_series_catalog,
)
from fred_pipeline.curve_config import (
    FALLBACK_TENORS,
    CurveConfigError,
    TenorDef,
    load_curve_defs,
)
from fred_pipeline.features import _group_sorted, compute_curve_spreads
from fred_pipeline.spread_config import SpreadDef
from fred_pipeline.terminal_views import (
    MARKET_CALENDARS,
    _recession_flags,
    build_dim_date,
    build_dim_series,
    compute_curve_spread_daily,
    compute_macro_dashboard,
    compute_market_calendar,
    compute_spread_inversion_episodes,
    compute_treasury_curve,
    resume_curve_spread_daily,
)


def _row(series_id, date, value, realtime_start="2024-01-01", is_missing=False):
    return {
        "series_id": series_id,
        "observation_date": date,
        "value": value,
        "realtime_start": realtime_start,
        "is_missing": is_missing,
    }


def _monthly(series_id, start_year, values):
    """Monthly rows starting January of start_year."""
    rows = []
    y, m = start_year, 1
    for v in values:
        rows.append(_row(series_id, f"{y:04d}-{m:02d}-01", v))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return rows


# ---- config loaders ---------------------------------------------------------


def test_load_series_catalog_missing_file_is_empty(tmp_path):
    assert load_series_catalog(str(tmp_path / "nope.yml")) == []


def test_load_series_catalog_parses_and_validates(tmp_path):
    p = tmp_path / "catalog.yml"
    p.write_text(
        "series:\n"
        "  - {series_id: UNRATE, econ_category: LABOR, polarity: -1,"
        " default_transform: level}\n"
        "  - {series_id: PAYEMS, econ_category: LABOR, polarity: 1,"
        " default_transform: chg, surprise_window: 6}\n"
    )
    entries = load_series_catalog(str(p))
    assert [e.series_id for e in entries] == ["UNRATE", "PAYEMS"]
    assert entries[0].polarity == -1
    assert entries[1].surprise_window == 6


@pytest.mark.parametrize(
    "body",
    [
        "series:\n  - {series_id: X, econ_category: NOPE}\n",  # bad category
        "series:\n  - {series_id: X, econ_category: LABOR, polarity: 2}\n",
        "series:\n  - {series_id: X, econ_category: LABOR, default_transform: yoy}\n",
        "series:\n  - {series_id: X, econ_category: LABOR, bogus: 1}\n",  # unknown field
        (
            "series:\n  - {series_id: X, econ_category: LABOR}\n"
            "  - {series_id: X, econ_category: RATES}\n"
        ),  # duplicate
    ],
)
def test_load_series_catalog_rejects_malformed(tmp_path, body):
    p = tmp_path / "catalog.yml"
    p.write_text(body)
    with pytest.raises(CatalogConfigError):
        load_series_catalog(str(p))


def test_repo_series_catalog_parses():
    entries = load_series_catalog("config/series_catalog.yml")
    assert len(entries) > 200
    assert any(e.series_id == "UNRATE" and e.polarity == -1 for e in entries)


def test_repo_series_catalog_covers_every_category():
    """Each bucket is populated, so no dashboard category renders empty."""
    entries = load_series_catalog("config/series_catalog.yml")
    covered = {e.econ_category for e in entries}
    assert covered == VALID_CATEGORIES


def test_repo_series_catalog_regional_entries_carry_geo():
    """A REGIONAL series without a geo can't be mapped -- guard against one
    slipping in. National entries stay blank."""
    entries = load_series_catalog("config/series_catalog.yml")
    regional = [e for e in entries if e.econ_category == "REGIONAL"]
    assert len(regional) >= 100
    assert all(e.geo for e in regional)
    assert any(e.series_id == "CAUR" and e.geo == "CA" for e in regional)
    assert not any(e.geo for e in entries if e.econ_category != "REGIONAL")


def test_repo_series_catalog_regional_entries_carry_metric():
    """REGIONAL mixes unemployment rates, activity indexes and house-price
    indexes -- three different units. Without a metric a report would have to
    re-derive it from the series-id suffix, and a visual could put them on one
    axis."""
    entries = load_series_catalog("config/series_catalog.yml")
    regional = [e for e in entries if e.econ_category == "REGIONAL"]
    assert all(e.metric for e in regional)
    assert {e.metric for e in regional} == {
        "Unemployment Rate",
        "Coincident Activity",
        "House Price Index",
    }
    # the metric must agree with the series-id family it came from
    for entry in regional:
        if entry.series_id.endswith("PHCI"):
            assert entry.metric == "Coincident Activity", entry.series_id
        elif entry.series_id.endswith("STHPI"):
            assert entry.metric == "House Price Index", entry.series_id
        elif entry.series_id.endswith("UR"):
            assert entry.metric == "Unemployment Rate", entry.series_id


def test_repo_series_catalog_entries_are_all_ingested():
    """Every cataloged series must be active in a manifest -- otherwise it
    produces a dim_series row that no observation ever joins to."""
    import glob

    import yaml

    active = set()
    for path in glob.glob("manifests/*.yml"):
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        for spec in doc.get("series") or []:
            if spec.get("active", True):
                active.add(spec["series_id"])
    orphans = [
        e.series_id
        for e in load_series_catalog("config/series_catalog.yml")
        if e.series_id not in active
    ]
    assert orphans == []


def test_load_curve_defs_fallback_and_repo_file(tmp_path):
    assert load_curve_defs(str(tmp_path / "nope.yml")) == list(FALLBACK_TENORS)
    tenors = load_curve_defs("config/curve.yml")
    assert [t.label for t in tenors][:3] == ["1M", "3M", "6M"]
    assert tenors == sorted(tenors, key=lambda t: t.months)


def test_load_curve_defs_rejects_duplicates(tmp_path):
    p = tmp_path / "curve.yml"
    p.write_text(
        "tenors:\n"
        "  - {label: 2Y, months: 24, series_id: DGS2}\n"
        "  - {label: 2Y, months: 25, series_id: DGS2X}\n"
    )
    with pytest.raises(CurveConfigError):
        load_curve_defs(str(p))


# ---- dimensions -------------------------------------------------------------


def test_build_dim_series_merges_meta():
    catalog = [CatalogEntry("UNRATE", "LABOR", polarity=-1)]
    meta = [
        {
            "series_id": "UNRATE",
            "title": "Unemployment Rate",
            "frequency": "m",
            "units": "Percent",
        }
    ]
    (row,) = build_dim_series(catalog, meta)
    assert row["title"] == "Unemployment Rate"
    assert row["econ_category"] == "LABOR"
    assert row["polarity"] == -1
    assert row["geo"] == ""  # national series carry no geography
    # not in meta -> blank descriptive fields, catalog fields intact
    (bare,) = build_dim_series([CatalogEntry("X", "RATES")], [])
    assert bare["title"] == "" and bare["econ_category"] == "RATES"


def test_build_dim_series_carries_geo_for_regional():
    catalog = [
        CatalogEntry(
            "CAUR", "REGIONAL", polarity=-1, geo="CA", metric="Unemployment Rate"
        )
    ]
    (row,) = build_dim_series(catalog, [])
    assert row["econ_category"] == "REGIONAL"
    assert row["geo"] == "CA"
    assert row["metric"] == "Unemployment Rate"


def test_build_dim_date_calendar_attributes():
    rows = build_dim_date("2024-09-29", "2024-10-02")
    assert [r["date"] for r in rows] == [
        "2024-09-29",
        "2024-09-30",
        "2024-10-01",
        "2024-10-02",
    ]
    sep30 = rows[1]
    assert sep30["is_month_end"] is True
    assert sep30["quarter"] == 3 and sep30["fiscal_year"] == 2024
    oct1 = rows[2]
    assert oct1["is_month_end"] is False
    assert oct1["quarter"] == 4 and oct1["fiscal_year"] == 2025  # US federal FY
    # no USREC ingested -> unknown, not false
    assert all(r["is_recession"] is None for r in rows)


def test_build_dim_date_recession_flag():
    usrec = [_row("USREC", "2020-03-01", 1.0), _row("USREC", "2020-05-01", 0.0)]
    rows = {r["date"]: r for r in build_dim_date("2020-03-01", "2020-05-02", usrec)}
    assert rows["2020-03-15"]["is_recession"] is True
    assert rows["2020-04-30"]["is_recession"] is True  # carries March/April print
    assert rows["2020-05-02"]["is_recession"] is False
    # before the first USREC print -> unknown
    early = build_dim_date("2020-02-01", "2020-02-01", usrec)
    assert early[0]["is_recession"] is None


def test_dim_date_date_key():
    rows = build_dim_date("2024-03-15", "2024-03-15")
    r = rows[0]
    assert r["date_key"] == 20240315


def test_dim_date_derivatives_marker_dates():
    # March 2024: 3rd Wednesday = Mar 20, 3rd Friday = Mar 15 (a quarter month
    # -> both IMM date and triple witching land on their respective days).
    rows = {r["date"]: r for r in build_dim_date("2024-03-01", "2024-03-31")}
    assert rows["2024-03-20"]["is_imm_date"] is True
    assert rows["2024-03-15"]["is_monthly_option_expiry"] is True
    assert rows["2024-03-15"]["is_triple_witching"] is True
    # a non-marker day in the same month
    assert rows["2024-03-01"]["is_imm_date"] is False
    assert rows["2024-03-01"]["is_monthly_option_expiry"] is False
    assert rows["2024-03-01"]["is_triple_witching"] is False


def test_dim_date_option_expiry_non_quarter_month_no_witching():
    # April is not a quarterly-cycle month -> monthly expiry fires, IMM/
    # triple witching don't.
    rows = {r["date"]: r for r in build_dim_date("2024-04-01", "2024-04-30")}
    apr19 = rows["2024-04-19"]  # 3rd Friday of April 2024
    assert apr19["is_monthly_option_expiry"] is True
    assert apr19["is_triple_witching"] is False
    assert all(not r["is_imm_date"] for r in rows.values())


# ---- market calendars (NYSE / SIFMA / FEDWIRE) ------------------------------


def test_market_calendar_covers_all_three_by_default():
    rows = compute_market_calendar("2026-01-01", "2026-01-31")
    assert {r["calendar_name"] for r in rows} == set(MARKET_CALENDARS)
    # 31 days x 3 calendars
    assert len(rows) == 31 * 3


def test_market_calendar_new_years_day_2026():
    # Jan 1, 2026 is a Thursday -> a holiday on all three calendars, no
    # observance shift needed.
    rows = compute_market_calendar("2026-01-01", "2026-01-01")
    by_cal = {r["calendar_name"]: r for r in rows}
    for cal in MARKET_CALENDARS:
        assert by_cal[cal]["is_holiday"] is True
        assert by_cal[cal]["holiday_name"] == "New Year's Day"
        assert by_cal[cal]["day_type"] == "Holiday"
        assert by_cal[cal]["is_business_day"] is False


def test_market_calendar_good_friday_closes_nyse_sifma_not_fedwire():
    # Good Friday 2026 = April 3 (Easter 2026 = April 5).
    rows = compute_market_calendar("2026-04-03", "2026-04-03")
    by_cal = {r["calendar_name"]: r for r in rows}
    assert by_cal["NYSE"]["is_holiday"] is True
    assert by_cal["SIFMA"]["is_holiday"] is True
    assert by_cal["FEDWIRE"]["is_holiday"] is False
    assert by_cal["FEDWIRE"]["is_business_day"] is True


def test_market_calendar_columbus_and_veterans_day_nyse_stays_open():
    # 2nd Monday of Oct 2026 = Oct 12; Veterans Day = Nov 11 (a Wednesday).
    rows = compute_market_calendar("2026-10-12", "2026-11-11")
    by_date = {(r["calendar_name"], r["calendar_date"]): r for r in rows}
    assert by_date[("NYSE", "2026-10-12")]["is_holiday"] is False
    assert by_date[("SIFMA", "2026-10-12")]["is_holiday"] is True
    assert by_date[("FEDWIRE", "2026-10-12")]["is_holiday"] is True
    assert by_date[("NYSE", "2026-11-11")]["is_holiday"] is False
    assert by_date[("SIFMA", "2026-11-11")]["is_holiday"] is True


def test_market_calendar_juneteenth_only_from_2022():
    rows_2021 = {
        r["calendar_name"]: r
        for r in compute_market_calendar("2021-06-18", "2021-06-18")
    }
    rows_2022 = {
        r["calendar_name"]: r
        for r in compute_market_calendar("2022-06-20", "2022-06-20")
    }
    # Jun 18 2021 is a Friday -> would be the observed date if the holiday
    # applied, but Juneteenth isn't federally recognized until 2022.
    assert all(not r["is_holiday"] for r in rows_2021.values())
    # Jun 19 2022 is a Sunday -> observed Monday Jun 20.
    assert all(r["is_holiday"] for r in rows_2022.values())
    assert rows_2022["NYSE"]["holiday_name"] == "Juneteenth National Independence Day"


def test_market_calendar_saturday_holiday_asymmetry():
    # July 4, 2026 is a Saturday: NYSE shifts the closure to Friday July 3;
    # SIFMA/FEDWIRE apply no Friday shift at all (faithfully ported from the
    # source query, not a bug -- see docs/dictionary/data_dictionary.md).
    rows = compute_market_calendar("2026-07-03", "2026-07-04")
    by_date = {(r["calendar_name"], r["calendar_date"]): r for r in rows}
    assert by_date[("NYSE", "2026-07-03")]["is_holiday"] is True
    assert by_date[("SIFMA", "2026-07-03")]["is_holiday"] is False
    assert by_date[("SIFMA", "2026-07-03")]["is_business_day"] is True
    assert by_date[("FEDWIRE", "2026-07-03")]["is_holiday"] is False


def test_market_calendar_business_day_walk_and_settlement():
    # 2026-01-01 is New Year's (Thu, holiday all cals); 01-02 is Fri (open);
    # 01-03/04 is weekend; 01-05 is Monday (open).
    rows = {
        r["calendar_date"]: r
        for r in compute_market_calendar("2025-12-30", "2026-01-06")
        if r["calendar_name"] == "NYSE"
    }
    assert rows["2026-01-01"]["prior_business_day"] == "2025-12-31"
    assert rows["2026-01-01"]["next_business_day"] == "2026-01-02"
    # T+2 settlement from Dec 31 (Wed): Thu Jan 1 is a holiday, so next
    # business day is Fri Jan 2, then Mon Jan 5.
    assert rows["2025-12-31"]["t2_settle_date"] == "2026-01-05"


def test_market_calendar_business_day_of_month_and_period_boundaries():
    rows = {
        r["calendar_date"]: r
        for r in compute_market_calendar("2026-01-01", "2026-01-31")
        if r["calendar_name"] == "NYSE"
    }
    # Jan 2, 2026 is the first business day of January (Jan 1 is a holiday).
    assert rows["2026-01-02"]["is_first_business_day_of_month"] is True
    assert rows["2026-01-02"]["business_day_of_month"] == 1
    # Jan 30 (Friday) is the last business day of January 2026.
    assert rows["2026-01-30"]["is_last_business_day_of_month"] is True
    assert rows["2026-01-31"]["is_last_business_day_of_month"] is False
    # non-business days carry no business_day_of_month ordinal
    assert rows["2026-01-03"]["business_day_of_month"] is None
    assert rows["2026-01-01"]["business_day_of_month"] is None


def test_market_calendar_only_requested_calendars_when_filtered():
    rows = compute_market_calendar("2026-01-01", "2026-01-01", calendars=("FEDWIRE",))
    assert {r["calendar_name"] for r in rows} == {"FEDWIRE"}


def test_market_calendar_manual_closure_applies_to_every_calendar():
    from datetime import date as _date

    rows = compute_market_calendar(
        "2026-01-05",
        "2026-01-05",
        manual_closures=[_date(2026, 1, 5)],
    )
    assert all(r["is_holiday"] and r["holiday_name"] == "Manual Closure" for r in rows)


def test_market_calendar_keeps_weekends_as_rows():
    # 2026-01-03/04 is a Saturday/Sunday -- rows must exist, not be dropped.
    rows = compute_market_calendar("2026-01-03", "2026-01-04")
    assert len(rows) == 2 * len(MARKET_CALENDARS)
    assert all(r["is_weekend"] for r in rows)
    assert all(not r["is_business_day"] for r in rows)


def test_dim_date_year_fields():
    rows = build_dim_date("2024-01-01", "2024-12-31")
    r_jan1 = rows[0]
    r_dec31 = rows[-1]
    assert r_jan1["is_year_start"] is True
    assert r_jan1["is_year_end"] is False
    assert r_dec31["is_year_start"] is False
    assert r_dec31["is_year_end"] is True
    assert r_jan1["year_label"] == "2024"
    assert r_jan1["year_start_date"] == "2024-01-01"
    assert r_jan1["year_end_date"] == "2024-12-31"
    # 2024 is a leap year
    assert r_jan1["is_leap_year"] is True


def test_dim_date_leap_year():
    # 2023 is not a leap year
    r = build_dim_date("2023-03-01", "2023-03-01")[0]
    assert r["is_leap_year"] is False


def test_dim_date_quarter_fields():
    # Q1: Jan 1 – Mar 31
    rows = {r["date"]: r for r in build_dim_date("2024-01-01", "2024-04-01")}
    jan1 = rows["2024-01-01"]
    assert jan1["quarter"] == 1
    assert jan1["quarter_label"] == "Q1"
    assert jan1["year_quarter"] == "2024-Q1"
    assert jan1["year_quarter_sort"] == 20241
    assert jan1["quarter_start_date"] == "2024-01-01"
    assert jan1["quarter_end_date"] == "2024-03-31"
    assert jan1["is_quarter_start"] is True
    assert jan1["is_quarter_end"] is False

    mar31 = rows["2024-03-31"]
    assert mar31["quarter"] == 1
    assert mar31["is_quarter_start"] is False
    assert mar31["is_quarter_end"] is True

    apr1 = rows["2024-04-01"]
    assert apr1["quarter"] == 2
    assert apr1["quarter_label"] == "Q2"
    assert apr1["quarter_start_date"] == "2024-04-01"
    assert apr1["quarter_end_date"] == "2024-06-30"
    assert apr1["is_quarter_start"] is True


def test_dim_date_month_fields():
    rows = {r["date"]: r for r in build_dim_date("2024-02-01", "2024-03-01")}
    feb1 = rows["2024-02-01"]
    assert feb1["month"] == 2
    assert feb1["month_name"] == "February"
    assert feb1["month_short_name"] == "Feb"
    assert feb1["year_month"] == "2024-02"
    assert feb1["year_month_sort"] == 202402
    assert feb1["month_start_date"] == "2024-02-01"
    assert feb1["month_end_date"] == "2024-02-29"  # 2024 is leap year
    assert feb1["is_month_start"] is True
    assert feb1["is_month_end"] is False
    assert feb1["days_in_month"] == 29

    feb29 = rows["2024-02-29"]
    assert feb29["is_month_end"] is True
    assert feb29["is_month_start"] is False

    mar1 = rows["2024-03-01"]
    assert mar1["days_in_month"] == 31
    assert mar1["is_month_start"] is True


def test_dim_date_iso_week_fields():
    # 2024-01-01 is a Monday → week 1 of 2024
    r = build_dim_date("2024-01-01", "2024-01-07")
    mon = r[0]  # 2024-01-01 Monday
    sun = r[6]  # 2024-01-07 Sunday
    assert mon["week_of_year"] == 1
    assert mon["iso_year"] == 2024
    assert mon["year_week"] == "2024-W01"
    assert mon["week_start_date"] == "2024-01-01"
    assert mon["week_end_date"] == "2024-01-07"
    assert mon["is_week_start"] is True
    assert mon["is_week_end"] is False
    assert sun["is_week_start"] is False
    assert sun["is_week_end"] is True


def test_dim_date_iso_year_boundary():
    # 2023-01-01 is a Sunday in ISO week 52 of 2022
    r = build_dim_date("2023-01-01", "2023-01-01")[0]
    assert r["iso_year"] == 2022
    assert r["week_of_year"] == 52


def test_dim_date_day_fields():
    # 2024-01-01 is a Monday
    r = build_dim_date("2024-01-01", "2024-01-01")[0]
    assert r["day_of_month"] == 1
    assert r["day_of_year"] == 1
    assert r["day_name"] == "Monday"
    assert r["day_short_name"] == "Mon"
    assert r["day_of_week_iso"] == 1  # 1=Monday ISO
    assert r["day_of_week_sun"] == 2  # Monday is 2 in Sun-first convention
    assert r["is_weekday"] is True
    assert r["is_weekend"] is False

    # 2024-01-06 is a Saturday
    sat = build_dim_date("2024-01-06", "2024-01-06")[0]
    assert sat["day_name"] == "Saturday"
    assert sat["day_of_week_iso"] == 6
    assert sat["day_of_week_sun"] == 7  # Saturday = 7 in Sun-first
    assert sat["is_weekday"] is False
    assert sat["is_weekend"] is True

    # 2024-01-07 is a Sunday
    sun = build_dim_date("2024-01-07", "2024-01-07")[0]
    assert sun["day_of_week_iso"] == 7
    assert sun["day_of_week_sun"] == 1  # Sunday = 1 in Sun-first


def test_dim_date_day_of_year():
    # 2024 is leap year; Dec 31 = day 366
    r = build_dim_date("2024-12-31", "2024-12-31")[0]
    assert r["day_of_year"] == 366
    # 2023 non-leap; Dec 31 = day 365
    r2 = build_dim_date("2023-12-31", "2023-12-31")[0]
    assert r2["day_of_year"] == 365


def test_dim_date_fiscal_year_fields():
    rows = {r["date"]: r for r in build_dim_date("2023-09-30", "2023-10-02")}
    sep30 = rows["2023-09-30"]  # last day of FY2023
    oct1 = rows["2023-10-01"]  # first day of FY2024
    oct2 = rows["2023-10-02"]

    assert sep30["fiscal_year"] == 2023
    assert sep30["fiscal_year_label"] == "FY2023"
    assert sep30["fiscal_month"] == 12  # Sep = fiscal month 12
    assert sep30["fiscal_quarter"] == 4
    assert sep30["fiscal_quarter_label"] == "FY2023-Q4"
    assert sep30["is_fiscal_year_end"] is True
    assert sep30["is_fiscal_year_start"] is False
    assert sep30["fiscal_year_start_date"] == "2022-10-01"
    assert sep30["fiscal_year_end_date"] == "2023-09-30"

    assert oct1["fiscal_year"] == 2024
    assert oct1["fiscal_year_label"] == "FY2024"
    assert oct1["fiscal_month"] == 1  # Oct = fiscal month 1
    assert oct1["fiscal_quarter"] == 1
    assert oct1["fiscal_quarter_label"] == "FY2024-Q1"
    assert oct1["is_fiscal_year_start"] is True
    assert oct1["is_fiscal_year_end"] is False
    assert oct1["is_fiscal_quarter_start"] is True
    assert oct1["fiscal_year_start_date"] == "2023-10-01"
    assert oct1["fiscal_year_end_date"] == "2024-09-30"
    assert oct1["fiscal_quarter_start_date"] == "2023-10-01"
    assert oct1["fiscal_quarter_end_date"] == "2023-12-31"

    assert oct2["is_fiscal_year_start"] is False
    assert oct2["is_fiscal_quarter_start"] is False


def test_dim_date_fiscal_quarter_boundaries():
    rows = {r["date"]: r for r in build_dim_date("2024-01-01", "2024-09-30")}
    # FQ2 = Jan–Mar of FY2024
    jan1 = rows["2024-01-01"]
    assert jan1["fiscal_quarter"] == 2
    assert jan1["is_fiscal_quarter_start"] is True
    assert jan1["fiscal_quarter_start_date"] == "2024-01-01"
    assert jan1["fiscal_quarter_end_date"] == "2024-03-31"

    mar31 = rows["2024-03-31"]
    assert mar31["fiscal_quarter"] == 2
    assert mar31["is_fiscal_quarter_end"] is True

    # FQ3 = Apr–Jun
    apr1 = rows["2024-04-01"]
    assert apr1["fiscal_quarter"] == 3
    assert apr1["is_fiscal_quarter_start"] is True

    # FQ4 = Jul–Sep
    jul1 = rows["2024-07-01"]
    assert jul1["fiscal_quarter"] == 4
    assert jul1["is_fiscal_quarter_start"] is True

    sep30 = rows["2024-09-30"]
    assert sep30["fiscal_quarter"] == 4
    assert sep30["is_fiscal_quarter_end"] is True
    assert sep30["is_fiscal_year_end"] is True


def test_dim_date_fiscal_year_quarter_sort():
    r = build_dim_date("2024-01-15", "2024-01-15")[0]
    # FY2024, FQ2
    assert r["fiscal_year_quarter_sort"] == 20242
    assert r["year_quarter_sort"] == 20241  # calendar Q1


def test_dim_date_contiguous_no_gaps():
    from datetime import date as _date
    from datetime import timedelta

    rows = build_dim_date("2020-01-01", "2020-12-31")
    assert len(rows) == 366  # 2020 is leap year
    for i, r in enumerate(rows):
        expected = (_date(2020, 1, 1) + timedelta(days=i)).isoformat()
        assert r["date"] == expected


def test_dim_date_complete_field_set():
    r = build_dim_date("2024-06-15", "2024-06-15")[0]
    expected_keys = {
        "date",
        "date_key",
        "year",
        "year_label",
        "year_start_date",
        "year_end_date",
        "is_year_start",
        "is_year_end",
        "is_leap_year",
        "quarter",
        "quarter_label",
        "year_quarter",
        "year_quarter_sort",
        "quarter_start_date",
        "quarter_end_date",
        "is_quarter_start",
        "is_quarter_end",
        "month",
        "month_name",
        "month_short_name",
        "year_month",
        "year_month_sort",
        "month_start_date",
        "month_end_date",
        "is_month_start",
        "is_month_end",
        "days_in_month",
        "iso_year",
        "week_of_year",
        "year_week",
        "week_start_date",
        "week_end_date",
        "is_week_start",
        "is_week_end",
        "day_of_month",
        "day_of_year",
        "day_name",
        "day_short_name",
        "day_of_week_iso",
        "day_of_week_sun",
        "is_weekday",
        "is_weekend",
        "fiscal_year",
        "fiscal_year_label",
        "fiscal_quarter",
        "fiscal_quarter_label",
        "fiscal_month",
        "fiscal_year_quarter_sort",
        "fiscal_year_start_date",
        "fiscal_year_end_date",
        "fiscal_quarter_start_date",
        "fiscal_quarter_end_date",
        "is_fiscal_year_start",
        "is_fiscal_year_end",
        "is_fiscal_quarter_start",
        "is_fiscal_quarter_end",
        "is_recession",
        "is_imm_date",
        "is_monthly_option_expiry",
        "is_triple_witching",
    }
    assert set(r.keys()) == expected_keys


# ---- ECON macro dashboard ---------------------------------------------------


def _labor_catalog(window=12):
    return [
        CatalogEntry("UNRATE", "LABOR", polarity=-1, surprise_window=window),
        CatalogEntry(
            "PAYEMS",
            "LABOR",
            polarity=1,
            default_transform="chg",
            surprise_window=window,
        ),
    ]


def test_macro_dashboard_core_columns():
    # 14 months of UNRATE: falls from 5.0 to 3.7 then ticks UP 0.2 at the end.
    unrate = [5.0 - 0.1 * i for i in range(13)] + [3.9]
    rows = _monthly("UNRATE", 2023, unrate)
    out = compute_macro_dashboard(rows, _labor_catalog())
    (d,) = out["dashboard"]
    assert d["series_id"] == "UNRATE"
    assert d["latest_value"] == pytest.approx(3.9)
    assert d["prior_value"] == pytest.approx(3.8)
    assert d["change_abs"] == pytest.approx(0.1)
    # date-based YoY: Feb 2024 vs Feb 2023
    assert d["yoy_pct"] == pytest.approx((3.9 - 4.9) / 4.9)
    # unemployment rising with polarity -1 -> deteriorating
    assert d["direction_is_good"] is False
    # surprise: latest minus trailing-12 mean (months 2..13 of the decline)
    window = unrate[1:13]
    assert d["surprise"] == pytest.approx(3.9 - sum(window) / len(window))
    assert d["staleness_days"] == 0
    assert d["as_of_date"] == "2024-02-01"


def test_macro_dashboard_polarity_and_summary():
    unrate = _monthly("UNRATE", 2023, [4.0, 3.9, 3.8])  # falling -> good
    payems = _monthly("PAYEMS", 2023, [157000, 157200, 157400])  # rising -> good
    out = compute_macro_dashboard(unrate + payems, _labor_catalog(window=2))
    by_id = {r["series_id"]: r for r in out["dashboard"]}
    assert by_id["UNRATE"]["direction_is_good"] is True
    assert by_id["PAYEMS"]["direction_is_good"] is True
    (cat,) = out["category_summary"]
    assert cat["econ_category"] == "LABOR"
    assert cat["n_series"] == 2
    assert cat["n_improving"] == 2 and cat["n_deteriorating"] == 0
    assert cat["breadth_pct"] == pytest.approx(1.0)


def test_macro_dashboard_sparkline_capped_and_ordered():
    rows = _monthly("UNRATE", 2020, [4.0 + 0.01 * i for i in range(50)])
    out = compute_macro_dashboard(rows, [CatalogEntry("UNRATE", "LABOR", polarity=-1)])
    spark = out["sparkline"]
    assert len(spark) == 36  # capped at SPARK_POINTS
    assert [p["point_index"] for p in spark] == list(range(36))
    assert spark[-1]["value"] == pytest.approx(4.49)  # newest = last point
    (d,) = out["dashboard"]
    assert d["spark_min"] == pytest.approx(spark[0]["value"])
    assert d["spark_max"] == pytest.approx(4.49)


def test_macro_dashboard_staleness_across_series():
    # PAYEMS is a month behind UNRATE; as_of is the global max date.
    unrate = _monthly("UNRATE", 2023, [4.0, 3.9, 3.8])
    payems = _monthly("PAYEMS", 2023, [157000, 157200])
    out = compute_macro_dashboard(unrate + payems, _labor_catalog(window=2))
    by_id = {r["series_id"]: r for r in out["dashboard"]}
    assert by_id["UNRATE"]["staleness_days"] == 0
    assert by_id["PAYEMS"]["staleness_days"] == 28  # 2023-02-01 -> 2023-03-01


def test_macro_dashboard_ignores_uncataloged_and_missing():
    rows = _monthly("UNRATE", 2023, [4.0, 3.9])
    rows += _monthly("NOT_IN_CATALOG", 2023, [1.0, 2.0])
    rows.append(_row("UNRATE", "2023-03-01", 99.0, is_missing=True))
    out = compute_macro_dashboard(rows, [CatalogEntry("UNRATE", "LABOR", polarity=-1)])
    (d,) = out["dashboard"]
    assert d["latest_value"] == pytest.approx(3.9)  # missing row excluded
    assert {r["series_id"] for r in out["sparkline"]} == {"UNRATE"}


def test_macro_dashboard_empty_catalog():
    out = compute_macro_dashboard(_monthly("UNRATE", 2023, [4.0]), [])
    assert out == {"dashboard": [], "sparkline": [], "category_summary": []}


# ---- Treasury Curve Lab -----------------------------------------------------

_TENORS = [
    TenorDef("3M", 3, "DGS3MO"),
    TenorDef("2Y", 24, "DGS2"),
    TenorDef("5Y", 60, "DGS5"),
    TenorDef("10Y", 120, "DGS10"),
    TenorDef("30Y", 360, "DGS30"),
]


def _curve_day(d, y3m, y2, y5, y10, y30):
    return [
        _row("DGS3MO", d, y3m),
        _row("DGS2", d, y2),
        _row("DGS5", d, y5),
        _row("DGS10", d, y10),
        _row("DGS30", d, y30),
    ]


def test_treasury_curve_rows_and_metrics():
    rows = _curve_day("2024-01-02", 5.4, 4.3, 4.0, 4.0, 4.2)
    out = compute_treasury_curve(rows, _TENORS)
    assert [c["tenor_label"] for c in out["curve"]] == ["3M", "2Y", "5Y", "10Y", "30Y"]
    assert out["curve"][0]["yield_pct"] == pytest.approx(5.4)
    (m,) = out["metrics"]
    assert m["level"] == pytest.approx((5.4 + 4.3 + 4.0 + 4.0 + 4.2) / 5)
    assert m["slope_10y2y"] == pytest.approx(-0.3)
    assert m["slope_10y3m"] == pytest.approx(-1.4)
    assert m["is_inverted_10y2y"] is True and m["is_inverted_10y3m"] is True
    assert m["curvature_2_5_10"] == pytest.approx(2 * 4.0 - 4.3 - 4.0)
    assert m["butterfly_2_10_30"] == pytest.approx(2 * 4.0 - 4.3 - 4.2)
    assert m["curve_move"] is None  # first date has no prior
    assert m["is_recession"] is None  # no USREC ingested


def test_treasury_curve_move_classification():
    rows = (
        _curve_day("2024-01-02", 5.4, 4.3, 4.0, 4.0, 4.2)
        # everything down, 2s10s wider -> bull steepener
        + _curve_day("2024-01-03", 5.2, 4.0, 3.9, 3.9, 4.1)
        # everything up, 2s10s tighter -> bear flattener
        + _curve_day("2024-01-04", 5.4, 4.5, 4.2, 4.1, 4.3)
    )
    out = compute_treasury_curve(rows, _TENORS)
    moves = [m["curve_move"] for m in out["metrics"]]
    assert moves == [None, "bull-steepener", "bear-flattener"]


def test_treasury_curve_skips_absent_tenors():
    # Only 3 of 5 tenors ingested: curve emits what exists; 2-5-10 curvature
    # is defined, 30Y-dependent metrics are not.
    rows = [
        _row("DGS2", "2024-01-02", 4.3),
        _row("DGS5", "2024-01-02", 4.0),
        _row("DGS10", "2024-01-02", 4.0),
    ]
    out = compute_treasury_curve(rows, _TENORS)
    assert len(out["curve"]) == 3
    (m,) = out["metrics"]
    assert m["slope_10y3m"] is None
    assert m["butterfly_2_10_30"] is None
    assert m["curvature_2_5_10"] is not None


# ---- enriched spread history --------------------------------------------------


def test_curve_spread_daily_enrichment():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = []
    # 4 days: positive, inverts for 2 days, re-steepens
    for d, y10, y2 in [
        ("2024-01-02", 4.0, 3.8),
        ("2024-01-03", 4.0, 4.1),
        ("2024-01-04", 4.0, 4.2),
        ("2024-01-05", 4.3, 4.0),
    ]:
        rows += [_row("DGS10", d, y10), _row("DGS2", d, y2)]
    out = compute_curve_spread_daily(rows, spreads)
    assert [r["value"] for r in out] == pytest.approx([0.2, -0.1, -0.2, 0.3])
    assert [r["value_bps"] for r in out] == pytest.approx([20.0, -10.0, -20.0, 30.0])
    assert [r["is_inverted"] for r in out] == [False, True, True, False]
    assert [r["inversion_run"] for r in out] == [0, 1, 2, 0]
    # expanding stats: first obs has no percentile, zscore null (std=0)
    assert out[0]["zscore"] is None and out[0]["percentile"] is None
    assert out[3]["percentile"] == pytest.approx(1.0)  # highest value so far
    assert all(r["is_recession"] is None for r in out)  # no USREC


def test_curve_spread_daily_ratio_has_no_inversion_semantics():
    spreads = [SpreadDef("RATIO", "DGS10", "DGS2", op="ratio")]
    rows = [_row("DGS10", "2024-01-02", 4.0), _row("DGS2", "2024-01-02", 2.0)]
    (r,) = compute_curve_spread_daily(rows, spreads)
    assert r["value"] == pytest.approx(2.0)
    assert r["value_bps"] is None
    assert r["is_inverted"] is None and r["inversion_run"] is None


def test_curve_spread_daily_recession_overlay():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = [
        _row("DGS10", "2020-03-15", 0.8),
        _row("DGS2", "2020-03-15", 0.4),
        _row("USREC", "2020-03-01", 1.0),
    ]
    (r,) = compute_curve_spread_daily(rows, spreads)
    assert r["is_recession"] is True


# ---- spec003 Phase 3: resumable curve_spread_daily ---------------------------


def _spread_base_rows(spread_name, spreads, rows):
    """The exact input resume_curve_spread_daily expects for one spread:
    compute_curve_spreads' output restricted to that spread, sorted."""
    base = compute_curve_spreads(rows, spreads)
    own = [r for r in base if r["spread_name"] == spread_name]
    return sorted(own, key=lambda r: r["observation_date"])


def _curve_spread_test_rows():
    rows = []
    for d, y10, y2 in [
        ("2024-01-02", 4.0, 3.8),
        ("2024-01-03", 4.0, 4.1),
        ("2024-01-04", 4.0, 4.2),
        ("2024-01-05", 4.3, 4.0),
    ]:
        rows += [_row("DGS10", d, y10), _row("DGS2", d, y2)]
    return rows


def test_resume_curve_spread_daily_matches_full_recompute_in_one_shot():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _curve_spread_test_rows()
    expected = compute_curve_spread_daily(rows, spreads)

    base_rows = _spread_base_rows("T10Y2Y", spreads, rows)
    flags = _recession_flags(rows)
    _state, out = resume_curve_spread_daily("T10Y2Y", True, base_rows, None, flags)

    assert out == expected


@pytest.mark.parametrize("k", [0, 1, 2, 3, 4])
def test_resume_curve_spread_daily_is_exact_across_a_split_point(k):
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _curve_spread_test_rows()
    expected = compute_curve_spread_daily(rows, spreads)

    base_rows = _spread_base_rows("T10Y2Y", spreads, rows)
    flags = _recession_flags(rows)

    state, out1 = resume_curve_spread_daily("T10Y2Y", True, base_rows[:k], None, flags)
    _state, out2 = resume_curve_spread_daily(
        "T10Y2Y", True, base_rows[k:], state, flags
    )

    assert out1 + out2 == expected


def test_resume_curve_spread_daily_ratio_never_sets_inversion_run():
    spreads = [SpreadDef("RATIO", "DGS10", "DGS2", op="ratio")]
    rows = [_row("DGS10", "2024-01-02", 4.0), _row("DGS2", "2024-01-02", 2.0)]
    base_rows = _spread_base_rows("RATIO", spreads, rows)
    flags = _recession_flags(rows)

    _state, out = resume_curve_spread_daily("RATIO", False, base_rows, None, flags)
    (r,) = out
    assert r["value_bps"] is None
    assert r["is_inverted"] is None and r["inversion_run"] is None


def test_resume_curve_spread_daily_carries_inversion_run_across_a_resume():
    """inversion_run is sequential state beyond the two shared primitives --
    this is the one thing a naive resume (state = just mean_std + percentile)
    would silently get wrong: resuming into an already-inverted run must
    continue counting, not reset to 0/1."""
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _curve_spread_test_rows()
    expected = compute_curve_spread_daily(rows, spreads)
    assert [r["inversion_run"] for r in expected] == [0, 1, 2, 0]

    base_rows = _spread_base_rows("T10Y2Y", spreads, rows)
    flags = _recession_flags(rows)

    # Split right after the run has started (day 2, already inverted once).
    state, out1 = resume_curve_spread_daily("T10Y2Y", True, base_rows[:2], None, flags)
    assert [r["inversion_run"] for r in out1] == [0, 1]
    _state, out2 = resume_curve_spread_daily(
        "T10Y2Y", True, base_rows[2:], state, flags
    )
    assert [r["inversion_run"] for r in out2] == [2, 0]
    assert out1 + out2 == expected


def test_resume_curve_spread_daily_recession_overlay():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = [
        _row("DGS10", "2020-03-15", 0.8),
        _row("DGS2", "2020-03-15", 0.4),
        _row("USREC", "2020-03-01", 1.0),
    ]
    base_rows = _spread_base_rows("T10Y2Y", spreads, rows)
    flags = _recession_flags(rows)
    _state, out = resume_curve_spread_daily("T10Y2Y", True, base_rows, None, flags)
    (r,) = out
    assert r["is_recession"] is True


# ---- inversion episodes -------------------------------------------------------


def _spread_days(pairs):
    """(date, y10, y2) triples -> latest rows for DGS10/DGS2."""
    rows = []
    for d, y10, y2 in pairs:
        rows += [_row("DGS10", d, y10), _row("DGS2", d, y2)]
    return rows


def test_inversion_episodes_split_on_resteepening():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days(
        [
            ("2024-01-01", 4.0, 3.8),  # +0.2
            ("2024-01-02", 4.0, 4.1),  # -0.1  episode 1 starts
            ("2024-01-03", 4.0, 4.3),  # -0.3  trough
            ("2024-01-04", 4.0, 4.2),  # -0.2
            ("2024-01-05", 4.2, 4.1),  # +0.1  episode 1 ends here
            ("2024-01-06", 4.0, 4.1),  # -0.1  episode 2 starts (new unique period)
            ("2024-01-07", 4.3, 4.0),  # +0.3  episode 2 ends here
        ]
    )
    eps = compute_spread_inversion_episodes(rows, spreads)
    assert len(eps) == 2
    e1, e2 = eps
    assert (e1["episode_number"], e2["episode_number"]) == (1, 2)
    assert e1["start_date"] == "2024-01-02"
    assert e1["end_date"] == "2024-01-05"  # first non-negative print
    assert e1["last_inverted_date"] == "2024-01-04"
    assert e1["observation_count"] == 3
    assert e1["calendar_days"] == 3  # Jan 2 -> Jan 5
    assert e1["trough_value"] == pytest.approx(-0.3)
    assert e1["trough_bps"] == pytest.approx(-30.0)
    assert e1["trough_date"] == "2024-01-03"
    assert e1["is_ongoing"] is False
    assert e2["start_date"] == "2024-01-06" and e2["end_date"] == "2024-01-07"
    assert e2["observation_count"] == 1


def test_inversion_episode_ongoing_at_end_of_history():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days(
        [
            ("2024-01-01", 4.0, 4.1),  # inverted from the first observation
            ("2024-01-03", 4.0, 4.2),  # still inverted at end of history
        ]
    )
    (ep,) = compute_spread_inversion_episodes(rows, spreads)
    assert ep["start_date"] == "2024-01-01"
    assert ep["end_date"] is None and ep["is_ongoing"] is True
    assert ep["last_inverted_date"] == "2024-01-03"
    assert ep["calendar_days"] == 2  # measured to last inverted obs
    assert ep["observation_count"] == 2


def test_inversion_episode_zero_is_not_inverted():
    # value == 0 matches is_inverted (v < 0) elsewhere: it closes an episode.
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days(
        [
            ("2024-01-01", 4.0, 4.1),  # -0.1
            ("2024-01-02", 4.0, 4.0),  #  0.0 -> episode ends
            ("2024-01-03", 4.0, 4.1),  # -0.1 -> new episode, ongoing
        ]
    )
    eps = compute_spread_inversion_episodes(rows, spreads)
    assert [e["episode_number"] for e in eps] == [1, 2]
    assert eps[0]["end_date"] == "2024-01-02"
    assert eps[1]["is_ongoing"] is True


def test_inversion_episodes_never_inverted_and_ratio_excluded():
    spreads = [
        SpreadDef("T10Y2Y", "DGS10", "DGS2"),
        SpreadDef("RATIO", "DGS10", "DGS2", op="ratio"),
    ]
    rows = _spread_days([("2024-01-01", 4.2, 4.0), ("2024-01-02", 4.3, 4.0)])
    assert compute_spread_inversion_episodes(rows, spreads) == []


def test_inversion_episode_recession_overlap():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days(
        [
            ("2020-02-15", 1.2, 1.3),  # inverted, pre-recession print unknown
            ("2020-03-15", 1.0, 1.2),  # inverted, in recession
            ("2020-04-15", 1.5, 1.0),  # re-steepens
        ]
    ) + [_row("USREC", "2020-03-01", 1.0)]
    (ep,) = compute_spread_inversion_episodes(rows, spreads)
    assert ep["recession_overlap"] is True
    # without USREC ingested the overlap is unknown, not false
    (ep2,) = compute_spread_inversion_episodes(
        _spread_days([("2020-02-15", 1.2, 1.3), ("2020-04-15", 1.5, 1.0)]),
        spreads,
    )
    assert ep2["recession_overlap"] is None


# ---- local backend integration ------------------------------------------------


def test_local_build_gold_populates_terminal_views(tmp_path, monkeypatch):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    # Point the dashboard at a temp catalog covering our test series.
    catalog = tmp_path / "catalog.yml"
    catalog.write_text(
        "series:\n"
        "  - {series_id: UNRATE, econ_category: LABOR, polarity: -1,"
        " surprise_window: 2}\n"
    )
    monkeypatch.setenv("FRED_SERIES_CATALOG_FILE", str(catalog))

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "t.db"),
    )
    silver = []
    for i, v in enumerate([4.0, 3.9, 3.8, 3.9]):
        silver.append(
            {
                "source": "fred",
                "series_id": "UNRATE",
                "observation_date": f"2024-0{i + 1}-01",
                "realtime_start": "",
                "realtime_end": "",
                "value": v,
                "raw_value": str(v),
                "is_missing": 0,
                "row_hash": f"h{i}",
                "revision_number": 1,
                "ingested_at": "2024-05-01T00:00:00",
                "run_id": "r1",
            }
        )
    for d, y10, y2 in [("2024-01-02", 4.0, 3.8), ("2024-01-03", 4.0, 4.1)]:
        for sid, v in (("DGS10", y10), ("DGS2", y2)):
            silver.append(
                {
                    "source": "fred",
                    "series_id": sid,
                    "observation_date": d,
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": v,
                    "raw_value": str(v),
                    "is_missing": 0,
                    "row_hash": f"{sid}{d}",
                    "revision_number": 1,
                    "ingested_at": "2024-05-01T00:00:00",
                    "run_id": "r1",
                }
            )
    wh.merge_silver(silver)
    results = wh.build_gold()
    for key in (
        "dim_series",
        "dim_date",
        "macro_indicator_dashboard",
        "macro_indicator_sparkline",
        "macro_category_summary",
        "treasury_curve",
        "treasury_curve_metrics",
        "curve_spread_daily",
        "spread_inversion_episode",
    ):
        assert results[key] == "ok"

    (dash,) = wh.query("SELECT * FROM gold_macro_indicator_dashboard")
    assert dash["series_id"] == "UNRATE"
    assert dash["direction_is_good"] == 0  # ticked up, polarity -1 -> stored int
    (dim,) = wh.query("SELECT * FROM gold_dim_series")
    assert dim["econ_category"] == "LABOR"
    curve = wh.query(
        "SELECT * FROM gold_treasury_curve ORDER BY as_of_date, tenor_months"
    )
    assert {c["tenor_label"] for c in curve} == {"2Y", "10Y"}
    metrics = wh.query("SELECT * FROM gold_treasury_curve_metrics ORDER BY as_of_date")
    assert metrics[0]["is_inverted_10y2y"] == 0
    assert metrics[1]["is_inverted_10y2y"] == 1
    spread = wh.query(
        "SELECT * FROM gold_curve_spread_daily WHERE spread_name='T10Y2Y' "
        "ORDER BY observation_date"
    )
    assert [r["inversion_run"] for r in spread] == [0, 1]
    (ep,) = wh.query(
        "SELECT * FROM gold_spread_inversion_episode WHERE spread_name='T10Y2Y'"
    )
    assert ep["start_date"] == "2024-01-03"
    assert ep["end_date"] is None and ep["is_ongoing"] == 1  # stored int
    # dim_date spans the observed range with month attributes
    days = wh.query(
        "SELECT COUNT(*) AS n, MIN(date) AS lo, MAX(date) AS hi FROM gold_dim_date"
    )[0]
    assert days["lo"] == "2024-01-01" and days["hi"] == "2024-04-01"
    wh.close()


# ---- Phase 4: rates complex ----------------------------------------------------

from fred_pipeline.rates_complex_config import (
    BenchmarkBoardConfig,
    BenchmarkRateDef,
    CreditConfig,
    CreditInstrumentDef,
    FundingConfig,
    FundingMetricDef,
    FundingSpreadDef,
    RatesComplexConfigError,
    StressComponent,
    load_benchmark_board,
    load_credit_config,
    load_funding_config,
)
from fred_pipeline.terminal_views import (
    compute_benchmark_rate_board,
    compute_credit_spread_daily,
    compute_funding_features,
    resume_credit_spread_daily,
    resume_funding_tape_entry,
)


def test_rates_complex_loaders_missing_files_are_empty(tmp_path):
    assert load_benchmark_board(str(tmp_path / "n.yml")).rates == ()
    cfg = load_funding_config(str(tmp_path / "n.yml"))
    assert cfg.metrics == () and cfg.spreads == ()
    assert load_credit_config(str(tmp_path / "n.yml")).instruments == ()


def test_rates_complex_repo_configs_parse():
    board = load_benchmark_board("config/benchmark_rates.yml")
    assert any(r.series_id == "SOFR" and r.benchmark == "EFFR" for r in board.rates)
    funding = load_funding_config("config/funding.yml")
    assert {s.name for s in funding.spreads} >= {"SOFR_EFFR", "SOFR_IORB"}
    assert all(
        c.spread in {s.name for s in funding.spreads} for c in funding.stress_components
    )
    credit = load_credit_config("config/credit.yml")
    assert any(c.instrument == "HY_OAS" for c in credit.instruments)
    assert credit.stress_percentile == pytest.approx(0.90)


def test_rates_complex_loaders_reject_malformed(tmp_path):
    p = tmp_path / "bad.yml"
    p.write_text("rates:\n  - {series_id: X, label: L, category: C, bogus: 1}\n")
    with pytest.raises(RatesComplexConfigError):
        load_benchmark_board(str(p))
    p.write_text("metrics:\n  - {name: A, series_id: X, metric_type: nope}\n")
    with pytest.raises(RatesComplexConfigError):
        load_funding_config(str(p))
    p.write_text(  # stress component referencing an unconfigured spread
        "metrics:\n  - {name: A, series_id: X, metric_type: rate}\n"
        "spreads:\n  - {name: S, long_leg: X, short_leg: Y}\n"
        "stress:\n  components:\n    - {spread: NOPE}\n"
    )
    with pytest.raises(RatesComplexConfigError):
        load_funding_config(str(p))
    p.write_text(
        "instruments:\n  - {instrument: I, series_id: X}\nstress_percentile: 1.5\n"
    )
    with pytest.raises(RatesComplexConfigError):
        load_credit_config(str(p))


def _board():
    return BenchmarkBoardConfig(
        rates=(
            BenchmarkRateDef("SOFR", "SOFR", "secured_overnight", benchmark="EFFR"),
            BenchmarkRateDef("EFFR", "EFFR", "policy"),
            BenchmarkRateDef("IORB", "IORB", "policy"),  # not ingested -> no row
        ),
        trend_window=2,
    )


def test_benchmark_rate_board_columns():
    rows = []
    for i, (d, sofr, effr) in enumerate(
        [
            ("2024-01-02", 5.31, 5.33),
            ("2024-01-03", 5.32, 5.33),
            ("2024-01-04", 5.34, 5.33),
            ("2024-01-05", 5.38, 5.33),
        ]
    ):
        rows += [_row("SOFR", d, sofr), _row("EFFR", d, effr)]
    out = compute_benchmark_rate_board(rows, _board())
    by_id = {r["series_id"]: r for r in out}
    assert set(by_id) == {"SOFR", "EFFR"}  # IORB absent -> no row
    sofr = by_id["SOFR"]
    assert sofr["latest_value"] == pytest.approx(5.38)
    assert sofr["change_bps"] == pytest.approx(4.0)
    # trend: latest (5.38) vs 2 obs ago (5.32) -> rising -> tightening
    assert sofr["trend"] == "rising" and sofr["regime"] == "tightening"
    assert sofr["spread_to_benchmark_bps"] == pytest.approx(5.0)
    assert sofr["staleness_days"] == 0
    effr = by_id["EFFR"]
    # flat within the 1bp dead-band -> stable, no benchmark spread
    assert effr["trend"] == "flat" and effr["regime"] == "stable"
    assert effr["spread_to_benchmark_bps"] is None
    assert effr["benchmark_series"] is None


def test_benchmark_rate_board_short_history_has_no_trend():
    rows = [_row("SOFR", "2024-01-02", 5.31), _row("SOFR", "2024-01-03", 5.32)]
    (r,) = compute_benchmark_rate_board(rows, _board())
    assert r["trend"] is None and r["regime"] is None
    assert r["prior_value"] == pytest.approx(5.31)


def _funding_cfg():
    return FundingConfig(
        metrics=(
            FundingMetricDef("SOFR", "SOFR", "rate"),
            FundingMetricDef("EFFR", "EFFR", "rate"),
            FundingMetricDef("RESERVES", "WRESBAL", "balance"),
        ),
        spreads=(FundingSpreadDef("SOFR_EFFR", "SOFR", "EFFR"),),
        stress_components=(StressComponent("SOFR_EFFR", 1.0),),
    )


def test_funding_tape_and_stress():
    rows = []
    # SOFR prints daily; EFFR too; reserves weekly. Last day SOFR spikes.
    for d, sofr, effr in [
        ("2024-01-02", 5.31, 5.33),
        ("2024-01-03", 5.31, 5.33),
        ("2024-01-04", 5.32, 5.33),
        ("2024-01-05", 5.45, 5.33),
    ]:
        rows += [_row("SOFR", d, sofr), _row("EFFR", d, effr)]
    rows.append(_row("WRESBAL", "2024-01-03", 3500.0))
    out = compute_funding_features(rows, _funding_cfg())
    tape = out["tape"]
    types = {(r["metric_name"], r["metric_type"]) for r in tape}
    assert ("SOFR", "rate") in types and ("RESERVES", "balance") in types
    assert ("SOFR_EFFR", "spread") in types
    spread_rows = [r for r in tape if r["metric_name"] == "SOFR_EFFR"]
    assert [r["value"] for r in spread_rows] == pytest.approx(
        [-0.02, -0.02, -0.01, 0.12]
    )
    # stress: one row per date where the (only) component prints
    stress = out["stress"]
    assert [s["observation_date"] for s in stress] == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
    ]
    # first obs has no z (expanding std 0) -> neutral 50 / "normal"
    assert stress[0]["stress_score"] == pytest.approx(50.0)
    assert stress[0]["stress_bucket"] == "normal"
    # the spike maps to the top bucket, clamped at 100
    assert stress[-1]["stress_score"] > 80.0
    assert stress[-1]["stress_bucket"] == "stressed"
    assert all(s["n_components"] == 1 for s in stress)


def test_funding_stress_requires_all_components():
    cfg = FundingConfig(
        metrics=(),
        spreads=(
            FundingSpreadDef("SOFR_EFFR", "SOFR", "EFFR"),
            FundingSpreadDef("SOFR_IORB", "SOFR", "IORB"),
        ),
        stress_components=(
            StressComponent("SOFR_EFFR"),
            StressComponent("SOFR_IORB"),
        ),
    )
    rows = [_row("SOFR", "2024-01-02", 5.31), _row("EFFR", "2024-01-02", 5.33)]
    out = compute_funding_features(rows, cfg)  # IORB absent
    assert [r["metric_name"] for r in out["tape"]] == ["SOFR_EFFR"]
    assert out["stress"] == []  # gauge needs every component


# ---- spec003 Phase 3: resumable funding tape --------------------------------


def _funding_test_rows():
    rows = []
    for d, sofr, effr in [
        ("2024-01-02", 5.31, 5.33),
        ("2024-01-03", 5.31, 5.33),
        ("2024-01-04", 5.32, 5.33),
        ("2024-01-05", 5.45, 5.33),
    ]:
        rows += [_row("SOFR", d, sofr), _row("EFFR", d, effr)]
    rows.append(_row("WRESBAL", "2024-01-03", 3500.0))
    return rows


def test_resume_funding_tape_entry_matches_full_recompute_for_a_metric():
    rows = _funding_test_rows()
    expected = [
        r
        for r in compute_funding_features(rows, _funding_cfg())["tape"]
        if r["metric_name"] == "SOFR"
    ]
    points = _group_sorted(rows).get("SOFR", [])
    _state, out = resume_funding_tape_entry("SOFR", "rate", points, None)
    assert out == expected


@pytest.mark.parametrize("k", [0, 1, 2, 3, 4])
def test_resume_funding_tape_entry_is_exact_across_a_split_point_for_a_metric(k):
    rows = _funding_test_rows()
    expected = [
        r
        for r in compute_funding_features(rows, _funding_cfg())["tape"]
        if r["metric_name"] == "SOFR"
    ]
    points = _group_sorted(rows).get("SOFR", [])
    state, out1 = resume_funding_tape_entry("SOFR", "rate", points[:k], None)
    _state, out2 = resume_funding_tape_entry("SOFR", "rate", points[k:], state)
    assert out1 + out2 == expected


def test_resume_funding_tape_entry_matches_full_recompute_for_a_spread():
    """The spread entity type (long - short, pre-differenced on common
    dates) must produce the same output as the metric type through the
    identical resumable function -- confirming the shared engine handles
    both tape row kinds correctly, not just raw metrics."""
    rows = _funding_test_rows()
    expected = [
        r
        for r in compute_funding_features(rows, _funding_cfg())["tape"]
        if r["metric_name"] == "SOFR_EFFR"
    ]
    by_series = _group_sorted(rows)
    short_map = dict(by_series["EFFR"])
    diff_points = [
        (d, v - short_map[d]) for d, v in by_series["SOFR"] if d in short_map
    ]
    _state, out = resume_funding_tape_entry("SOFR_EFFR", "spread", diff_points, None)
    assert out == expected


def test_credit_spread_daily_stress_and_recession():
    cfg = CreditConfig(
        instruments=(CreditInstrumentDef("HY_OAS", "BAMLH0A0HYM2", "headline"),),
        stress_percentile=0.75,
    )
    rows = [
        _row("BAMLH0A0HYM2", "2020-02-01", 3.5),
        _row("BAMLH0A0HYM2", "2020-03-01", 4.0),
        _row("BAMLH0A0HYM2", "2020-03-20", 8.7),
        _row("BAMLH0A0HYM2", "2020-04-10", 7.5),
        _row("USREC", "2020-03-01", 1.0),
    ]
    out = compute_credit_spread_daily(rows, cfg)
    assert [r["oas_bps"] for r in out] == pytest.approx([350.0, 400.0, 870.0, 750.0])
    assert out[2]["change_bps"] == pytest.approx(470.0)
    # first obs has no percentile -> unknown, not a stress episode verdict
    assert out[0]["is_stress_episode"] is None
    assert out[2]["is_stress_episode"] is True  # highest so far (pct 1.0)
    assert out[3]["is_stress_episode"] is False  # pct 2/3 < 0.75
    assert out[0]["is_recession"] is None  # before first USREC print
    assert out[2]["is_recession"] is True


def test_credit_spread_daily_absent_series_emit_nothing():
    cfg = CreditConfig(instruments=(CreditInstrumentDef("IG_OAS", "BAMLC0A0CM"),))
    assert compute_credit_spread_daily([_row("DGS10", "2024-01-02", 4.0)], cfg) == []


# ---- spec003 Phase 3: resumable credit_spread_daily --------------------------


def _credit_test_rows():
    return [
        _row("BAMLH0A0HYM2", "2020-02-01", 3.5),
        _row("BAMLH0A0HYM2", "2020-03-01", 4.0),
        _row("BAMLH0A0HYM2", "2020-03-20", 8.7),
        _row("BAMLH0A0HYM2", "2020-04-10", 7.5),
        _row("USREC", "2020-03-01", 1.0),
    ]


def _credit_points(rows):
    """(date, value) pairs for BAMLH0A0HYM2, sorted -- the exact input shape
    resume_credit_spread_daily expects."""
    by_series = _group_sorted(rows)
    return by_series.get("BAMLH0A0HYM2", [])


def test_resume_credit_spread_daily_matches_full_recompute_in_one_shot():
    cfg = CreditConfig(
        instruments=(CreditInstrumentDef("HY_OAS", "BAMLH0A0HYM2", "headline"),),
        stress_percentile=0.75,
    )
    rows = _credit_test_rows()
    expected = compute_credit_spread_daily(rows, cfg)

    points = _credit_points(rows)
    flags = _recession_flags(rows)
    _state, out = resume_credit_spread_daily(
        "HY_OAS", "BAMLH0A0HYM2", "headline", 0.75, points, None, flags
    )
    assert out == expected


@pytest.mark.parametrize("k", [0, 1, 2, 3, 4])
def test_resume_credit_spread_daily_is_exact_across_a_split_point(k):
    cfg = CreditConfig(
        instruments=(CreditInstrumentDef("HY_OAS", "BAMLH0A0HYM2", "headline"),),
        stress_percentile=0.75,
    )
    rows = _credit_test_rows()
    expected = compute_credit_spread_daily(rows, cfg)

    points = _credit_points(rows)
    flags = _recession_flags(rows)
    state, out1 = resume_credit_spread_daily(
        "HY_OAS", "BAMLH0A0HYM2", "headline", 0.75, points[:k], None, flags
    )
    _state, out2 = resume_credit_spread_daily(
        "HY_OAS", "BAMLH0A0HYM2", "headline", 0.75, points[k:], state, flags
    )
    assert out1 + out2 == expected


def test_resume_credit_spread_daily_carries_last_value_across_a_resume():
    """change_bps needs the prior observation's raw value -- the one piece
    of state beyond the two shared primitives. A naive resume that only
    carried mean_std/percentile state would silently emit change_bps=None
    for the first point of every resumed chunk instead of the real delta."""
    cfg = CreditConfig(
        instruments=(CreditInstrumentDef("HY_OAS", "BAMLH0A0HYM2", "headline"),),
        stress_percentile=0.75,
    )
    rows = _credit_test_rows()
    expected = compute_credit_spread_daily(rows, cfg)
    assert expected[2]["change_bps"] == pytest.approx(470.0)

    points = _credit_points(rows)
    flags = _recession_flags(rows)
    # Split right before the big move (index 2) so it lands in the second
    # chunk -- its change_bps must still reflect the point from chunk one.
    state, out1 = resume_credit_spread_daily(
        "HY_OAS", "BAMLH0A0HYM2", "headline", 0.75, points[:2], None, flags
    )
    _state, out2 = resume_credit_spread_daily(
        "HY_OAS", "BAMLH0A0HYM2", "headline", 0.75, points[2:], state, flags
    )
    assert out2[0]["change_bps"] == pytest.approx(470.0)
    assert out1 + out2 == expected


# ---- Phase 2: Inflation Explorer -------------------------------------------------

from fred_pipeline.inflation_config import (
    InflationConfigError,
    InflationItemDef,
    load_inflation_items,
)
from fred_pipeline.terminal_views import compute_inflation_explorer


def test_inflation_items_loader_missing_and_repo_file(tmp_path):
    assert load_inflation_items(str(tmp_path / "nope.yml")) == []
    items = load_inflation_items("config/inflation_items.yml")
    assert any(i.series_id == "CPIAUCSL" and i.level == 0 for i in items)
    # every parent resolves; exactly one root per (basket, sa_nsa) tree
    ids = {i.series_id for i in items}
    assert all(i.parent in ids for i in items if i.parent)
    roots = [(i.basket, i.sa_nsa) for i in items if i.level == 0]
    assert len(roots) == len(set(roots))
    # the 8 CPI/SA major groups carry weights and the waterfall flag
    wf = [i for i in items if i.waterfall and i.sa_nsa == "SA" and i.basket == "CPI"]
    assert len(wf) == 8 and all(i.weight for i in wf)


@pytest.mark.parametrize(
    "body",
    [
        # level-0 with a parent
        (
            "items:\n  - {series_id: A, label: L, basket: CPI, sa_nsa: SA,"
            " level: 0, parent: B}\n"
            "  - {series_id: B, label: L2, basket: CPI, sa_nsa: SA, level: 0}\n"
        ),
        # unknown parent
        (
            "items:\n  - {series_id: A, label: L, basket: CPI, sa_nsa: SA,"
            " level: 1, parent: NOPE}\n"
        ),
        # two roots in one tree
        (
            "items:\n  - {series_id: A, label: L, basket: CPI, sa_nsa: SA, level: 0}\n"
            "  - {series_id: B, label: L2, basket: CPI, sa_nsa: SA, level: 0}\n"
        ),
        # waterfall without weight
        (
            "items:\n  - {series_id: A, label: L, basket: CPI, sa_nsa: SA,"
            " level: 0, waterfall: true}\n"
        ),
        # bad basket
        ("items:\n  - {series_id: A, label: L, basket: RPI, sa_nsa: SA, level: 0}\n"),
    ],
)
def test_inflation_items_loader_rejects_malformed(tmp_path, body):
    p = tmp_path / "items.yml"
    p.write_text(body)
    with pytest.raises(InflationConfigError):
        load_inflation_items(str(p))


def _cpi_tree():
    return [
        InflationItemDef("CPIAUCSL", "All Items", "CPI", "SA", level=0),
        InflationItemDef(
            "FOOD",
            "Food",
            "CPI",
            "SA",
            parent="CPIAUCSL",
            level=1,
            weight=20.0,
            waterfall=True,
        ),
        InflationItemDef(
            "ENERGY",
            "Energy",
            "CPI",
            "SA",
            parent="CPIAUCSL",
            level=1,
            weight=10.0,
            waterfall=True,
        ),
    ]


def test_inflation_explorer_math():
    # 14 months of a smooth 0.5%/mo headline, then a 1.0% jump in the last month.
    vals = [100.0 * 1.005**i for i in range(13)] + [100.0 * 1.005**12 * 1.01]
    rows = _monthly("CPIAUCSL", 2023, vals)
    out = compute_inflation_explorer(
        rows, [InflationItemDef("CPIAUCSL", "All Items", "CPI", "SA")]
    )
    last = out["explorer"][-1]
    assert last["observation_date"] == "2024-02-01"
    assert last["mom_pct"] == pytest.approx(0.01)
    # YoY: months 2..13 grew 0.5%*11 then 1.0%: (1.005^11 * 1.01) - 1
    assert last["yoy_pct"] == pytest.approx(1.005**11 * 1.01 - 1)
    # acceleration: 1.0% this month vs 0.5% last month
    assert last["mom_accel"] == pytest.approx(0.005)
    # 3m annualized: (1.005^2 * 1.01)^4 - 1
    assert last["three_month_annualized"] == pytest.approx((1.005**2 * 1.01) ** 4 - 1)
    # steady months have ~zero acceleration
    mid = out["explorer"][6]
    assert mid["mom_accel"] == pytest.approx(0.0, abs=1e-9)
    # no weight configured -> no contribution
    assert last["contribution_pp"] is None


def test_inflation_contribution_waterfall():
    rows = (
        _monthly("CPIAUCSL", 2024, [100.0, 100.5])  # +0.5% headline
        + _monthly("FOOD", 2024, [100.0, 102.0])  # +2.0% * 20 -> 0.40pp
        + _monthly("ENERGY", 2024, [100.0, 99.0])  # -1.0% * 10 -> -0.10pp
    )
    out = compute_inflation_explorer(rows, _cpi_tree())
    feb = [r for r in out["contribution"] if r["observation_date"] == "2024-02-01"]
    assert len(feb) == 3
    head = next(r for r in feb if r["is_headline_total"])
    assert head["contribution_pp"] == pytest.approx(0.5)  # headline MoM in pp
    assert head["rank_in_month"] is None
    food = next(r for r in feb if r["series_id"] == "FOOD")
    energy = next(r for r in feb if r["series_id"] == "ENERGY")
    assert food["contribution_pp"] == pytest.approx(0.40)
    assert energy["contribution_pp"] == pytest.approx(-0.10)
    assert food["rank_in_month"] == 1 and energy["rank_in_month"] == 2
    # January: headline has no MoM (first obs) -> no waterfall rows at all
    assert not [r for r in out["contribution"] if r["observation_date"] == "2024-01-01"]
    # explorer rows carry the same contribution for waterfall items
    food_row = [r for r in out["explorer"] if r["series_id"] == "FOOD"][-1]
    assert food_row["contribution_pp"] == pytest.approx(0.40)
    assert food_row["weight"] == pytest.approx(20.0)


def test_inflation_explorer_gap_yields_nulls_not_wrong_months():
    # Jan, Feb, then a gap, then May: May's MoM must be null (no April),
    # not Feb-vs-May masquerading as month-over-month.
    rows = [
        _row("CPIAUCSL", "2024-01-01", 100.0),
        _row("CPIAUCSL", "2024-02-01", 100.5),
        _row("CPIAUCSL", "2024-05-01", 101.5),
    ]
    out = compute_inflation_explorer(
        rows, [InflationItemDef("CPIAUCSL", "All Items", "CPI", "SA")]
    )
    may = out["explorer"][-1]
    assert may["mom_pct"] is None and may["mom_accel"] is None
    assert may["three_month_annualized"] == pytest.approx(
        (101.5 / 100.5) ** 4 - 1
    )  # Feb IS exactly 3 months back


def test_inflation_explorer_absent_series_and_empty_config():
    out = compute_inflation_explorer(_monthly("CPIAUCSL", 2024, [100.0, 100.5]), [])
    assert out == {"explorer": [], "contribution": []}
    out = compute_inflation_explorer(
        [], [InflationItemDef("CPIAUCSL", "All Items", "CPI", "SA")]
    )
    assert out == {"explorer": [], "contribution": []}


# ---- rolling-window stats companions ----------------------------------------------

from datetime import date as _date
from datetime import timedelta as _timedelta

from fred_pipeline.terminal_views import (
    ROLLING_WINDOWS,
    _rolling_window_rows,
    compute_credit_spread_rolling,
    compute_curve_spread_rolling,
    compute_treasury_curve_rolling,
)


def _daily_series(values, start="2024-01-01"):
    d0 = _date.fromisoformat(start)
    return [(d0 + _timedelta(days=i), float(v)) for i, v in enumerate(values)]


def test_rolling_window_rows_math():
    # 10 obs, windows 1 and 5.
    series = _daily_series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    rows = _rolling_window_rows(series, windows=(1, 5))
    w1 = [r for r in rows if r["window"] == 1]
    w5 = [r for r in rows if r["window"] == 5]
    # window 1: starts at the 2nd obs; daily change; zscore always None (std 0)
    assert len(w1) == 9
    assert w1[0]["change"] == pytest.approx(1.0)
    assert w1[0]["pct_change"] == pytest.approx(1.0)  # 2 vs 1
    assert all(r["zscore"] is None for r in w1)
    # window 5: first row at the 6th obs (index 5): v=6, base=v[0]=1
    assert len(w5) == 5
    first = w5[0]
    assert first["observation_date"] == "2024-01-06"
    assert first["change"] == pytest.approx(5.0)
    assert first["pct_change"] == pytest.approx(5.0)
    # rolling mean of [2..6] = 4, pop std = sqrt(2) -> z = (6-4)/sqrt(2)
    assert first["zscore"] == pytest.approx(2.0 / 2.0**0.5)


def test_rolling_window_rows_flat_and_zero_base():
    # flat series: change 0, zscore None (std 0)
    rows = _rolling_window_rows(_daily_series([5, 5, 5, 5, 5, 5]), windows=(5,))
    (r,) = rows
    assert r["change"] == pytest.approx(0.0) and r["zscore"] is None
    # zero base: pct_change None, change still defined
    rows = _rolling_window_rows(_daily_series([0.0, 1.0]), windows=(1,))
    assert rows[0]["pct_change"] is None
    assert rows[0]["change"] == pytest.approx(1.0)


def test_rolling_window_no_partial_windows():
    rows = _rolling_window_rows(_daily_series([1, 2, 3]), windows=ROLLING_WINDOWS)
    assert {r["window"] for r in rows} == {1}  # only w=1 fully populated


def test_curve_spread_rolling_keys():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = []
    for i in range(7):
        d = f"2024-01-{i + 1:02d}"
        rows += [_row("DGS10", d, 4.0 + 0.01 * i), _row("DGS2", d, 3.8)]
    out = compute_curve_spread_rolling(rows, spreads, windows=(1, 5))
    assert {r["spread_name"] for r in out} == {"T10Y2Y"}
    w5 = [r for r in out if r["window"] == 5]
    assert len(w5) == 2
    assert w5[0]["change"] == pytest.approx(0.05)  # 5 * 1bp/day in pp


def test_credit_spread_rolling_in_bps():
    cfg = CreditConfig(instruments=(CreditInstrumentDef("HY_OAS", "BAMLH0A0HYM2"),))
    rows = [
        _row("BAMLH0A0HYM2", f"2024-01-{i + 1:02d}", 3.5 + 0.1 * i) for i in range(3)
    ]
    out = compute_credit_spread_rolling(rows, cfg, windows=(1,))
    assert [r["oas_bps"] for r in out] == pytest.approx([360.0, 370.0])
    assert all(r["change_bps"] == pytest.approx(10.0) for r in out)
    assert out[0]["instrument"] == "HY_OAS"


def test_treasury_curve_rolling_per_tenor():
    tenors = [TenorDef("2Y", 24, "DGS2"), TenorDef("10Y", 120, "DGS10")]
    rows = []
    for i in range(3):
        d = f"2024-01-{i + 1:02d}"
        rows += [_row("DGS2", d, 4.0 - 0.05 * i), _row("DGS10", d, 4.2)]
    out = compute_treasury_curve_rolling(rows, tenors, windows=(1,))
    by_tenor = {}
    for r in out:
        by_tenor.setdefault(r["tenor_label"], []).append(r)
    assert set(by_tenor) == {"2Y", "10Y"}
    assert all(r["change"] == pytest.approx(-0.05) for r in by_tenor["2Y"])
    assert all(r["change"] == pytest.approx(0.0) for r in by_tenor["10Y"])
    assert by_tenor["2Y"][0]["tenor_months"] == 24
