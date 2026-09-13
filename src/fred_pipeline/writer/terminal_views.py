"""Market-terminal analytical views (pure Python).

Gold objects that recreate the ``market_terminal`` project's economic-analysis
surfaces for Power BI (plan: ``docs/market_terminal_gold_views.md``):

  * **dim_series / dim_date** — the star-schema dimensions every fact joins to;
  * **macro_indicator_dashboard** (+ sparkline + category summary) — the ECON
    macro grid: latest/prior/change/YoY/z-score/percentile/surprise/polarity
    per cataloged series, with category breadth and a surprise index;
  * **treasury_curve** (+ metrics) — the Curve Lab: the tidy tenor×date curve,
    level/slope/curvature/butterfly, inversion flags, recession overlay, and
    bull/bear × steepen/flatten curve-move classification;
  * **curve_spread_daily** — the configured spreads enriched with expanding
    (point-in-time safe) z-score/percentile, inversion flags/runs, recession;
  * **spread_inversion_episode** — one row per *unique inversion episode* per
    spread: opens on the first negative observation, closes when the spread
    turns non-negative again (with trough, duration, and recession overlap);
  * **benchmark_rate_board** — the BMRK rate board: latest/prior/change,
    trend, spread-to-benchmark, z-score/percentile, regime tag per rate;
  * **funding_tape_daily / funding_stress_daily** — the FUND tape (corridor
    rates, balances, spreads with expanding stats) and the 0–100 stress gauge;
  * **credit_spread_daily** — the CRDT OAS history with expanding stats and
    percentile-based stress-episode flags;
  * **inflation_explorer / inflation_contribution** — the INFL item trees
    (CPI SA/NSA, PCE): index/MoM/YoY/acceleration/3m-annualized per item, and
    the weight × MoM contribution waterfall against the headline print.
  * **market_calendar** — holiday-aware business-day calendars for NYSE,
    SIFMA, and FEDWIRE (long/tidy by ``calendar_name``), ported from a
    standalone Power Query "reusable quant date calendar." ``dim_date``
    gains the calendar-agnostic derivatives marker dates (IMM date, monthly
    option expiry, triple witching) from the same source query.

Kept pure (dict-in → dict-out, no Spark, no SQLite) so the Local and
Databricks backends share the same tested logic — the same pattern as
:mod:`fred_pipeline.features`. Rolling statistics are expanding-window only,
so nothing here leaks future information into historical rows.
"""

from __future__ import annotations

import calendar
from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from typing import Any

from fred_pipeline.catalog_config import CatalogEntry, load_series_catalog
from fred_pipeline.curve_config import TenorDef, load_curve_defs
from fred_pipeline.features import (
    _expanding_mean_std,
    _group_sorted,
    _parse,
    _pct_change,
    _year_ago_value,
    compute_curve_spreads,
    init_expanding_mean_std_state,
    init_expanding_percentile_state,
    resume_expanding_mean_std,
    resume_expanding_percentile,
)
from fred_pipeline.gold_config.fomc_config import FOMCConfig, load_fomc_config
from fred_pipeline.gold_config.release_calendar_config import (
    ReleaseCalendarEntry,
    load_release_calendar_config,
)
from fred_pipeline.inflation_config import InflationItemDef, load_inflation_items
from fred_pipeline.rates_complex_config import (
    BenchmarkBoardConfig,
    CreditConfig,
    FundingConfig,
    load_benchmark_board,
    load_credit_config,
    load_funding_config,
)
from fred_pipeline.spread_config import SpreadDef, load_spread_defs

# Series used for the recession overlay (NBER USREC, 1 = recession month).
RECESSION_SERIES = "USREC"

# Sparkline length the ECON dashboard renders (terminal shows 36 points).
SPARK_POINTS = 36


# ---- shared helpers ---------------------------------------------------------


def _recession_flags(
    latest_rows: Iterable[dict[str, Any]], series_id: str = RECESSION_SERIES
) -> list[tuple[date, bool]]:
    """Date-sorted ``(observation_date, in_recession)`` from the USREC series.

    Empty when USREC isn't ingested — callers then emit ``None`` for
    ``is_recession`` (unknown), not ``False`` (known expansion).
    """
    flags = [
        (d, v >= 1.0)
        for d, v in _group_sorted(
            r for r in latest_rows if r.get("series_id") == series_id
        ).get(series_id, [])
    ]
    return flags


def _recession_at(flags: list[tuple[date, bool]], d: date) -> bool | None:
    """USREC is monthly; a date's flag is the latest USREC obs on-or-before it."""
    if not flags:
        return None
    pos = bisect_right(flags, (d, True)) - 1
    if pos < 0:
        return None
    # Don't extrapolate more than ~2 months past the last USREC print.
    if (d - flags[pos][0]).days > 62:
        return None
    return flags[pos][1]


def _expanding_percentile(values: list[float]) -> list[float | None]:
    """Percent-rank of each value within the history up to and including it
    (0 = lowest seen so far, 1 = highest). PIT-safe: rank ``i`` uses only
    ``values[0..i]``. First observation has no rank (``None``)."""
    out: list[float | None] = []
    for i, v in enumerate(values):
        if i == 0:
            out.append(None)
            continue
        below = sum(1 for x in values[: i + 1] if x < v)
        out.append(below / i)
    return out


# ---- dimensions -------------------------------------------------------------


def build_dim_series(
    catalog: Iterable[CatalogEntry] | None = None,
    meta_rows: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    """``gold.dim_series``: one row per cataloged series, presentation semantics
    from ``config/series_catalog.yml`` merged with title/frequency/units from
    the ``meta`` layer (blank when the series isn't in meta yet)."""
    if catalog is None:
        catalog = load_series_catalog()
    meta = {m["series_id"]: m for m in meta_rows if m.get("series_id")}
    out: list[dict[str, Any]] = []
    for e in catalog:
        m = meta.get(e.series_id, {})
        out.append(
            {
                "series_id": e.series_id,
                "title": m.get("title") or "",
                "source": e.source,
                "frequency": m.get("frequency") or "",
                "units": m.get("units") or "",
                "econ_category": e.econ_category,
                "polarity": e.polarity,
                "default_transform": e.default_transform,
                "scale": e.scale,
                "decimals": e.decimals,
                "geo": e.geo,
                "metric": e.metric,
                "notes": e.notes,
            }
        )
    return sorted(out, key=lambda r: (r["econ_category"], r["series_id"]))


# US Federal fiscal year starts October 1.
# Maps fiscal_quarter → (start_calendar_month, end_calendar_month).
_FISCAL_Q_CAL = {1: (10, 12), 2: (1, 3), 3: (4, 6), 4: (7, 9)}

_DAY_NAMES = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)
_DAY_SHORT = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

# Standard quarterly-cycle months for IMM dates / triple witching.
_QUARTER_MONTHS = frozenset({3, 6, 9, 12})


def build_dim_date(
    start: Any, end: Any, recession_rows: Iterable[dict[str, Any]] = ()
) -> list[dict[str, Any]]:
    """``gold.dim_date``: full time-intelligence calendar dimension.

    One row per calendar day in [start, end].  Covers every attribute Power BI
    DAX time-intelligence functions need (date key, period start/end anchors,
    ISO week, day-of-week in both ISO and Sunday-=1 conventions, US Federal
    fiscal calendar, leap-year flag) plus the NBER recession flag (``None``
    until USREC is ingested — unknown, not false).

    US Federal fiscal year (October start): FY N runs Oct 1 of year N-1 through
    Sep 30 of year N.  fiscal_month=1 → October, fiscal_quarter=1 → Oct–Dec.
    """
    lo, hi = _parse(start), _parse(end)
    if lo is None or hi is None or lo > hi:
        return []
    flags = _recession_flags(recession_rows)
    out: list[dict[str, Any]] = []
    d = lo
    while d <= hi:
        # ---- day of week ------------------------------------------------
        dow = d.weekday()  # 0=Mon … 6=Sun
        iso_year, iso_week, _ = d.isocalendar()

        # ---- month -------------------------------------------------------
        _, dim = calendar.monthrange(d.year, d.month)
        month_start = date(d.year, d.month, 1)
        month_end = date(d.year, d.month, dim)

        # ---- quarter -----------------------------------------------------
        q = (d.month - 1) // 3 + 1
        q_start_month = (q - 1) * 3 + 1
        q_end_month = q * 3
        _, q_end_days = calendar.monthrange(d.year, q_end_month)
        quarter_start = date(d.year, q_start_month, 1)
        quarter_end = date(d.year, q_end_month, q_end_days)

        # ---- year --------------------------------------------------------
        year_start = date(d.year, 1, 1)
        year_end = date(d.year, 12, 31)

        # ---- ISO week bounds ---------------------------------------------
        week_start = d - timedelta(days=dow)  # Monday
        week_end = week_start + timedelta(days=6)  # Sunday

        # ---- US Federal fiscal calendar ----------------------------------
        fy = d.year + 1 if d.month >= 10 else d.year
        # fiscal_month: October=1, November=2, … September=12
        fm = (d.month - 10) % 12 + 1
        fq = (fm - 1) // 3 + 1
        fy_start = date(fy - 1, 10, 1)
        fy_end = date(fy, 9, 30)  # September always has 30 days
        fq_start_cm, fq_end_cm = _FISCAL_Q_CAL[fq]
        fq_start_cy = fy - 1 if fq_start_cm >= 10 else fy
        fq_end_cy = fy - 1 if fq_end_cm >= 10 else fy
        fq_start = date(fq_start_cy, fq_start_cm, 1)
        _, fq_end_days = calendar.monthrange(fq_end_cy, fq_end_cm)
        fq_end = date(fq_end_cy, fq_end_cm, fq_end_days)

        out.append(
            {
                # ---- date identifiers ----------------------------------------
                "date": d.isoformat(),
                "date_key": d.year * 10000 + d.month * 100 + d.day,
                # ---- calendar year -------------------------------------------
                "year": d.year,
                "year_label": str(d.year),
                "year_start_date": year_start.isoformat(),
                "year_end_date": year_end.isoformat(),
                "is_year_start": d == year_start,
                "is_year_end": d == year_end,
                "is_leap_year": calendar.isleap(d.year),
                # ---- calendar quarter ----------------------------------------
                "quarter": q,
                "quarter_label": f"Q{q}",
                "year_quarter": f"{d.year}-Q{q}",
                "year_quarter_sort": d.year * 10 + q,
                "quarter_start_date": quarter_start.isoformat(),
                "quarter_end_date": quarter_end.isoformat(),
                "is_quarter_start": d == quarter_start,
                "is_quarter_end": d == quarter_end,
                # ---- calendar month ------------------------------------------
                "month": d.month,
                "month_name": calendar.month_name[d.month],
                "month_short_name": calendar.month_abbr[d.month],
                "year_month": f"{d.year}-{d.month:02d}",
                "year_month_sort": d.year * 100 + d.month,
                "month_start_date": month_start.isoformat(),
                "month_end_date": month_end.isoformat(),
                "is_month_start": d == month_start,
                "is_month_end": d == month_end,
                "days_in_month": dim,
                # ---- ISO week ------------------------------------------------
                "iso_year": iso_year,
                "week_of_year": iso_week,
                "year_week": f"{iso_year}-W{iso_week:02d}",
                "week_start_date": week_start.isoformat(),
                "week_end_date": week_end.isoformat(),
                "is_week_start": dow == 0,
                "is_week_end": dow == 6,
                # ---- day -----------------------------------------------------
                "day_of_month": d.day,
                "day_of_year": d.timetuple().tm_yday,
                "day_name": _DAY_NAMES[dow],
                "day_short_name": _DAY_SHORT[dow],
                # ISO: 1=Monday … 7=Sunday  (Power BI "Monday as first day")
                "day_of_week_iso": dow + 1,
                # Sunday-first: 1=Sunday … 7=Saturday  (Power BI / DAX default)
                "day_of_week_sun": (dow + 1) % 7 + 1,
                "is_weekday": dow < 5,
                "is_weekend": dow >= 5,
                # ---- US Federal fiscal year (Oct start) ----------------------
                "fiscal_year": fy,
                "fiscal_year_label": f"FY{fy}",
                "fiscal_quarter": fq,
                "fiscal_quarter_label": f"FY{fy}-Q{fq}",
                "fiscal_month": fm,
                "fiscal_year_quarter_sort": fy * 10 + fq,
                "fiscal_year_start_date": fy_start.isoformat(),
                "fiscal_year_end_date": fy_end.isoformat(),
                "fiscal_quarter_start_date": fq_start.isoformat(),
                "fiscal_quarter_end_date": fq_end.isoformat(),
                "is_fiscal_year_start": d == fy_start,
                "is_fiscal_year_end": d == fy_end,
                "is_fiscal_quarter_start": d == fq_start,
                "is_fiscal_quarter_end": d == fq_end,
                # ---- NBER recession (None = unknown / not yet ingested) ------
                "is_recession": _recession_at(flags, d),
                # ---- quant/derivatives marker dates (calendar-agnostic --
                # same for every market_calendar row below, so kept here once
                # rather than repeated per calendar) --------------------------
                "is_imm_date": (
                    d.month in _QUARTER_MONTHS and dow == 2 and 15 <= d.day <= 21
                ),  # 3rd Wednesday of a quarter month
                "is_monthly_option_expiry": dow == 4 and 15 <= d.day <= 21,
                "is_triple_witching": (
                    d.month in _QUARTER_MONTHS and dow == 4 and 15 <= d.day <= 21
                ),  # 3rd Friday of a quarter month
            }
        )
        d += timedelta(days=1)
    return out


# ---- market calendars (NYSE / SIFMA / FEDWIRE) ------------------------------
#
# Ported from a standalone Power Query (M) "reusable quant date calendar":
# holiday-aware business-day logic for three US market calendars, each with
# its own holiday set (SIFMA observes Good Friday; FEDWIRE doesn't; NYSE
# observes Columbus Day/Veterans Day, SIFMA/FEDWIRE don't). Unlike the source
# query -- which picks ONE calendar via a parameter -- this keeps all three,
# long/tidy by ``calendar_name`` (the convention every other multi-cut Gold
# table here uses: ``curve_spread_rolling`` by window, `realized_volatility`
# by ticker x window, etc.) so a report can filter to whichever calendar its
# instrument settles against without three near-duplicate tables.
#
# Calendar-agnostic pieces of the source query (period parts, fiscal year,
# IMM/option-expiry/triple-witching marker dates) are NOT duplicated per
# calendar here -- they're pure date properties independent of which market
# is open, so they live once in ``gold.dim_date`` instead (the IMM/expiry
# flags added just above). The source query's wall-clock-relative section
# ("RELATIVE-TO-TODAY... dynamic; recalculated every refresh") is dropped
# entirely: every other table in this module is a deterministic function of
# its inputs (no wall-clock dependency), and "is this date in the future"
# is one comparison against CURRENT_DATE in any BI tool -- not worth
# persisting a column that changes value on every rebuild for the same row.

MARKET_CALENDARS: tuple[str, ...] = ("NYSE", "SIFMA", "FEDWIRE")


def _easter(year: int) -> date:
    """Western (Gregorian) Easter Sunday via the Gauss/Meeus algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d4 = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d4 - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = (h + ell - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def _good_friday(year: int) -> date:
    return _easter(year) - timedelta(days=2)


def _nth_weekday(year: int, month: int, weekday_sun0: int, n: int) -> date:
    """The nth occurrence of ``weekday_sun0`` (Sunday=0 … Saturday=6) in a
    month -- e.g. ``_nth_weekday(2026, 11, 4, 4)`` = the 4th Thursday of
    November (Thanksgiving)."""
    first = date(year, month, 1)
    first_dow = (first.weekday() + 1) % 7  # Python Monday=0 -> Sunday=0
    offset = (weekday_sun0 - first_dow + 7) % 7
    return date(year, month, 1 + offset + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday_sun0: int) -> date:
    """The last occurrence of ``weekday_sun0`` in a month -- e.g. the last
    Monday of May (Memorial Day)."""
    last_day = calendar.monthrange(year, month)[1]
    last = date(year, month, last_day)
    last_dow = (last.weekday() + 1) % 7
    offset = (last_dow - weekday_sun0 + 7) % 7
    return date(year, month, last_day - offset)


@dataclass(frozen=True)
class _HolidaySpec:
    name: str
    get: Any  # Callable[[int], date]
    is_new_year: bool
    nyse: bool
    sifma: bool
    fedwire: bool
    from_year: int | None = None


_HOLIDAY_SPECS: tuple[_HolidaySpec, ...] = (
    _HolidaySpec("New Year's Day", lambda y: date(y, 1, 1), True, True, True, True),
    _HolidaySpec(
        "Martin Luther King Jr. Day",
        lambda y: _nth_weekday(y, 1, 1, 3),
        False,
        True,
        True,
        True,
    ),
    _HolidaySpec(
        "Washington's Birthday",
        lambda y: _nth_weekday(y, 2, 1, 3),
        False,
        True,
        True,
        True,
    ),
    _HolidaySpec("Good Friday", lambda y: _good_friday(y), False, True, True, False),
    _HolidaySpec(
        "Memorial Day", lambda y: _last_weekday(y, 5, 1), False, True, True, True
    ),
    _HolidaySpec(
        "Juneteenth National Independence Day",
        lambda y: date(y, 6, 19),
        False,
        True,
        True,
        True,
        from_year=2022,
    ),
    _HolidaySpec("Independence Day", lambda y: date(y, 7, 4), False, True, True, True),
    _HolidaySpec(
        "Labor Day", lambda y: _nth_weekday(y, 9, 1, 1), False, True, True, True
    ),
    _HolidaySpec(
        "Columbus Day", lambda y: _nth_weekday(y, 10, 1, 2), False, False, True, True
    ),
    _HolidaySpec("Veterans Day", lambda y: date(y, 11, 11), False, False, True, True),
    _HolidaySpec(
        "Thanksgiving Day", lambda y: _nth_weekday(y, 11, 4, 4), False, True, True, True
    ),
    _HolidaySpec("Christmas Day", lambda y: date(y, 12, 25), False, True, True, True),
)


def _observe_nyse(d: date, is_new_year: bool) -> date | None:
    dow = (d.weekday() + 1) % 7  # Sunday=0
    if dow == 0:
        return d + timedelta(days=1)
    if dow == 6:  # Saturday: NYSE drops a Saturday New Year's, else moves back
        return None if is_new_year else d - timedelta(days=1)
    return d


def _observe_std(d: date) -> date | None:
    dow = (d.weekday() + 1) % 7
    if dow == 0:
        return d + timedelta(days=1)
    if dow == 6:
        return None
    return d


def _holidays_for_calendar(
    calendar_name: str,
    years: Iterable[int],
    manual_closures: Iterable[date] = (),
) -> dict[date, str]:
    """Observed holiday dates -> holiday name for one market calendar."""
    attr = calendar_name.lower()
    out: dict[date, str] = {d: "Manual Closure" for d in manual_closures}
    for y in years:
        for spec in _HOLIDAY_SPECS:
            if not getattr(spec, attr):
                continue
            if spec.from_year is not None and y < spec.from_year:
                continue
            actual = spec.get(y)
            observed = (
                _observe_nyse(actual, spec.is_new_year)
                if calendar_name == "NYSE"
                else _observe_std(actual)
            )
            if observed is not None:
                out[observed] = spec.name
    return out


def _is_business_day(d: date, holidays: dict[date, str]) -> bool:
    return d.weekday() < 5 and d not in holidays


def _prev_business_day(d: date, holidays: dict[date, str]) -> date:
    p = d - timedelta(days=1)
    while not _is_business_day(p, holidays):
        p -= timedelta(days=1)
    return p


def _next_business_day(d: date, holidays: dict[date, str]) -> date:
    n = d + timedelta(days=1)
    while not _is_business_day(n, holidays):
        n += timedelta(days=1)
    return n


def _last_bus_on_or_before(d: date, holidays: dict[date, str]) -> date:
    return d if _is_business_day(d, holidays) else _prev_business_day(d, holidays)


def _first_bus_on_or_after(d: date, holidays: dict[date, str]) -> date:
    return d if _is_business_day(d, holidays) else _next_business_day(d, holidays)


def compute_market_calendar(
    start: Any,
    end: Any,
    calendars: tuple[str, ...] = MARKET_CALENDARS,
    manual_closures: Iterable[date] = (),
) -> list[dict[str, Any]]:
    """``gold.market_calendar``: holiday-aware business-day calendar for each
    of ``calendars`` (default NYSE/SIFMA/FEDWIRE), long/tidy by
    ``calendar_name``. One row per ``(calendar_name, calendar_date)`` over
    every calendar day in ``[start, end]`` -- weekends/holidays are kept as
    rows (never dropped), described by flags.

    ``manual_closures`` are ad hoc, non-rule-based full closures (national
    mourning, disasters) applied to every calendar, matching the source
    query's escape hatch (empty by default here, same as there).

    NYSE observes Columbus Day and Veterans Day; SIFMA and FEDWIRE don't.
    SIFMA and NYSE observe Good Friday; FEDWIRE doesn't -- FEDWIRE staying
    open on Good Friday is exactly why it's the better calendar for T+1/T+2
    settlement math against a Fed-cleared instrument.
    """
    lo, hi = _parse(start), _parse(end)
    if lo is None or hi is None or lo > hi:
        return []
    # Pad a year on each side so business-day walks near the range boundary
    # still see the holidays just outside it.
    years = range(lo.year - 1, hi.year + 2)
    closures = list(manual_closures)

    out: list[dict[str, Any]] = []
    for cal_name in calendars:
        holidays = _holidays_for_calendar(cal_name, years, closures)

        # Pass 1: classify every date (business day, holiday, weekend).
        dates: list[date] = []
        is_bus: list[bool] = []
        d = lo
        while d <= hi:
            dates.append(d)
            is_bus.append(_is_business_day(d, holidays))
            d += timedelta(days=1)

        # Pass 2: full business-day list per (year, month) touched by the
        # range, computed once per month rather than per row (the source
        # query's fnBusDaysInRange would be O(n^2) if evaluated per day over
        # an 11k-row spine) -- and derived from the WHOLE month, not just
        # whatever falls inside [lo, hi], so business_day_of_month/
        # business_days_in_month are correct even when the query window
        # starts or ends mid-month (not just for a full min..max data range).
        month_bus_days: dict[tuple[int, int], list[date]] = {}

        # month_bus_days/holidays are per-cal_name loop variables; bound as
        # default args (evaluated once, at this def's execution on each
        # cal_name iteration) rather than closed over, so each iteration's
        # _month_business_days unambiguously uses that iteration's own
        # dicts -- not a B023 risk since they're mutated in place, never
        # reassigned, but binding them explicitly removes the ambiguity
        # entirely instead of relying on that reasoning.
        def _month_business_days(
            y: int, m: int, *, _cache=month_bus_days, _holidays=holidays
        ) -> list[date]:
            key = (y, m)
            cached = _cache.get(key)
            if cached is None:
                ndays = calendar.monthrange(y, m)[1]
                cached = [
                    date(y, m, day)
                    for day in range(1, ndays + 1)
                    if _is_business_day(date(y, m, day), _holidays)
                ]
                _cache[key] = cached
            return cached

        for d, b in zip(dates, is_bus):
            holiday_name = holidays.get(d)
            month_end = date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])
            quarter_end_month = ((d.month - 1) // 3 + 1) * 3
            quarter_end = date(
                d.year,
                quarter_end_month,
                calendar.monthrange(d.year, quarter_end_month)[1],
            )
            year_end = date(d.year, 12, 31)
            bdays = _month_business_days(d.year, d.month)
            out.append(
                {
                    "calendar_name": cal_name,
                    "calendar_date": d.isoformat(),
                    "is_weekend": d.weekday() >= 5,
                    "is_holiday": holiday_name is not None,
                    "holiday_name": holiday_name,
                    "day_type": (
                        "Holiday"
                        if holiday_name is not None
                        else "Weekend"
                        if d.weekday() >= 5
                        else "Business Day"
                    ),
                    "is_business_day": b,
                    "prior_business_day": _prev_business_day(d, holidays).isoformat(),
                    "next_business_day": _next_business_day(d, holidays).isoformat(),
                    "t2_settle_date": _next_business_day(
                        _next_business_day(d, holidays), holidays
                    ).isoformat(),
                    "business_day_of_month": bdays.index(d) + 1 if b else None,
                    "business_days_in_month": len(bdays),
                    "is_first_business_day_of_month": (
                        d == _first_bus_on_or_after(date(d.year, d.month, 1), holidays)
                    ),
                    "is_last_business_day_of_month": (
                        d == _last_bus_on_or_before(month_end, holidays)
                    ),
                    "is_last_business_day_of_quarter": (
                        d == _last_bus_on_or_before(quarter_end, holidays)
                    ),
                    "is_last_business_day_of_year": (
                        d == _last_bus_on_or_before(year_end, holidays)
                    ),
                }
            )
    return out


# ---- ECON macro dashboard ---------------------------------------------------


def compute_macro_dashboard(
    latest_rows: Iterable[dict[str, Any]],
    catalog: Iterable[CatalogEntry] | None = None,
    *,
    as_of: str | None = None,
    spark_points: int = SPARK_POINTS,
) -> dict[str, list[dict[str, Any]]]:
    """The ECON macro grid over cataloged series, from latest-revision rows.

    Returns three row sets keyed ``dashboard`` / ``sparkline`` /
    ``category_summary``. ``as_of`` defaults to the latest observation date
    across the cataloged series (deterministic — no wall-clock dependency), and
    drives ``staleness_days``. ``surprise`` is the no-consensus proxy the plan
    documents: latest value minus the trailing ``surprise_window`` mean
    (``surprise_z`` divides by that window's std). z-score/percentile are
    expanding (PIT-safe).
    """
    if catalog is None:
        catalog = load_series_catalog()
    entries = {e.series_id: e for e in catalog}
    if not entries:
        return {"dashboard": [], "sparkline": [], "category_summary": []}

    # Group per series, keeping realtime_start for the provenance column.
    per_series: dict[str, list[tuple[date, float, str]]] = {}
    for r in latest_rows:
        sid = r.get("series_id")
        if sid not in entries or r.get("is_missing"):
            continue
        v, d = r.get("value"), _parse(r.get("observation_date"))
        if v is None or d is None:
            continue
        per_series.setdefault(sid, []).append(
            (d, float(v), str(r.get("realtime_start") or "")[:10])
        )
    for pts in per_series.values():
        pts.sort(key=lambda t: t[0])

    as_of_date = (
        _parse(as_of)
        if as_of
        else max((pts[-1][0] for pts in per_series.values()), default=None)
    )
    if as_of_date is None:
        return {"dashboard": [], "sparkline": [], "category_summary": []}

    dashboard: list[dict[str, Any]] = []
    sparkline: list[dict[str, Any]] = []
    for sid, pts in sorted(per_series.items()):
        e = entries[sid]
        dates = [d for d, _v, _rt in pts]
        values = [v for _d, v, _rt in pts]
        i = len(pts) - 1
        latest_d, latest_v, latest_rt = pts[i]
        prior_d, prior_v = (pts[i - 1][0], pts[i - 1][1]) if i > 0 else (None, None)

        change_abs = (latest_v - prior_v) if prior_v is not None else None
        yoy = _pct_change(latest_v, _year_ago_value(dates, values, i))
        means, stds = _expanding_mean_std(values)
        zscore = ((latest_v - means[i]) / stds[i]) if stds[i] else None
        percentile = _expanding_percentile(values)[i]

        window = values[max(0, i - e.surprise_window) : i]  # excludes latest
        surprise = surprise_z = None
        if len(window) >= 2:
            w_mean = sum(window) / len(window)
            surprise = latest_v - w_mean
            w_std = (sum((x - w_mean) ** 2 for x in window) / len(window)) ** 0.5
            surprise_z = (surprise / w_std) if w_std else None

        direction_is_good: bool | None = None
        if e.polarity and change_abs:
            direction_is_good = (e.polarity * change_abs) > 0

        spark = values[-spark_points:]
        dashboard.append(
            {
                "series_id": sid,
                "econ_category": e.econ_category,
                "polarity": e.polarity,
                "default_transform": e.default_transform,
                "as_of_date": as_of_date.isoformat(),
                "latest_date": latest_d.isoformat(),
                "latest_value": latest_v,
                "prior_date": prior_d.isoformat() if prior_d else None,
                "prior_value": prior_v,
                "change_abs": change_abs,
                "change_pct": _pct_change(latest_v, prior_v),
                "yoy_pct": yoy,
                "zscore": zscore,
                "percentile": percentile,
                "surprise": surprise,
                "surprise_z": surprise_z,
                "direction_is_good": direction_is_good,
                "spark_min": min(spark),
                "spark_max": max(spark),
                "staleness_days": (as_of_date - latest_d).days,
                "realtime_start": latest_rt,
            }
        )
        for idx, (d, v, _rt) in enumerate(pts[-spark_points:]):
            sparkline.append(
                {
                    "series_id": sid,
                    "point_index": idx,
                    "observation_date": d.isoformat(),
                    "value": v,
                }
            )

    by_cat: dict[str, list[dict[str, Any]]] = {}
    for row in dashboard:
        by_cat.setdefault(row["econ_category"], []).append(row)
    category_summary: list[dict[str, Any]] = []
    for cat in sorted(by_cat):
        rows = by_cat[cat]
        directional = [r for r in rows if r["direction_is_good"] is not None]
        improving = sum(1 for r in directional if r["direction_is_good"])
        zscores = [r["zscore"] for r in rows if r["zscore"] is not None]
        surprises = [r["surprise_z"] for r in rows if r["surprise_z"] is not None]
        category_summary.append(
            {
                "econ_category": cat,
                "as_of_date": as_of_date.isoformat(),
                "n_series": len(rows),
                "n_improving": improving,
                "n_deteriorating": len(directional) - improving,
                "breadth_pct": (improving / len(directional)) if directional else None,
                "avg_zscore": (sum(zscores) / len(zscores)) if zscores else None,
                "surprise_index": (sum(surprises) / len(surprises))
                if surprises
                else None,
            }
        )
    return {
        "dashboard": dashboard,
        "sparkline": sparkline,
        "category_summary": category_summary,
    }


# ---- Treasury Curve Lab -----------------------------------------------------


def _tenor_yield(day: dict[int, float], months: int) -> float | None:
    return day.get(months)


def compute_treasury_curve(
    latest_rows: Iterable[dict[str, Any]],
    tenors: Iterable[TenorDef] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """The Curve Lab tables from daily constant-maturity history.

    Returns ``curve`` (tidy: one row per as-of date × tenor with data) and
    ``metrics`` (one row per as-of date): level (mean of available tenors),
    2s10s / 3m10s slopes, 2-5-10 curvature, 2-10-30 butterfly, inversion
    flags, the NBER recession flag (``None`` until USREC is ingested), and the
    bull/bear × steepen/flatten classification of the move vs the prior curve
    date (level down = bull; 2s10s wider = steepen).
    """
    if tenors is None:
        tenors = load_curve_defs()
    tenor_list = sorted(tenors, key=lambda t: t.months)
    by_series = _group_sorted(
        r
        for r in latest_rows
        if r.get("series_id") in {t.series_id for t in tenor_list}
    )
    flags = _recession_flags(latest_rows)

    # date -> {tenor_months: yield}
    by_date: dict[date, dict[int, float]] = {}
    curve: list[dict[str, Any]] = []
    for t in tenor_list:
        for d, v in by_series.get(t.series_id, []):
            by_date.setdefault(d, {})[t.months] = v
            curve.append(
                {
                    "as_of_date": d.isoformat(),
                    "tenor_label": t.label,
                    "tenor_months": t.months,
                    "series_id": t.series_id,
                    "yield_pct": v,
                }
            )
    curve.sort(key=lambda r: (r["as_of_date"], r["tenor_months"]))

    metrics: list[dict[str, Any]] = []
    prev_level: float | None = None
    prev_slope: float | None = None
    for d in sorted(by_date):
        day = by_date[d]
        level = sum(day.values()) / len(day)
        y3m, y2, y5 = day.get(3), day.get(24), day.get(60)
        y10, y30 = day.get(120), day.get(360)
        slope_10y2y = (y10 - y2) if (y10 is not None and y2 is not None) else None
        slope_10y3m = (y10 - y3m) if (y10 is not None and y3m is not None) else None
        curvature = (
            2 * y5 - y2 - y10
            if (y5 is not None and y2 is not None and y10 is not None)
            else None
        )
        butterfly = (
            2 * y10 - y2 - y30
            if (y10 is not None and y2 is not None and y30 is not None)
            else None
        )

        curve_move: str | None = None
        if (
            prev_level is not None
            and prev_slope is not None
            and slope_10y2y is not None
        ):
            d_level, d_slope = level - prev_level, slope_10y2y - prev_slope
            rally = "bull" if d_level < 0 else "bear"
            shape = "steepener" if d_slope > 0 else "flattener"
            if d_level and d_slope:
                curve_move = f"{rally}-{shape}"
            elif d_level:
                curve_move = f"parallel-{rally}"
            elif d_slope:
                curve_move = f"twist-{shape}"
        metrics.append(
            {
                "as_of_date": d.isoformat(),
                "level": level,
                "slope_10y2y": slope_10y2y,
                "slope_10y3m": slope_10y3m,
                "curvature_2_5_10": curvature,
                "butterfly_2_10_30": butterfly,
                "is_inverted_10y2y": (slope_10y2y < 0)
                if slope_10y2y is not None
                else None,
                "is_inverted_10y3m": (slope_10y3m < 0)
                if slope_10y3m is not None
                else None,
                "is_recession": _recession_at(flags, d),
                "curve_move": curve_move,
            }
        )
        prev_level = level
        if slope_10y2y is not None:
            prev_slope = slope_10y2y
    return {"curve": curve, "metrics": metrics}


# ---- enriched spread history -------------------------------------------------


def compute_curve_spread_daily(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Iterable[SpreadDef] | None = None,
) -> list[dict[str, Any]]:
    """``gold.curve_spread_daily``: the configured spreads/ratios
    (``config/spreads.yml``) enriched with expanding (PIT-safe) z-score and
    percentile, inversion flag + consecutive-inverted-observation run (spreads
    only — a ratio has no zero line), value in bps, and the recession flag."""
    if spreads is None:
        spreads = load_spread_defs()
    spread_list = list(spreads)
    ops = {sd.name: sd.op for sd in spread_list}
    base = compute_curve_spreads(latest_rows, spread_list)
    flags = _recession_flags(latest_rows)

    by_name: dict[str, list[dict[str, Any]]] = {}
    for row in base:
        by_name.setdefault(row["spread_name"], []).append(row)

    out: list[dict[str, Any]] = []
    for name in sorted(by_name):
        rows = sorted(by_name[name], key=lambda r: r["observation_date"])
        values = [r["value"] for r in rows]
        means, stds = _expanding_mean_std(values)
        pcts = _expanding_percentile(values)
        is_spread = ops.get(name) == "spread"
        run = 0
        for i, r in enumerate(rows):
            v = r["value"]
            inverted = (v < 0) if is_spread else None
            run = (run + 1) if inverted else 0
            d = _parse(r["observation_date"])
            out.append(
                {
                    "spread_name": name,
                    "observation_date": r["observation_date"],
                    "long_leg": r["long_leg"],
                    "short_leg": r["short_leg"],
                    "value": v,
                    "value_bps": (v * 100.0) if is_spread else None,
                    "zscore": ((v - means[i]) / stds[i]) if stds[i] else None,
                    "percentile": pcts[i],
                    "is_inverted": inverted,
                    "inversion_run": run if is_spread else None,
                    "is_recession": _recession_at(flags, d) if d else None,
                }
            )
    return out


def resume_curve_spread_daily(
    spread_name: str,
    is_spread: bool,
    new_base_rows: list[dict[str, Any]],
    state: dict[str, Any] | None,
    flags: list[tuple[date, bool]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """spec003 Phase 3: checkpointed/resumable per-spread body of
    :func:`compute_curve_spread_daily`, for one spread at a time.

    ``new_base_rows`` must already be exactly the rows past this spread's
    checkpoint frontier (from :func:`compute_curve_spreads`, restricted to
    this spread, sorted by ``observation_date``) -- this function does not
    filter or sort. ``state`` is ``None`` for a spread with no checkpoint yet
    (equivalent to a fresh start). ``flags`` is the shared recession overlay
    (small, global -- not itself entity-scoped state, since USREC changing
    is rare enough to just always recompute rather than checkpoint).

    In addition to the two expanding-stat primitives, ``inversion_run`` (the
    consecutive-inverted-observation counter) is itself sequential state
    that must carry across a resume, not just reset to 0 -- the one place
    this engine's state is more than the two shared primitives.
    """
    if state is None:
        mean_std_state = init_expanding_mean_std_state()
        pct_state = init_expanding_percentile_state()
        run = 0
    else:
        mean_std_state = state["mean_std"]
        pct_state = state["percentile"]
        run = state["inversion_run"]

    values = [r["value"] for r in new_base_rows]
    mean_std_state, means, stds = resume_expanding_mean_std(mean_std_state, values)
    pct_state, pcts = resume_expanding_percentile(pct_state, values)

    out: list[dict[str, Any]] = []
    for i, r in enumerate(new_base_rows):
        v = r["value"]
        inverted = (v < 0) if is_spread else None
        run = (run + 1) if inverted else 0
        d = _parse(r["observation_date"])
        out.append(
            {
                "spread_name": spread_name,
                "observation_date": r["observation_date"],
                "long_leg": r["long_leg"],
                "short_leg": r["short_leg"],
                "value": v,
                "value_bps": (v * 100.0) if is_spread else None,
                "zscore": ((v - means[i]) / stds[i]) if stds[i] else None,
                "percentile": pcts[i],
                "is_inverted": inverted,
                "inversion_run": run if is_spread else None,
                "is_recession": _recession_at(flags, d) if d else None,
            }
        )
    new_state = {
        "mean_std": mean_std_state,
        "percentile": pct_state,
        "inversion_run": run,
    }
    return new_state, out


def compute_spread_inversion_episodes(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Iterable[SpreadDef] | None = None,
) -> list[dict[str, Any]]:
    """``gold.spread_inversion_episode``: one row per unique inversion episode
    per configured spread (``op: spread`` only — a ratio has no zero line).

    An episode **opens** on the first observation where the spread is negative
    and **closes** on the first later observation where it is non-negative
    again (``end_date`` = that re-steepening date; a single positive print
    between two inversions therefore splits them into two distinct episodes).
    An episode still negative at the end of history is **ongoing**:
    ``end_date`` is ``None`` and duration is measured to ``last_inverted_date``.
    Each row carries the trough (most negative value and its date), the
    inverted-observation count, calendar duration, and whether any inverted
    date overlapped an NBER recession (``None`` until USREC is ingested).
    """
    if spreads is None:
        spreads = load_spread_defs()
    spread_list = [sd for sd in spreads if sd.op == "spread"]
    base = compute_curve_spreads(latest_rows, spread_list)
    flags = _recession_flags(latest_rows)

    by_name: dict[str, list[dict[str, Any]]] = {}
    for row in base:
        by_name.setdefault(row["spread_name"], []).append(row)

    out: list[dict[str, Any]] = []
    for name in sorted(by_name):
        rows = sorted(by_name[name], key=lambda r: r["observation_date"])
        episode: dict[str, Any] | None = None
        number = 0

        # episode is a per-name loop variable, reassigned through the inner
        # loop below; _close is called synchronously within that same loop
        # (never stored/deferred), so it always reads the current value --
        # B023 can't verify that liveness property statically.
        def _close(end_date: str | None) -> None:
            ep = episode  # noqa: B023 -- intentional late-binding read, see above
            last = _parse(ep["last_inverted_date"])
            start = _parse(ep["start_date"])
            end = _parse(end_date) if end_date else None
            ep["end_date"] = end_date
            ep["is_ongoing"] = end_date is None
            ep["calendar_days"] = ((end or last) - start).days
            ep["trough_bps"] = ep["trough_value"] * 100.0
            out.append(ep)

        for r in rows:
            v, d = r["value"], str(r["observation_date"])[:10]
            if v < 0:
                rec = _recession_at(flags, _parse(d))
                if episode is None:  # first negative print opens the episode
                    number += 1
                    episode = {
                        "spread_name": name,
                        "long_leg": r["long_leg"],
                        "short_leg": r["short_leg"],
                        "episode_number": number,
                        "start_date": d,
                        "end_date": None,
                        "last_inverted_date": d,
                        "observation_count": 1,
                        "calendar_days": 0,
                        "trough_value": v,
                        "trough_bps": v * 100.0,
                        "trough_date": d,
                        "is_ongoing": True,
                        "recession_overlap": rec,
                    }
                else:
                    episode["last_inverted_date"] = d
                    episode["observation_count"] += 1
                    if v < episode["trough_value"]:
                        episode["trough_value"] = v
                        episode["trough_date"] = d
                    if rec is not None:
                        episode["recession_overlap"] = (
                            bool(episode["recession_overlap"]) or rec
                        )
            elif episode is not None:
                _close(d)  # re-steepened: this date ends the episode
                episode = None
        if episode is not None:
            _close(None)  # still inverted at the end of history
    return out


# ---- BMRK benchmark rate board ------------------------------------------------

# A move smaller than this (in the rate's native percent units) counts as flat
# for the trend verdict: 1bp.
TREND_EPSILON = 0.01


def compute_benchmark_rate_board(
    latest_rows: Iterable[dict[str, Any]],
    board: BenchmarkBoardConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.benchmark_rate_board``: one row per configured rate
    (``config/benchmark_rates.yml``) at its latest observation — latest/prior,
    change in bps, a trend verdict (latest vs. ``trend_window`` observations
    ago, ±1bp dead-band), expanding (PIT-safe) z-score/percentile, the spread
    to the configured benchmark (benchmark's last value on-or-before the
    rate's date), a regime tag from the trend (rising→tightening,
    falling→easing, flat→stable), and staleness vs. the board's as-of date.
    Rates whose series aren't ingested emit no row."""
    if board is None:
        board = load_benchmark_board()
    if not board.rates:
        return []
    wanted = {rd.series_id for rd in board.rates} | {
        rd.benchmark for rd in board.rates if rd.benchmark
    }
    by_series = _group_sorted(r for r in latest_rows if r.get("series_id") in wanted)
    as_of = max((s[-1][0] for s in by_series.values()), default=None)
    if as_of is None:
        return []

    out: list[dict[str, Any]] = []
    for rd in board.rates:
        series = by_series.get(rd.series_id)
        if not series:
            continue
        values = [v for _d, v in series]
        i = len(series) - 1
        latest_d, latest_v = series[i]
        prior_v = values[i - 1] if i > 0 else None

        back = i - board.trend_window
        trend = None
        if back >= 0:
            delta = latest_v - values[back]
            if abs(delta) <= TREND_EPSILON:
                trend = "flat"
            else:
                trend = "rising" if delta > 0 else "falling"
        regime = {"rising": "tightening", "falling": "easing", "flat": "stable"}.get(
            trend
        )

        spread_bps = None
        if rd.benchmark:
            bench = by_series.get(rd.benchmark)
            if bench:
                bdates = [d for d, _v in bench]
                pos = bisect_right(bdates, latest_d) - 1
                if pos >= 0:
                    spread_bps = (latest_v - bench[pos][1]) * 100.0

        means, stds = _expanding_mean_std(values)
        out.append(
            {
                "series_id": rd.series_id,
                "rate_label": rd.label,
                "rate_category": rd.category,
                "benchmark_series": rd.benchmark or None,
                "as_of_date": as_of.isoformat(),
                "latest_date": latest_d.isoformat(),
                "latest_value": latest_v,
                "prior_value": prior_v,
                "change_bps": ((latest_v - prior_v) * 100.0)
                if prior_v is not None
                else None,
                "trend": trend,
                "spread_to_benchmark_bps": spread_bps,
                "zscore": ((latest_v - means[i]) / stds[i]) if stds[i] else None,
                "percentile": _expanding_percentile(values)[i],
                "regime": regime,
                "staleness_days": (as_of - latest_d).days,
            }
        )
    return out


# ---- FUND funding tape + stress gauge -------------------------------------------

# Gauge mapping: stress_score = clamp(50 + STRESS_Z_SCALE * composite_z, 0, 100).
STRESS_Z_SCALE = 20.0
STRESS_BUCKETS = ((40.0, "calm"), (60.0, "normal"), (80.0, "elevated"))


def _stress_bucket(score: float) -> str:
    for bound, label in STRESS_BUCKETS:
        if score < bound:
            return label
    return "stressed"


def compute_funding_features(
    latest_rows: Iterable[dict[str, Any]],
    cfg: FundingConfig | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """The FUND surfaces from ``config/funding.yml``.

    Returns ``tape`` (one row per metric × date: corridor rates and balances
    as configured, plus each funding spread on the dates both legs print, all
    with expanding PIT-safe z-score/percentile) and ``stress`` (one row per
    date where **every** stress component spread has a value:
    ``composite_z`` = weighted mean of the component spreads' expanding
    z-scores, mapped to the 0–100 ``stress_score`` and bucketed calm/normal/
    elevated/stressed). Metrics whose series aren't ingested emit no rows."""
    if cfg is None:
        cfg = load_funding_config()
    if not cfg.metrics and not cfg.spreads:
        return {"tape": [], "stress": []}
    wanted = {m.series_id for m in cfg.metrics} | {
        s for sp in cfg.spreads for s in (sp.long_leg, sp.short_leg)
    }
    by_series = _group_sorted(r for r in latest_rows if r.get("series_id") in wanted)

    tape: list[dict[str, Any]] = []
    # spread name -> {date_iso: zscore} for the gauge
    spread_z: dict[str, dict[str, float | None]] = {}

    def _emit(name: str, metric_type: str, series: list[tuple[date, float]]) -> None:
        values = [v for _d, v in series]
        means, stds = _expanding_mean_std(values)
        pcts = _expanding_percentile(values)
        zmap: dict[str, float | None] = {}
        for i, (d, v) in enumerate(series):
            z = ((v - means[i]) / stds[i]) if stds[i] else None
            zmap[d.isoformat()] = z
            tape.append(
                {
                    "metric_name": name,
                    "metric_type": metric_type,
                    "observation_date": d.isoformat(),
                    "value": v,
                    "zscore": z,
                    "percentile": pcts[i],
                }
            )
        if metric_type == "spread":
            spread_z[name] = zmap

    for m in cfg.metrics:
        series = by_series.get(m.series_id)
        if series:
            _emit(m.name, m.metric_type, series)
    for sp in cfg.spreads:
        long_s, short_s = by_series.get(sp.long_leg), by_series.get(sp.short_leg)
        if not long_s or not short_s:
            continue
        short_map = {d: v for d, v in short_s}
        series = [(d, v - short_map[d]) for d, v in long_s if d in short_map]
        if series:
            _emit(sp.name, "spread", series)

    stress: list[dict[str, Any]] = []
    if cfg.stress_components and all(
        c.spread in spread_z for c in cfg.stress_components
    ):
        common = set.intersection(
            *(set(spread_z[c.spread]) for c in cfg.stress_components)
        )
        total_w = sum(c.weight for c in cfg.stress_components)
        for d in sorted(common):
            # An early observation with no z yet (expanding std = 0) is
            # neutral, not missing — it contributes 0 to the composite.
            composite = (
                sum(
                    c.weight * (spread_z[c.spread][d] or 0.0)
                    for c in cfg.stress_components
                )
                / total_w
            )
            score = min(100.0, max(0.0, 50.0 + STRESS_Z_SCALE * composite))
            stress.append(
                {
                    "observation_date": d,
                    "composite_z": composite,
                    "stress_score": score,
                    "stress_bucket": _stress_bucket(score),
                    "n_components": len(cfg.stress_components),
                }
            )
    return {"tape": tape, "stress": stress}


# ---- CRDT credit spreads ---------------------------------------------------------


def compute_credit_spread_daily(
    latest_rows: Iterable[dict[str, Any]],
    cfg: CreditConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.credit_spread_daily``: OAS history per configured instrument
    (``config/credit.yml``; FRED publishes ICE BofA OAS in percent —
    ``oas_bps`` is ×100) with change vs. prior print, expanding (PIT-safe)
    z-score/percentile, a stress-episode flag (expanding percentile at/above
    ``stress_percentile``), and the NBER recession overlay (``None`` until
    USREC is ingested). Instruments whose series aren't ingested emit no rows."""
    if cfg is None:
        cfg = load_credit_config()
    if not cfg.instruments:
        return []
    by_series = _group_sorted(
        r
        for r in latest_rows
        if r.get("series_id") in {c.series_id for c in cfg.instruments}
    )
    flags = _recession_flags(latest_rows)

    out: list[dict[str, Any]] = []
    for cd in cfg.instruments:
        series = by_series.get(cd.series_id)
        if not series:
            continue
        values = [v for _d, v in series]
        means, stds = _expanding_mean_std(values)
        pcts = _expanding_percentile(values)
        for i, (d, v) in enumerate(series):
            pct = pcts[i]
            out.append(
                {
                    "instrument": cd.instrument,
                    "series_id": cd.series_id,
                    "category": cd.category,
                    "observation_date": d.isoformat(),
                    "oas_pct": v,
                    "oas_bps": v * 100.0,
                    "change_bps": ((v - values[i - 1]) * 100.0) if i > 0 else None,
                    "zscore": ((v - means[i]) / stds[i]) if stds[i] else None,
                    "percentile": pct,
                    "is_stress_episode": (
                        (pct >= cfg.stress_percentile) if pct is not None else None
                    ),
                    "is_recession": _recession_at(flags, d),
                }
            )
    return sorted(out, key=lambda r: (r["instrument"], r["observation_date"]))


def resume_credit_spread_daily(
    instrument: str,
    series_id: str,
    category: str,
    stress_percentile: float,
    new_points: list[tuple[date, float]],
    state: dict[str, Any] | None,
    flags: list[tuple[date, bool]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """spec003 Phase 3: checkpointed/resumable per-instrument body of
    :func:`compute_credit_spread_daily`, for one instrument at a time.

    ``new_points`` must already be exactly the ``(date, oas_pct)`` pairs past
    this instrument's checkpoint frontier, sorted by date. ``state`` is
    ``None`` for an instrument with no checkpoint yet. Beyond the two shared
    expanding primitives, ``change_bps`` needs the single prior observation's
    value carried across a resume -- the one piece of state this engine adds.
    """
    if state is None:
        mean_std_state = init_expanding_mean_std_state()
        pct_state = init_expanding_percentile_state()
        last_value: float | None = None
    else:
        mean_std_state = state["mean_std"]
        pct_state = state["percentile"]
        last_value = state["last_value"]

    values = [v for _d, v in new_points]
    mean_std_state, means, stds = resume_expanding_mean_std(mean_std_state, values)
    pct_state, pcts = resume_expanding_percentile(pct_state, values)

    out: list[dict[str, Any]] = []
    prev = last_value
    for i, (d, v) in enumerate(new_points):
        pct = pcts[i]
        out.append(
            {
                "instrument": instrument,
                "series_id": series_id,
                "category": category,
                "observation_date": d.isoformat(),
                "oas_pct": v,
                "oas_bps": v * 100.0,
                "change_bps": ((v - prev) * 100.0) if prev is not None else None,
                "zscore": ((v - means[i]) / stds[i]) if stds[i] else None,
                "percentile": pct,
                "is_stress_episode": (
                    (pct >= stress_percentile) if pct is not None else None
                ),
                "is_recession": _recession_at(flags, d),
            }
        )
        prev = v
    new_state = {
        "mean_std": mean_std_state,
        "percentile": pct_state,
        "last_value": prev,
    }
    return new_state, out


# ---- INFL inflation explorer -------------------------------------------------


def _month_index(d: date) -> int:
    return d.year * 12 + (d.month - 1)


def compute_inflation_explorer(
    latest_rows: Iterable[dict[str, Any]],
    items: Iterable[InflationItemDef] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """The INFL surfaces from ``config/inflation_items.yml``.

    Returns ``explorer`` (one row per item × month: index level, MoM %, YoY %,
    ΔMoM/ΔYoY acceleration, trailing-3-month annualized rate, the item's
    relative-importance weight, and ``weight × MoM`` contribution in headline
    percentage points) and ``contribution`` (the waterfall: per month and
    tree, one row per ``waterfall: true`` item ranked by contribution, plus an
    ``is_headline_total`` row carrying the headline's own MoM in pp).

    All month arithmetic is calendar-based (``year × 12 + month``), so a
    publication gap yields nulls rather than comparing the wrong months.
    Items whose series aren't ingested emit no rows; the waterfall for a month
    appears only when the tree's headline printed that month.
    """
    if items is None:
        items = load_inflation_items()
    item_list = list(items)
    if not item_list:
        return {"explorer": [], "contribution": []}
    by_series = _group_sorted(
        r for r in latest_rows if r.get("series_id") in {i.series_id for i in item_list}
    )

    explorer: list[dict[str, Any]] = []
    # (basket, sa_nsa) -> {month_index: {...}} for the waterfall pass
    mom_by_tree: dict[tuple[str, str], dict[str, dict[int, Any]]] = {}

    for item in item_list:
        series = by_series.get(item.series_id)
        if not series:
            continue
        # month_index -> (date, value); ascending input, last obs in a month wins
        monthly: dict[int, tuple[date, float]] = {
            _month_index(d): (d, v) for d, v in series
        }
        mom_map: dict[int, float | None] = {}
        for m in monthly:
            prev = monthly.get(m - 1)
            mom_map[m] = _pct_change(monthly[m][1], prev[1]) if prev else None
        tree = mom_by_tree.setdefault((item.basket, item.sa_nsa), {})
        tree[item.series_id] = mom_map

        for m in sorted(monthly):
            d, v = monthly[m]
            year_ago = monthly.get(m - 12)
            three_back = monthly.get(m - 3)
            prev_mom, cur_mom = mom_map.get(m - 1), mom_map[m]
            yoy = _pct_change(v, year_ago[1]) if year_ago else None
            prev_yoy = None
            if monthly.get(m - 1) and monthly.get(m - 13):
                prev_yoy = _pct_change(monthly[m - 1][1], monthly[m - 13][1])
            explorer.append(
                {
                    "series_id": item.series_id,
                    "item_label": item.label,
                    "parent_item": item.parent or None,
                    "hierarchy_level": item.level,
                    "basket": item.basket,
                    "sa_nsa": item.sa_nsa,
                    "observation_date": d.isoformat(),
                    "index_value": v,
                    "mom_pct": cur_mom,
                    "yoy_pct": yoy,
                    "mom_accel": (
                        cur_mom - prev_mom
                        if cur_mom is not None and prev_mom is not None
                        else None
                    ),
                    "yoy_accel": (
                        yoy - prev_yoy
                        if yoy is not None and prev_yoy is not None
                        else None
                    ),
                    "three_month_annualized": (
                        (v / three_back[1]) ** 4 - 1
                        if three_back and three_back[1] > 0 and v > 0
                        else None
                    ),
                    "weight": item.weight,
                    "contribution_pp": (
                        item.weight * cur_mom
                        if item.weight is not None and cur_mom is not None
                        else None
                    ),
                }
            )

    # Waterfall: per tree × month where the headline printed, the waterfall
    # items' contributions ranked largest-first, plus the headline-total row.
    contribution: list[dict[str, Any]] = []
    items_by_tree: dict[tuple[str, str], list[InflationItemDef]] = {}
    for item in item_list:
        items_by_tree.setdefault((item.basket, item.sa_nsa), []).append(item)
    for key in sorted(items_by_tree):
        basket, sa_nsa = key
        tree_items = items_by_tree[key]
        head = next((i for i in tree_items if i.level == 0), None)
        wf = [i for i in tree_items if i.waterfall]
        head_moms = mom_by_tree.get(key, {}).get(head.series_id, {}) if head else {}
        for m in sorted(head_moms):
            head_mom = head_moms[m]
            if head_mom is None:
                continue
            d = date(m // 12, m % 12 + 1, 1)
            rows = []
            for i in wf:
                mom = mom_by_tree[key].get(i.series_id, {}).get(m)
                if mom is None:
                    continue
                rows.append(
                    {
                        "observation_date": d.isoformat(),
                        "basket": basket,
                        "sa_nsa": sa_nsa,
                        "series_id": i.series_id,
                        "item_label": i.label,
                        "contribution_pp": i.weight * mom,
                        "rank_in_month": 0,
                        "is_headline_total": False,
                    }
                )
            rows.sort(key=lambda r: -r["contribution_pp"])
            for rank, r in enumerate(rows, start=1):
                r["rank_in_month"] = rank
            contribution.extend(rows)
            contribution.append(
                {
                    "observation_date": d.isoformat(),
                    "basket": basket,
                    "sa_nsa": sa_nsa,
                    "series_id": head.series_id,
                    "item_label": head.label,
                    "contribution_pp": head_mom * 100.0,  # headline MoM in pp
                    "rank_in_month": None,
                    "is_headline_total": True,
                }
            )
    return {"explorer": explorer, "contribution": contribution}


# ---- rolling-window stats companions -------------------------------------------

# Trailing observation-count windows (~trading-day horizons: day, week,
# 2 weeks, month, quarter, half-year, year).
ROLLING_WINDOWS = (1, 5, 10, 21, 63, 126, 252)


def _rolling_window_rows(
    series: list[tuple[date, float]],
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """Per observation × window: trailing change, percent change, and rolling
    z-score, computed with prefix sums (O(n × windows), not O(n × w)).

    A (observation, window) row is emitted only once the window is **fully
    populated** (observation index ≥ w) — no partial-window stats. ``change``
    is ``v_t − v_{t−w}`` in the series' native units; ``pct_change`` is
    relative to ``v_{t−w}`` (``None`` at a zero base); ``zscore`` is against
    the trailing-w rolling mean/std *including* the current value (``None``
    when the window std is 0 — always the case for w=1). Windows are
    observation counts, so for daily series they approximate trading-day
    horizons; the stats are trailing-only (point-in-time safe).
    """
    n = len(series)
    values = [v for _d, v in series]
    s = [0.0] * (n + 1)  # prefix sums of x and x²
    s2 = [0.0] * (n + 1)
    for i, v in enumerate(values):
        s[i + 1] = s[i] + v
        s2[i + 1] = s2[i] + v * v

    out: list[dict[str, Any]] = []
    for i in range(n):
        d, v = series[i]
        for w in windows:
            if i < w:
                continue
            base = values[i - w]
            mean = (s[i + 1] - s[i + 1 - w]) / w
            var = max((s2[i + 1] - s2[i + 1 - w]) / w - mean * mean, 0.0)
            std = var**0.5
            out.append(
                {
                    "observation_date": d.isoformat(),
                    "window": w,
                    "value": v,
                    "change": v - base,
                    "pct_change": ((v - base) / base) if base != 0 else None,
                    "zscore": ((v - mean) / std) if std > 1e-12 else None,
                }
            )
    return out


def compute_curve_spread_rolling(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Iterable[SpreadDef] | None = None,
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.curve_spread_rolling``: rolling-window companions to
    ``curve_spread_daily`` — per configured spread/ratio, the trailing change,
    percent change, and rolling z-score over each window. ``value``/``change``
    are in the spread's native units (percent points for rate spreads); note
    ``pct_change`` on a spread that crosses zero is of limited meaning and is
    provided for uniformity."""
    if spreads is None:
        spreads = load_spread_defs()
    base = compute_curve_spreads(latest_rows, spreads)
    by_name: dict[str, list[tuple[date, float]]] = {}
    for r in base:
        d = _parse(r["observation_date"])
        if d is not None:
            by_name.setdefault(r["spread_name"], []).append((d, r["value"]))
    out: list[dict[str, Any]] = []
    for name in sorted(by_name):
        series = sorted(by_name[name], key=lambda t: t[0])
        for row in _rolling_window_rows(series, windows):
            out.append({"spread_name": name, **row})
    return out


def compute_credit_spread_rolling(
    latest_rows: Iterable[dict[str, Any]],
    cfg: CreditConfig | None = None,
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.credit_spread_rolling``: rolling-window companions to
    ``credit_spread_daily`` — per configured OAS instrument, over the spread
    in **bps** (credit convention), so ``change`` is a bps move."""
    if cfg is None:
        cfg = load_credit_config()
    by_series = _group_sorted(
        r
        for r in latest_rows
        if r.get("series_id") in {c.series_id for c in cfg.instruments}
    )
    out: list[dict[str, Any]] = []
    for cd in cfg.instruments:
        series = by_series.get(cd.series_id)
        if not series:
            continue
        bps = [(d, v * 100.0) for d, v in series]
        for row in _rolling_window_rows(bps, windows):
            out.append(
                {
                    "instrument": cd.instrument,
                    "series_id": cd.series_id,
                    "observation_date": row["observation_date"],
                    "window": row["window"],
                    "oas_bps": row["value"],
                    "change_bps": row["change"],
                    "pct_change": row["pct_change"],
                    "zscore": row["zscore"],
                }
            )
    return out


def compute_treasury_curve_rolling(
    latest_rows: Iterable[dict[str, Any]],
    tenors: Iterable[TenorDef] | None = None,
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.treasury_curve_rolling``: rolling-window companions to
    ``treasury_curve`` — per tenor, over the constant-maturity yield in
    percent, so ``change`` is a percent-point move (×100 for bps)."""
    if tenors is None:
        tenors = load_curve_defs()
    tenor_list = sorted(tenors, key=lambda t: t.months)
    by_series = _group_sorted(
        r
        for r in latest_rows
        if r.get("series_id") in {t.series_id for t in tenor_list}
    )
    out: list[dict[str, Any]] = []
    for t in tenor_list:
        series = by_series.get(t.series_id)
        if not series:
            continue
        for row in _rolling_window_rows(series, windows):
            out.append(
                {
                    "tenor_label": t.label,
                    "tenor_months": t.months,
                    "series_id": t.series_id,
                    "observation_date": row["observation_date"],
                    "window": row["window"],
                    "yield_pct": row["value"],
                    "change": row["change"],
                    "pct_change": row["pct_change"],
                    "zscore": row["zscore"],
                }
            )
    return out


def compute_release_calendar(
    release_dates: Iterable[dict[str, Any]],
    entries: Iterable[ReleaseCalendarEntry] | None = None,
    *,
    fetched_at: str | None = None,
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    """``gold.release_calendar`` (terminal module CAL): the curated releases
    from ``config/release_calendar.yml``, filtered from a raw FRED
    ``releases/dates`` pull (:meth:`FredClient.get_release_dates`).

    Unlike every other table in this module, the input isn't a Silver
    observation history — it's a live snapshot of FRED's forward release
    schedule, so ``fetched_at`` stamps when it was pulled (staleness marker)
    and ``is_future`` is computed relative to ``as_of`` (defaults to today).
    Rows for releases not in the curated config are dropped; multiple raw
    rows for the same ``(release_id, date)`` collapse to one.
    """
    if entries is None:
        entries = load_release_calendar_config()
    by_id = {e.release_id: e for e in entries}
    if not by_id:
        return []

    stamp = fetched_at or datetime.now(timezone.utc).isoformat()
    today = as_of or datetime.now(timezone.utc).date()

    seen: set[tuple[int, str]] = set()
    out: list[dict[str, Any]] = []
    for row in release_dates:
        release_id = row.get("release_id")
        entry = by_id.get(release_id)
        if entry is None:
            continue
        release_date = row.get("date")
        if not release_date:
            continue
        key = (release_id, release_date)
        if key in seen:
            continue
        seen.add(key)
        try:
            is_future = date.fromisoformat(release_date) >= today
        except ValueError:
            is_future = None
        out.append(
            {
                "release_id": entry.release_id,
                "release_name": entry.release_name,
                "release_date": release_date,
                "importance": entry.importance,
                "econ_category": entry.econ_category,
                "representative_series_id": entry.representative_series_id,
                "is_future": is_future,
                "fetched_at": stamp,
            }
        )
    return sorted(out, key=lambda r: (r["release_date"], r["release_id"]))


def _last_on_or_before(
    series: list[tuple[date, float]],
    as_of: date,
) -> float | None:
    """Latest value at or before ``as_of`` from a ``_group_sorted`` series."""
    pos = bisect_right(series, (as_of, float("inf"))) - 1
    return series[pos][1] if pos >= 0 else None


def _zero_yield_at_months(
    curve: list[tuple[int, float]],
    months: float,
) -> float | None:
    """Interpolate the zero-coupon yield (percent) at ``months`` from a
    ``(tenor_months, yield_pct)`` curve sorted ascending by tenor. Clamps to
    the nearest tenor's yield beyond either end (flat extrapolation)."""
    if not curve:
        return None
    if months <= curve[0][0]:
        return curve[0][1]
    if months >= curve[-1][0]:
        return curve[-1][1]
    for (m1, y1), (m2, y2) in pairwise(curve):
        if m1 <= months <= m2:
            if m2 == m1:
                return y1
            frac = (months - m1) / (m2 - m1)
            return y1 + frac * (y2 - y1)
    return curve[-1][1]  # pragma: no cover - unreachable given the bounds above


def _fomc_outcome_ladder(
    rate_before: float,
    rate_after: float,
    step_bps: int,
    n_outcomes: int = 7,
) -> dict[float, float]:
    """Distribute probability across a ``step_bps`` ladder around
    ``rate_before`` (percent), splitting the two bracketing rungs around
    ``rate_after`` by linear interpolation.

    Ported from ``multi_outcome_distribution`` in the sibling
    ``market_terminal`` project's reference engine
    (``macro_data_etl/src/analytics/fed_probability.py``) — the distribution
    math is source-agnostic (it only needs ``rate_before``/``rate_after``,
    not a futures price), so it carries over unchanged from the CME-based
    original; only how ``rate_after`` is derived differs (see
    :func:`compute_fomc_probability`).
    """
    step = step_bps / 100
    mid_idx = n_outcomes // 2
    outcomes = [round(rate_before + (i - mid_idx) * step, 4) for i in range(n_outcomes)]
    probs = {o: 0.0 for o in outcomes}

    if rate_after <= outcomes[0]:
        probs[outcomes[0]] = 1.0
    elif rate_after >= outcomes[-1]:
        probs[outcomes[-1]] = 1.0
    else:
        for lo, hi in pairwise(outcomes):
            if lo <= rate_after <= hi:
                p_upper = (rate_after - lo) / (hi - lo)
                probs[lo] += 1.0 - p_upper
                probs[hi] += p_upper
                break

    return {k: round(v, 6) for k, v in probs.items() if v > 0.0001}


def compute_fomc_probability(
    latest_rows: Iterable[dict[str, Any]],
    cfg: FOMCConfig | None = None,
    *,
    as_of: date | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """``gold.fomc_probability`` + ``gold.fomc_meeting_path`` (terminal module
    FOMC): CME-FedWatch-style meeting hike/cut/hold odds, computed WITHOUT a
    CME feed (locked decision, ``docs/handoffs/terminal_phase0_gaps.md`` item
    3) — derived entirely from FRED short-rate/target series already
    ingested.

    The distribution math (:func:`_fomc_outcome_ladder`) and the meeting-
    chaining loop (each meeting's ``rate_before`` = the previous meeting's
    resolved ``expected_rate``) are ported from the reference futures-based
    engine. What replaces the futures price: the short end of the Treasury
    curve (``config/fomc.yml`` ``tenors``) is treated as a zero-coupon curve
    and bootstrapped for the implied *incremental* forward rate between each
    pair of consecutive meeting horizons — ``f`` s.t. ``(1+y1)**t1 *
    (1+f)**(t2-t1) == (1+y2)**t2`` — which plays the same role as the
    reference engine's day-weighted implied post-meeting rate, without
    needing a futures settlement price. The first meeting's window runs from
    ``as_of`` (t=0) to the first meeting, so it degenerates to the curve's
    own yield at that horizon; ``rate_before`` for the first meeting is the
    current effective rate (``effective_rate_series``).

    Only meetings on or after ``as_of`` are included (a resolved meeting has
    no probability distribution left to compute).
    """
    if cfg is None:
        cfg = load_fomc_config()
    if cfg is None:
        return {"probability": [], "meeting_path": []}

    today = as_of or datetime.now(timezone.utc).date()
    by_series = _group_sorted(latest_rows)

    curve: list[tuple[int, float]] = []
    for tenor in cfg.tenors:
        y = _last_on_or_before(by_series.get(tenor.series_id, []), today)
        if y is not None:
            curve.append((tenor.tenor_months, y))
    curve.sort(key=lambda p: p[0])
    if len(curve) < 2:
        return {"probability": [], "meeting_path": []}

    effr = _last_on_or_before(by_series.get(cfg.effective_rate_series, []), today)
    target_low = _last_on_or_before(by_series.get(cfg.target_low_series, []), today)
    target_high = _last_on_or_before(by_series.get(cfg.target_high_series, []), today)
    if effr is None:
        if target_low is None or target_high is None:
            return {"probability": [], "meeting_path": []}
        effr = (target_low + target_high) / 2
    target_lower_bps = round(target_low * 100) if target_low is not None else None
    target_upper_bps = round(target_high * 100) if target_high is not None else None

    meetings = [d for d in cfg.meeting_dates if d >= today]
    if not meetings:
        return {"probability": [], "meeting_path": []}

    n_inputs = len(curve)
    model_vintage = today.isoformat()
    rate_before = effr
    prev_t, prev_y = 0.0, effr
    cumulative_move_bps = 0.0

    probability_rows: list[dict[str, Any]] = []
    path_rows: list[dict[str, Any]] = []
    for meeting_date in meetings:
        t = (meeting_date - today).days / 365.0
        months = (meeting_date - today).days / 30.4375
        y = _zero_yield_at_months(curve, months)
        if y is None:
            break
        if t - prev_t < 1e-6:
            rate_after = y
        else:
            rate_after = (
                ((1 + y / 100) ** t / (1 + prev_y / 100) ** prev_t)
                ** (1 / (t - prev_t))
                - 1
            ) * 100

        distribution = _fomc_outcome_ladder(
            rate_before, rate_after, cfg.bucket_step_bps
        )
        expected = sum(rate * prob for rate, prob in distribution.items())
        implied_move_bps = round((expected - rate_before) * 100, 1)
        cumulative_move_bps = round(cumulative_move_bps + implied_move_bps, 1)

        for outcome_rate, prob in sorted(distribution.items()):
            probability_rows.append(
                {
                    "meeting_date": meeting_date.isoformat(),
                    "target_lower_bps": target_lower_bps,
                    "target_upper_bps": target_upper_bps,
                    "outcome_bps": round(outcome_rate * 100),
                    "probability": prob,
                    "model_vintage": model_vintage,
                    "n_inputs": n_inputs,
                }
            )
        path_rows.append(
            {
                "meeting_date": meeting_date.isoformat(),
                "implied_rate": round(expected, 4),
                "implied_move_bps": implied_move_bps,
                "cumulative_move_bps": cumulative_move_bps,
                "model_vintage": model_vintage,
            }
        )

        rate_before = expected
        prev_t, prev_y = t, y

    return {"probability": probability_rows, "meeting_path": path_rows}
