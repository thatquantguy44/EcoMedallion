"""FOMC calendar scraper: parsing, diffing, and YAML rendering.

Spec: ``docs/handoffs/fomc_calendar_scraper.md``.

These tests cover the parser, the differ, the YAML renderer, and the `requests`
fetch backend (intercepted with `responses`, so the suite stays hermetic — no
network). The Selenium backend is deliberately untested: driving a real browser
would make the suite slow, flaky, and driver-dependent, which the rest of this
repo's tests are not.

Two fixtures, deliberately:

``fomccalendars.html`` is a real capture of the live page (2026-09-15) and
carries the main assertions — so these tests now prove the parser matches
real markup, which until that capture they could not.

``fomccalendars_synthetic.html`` is the older hand-built page, kept only for
"(unscheduled)" rows, which have aged off the live calendar. See the banner
inside each file.

One-day meetings are a shape NEITHER fixture contains — every meeting on the
captured page is two-day, and the synthetic page has no one-day row either.
``test_label_shapes`` covers that shape with its own inline HTML.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest
import responses

from fred_pipeline.catalogs.fomc_calendar import (
    FOMC_CALENDAR_URL,
    FOMCMeeting,
    FOMCScrapeError,
    diff_against_config,
    fetch_calendar_html,
    fetch_calendar_html_requests,
    format_yaml_block,
    parse_fomc_calendar,
)

FIXTURE = Path(__file__).parent / "fixtures" / "fomccalendars.html"
SYNTHETIC_FIXTURE = Path(__file__).parent / "fixtures" / "fomccalendars_synthetic.html"


@pytest.fixture(scope="module")
def meetings() -> list[FOMCMeeting]:
    """Parsed from the real captured page."""
    return parse_fomc_calendar(FIXTURE.read_text(encoding="utf-8"), warn=False)


@pytest.fixture(scope="module")
def synthetic_meetings() -> list[FOMCMeeting]:
    """Parsed from the hand-built page, for shapes the live capture lacks."""
    return parse_fomc_calendar(
        SYNTHETIC_FIXTURE.read_text(encoding="utf-8"), warn=False
    )


# ---- parsing ----------------------------------------------------------------


def test_parses_every_scheduled_meeting(meetings):
    """The real page: seven complete years at the Fed's 8/year cadence, plus
    the single 2028 meeting that only the advance-notice sentence announces."""
    assert len(meetings) == 57
    per_year = {}
    for m in meetings:
        per_year[m.decision_date.year] = per_year.get(m.decision_date.year, 0) + 1
    assert per_year == {
        2021: 8,
        2022: 8,
        2023: 8,
        2024: 8,
        2025: 8,
        2026: 8,
        2027: 8,
        2028: 1,
    }


def test_decision_date_is_the_second_day_of_a_two_day_meeting(meetings):
    """The rule the whole tool exists to get right: the statement lands on
    day two, so day two is the date the model chains between."""
    january = next(m for m in meetings if m.decision_date == date(2027, 1, 27))
    assert january.start_date == date(2027, 1, 26)
    assert january.is_two_day


def test_month_boundary_meeting_decides_in_the_second_month(meetings):
    """'Apr/May 30-1' decides on 1 MAY. A naive parser reads the first month
    for both days and silently produces 1 April -- a month early.

    All three of these are real rows on the captured page.
    """
    apr_may = next(m for m in meetings if m.decision_date == date(2024, 5, 1))
    assert apr_may.start_date == date(2024, 4, 30)

    # Both of these also cross a YEAR-adjacent month boundary within the panel.
    jan_feb = next(m for m in meetings if m.decision_date == date(2023, 2, 1))
    assert jan_feb.start_date == date(2023, 1, 31)

    oct_nov = next(m for m in meetings if m.decision_date == date(2023, 11, 1))
    assert oct_nov.start_date == date(2023, 10, 31)


def test_projection_asterisk_is_captured(meetings):
    sep = next(m for m in meetings if m.decision_date == date(2027, 3, 17))
    assert sep.is_projection_meeting
    plain = next(m for m in meetings if m.decision_date == date(2027, 1, 27))
    assert not plain.is_projection_meeting
    # The Fed runs four SEP meetings a year across the seven complete years.
    assert sum(1 for m in meetings if m.is_projection_meeting) == 28


def test_advance_notice_meeting_is_parsed_with_its_stated_year(meetings):
    """The furthest-out meeting is announced in prose BELOW the last year
    panel -- 'A two-day meeting is scheduled for January 25-26, 2028.' -- and
    is the only place that date appears. Reading it with the enclosing panel's
    year instead of its own produced a phantom 2027-01-26 meeting.
    """
    assert date(2028, 1, 26) in {m.decision_date for m in meetings}
    assert date(2027, 1, 26) not in {m.decision_date for m in meetings}


def test_page_footer_is_not_mistaken_for_a_meeting(meetings):
    """The page ends with 'Last Update: August 19, 2026'. Matching a month and
    a number anywhere in a line turned that stamp into an August 2027 meeting
    -- a date the Fed has never scheduled, in a month it does not meet in.
    """
    assert date(2027, 8, 19) not in {m.decision_date for m in meetings}
    assert not [m for m in meetings if m.decision_date.month == 8]


def test_notation_vote_is_skipped(meetings):
    """A notation vote is not a rate decision."""
    labels = " ".join(m.raw_label.lower() for m in meetings)
    assert "notation" not in labels


def test_unscheduled_meetings_are_skipped(synthetic_meetings):
    """Emergency inter-meeting cuts are not scheduled decisions, and the
    probability engine models the scheduled path.

    Synthetic fixture: the 2020 emergency cuts have aged off the live page.
    """
    dates = {m.decision_date for m in synthetic_meetings}
    assert date(2020, 3, 3) not in dates
    assert date(2020, 3, 15) not in dates
    # ... while the scheduled meetings in that same panel survive
    assert date(2020, 1, 29) in dates
    assert date(2020, 4, 29) in dates


def test_publication_dates_are_not_mistaken_for_meetings(meetings):
    """'Minutes: PDF | HTML (Released February 18, 2026)' must not parse as a
    meeting on 18 February. Real markup -- the captured page is full of these.
    """
    dates = {m.decision_date for m in meetings}
    assert date(2026, 2, 18) not in dates
    assert date(2026, 4, 8) not in dates


def test_results_are_sorted_and_deduplicated(meetings):
    dates = [m.decision_date for m in meetings]
    assert dates == sorted(dates)
    assert len(dates) == len(set(dates))


@pytest.mark.parametrize(
    "label,year,expected",
    [
        ("January 27-28", 2026, date(2026, 1, 28)),
        ("March 3", 2026, date(2026, 3, 3)),  # one-day meeting
        ("November 4-5*", 2026, date(2026, 11, 5)),  # projections
        ("April/May 30-1", 2026, date(2026, 5, 1)),  # month boundary
        ("Dec/Jan 31-1", 2026, date(2027, 1, 1)),  # YEAR boundary
    ],
)
def test_label_shapes(label, year, expected):
    html = (
        f'<h4>{year} FOMC Meetings</h4><div class="panel-body">'
        f'<div class="row"><div>{label}</div></div></div>'
    )
    (meeting,) = parse_fomc_calendar(html, warn=False)
    assert meeting.decision_date == expected


def test_month_and_days_in_sibling_elements():
    """The live page splits them, which is why the parser is line-based rather
    than element-based."""
    html = (
        '<h4>2027 FOMC Meetings</h4><div class="panel-body">'
        '<div class="row fomc-meeting">'
        '<div class="fomc-meeting__month"><strong>September</strong></div>'
        '<div class="fomc-meeting__date">14-15*</div>'
        "</div></div>"
    )
    (meeting,) = parse_fomc_calendar(html, warn=False)
    assert meeting.decision_date == date(2027, 9, 15)
    assert meeting.is_projection_meeting


# ---- failure behaviour ------------------------------------------------------


@pytest.mark.parametrize(
    "html", ["", "   ", "<html><body><p>Nothing here</p></body></html>"]
)
def test_unparseable_page_raises_rather_than_returning_empty(html):
    """An empty list would read as 'no meetings scheduled' and silently shorten
    the modelled policy path. A structure change must be loud."""
    with pytest.raises(FOMCScrapeError):
        parse_fomc_calendar(html, warn=False)


def test_importing_the_module_does_not_import_selenium():
    """Selenium is an optional extra for this tool, not a pipeline dependency.

    Checked in a subprocess: selenium may well be installed in the developer's
    environment and already imported by something else in this one, so an
    in-process ``sys.modules`` check would prove nothing.
    """
    import subprocess
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    probe = (
        "import sys; import fred_pipeline.catalogs.fomc_calendar; "
        "print('selenium' in sys.modules)"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        cwd=repo_root,
        env={"PYTHONPATH": str(repo_root / "src"), "PATH": os.environ.get("PATH", "")},
        check=True,
    )
    assert result.stdout.strip() == "False", (
        "importing fomc_calendar pulled in selenium; the import must stay "
        "inside fetch_calendar_html so the module works without the extra"
    )


# ---- fetch backends ---------------------------------------------------------
# The requests backend IS tested -- `responses` intercepts HTTP, so these stay
# hermetic. The Selenium backend is not: driving a real browser would make the
# suite slow, flaky and driver-dependent.


@responses.activate
def test_requests_backend_returns_the_page_body():
    responses.add(responses.GET, FOMC_CALENDAR_URL, body="<html>ok</html>", status=200)
    assert fetch_calendar_html_requests(FOMC_CALENDAR_URL) == "<html>ok</html>"


@responses.activate
def test_requests_backend_identifies_itself_honestly():
    """The User-Agent says what the tool is rather than impersonating a
    browser, and points at the repo so an admin can see who is calling."""
    responses.add(responses.GET, FOMC_CALENDAR_URL, body="<html>ok</html>", status=200)
    fetch_calendar_html_requests(FOMC_CALENDAR_URL)
    sent = responses.calls[0].request.headers["User-Agent"]
    assert "fomc-calendar-scraper" in sent
    assert "Mozilla" not in sent


@responses.activate
@pytest.mark.parametrize("status", [403, 404, 500, 503])
def test_requests_backend_raises_on_http_error(status):
    responses.add(responses.GET, FOMC_CALENDAR_URL, body="nope", status=status)
    with pytest.raises(FOMCScrapeError, match="HTTP GET"):
        fetch_calendar_html_requests(FOMC_CALENDAR_URL)


@responses.activate
def test_requests_backend_raises_on_empty_body():
    responses.add(responses.GET, FOMC_CALENDAR_URL, body="   ", status=200)
    with pytest.raises(FOMCScrapeError, match="empty body"):
        fetch_calendar_html_requests(FOMC_CALENDAR_URL)


@responses.activate
def test_auto_backend_uses_requests_when_it_works():
    """Selenium must not be touched on the happy path -- if it were, the
    scraper would need a driver for a page that is plain static HTML."""
    responses.add(responses.GET, FOMC_CALENDAR_URL, body="<html>ok</html>", status=200)
    assert fetch_calendar_html(FOMC_CALENDAR_URL, backend="auto") == "<html>ok</html>"
    assert len(responses.calls) == 1


@responses.activate
def test_auto_backend_falls_back_to_selenium_and_reports_both_failures(monkeypatch):
    responses.add(responses.GET, FOMC_CALENDAR_URL, status=503)

    def _boom(*_args, **_kwargs):
        raise FOMCScrapeError("driver unavailable")

    monkeypatch.setattr(
        "fred_pipeline.catalogs.fomc_calendar.fetch_calendar_html_selenium", _boom
    )
    with pytest.raises(FOMCScrapeError) as excinfo:
        fetch_calendar_html(FOMC_CALENDAR_URL, backend="auto")

    message = str(excinfo.value)
    # Both causes must survive: the requests failure is usually the
    # informative one, and hiding it sends you debugging the wrong layer.
    assert "requests:" in message and "HTTP GET" in message
    assert "selenium:" in message and "driver unavailable" in message


@responses.activate
def test_explicit_requests_backend_never_falls_back(monkeypatch):
    responses.add(responses.GET, FOMC_CALENDAR_URL, status=500)
    monkeypatch.setattr(
        "fred_pipeline.catalogs.fomc_calendar.fetch_calendar_html_selenium",
        lambda *a, **k: pytest.fail("selenium must not be used for backend='requests'"),
    )
    with pytest.raises(FOMCScrapeError):
        fetch_calendar_html(FOMC_CALENDAR_URL, backend="requests")


def test_unknown_backend_is_rejected():
    with pytest.raises(FOMCScrapeError, match="unknown backend"):
        fetch_calendar_html(FOMC_CALENDAR_URL, backend="curl")


@responses.activate
def test_requests_backend_feeds_the_parser_end_to_end():
    """The two halves fit: fetch over HTTP, parse to meetings, no browser."""
    responses.add(
        responses.GET,
        FOMC_CALENDAR_URL,
        body=FIXTURE.read_text(encoding="utf-8"),
        status=200,
    )
    html = fetch_calendar_html(FOMC_CALENDAR_URL, backend="requests")
    assert len(parse_fomc_calendar(html, warn=False)) == 57


# ---- diffing ----------------------------------------------------------------


def _m(d: date) -> FOMCMeeting:
    return FOMCMeeting(decision_date=d, start_date=None, year=d.year)


def test_diff_reports_meetings_missing_from_config():
    today = date(2026, 8, 13)
    scraped = [_m(date(2026, 9, 16)), _m(date(2026, 12, 9)), _m(date(2027, 1, 27))]
    configured = [date(2026, 9, 16), date(2026, 12, 9)]
    diff = diff_against_config(scraped, configured, today=today)
    assert diff.missing_from_config == (date(2027, 1, 27),)
    assert diff.absent_upstream == ()
    assert not diff.in_sync


def test_diff_reports_configured_dates_the_fed_no_longer_lists():
    today = date(2026, 8, 13)
    diff = diff_against_config(
        [_m(date(2026, 9, 16))],
        [date(2026, 9, 16), date(2026, 11, 4)],
        today=today,
    )
    assert diff.absent_upstream == (date(2026, 11, 4),)


def test_diff_ignores_past_meetings():
    """Past meetings drop off the forward calendar but stay in the config --
    comparing them would report drift on every single run."""
    today = date(2026, 8, 13)
    diff = diff_against_config(
        [_m(date(2026, 9, 16))],
        [date(2026, 1, 28), date(2026, 3, 18), date(2026, 9, 16)],
        today=today,
    )
    assert diff.in_sync


def test_diff_against_the_repo_config(meetings):
    """Sanity check that the two halves fit together; the fixture is synthetic,
    so this asserts the shape of the result, not its contents."""
    diff = diff_against_config(meetings, [], today=date(2026, 8, 13))
    assert all(d >= date(2026, 8, 13) for d in diff.missing_from_config)


# ---- rendering --------------------------------------------------------------


def test_format_yaml_block_is_paste_ready():
    block = format_yaml_block(
        [
            FOMCMeeting(date(2027, 12, 8), None, 2027, is_projection_meeting=True),
            FOMCMeeting(date(2027, 1, 27), None, 2027),
        ]
    )
    assert block.splitlines() == [
        '  - "2027-01-27"',
        '  - "2027-12-08"  # SEP / projections',
    ]


def test_format_yaml_block_parses_back_as_yaml():
    import yaml

    block = format_yaml_block([FOMCMeeting(date(2028, 1, 26), None, 2028)])
    loaded = yaml.safe_load("meeting_dates:\n" + block)
    assert loaded["meeting_dates"] == ["2028-01-26"]
