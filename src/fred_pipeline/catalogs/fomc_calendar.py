"""Scrape the Federal Reserve's FOMC meeting calendar into ``config/fomc.yml``.

Spec: ``docs/handoffs/fomc_calendar_scraper.md``.

``config/fomc.yml`` declares the scheduled FOMC decision dates that
:func:`fred_pipeline.writer.terminal_views.compute_fomc_probability` builds
``gold.fomc_probability`` / ``gold.fomc_meeting_path`` from. That list expires:
the engine filters to ``d >= today``, so once the last configured meeting passes
both tables emit nothing and the Power BI Fed Policy Watch report goes blank
without raising anything. This module turns the refresh into one command.

Layout follows the repo's convention of keeping business logic pure and
importing I/O lazily (as the pipeline does with PySpark):

* :func:`parse_fomc_calendar` and :func:`diff_against_config` are pure — HTML or
  dates in, data out. No network, no browser. Fully unit-tested.
* :func:`fetch_calendar_html` is the only I/O, and it dispatches between two
  backends.

Two fetch backends, because they fail in different ways:

``requests``
    The page is server-rendered static HTML, so a plain GET is enough. Fast,
    no browser, no driver, and ``requests`` is already a core dependency of
    this pipeline. This is the default.
``selenium``
    Renders JavaScript and behaves like a real browser. Slower and needs a
    matching chromedriver/Chrome pair, but it is the answer if the Fed ever
    makes the calendar client-rendered or starts refusing plain HTTP clients.
    Imported inside the function body, so ``import fred_pipeline`` never
    requires selenium to be installed.

``backend="auto"`` (the default) tries ``requests`` and falls back to
``selenium`` — see :func:`fetch_calendar_html`.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date

FOMC_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

# The Fed holds 8 scheduled meetings a year. A parsed year with fewer is not
# necessarily wrong (the current year is partly in the past, the final year is
# often preliminary), but it is worth saying out loud.
EXPECTED_MEETINGS_PER_YEAR = 8
_PARTIAL_YEAR_WARN_THRESHOLD = 6

_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

# Rows that exist on the page but are not scheduled rate decisions.
_SKIP_MARKERS = ("unscheduled", "notation vote", "conference call")


class FOMCScrapeError(RuntimeError):
    """Raised when the calendar cannot be fetched or cannot be parsed.

    Deliberately loud: a scraper that quietly returns fewer meetings than the
    page lists would silently shorten the modelled policy path.
    """


@dataclass(frozen=True)
class FOMCMeeting:
    """One scheduled FOMC meeting.

    ``decision_date`` is the LAST day of the meeting — day two of a two-day
    meeting, when the statement is released. That is what ``config/fomc.yml``
    declares and what the probability engine chains between.
    """

    decision_date: date
    start_date: date | None
    year: int
    is_projection_meeting: bool = False
    raw_label: str = ""
    # True when this came from the advance-notice sentence below the last year
    # panel rather than from a panel row. Worth knowing: that sentence is the
    # single most fragile input this parser has (it caused one of the two bugs
    # the first live run found), and it is the ONLY source of the furthest-out
    # meeting. A caller that cares about the horizon is depending on prose.
    from_advance_notice: bool = False

    @property
    def is_two_day(self) -> bool:
        return self.start_date is not None and self.start_date != self.decision_date


# ---- HTML helpers -----------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[\s ]+")


def _strip_tags(fragment: str) -> str:
    """Crude tag strip. Adequate here: we only need the visible text of small
    fragments, and adding a parser dependency for that is not worth it."""
    text = _TAG_RE.sub(" ", fragment)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&#8211;", "-")  # en dash
        .replace("&ndash;", "-")
        .replace("–", "-")  # en dash, literal
        .replace("—", "-")  # em dash
    )
    return _WS_RE.sub(" ", text).strip()


def _month_number(name: str) -> int | None:
    return _MONTHS.get(name.strip().lower().rstrip("."))


# ---- parsing ----------------------------------------------------------------

# "January 28-29", "April/May 29-1", "November 4-5*", "March 3"
_MEETING_RE = re.compile(
    r"(?P<months>[A-Za-z]+(?:\s*/\s*[A-Za-z]+)?)\s+"
    r"(?P<days>\d{1,2}(?:\s*-\s*\d{1,2})?)\s*(?P<star>\*)?",
)

# The same shape, anchored to the WHOLE line. A meeting row is a bare label
# ("January 28-29"), never a sentence -- and the live page's footer is full of
# sentences that contain a month and a number. Matching _MEETING_RE anywhere in
# a line turned "Last Update: August 19, 2026" into an August 2027 meeting that
# does not exist. Found by the first live run; see
# docs/handoffs/fomc_calendar_scraper.md §11 (and §4.1 for the row contract).
_MEETING_LINE_RE = re.compile(
    r"^(?:[A-Za-z]+(?:\s*/\s*[A-Za-z]+)?)\s+"
    r"(?:\d{1,2}(?:\s*-\s*\d{1,2})?)\s*\*?\s*$",
)

# The Fed announces its furthest-out meeting in prose BELOW the last year
# panel, not as a row inside one:
#   "Note: A two-day meeting is scheduled for January 25-26, 2028."
# It is the only place that date appears, so it is parsed deliberately rather
# than left to the row scanner -- but only in this exact shape, and only with
# an EXPLICIT year, which is what keeps it from re-admitting arbitrary prose.
_ADVANCE_NOTICE_RE = re.compile(
    r"meeting\s+is\s+scheduled\s+for\s+"
    r"(?P<months>[A-Za-z]+(?:\s*/\s*[A-Za-z]+)?)\s+"
    r"(?P<days>\d{1,2}(?:\s*-\s*\d{1,2})?)\s*,?\s*"
    r"(?P<year>(?:19|20)\d{2})",
    re.IGNORECASE,
)


def _parse_meeting_label(label: str, year: int) -> FOMCMeeting | None:
    """Parse one meeting row's visible text into a meeting, or None to skip.

    Returns None for rows that are not scheduled rate decisions and for rows
    whose day range cannot be read; the caller warns about the latter.
    """
    lowered = label.lower()
    if any(marker in lowered for marker in _SKIP_MARKERS):
        return None

    match = _MEETING_RE.search(label)
    if not match:
        return None

    month_part = match.group("months")
    day_part = match.group("days")
    is_projection = bool(match.group("star"))

    months = [m for m in (_month_number(p) for p in month_part.split("/")) if m]
    if not months:
        return None

    days = [int(d) for d in re.split(r"\s*-\s*", day_part) if d.strip()]
    if not days:
        return None

    start_month = months[0]
    # A "April/May 29-1" meeting starts in the first month and DECIDES in the
    # second. A same-month meeting uses the one month for both days.
    end_month = months[-1]

    start_day = days[0]
    end_day = days[-1]

    # A meeting listed under one year that crosses into January belongs to the
    # next calendar year on its decision day (e.g. "December/January 31-1").
    start_year = year
    end_year = year
    if end_month < start_month:
        end_year = year + 1

    try:
        decision = date(end_year, end_month, end_day)
        start = date(start_year, start_month, start_day)
    except ValueError:
        return None

    return FOMCMeeting(
        decision_date=decision,
        start_date=start,
        year=year,
        is_projection_meeting=is_projection,
        raw_label=label.strip(),
    )


# Year panels. The live page heads each with "2027 FOMC Meetings"; the bare
# <h4>2027</h4> form is a fallback in case that wording changes.
_YEAR_HEADING_RE = re.compile(r"((?:19|20)\d{2})\s*FOMC\s+Meetings", re.IGNORECASE)
_YEAR_TAG_RE = re.compile(
    r"<h[1-6][^>]*>\s*((?:19|20)\d{2})\s*</h[1-6]>", re.IGNORECASE
)

# Lines that are publication metadata rather than a meeting. Past-year rows
# carry "Minutes: PDF | HTML (Released April 9, 2025)", whose "April 9" would
# otherwise read as a one-day meeting.
_PUBLICATION_MARKERS = (
    "released",
    "minutes",
    "statement",
    "transcript",
    "pdf",
    "html",
    "projection materials",
    "press conference",
    "implementation note",
)

# Block-level boundaries become line breaks, so a meeting's month and day-range
# stay separable from the links that follow them in the same row.
_BLOCK_END_RE = re.compile(
    r"</(?:div|tr|td|th|li|p|h[1-6]|strong|span)\s*>|<br\s*/?>", re.IGNORECASE
)

# A line that is only a month, or only a month pair ("April/May").
_MONTH_ONLY_RE = re.compile(r"^(?P<months>[A-Za-z]+(?:\s*/\s*[A-Za-z]+)?)\s*$")
# A line that is only a day range ("27-28", "17-18*", "3").
_DAYS_ONLY_RE = re.compile(r"^(?P<days>\d{1,2}(?:\s*-\s*\d{1,2})?)\s*(?P<star>\*)?\s*$")


def _text_lines(fragment: str) -> list[str]:
    """Visible text of a fragment, one line per block-level element.

    Keeping block boundaries is what lets the parser tell "March / 3
    (unscheduled)" apart from the scheduled meeting two rows above it, and stops
    a row's Statement/Minutes links from swallowing the meeting itself.
    """
    with_breaks = _BLOCK_END_RE.sub("\n", fragment)
    lines = []
    for raw in with_breaks.split("\n"):
        text = _strip_tags(raw)
        if text:
            lines.append(text)
    return lines


def _year_segments(html: str) -> list[tuple[int, str]]:
    """Split the page into (year, html-fragment) panels, in document order."""
    anchors: list[tuple[int, int]] = []  # (position, year)
    for pattern in (_YEAR_HEADING_RE, _YEAR_TAG_RE):
        for match in pattern.finditer(html):
            year = int(match.group(1))
            if 1990 <= year <= date.today().year + 10:
                anchors.append((match.start(), year))
    if not anchors:
        return []

    anchors.sort()
    # De-duplicate anchors that point at the same year in the same place.
    segments: list[tuple[int, str]] = []
    for index, (start, year) in enumerate(anchors):
        end = anchors[index + 1][0] if index + 1 < len(anchors) else len(html)
        if end > start:
            segments.append((year, html[start:end]))
    return segments


def parse_fomc_calendar(html: str, *, warn: bool = True) -> list[FOMCMeeting]:
    """Parse the Fed's FOMC calendar page into scheduled meetings.

    Pure: no network, no browser. Results are sorted ascending by
    ``decision_date`` and de-duplicated, matching what
    ``FOMCConfig.__post_init__`` requires of ``config/fomc.yml``.

    The page nests each meeting's month and day-range in sibling elements
    (``<div class="fomc-meeting__month">January</div>`` beside
    ``<div class="fomc-meeting__date">27-28</div>``), so this strips tags per
    year panel and scans the resulting text rather than trying to match element
    boundaries — markup nesting changes far more often than the visible text.

    Raises :class:`FOMCScrapeError` when nothing parses — an empty result is
    treated as "the page structure changed", never as "no meetings scheduled".
    """
    if not html or not html.strip():
        raise FOMCScrapeError("empty HTML passed to parse_fomc_calendar")

    meetings: dict[date, FOMCMeeting] = {}
    per_year: dict[int, int] = {}
    skipped: list[str] = []

    for year, fragment in _year_segments(html):
        pending_month: str | None = None

        for line in _text_lines(fragment):
            lowered = line.lower()

            # Publication metadata ("Minutes ... (Released April 9, 2025)").
            # Checked first: its embedded date must never read as a meeting.
            if any(marker in lowered for marker in _PUBLICATION_MARKERS):
                continue

            # "(unscheduled)" / "notation vote" sits on the meeting's own line,
            # so this cancels exactly that meeting and no other.
            if any(marker in lowered for marker in _SKIP_MARKERS):
                skipped.append(f"{line} [unscheduled/notation]")
                pending_month = None
                continue

            month_only = _MONTH_ONLY_RE.match(line)
            if month_only and _month_number(month_only.group("months").split("/")[0]):
                pending_month = month_only.group("months")
                continue

            days_only = _DAYS_ONLY_RE.match(line)
            if days_only and pending_month:
                line = f"{pending_month} {days_only.group('days')}" + (
                    days_only.group("star") or ""
                )
                pending_month = None
            elif not _MEETING_LINE_RE.match(line):
                # Anchored: a bare "January 28-29" row is a meeting, a sentence
                # that merely mentions a month is not. The page's own footer
                # ("Last Update: August 19, 2026") is the reason this is not a
                # substring search.
                continue

            meeting = _parse_meeting_label(line, year)
            if meeting is None:
                skipped.append(line)
                continue
            # First parse of a decision date wins; the page repeats dates in
            # statement/minutes rows for past meetings.
            if meeting.decision_date not in meetings:
                meetings[meeting.decision_date] = meeting
                per_year[year] = per_year.get(year, 0) + 1

    # The furthest-out meeting is announced in prose below the last panel, so
    # it has no year panel of its own and the row scanner above cannot see it.
    # Deliberately NOT counted in per_year: that warning measures how well each
    # PANEL parsed, and a one-meeting advance notice would trip it on every
    # single run, which is how a useful warning becomes background noise.
    for line in _text_lines(html):
        notice = _ADVANCE_NOTICE_RE.search(line)
        if not notice:
            continue
        meeting = _parse_meeting_label(
            f"{notice.group('months')} {notice.group('days')}",
            int(notice.group("year")),
        )
        if meeting is not None and meeting.decision_date not in meetings:
            meetings[meeting.decision_date] = replace(meeting, from_advance_notice=True)

    if not meetings:
        raise FOMCScrapeError(
            "parsed 0 meetings from the FOMC calendar page. The page structure "
            "has almost certainly changed — re-read "
            "docs/handoffs/fomc_calendar_scraper.md §4 and update the parser. "
            "Refusing to return an empty list, which a caller could mistake "
            "for 'no meetings scheduled'."
        )

    overfull = overfull_years(per_year)
    if overfull:
        # Deliberately an exception, not a warning. The two bugs the first live
        # run found (2026-09-15) both manifested as EXTRA meetings, and the
        # only structural check at the time fired on too FEW -- so both sailed
        # through silently. A phantom meeting is strictly worse than a missing
        # one here: a missing date shortens the modelled path, which the runway
        # alarm eventually catches, while a phantom date the Fed never
        # scheduled is chained into the rate path and nothing downstream
        # questions it. See docs/handoffs/fomc_calendar_scraper.md §11.
        detail = ", ".join(f"{y}: {per_year[y]} meetings" for y in sorted(overfull))
        raise FOMCScrapeError(
            f"parsed MORE than {EXPECTED_MEETINGS_PER_YEAR} scheduled meetings "
            f"in a year ({detail}). The Fed schedules "
            f"{EXPECTED_MEETINGS_PER_YEAR}/year, so this means the parser is "
            f"reading something that is not a meeting row -- prose, a footer, "
            f"a publication date. Re-read "
            f"docs/handoffs/fomc_calendar_scraper.md §4.1 and §11. Refusing to "
            f"return dates that would be chained into the modelled rate path."
        )

    if warn:
        for parsed_year, count in sorted(per_year.items()):
            if count < _PARTIAL_YEAR_WARN_THRESHOLD:
                print(
                    f"warning: parsed only {count} meetings for {parsed_year} "
                    f"(the Fed schedules {EXPECTED_MEETINGS_PER_YEAR}/year). "
                    f"Normal for the current or a preliminary year; suspicious "
                    f"otherwise.",
                    file=sys.stderr,
                )
        for label in skipped:
            print(f"note: skipped calendar entry {label!r}", file=sys.stderr)

    return sorted(meetings.values(), key=lambda m: m.decision_date)


def overfull_years(per_year: dict[int, int]) -> list[int]:
    """Years that parsed to more scheduled meetings than the Fed ever holds.

    Split out from :func:`parse_fomc_calendar` so the rule is testable on its
    own and so a caller can ask the question without re-parsing.
    """
    return [y for y, n in per_year.items() if n > EXPECTED_MEETINGS_PER_YEAR]


# ---- config diffing ---------------------------------------------------------


@dataclass(frozen=True)
class CalendarDiff:
    """What the live page says versus what ``config/fomc.yml`` declares."""

    missing_from_config: tuple[date, ...]
    absent_upstream: tuple[date, ...]
    # True when `absent_upstream` is most likely explained by the advance-notice
    # sentence no longer parsing, rather than by a meeting actually moving.
    # Without this the two are indistinguishable, and they want opposite
    # responses: a moved meeting is a calendar question for a human, a
    # stopped-parsing note is a parser bug.
    advance_notice_missing: bool = False

    @property
    def in_sync(self) -> bool:
        return not self.missing_from_config and not self.absent_upstream


def diff_against_config(
    scraped: Iterable[FOMCMeeting],
    configured: Iterable[date],
    *,
    today: date | None = None,
) -> CalendarDiff:
    """Compare scraped meetings with the configured list.

    Only FUTURE meetings are compared. Past meetings legitimately drop off the
    forward calendar and legitimately stay in the config, so including them
    would report drift on every run.
    """
    today = today or date.today()
    scraped = list(scraped)
    scraped_future = {m.decision_date for m in scraped if m.decision_date >= today}
    configured_future = {d for d in configured if d >= today}
    absent_upstream = tuple(sorted(configured_future - scraped_future))

    # The furthest-out meeting comes only from the advance-notice sentence. If
    # that sentence stops matching, the horizon silently shrinks to the last
    # panel row -- and the config's furthest date then shows up as "the Fed no
    # longer lists this", which reads as a MOVED MEETING and sends whoever gets
    # the alert to the wrong problem entirely. Distinguish the two by their
    # signature: no advance-notice meeting was parsed at all, yet the config
    # has dates beyond everything we did parse.
    saw_advance_notice = any(m.from_advance_notice for m in scraped)
    furthest_scraped = max(scraped_future, default=None)
    advance_notice_missing = bool(
        absent_upstream
        and not saw_advance_notice
        and furthest_scraped is not None
        and all(d > furthest_scraped for d in absent_upstream)
    )

    return CalendarDiff(
        missing_from_config=tuple(sorted(scraped_future - configured_future)),
        absent_upstream=absent_upstream,
        advance_notice_missing=advance_notice_missing,
    )


# ---- runway / escalation ----------------------------------------------------

# Days of remaining calendar below which each escalation level kicks in.
# spec008 §5 Decision 4. Ordered most- to least-slack; a level applies when
# runway is <= its threshold and > the next one down.
RUNWAY_THRESHOLDS: tuple[tuple[str, int], ...] = (
    ("due", 270),  # open/refresh a tracking issue
    ("priority", 120),  # escalate; label it
    ("urgent", 45),  # last window a normal review cycle still fits in
    ("expired", 0),  # the Gold tables are already emitting nothing
)

RUNWAY_OK = "ok"


def runway_days(
    meeting_dates: Iterable[date], *, today: date | None = None
) -> int | None:
    """Days until the LAST configured meeting. ``None`` if there are none.

    ``today`` is injectable on purpose: a wall-clock read buried in a helper is
    how the existing runway check became a test that fails on a fixed future
    date regardless of the code around it.
    """
    dates = list(meeting_dates)
    if not dates:
        return None
    return (max(dates) - (today or date.today())).days


def runway_level(
    meeting_dates: Iterable[date],
    *,
    today: date | None = None,
    thresholds: tuple[tuple[str, int], ...] = RUNWAY_THRESHOLDS,
) -> str:
    """Escalation level for the remaining calendar runway.

    Returns ``"ok"`` while there is more slack than the largest threshold, and
    otherwise the most severe level whose threshold the runway has fallen to or
    below. An empty calendar is ``"expired"`` -- there is nothing left to run
    out, which is the same operational state.
    """
    remaining = runway_days(meeting_dates, today=today)
    if remaining is None:
        return "expired"
    level = RUNWAY_OK
    for name, threshold in thresholds:
        if remaining <= threshold:
            level = name
    return level


def _render_meeting_item(meeting: FOMCMeeting) -> str:
    suffix = "  # SEP / projections" if meeting.is_projection_meeting else ""
    return f'  - "{meeting.decision_date.isoformat()}"{suffix}'


def format_yaml_block(meetings: Iterable[FOMCMeeting]) -> str:
    """Render meetings as the ``meeting_dates`` lines of ``config/fomc.yml``.

    Deliberately returns the list items only, not the whole file: the config
    carries hand-written provenance comments that a generated rewrite would
    destroy, so a human pastes this in and keeps the surrounding commentary.
    """
    return "\n".join(
        _render_meeting_item(m) for m in sorted(meetings, key=lambda m: m.decision_date)
    )


# ---- config editing (spec008 §5 Decision 2 / §8 Phase 2) --------------------
#
# ``apply_calendar_refresh`` is the automated half of "machine proposes, human
# merges": it edits config/fomc.yml well enough to open a reviewable PR from,
# without ever doing a full-file rewrite that would destroy the hand-written
# provenance comments format_yaml_block's own docstring protects. It only
# touches (a) the meeting_dates list items -- inserting new ones, never
# reformatting existing ones -- and (b) the specific calendar_provenance
# fields a refresh actually changes. Every other byte of the file, including
# every comment, is untouched line-for-line.

_MEETING_ITEM_RE = re.compile(r'^\s*-\s*"(\d{4}-\d{2}-\d{2})"')


def _find_top_level_key(lines: list[str], key: str) -> int | None:
    """Index of the line that is exactly ``"<key>:"`` at column 0, or ``None``."""
    target = f"{key}:"
    for i, line in enumerate(lines):
        if line.rstrip() == target:
            return i
    return None


def _indented_block_end(lines: list[str], start: int) -> int:
    """Index (exclusive) of the end of the blank-or-indented block starting at
    ``start`` -- the first line that begins with a non-whitespace character,
    i.e. the next top-level key. Blank lines count as part of the block."""
    i = start
    while i < len(lines) and (lines[i] == "" or lines[i][:1].isspace()):
        i += 1
    return i


def _split_meeting_chunks(
    block_lines: list[str],
) -> tuple[list[tuple[list[str], str, date]], list[str]]:
    """Group ``meeting_dates`` block lines into one chunk per list item.

    A chunk is ``(leading_lines, item_line, item_date)`` — any comment/blank
    lines immediately before an item travel with it (e.g. the four-line note
    above the 2028-01-26 entry), so re-sorting or inserting chunks can never
    separate a date from the commentary that explains it. Lines after the
    last item (normally just the blank line before the next top-level key)
    are returned separately as ``trailing``.
    """
    chunks: list[tuple[list[str], str, date]] = []
    pending: list[str] = []
    for line in block_lines:
        m = _MEETING_ITEM_RE.match(line)
        if m:
            chunks.append((pending, line, date.fromisoformat(m.group(1))))
            pending = []
        else:
            pending.append(line)
    return chunks, pending


def _merge_new_meetings(
    chunks: list[tuple[list[str], str, date]],
    trailing: list[str],
    new_meetings: Iterable[FOMCMeeting],
) -> list[str] | None:
    """Insert genuinely-new meetings into existing chunks, ascending by date.

    Returns the rebuilt block's lines, or ``None`` if every meeting in
    ``new_meetings`` already has a chunk (a true no-op — the caller returns
    the input text unchanged rather than reconstruct a byte-identical block).
    """
    existing_dates = {d for _, _, d in chunks}
    by_date = {m.decision_date: m for m in new_meetings}
    to_add = sorted(
        (m for d, m in by_date.items() if d not in existing_dates),
        key=lambda m: m.decision_date,
    )
    if not to_add:
        return None

    merged = list(chunks)
    for meeting in to_add:
        insert_at = len(merged)
        for i, (_, _, existing_date) in enumerate(merged):
            if existing_date > meeting.decision_date:
                insert_at = i
                break
        merged.insert(insert_at, ([], _render_meeting_item(meeting), meeting.decision_date))

    new_block_lines: list[str] = []
    for leading, item_line, _ in merged:
        new_block_lines.extend(leading)
        new_block_lines.append(item_line)
    new_block_lines.extend(trailing)
    return new_block_lines


_PROVENANCE_FIELD_RE = {
    "last_verified": re.compile(r"^(\s*last_verified:\s*)\S+"),
    "verified_by": re.compile(r"^(\s*verified_by:\s*)\S+"),
    "published_through": re.compile(r"^(\s*published_through:\s*)\S+"),
}


def _bump_provenance_lines(
    lines: list[str],
    *,
    verified_by: str,
    last_verified: date,
    published_through: date | None,
) -> None:
    """Mutate ``lines`` in place, updating only the named ``calendar_provenance``
    fields present. Missing the block entirely is fine and does nothing —
    a pre-spec008 config without one must keep working (AC-009)."""
    cp_key_idx = _find_top_level_key(lines, "calendar_provenance")
    if cp_key_idx is None:
        return
    block_start = cp_key_idx + 1
    block_end = _indented_block_end(lines, block_start)
    values = {
        "last_verified": last_verified.isoformat(),
        "verified_by": verified_by,
        "published_through": published_through.isoformat() if published_through else None,
    }
    for i in range(block_start, block_end):
        for field, pattern in _PROVENANCE_FIELD_RE.items():
            value = values[field]
            if value is None:
                continue
            m = pattern.match(lines[i])
            if m:
                lines[i] = f"{m.group(1)}{value}"
                break


def apply_calendar_refresh(
    config_text: str,
    new_meetings: Iterable[FOMCMeeting],
    *,
    verified_by: str = "scraper",
    last_verified: date | None = None,
    published_through: date | None = None,
) -> str:
    """Insert newly-published FOMC meeting dates and bump provenance.

    The automated half of spec008 Decision 2 ("machine proposes, human
    merges"): this is what the scheduled workflow runs to produce the diff a
    human then reviews on a PR, never something that commits itself.

    ``new_meetings`` is typically ``CalendarDiff.missing_from_config``'s
    dates paired back to their ``FOMCMeeting`` objects (to preserve the SEP
    suffix) — a date already present in ``meeting_dates`` is skipped rather
    than duplicated, so calling this with nothing genuinely new is a true
    no-op: the returned text is the input, unchanged, including no
    provenance bump. ``published_through`` is left alone unless the caller
    passes it explicitly (typically ``max(m.decision_date for m in all
    scraped meetings)``, not just the new ones) — this function has no way to
    know the Fed's real horizon from ``new_meetings`` alone.

    Raises :class:`FOMCScrapeError` if ``config_text`` has no top-level
    ``meeting_dates:`` line — a malformed-input failure should be loud, not a
    silent pass-through that looks like "nothing to do".
    """
    lines = config_text.splitlines()

    md_key_idx = _find_top_level_key(lines, "meeting_dates")
    if md_key_idx is None:
        raise FOMCScrapeError(
            "apply_calendar_refresh: no top-level 'meeting_dates:' line found"
        )
    block_start = md_key_idx + 1
    block_end = _indented_block_end(lines, block_start)
    chunks, trailing = _split_meeting_chunks(lines[block_start:block_end])

    merged_block = _merge_new_meetings(chunks, trailing, new_meetings)
    if merged_block is None:
        return config_text

    lines = lines[:block_start] + merged_block + lines[block_end:]
    _bump_provenance_lines(
        lines,
        verified_by=verified_by,
        last_verified=last_verified or date.today(),
        published_through=published_through,
    )

    text = "\n".join(lines)
    if config_text.endswith("\n"):
        text += "\n"
    return text


# ---- fetching (the only I/O in this module) ---------------------------------

VALID_BACKENDS = ("auto", "requests", "selenium")

# Identify the tool honestly rather than impersonating a browser. A plain
# python-requests UA is sometimes refused; this says what it is and why.
DEFAULT_USER_AGENT = (
    "fred-bronze-to-gold-pipeline/fomc-calendar-scraper "
    "(+https://github.com/joshualutkemuller/fred-bronze-to-gold-pipeline; "
    "refreshes config/fomc.yml a few times a year)"
)


def fetch_calendar_html(
    url: str = FOMC_CALENDAR_URL,
    *,
    backend: str = "auto",
    headless: bool = True,
    chromedriver_path: str | None = None,
    chrome_binary: str | None = None,
    timeout_seconds: int = 30,
    user_agent: str = DEFAULT_USER_AGENT,
) -> str:
    """Fetch the calendar page and return its HTML.

    ``backend``:

    ``"requests"``
        Plain HTTP GET. The page is server-rendered, so this is sufficient and
        is the fast path — no browser, no driver, and ``requests`` is already a
        core dependency.
    ``"selenium"``
        Drive a real browser. Needed only if the page becomes JavaScript-
        rendered or starts refusing plain HTTP clients.
    ``"auto"`` (default)
        Try ``requests``; on failure fall back to ``selenium``. If both fail,
        the raised error reports both causes — the requests failure is usually
        the informative one, and hiding it behind a driver error would send you
        debugging the wrong layer.
    """
    if backend not in VALID_BACKENDS:
        raise FOMCScrapeError(
            f"unknown backend {backend!r}; expected one of {list(VALID_BACKENDS)}"
        )

    if backend == "requests":
        return fetch_calendar_html_requests(
            url, timeout_seconds=timeout_seconds, user_agent=user_agent
        )
    if backend == "selenium":
        return fetch_calendar_html_selenium(
            url,
            headless=headless,
            chromedriver_path=chromedriver_path,
            chrome_binary=chrome_binary,
            timeout_seconds=timeout_seconds,
        )

    # auto
    try:
        return fetch_calendar_html_requests(
            url, timeout_seconds=timeout_seconds, user_agent=user_agent
        )
    except FOMCScrapeError as requests_exc:
        print(
            f"note: requests backend failed ({requests_exc}); trying Selenium",
            file=sys.stderr,
        )
        try:
            return fetch_calendar_html_selenium(
                url,
                headless=headless,
                chromedriver_path=chromedriver_path,
                chrome_binary=chrome_binary,
                timeout_seconds=timeout_seconds,
            )
        except FOMCScrapeError as selenium_exc:
            raise FOMCScrapeError(
                f"both fetch backends failed for {url}.\n"
                f"  requests: {requests_exc}\n"
                f"  selenium: {selenium_exc}"
            ) from selenium_exc


def fetch_calendar_html_requests(
    url: str = FOMC_CALENDAR_URL,
    *,
    timeout_seconds: int = 30,
    user_agent: str = DEFAULT_USER_AGENT,
) -> str:
    """Fetch the calendar page with a plain HTTP GET.

    The page is server-rendered static HTML, so this is the right tool: no
    browser, no driver-version coupling, and ``requests`` is already in
    ``requirements.txt``. Kept deliberately small — no retry loop, because this
    is a hand-run maintenance tool used a couple of times a year, not an
    ingestion path.
    """
    try:
        import requests
    except ImportError as exc:  # pragma: no cover - requests is a core dep
        raise FOMCScrapeError(
            "requests is not installed (it is a core dependency of this "
            "pipeline; run `pip install -r requirements.txt`). Alternatively "
            "use --backend selenium, or --html-file to parse a saved page."
        ) from exc

    try:
        response = requests.get(
            url,
            timeout=timeout_seconds,
            headers={"User-Agent": user_agent, "Accept": "text/html"},
        )
        response.raise_for_status()
    except Exception as exc:
        raise FOMCScrapeError(f"HTTP GET {url} failed: {exc}") from exc

    if not response.text or not response.text.strip():
        raise FOMCScrapeError(f"HTTP GET {url} returned an empty body")
    return response.text


def fetch_calendar_html_selenium(
    url: str = FOMC_CALENDAR_URL,
    *,
    headless: bool = True,
    chromedriver_path: str | None = None,
    chrome_binary: str | None = None,
    timeout_seconds: int = 30,
) -> str:
    """Fetch the calendar page with Selenium and return its rendered HTML.

    Selenium is imported here rather than at module scope so that importing
    :mod:`fred_pipeline` never requires it — it is an optional extra for this
    maintenance tool, not a pipeline dependency (see the spec §8).

    Driver/browser resolution, first hit wins: explicit argument, then the
    ``FOMC_CHROMEDRIVER`` / ``FOMC_CHROME_BINARY`` environment variables, then
    Selenium Manager's own auto-resolution.
    """
    import os

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise FOMCScrapeError(
            "selenium is not installed. It is an optional extra for this "
            "maintenance tool, not a pipeline dependency:\n"
            "    pip install selenium>=4.15\n"
            "Alternatively parse a saved page with --html-file and no browser."
        ) from exc

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,2400")

    binary = chrome_binary or os.environ.get("FOMC_CHROME_BINARY")
    if binary:
        options.binary_location = binary

    driver_path = chromedriver_path or os.environ.get("FOMC_CHROMEDRIVER")
    service = Service(executable_path=driver_path) if driver_path else Service()

    driver = None
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(timeout_seconds)
        driver.get(url)
        # The panels are server-rendered; waiting on <body> is enough to cover
        # a slow load without pinning the wait to a specific CSS class that the
        # Fed may rename.
        WebDriverWait(driver, timeout_seconds).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        return driver.page_source
    except FOMCScrapeError:
        raise
    except Exception as exc:
        detail = str(exc)
        hint = (
            "Check that chromedriver and a Chrome/Chromium binary are available "
            "(FOMC_CHROMEDRIVER / FOMC_CHROME_BINARY), and that the host is "
            "reachable from this network."
        )
        # By far the most common Selenium failure, and the one whose message is
        # easiest to misread as "the scraper is broken".
        if "only supports Chrome version" in detail or "session not created" in detail:
            hint = (
                "chromedriver and the browser are different major versions. "
                "Either install a matching pair, or unset FOMC_CHROMEDRIVER and "
                "let Selenium Manager resolve one (it needs network access to "
                "googlechromelabs.github.io to download it). Meanwhile "
                "--html-file parses a saved page with no browser at all."
            )
        raise FOMCScrapeError(
            f"failed to fetch {url} with Selenium: {detail}\n{hint}"
        ) from exc
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:  # pragma: no cover - best-effort teardown
                pass
