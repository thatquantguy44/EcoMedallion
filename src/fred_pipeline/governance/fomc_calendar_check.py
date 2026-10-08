"""Pre-Gold health check (and optional refresh) of ``config/fomc.yml``.

``gold.fomc_probability`` and ``gold.fomc_meeting_path`` are built from the
meeting dates in ``config/fomc.yml``. When that list is missing, unreadable or
expired the build does not fail -- it emits empty tables and the run still
reports success, so the Power BI Fed Policy Watch report goes blank with
nothing to say why. This module turns those states into a recorded stage.

Two layers, deliberately separate:

**The check (always on, offline).** Reads only the local file. No network, so
it adds no new failure mode to a run. Severity is chosen to avoid alert
fatigue: a ``WARNED`` stage makes the whole run ``partial`` (see
``governance.stages.overall_verdict``) and so triggers the run-alert email.

=========================  =========  ====================================
Calendar state             Status     Why
=========================  =========  ====================================
>120 days of runway        ok         the weekly refresh job owns this
121-270 days ("due")       ok         logged only, same reason
<=120 days                 warn       the refresh PR should have landed
missing / malformed        fail       Gold would silently be empty
expired                    fail       Gold would silently be empty
=========================  =========  ====================================

**The refresh (opt-in, ``--refresh-fomc-calendar``).** Fetches the Fed's page
and inserts newly published meetings into the file. spec008 Decision 1 argued
against putting the Fed fetch in the run at all (a Fed outage or redesign
should not touch unrelated runs), and that argument stands -- which is why
this is off by default and why *nothing it can do is allowed to stop the run*:

* any fetch, parse or write failure is caught, logged, and the calendar
  already on disk is used unchanged;
* it is **additive only** -- a date the Fed page no longer lists is reported,
  never removed, because that usually means a meeting moved;
* a write happens only after the new text loads as a valid config, and goes
  through a temp file + ``os.replace`` so a crash cannot leave a half-written
  file.

Note what the refresh edits: the tracked file in the working tree. It does not
commit. A run that refreshes leaves ``config/fomc.yml`` modified for a human
to review and commit, which is the same "machine proposes, human merges"
stance as the scheduled workflow (``scripts/fomc_calendar_automation.py``).
"""

from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from fred_pipeline.catalogs.fomc_calendar import (
    apply_calendar_refresh,
    diff_against_config,
    fetch_calendar_html,
    parse_fomc_calendar,
    runway_days,
    runway_level,
)
from fred_pipeline.gold_config.fomc_config import (
    FOMCConfigError,
    load_fomc_config,
    resolve_fomc_config_path,
)

log = logging.getLogger("fred_pipeline")

OK = "ok"
WARN = "warn"
FAIL = "fail"

#: Runway levels (see ``catalogs.fomc_calendar.RUNWAY_THRESHOLDS``) at which the
#: stage is marked WARNED. Above these the run stays quiet: the scheduled
#: workflow opens an issue from 270 days out, and warning here too would turn
#: every run for months into a "partial" one.
WARN_LEVELS = frozenset({"priority", "urgent"})

REFRESH_TIMEOUT_SECONDS = 15


class FOMCCalendarError(RuntimeError):
    """The calendar cannot support the FOMC Gold tables (raised by the stage)."""


@dataclass
class FOMCCalendarCheck:
    status: str
    path: str
    runway_days: int | None = None
    level: str = ""
    refreshed: bool = False
    added_dates: tuple[str, ...] = ()
    messages: list[str] = field(default_factory=list)

    def detail(self) -> dict[str, Any]:
        """Stage-summary sub-line values (kept to simple scalars)."""
        out: dict[str, Any] = {"path": self.path, "runway_level": self.level}
        if self.runway_days is not None:
            out["runway_days"] = self.runway_days
        if self.refreshed:
            out["refreshed"] = True
            out["added_dates"] = ", ".join(self.added_dates)
        return out


def check_fomc_calendar(
    path: str | None = None,
    *,
    refresh: bool = False,
    today: date | None = None,
    fetch: Callable[..., str] = fetch_calendar_html,
    timeout_seconds: int = REFRESH_TIMEOUT_SECONDS,
) -> FOMCCalendarCheck:
    """Check the FOMC calendar, optionally refreshing it first. Never raises
    for an expected problem -- those come back as ``status`` + ``messages``.

    ``today`` and ``fetch`` are injectable so tests need neither the wall
    clock nor the network.
    """
    today = today or date.today()
    resolved = resolve_fomc_config_path(path)
    messages: list[str] = []
    refreshed = False
    added: tuple[str, ...] = ()
    refresh_problem = False

    if refresh:
        try:
            added, notes, refresh_problem = _try_refresh(
                resolved, today, fetch, timeout_seconds
            )
            refreshed = bool(added)
            messages.extend(notes)
        except Exception as exc:  # noqa: BLE001 -- the fallback is the point: a
            # Fed outage, a redesigned page, a read-only checkout or a parser
            # bug must all end the same way, with the file on disk untouched.
            refresh_problem = True
            messages.append(
                f"FOMC calendar refresh failed ({type(exc).__name__}: {exc}); "
                f"continuing with the calendar already on disk"
            )

    health, health_msgs, remaining, level = _assess(resolved, today)
    messages.extend(health_msgs)

    status = health
    if status == OK and refresh_problem:
        status = WARN

    return FOMCCalendarCheck(
        status=status,
        path=resolved,
        runway_days=remaining,
        level=level,
        refreshed=refreshed,
        added_dates=added,
        messages=messages,
    )


def _assess(resolved: str, today: date) -> tuple[str, list[str], int | None, str]:
    """Judge the calendar file as it is on disk right now."""
    try:
        cfg = load_fomc_config(resolved)
    except FOMCConfigError as exc:
        return (
            FAIL,
            [f"{resolved} is malformed, so the FOMC Gold tables will be empty: {exc}"],
            None,
            "",
        )
    if cfg is None:
        return (
            FAIL,
            [
                f"FOMC calendar not found at {resolved!r} (cwd: {os.getcwd()}). "
                f"The default path is relative to the working directory, so "
                f"running from outside the repo root lands here. Without it "
                f"gold.fomc_probability and gold.fomc_meeting_path will be EMPTY. "
                f"Set FRED_FOMC_CONFIG_FILE or run from the repo root."
            ],
            None,
            "",
        )

    remaining = runway_days(cfg.meeting_dates, today=today)
    level = runway_level(cfg.meeting_dates, today=today)
    last = max(cfg.meeting_dates)

    if level == "expired":
        return (
            FAIL,
            [
                f"FOMC calendar has run out (last meeting {last}, "
                f"{remaining} days). gold.fomc_probability and "
                f"gold.fomc_meeting_path will be EMPTY."
            ],
            remaining,
            level,
        )
    if level in WARN_LEVELS:
        return (
            WARN,
            [
                f"FOMC calendar runs out in {remaining} days (last meeting "
                f"{last}). The scheduled refresh should have opened a PR -- "
                f"see docs/handoffs/fomc_calendar_scraper.md."
            ],
            remaining,
            level,
        )
    if level == "due":
        log.info(
            "FOMC calendar has %d days of runway (last meeting %s); the weekly "
            "refresh job owns this nudge",
            remaining,
            last,
        )
    return OK, [], remaining, level


def _try_refresh(
    resolved: str,
    today: date,
    fetch: Callable[..., str],
    timeout_seconds: int,
) -> tuple[tuple[str, ...], list[str], bool]:
    """Fetch the Fed page and add newly published meetings.

    Returns ``(added_dates, notes, problem)``. Raises on anything unexpected;
    the caller turns every exception into "keep the file we have".
    """
    path = Path(resolved)
    original = path.read_text(encoding="utf-8")
    cfg = load_fomc_config(resolved)
    if cfg is None:  # pragma: no cover - read_text above would have raised
        raise FileNotFoundError(resolved)

    html = fetch(backend="requests", timeout_seconds=timeout_seconds)
    meetings = parse_fomc_calendar(html, warn=False)
    diff = diff_against_config(meetings, cfg.meeting_dates, today=today)

    notes: list[str] = []
    problem = False

    if diff.advance_notice_missing:
        problem = True
        notes.append(
            "FOMC refresh: the Fed page's advance-notice sentence no longer "
            "parses, so its furthest meeting is invisible. Most likely a parser "
            "problem, not a moved meeting; calendar not touched on that account."
        )
    elif diff.absent_upstream:
        problem = True
        gone = ", ".join(d.isoformat() for d in diff.absent_upstream)
        notes.append(
            f"FOMC refresh: configured meeting date(s) not on the Fed page: "
            f"{gone}. Left in place -- a missing date usually means a meeting "
            f"moved, which needs a human."
        )

    added: tuple[str, ...] = ()
    if diff.missing_from_config:
        wanted = set(diff.missing_from_config)
        new = [m for m in meetings if m.decision_date in wanted]
        updated = apply_calendar_refresh(
            original,
            new,
            verified_by="scraper",
            last_verified=today,
            published_through=max(m.decision_date for m in meetings),
        )
        if updated != original:
            _write_validated(path, updated, must_keep=set(cfg.meeting_dates))
            added = tuple(d.isoformat() for d in diff.missing_from_config)
            log.warning(
                "FOMC calendar refreshed: added %s to %s. The file is modified "
                "but NOT committed -- review and commit it.",
                ", ".join(added),
                resolved,
            )
    return added, notes, problem


def _write_validated(path: Path, text: str, *, must_keep: set[date]) -> None:
    """Replace ``path`` with ``text`` only if ``text`` is a valid config that
    still contains every date it started with. Atomic via temp file +
    ``os.replace`` in the same directory."""
    tmp = path.with_name(f".{path.name}.refresh-{os.getpid()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        new_cfg = load_fomc_config(str(tmp))
        if new_cfg is None:  # pragma: no cover
            raise ValueError("refreshed calendar did not load")
        lost = must_keep - set(new_cfg.meeting_dates)
        if lost:
            raise ValueError(
                "refreshed calendar would drop existing date(s): "
                + ", ".join(sorted(d.isoformat() for d in lost))
            )
        shutil.copymode(path, tmp)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
