#!/usr/bin/env python3
"""Drive config/fomc.yml's automated refresh loop — spec008 Phase 3.

Spec: ``specs/spec008/README.md`` (Decisions 1-4, §6 Proposed Approach).
Builds on ``scripts/scrape_fomc_calendar.py`` (the manual tool this
automates) and ``catalogs/fomc_calendar.py`` (the pure parser/differ/editor
every function here is built from — nothing new is invented here, this is
wiring).

This is the one script in the FOMC toolchain allowed to write
``config/fomc.yml`` and to open pull requests / issues. It exists
specifically so nothing else has to: the manual CLI's "never write
automatically" promise (its own docstring, quoted throughout this repo) is
about *interactive* use, and stays true there. This script is the automated
path spec008 Decision 2 calls "machine proposes, human merges" — it commits
to a branch and opens a PR; nothing here merges to ``main`` on its own.

Four-way branch (spec008 §6), all independently possible in one run:

1. **New meetings published** (``missing_from_config``) → apply
   :func:`catalogs.fomc_calendar.apply_calendar_refresh`, commit to a
   deterministic branch, open or update a PR. Never auto-merged.
2. **A configured date vanished** (``absent_upstream``, and the
   advance-notice sentence still parses) → open/update an issue. Config is
   never edited for this case — a vanished date usually means a meeting
   *moved*, and that is a human's call.
3. **The advance-notice sentence stopped parsing**
   (``advance_notice_missing``) → open/update a *different* issue, one that
   says PARSER PROBLEM rather than MOVED MEETING, because those need
   different responses from whoever picks it up.
4. **Fetch or parse failed outright** → save whatever HTML was received (if
   any) as an artifact path the workflow uploads, open/update the parser
   issue with both backends' errors attached.

Independently of all four: **runway escalation** (Decision 4) opens or
updates a single tracking issue once runway drops to or below 270 days,
escalating its labels as it keeps shrinking. A green, in-sync, plenty-of-
runway run does none of this and prints a one-line summary.

**Simplification worth naming**: unlike the interactive CLI's ``auto``
backend, this script does not retry via Selenium when a fetch succeeds but
parses to zero meetings — Selenium is this toolchain's explicit break-glass
path (``docs/handoffs/fomc_calendar_scraper.md`` §8), and retrying it
unattended in a scheduled job is a nice-to-have, not a correctness
requirement. A zero-meetings parse here is reported as a parser-problem
issue like any other parse failure; a human can rerun with
``--backend selenium`` by hand if that turns out to be the real cause.

**What is and is not verified.** The decision logic (``plan_actions``) and
the git/gh command construction are unit-tested with an injectable runner —
see ``tests/test_fomc_calendar_automation.py``. The actual git/gh
*execution* against a real repository has not been run for real from this
environment (no live repo or ``gh`` auth available here) — the same category
of caveat this repo already applies honestly elsewhere (`sources/imf.py`'s
module docstring is the closest parallel: built against a well-understood,
documented contract, but only genuinely proven by a first live run). Per
spec008 §8 Phase 3's own exit gate: **validate with ``workflow_dispatch``
against a pinned fixture before pointing this at the real Fed page.**

Usage:

    # what a scheduled run would decide, without touching anything
    python scripts/fomc_calendar_automation.py --html-file tests/fixtures/fomccalendars.html --dry-run

    # the real thing (only makes sense inside the GitHub Actions workflow,
    # where `git`/`gh` are configured and authenticated)
    python scripts/fomc_calendar_automation.py --live

Exit codes: 0 = ran to completion cleanly, whether quiet or a run that
opened/updated a PR or issue (check the printed JSON summary on stderr for
what it decided); 1 = the calendar page could not be fetched or parsed
(spec008 AC-006 wants this loud even though the parser-problem issue was
still opened/would still be opened — a red job in Actions is itself a
useful, independent signal); 2 = a git/gh command failed while executing a
planned action in ``--live`` mode.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import yaml  # noqa: E402

from fred_pipeline.catalogs.fomc_calendar import (  # noqa: E402
    FOMC_CALENDAR_URL,
    VALID_BACKENDS,
    FOMCMeeting,
    FOMCScrapeError,
    apply_calendar_refresh,
    diff_against_config,
    fetch_calendar_html,
    parse_fomc_calendar,
    runway_days,
    runway_level,
)

DEFAULT_CONFIG = REPO_ROOT / "config" / "fomc.yml"

# Deterministic so repeated runs update one PR/issue instead of opening a new
# one every week (spec008 Decision 2's "one PR, not 52" / Decision 3's
# "deduplicated on a stable title").
REFRESH_BRANCH = "automation/fomc-calendar-refresh"
MOVED_MEETING_ISSUE_TITLE = "FOMC calendar: a configured meeting date is no longer listed"
PARSER_ISSUE_TITLE = "FOMC calendar scraper: page structure may have changed"
RUNWAY_ISSUE_TITLE = "FOMC calendar refresh due"

RUNWAY_ISSUE_LABELS = {
    "due": ["fomc-calendar"],
    "priority": ["fomc-calendar", "priority"],
    "urgent": ["fomc-calendar", "priority", "urgent"],
    "expired": ["fomc-calendar", "priority", "urgent", "expired"],
}


def _configured_dates(path: Path) -> list[date]:
    """Same tiny parse as scrape_fomc_calendar.py's own helper -- duplicated
    rather than imported across two standalone scripts, the same
    self-contained-scripts reasoning this repo already applies to source
    clients (see e.g. oecd.py's module docstring)."""
    if not path.is_file():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    out = []
    for raw in data.get("meeting_dates") or []:
        out.append(raw if isinstance(raw, date) else date.fromisoformat(str(raw)))
    return sorted(out)


# ---- pure decision logic ----------------------------------------------------
#
# Everything above the execution layer below is pure: dates and meetings in,
# a plan out. No subprocess, no file writes, no network. This is what
# tests/test_fomc_calendar_automation.py exercises directly.


@dataclass(frozen=True)
class AutomationPlan:
    """What one run decided to do, computed once and then acted on --
    kept separate from execution so the decision logic (spec008 §5
    Decisions 2-4) is fully unit-testable without git/gh/subprocess."""

    propose_pr: bool
    new_meetings: tuple[FOMCMeeting, ...]
    open_moved_meeting_issue: bool
    open_parser_issue: bool
    moved_dates: tuple[date, ...]
    runway_issue_level: str | None  # None ("ok"), or "due" / "priority" / "urgent" / "expired"
    runway_days_remaining: int | None
    parsed_through: date | None
    configured_through: date | None

    @property
    def is_quiet(self) -> bool:
        """True when nothing here needs to open or update anything."""
        return not (
            self.propose_pr
            or self.open_moved_meeting_issue
            or self.open_parser_issue
            or self.runway_issue_level is not None
        )


def plan_actions(
    meetings: Sequence[FOMCMeeting],
    configured: Sequence[date],
    *,
    today: date | None = None,
) -> AutomationPlan:
    """Pure decision function -- spec008 §5 Decisions 2-4, applied together.

    All of a proposed PR, a moved-meeting issue, a parser issue, and a
    runway-escalation issue can independently apply in the same run; this
    mirrors the parallel branches in spec008 §6's diagram, not a single
    either/or outcome.
    """
    diff = diff_against_config(meetings, configured, today=today)
    new_meetings = tuple(
        sorted(
            (m for m in meetings if m.decision_date in diff.missing_from_config),
            key=lambda m: m.decision_date,
        )
    )
    level = runway_level(configured, today=today)
    return AutomationPlan(
        propose_pr=bool(new_meetings),
        new_meetings=new_meetings,
        open_moved_meeting_issue=bool(diff.absent_upstream) and not diff.advance_notice_missing,
        open_parser_issue=diff.advance_notice_missing,
        moved_dates=diff.absent_upstream,
        runway_issue_level=None if level == "ok" else level,
        runway_days_remaining=runway_days(configured, today=today),
        parsed_through=meetings[-1].decision_date if meetings else None,
        configured_through=max(configured) if configured else None,
    )


def _pr_title(plan: AutomationPlan) -> str:
    if plan.parsed_through:
        return f"Refresh FOMC calendar through {plan.parsed_through.isoformat()}"
    return "Refresh FOMC calendar"


def _pr_body(plan: AutomationPlan) -> str:
    lines = [
        "Automated refresh — spec008 Decision 2 (\"machine proposes, human merges\").",
        "",
        "New meeting dates found on the Fed's calendar page, not yet in "
        "`config/fomc.yml`:",
        "",
    ]
    for m in plan.new_meetings:
        suffix = " (SEP / projections)" if m.is_projection_meeting else ""
        lines.append(f"- `{m.decision_date.isoformat()}` — raw label: `{m.raw_label}`{suffix}")
    lines += [
        "",
        "The raw label is included so review is a comparison against the live "
        "page, not an act of faith (spec008 R-02).",
        "",
        "This PR only inserts dates and bumps `calendar_provenance` — see the "
        "diff. Nothing else in the file should have changed.",
    ]
    return "\n".join(lines)


def _moved_meeting_issue_body(plan: AutomationPlan) -> str:
    lines = [
        "The following date(s) in `config/fomc.yml`'s `meeting_dates` are no "
        "longer listed on the Fed's calendar page:",
        "",
    ]
    lines += [f"- `{d.isoformat()}`" for d in plan.moved_dates]
    lines += [
        "",
        "This usually means a meeting moved. **Nothing has been edited** — "
        "spec008 Decision 2 explicitly never removes a configured date "
        "automatically. Verify against the live page "
        f"({FOMC_CALENDAR_URL}) before removing it by hand.",
    ]
    return "\n".join(lines)


def _parser_issue_body(*, error: str | None, saved_html_path: str | None) -> str:
    lines = [
        "The FOMC calendar scraper could not confirm its own output this run.",
        "",
    ]
    if error:
        lines += ["Error:", "", "```", error, "```", ""]
    else:
        lines += [
            "The page parsed, but the advance-notice sentence "
            "(\"A two-day meeting is scheduled for ...\") that announces the "
            "furthest-out meeting did not match. This is the single most "
            "fragile input this parser has — see "
            "`docs/handoffs/fomc_calendar_scraper.md` §11 and "
            "`catalogs/fomc_calendar.py`'s `_ADVANCE_NOTICE_RE`.",
            "",
        ]
    if saved_html_path:
        lines.append(f"The HTML this run actually received was uploaded as a workflow artifact ({saved_html_path}).")
    lines += [
        "",
        "See `docs/handoffs/fomc_calendar_scraper.md` §4.1 for the parsing "
        "contract this needs to keep matching.",
    ]
    return "\n".join(lines)


def _runway_issue_body(plan: AutomationPlan) -> str:
    lines = [
        f"Runway remaining on `config/fomc.yml`: **{plan.runway_days_remaining} days** "
        f"(level: `{plan.runway_issue_level}`).",
        "",
        f"Configured through: `{plan.configured_through.isoformat() if plan.configured_through else 'n/a'}`.",
        "",
        "The Fed has not published a new meeting beyond this yet (or this run "
        "hasn't seen it) — this is spec008 Decision 4's runway ladder, "
        "independent of whether there's a PR to review right now.",
    ]
    if plan.runway_issue_level == "expired":
        lines.append(
            "\n**`gold.fomc_probability` / `gold.fomc_meeting_path` are "
            "already emitting nothing.** This is no longer advance warning."
        )
    return "\n".join(lines)


# ---- execution (git/gh) ------------------------------------------------------
#
# Runner is injectable so tests can verify exactly which commands this script
# would issue without a real repo, real git, or real gh -- same reasoning as
# every fetch_transport / session parameter elsewhere in this codebase.

CommandResult = subprocess.CompletedProcess
Runner = Callable[[list[str]], "CommandResult"]


def default_runner(cmd: list[str]) -> CommandResult:
    return subprocess.run(  # noqa: S603 -- fixed, non-shell argv, no user input
        cmd, capture_output=True, text=True, check=False, cwd=REPO_ROOT
    )


def _print_only_runner(cmd: list[str]) -> CommandResult:
    """Never executes anything. Prints the command, and returns an empty-but-
    successful result so callers that parse JSON output (`gh pr list`, `gh
    issue list`) see "nothing exists yet" rather than crashing on empty
    stdout -- so a dry run always takes the "create" branch, never "update",
    which is the more informative one to show."""
    print(f"[dry-run] would run: {shlex.join(cmd)}", file=sys.stderr)
    stdout = "[]" if cmd[:2] in (["gh", "pr"], ["gh", "issue"]) and "list" in cmd else ""
    return subprocess.CompletedProcess(cmd, returncode=0, stdout=stdout, stderr="")


class AutomationError(RuntimeError):
    """Raised when a planned git/gh action fails to execute."""


def _run(runner: Runner, cmd: list[str], *, allow_failure_text: str | None = None) -> CommandResult:
    result = runner(cmd)
    if result.returncode != 0:
        combined = f"{result.stdout}\n{result.stderr}"
        if allow_failure_text and allow_failure_text in combined:
            return result
        raise AutomationError(
            f"command failed ({result.returncode}): {' '.join(cmd)}\n{combined}"
        )
    return result


def _existing_pr_number(runner: Runner, branch: str) -> int | None:
    result = _run(
        runner,
        ["gh", "pr", "list", "--head", branch, "--state", "open", "--json", "number"],
    )
    numbers = json.loads(result.stdout or "[]")
    return numbers[0]["number"] if numbers else None


def _existing_issue_number(runner: Runner, title: str) -> int | None:
    result = _run(
        runner,
        [
            "gh", "issue", "list", "--search", f'"{title}" in:title',
            "--state", "open", "--json", "number",
        ],
    )
    numbers = json.loads(result.stdout or "[]")
    return numbers[0]["number"] if numbers else None


def _open_or_update_issue(
    runner: Runner, *, title: str, body: str, labels: list[str]
) -> None:
    existing = _existing_issue_number(runner, title)
    if existing is not None:
        _run(runner, ["gh", "issue", "comment", str(existing), "--body", body])
        for label in labels:
            _run(runner, ["gh", "issue", "edit", str(existing), "--add-label", label])
    else:
        cmd = ["gh", "issue", "create", "--title", title, "--body", body]
        for label in labels:
            cmd += ["--label", label]
        _run(runner, cmd)


def apply_and_open_pr(
    plan: AutomationPlan, *, config_path: Path, runner: Runner, dry_run: bool = False
) -> None:
    original = config_path.read_text(encoding="utf-8")
    updated = apply_calendar_refresh(
        original,
        plan.new_meetings,
        verified_by="scraper",
        published_through=plan.parsed_through,
    )
    if updated == original:
        return  # defensive: shouldn't happen when propose_pr is True

    if dry_run:
        print(f"[dry-run] would write {config_path}:", file=sys.stderr)
        print(updated, file=sys.stderr)
    else:
        config_path.write_text(updated, encoding="utf-8")

    _run(runner, ["git", "checkout", "-B", REFRESH_BRANCH])
    _run(runner, ["git", "add", os.path.relpath(config_path, REPO_ROOT)])
    _run(
        runner,
        ["git", "commit", "-m", _pr_title(plan)],
        allow_failure_text="nothing to commit",
    )
    _run(runner, ["git", "push", "-u", "origin", REFRESH_BRANCH, "--force"])

    title = _pr_title(plan)
    body = _pr_body(plan)
    existing = _existing_pr_number(runner, REFRESH_BRANCH)
    if existing is not None:
        _run(runner, ["gh", "pr", "edit", str(existing), "--title", title, "--body", body])
    else:
        _run(
            runner,
            ["gh", "pr", "create", "--title", title, "--body", body,
             "--head", REFRESH_BRANCH, "--base", "main"],
        )


def execute_plan(
    plan: AutomationPlan,
    *,
    config_path: Path,
    runner: Runner,
    parse_error: str | None = None,
    saved_html_path: str | None = None,
    dry_run: bool = False,
) -> None:
    """Act on a plan. Each branch is independent -- see plan_actions's docstring.

    ``dry_run`` only guards the one thing ``runner`` can't: the direct
    ``config_path.write_text`` call. Every git/gh command still flows through
    ``runner``, so passing a print-only runner (see ``_print_only_runner``)
    is what actually makes a dry run touch nothing -- this flag alone would
    still let every command execute for real.
    """
    if plan.propose_pr:
        apply_and_open_pr(plan, config_path=config_path, runner=runner, dry_run=dry_run)
    if plan.open_moved_meeting_issue:
        _open_or_update_issue(
            runner,
            title=MOVED_MEETING_ISSUE_TITLE,
            body=_moved_meeting_issue_body(plan),
            labels=["fomc-calendar"],
        )
    if plan.open_parser_issue:
        _open_or_update_issue(
            runner,
            title=PARSER_ISSUE_TITLE,
            body=_parser_issue_body(error=parse_error, saved_html_path=saved_html_path),
            labels=["fomc-calendar", "broken-parser"],
        )
    if plan.runway_issue_level is not None:
        _open_or_update_issue(
            runner,
            title=RUNWAY_ISSUE_TITLE,
            body=_runway_issue_body(plan),
            labels=RUNWAY_ISSUE_LABELS[plan.runway_issue_level],
        )


# ---- CLI ---------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default=FOMC_CALENDAR_URL)
    p.add_argument("--html-file", type=Path, help="parse this saved HTML instead of fetching")
    p.add_argument("--save-html", type=Path, help="path to save fetched/failed HTML to, for artifact upload")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p.add_argument("--backend", choices=VALID_BACKENDS, default="auto")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument(
        "--live", action="store_true",
        help="actually run git/gh commands (default: --dry-run behavior; see below)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="compute and print the plan and the commands that would run, execute nothing (default)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    live = args.live and not args.dry_run

    parse_error: str | None = None
    saved_html_path: str | None = None
    meetings: list[FOMCMeeting] = []

    try:
        if args.html_file:
            html = args.html_file.read_text(encoding="utf-8")
        else:
            html = fetch_calendar_html(
                args.url, backend=args.backend, timeout_seconds=args.timeout
            )
            if args.save_html:
                args.save_html.write_text(html, encoding="utf-8")
                saved_html_path = str(args.save_html)
        meetings = parse_fomc_calendar(html)
    except FOMCScrapeError as exc:
        parse_error = str(exc)
        if args.save_html and args.save_html.exists():
            saved_html_path = str(args.save_html)
    except FileNotFoundError as exc:
        parse_error = str(exc)

    configured = _configured_dates(args.config)

    if parse_error is not None:
        plan = AutomationPlan(
            propose_pr=False,
            new_meetings=(),
            open_moved_meeting_issue=False,
            open_parser_issue=True,
            moved_dates=(),
            runway_issue_level=(
                lvl if (lvl := runway_level(configured)) != "ok" else None
            ),
            runway_days_remaining=runway_days(configured),
            parsed_through=None,
            configured_through=max(configured) if configured else None,
        )
    else:
        plan = plan_actions(meetings, configured)

    print(
        json.dumps(
            {
                "propose_pr": plan.propose_pr,
                "new_meeting_count": len(plan.new_meetings),
                "open_moved_meeting_issue": plan.open_moved_meeting_issue,
                "open_parser_issue": plan.open_parser_issue,
                "runway_issue_level": plan.runway_issue_level,
                "runway_days_remaining": plan.runway_days_remaining,
                "is_quiet": plan.is_quiet,
                "parse_error": parse_error,
            },
            indent=2,
        ),
        file=sys.stderr,
    )

    runner = default_runner if live else _print_only_runner
    if not live:
        print("[dry-run] simulating -- no config write, no real git/gh commands", file=sys.stderr)

    try:
        execute_plan(
            plan,
            config_path=args.config,
            runner=runner,
            parse_error=parse_error,
            saved_html_path=saved_html_path,
            dry_run=not live,
        )
    except AutomationError as exc:
        print(f"ERROR executing plan: {exc}", file=sys.stderr)
        return 2

    # AC-006: a broken parser must make the job itself go red, independent of
    # (and in addition to) the parser-problem issue plan_actions already
    # opened/would open above.
    return 1 if parse_error is not None else 0


if __name__ == "__main__":
    raise SystemExit(main())
