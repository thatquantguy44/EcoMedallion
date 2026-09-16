#!/usr/bin/env python3
"""Refresh the FOMC meeting dates in ``config/fomc.yml`` from the Fed's calendar.

Spec: ``docs/handoffs/fomc_calendar_scraper.md``.

Run this when ``test_fomc_meeting_dates_have_runway`` starts failing — it fires
120 days before ``config/fomc.yml`` expires. An expired list makes
``gold.fomc_probability`` / ``gold.fomc_meeting_path`` emit nothing and the
Power BI Fed Policy Watch report render blank, with no error anywhere.

    # what the Fed currently lists (plain HTTP; the page is static HTML)
    python scripts/scrape_fomc_calendar.py

    # only what the config is missing, paste-ready
    python scripts/scrape_fomc_calendar.py --missing-only

    # CI / cron: non-zero exit when config and page disagree
    python scripts/scrape_fomc_calendar.py --check

    # force a real browser (needed only if the page becomes JS-rendered)
    python scripts/scrape_fomc_calendar.py --backend selenium

    # no browser, no network -- parse a saved page
    python scripts/scrape_fomc_calendar.py --html-file tests/fixtures/fomccalendars.html

Exit codes: 0 = success / in sync, 1 = drift found in --check, 2 = scrape or
parse failure.

This tool never edits ``config/fomc.yml``. That file carries hand-written
provenance comments, and a config driving a rate-path model deserves a human
diff -- so this prints and a person pastes.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import yaml  # noqa: E402

from fred_pipeline.catalogs.fomc_calendar import (  # noqa: E402
    FOMC_CALENDAR_URL,
    VALID_BACKENDS,
    FOMCScrapeError,
    diff_against_config,
    fetch_calendar_html,
    format_yaml_block,
    parse_fomc_calendar,
    runway_days,
    runway_level,
)

DEFAULT_CONFIG = REPO_ROOT / "config" / "fomc.yml"


def _configured_dates(path: Path) -> list[date]:
    if not path.is_file():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    out = []
    for raw in data.get("meeting_dates") or []:
        out.append(raw if isinstance(raw, date) else date.fromisoformat(str(raw)))
    return sorted(out)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Scrape scheduled FOMC meeting dates from federalreserve.gov.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--url", default=FOMC_CALENDAR_URL, help="calendar URL")
    p.add_argument(
        "--html-file",
        type=Path,
        help="parse this saved HTML instead of fetching (no browser, no network)",
    )
    p.add_argument(
        "--save-html",
        type=Path,
        help="write the fetched page here (use it to refresh the test fixture)",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"config to compare against (default: {DEFAULT_CONFIG})",
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the config and the page disagree (CI/cron mode)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help=(
            "with --check, emit the result as JSON on stdout instead of prose "
            "(for automation: parse this, never the human output)"
        ),
    )
    p.add_argument(
        "--missing-only",
        action="store_true",
        help="print only the meetings the config lacks",
    )
    p.add_argument(
        "--from-year",
        type=int,
        help="ignore meetings before this calendar year",
    )
    p.add_argument(
        "--backend",
        choices=VALID_BACKENDS,
        default="auto",
        help=(
            "how to fetch: 'requests' (fast, no browser -- the page is static "
            "HTML), 'selenium' (real browser), or 'auto' (default: requests, "
            "falling back to selenium)"
        ),
    )
    p.add_argument(
        "--no-headless",
        action="store_true",
        help="show the browser (debugging a parse failure; selenium only)",
    )
    p.add_argument("--chromedriver", help="path to chromedriver")
    p.add_argument("--chrome-binary", help="path to the Chrome/Chromium binary")
    p.add_argument(
        "--timeout", type=int, default=30, help="page-load timeout in seconds"
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    def _fetch(backend: str) -> str:
        html = fetch_calendar_html(
            args.url,
            backend=backend,
            headless=not args.no_headless,
            chromedriver_path=args.chromedriver,
            chrome_binary=args.chrome_binary,
            timeout_seconds=args.timeout,
        )
        if args.save_html:
            args.save_html.write_text(html, encoding="utf-8")
            print(f"saved page to {args.save_html}", file=sys.stderr)
        return html

    try:
        if args.html_file:
            html = args.html_file.read_text(encoding="utf-8")
            print(f"parsing {args.html_file} (no browser)", file=sys.stderr)
            meetings = parse_fomc_calendar(html)
        else:
            print(f"fetching {args.url} (backend: {args.backend})", file=sys.stderr)
            html = _fetch(args.backend)
            try:
                meetings = parse_fomc_calendar(html)
            except FOMCScrapeError:
                # A page that fetches fine but parses to nothing is the
                # signature of client-side rendering -- exactly the case
                # Selenium exists for. Retry once through the browser before
                # concluding the parser is broken.
                if args.backend != "auto":
                    raise
                print(
                    "note: page fetched but parsed to 0 meetings; retrying "
                    "with Selenium in case it is now JavaScript-rendered",
                    file=sys.stderr,
                )
                html = _fetch("selenium")
                meetings = parse_fomc_calendar(html)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except FOMCScrapeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.from_year:
        meetings = [m for m in meetings if m.decision_date.year >= args.from_year]

    print(
        f"parsed {len(meetings)} scheduled meetings "
        f"({meetings[0].decision_date} … {meetings[-1].decision_date})",
        file=sys.stderr,
    )

    configured = _configured_dates(args.config)
    diff = diff_against_config(meetings, configured)

    if args.check and args.json:
        configured_all = _configured_dates(args.config)
        print(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "in_sync": diff.in_sync,
                    "missing_from_config": [
                        d.isoformat() for d in diff.missing_from_config
                    ],
                    "absent_upstream": [d.isoformat() for d in diff.absent_upstream],
                    # A stopped-parsing advance notice masquerades as a moved
                    # meeting. Automation must branch on this, not on
                    # absent_upstream alone.
                    "advance_notice_missing": diff.advance_notice_missing,
                    "parsed_count": len(meetings),
                    "parsed_through": (
                        meetings[-1].decision_date.isoformat() if meetings else None
                    ),
                    "configured_through": (
                        max(configured_all).isoformat() if configured_all else None
                    ),
                    "runway_days": runway_days(configured_all),
                    "runway_level": runway_level(configured_all),
                    "yaml_to_add": format_yaml_block(
                        m
                        for m in meetings
                        if m.decision_date in diff.missing_from_config
                    ),
                },
                indent=2,
            )
        )
        return 0 if diff.in_sync else 1

    if args.check:
        if diff.in_sync:
            print(f"in sync with {args.config} (future meetings only)")
            return 0
        if diff.missing_from_config:
            print("Meetings on the Fed calendar but NOT in the config:")
            print(
                format_yaml_block(
                    m for m in meetings if m.decision_date in diff.missing_from_config
                )
            )
        if diff.absent_upstream:
            if diff.advance_notice_missing:
                print(
                    "\nDates in the config that are beyond everything parsed, "
                    "with NO advance-notice sentence found on the page.\n"
                    "This is most likely a PARSER problem, not a moved meeting: "
                    "the Fed announces its\nfurthest-out meeting in prose below "
                    "the last year panel, and if that wording\nchanges the "
                    "horizon silently shrinks to the last panel row. Check the "
                    "page for a\n'a two-day meeting is scheduled for ...' "
                    "sentence before touching the config:"
                )
            else:
                print(
                    "\nDates in the config the Fed no longer lists "
                    "(a meeting may have moved — verify before removing):"
                )
            for d in diff.absent_upstream:
                print(f"  - {d.isoformat()}")
        return 1

    selected = meetings
    if args.missing_only:
        selected = [m for m in meetings if m.decision_date in diff.missing_from_config]
        if not selected:
            print("# config/fomc.yml already has every future meeting listed")
            return 0
        print("# add to meeting_dates in config/fomc.yml:")

    print(format_yaml_block(selected))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
