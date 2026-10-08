#!/usr/bin/env python3
"""Probe CFTC's Commitments of Traders data to find out what format it's
actually in -- prep work for specs/spec006 (§5.2, §9 #2).

spec006's §5.2 row for CFTC COT is an *evaluation*, not a verified finding
like OECD/IMF got in §5.1: it assumes "fixed-width/CSV bulk files" and
recommends the `sources/ishares.py` bulk-CSV-explode shape (per open
decision #2, resolved 2026-09-12) without anyone having actually pulled a
real file and looked at its columns. That assumption might be wrong in a way
that changes the whole design -- CFTC also runs a modern Socrata-based open
data API (`publicreporting.cftc.gov`), which if it works would be a much
better structural fit (a real REST endpoint with documented fields) than
scraping bulk text files the way this assumed.

This script does NOT assume which path is right. It tries several
candidates -- the Socrata catalog/API route and a couple of legacy
cftc.gov bulk-file URLs -- and reports what each one actually returns
(status, content-type, first lines/bytes), so a human can pick the real
path with evidence instead of the current guess. **None of the URLs below
have been live-verified** -- network access to any cftc.gov / socrata host
was blocked in every environment this has been worked from so far. Treat
every candidate as a hypothesis, per spec006's own verification-caveat
culture, not a confirmed working endpoint (`ishares.py`'s own
``HOLDINGS_URLS`` docstring makes the same caveat for its URLs, for the
same reason: these things move).

Never writes to config/ or manifests/ -- prints a diagnostic report and
(optionally) saves each candidate's raw response for later use as a real
test fixture.

Usage:
    # try every candidate, print a report, save nothing
    python scripts/probe_cftc_format.py

    # also save each candidate's raw response to disk
    python scripts/probe_cftc_format.py --out-dir /tmp/cftc_probe

    # only try the Socrata API candidates (skip the legacy bulk-file guesses)
    python scripts/probe_cftc_format.py --socrata-only

Exit codes: 0 = at least one candidate returned HTTP 200 (check the report
for which), 1 = every candidate failed (network blocked, or every guessed
URL is stale -- both are useful outcomes to know).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import requests

USER_AGENT = (
    "fred-bronze-to-gold-pipeline/spec006-probe "
    "(https://github.com/OWNER/REPOSITORY; research probe, not "
    "a production client -- contact repo owner)"
)


@dataclass
class Candidate:
    name: str
    url: str
    params: dict[str, str] | None = None
    note: str = ""


# Socrata's catalog-search API is a stable, documented entry point across
# every Socrata-hosted open-data site (not CFTC-specific), so this one is
# higher-confidence than the others even though it hasn't been run live: it
# doesn't require guessing a CFTC-specific dataset id, just the domain.
SOCRATA_CANDIDATES = [
    Candidate(
        name="socrata_catalog_search",
        url="https://api.us.socrata.com/api/catalog/v1",
        params={
            "domains": "publicreporting.cftc.gov",
            "q": "commitments of traders",
            "limit": "20",
        },
        note="Socrata's generic dataset-discovery API, scoped to CFTC's open-data domain. "
        "If this returns dataset ids, each one likely has a stable SODA REST endpoint "
        "(a real structural-fit win over bulk files) -- follow up on whichever id looks "
        "like the Legacy/Disaggregated/TFF COT report.",
    ),
]

# Legacy bulk-file guesses -- lower confidence, current as of no verified
# date. CFTC has historically published weekly COT reports as compressed
# historical archives plus a "current report" text file per report type
# under cftc.gov/MarketReports/CommitmentsofTraders/..., but the exact path
# segments are not confirmed here.
LEGACY_CANDIDATES = [
    Candidate(
        name="cftc_gov_market_reports_index",
        url="https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
        note="The human-facing index page, not a data file -- if reachable, read it for the "
        "real current download links rather than trusting the guesses below.",
    ),
    Candidate(
        name="cftc_gov_legacy_futures_only_current",
        url="https://www.cftc.gov/dea/newcot/deacot.txt",
        note="A commonly-referenced 'current Legacy Futures-Only COT' short-format text file "
        "path from CFTC's older file layout -- may well be stale; verify against the "
        "index page above before trusting it.",
    ),
]


def _probe(
    candidate: Candidate, timeout: int
) -> tuple[Candidate, requests.Response | None, Exception | None]:
    try:
        resp = requests.get(
            candidate.url,
            params=candidate.params,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            timeout=timeout,
        )
        return candidate, resp, None
    except requests.RequestException as exc:
        return candidate, None, exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--socrata-only", action="store_true")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="save each candidate's raw response body here",
    )
    args = parser.parse_args()

    candidates = list(SOCRATA_CANDIDATES)
    if not args.socrata_only:
        candidates += LEGACY_CANDIDATES

    any_ok = False
    for candidate in candidates:
        print(f"\n=== {candidate.name} ===")
        print(f"URL:  {candidate.url}")
        if candidate.params:
            print(f"Params: {candidate.params}")
        print(f"Note: {candidate.note}")

        cand, resp, err = _probe(candidate, args.timeout)
        if err is not None:
            print(f"FAILED: {err}")
            continue

        content_type = resp.headers.get("content-type", "")
        print(
            f"HTTP {resp.status_code}, content-type={content_type!r}, "
            f"{len(resp.content)} bytes"
        )

        if resp.status_code == 200:
            any_ok = True
            preview = resp.text[:1000]
            print("--- first ~1000 chars of body ---")
            print(preview)
            print("--- end preview ---")
            if args.out_dir:
                args.out_dir.mkdir(parents=True, exist_ok=True)
                out_path = args.out_dir / f"{candidate.name}.raw"
                out_path.write_bytes(resp.content)
                print(f"Saved full response to {out_path}")
        else:
            print(f"Body (truncated): {resp.text[:300]!r}")

    print("\n" + "=" * 60)
    if any_ok:
        print(
            "At least one candidate returned HTTP 200 -- inspect the "
            "preview(s) above to decide: Socrata REST API vs. bulk-file "
            "download vs. neither actually matches what §5.2 assumed."
        )
        return 0
    print(
        "Every candidate failed. Either network access to these hosts is "
        "blocked in this environment (the common case so far), or every "
        "guessed URL above is stale and needs a fresh check against "
        "cftc.gov / publicreporting.cftc.gov directly."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
