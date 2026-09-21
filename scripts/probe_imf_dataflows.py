#!/usr/bin/env python3
"""Probe IMF's SDMX 3.0 dataflow catalogue and split it into stable vs.
vintage-rotating dataflow ids -- prep work for specs/spec006 (§5.1, §9 #5).

specs/spec006/README.md records a live finding from 2026-09-12: IMF's
catalogue has 191 dataflows, of which only 77 carry stable ids -- the other
114 are month-stamped "vintage" flows (e.g. ``MFS_FMP_2026_JAN_VINTAGE``,
annotated ``historySettingType: FULL_HISTORY``) that rotate every month and
therefore fail spec006's stable-identifier gate (§4.1.4). That finding was
never written down as an actual id list, and no fixture of the raw response
exists anywhere in this repo -- this script exists so the next session with
network access can regenerate that list mechanically instead of guessing at
it, and so the classification logic (what counts as "vintage") is pinned
down in code and reviewable, not re-derived from memory each time.

This is a **diagnostic probe, not a finished client**. IMF's exact SDMX 3.0
JSON structure-message shape was not independently re-confirmed while
writing this script (network access to api.imf.org was blocked in every
environment this has been worked from so far) -- the parser below tries the
shape spec006 describes, and falls back to dumping raw top-level JSON keys
if that shape doesn't match, so a human can adapt it quickly rather than the
script failing silently or asserting something false.

**Also probes one actual data query** (default dataflow: COFER), because
spec006 §5.1 records "Data query verified (COFER 2024 -> real reserve
values)" from the 2026-09-12 live session, but nobody saved the response --
so that verification isn't reproducible or reviewable, only asserted. SDMX
3.0's REST addressing is `/data/dataflow/{agency}/{id}/{version}/{key}`, and
the agency that actually maintains a given flow isn't always `IMF` (the same
"agency isn't always the obvious one" trap spec006 §5.1 already documents
for OECD's catalogue), so this tries a small set of plausible agency ids
rather than assuming one. This half is even more exploratory than the
dataflow-list probe above: it exists to CAPTURE a real response for the
first time, not to parse one confidently -- unlike ``sources/french.py``'s
CSV format (decades-stable, extremely well-documented), IMF's exact
SDMX-JSON dialect (dimension-index key encoding, attribute array shape) has
real known variation across providers and SDMX-JSON schema versions, so
building `sources/imf.py`'s real ``normalize()`` before this actually
returns something would be encoding a guess as if it were verified.

Never writes to config/ or manifests/ -- prints a summary and (optionally)
saves the raw response + the stable/vintage split to JSON files for later
use as a real fixture and as the input list for building sources/imf.py.

Usage:
    # hit the live API, print a summary, save nothing
    python scripts/probe_imf_dataflows.py

    # also save the raw response and the classified split to disk
    python scripts/probe_imf_dataflows.py --out-dir /tmp/imf_probe

    # try a different base URL (in case api.imf.org's path has moved again --
    # it already moved once, off dataservices.imf.org, per spec006 §5.1)
    python scripts/probe_imf_dataflows.py --base-url https://api.imf.org/external/sdmx/3.0

    # try a different sample dataflow for the data-query half, or skip it
    python scripts/probe_imf_dataflows.py --sample-dataflow BOP
    python scripts/probe_imf_dataflows.py --skip-data-query

Exit codes: 0 = the dataflow-list probe fetched and parsed (even if the
split looks odd -- check the printed summary), 1 = that request failed, 2 =
that response didn't parse as JSON or didn't match any known shape (raw keys
are printed either way). The data-query half's outcome is reported
separately and does not change the exit code -- it is a bonus capture
attempt, not this script's primary contract.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import requests

DEFAULT_BASE_URL = "https://api.imf.org/external/sdmx/3.0"
# IMF serves SDMX 3.0 *JSON* structures (confirmed live 2026-09-12, per
# spec006 §5.1) -- unlike OECD/ECB's SDMX 2.1 XML. The exact Accept-header
# version suffix was not re-verified while writing this script; several
# plausible values are tried in order and the first one that returns JSON
# wins, so a wrong guess here doesn't block the probe.
CANDIDATE_ACCEPT_HEADERS = (
    "application/vnd.sdmx.structure+json;version=2.0.0",
    "application/vnd.sdmx.structure+json;version=1.0.0",
    "application/vnd.sdmx.structure+json",
    "application/json",
)

# The known 2026-09-12 baseline, for a sanity check against a fresh pull --
# not asserted as still-current, just what to compare against.
BASELINE_TOTAL = 191
BASELINE_STABLE = 77
BASELINE_VINTAGE = 114

_VINTAGE_ID_RE = re.compile(r"_\d{4}_[A-Z]{3}_VINTAGE$", re.IGNORECASE)

USER_AGENT = (
    "fred-bronze-to-gold-pipeline/spec006-probe "
    "(https://github.com/thatquantguy44/EcoMedallion; research probe, not "
    "a production client -- contact repo owner)"
)

# SDMX 3.0 data queries also serve JSON in more than one dialect; same
# negotiate-and-take-the-first-that-parses approach as the structure probe.
DATA_ACCEPT_HEADERS = (
    "application/vnd.sdmx.data+json;version=2.0.0",
    "application/vnd.sdmx.data+json;version=1.0.0",
    "application/vnd.sdmx.data+json",
    "application/json",
)

# Agencies known to maintain IMF-published dataflows. Not exhaustive --
# IMF's real maintaining-agency list isn't known without a live structure
# probe (same "the obvious agency isn't always right" trap as OECD's
# catalogue, spec006 §5.1) -- but these are the plausible candidates from
# public documentation, tried in order.
CANDIDATE_AGENCIES = ("IMF.STA", "IMF", "IMF.RES")


def _fetch_sample_data(
    base_url: str, dataflow_id: str, timeout: int
) -> tuple[dict[str, Any], str, str]:
    """Try a handful of (agency, Accept-header) combinations against the
    SDMX 3.0 REST data endpoint for one dataflow, wildcarding version and key
    so the query matches whatever is actually published. Returns
    ``(payload, agency_used, accept_used)``. Raises on total failure.
    """
    last_err: Exception | None = None
    for agency in CANDIDATE_AGENCIES:
        url = f"{base_url.rstrip('/')}/data/dataflow/{agency}/{dataflow_id}/+/all"
        for accept in DATA_ACCEPT_HEADERS:
            try:
                resp = requests.get(
                    url,
                    headers={"Accept": accept, "User-Agent": USER_AGENT},
                    timeout=timeout,
                )
            except requests.RequestException as exc:
                last_err = exc
                continue
            print(
                f"  data query agency={agency!r} Accept={accept!r} -> "
                f"HTTP {resp.status_code}, "
                f"content-type={resp.headers.get('content-type')!r}",
                file=sys.stderr,
            )
            if resp.status_code != 200:
                last_err = RuntimeError(
                    f"HTTP {resp.status_code} for agency={agency!r} "
                    f"Accept={accept!r}: {resp.text[:500]!r}"
                )
                continue
            try:
                return resp.json(), agency, accept
            except ValueError as exc:
                last_err = exc
                continue
    raise RuntimeError(
        f"no (agency, Accept) combination returned parseable JSON for "
        f"dataflow {dataflow_id!r}"
    ) from last_err


def _fetch_dataflows(base_url: str, timeout: int) -> tuple[dict[str, Any], str]:
    """Try the dataflow-list structure endpoint with each candidate Accept
    header until one returns parseable JSON. Returns (payload, accept_used).
    Raises requests.RequestException / ValueError on total failure.
    """
    # SDMX 3.0 REST structure query: /structure/dataflow/{agency}/{id}/{ver}
    # "all" wildcards every segment -- the same convention ecb_discovery.py
    # / oecd.py use for ECB/OECD's 2.1 dataflow-list queries.
    url = f"{base_url.rstrip('/')}/structure/dataflow/all/all/all"
    last_err: Exception | None = None
    for accept in CANDIDATE_ACCEPT_HEADERS:
        try:
            resp = requests.get(
                url,
                headers={"Accept": accept, "User-Agent": USER_AGENT},
                params={"references": "none"},
                timeout=timeout,
            )
        except requests.RequestException as exc:  # network/proxy failure
            last_err = exc
            continue
        print(f"  tried Accept={accept!r} -> HTTP {resp.status_code}, "
              f"content-type={resp.headers.get('content-type')!r}",
              file=sys.stderr)
        if resp.status_code != 200:
            last_err = RuntimeError(
                f"HTTP {resp.status_code} for Accept={accept!r}: "
                f"{resp.text[:500]!r}"
            )
            continue
        try:
            return resp.json(), accept
        except ValueError as exc:
            last_err = exc
            continue
    raise RuntimeError(
        f"no candidate Accept header returned parseable JSON from {url}"
    ) from last_err


def _iter_dataflow_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Best-effort extraction of dataflow records from an SDMX-JSON
    structure message. Tries the shape spec006 implies (a top-level
    ``data.dataflows`` array of {id, name, annotations: [...]}); if that
    doesn't match, returns [] so the caller falls back to dumping raw keys.
    """
    data = payload.get("data")
    if isinstance(data, dict):
        flows = data.get("dataflows")
        if isinstance(flows, list):
            return [f for f in flows if isinstance(f, dict)]
    # Some SDMX-JSON structure messages nest under "structures" instead of
    # "data" -- try that shape too before giving up.
    structures = payload.get("structures")
    if isinstance(structures, dict):
        flows = structures.get("dataflows")
        if isinstance(flows, list):
            return [f for f in flows if isinstance(f, dict)]
    return []


def _is_vintage(record: dict[str, Any]) -> bool:
    flow_id = str(record.get("id") or "")
    if _VINTAGE_ID_RE.search(flow_id):
        return True
    annotations = record.get("annotations")
    if isinstance(annotations, list):
        for ann in annotations:
            if not isinstance(ann, dict):
                continue
            text = " ".join(
                str(ann.get(k, "")) for k in ("title", "text", "type")
            ).upper()
            if "VINTAGE" in text:
                return True
    return False


def _probe_sample_data_query(args: argparse.Namespace) -> None:
    """Best-effort capture of one real data query. Never raises past this
    function and never affects the script's exit code -- see the module
    docstring for why this is a capture attempt, not a parser to trust yet.
    """
    if args.skip_data_query:
        return
    print(
        f"\nProbing a data query for dataflow {args.sample_dataflow!r} "
        f"(agencies tried: {list(CANDIDATE_AGENCIES)}) ...",
        file=sys.stderr,
    )
    try:
        payload, agency_used, accept_used = _fetch_sample_data(
            args.base_url, args.sample_dataflow, args.timeout
        )
    except Exception as exc:  # noqa: BLE001 -- bonus probe, report and move on
        print(f"Data query FAILED: {exc}", file=sys.stderr)
        return

    print(
        f"Data query fetched OK: agency={agency_used!r}, Accept={accept_used!r}. "
        f"Top-level keys: {list(payload.keys())}",
        file=sys.stderr,
    )
    if args.out_dir:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        out_path = args.out_dir / f"imf_{args.sample_dataflow.lower()}_data_probe_raw.json"
        out_path.write_text(json.dumps(payload, indent=2))
        print(
            f"Saved raw data-query response to {out_path} -- this is the "
            f"real fixture the module docstring says is missing; use it to "
            f"write sources/imf.py's normalize() against reality instead of "
            f"the general SDMX-JSON spec.",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--out-dir", type=Path, default=None,
        help="save the raw response and classified split as JSON here",
    )
    parser.add_argument(
        "--sample-dataflow", default="COFER",
        help="dataflow id to attempt a real data query against (default: COFER)",
    )
    parser.add_argument(
        "--skip-data-query", action="store_true",
        help="only run the dataflow-list probe, skip the data-query capture attempt",
    )
    args = parser.parse_args()

    _probe_sample_data_query(args)

    print(f"\nProbing {args.base_url} ...", file=sys.stderr)
    try:
        payload, accept_used = _fetch_dataflows(args.base_url, args.timeout)
    except Exception as exc:  # noqa: BLE001 -- top-level probe, report and exit
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1

    print(f"Fetched OK using Accept={accept_used!r}.", file=sys.stderr)

    records = _iter_dataflow_records(payload)
    if not records:
        print(
            "Response parsed as JSON but no known dataflow-list shape "
            "matched. Top-level keys:", list(payload.keys()),
            file=sys.stderr,
        )
        print(
            "-> Inspect the raw payload (rerun with --out-dir) and update "
            "_iter_dataflow_records() with the real shape before trusting "
            "any count below.",
            file=sys.stderr,
        )
        if args.out_dir:
            args.out_dir.mkdir(parents=True, exist_ok=True)
            (args.out_dir / "imf_dataflow_probe_raw.json").write_text(
                json.dumps(payload, indent=2)
            )
            print(f"Saved raw payload to {args.out_dir / 'imf_dataflow_probe_raw.json'}",
                  file=sys.stderr)
        return 2

    stable = []
    vintage = []
    missing_id = 0
    for rec in records:
        flow_id = rec.get("id")
        if not flow_id:
            missing_id += 1
            continue
        (vintage if _is_vintage(rec) else stable).append(flow_id)

    print(f"\nTotal dataflows:   {len(records)}")
    print(f"Stable ids:        {len(stable)}")
    print(f"Vintage-rotating:  {len(vintage)}")
    if missing_id:
        print(
            f"Missing 'id':      {missing_id} (excluded from both lists -- "
            f"a record with no id means _iter_dataflow_records's assumed "
            f"shape is only a partial match; inspect the raw payload)",
            file=sys.stderr,
        )
    print(
        f"\n2026-09-12 baseline was {BASELINE_TOTAL} total / "
        f"{BASELINE_STABLE} stable / {BASELINE_VINTAGE} vintage."
    )
    if (len(records), len(stable), len(vintage)) != (
        BASELINE_TOTAL, BASELINE_STABLE, BASELINE_VINTAGE,
    ):
        print(
            "-> Counts differ from the recorded baseline. Could be a real "
            "catalogue change (IMF added/removed dataflows since) or a "
            "classification-logic gap in this script -- spot-check a few "
            "ids from each bucket before trusting either list.",
            file=sys.stderr,
        )

    if args.out_dir:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        (args.out_dir / "imf_dataflow_probe_raw.json").write_text(
            json.dumps(payload, indent=2)
        )
        (args.out_dir / "imf_stable_dataflow_ids.json").write_text(
            json.dumps(sorted(stable), indent=2)
        )
        (args.out_dir / "imf_vintage_dataflow_ids.json").write_text(
            json.dumps(sorted(vintage), indent=2)
        )
        print(f"\nSaved raw payload + classified id lists under {args.out_dir}",
              file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
