#!/usr/bin/env python3
"""Probe the live sources behind config/mmkt_instruments.yml (specs/spec009).

The build environments this pipeline is developed in cannot reach FRED, so the
series MMKT depends on were assembled from documentation, not checked. This
script is the check: run it once with network access and it tells you, per
configured instrument, whether the FRED id resolves, whether its metadata
agrees with what the config says about it, and whether its latest observation
is fresh. It also captures the one thing spec009 phase 2 cannot be designed
without -- the real field names of the Treasury Fiscal Data auctions dataset.

**The check that matters most is the quoting basis.** config/mmkt_instruments.yml
declares each series as `discount`, `bond_equivalent`, or `money_market`, and
the snapshot's conversion depends on it. A wrong basis does not fail anything:
it silently mis-ranks instruments by a few basis points. FRED states the basis
in a series' title or notes ("..., Discount Basis"; "... Quoted on an
Investment Basis"), so this script reads it back and flags a disagreement.
A series whose text states no basis (SOFR, EFFR, IORB ...) is reported as
`unverifiable`, never as a match.

What this script does NOT do: it never writes to config/ or manifests/, and it
does not activate anything. Activation (`active: true`) is a human decision
after reading the report.

Usage:
    python scripts/probe_mmkt_series.py --fred-key "$FRED_API_KEY"
    python scripts/probe_mmkt_series.py --fred-key "$FRED_API_KEY" --out report.json
    python scripts/probe_mmkt_series.py --skip-treasury      # FRED only

Exit codes: 0 = every configured series resolved and no basis disagreement,
1 = at least one series failed (unresolved id, basis disagreement, wrong
frequency/units, or stale), 2 = could not run (no key / no config).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from typing import Any, Optional

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

FRED_BASE = "https://api.stlouisfed.org/fred"
FISCAL_DATA_AUCTIONS = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    "v1/accounting/od/auctions_query"
)
USER_AGENT = "fred-bronze-to-gold-pipeline spec009 probe (contact: set via env)"

# FRED states a series' quoting basis in its title and/or notes.
_DISCOUNT_MARKERS = ("discount basis", "quoted on a discount basis")
_INVESTMENT_MARKERS = ("investment basis", "quoted on an investment basis", "bond equivalent")

_FREQUENCY_NAMES = {"d": "Daily", "w": "Weekly", "m": "Monthly", "q": "Quarterly"}


def stated_basis(title: str, notes: str = "") -> Optional[str]:
    """The quoting basis FRED's own text states, or ``None`` when it states none.

    ``None`` is deliberate and load-bearing: SOFR's title says nothing about a
    basis, and treating silence as agreement would let a wrong config pass.
    Both markers present is ambiguous and also returns ``None``.
    """
    text = f"{title} {notes}".lower()
    discount = any(m in text for m in _DISCOUNT_MARKERS)
    investment = any(m in text for m in _INVESTMENT_MARKERS)
    if discount == investment:
        return None
    return "discount" if discount else "bond_equivalent"


def classify_series(
    *,
    instrument_id: str,
    series_id: str,
    expected_basis: str,
    max_staleness_days: int,
    meta: Optional[dict[str, Any]],
    last_observation_date: Optional[date],
    today: date,
) -> dict[str, Any]:
    """Turn one series' fetched metadata into a pass/fail record with reasons.

    ``meta`` is the first element of FRED's ``/fred/series`` ``seriess`` array
    (``None`` when the id did not resolve). Pure, so it is tested without a
    network.
    """
    problems: list[str] = []
    record: dict[str, Any] = {
        "instrument_id": instrument_id,
        "series_id": series_id,
        "resolved": meta is not None,
        "expected_basis": expected_basis,
        "stated_basis": None,
        "basis_check": "unverifiable",
        "title": None,
        "units": None,
        "frequency": None,
        "last_observation_date": last_observation_date.isoformat() if last_observation_date else None,
        "staleness_days": None,
    }
    if meta is None:
        record["problems"] = ["id did not resolve on FRED"]
        record["ok"] = False
        return record

    title = str(meta.get("title", ""))
    notes = str(meta.get("notes", ""))
    record["title"] = title
    record["units"] = meta.get("units")
    record["frequency"] = meta.get("frequency_short") or meta.get("frequency")

    if str(meta.get("units", "")).strip().lower() not in ("percent", "%"):
        problems.append(f"units are {meta.get('units')!r}, expected 'Percent'")
    if str(record["frequency"] or "").strip().lower() not in ("d", "daily"):
        problems.append(f"frequency is {record['frequency']!r}, expected daily")

    stated = stated_basis(title, notes)
    record["stated_basis"] = stated
    if stated is None:
        # Overnight/repo rates legitimately state no basis; a series configured as
        # discount/bond_equivalent that states none is worth a human look.
        record["basis_check"] = "unverifiable"
        if expected_basis != "money_market":
            problems.append(
                f"config says {expected_basis!r} but FRED's text states no basis -- verify by hand"
            )
    elif stated == expected_basis:
        record["basis_check"] = "match"
    else:
        record["basis_check"] = "MISMATCH"
        problems.append(f"config says {expected_basis!r} but FRED states {stated!r}")

    if last_observation_date is None:
        problems.append("no observations returned")
    else:
        staleness = (today - last_observation_date).days
        record["staleness_days"] = staleness
        if staleness > max_staleness_days:
            problems.append(f"latest observation is {staleness} days old (limit {max_staleness_days})")

    record["problems"] = problems
    record["ok"] = not problems
    return record


def summarize_auction_fields(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Describe the Fiscal Data auctions dataset from a sample page, asserting nothing.

    spec009 phase 2 needs to know which field is the date key (the Treasury
    client keys on ``record_date``; auctions may not) and which fields carry the
    rate. This reports what is there so the client change is designed from a
    capture, not a guess.
    """
    fields = sorted({key for rec in records for key in rec})
    return {
        "sample_size": len(records),
        "fields": fields,
        "date_like_fields": [f for f in fields if "date" in f.lower()],
        "rate_like_fields": [f for f in fields if any(t in f.lower() for t in ("rate", "yield"))],
        "has_record_date": "record_date" in fields,
    }


def _get(url: str, params: dict[str, Any], timeout: float) -> Any:
    response = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.json()


def fetch_fred(series_id: str, api_key: str, timeout: float) -> tuple[Optional[dict], Optional[date]]:
    """``(series metadata or None, latest observation date or None)``."""
    base = {"api_key": api_key, "file_type": "json"}
    try:
        payload = _get(f"{FRED_BASE}/series", {**base, "series_id": series_id}, timeout)
    except requests.HTTPError as exc:  # FRED answers 400 for an unknown id
        if exc.response is not None and exc.response.status_code in (400, 404):
            return None, None
        raise
    seriess = payload.get("seriess") or []
    if not seriess:
        return None, None
    obs = _get(
        f"{FRED_BASE}/series/observations",
        {**base, "series_id": series_id, "sort_order": "desc", "limit": 5},
        timeout,
    ).get("observations") or []
    latest: Optional[date] = None
    for row in obs:
        if row.get("value") not in (None, ".", ""):
            latest = datetime.strptime(row["date"], "%Y-%m-%d").date()
            break
    return seriess[0], latest


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--fred-key", default=None)
    parser.add_argument("--config", default=None, help="path to mmkt_instruments.yml")
    parser.add_argument("--out", default=None, help="write the JSON report here")
    parser.add_argument("--skip-treasury", action="store_true")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args(argv)

    if requests is None:
        print("requests is required", file=sys.stderr)
        return 2
    import os

    from fred_pipeline.gold_config.mmkt_config import load_mmkt_config

    api_key = args.fred_key or os.environ.get("FRED_API_KEY")
    if not api_key:
        print("no FRED key: pass --fred-key or set FRED_API_KEY", file=sys.stderr)
        return 2
    config = load_mmkt_config(args.config)
    if not config.instruments:
        print("no instruments configured", file=sys.stderr)
        return 2

    today = date.today()
    results = []
    for inst in config.instruments:
        try:
            meta, latest = fetch_fred(inst.series_id, api_key, args.timeout)
        except Exception as exc:  # network / auth: report, keep going
            results.append(
                {
                    "instrument_id": inst.instrument_id,
                    "series_id": inst.series_id,
                    "ok": False,
                    "problems": [f"request failed: {type(exc).__name__}: {exc}"],
                }
            )
            continue
        results.append(
            classify_series(
                instrument_id=inst.instrument_id,
                series_id=inst.series_id,
                expected_basis=inst.basis,
                max_staleness_days=inst.max_staleness_days,
                meta=meta,
                last_observation_date=latest,
                today=today,
            )
        )

    report: dict[str, Any] = {"probed_on": today.isoformat(), "fred": results}

    if not args.skip_treasury:
        try:
            payload = _get(
                FISCAL_DATA_AUCTIONS,
                {"sort": "-auction_date", "page[size]": 25, "format": "json"},
                args.timeout,
            )
            report["treasury_auctions"] = summarize_auction_fields(payload.get("data") or [])
        except Exception as exc:
            report["treasury_auctions"] = {"error": f"{type(exc).__name__}: {exc}"}

    for r in results:
        flag = "ok " if r.get("ok") else "FAIL"
        print(f"{flag} {r['instrument_id']:14} {r['series_id']:14} {'; '.join(r.get('problems') or []) or r.get('basis_check', '')}")
    if "treasury_auctions" in report:
        print("treasury_auctions:", json.dumps(report["treasury_auctions"]))

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, sort_keys=True)
    return 0 if all(r.get("ok") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
