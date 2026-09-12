"""OECD SDMX source client — a keyless `SourceClient`.

OECD publishes its statistics as SDMX 2.1 over a public REST API. A manifest
``series_id`` encodes the three things the REST path needs — the owning
agency, the dataflow, and the series key — separated by colons::

    OECD:OECD.SDD.STES:DSD_STES@DF_CLI:USA.M.LI...AA...H
    └sr┘ └── agency ──┘ └─ dataflow ──┘ └──── key ─────┘

The agency is part of the id because OECD's REST path requires it and it is
*not* always ``OECD`` — the catalogue also serves flows owned by ``ESTAT``,
``IAEG-SDGs``, and per-directorate agencies like ``OECD.SDD.STES``. Keys use
dots, so a colon split is unambiguous.

Why this client looks so much like :mod:`fred_pipeline.sources.ecb`: OECD
serves the same SDMX 2.1 CSV shape the ECB does, down to the ``TIME_PERIOD`` /
``OBS_VALUE`` column names. The helpers below are duplicated rather than
imported from the ECB client on purpose — every source client in this package
is self-contained (``bis.py`` duplicates the same two helpers for the same
reason), so one source's parsing can't break another's.

Two OECD-specific differences worth knowing:

* **No vintages.** OECD's SDMX CSV carries no ``VALID_FROM``/``VALID_TO``, so
  ``realtime_start``/``realtime_end`` are always empty and manifests should
  ship ``vintage_enabled: false``. Point-in-time queries over OECD data
  resolve to latest-revised only.
* **Version is left open.** The path sends an empty version segment
  (``agency,dataflow,``) so OECD serves the current version rather than one
  pinned here — dataflow versions roll (``DSD_STES@DF_CLI`` answered as
  ``4.1`` when this client was written) and pinning would silently 404 on the
  next bump.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from collections.abc import Callable
from datetime import date
from typing import Any

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.oecd")

OECD_BASE_URL = "https://sdmx.oecd.org/public/rest"
# SDMX-CSV rather than the XML default: same rows, far less to parse.
OECD_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"


class OECDAPIError(SourceError):
    """Raised when the OECD API returns an unrecoverable error."""


def parse_oecd_series_id(series_id: str) -> tuple[str, str, str]:
    """Split ``OECD:<agency>:<dataflow>:<key>`` into its three parts.

    The leading ``OECD:`` prefix is optional so a caller can pass either the
    manifest id or the bare coordinates.
    """
    text = (series_id or "").strip()
    if not text:
        raise OECDAPIError("OECD series_id must not be empty")

    parts = text.split(":")
    if parts and parts[0].upper() == "OECD" and len(parts) == 4:
        parts = parts[1:]
    if len(parts) != 3 or not all(p.strip() for p in parts):
        raise OECDAPIError(
            "OECD series_id must be 'OECD:<agency>:<dataflow>:<key>' "
            f"(or '<agency>:<dataflow>:<key>'), got {series_id!r}"
        )
    agency, dataflow, key = (p.strip() for p in parts)
    return agency, dataflow, key


def _date_or_none(year: int, month: int, day: int) -> str | None:
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _oecd_period_to_date(period: Any) -> str | None:
    """Map an SDMX ``TIME_PERIOD`` to an ISO observation date (period start)."""
    if period is None:
        return None
    text = str(period).strip().upper()
    if not text:
        return None

    if re.fullmatch(r"\d{4}", text):
        return f"{text}-01-01"

    match = re.fullmatch(r"(\d{4})-?S([12])", text)
    if match:
        year, half = match.groups()
        return f"{int(year):04d}-{'01' if half == '1' else '07'}-01"

    match = re.fullmatch(r"(\d{4})-?Q([1-4])", text)
    if match:
        year, quarter = match.groups()
        return f"{int(year):04d}-{(int(quarter) - 1) * 3 + 1:02d}-01"

    if re.fullmatch(r"\d{4}-\d{2}", text):
        year, month = text.split("-")
        return _date_or_none(int(year), int(month), 1)

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return date.fromisoformat(text).isoformat()
        except ValueError:
            return None

    return None


def _oecd_period_from_date(value: str | None) -> str | None:
    """Map an ISO date to the coarsest OECD period the API accepts.

    OECD's ``startPeriod``/``endPeriod`` take ``YYYY``, ``YYYY-MM``, or a full
    date. Sending ``YYYY-MM`` is safe across annual, quarterly, and monthly
    flows — the server widens it to the flow's own frequency.
    """
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}", text):
        return text
    match = re.fullmatch(r"(\d{4})-(\d{2})(?:-\d{2})?", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return None


def _csv_records(csv_text: str) -> list[dict[str, str]]:
    """Parse SDMX CSV rows, tolerating blank/comment preamble lines."""
    lines = [
        line
        for line in (csv_text or "").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not lines:
        return []
    return list(csv.DictReader(io.StringIO("\n".join(lines))))


def _field(rec: dict[str, Any], *names: str) -> Any:
    """Case-insensitive field lookup for SDMX CSV column-name variants."""
    lookup = {str(k).upper(): k for k in rec}
    for name in names:
        key = lookup.get(name.upper())
        if key is not None:
            return rec.get(key)
    return None


def normalize_oecd_observations(
    series_id: str,
    payload: dict[str, Any] | str,
    *,
    run_id: str | None = None,
    ingested_at: str | None = None,
    track_vintage: bool = True,
    source: str = "oecd",
) -> list[dict[str, Any]]:
    """Convert raw OECD SDMX CSV into canonical silver rows.

    ``track_vintage`` is accepted for contract parity with the other clients
    but has no effect: OECD publishes no vintage columns, so
    ``realtime_start``/``realtime_end`` are always empty.
    """
    ingested_at = ingested_at or _utc_now_iso()
    csv_text = payload.get("data", "") if isinstance(payload, dict) else str(payload)

    rows: list[dict[str, Any]] = []
    for rec in _csv_records(csv_text):
        if str(_field(rec, "ACTION") or "").strip().lower() == "delete":
            continue
        obs_date = _oecd_period_to_date(_field(rec, "TIME_PERIOD", "PERIOD"))
        if not obs_date:
            continue
        raw_value = _field(rec, "OBS_VALUE", "VALUE")
        value = parse_value(raw_value)
        rows.append(
            {
                "source": source,
                "series_id": series_id,
                "observation_date": obs_date,
                "realtime_start": "",
                "realtime_end": "",
                "value": value,
                "raw_value": None if raw_value is None else str(raw_value),
                "is_missing": value is None,
                "row_hash": _row_hash(series_id, obs_date, "", raw_value),
                "ingested_at": ingested_at,
                "run_id": run_id,
            }
        )
    return rows


class OECDClient(HTTPSource):
    """Retrying, rate-limited OECD SDMX client."""

    source_name = "OECD"
    error_cls = OECDAPIError

    def __init__(
        self,
        base_url: str = OECD_BASE_URL,
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 30,
        sleep: Callable[[float], None] = time.sleep,
    ):
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _request_headers(self) -> dict[str, str]:
        return {"Accept": OECD_ACCEPT}

    def _error_detail(self, resp: Any) -> str:
        text = str(getattr(resp, "text", "") or "").strip()
        if text:
            return text[:500]
        return "<no body>"

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        agency, dataflow, key = parse_oecd_series_id(series_id)
        # Trailing comma = empty version segment; see the module docstring.
        return f"data/{agency},{dataflow},/{key}"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: str | None = None,
        observation_end: str | None = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch one OECD series as raw SDMX CSV."""
        agency, dataflow, key = parse_oecd_series_id(series_id)
        params: dict[str, Any] = {}
        start_period = _oecd_period_from_date(observation_start)
        end_period = _oecd_period_from_date(observation_end)
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        csv_text = self._request(
            self.observations_endpoint(series_id), params, as_text=True
        )
        return {
            "data": csv_text,
            "meta": {
                "series_id": series_id,
                "agency": agency,
                "dataflow": dataflow,
                "key": key,
                "format": "sdmx-csv",
            },
        }

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        track_vintage: bool = True,
        source: str = "oecd",
    ) -> list[dict[str, Any]]:
        return normalize_oecd_observations(
            series_id,
            payload,
            run_id=run_id,
            track_vintage=track_vintage,
            source=source,
        )
