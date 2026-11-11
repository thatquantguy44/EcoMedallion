"""BEA (Bureau of Economic Analysis) source client.

The BEA API is a single `/data` endpoint whose rows are table lines, so a
manifest `series_id` encodes the coordinates that pin down one line series::

    NIPA:T10101:1:Q
    │    │      │ └ frequency (A / Q / M)
    │    │      └── LineNumber within the table
    │    └───────── TableName
    └────────────── datasetname

BEA-specific bits only: `UserID` auth (required), errors carried in a
`BEAAPI.Error` block (often on HTTP 200), and rows nested under
`BEAAPI.Results.Data` dated by `TimePeriod`. The rest comes from
:class:`fred_pipeline.sources.base.HTTPSource`.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.bea")


class BEAAPIError(SourceError):
    """Raised when the BEA API returns an unrecoverable error."""


def _parse_series_id(series_id: str) -> tuple[str, str, str, str]:
    """Split ``dataset:table:line:frequency`` into its four parts."""
    parts = series_id.split(":")
    if len(parts) != 4 or not all(p.strip() for p in parts):
        raise BEAAPIError(
            f"BEA series_id must be '<dataset>:<table>:<line>:<freq>', "
            f"got {series_id!r}"
        )
    dataset, table, line, freq = (p.strip() for p in parts)
    return dataset, table, line, freq


def _bea_period_to_date(period: Any) -> Optional[str]:
    """Map a BEA ``TimePeriod`` (``2023``, ``2023Q1``, ``2023M01``) to ISO."""
    if not period:
        return None
    p = str(period).strip().upper()
    if "Q" in p:
        year, _, q = p.partition("Q")
        try:
            n = int(q)
        except ValueError:
            return None
        if not 1 <= n <= 4:
            return None
        return f"{int(year):04d}-{(n - 1) * 3 + 1:02d}-01"
    if "M" in p:
        year, _, m = p.partition("M")
        try:
            n = int(m)
        except ValueError:
            return None
        if not 1 <= n <= 12:
            return None
        return f"{int(year):04d}-{n:02d}-01"
    if p.isdigit() and len(p) == 4:
        return f"{p}-01-01"
    return None


def _results_data(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results = (payload.get("BEAAPI") or {}).get("Results")
    if isinstance(results, list):
        results = next((r for r in results if isinstance(r, dict) and "Data" in r), {})
    if not isinstance(results, dict):
        return []
    return results.get("Data") or []


def normalize_bea_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "bea",
) -> list[dict[str, Any]]:
    """Convert a raw BEA payload into canonical silver rows for one table line.

    GetData returns the latest vintage only, so ``realtime_start``/
    ``realtime_end`` are blanked (as with BLS/EIA/Treasury/World Bank).
    """
    ingested_at = ingested_at or _utc_now_iso()
    _dataset, _table, line, _freq = _parse_series_id(series_id)
    rows: list[dict[str, Any]] = []
    for rec in _results_data(payload):
        if str(rec.get("LineNumber")) != line:
            continue
        obs_date = _bea_period_to_date(rec.get("TimePeriod"))
        if not obs_date:
            continue
        raw_value = rec.get("DataValue")
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


class BEAClient(HTTPSource):
    """Retrying, rate-limited BEA API client."""

    source_name = "BEA"
    error_cls = BEAAPIError

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://apps.bea.gov/api",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 100,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not api_key:
            raise BEAAPIError("A BEA API key (UserID) is required (got empty string)")
        self.api_key = api_key
        self._table_cache: dict[tuple[str, ...], dict[str, Any]] = {}
        self._table_lock = threading.Lock()
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _default_query(self) -> dict[str, Any]:
        return {"UserID": self.api_key, "ResultFormat": "JSON"}

    def _error_detail(self, resp: Any) -> str:
        try:
            body = resp.json()
            err = (
                (body.get("BEAAPI") or {}).get("Error")
                if isinstance(body, dict)
                else None
            )
            if isinstance(err, dict):
                return str(err.get("APIErrorDescription") or err)
            return str(body)
        except Exception:
            return getattr(resp, "text", "<no body>")

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        return "data"

    # ---- SourceClient contract ------------------------------------------

    def clear_run_cache(self) -> None:
        """Release table snapshots before/after each pipeline extraction run."""
        with self._table_lock:
            self._table_cache.clear()

    def _checked_request(
        self, params: dict[str, Any], series_id: str
    ) -> dict[str, Any]:
        payload = self._request("data", params)
        beaapi = payload.get("BEAAPI") or {}
        results = beaapi.get("Results") or {}
        err = beaapi.get("Error") or (
            results.get("Error") if isinstance(results, dict) else None
        )
        if isinstance(err, dict):
            raise BEAAPIError(
                f"BEA error for {series_id!r}: {err.get('APIErrorDescription') or err}"
            )
        return payload

    def _monthly_year_chunks(
        self,
        dataset: str,
        table: str,
        series_id: str,
        observation_start: Optional[str],
        observation_end: Optional[str],
    ) -> list[str]:
        """Bound large monthly tables using BEA's published year coverage."""
        if observation_start:
            first = int(str(observation_start)[:4])
            last = datetime.now(timezone.utc).year
        else:
            metadata = self._checked_request(
                {
                    "method": "GetParameterValues",
                    "datasetname": dataset,
                    "ParameterName": "Year",
                },
                series_id,
            )
            values = metadata["BEAAPI"]["Results"].get("ParamValue", [])
            coverage = next((v for v in values if v.get("TableName") == table), {})
            first = int(coverage.get("FirstMonthlyYear") or 0)
            last = int(coverage.get("LastMonthlyYear") or 0)
        if observation_end:
            last = min(last, int(str(observation_end)[:4]))
        if first <= 0 or last < first:
            raise BEAAPIError(f"No valid monthly year coverage for {series_id!r}")
        return [
            ",".join(str(y) for y in range(start, min(start + 10, last + 1)))
            for start in range(first, last + 1, 10)
        ]

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch a NIPA-style table; the target line is selected in normalize."""
        dataset, table, _line, freq = _parse_series_id(series_id)
        params = {
            "method": "GetData",
            "datasetname": dataset,
            "TableName": table,
            "Frequency": freq,
            "Year": "ALL",
        }
        # GetData returns the complete table, so all selected lines share one
        # capture. Serialize the first fetch to avoid concurrent duplicate calls.
        key = (
            dataset,
            table,
            freq,
            str(observation_start or "")[:4],
            str(observation_end or "")[:4],
        )
        with self._table_lock:
            if key not in self._table_cache:
                if dataset == "NIUnderlyingDetail" and freq == "M":
                    years = self._monthly_year_chunks(
                        dataset, table, series_id, observation_start, observation_end
                    )
                    records: list[dict[str, Any]] = []
                    batches = []
                    payload = None
                    for year in years:
                        log.info(
                            "BEA monthly table %s: requesting years %s", table, year
                        )
                        part = self._checked_request(
                            {**params, "Year": year}, series_id
                        )
                        results = part["BEAAPI"]["Results"]
                        rows = _results_data(part)
                        records.extend(rows)
                        batches.append(
                            {
                                "Year": year,
                                "RowCount": len(rows),
                                "UTCProductionTime": results.get("UTCProductionTime"),
                            }
                        )
                        if payload is None:
                            payload = {
                                **part,
                                "BEAAPI": {
                                    **part["BEAAPI"],
                                    "Results": {**results, "Data": records},
                                    "RequestBatches": batches,
                                },
                            }
                    assert payload is not None  # Valid year coverage is nonempty.
                else:
                    payload = self._checked_request(params, series_id)
                self._table_cache[key] = payload
            return self._table_cache[key]

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,  # GetData returns the latest vintage only
        source: str = "bea",
    ) -> list[dict[str, Any]]:
        return normalize_bea_observations(
            series_id, payload, run_id=run_id, source=source
        )
