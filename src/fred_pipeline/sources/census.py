"""US Census Bureau source client (time-series datasets).

Census datasets are queried with dataset-specific predicates, so a manifest
`series_id` encodes the dataset path plus the predicate set that pins down one
series, separated by the first colon::

    timeseries/eits/marts:category_code=44X72,data_type_code=SM,seasonally_adj=yes
    └──── dataset path ───┘ └──────────────── predicates (k=v,...) ─────────────┘

Census-specific bits only: an optional `key` (keyless works at a lower quota),
and a **2-D array** response (`[[header...], [row...], ...]`) rather than an
object. The transport is inherited from
:class:`fred_pipeline.sources.base.HTTPSource`.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.census")

# Time is returned from its required predicate, not requested in ``get``:
# https://api.census.gov/data/timeseries/eits/marts/variables.html
VALUE_COL = "cell_value"
TIME_COL = "time"


class CensusAPIError(SourceError):
    """Raised when the Census API returns an unrecoverable error."""


def _parse_series_id(series_id: str) -> tuple[str, dict[str, str]]:
    """Split ``dataset/path:k1=v1,k2=v2`` into ``(dataset_path, predicates)``."""
    dataset, sep, predicate_str = series_id.partition(":")
    if not sep or not dataset:
        raise CensusAPIError(
            f"Census series_id must be '<dataset_path>:<k=v,...>', got {series_id!r}"
        )
    predicates: dict[str, str] = {}
    for pair in predicate_str.split(","):
        if not pair.strip():
            continue
        k, _, v = pair.partition("=")
        predicates[k.strip()] = v.strip()
    return dataset, predicates


def _census_time_to_date(period: Any) -> Optional[str]:
    """Map a Census ``time`` value (``YYYY`` or ``YYYY-MM``) to an ISO date."""
    if not period:
        return None
    p = str(period).strip()
    if len(p) == 4 and p.isdigit():
        return f"{p}-01-01"
    if len(p) == 7 and p[4] == "-" and p[:4].isdigit() and p[5:].isdigit():
        return f"{p}-01"
    if len(p) == 10:  # already YYYY-MM-DD
        return p
    return None


def normalize_census_observations(
    series_id: str,
    payload: Any,
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "census",
) -> list[dict[str, Any]]:
    """Convert a raw Census 2-D array payload into canonical silver rows."""
    ingested_at = ingested_at or _utc_now_iso()
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    header = payload[0]
    try:
        vi = header.index(VALUE_COL)
        ti = header.index(TIME_COL)
    except (ValueError, AttributeError):
        return []

    rows: list[dict[str, Any]] = []
    for rec in payload[1:]:
        if not isinstance(rec, list) or len(rec) <= max(vi, ti):
            continue
        obs_date = _census_time_to_date(rec[ti])
        if not obs_date:
            continue
        raw_value = rec[vi]
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


class CensusClient(HTTPSource):
    """Retrying, rate-limited Census API client (key optional)."""

    source_name = "CENSUS"
    error_cls = CensusAPIError

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.census.gov/data",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 30,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.api_key = api_key
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _default_query(self) -> dict[str, Any]:
        return {"key": self.api_key} if self.api_key else {}

    def observations_endpoint(self, series_id: str) -> str:
        """The dataset path hit for observations (recorded in Bronze lineage)."""
        dataset, _predicates = _parse_series_id(series_id)
        return dataset

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> Any:
        """Fetch a Census time series, returning the raw 2-D array payload."""
        dataset, predicates = _parse_series_id(series_id)
        params: dict[str, Any] = {"get": VALUE_COL, **predicates}
        bounds = []
        if observation_start:
            bounds.append(f"from {str(observation_start)[:4]}")
        if observation_end:
            bounds.append(f"to {str(observation_end)[:4]}")
        params["time"] = " ".join(bounds) or predicates.get("time", "from 1900")
        return self._request(dataset, params)

    def normalize(
        self,
        series_id: str,
        payload: Any,
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,  # Census time series carry no vintages here
        source: str = "census",
    ) -> list[dict[str, Any]]:
        return normalize_census_observations(
            series_id, payload, run_id=run_id, source=source
        )
