"""BLS (Bureau of Labor Statistics) source client — a second implementation of
the :class:`SourceClient` contract, proving the abstraction holds.

BLS differs from FRED in three ways, and *only* those three live here:

  * **Auth** — a ``registrationkey`` query param (optional; keyless works at a
    lower daily quota) instead of FRED's ``api_key`` + ``file_type``.
  * **Errors** — BLS returns HTTP 200 even for a logically failed request, with
    ``status != "REQUEST_SUCCEEDED"`` and a ``message`` list; ``get_observations``
    surfaces that as an error.
  * **Response shape** — observations are nested under
    ``Results.series[].data[]`` and dated by ``year`` + ``period`` (``M01``..
    ``M12``, ``Q01``.. , ``S01``/``S02``, ``A01``) rather than an ISO ``date``.
    ``normalize`` maps that into the *same* canonical silver rows FRED produces,
    so DQ / Silver MERGE / Gold don't know or care which source a row came from.

Everything else — rate limiting, retry/backoff, the request engine — is
inherited unchanged from :class:`fred_pipeline.sources.base.HTTPSource`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.bls")


class BLSAPIError(SourceError):
    """Raised when the BLS API returns an unrecoverable error."""


def _bls_period_to_date(year: Any, period: Any) -> str | None:
    """Map a BLS (year, period) to an ISO observation date (period start).

    Returns ``None`` for periods we don't ingest as points — notably ``M13``
    (annual average) and anything unrecognized.
    """
    if not year or not period or len(str(period)) < 3:
        return None
    kind, num = str(period)[0], str(period)[1:]
    try:
        n = int(num)
        y = int(year)
    except (TypeError, ValueError):
        return None
    if kind == "M":
        if n == 13:  # annual average, not a monthly point
            return None
        month = n
    elif kind == "Q":
        month = (n - 1) * 3 + 1
    elif kind == "S":
        month = 1 if n == 1 else 7
    elif kind == "A":
        month = 1
    else:
        return None
    if not 1 <= month <= 12:
        return None
    return f"{y:04d}-{month:02d}-01"


def normalize_bls_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: str | None = None,
    ingested_at: str | None = None,
    source: str = "bls",
) -> list[dict[str, Any]]:
    """Convert a raw BLS timeseries payload into canonical silver rows.

    Produces exactly the schema in :data:`fred_pipeline.transform.SILVER_COLUMNS`.
    BLS's single response carries only the latest values (no real-time/vintage
    window), so ``realtime_start``/``realtime_end`` are blanked — the same
    convention FRED's non-vintage series use, collapsing the MERGE key to
    ``(series_id, observation_date)`` so re-runs update in place.
    """
    ingested_at = ingested_at or _utc_now_iso()
    series_blocks = (payload.get("Results") or {}).get("series") or []
    rows: list[dict[str, Any]] = []
    for block in series_blocks:
        for obs in block.get("data") or []:
            obs_date = _bls_period_to_date(obs.get("year"), obs.get("period"))
            if not obs_date:
                continue
            raw_value = obs.get("value")
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


class BLSClient(HTTPSource):
    """Retrying, rate-limited BLS API client (v2 public data API)."""

    source_name = "BLS"
    error_cls = BLSAPIError

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.bls.gov/publicAPI/v2",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        # BLS v2 default: ~500 requests/day per registered key.
        rate_limit_per_minute: int = 25,
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
        # Keyless requests are allowed (lower quota); only send a key if set.
        return {"registrationkey": self.api_key} if self.api_key else {}

    def _error_detail(self, resp: Any) -> str:
        try:
            body = resp.json()
            msg = body.get("message")
            if msg:
                return "; ".join(msg) if isinstance(msg, list) else str(msg)
            return str(body)
        except ValueError:
            return getattr(resp, "text", "<no body>")

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        return f"timeseries/data/{series_id}"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        start_year: str | None = None,
        end_year: str | None = None,
        observation_start: str | None = None,
        observation_end: str | None = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch a single series' observations, returning the raw JSON payload.

        Accepts ``observation_start``/``observation_end`` (ISO dates) so the
        pipeline's extract call signature carries over unchanged; the year is
        derived from them. FRED-only kwargs (``realtime_start`` etc.) are
        accepted and ignored — BLS has no point-in-time window here.
        """
        if observation_start and not start_year:
            start_year = str(observation_start)[:4]
        if observation_end and not end_year:
            end_year = str(observation_end)[:4]
        if start_year and not end_year:
            # BLS rejects requests that send startyear without endyear. The
            # pipeline's incremental plan often supplies only observation_start,
            # so bound the request through the current year.
            end_year = str(datetime.now(timezone.utc).year)
        elif not start_year:
            # Full load (no observation_start supplied): BLS without year params
            # returns only the current year, which leaves all prior months
            # permanently null because the incremental restate window never
            # reaches back past the first date stored. Default to a 20-year
            # window so first-ingestion captures meaningful history.
            end_year = str(datetime.now(timezone.utc).year)
            start_year = str(datetime.now(timezone.utc).year - 19)

        params: dict[str, Any] = {}
        if start_year:
            params["startyear"] = str(start_year)
        if end_year:
            params["endyear"] = str(end_year)

        payload = self._request(f"timeseries/data/{series_id}", params)
        status = payload.get("status")
        if status and status != "REQUEST_SUCCEEDED":
            messages = payload.get("message") or []
            detail = (
                "; ".join(messages) if isinstance(messages, list) else str(messages)
            )
            raise BLSAPIError(f"BLS request not processed for {series_id!r}: {detail}")
        return payload

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        track_vintage: bool = False,  # BLS single-fetch carries no vintages
        source: str = "bls",
    ) -> list[dict[str, Any]]:
        return normalize_bls_observations(
            series_id, payload, run_id=run_id, source=source
        )
