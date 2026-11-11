"""Tiingo source client — free-tier daily prices with dividends & splits.

Tiingo's daily endpoint returns, per ticker, ``close``/``adjClose``/``divCash``/
``splitFactor`` (and OHLCV) in one JSON call — the only free tier that hands
you dividend cash amounts, so Gold can compute **true total return** from raw
inputs (``gold.equity_total_return_index``). Free tier needs a (free) account
key; it is metered (personal use).

Unlike Stooq (one field per ``series_id``), a Tiingo manifest ``series_id`` is
the **bare ticker** (e.g. ``AAPL``): one fetch explodes into several scalar
Silver series — ``<ticker>:close``, ``<ticker>:divCash``,
``<ticker>:splitFactor``, ``<ticker>:adjClose`` — so a ~500-name core list is
~500 requests/day, not 500×fields (the daily-request quota, distinct from the
unique-symbol cap, is the binding one under per-field fetching).

The transport (retry/backoff/rate-limit + the key) is inherited from
:class:`fred_pipeline.sources.base.HTTPSource`; only the JSON shape is
Tiingo-specific.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.tiingo")

# Tiingo daily fields exploded into Silver series (<ticker>:<field>). close /
# divCash / splitFactor are the raw inputs the total-return engine reconstructs
# from; adjClose is Tiingo's own split+dividend-adjusted close, kept for
# reconciliation.
EXPLODE_FIELDS = ("close", "divCash", "splitFactor", "adjClose")

# Unlike FRED, Tiingo's daily endpoint does NOT return full history when
# ``startDate`` is omitted — it silently defaults to only the most recent
# trading day. The pipeline's "full load" contract (SourceClient.
# get_observations(observation_start=None) -> everything) relies on that
# omission meaning "no lower bound", so a full load needs an explicit
# startDate anyway; this is safely before any US equity's actual listing
# date. Incremental "restate last N" reruns are unaffected — those already
# pass a real observation_start from the warehouse watermark.
FULL_HISTORY_START = "1900-01-01"


class TiingoAPIError(SourceError):
    """Raised when Tiingo returns an unrecoverable error."""


def _ticker(series_id: str) -> str:
    """Manifest series_id is the bare ticker; tolerate a stray ``:field`` by
    taking the part before the first colon."""
    t = series_id.partition(":")[0].strip().upper()
    if not t:
        raise TiingoAPIError(f"Tiingo series_id must be a ticker, got {series_id!r}")
    return t


def normalize_tiingo_observations(
    series_id: str,
    payload: Any,
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "tiingo",
) -> list[dict[str, Any]]:
    """Explode a Tiingo daily JSON array into scalar Silver rows — one per
    ``(<ticker>:<field>, date)`` across :data:`EXPLODE_FIELDS`.

    ``payload`` is :meth:`TiingoClient.get_observations`'s envelope
    ``{"format": "tiingo", "ticker": <T>, "data": [ {date, close, divCash,
    splitFactor, adjClose, ...}, ... ]}``. A missing field on a record is
    skipped (not emitted as missing) so a series only carries genuine prints.
    """
    ingested_at = ingested_at or _utc_now_iso()
    if not isinstance(payload, dict):
        return []
    ticker = (payload.get("ticker") or _ticker(series_id)).upper()
    data = payload.get("data")
    if not isinstance(data, list):
        return []

    rows: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        obs_date = str(rec.get("date") or "")[:10]
        if len(obs_date) != 10 or obs_date[4] != "-":
            continue
        for field in EXPLODE_FIELDS:
            if field not in rec:
                continue
            raw_value = rec.get(field)
            value = parse_value(raw_value)
            sid = f"{ticker}:{field}"
            rows.append(
                {
                    "source": source,
                    "series_id": sid,
                    "observation_date": obs_date,
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": value,
                    "raw_value": None if raw_value is None else str(raw_value),
                    "is_missing": value is None,
                    "row_hash": _row_hash(sid, obs_date, "", raw_value),
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                }
            )
    return rows


class TiingoClient(HTTPSource):
    """Retrying, rate-limited Tiingo daily-price client (key required)."""

    source_name = "TIINGO"
    error_cls = TiingoAPIError

    def __init__(
        self,
        api_key: str = "",
        base_url: str = "https://api.tiingo.com",
        *,
        backup_api_keys: Optional[list[str]] = None,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 50,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not api_key:
            raise TiingoAPIError(
                "Tiingo requires an API key (set TIINGO_API_KEY / tiingo_api_key)"
            )
        self.api_key = api_key
        # Additional accounts to rotate to when the active key's hourly quota
        # is exhausted (Tiingo's free-tier quota is tracked per key/account).
        # Popped from the front as each is exhausted, so this only ever moves
        # forward within one client's lifetime — no cycling back to an
        # already-exhausted key within the same run.
        self._backup_keys: list[str] = list(backup_api_keys or [])
        self._key_lock = threading.Lock()
        self._request_state = threading.local()
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _default_query(self) -> dict[str, Any]:
        key = self.api_key
        # Record the actual transport key, rather than a potentially older
        # snapshot taken before another worker rotated it.
        self._request_state.key = key
        return {"token": key, "format": "json"}

    @staticmethod
    def _is_quota_error(exc: "TiingoAPIError") -> bool:
        if exc.status_code == 429:
            return True
        message = str(exc).lower()
        return "hourly request allocation" in message or "quota" in message

    def _request_headers(self) -> dict[str, str]:
        return {"Content-Type": "application/json"}

    def observations_endpoint(self, series_id: str) -> str:
        return f"tiingo/daily/{_ticker(series_id).lower()}/prices"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch a ticker's full daily history once; the envelope is archived
        verbatim in Bronze and exploded per field by :func:`normalize`."""
        ticker = _ticker(series_id)
        params: dict[str, Any] = {
            "startDate": str(observation_start)[:10]
            if observation_start
            else FULL_HISTORY_START,
        }
        if observation_end:
            params["endDate"] = str(observation_end)[:10]
        endpoint = self.observations_endpoint(series_id)
        while True:
            self._request_state.key = self.api_key
            try:
                data = self._request(endpoint, params)
                break
            except TiingoAPIError as exc:
                if not self._is_quota_error(exc):
                    raise
                with self._key_lock:
                    if self.api_key != self._request_state.key:
                        # Another worker already advanced the shared key while
                        # this request was in flight. Retry that active key even
                        # when the last reserve has now been consumed.
                        continue
                    if not self._backup_keys:
                        raise
                    self.api_key = self._backup_keys.pop(0)
                    log.info(
                        "Tiingo quota exhausted; rotating to backup API key "
                        "(%d more in reserve) and retrying %s.",
                        len(self._backup_keys),
                        ticker,
                    )
        if not isinstance(data, list):
            data = []
        return {"format": "tiingo", "ticker": ticker, "data": data}

    def normalize(
        self,
        series_id: str,
        payload: Any,
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,  # market prices carry no vintages
        source: str = "tiingo",
        **_ignored: Any,
    ) -> list[dict[str, Any]]:
        return normalize_tiingo_observations(
            series_id, payload, run_id=run_id, source=source
        )
