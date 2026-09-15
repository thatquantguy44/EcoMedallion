"""Shared HTTP transport and the source-client contract.

This module holds the parts of a data-source client that are *not* specific to
any one API: a client-side rate limiter, retry with exponential backoff +
jitter, and a single ``_request`` engine. A concrete source (FRED, BLS, ...)
subclasses :class:`HTTPSource` and overrides only the source-specific hooks —
``_default_query`` (auth), ``_error_detail`` (error body shape), and the
``source_name`` / ``error_cls`` class attributes.

The :class:`SourceClient` protocol is the contract the pipeline depends on:
``get_observations`` (fetch a raw payload) plus ``normalize`` (map that payload
into the canonical silver row schema). Keeping the protocol tiny is deliberate —
it is the entire surface the orchestrator needs, so any new source that
implements it drops straight into the existing Bronze/Silver/Gold flow.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Optional, Protocol, runtime_checkable

try:  # requests is a light, ubiquitous dependency; import guarded for clarity.
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

log = logging.getLogger("fred_pipeline.sources")

# HTTP statuses worth retrying (transient/server-side/throttling).
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class SourceError(RuntimeError):
    """Base error for any data-source client (carries an optional HTTP status)."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class RateLimiter:
    """Token-free rate limiter based on a minimum inter-call interval.

    Safe to share across threads: the whole wait-then-stamp sequence is
    serialized under a lock, so concurrent extraction workers still respect a
    single aggregate ``per_minute`` ceiling against the upstream API.
    """

    per_minute: int
    _last_call: float = 0.0
    _sleep: Callable[[float], None] = time.sleep
    _now: Callable[[], float] = time.monotonic
    _lock: "threading.Lock" = field(default_factory=threading.Lock)

    def acquire(self) -> None:
        if self.per_minute <= 0:
            return
        min_interval = 60.0 / self.per_minute
        with self._lock:
            elapsed = self._now() - self._last_call
            if elapsed < min_interval:
                self._sleep(min_interval - elapsed)
            self._last_call = self._now()


@runtime_checkable
class SourceClient(Protocol):
    """The contract every source presents to the pipeline.

    ``get_observations`` returns the *raw* upstream payload (archived verbatim
    in Bronze). ``normalize`` maps that payload into the canonical silver row
    schema (see :data:`fred_pipeline.transform.SILVER_COLUMNS`) so DQ, the
    Silver MERGE, and Gold are source-agnostic.
    """

    source_name: str

    def get_observations(self, series_id: str, **kwargs: Any) -> dict[str, Any]: ...

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = True,
        source: str = "fred",
    ) -> list[dict[str, Any]]: ...


class HTTPSource:
    """Retrying, rate-limited HTTP transport shared by all source clients.

    Subclasses set ``source_name``/``error_cls`` and override ``_default_query``
    (params injected into every request, e.g. an API key) and, if needed,
    ``_error_detail``. The ``session`` is injectable so tests can supply a fake
    without patching globals.
    """

    source_name: str = "source"
    error_cls: type[SourceError] = SourceError
    retryable_status: frozenset[int] = RETRYABLE_STATUS

    def __init__(
        self,
        *,
        base_url: str,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 120,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._sleep = sleep
        self._rate_limiter = RateLimiter(rate_limit_per_minute, _sleep=sleep)
        if session is not None:
            self._session = session
        elif requests is not None:
            self._session = requests.Session()
        else:  # pragma: no cover
            raise self.error_cls(
                "`requests` is not installed and no session provided"
            )

    # ---- source-specific hooks ------------------------------------------

    def _default_query(self) -> dict[str, Any]:
        """Params merged into every request (e.g. auth key). Override per source."""
        return {}

    def _error_detail(self, resp: Any) -> str:
        """Human-readable detail pulled from an error response body."""
        try:
            body = resp.json()
            if isinstance(body, dict):
                return str(body.get("error_message", body))
            return str(body)
        except Exception:
            return getattr(resp, "text", "<no body>")

    def _request_headers(self) -> dict[str, str]:
        """HTTP headers sent on every request (e.g. a required User-Agent).
        Empty by default; override per source."""
        return {}

    # ---- transport ------------------------------------------------------

    def _request(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        method: str = "GET",
        as_text: bool = False,
        as_bytes: bool = False,
    ) -> Any:
        """Fetch with retry + rate limiting. Returns parsed JSON by default; with
        ``as_text=True`` returns the raw response body as a string (for CSV
        sources like Stooq / ETF-holdings files); with ``as_bytes=True`` returns
        the raw response body as bytes (for binary downloads like French's ZIP
        archives)."""
        if endpoint.startswith(("http://", "https://")):
            url = endpoint
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
        query = {**params, **self._default_query()}

        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            self._rate_limiter.acquire()
            try:
                resp = self._send(url, query, method)
            except Exception as exc:  # network error, DNS, timeout, etc.
                last_exc = exc
                self._backoff(attempt)
                continue

            status = getattr(resp, "status_code", 200)
            if status == 200:
                if as_bytes:
                    return resp.content
                return resp.text if as_text else resp.json()
            if status in self.retryable_status and attempt < self.max_retries:
                last_exc = self.error_cls(
                    f"HTTP {status} from {endpoint}", status
                )
                self._backoff(attempt, resp)
                continue
            # Non-retryable (e.g. 400 bad id, 403 bad key) or retries exhausted.
            detail = self._error_detail(resp)
            raise self.error_cls(
                f"{self.source_name} API error on {endpoint} "
                f"(HTTP {status}): {detail}",
                status,
            )

        raise self.error_cls(
            f"{self.source_name} API request to {endpoint} failed after "
            f"{self.max_retries + 1} attempts: {last_exc}"
        )

    def _send(self, url: str, query: dict[str, Any], method: str) -> Any:
        headers = self._request_headers() or None
        if method == "GET":
            return self._session.get(
                url, params=query, timeout=self.timeout, headers=headers
            )
        return self._session.post(
            url, json=query, timeout=self.timeout, headers=headers
        )

    def _backoff(self, attempt: int, resp: Any = None) -> None:
        retry_after = self._retry_after_seconds(resp)
        if retry_after is not None:
            self._sleep(min(retry_after, 300.0))
            return
        # Exponential backoff with full jitter, capped at 30s.
        delay = min(2 ** attempt, 30) + random.uniform(0, 0.5)
        self._sleep(delay)

    def _retry_after_seconds(self, resp: Any = None) -> Optional[float]:
        if resp is None:
            return None
        headers = getattr(resp, "headers", None) or {}
        raw = None
        if hasattr(headers, "get"):
            raw = headers.get("Retry-After") or headers.get("retry-after")
        if raw is None:
            return None
        text = str(raw).strip()
        if not text:
            return None
        try:
            return max(0.0, float(text))
        except ValueError:
            pass
        try:
            dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                return None
            return max(0.0, dt.timestamp() - time.time())
        except Exception:
            return None
