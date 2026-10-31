"""FRED source client (implements the shared :class:`SourceClient` contract).

Only the FRED-specific behavior lives here — auth params, the vintage-date cap
recovery, complete-vintage batching, and discovery paging. The generic HTTP
transport (rate limiting, retry/backoff, ``_request``) is inherited from
:class:`fred_pipeline.sources.base.HTTPSource`.

The client returns *raw* JSON payloads unchanged so Bronze can archive them
verbatim, and ``normalize`` maps a payload into the canonical silver schema.
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import (
    HTTPSource,
    RateLimiter as RateLimiter,
    SourceError,
)

log = logging.getLogger("fred_pipeline.sources.fred")

# Progressive real-time lookbacks (years) tried when a full-vintage request
# exceeds FRED's cap of 2000 vintage dates per request.
VINTAGE_FALLBACK_YEARS = (10, 3, 1)


class FredAPIError(SourceError):
    """Raised when the FRED API returns an unrecoverable error."""


def _is_vintage_cap_error(exc: "FredAPIError") -> bool:
    """True for FRED's 400 'exceeded maximum number of vintage dates' error."""
    return exc.status_code == 400 and "vintage date" in str(exc).lower()


def _coalesce_observations(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse consecutive equal-value vintages per observation date.

    Batched real-time windows clip each row's realtime window to the batch
    bounds, splitting an unchanged value into multiple rows at batch seams. A
    genuine revision changes the value; a clipping artifact repeats it — so
    merging adjacent equal-value rows (extending realtime_end) reconstructs the
    true revision history. Rows are keyed/ordered by (date, realtime_start).
    """
    by_date: dict[str, list[dict[str, Any]]] = {}
    for obs in observations:
        by_date.setdefault(obs.get("date"), []).append(obs)

    out: list[dict[str, Any]] = []
    for _date, rows in by_date.items():
        rows.sort(key=lambda o: o.get("realtime_start", ""))
        run: Optional[dict[str, Any]] = None
        for obs in rows:
            if (
                run is not None
                and obs.get("value") == run.get("value")
                and (
                    run["realtime_end"] == "9999-12-31"
                    or date.fromisoformat(obs["realtime_start"])
                    <= date.fromisoformat(run["realtime_end"]) + timedelta(days=1)
                )
            ):
                # same value continuing -> extend the vintage window
                run["realtime_end"] = obs.get("realtime_end", run.get("realtime_end"))
            else:
                if run is not None:
                    out.append(run)
                run = dict(obs)
        if run is not None:
            out.append(run)
    return sorted(out, key=lambda o: (o.get("date", ""), o.get("realtime_start", "")))


class FredClient(HTTPSource):
    """Retrying, rate-limited FRED API client."""

    source_name = "FRED"
    error_cls = FredAPIError

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.stlouisfed.org/fred",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 120,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not api_key:
            raise FredAPIError("A FRED API key is required (got empty string)")
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
        return {"api_key": self.api_key, "file_type": "json"}

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        return "series/observations"

    # ---- SourceClient contract ------------------------------------------

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = True,
        source: str = "fred",
    ) -> list[dict[str, Any]]:
        """Map a raw FRED observations payload into canonical silver rows."""
        from fred_pipeline.transform import normalize_observations

        return normalize_observations(
            series_id,
            payload,
            run_id=run_id,
            track_vintage=track_vintage,
            source=source,
        )

    # ---- public API -----------------------------------------------------

    def get_series_metadata(self, series_id: str) -> dict[str, Any]:
        """Return the ``seriess`` metadata block for a series (title, units...)."""
        payload = self._request("series", {"series_id": series_id})
        seriess = payload.get("seriess") or []
        if not seriess:
            raise FredAPIError(f"No metadata returned for series {series_id!r}")
        return seriess[0]

    def _observation_pages(self, params: dict[str, Any]) -> dict[str, Any]:
        """Read every observation page; refuse truncated or drifting responses.

        Vintage-date batching does not paginate observation rows. Apply both.
        Keep each API response page for Bronze lineage, without request/auth params.
        """
        parameters = {**params, "limit": 100000, "offset": 0}
        first = self._request("series/observations", parameters)
        if "count" not in first:
            raise FredAPIError("Observation response is missing its count")
        count = int(first["count"])
        if count < 0 or count > 1000000:
            raise FredAPIError(
                "Observation response exceeds the one-million-row recovery limit"
            )
        rows = list(first.get("observations") or [])
        pages = [first]
        while len(rows) < count:
            offset = len(rows)
            page = self._request(
                "series/observations", {**parameters, "offset": offset}
            )
            batch = page.get("observations") or []
            if (
                int(page.get("count", -1)) != count
                or int(page.get("offset", -1)) != offset
                or not batch
            ):
                raise FredAPIError(
                    "Observation pagination is incomplete or changed during extraction"
                )
            rows.extend(batch)
            pages.append(page)
        if len(rows) != count:
            raise FredAPIError("Observation count does not match extracted rows")
        if len(pages) == 1:
            return first
        return {**first, "observations": rows, "offset": 0, "response_pages": pages}

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None,
        units: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch observations for a series, returning the raw JSON payload.

        Passing ``realtime_start``/``realtime_end`` (e.g. both set to a past
        date, or ``1776-07-04``/``9999-12-31`` for full vintage history) is how
        point-in-time / ALFRED data is retrieved.
        """
        params: dict[str, Any] = {"series_id": series_id}
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        if realtime_start:
            params["realtime_start"] = realtime_start
        if realtime_end:
            params["realtime_end"] = realtime_end
        if units:
            params["units"] = units
        try:
            return self._observation_pages(params)
        except FredAPIError as exc:
            if realtime_start and _is_vintage_cap_error(exc):
                return self._observations_bounded_vintage(series_id, params)
            raise

    def _observations_bounded_vintage(
        self, series_id: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Recover from the 'maximum vintage dates' cap by narrowing the window.

        FRED caps a real-time window at 2000 vintage dates. For a series with
        more revisions than that, progressively shorten ``realtime_start`` to
        recent years (capturing recent revisions), and finally fall back to the
        latest-only view. Full history for such series would require per-vintage
        batching; recent vintages are what feature pipelines actually use.
        """
        for years in VINTAGE_FALLBACK_YEARS:
            start = (date.today() - timedelta(days=365 * years)).isoformat()
            attempt = {**params, "realtime_start": start}
            log.warning(
                "Series %s exceeded FRED vintage-date cap; retrying with "
                "realtime_start=%s (last %d years of vintages)",
                series_id,
                start,
                years,
            )
            try:
                return self._observation_pages(attempt)
            except FredAPIError as exc:
                if _is_vintage_cap_error(exc):
                    continue
                raise
        # Last resort: latest revision only (no vintage history).
        latest = {
            k: v
            for k, v in params.items()
            if k not in ("realtime_start", "realtime_end")
        }
        log.warning(
            "Series %s still over the cap; falling back to latest-only "
            "(no vintage history for this run)",
            series_id,
        )
        return self._observation_pages(latest)

    # ---- complete vintage history (batched) -----------------------------

    def get_vintage_dates(self, series_id: str, *, page_size: int = 10000) -> list[str]:
        """Return every vintage (release) date for a series, ascending."""
        out: list[str] = []
        offset = 0
        limit = min(page_size, 10000)
        while True:
            payload = self._request(
                "series/vintagedates",
                {"series_id": series_id, "limit": limit, "offset": offset},
            )
            batch = payload.get("vintage_dates") or []
            out.extend(batch)
            offset += len(batch)
            if not batch or len(batch) < limit:
                break
        return out

    def get_observations_all_vintages(
        self,
        series_id: str,
        *,
        batch_size: int = 2000,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        units: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch the *complete* vintage history, batched under FRED's cap.

        FRED allows at most 2000 vintage dates per observations request. This
        enumerates the series' vintage dates and pulls observations in
        contiguous real-time windows of ``batch_size`` vintages each, then
        coalesces adjacent equal-value vintages — which removes the artificial
        "revisions" that window-clipping introduces at batch seams — so the
        merged result is equivalent to one uncapped request.
        """
        if not 1 <= batch_size <= 2000:
            raise ValueError("vintage batch size must be between 1 and 2000")
        # FRED may include one boundary-state vintage in addition to the dated
        # releases. Reserve its slot rather than requesting 2001 states.
        batch_size = min(batch_size, 1999)
        base: dict[str, Any] = {"series_id": series_id}
        if observation_start:
            base["observation_start"] = observation_start
        if observation_end:
            base["observation_end"] = observation_end
        if units:
            base["units"] = units

        vintages = self.get_vintage_dates(series_id)
        if not vintages:  # brand-new series with no releases yet
            return self._observation_pages(
                {**base, "realtime_start": "1776-07-04", "realtime_end": "9999-12-31"},
            )
        if len(vintages) <= batch_size:
            return self._observation_pages(
                {**base, "realtime_start": vintages[0], "realtime_end": "9999-12-31"},
            )

        merged: list[dict[str, Any]] = []
        for i in range(0, len(vintages), batch_size):
            chunk = vintages[i : i + batch_size]
            payload = self._observation_pages(
                {
                    **base,
                    "realtime_start": chunk[0],
                    "realtime_end": (
                        (
                            date.fromisoformat(vintages[i + batch_size])
                            - timedelta(days=1)
                        ).isoformat()
                        if i + batch_size < len(vintages)
                        else "9999-12-31"
                    ),
                },
            )
            merged.extend(payload.get("observations") or [])
        log.info(
            "Series %s: assembled %d vintages in %d batch(es)",
            series_id,
            len(vintages),
            (len(vintages) + batch_size - 1) // batch_size,
        )
        return {"observations": _coalesce_observations(merged)}

    # ---- discovery (for generating manifests) ---------------------------

    def list_series(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        max_results: Optional[int] = 1000,
        order_by: str = "popularity",
        sort_order: str = "desc",
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """Page through an endpoint that returns a ``seriess`` list.

        Used by category / release / search discovery below. FRED caps each page
        at 1000 rows; this transparently follows ``offset`` until exhausted or
        ``max_results`` is reached.
        """
        collected: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = dict(params)
            page.update(
                {
                    "limit": min(page_size, 1000),
                    "offset": offset,
                    "order_by": order_by,
                    "sort_order": sort_order,
                }
            )
            payload = self._request(endpoint, page)
            batch = payload.get("seriess") or []
            collected.extend(batch)
            offset += len(batch)
            if not batch or len(batch) < page["limit"]:
                break
            if max_results and len(collected) >= max_results:
                break
        return collected[:max_results] if max_results else collected

    def get_category_series(
        self, category_id: int, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """All series belonging to a FRED category id."""
        return self.list_series(
            "category/series", {"category_id": category_id}, **kwargs
        )

    def get_release_series(
        self, release_id: int, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """All series belonging to a FRED release id."""
        return self.list_series("release/series", {"release_id": release_id}, **kwargs)

    def search_series(self, search_text: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Full-text search over the FRED catalog."""
        return self.list_series("series/search", {"search_text": search_text}, **kwargs)

    def get_release_dates(
        self,
        *,
        release_id: Optional[int] = None,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None,
        include_release_dates_with_no_data: bool = True,
        sort_order: str = "asc",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Page through release dates in ``[realtime_start, realtime_end]``.

        With ``release_id`` set, hits the **singular** ``release/dates``
        endpoint, scoped server-side to that one release — fast (confirmed
        live: a single release, even over an 18-month window, returns in
        well under a second). Without it, hits the **plural**
        ``releases/dates`` endpoint for *all* releases at once — FRED does
        not support filtering that endpoint by ``release_id`` server-side
        (confirmed live: the parameter is silently ignored), and with
        ``include_release_dates_with_no_data=true`` the unfiltered result
        set is large enough that offset-based pagination degrades sharply
        after the first page (page 1: ~1s; page 2+: 30s+ timeouts observed
        live) — avoid this form for anything but a one-off full-catalog
        pull; prefer looping per curated ``release_id`` instead (see
        :func:`fred_pipeline.pipeline.FredPipeline._refresh_release_calendar`).

        Returns rows of ``{release_id, date}`` (the singular endpoint) or
        ``{release_id, release_name, release_last_updated, date}`` (the
        plural one) — unlike :meth:`list_series`, this endpoint's payload
        key is ``release_dates``, not ``seriess``.
        """
        endpoint = "release/dates" if release_id else "releases/dates"
        params: dict[str, Any] = {
            "sort_order": sort_order,
            "include_release_dates_with_no_data": str(
                include_release_dates_with_no_data
            ).lower(),
        }
        if release_id:
            params["release_id"] = release_id
        if realtime_start:
            params["realtime_start"] = realtime_start
        if realtime_end:
            params["realtime_end"] = realtime_end

        collected: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = dict(params)
            page.update({"limit": min(limit, 1000), "offset": offset})
            payload = self._request(endpoint, page)
            batch = payload.get("release_dates") or []
            collected.extend(batch)
            offset += len(batch)
            if not batch or len(batch) < page["limit"]:
                break
        return collected
