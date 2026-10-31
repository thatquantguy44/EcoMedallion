"""ECB Data Portal source client (SDMX 2.1 CSV) -- keyless SourceClient.

Manifest ``series_id`` values encode the ECB dataflow and SDMX series key:

    ECB:EXR:D.USD.EUR.SP00.A
    └src┘└flow┘└──── key ─────┘

ECB serves SDMX CSV from ``/data/{flow}/{key}``. Bronze keeps the raw CSV body
inside a small JSON envelope, while Silver normalization reads ``TIME_PERIOD``
and ``OBS_VALUE`` into the canonical row schema. When the manifest has
``vintage_enabled: true``, the pipeline passes realtime kwargs; this client maps
that to ``includeHistory=true`` and uses ``VALID_FROM`` / ``VALID_TO`` as the
real-time window.
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
from fred_pipeline.transform import (
    _row_hash,
    _utc_now_iso,
    parse_decimal_value,
    parse_value,
)

log = logging.getLogger("fred_pipeline.sources.ecb")

ECB_ACCEPT = "text/csv"


class ECBAPIError(SourceError):
    """Raised when the ECB Data Portal API returns an unrecoverable error."""


def parse_ecb_series_id(series_id: str) -> tuple[str, str]:
    """Split ``ECB:<flow_ref>:<key>`` into ``(flow_ref, key)``."""
    parts = str(series_id).strip().split(":", 2)
    if len(parts) != 3 or parts[0].upper() != "ECB":
        raise ECBAPIError(
            f"ECB series_id must be 'ECB:<flow_ref>:<key>', got {series_id!r}"
        )
    flow_ref = parts[1].strip()
    key = parts[2].strip()
    if not flow_ref or not key:
        raise ECBAPIError(
            "ECB series_id must include a non-empty flow_ref and key, "
            f"got {series_id!r}"
        )
    return flow_ref, key


def _date_or_none(year: int, month: int, day: int) -> str | None:
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _ecb_period_to_date(period: Any) -> str | None:
    """Map an SDMX ``TIME_PERIOD`` to an ISO observation date."""
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

    match = re.fullmatch(r"(\d{4})-?W(\d{1,2})", text)
    if match:
        year, week = match.groups()
        try:
            return date.fromisocalendar(int(year), int(week), 1).isoformat()
        except ValueError:
            return None

    if re.fullmatch(r"\d{4}-\d{2}", text):
        year, month = text.split("-")
        return _date_or_none(int(year), int(month), 1)

    if re.fullmatch(r"\d{6}", text):
        return _date_or_none(int(text[:4]), int(text[4:6]), 1)

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return date.fromisoformat(text).isoformat()
        except ValueError:
            return None

    if re.fullmatch(r"\d{8}", text):
        return _date_or_none(int(text[:4]), int(text[4:6]), int(text[6:8]))

    return None


def _ecb_period_from_date(value: str | None) -> str | None:
    """Map an ISO-ish date to an ECB ``startPeriod`` / ``endPeriod``."""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", text) else text


def _valid_time_to_date(value: Any) -> str:
    """Map ECB ``VALID_FROM`` / ``VALID_TO`` timestamp fields to date strings."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        return text[:10]
    return text


def _text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _exr_currencies(
    series_id: str, rec: dict[str, Any]
) -> tuple[str | None, str | None]:
    """Return the verified ``(base, quote)`` orientation for an EXR record."""
    flow_ref, key = parse_ecb_series_id(series_id)
    if flow_ref.upper() != "EXR":
        return None, None
    dimensions = key.split(".")
    if len(dimensions) < 3:
        raise ECBAPIError(f"ECB EXR key is missing currency dimensions: {key!r}")
    quote, base = dimensions[1].upper(), dimensions[2].upper()
    record_quote = _text_or_none(_field(rec, "CURRENCY"))
    record_base = _text_or_none(_field(rec, "CURRENCY_DENOM"))
    if record_quote and record_quote.upper() != quote:
        raise ECBAPIError(
            f"ECB EXR currency mismatch for {series_id}: "
            f"key quote={quote}, record quote={record_quote}"
        )
    if record_base and record_base.upper() != base:
        raise ECBAPIError(
            f"ECB EXR denominator mismatch for {series_id}: "
            f"key base={base}, record base={record_base}"
        )
    if base != "EUR":
        raise ECBAPIError(f"ECB EXR base must be EUR for {series_id}, got {base}")
    return base, quote


def _unit_multiplier(series_id: str, rec: dict[str, Any]) -> int | None:
    value = _text_or_none(_field(rec, "UNIT_MULT"))
    if value is None:
        return None
    try:
        multiplier = int(value)
    except ValueError as exc:
        raise ECBAPIError(f"Invalid ECB UNIT_MULT {value!r} for {series_id}") from exc
    if parse_ecb_series_id(series_id)[0].upper() == "EXR" and multiplier != 0:
        raise ECBAPIError(
            f"ECB EXR UNIT_MULT must be 0 for {series_id}, got {multiplier}"
        )
    return multiplier


def _csv_records(csv_text: str) -> list[dict[str, str]]:
    """Parse ECB CSV rows, tolerating blank/comment preamble lines."""
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


def normalize_ecb_observations(
    series_id: str,
    payload: dict[str, Any] | str,
    *,
    run_id: str | None = None,
    ingested_at: str | None = None,
    track_vintage: bool = True,
    source: str = "ecb",
) -> list[dict[str, Any]]:
    """Convert raw ECB SDMX CSV into canonical silver rows."""
    ingested_at = ingested_at or _utc_now_iso()
    csv_text = payload.get("data", "") if isinstance(payload, dict) else str(payload)

    rows: list[dict[str, Any]] = []
    for rec in _csv_records(csv_text):
        obs_date = _ecb_period_to_date(_field(rec, "TIME_PERIOD", "PERIOD"))
        if not obs_date:
            continue
        action = (_text_or_none(_field(rec, "ACTION")) or "replace").lower()
        is_deleted = action == "delete"
        raw_value = _field(rec, "OBS_VALUE", "VALUE")
        value = None if is_deleted else parse_value(raw_value)
        exact_value = None if is_deleted else parse_decimal_value(raw_value)
        provider_valid_from = _text_or_none(_field(rec, "VALID_FROM"))
        provider_valid_to = _text_or_none(_field(rec, "VALID_TO"))
        rt_start = _valid_time_to_date(provider_valid_from) if track_vintage else ""
        rt_end = _valid_time_to_date(provider_valid_to) if track_vintage else ""
        base_currency, quote_currency = _exr_currencies(series_id, rec)
        unit_multiplier = _unit_multiplier(series_id, rec)
        status = _text_or_none(_field(rec, "OBS_STATUS"))
        semantics = {
            "action": action,
            "base_currency": base_currency,
            "exact_value": str(exact_value) if exact_value is not None else None,
            "is_deleted": is_deleted,
            "observation_status": status,
            "provider_valid_from": provider_valid_from,
            "provider_valid_to": provider_valid_to,
            "quote_currency": quote_currency,
            "unit_multiplier": unit_multiplier,
        }
        rows.append(
            {
                "source": source,
                "series_id": series_id,
                "observation_date": obs_date,
                "realtime_start": rt_start,
                "realtime_end": rt_end,
                "value": value,
                "raw_value": None if raw_value is None else str(raw_value),
                "exact_value": str(exact_value) if exact_value is not None else None,
                "is_missing": value is None,
                "row_hash": _row_hash(
                    series_id, obs_date, rt_start, raw_value, semantics=semantics
                ),
                "provider_valid_from": provider_valid_from,
                "provider_valid_to": provider_valid_to,
                "base_currency": base_currency,
                "quote_currency": quote_currency,
                "unit_multiplier": unit_multiplier,
                "observation_status": status,
                "observation_action": action,
                "is_deleted": is_deleted,
                "observation_event_id": None,
                "ingested_at": ingested_at,
                "run_id": run_id,
            }
        )
    return rows


class ECBClient(HTTPSource):
    """Retrying, rate-limited ECB Data Portal client."""

    source_name = "ECB"
    error_cls = ECBAPIError

    def __init__(
        self,
        base_url: str = "https://data-api.ecb.europa.eu/service",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 60,
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
        return {"Accept": ECB_ACCEPT}

    def _error_detail(self, resp: Any) -> str:
        text = str(getattr(resp, "text", "") or "").strip()
        if text:
            return text[:500]
        json_fn = getattr(resp, "json", None)
        if callable(json_fn):
            try:
                return str(json_fn())
            except ValueError:
                pass
        return "<no body>"

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        flow_ref, key = parse_ecb_series_id(series_id)
        return f"data/{flow_ref}/{key}"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: str | None = None,
        observation_end: str | None = None,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
        include_history: bool | None = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch one ECB series as raw SDMX CSV."""
        flow_ref, key = parse_ecb_series_id(series_id)
        history = bool(include_history) or bool(realtime_start or realtime_end)
        params: dict[str, Any] = {"format": "csvdata", "detail": "full"}
        start_period = _ecb_period_from_date(observation_start)
        end_period = _ecb_period_from_date(observation_end)
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if history:
            params["includeHistory"] = "true"

        csv_text = self._request(
            self.observations_endpoint(series_id), params, as_text=True
        )
        return {
            "data": csv_text,
            "meta": {
                "series_id": series_id,
                "flow_ref": flow_ref,
                "key": key,
                "format": "sdmx-csv",
                "include_history": history,
            },
        }

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        track_vintage: bool = True,
        source: str = "ecb",
    ) -> list[dict[str, Any]]:
        return normalize_ecb_observations(
            series_id,
            payload,
            run_id=run_id,
            track_vintage=track_vintage,
            source=source,
        )
