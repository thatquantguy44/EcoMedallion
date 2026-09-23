"""IMF SDMX 3.0 source client — a keyless `SourceClient`.

⚠️⚠️ **UNVERIFIED AGAINST A LIVE RESPONSE.** This is the one source client in
this package built without ever seeing real output from its own upstream.
Every other client here (`ecb.py`, `oecd.py`, `bis.py`, even `french.py`'s
decades-stable CSV) was either live-verified during construction or built
against a format so old and public that the risk was low. Neither is true
here: `api.imf.org` has been blocked in every environment this repo has been
worked from (`scripts/probe_imf_dataflows.py` documents the same block,
repeatedly, and is the tool meant to finally break that). What follows is
built against the **public SDMX-JSON 2.0.0 data-message specification**
(a real, versioned, documented interchange format — not invented), which is
a much better starting point than a blind guess, but IMF's own dialect —
`structures` vs. `structure`, the exact index-key separator, which agency
actually maintains a given dataflow — has real, documented variation across
SDMX-JSON providers and has never been confirmed against IMF specifically.

**Do not treat this module as verified.** No manifest ships with it (see
`docs/catalog/imf.md`) — fabricating a plausible-looking series id here
would look vetted when it isn't, the same trap this repo's own catalogue
docs warn about for ECB ("structurally plausible ≠ actually published").
Before this client ships even one `active: false` manifest entry, someone
needs to run `scripts/probe_imf_dataflows.py --sample-dataflow <id>
--out-dir ...` from an environment with real network access, and this
module's parsing needs to be checked — and very possibly corrected — against
whatever comes back.

A manifest `series_id`, when one exists, follows the same multi-part-id
convention OECD (`specs/spec006`'s own worked example) established for
sources whose real identifier needs more than one coordinate:

    IMF:<agency>:<dataflow>:<key>
    └sr┘ └agency┘ └dataflow┘ └key┘

The agency segment exists for the same reason OECD's does: `IMF` is not
always the maintaining agency for a flow IMF's own catalogue serves — this
client makes no assumption otherwise. `<key>` is passed through opaquely
(dot-joined dimension values, per SDMX convention) to the query URL; this
client does not attempt to validate or decode it before sending.

Self-contained parsing, deliberately duplicated rather than imported from
`ecb.py`/`oecd.py`: those two are SDMX 2.1 *CSV*; this is SDMX 3.0 *JSON*, a
different enough shape that sharing code would blur where one source's
parsing ends and another's begins — the same reasoning `oecd.py`'s own
docstring gives for not importing `ecb.py`'s CSV helpers.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from datetime import date
from typing import Any

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.imf")

IMF_BASE_URL = "https://api.imf.org/external/sdmx/3.0"

# Picked as the single best-guess Accept header (SDMX-JSON 2.0.0, the modern
# version) rather than negotiating across several like scripts/
# probe_imf_dataflows.py does -- every other SDMX client in this package
# (ecb.py, oecd.py, bis.py) also commits to one Accept header rather than
# retrying across several, and a wrong guess here fails loudly with a clear
# HTTP/parse error instead of silently misbehaving. If this turns out wrong,
# the probe script's CANDIDATE list is the place to find the right one.
IMF_DATA_ACCEPT = "application/vnd.sdmx.data+json;version=2.0.0"


class IMFAPIError(SourceError):
    """Raised when the IMF API returns an unrecoverable error, or when a
    response doesn't match the SDMX-JSON shape this client assumes."""


def parse_imf_series_id(series_id: str) -> tuple[str, str, str]:
    """Split ``IMF:<agency>:<dataflow>:<key>`` into its three parts.

    The leading ``IMF:`` prefix is optional, mirroring
    :func:`fred_pipeline.sources.oecd.parse_oecd_series_id`.
    """
    text = (series_id or "").strip()
    if not text:
        raise IMFAPIError("IMF series_id must not be empty")

    parts = text.split(":")
    if parts and parts[0].upper() == "IMF" and len(parts) == 4:
        parts = parts[1:]
    if len(parts) != 3 or not all(p.strip() for p in parts):
        raise IMFAPIError(
            "IMF series_id must be 'IMF:<agency>:<dataflow>:<key>' "
            f"(or '<agency>:<dataflow>:<key>'), got {series_id!r}"
        )
    agency, dataflow, key = (p.strip() for p in parts)
    return agency, dataflow, key


# ---- SDMX period -> ISO date -------------------------------------------
#
# Duplicated from oecd.py's _oecd_period_to_date rather than imported -- see
# the module docstring. Same general SDMX TIME_PERIOD conventions (bare
# year, quarter, semester, year-month, full date), because that convention
# is the SDMX standard's, not any one provider's.


def _date_or_none(year: int, month: int, day: int) -> str | None:
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _imf_period_to_date(period: Any) -> str | None:
    """Map an SDMX ``TIME_PERIOD`` value to an ISO observation date (period start)."""
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


# ---- SDMX-JSON 2.0.0 data-message decoding ------------------------------
#
# Per the public SDMX-JSON spec: data.structures[0].dimensions.observation
# lists the per-observation dimensions (normally just TIME_PERIOD) with
# their ordered .values[]; data.dataSets[0].series is a dict keyed by a
# colon-joined string of zero-based indices into dimensions.series[]'s value
# lists, and each series' own "observations" dict is keyed by an index (or,
# with more than one observation-level dimension, a colon-joined compound
# index) into dimensions.observation[]'s value lists. This client only
# supports the single-observation-dimension case (TIME_PERIOD alone), which
# is what every scalar economic time series in this pipeline actually is --
# a real multi-dimensional-observation response is a genuine surprise this
# raises on rather than silently mis-decodes.


def _structures(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise IMFAPIError(
            "IMF response has no top-level 'data' object -- "
            "does not match the SDMX-JSON shape this client assumes"
        )
    structures = data.get("structures")
    if isinstance(structures, list) and structures and isinstance(structures[0], dict):
        return structures[0]
    # SDMX-JSON 1.0.0 used a singular "structure" key rather than a
    # "structures" array -- tolerate it rather than assuming 2.0.0 only.
    structure = data.get("structure")
    if isinstance(structure, dict):
        return structure
    raise IMFAPIError(
        "IMF response has no 'data.structures[0]' or 'data.structure' -- "
        "does not match either SDMX-JSON dialect this client knows"
    )


def _observation_period_values(structure: dict[str, Any]) -> list[str]:
    dimensions = structure.get("dimensions")
    if not isinstance(dimensions, dict):
        raise IMFAPIError("IMF structure message has no 'dimensions' block")
    obs_dims = dimensions.get("observation")
    if not isinstance(obs_dims, list) or not obs_dims:
        raise IMFAPIError("IMF structure message has no observation-level dimensions")
    if len(obs_dims) != 1:
        # Genuinely unsupported rather than silently mis-decoded -- see the
        # module-level note above.
        raise IMFAPIError(
            f"IMF response has {len(obs_dims)} observation-level dimensions; "
            f"this client only supports exactly one (TIME_PERIOD)"
        )
    values = obs_dims[0].get("values")
    if not isinstance(values, list):
        raise IMFAPIError("IMF observation dimension has no 'values' list")
    return [str(v.get("id")) for v in values if isinstance(v, dict)]


def _data_series(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload["data"]
    datasets = data.get("dataSets")
    if not isinstance(datasets, list) or not datasets or not isinstance(datasets[0], dict):
        raise IMFAPIError("IMF response has no 'data.dataSets[0]'")
    series = datasets[0].get("series")
    if not isinstance(series, dict):
        raise IMFAPIError("IMF response's dataSets[0] has no 'series' dict")
    return series


def normalize_imf_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: str | None = None,
    ingested_at: str | None = None,
    track_vintage: bool = True,
    source: str = "imf",
) -> list[dict[str, Any]]:
    """Convert a raw IMF SDMX-JSON data message into canonical silver rows.

    Expects exactly one series in the response (the query's ``key`` should
    already pin every series-level dimension) -- more than one is a genuine
    surprise this raises on rather than silently picking one or merging.

    ``track_vintage`` is accepted for contract parity with the other SDMX
    clients but has no effect: like OECD/BIS/ECB's non-``_PUB`` flows, IMF's
    SDMX-JSON carries no per-observation revision timestamps in the shape
    this client decodes, so ``realtime_start``/``realtime_end`` are always
    empty.
    """
    ingested_at = ingested_at or _utc_now_iso()
    structure = _structures(payload)
    period_values = _observation_period_values(structure)
    series = _data_series(payload)

    if len(series) != 1:
        raise IMFAPIError(
            f"IMF query for {series_id!r} returned {len(series)} series, "
            f"expected exactly 1 -- the key likely under-specifies at least "
            f"one series-level dimension"
        )
    (_, series_record), = series.items()
    observations = series_record.get("observations")
    if not isinstance(observations, dict):
        raise IMFAPIError(f"IMF series for {series_id!r} has no 'observations' dict")

    rows: list[dict[str, Any]] = []
    for obs_key, obs_value in sorted(observations.items(), key=lambda kv: int(kv[0])):
        idx = int(obs_key)
        if idx >= len(period_values):
            raise IMFAPIError(
                f"IMF observation index {idx} has no matching TIME_PERIOD "
                f"value (only {len(period_values)} known)"
            )
        obs_date = _imf_period_to_date(period_values[idx])
        if not obs_date:
            continue
        raw_value = obs_value[0] if isinstance(obs_value, list) and obs_value else None
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


class IMFClient(HTTPSource):
    """Retrying, rate-limited IMF SDMX 3.0 client.

    ⚠️ See the module docstring: unverified against a live response. Keyless
    (IMF's SDMX API, like OECD's, requires no key), but that too is
    unconfirmed against a real request.
    """

    source_name = "IMF"
    error_cls = IMFAPIError

    def __init__(
        self,
        base_url: str = IMF_BASE_URL,
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
        return {"Accept": IMF_DATA_ACCEPT}

    def _error_detail(self, resp: Any) -> str:
        text = str(getattr(resp, "text", "") or "").strip()
        return text[:500] if text else "<no body>"

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage).

        SDMX 3.0 REST addressing: ``/data/dataflow/{agency}/{id}/{version}/{key}``.
        Version is wildcarded with ``+`` (SDMX 3.0's "latest" token) rather
        than pinned, the same reasoning ``oecd.py`` gives for leaving its own
        version segment open: a pinned version silently 404s on the next bump.
        """
        agency, dataflow, key = parse_imf_series_id(series_id)
        return f"data/dataflow/{agency}/{dataflow}/+/{key}"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(self, series_id: str, **_ignored: Any) -> dict[str, Any]:
        """Fetch one IMF series as a raw SDMX-JSON data message."""
        agency, dataflow, key = parse_imf_series_id(series_id)
        payload = self._request(self.observations_endpoint(series_id), {})
        if not isinstance(payload, dict):
            raise IMFAPIError(
                f"IMF response for {series_id!r} was not a JSON object"
            )
        return {
            "data": payload.get("data", payload),
            "meta": {
                "series_id": series_id,
                "agency": agency,
                "dataflow": dataflow,
                "key": key,
                "format": "sdmx-json",
            },
        }

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        track_vintage: bool = True,
        source: str = "imf",
    ) -> list[dict[str, Any]]:
        return normalize_imf_observations(
            series_id,
            payload,
            run_id=run_id,
            track_vintage=track_vintage,
            source=source,
        )
