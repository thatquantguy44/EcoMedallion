"""BLS series-catalog discovery helpers.

The observation client in :mod:`fred_pipeline.sources.bls` fetches one
already-known ``series_id`` at a time from BLS's v2 JSON API, which has no
series-search or metadata-enumeration endpoint. This module is the metadata
side of that story, but it has to talk to a different BLS surface entirely:

* ``GET https://api.bls.gov/publicAPI/v2/surveys`` lists survey abbreviations
  (``CU``, ``CE``, ``LN``, ...) and names -- the closest BLS analogue to
  ECB's dataflow list, though far coarser (no series-level detail).
* ``https://download.bls.gov/pub/time.series/<survey>/<survey>.series`` is a
  tab-delimited flat file: the *only* place BLS publishes the full list of
  series that actually exist for a survey. Unlike ECB's SDMX dimensions
  (:mod:`fred_pipeline.catalogs.ecb_discovery`), there is nothing to
  combinatorially expand here -- each row already names a real, currently
  published series, so candidate generation is filtering + capping, not
  Cartesian expansion.

Column layout on those flat files differs per survey (CPI dimensions series
by area/item code; Employment by industry/data-type code; ...), and this
module was written without live access to BLS to verify exact column names
survey-by-survey. Rather than hardcode a schema, every column is kept
verbatim in :attr:`BLSSeriesRow.fields`, keyed exactly as BLS names it --
``--inspect`` exists specifically to let a caller with network access see the
real header and pick ``--column`` filters before generating candidates.
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from itertools import islice
from typing import Any

import yaml

from fred_pipeline.manifest import Manifest, ManifestError, SeriesSpec
from fred_pipeline.sources.base import HTTPSource, SourceError

BLS_FLATFILE_BASE_URL = "https://download.bls.gov/pub/time.series"
BLS_SURVEYS_URL = "https://api.bls.gov/publicAPI/v2/surveys"

_DEFAULT_USER_AGENT = (
    "fred-bronze-to-gold-pipeline (set BLS_USER_AGENT to your contact)"
)

_MANIFEST_FREQUENCIES = {"d", "w", "m", "q", "sa", "a"}


class BLSDiscoveryError(SourceError):
    """Raised when BLS catalog discovery fails."""


@dataclass(frozen=True)
class BLSSurvey:
    """One BLS survey advertised by the v2 surveys endpoint."""

    abbreviation: str
    name: str = ""

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class BLSSeriesRow:
    """One row from a BLS survey's flat-file series catalog.

    ``series_id`` and ``series_title`` are promoted to first-class attributes
    since every observed survey carries them; every other column stays in
    ``fields`` (lower-cased header -> value) because the rest of the schema
    is survey-specific.
    """

    series_id: str
    series_title: str = ""
    fields: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "series_id": self.series_id,
            "series_title": self.series_title,
            "fields": dict(self.fields),
        }


class BLSFlatFileClient(HTTPSource):
    """Retrying, rate-limited client for BLS's survey list and flat-file catalogs.

    Distinct from :class:`fred_pipeline.sources.bls.BLSClient`: that class
    fetches *observations* for one known series from the v2 JSON API; this
    one fetches *what series exist* from BLS's separate flat-file server (plus
    the v2 surveys endpoint for the coarse survey list). Only used by
    ``discover-bls`` -- it never touches Bronze/Silver/Gold.
    """

    source_name = "BLS flat files"
    error_cls = BLSDiscoveryError

    def __init__(
        self,
        base_url: str = BLS_FLATFILE_BASE_URL,
        *,
        user_agent: str = _DEFAULT_USER_AGENT,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 20,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._user_agent = user_agent or _DEFAULT_USER_AGENT
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _request_headers(self) -> dict[str, str]:
        return {"User-Agent": self._user_agent}

    def _error_detail(self, resp: Any) -> str:
        text = str(getattr(resp, "text", "") or "").strip()
        return text[:500] if text else "<no body>"

    def list_surveys(self) -> list[BLSSurvey]:
        """Fetch the BLS survey abbreviation/name list from the v2 JSON API."""
        payload = self._request(BLS_SURVEYS_URL, {})
        return parse_surveys_payload(payload)

    def fetch_series_catalog(self, survey: str) -> list[BLSSeriesRow]:
        """Fetch and parse one survey's full series catalog flat file."""
        survey_lower = survey.strip().lower()
        if not survey_lower:
            raise BLSDiscoveryError("survey abbreviation must not be empty")
        text = self._request(f"{survey_lower}/{survey_lower}.series", {}, as_text=True)
        return parse_bls_series_flatfile(text)


def parse_surveys_payload(payload: dict[str, Any]) -> list[BLSSurvey]:
    """Parse the v2 ``/surveys`` JSON payload into stable records."""
    results = (payload.get("Results") or {}).get("survey") or []
    surveys = [
        BLSSurvey(
            abbreviation=(row.get("survey_abbreviation") or "").strip(),
            name=(row.get("survey_name") or "").strip(),
        )
        for row in results
        if isinstance(row, dict) and (row.get("survey_abbreviation") or "").strip()
    ]
    return sorted(surveys, key=lambda s: s.abbreviation)


def parse_bls_series_flatfile(text: str) -> list[BLSSeriesRow]:
    """Parse a BLS ``<survey>.series`` tab-delimited catalog file.

    ``series_id`` is required (rows without one are dropped as blank/trailer
    lines); ``series_title`` is used when the column is present.
    """
    reader = csv.reader(io.StringIO(text), delimiter="\t")
    try:
        header = [h.strip().lower() for h in next(reader)]
    except StopIteration:
        return []
    if "series_id" not in header:
        raise BLSDiscoveryError(
            f"BLS series catalog has no series_id column; got columns {header}"
        )

    rows: list[BLSSeriesRow] = []
    for raw in reader:
        if not raw or all(not cell.strip() for cell in raw):
            continue
        cells = [c.strip() for c in raw]
        if len(cells) < len(header):
            cells += [""] * (len(header) - len(cells))
        record = dict(zip(header, cells))
        series_id = record.pop("series_id", "").strip()
        if not series_id:
            continue
        series_title = record.pop("series_title", "").strip()
        rows.append(
            BLSSeriesRow(series_id=series_id, series_title=series_title, fields=record)
        )
    return rows


def inspect_series_catalog(
    rows: Iterable[BLSSeriesRow],
    *,
    sample_size: int = 8,
) -> dict[str, Any]:
    """Summarize a survey's flat-file schema: columns seen and value samples.

    This is the tool for the exact uncertainty this module was built under --
    BLS's per-survey column layout isn't verified here, so use this to see the
    real header and representative values before picking ``--column`` filters
    or a ``--frequency`` for candidate generation.
    """
    rows = list(rows)
    columns: dict[str, set[str]] = {}
    for row in rows:
        for col, value in row.fields.items():
            if not value:
                continue
            columns.setdefault(col, set()).add(value)
    return {
        "row_count": len(rows),
        "columns": [
            {
                "column": col,
                "distinct_count_in_sample": len(values),
                "sample_values": sorted(values)[:sample_size],
            }
            for col, values in sorted(columns.items())
        ],
    }


def filter_surveys(
    surveys: Iterable[BLSSurvey],
    *,
    search: str | None = None,
    max_results: int | None = None,
) -> list[BLSSurvey]:
    """Filter surveys by text and cap returned rows."""
    needle = (search or "").strip().lower()
    kept: list[BLSSurvey] = []
    for survey in surveys:
        haystack = f"{survey.abbreviation} {survey.name}".lower()
        if needle and needle not in haystack:
            continue
        kept.append(survey)
        if max_results is not None and len(kept) >= max_results:
            break
    return kept


def surveys_to_rows(surveys: Iterable[BLSSurvey]) -> list[dict[str, str]]:
    """Return JSON/table-friendly survey rows."""
    return [survey.to_dict() for survey in surveys]


def parse_column_filters(values: Iterable[str] | None) -> dict[str, set[str]]:
    """Parse repeated ``COLUMN=VALUE[,VALUE]`` filter arguments."""
    filters: dict[str, set[str]] = {}
    for raw in values or []:
        if "=" not in raw:
            raise BLSDiscoveryError(
                f"Invalid column filter {raw!r}; expected COLUMN=VALUE[,VALUE]"
            )
        col, raw_values = raw.split("=", 1)
        col = col.strip().lower()
        vals = {v.strip() for v in raw_values.split(",") if v.strip()}
        if not col or not vals:
            raise BLSDiscoveryError(
                f"Invalid column filter {raw!r}; expected COLUMN=VALUE[,VALUE]"
            )
        filters.setdefault(col, set()).update(vals)
    return filters


def filter_series_rows(
    rows: Iterable[BLSSeriesRow],
    *,
    column_filters: dict[str, set[str]] | None = None,
    search: str | None = None,
) -> list[BLSSeriesRow]:
    """Filter catalog rows by exact column values and/or a text search."""
    filters = {k.lower(): v for k, v in (column_filters or {}).items()}
    needle = (search or "").strip().lower()
    kept: list[BLSSeriesRow] = []
    for row in rows:
        if filters and not all(
            row.fields.get(col, "") in values for col, values in filters.items()
        ):
            continue
        if needle and needle not in f"{row.series_id} {row.series_title}".lower():
            continue
        kept.append(row)
    return kept


def infer_bls_category(survey: str, row: BLSSeriesRow) -> str:
    """Infer a broad manifest category from the survey abbreviation/title."""
    text = f"{survey} {row.series_title}".lower()
    if any(
        token in text for token in ("cpi", "consumer price", "producer price", "ppi")
    ):
        return "inflation"
    if any(
        token in text
        for token in (
            "employment",
            "unemployment",
            "labor force",
            "wage",
            "earnings",
            "jolts",
            "job openings",
        )
    ):
        return "labor"
    return "labor"


def _expected_update_frequency(frequency: str) -> str:
    return {
        "d": "daily",
        "w": "weekly",
        "m": "monthly",
        "q": "quarterly",
        "sa": "semiannual",
        "a": "annual",
    }.get(frequency, frequency)


def generate_bls_candidate_specs(
    survey: str,
    rows: Iterable[BLSSeriesRow],
    *,
    frequency: str,
    category: str | None = None,
    max_results: int = 100,
    exclude_ids: Iterable[str] | None = None,
) -> tuple[list[SeriesSpec], list[dict[str, Any]]]:
    """Build inactive BLS candidate manifest specs from filtered catalog rows.

    Unlike ECB's SDMX dimension expansion, BLS flat-file rows already name
    real, currently-published series, so this only filters and caps -- there
    is no Cartesian step. ``frequency`` is required rather than inferred: BLS
    flat files don't self-describe it the way ECB's SDMX FREQ dimension does
    (a documented code list bundled in the same structure response), so a
    caller must confirm it from the survey's own documentation first.
    """
    freq = frequency.strip().lower()
    if freq not in _MANIFEST_FREQUENCIES:
        raise BLSDiscoveryError(
            f"--frequency must be one of {sorted(_MANIFEST_FREQUENCIES)}, "
            f"got {frequency!r}"
        )

    existing = set(exclude_ids or [])
    specs: list[SeriesSpec] = []
    skipped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in islice(rows, max_results):
        if row.series_id in existing:
            skipped.append(
                {"series_id": row.series_id, "reason": "already in manifest"}
            )
            continue
        if row.series_id in seen:
            skipped.append(
                {"series_id": row.series_id, "reason": "duplicate candidate"}
            )
            continue
        manifest_category = category or infer_bls_category(survey, row)
        spec = {
            "series_id": row.series_id,
            "title": row.series_title or row.series_id,
            "category": manifest_category,
            "frequency": freq,
            "units": "",
            "active": False,
            "source": "bls",
            "load_type": "incremental",
            "expected_update_frequency": _expected_update_frequency(freq),
            "vintage_enabled": False,
            "validation_profile": "lenient",
            "downstream_use_case": "bls_candidate_review",
            "priority": 3,
            "tags": ["bls", survey.lower(), manifest_category],
        }
        try:
            specs.append(SeriesSpec(**spec))
            seen.add(row.series_id)
        except ManifestError as exc:
            skipped.append({"series_id": row.series_id, "reason": f"validation: {exc}"})
    return specs, skipped


def build_bls_manifest_dict(
    name: str,
    specs: Iterable[SeriesSpec],
    *,
    description: str = "",
    version: int = 1,
) -> dict[str, Any]:
    """Assemble a BLS candidate manifest dictionary."""
    return {
        "name": name,
        "description": description,
        "version": version,
        "series": [spec.to_dict() for spec in specs],
    }


def bls_manifest_to_yaml(manifest_dict: dict[str, Any]) -> str:
    """Serialize and validate a BLS candidate manifest."""
    Manifest.from_dict(manifest_dict)
    return yaml.safe_dump(manifest_dict, sort_keys=False, default_flow_style=False)
