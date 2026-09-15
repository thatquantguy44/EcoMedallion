"""Kenneth French Data Library source client — Fama-French factor returns.

The library (`mba.tuck.dartmouth.edu/pages/faculty/ken.french/`) publishes
each factor set as a ZIP containing one CSV, with the same layout every
dataset shares: a one-line text preamble, a **monthly** table
(``,<factors...>`` header, then rows keyed by a 6-digit ``YYYYMM`` date),
a blank line, then an "Annual Factors" table keyed by 4-digit years. This
client reads only the monthly table for the first build slice (`specs/spec006`
§7) — the annual table and the separate daily-CSV variants are a follow-up,
not this one.

A manifest ``series_id`` is the **dataset name** (e.g.
``F-F_Research_Data_Factors``); one fetch downloads that dataset's ZIP once
and ``normalize`` explodes it into one scalar series per factor column —
``<dataset>:<factor>`` (e.g. ``F-F_Research_Data_Factors:Mkt-RF``) — the same
"one fetch, several exploded series" shape as `sources/tiingo.py`. See
:data:`DATASET_FILES` for the registered datasets and their factor columns.

⚠️ **Licensing verification blocked.** This client and its manifest were built
without a primary read of the library's terms — this session's environment
blocks egress to `mba.tuck.dartmouth.edu` (see `config/data_licensing.yml`'s
`french` entry). The manifest ships ``active: false`` for that reason among
others; a human must confirm the current terms before flipping anything on.

Values in the source CSV are percentages (e.g. ``2.96`` means +2.96%), kept
as-is here (matching FRED's convention of storing raw source units) rather
than converted to a decimal fraction. The library uses ``-99.99`` as its
missing-observation sentinel; :data:`_MISSING_SENTINELS` maps it to
``is_missing=True`` / ``value=None`` like any other DQ-flagged gap.
"""

from __future__ import annotations

import csv
import io
import logging
import time
import zipfile
from datetime import date
from typing import Any, Callable, Optional

from fred_pipeline.data.transform import _row_hash, _utc_now_iso, parse_value
from fred_pipeline.sources.base import HTTPSource, SourceError

log = logging.getLogger("fred_pipeline.sources.french")

FRENCH_BASE_URL = "https://mba.tuck.dartmouth.edu"
FRENCH_FTP_PATH = "/pages/faculty/ken.french/ftp"

# dataset name -> (zip filename under FRENCH_FTP_PATH, factor columns to
# explode). First-build slice only: the classic 3-factor set, its 5-factor
# successor, and momentum -- all monthly. Daily variants and the
# industry-portfolio files are a follow-up (see specs/spec006 §5.3/§6.3).
DATASET_FILES: dict[str, tuple[str, tuple[str, ...]]] = {
    "F-F_Research_Data_Factors": (
        "F-F_Research_Data_Factors_CSV.zip",
        ("Mkt-RF", "SMB", "HML", "RF"),
    ),
    "F-F_Research_Data_5_Factors_2x3": (
        "F-F_Research_Data_5_Factors_2x3_CSV.zip",
        ("Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"),
    ),
    "F-F_Momentum_Factor": (
        "F-F_Momentum_Factor_CSV.zip",
        ("Mom",),
    ),
}

# The library's own sentinel for a missing monthly observation.
_MISSING_SENTINELS = frozenset({-99.99, -999.0})


class FrenchAPIError(SourceError):
    """Raised when a Kenneth French Data Library fetch or parse fails."""


def _dataset_name(series_id: str) -> str:
    """Manifest series_id is the bare dataset name; tolerate a stray
    ``:factor`` suffix by taking the part before the first colon."""
    name = (series_id or "").partition(":")[0].strip()
    if name not in DATASET_FILES:
        raise FrenchAPIError(
            f"french: unknown dataset {name!r}; add it to DATASET_FILES "
            f"(known: {sorted(DATASET_FILES)})"
        )
    return name


def _month_to_date(token: str) -> Optional[str]:
    """``YYYYMM`` -> ISO date at month start, or ``None`` if not that shape."""
    text = (token or "").strip()
    if len(text) != 6 or not text.isdigit():
        return None
    year, month = int(text[:4]), int(text[4:6])
    if not (1 <= month <= 12):
        return None
    try:
        return date(year, month, 1).isoformat()
    except ValueError:
        return None


def _extract_csv_member(blob: bytes, dataset: str) -> str:
    """Read the (single) CSV member out of a Kenneth French ZIP archive."""
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise FrenchAPIError(
                    f"french: {dataset} zip has no .csv member (found {zf.namelist()})"
                )
            return zf.read(names[0]).decode("utf-8", errors="replace")
    except zipfile.BadZipFile as exc:
        raise FrenchAPIError(f"french: {dataset} response is not a valid zip") from exc


def _find_monthly_block(text: str) -> list[list[str]]:
    """Return ``[header, *data_rows]`` for the monthly table only.

    A Kenneth French CSV is a one-line text preamble, then the monthly table
    (a header row shaped like ``,Mkt-RF,SMB,HML,RF`` immediately followed by
    rows keyed by a 6-digit ``YYYYMM``), a blank line, then an "Annual
    Factors" table this function deliberately stops before (see the module
    docstring — annual is a separate, not-yet-built dataset variant).
    """
    rows = list(csv.reader(io.StringIO(text)))
    header_idx = None
    for i, row in enumerate(rows):
        cells = [c.strip() for c in row]
        if len(cells) < 2 or cells[0] != "" or not any(cells[1:]):
            continue
        nxt = rows[i + 1] if i + 1 < len(rows) else []
        if nxt and _month_to_date(nxt[0]):
            header_idx = i
            break
    if header_idx is None:
        return []

    header = [c.strip() for c in rows[header_idx]]
    data_rows: list[list[str]] = []
    for row in rows[header_idx + 1:]:
        if not row or not _month_to_date(row[0] if row else ""):
            break  # blank line or the "Annual Factors" section ends the block
        data_rows.append(row)
    return [header, *data_rows]


def normalize_french_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    track_vintage: bool = False,
    source: str = "french",
) -> list[dict[str, Any]]:
    """Explode a Kenneth French monthly CSV into scalar Silver rows — one per
    ``(<dataset>:<factor>, month)``.

    ``payload`` is :meth:`FrenchClient.get_observations`'s envelope
    ``{"format": "french-csv", "dataset": <name>, "text": <csv>}``.
    ``track_vintage`` is accepted for contract parity but has no effect: the
    library carries no vintages, so ``realtime_start``/``realtime_end`` are
    always empty, exactly like OECD/BIS/World Bank.
    """
    ingested_at = ingested_at or _utc_now_iso()
    if not isinstance(payload, dict):
        return []
    text = payload.get("text") or ""
    dataset = payload.get("dataset") or _dataset_name(series_id)
    _, factor_cols = DATASET_FILES.get(dataset, ("", ()))

    block = _find_monthly_block(text)
    if len(block) < 2:
        return []
    header, data_rows = block[0], block[1:]
    col_idx = {c: idx for idx, c in enumerate(header) if c}

    rows: list[dict[str, Any]] = []
    for data_row in data_rows:
        obs_date = _month_to_date(data_row[0])
        if not obs_date:
            continue
        for factor in factor_cols:
            idx = col_idx.get(factor)
            if idx is None or idx >= len(data_row):
                continue
            raw = data_row[idx]
            value = parse_value(raw)
            is_missing = value is None or value in _MISSING_SENTINELS
            if is_missing:
                value = None
            fac_series = f"{dataset}:{factor}"
            rows.append(
                {
                    "source": source,
                    "series_id": fac_series,
                    "observation_date": obs_date,
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": value,
                    "raw_value": None if raw is None else str(raw).strip(),
                    "is_missing": is_missing,
                    "row_hash": _row_hash(fac_series, obs_date, "", raw),
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                }
            )
    return rows


class FrenchClient(HTTPSource):
    """Keyless client for the Kenneth French Data Library's factor-return
    ZIP/CSV files."""

    source_name = "FRENCH"
    error_cls = FrenchAPIError

    def __init__(
        self,
        base_url: str = FRENCH_BASE_URL,
        *,
        dataset_files: Optional[dict[str, tuple[str, tuple[str, ...]]]] = None,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 20,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.dataset_files = dataset_files or dict(DATASET_FILES)
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        dataset = _dataset_name(series_id)
        filename, _ = self.dataset_files[dataset]
        return f"{FRENCH_FTP_PATH}/{filename}"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(self, series_id: str, **_ignored: Any) -> dict[str, Any]:
        """Download a dataset's ZIP and return its CSV member verbatim."""
        dataset = _dataset_name(series_id)
        blob = self._request(self.observations_endpoint(series_id), {}, as_bytes=True)
        text = _extract_csv_member(blob, dataset)
        return {"format": "french-csv", "dataset": dataset, "text": text}

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,
        source: str = "french",
        **_ignored: Any,
    ) -> list[dict[str, Any]]:
        return normalize_french_observations(
            series_id, payload, run_id=run_id, track_vintage=track_vintage, source=source
        )
