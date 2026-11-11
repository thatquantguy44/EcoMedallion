"""ETF constituent-holdings source client (iShares / State Street CSV).

Index *membership* is licensed data with no good free API. The free,
defensible workaround: iShares and State Street publish full ETF holdings as
daily CSV. This client fetches a fund's holdings file (keyless) and explodes
it into per-constituent **weight** rows, so ``gold.index_constituents`` can be
built and the same list can seed the equity symbol universe
(:func:`build_equity_manifest`).

A manifest ``series_id`` is the ETF ticker (e.g. ``IVV``, ``SPY``); the
download URL is resolved from :data:`HOLDINGS_URLS`. One fetch explodes into
one scalar Silver series per constituent — ``series_id = <ETF>:<constituent>``,
``value = weight %``, ``observation_date = the file's "as of" date`` — so
membership/weight history accumulates through the normal incremental path with
no schema change. The holdings CSV has a preamble; the parser locates the
"as of" date and the header row tolerantly rather than assuming fixed offsets.

⚠️ The URLs in :data:`HOLDINGS_URLS` are product-specific and change; verify
each against the fund's page before activating.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from datetime import datetime
from html import unescape
from typing import Any, Callable, Optional
from xml.etree import ElementTree as ET

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.ishares")

# ETF ticker -> holdings-CSV URL. Starter set; VERIFY before activating.
HOLDINGS_URLS: dict[str, str] = {
    # iShares Core S&P 500 ETF (IVV) — "Detailed Holdings and Analytics".
    "IVV": (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/"
        "product-data/api/v1/get-fund-document?appType=PRODUCT_PAGE"
        "&appSubType=ISHARES&targetSite=us-ishares&locale=en_US"
        "&portfolioId=239726&component=fundDownload&userType=individual"
    ),
    # State Street SPDR S&P 500 ETF (SPY) — SSGA holds-daily CSV.
    "SPY": (
        "https://www.ssga.com/us/en/intermediary/etfs/library-content/"
        "products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    ),
}

# "as of" date formats seen in iShares/SSGA preambles.
_DATE_FORMATS = ("%b %d, %Y", "%d-%b-%Y", "%m/%d/%Y", "%Y-%m-%d")
_ASOF_RE = re.compile(r"as of[\"',]*\s*[\"']?([A-Za-z0-9 ,/\-]+?)[\"']?\s*$", re.I)

# Asset-class / ticker values that are not equity constituents.
_NON_EQUITY = {"", "-", "CASH", "USD", "MARGIN_USD", "BRL", "FX"}


def _parse_asof_value(raw: str) -> Optional[str]:
    raw = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _parse_asof(text: str) -> Optional[str]:
    """Pull the holdings 'as of' date from the CSV preamble → ISO date."""
    for line in text.splitlines()[:15]:
        m = _ASOF_RE.search(line.strip())
        if not m:
            continue
        parsed = _parse_asof_value(m.group(1))
        if parsed:
            return parsed
    return None


def _find_header(lines: list[str]) -> int:
    """Index of the holdings header row (contains 'Ticker' and a 'Weight'
    column), or -1."""
    for i, line in enumerate(lines):
        low = line.lower()
        if "ticker" in low and "weight" in low:
            return i
    return -1


_SS_NS = "{urn:schemas-microsoft-com:office:spreadsheet}"
_TAG = r"(?:\w+:)?"


def _strip_cell_text(value: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", value)).strip()


def _xml_spreadsheet_rows_regex(text: str) -> list[list[str]]:
    sheet_match = re.search(
        rf"<{_TAG}Worksheet\b[^>]*(?:\w+:)?Name=\"Holdings\"[^>]*>"
        rf"(.*?)</{_TAG}Worksheet>",
        text,
        re.S,
    )
    sheet = sheet_match.group(1) if sheet_match else text
    rows: list[list[str]] = []
    for row_match in re.finditer(rf"<{_TAG}Row\b[^>]*>(.*?)</{_TAG}Row>", sheet, re.S):
        values: list[str] = []
        for cell_match in re.finditer(
            rf"<{_TAG}Cell\b([^>]*)>(.*?)</{_TAG}Cell>",
            row_match.group(1),
            re.S,
        ):
            attrs, body = cell_match.groups()
            index_match = re.search(r'(?:\w+:)?Index="(\d+)"', attrs)
            if index_match:
                while len(values) < int(index_match.group(1)) - 1:
                    values.append("")
            data_match = re.search(
                rf"<{_TAG}Data\b[^>]*>(.*?)</{_TAG}Data>", body, re.S
            )
            values.append(_strip_cell_text(data_match.group(1)) if data_match else "")
        rows.append(values)
    return rows


def _xml_spreadsheet_rows(text: str) -> list[list[str]]:
    """Read the current BlackRock/iShares XML workbook holdings sheet."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return _xml_spreadsheet_rows_regex(text)
    worksheet = None
    for candidate in root.findall(f".//{_SS_NS}Worksheet"):
        if candidate.attrib.get(f"{_SS_NS}Name") == "Holdings":
            worksheet = candidate
            break
    if worksheet is None:
        worksheet = root.find(f".//{_SS_NS}Worksheet")
    if worksheet is None:
        return []

    rows: list[list[str]] = []
    for row in worksheet.findall(f".//{_SS_NS}Row"):
        values: list[str] = []
        for cell in row.findall(f"{_SS_NS}Cell"):
            index = cell.attrib.get(f"{_SS_NS}Index")
            if index and index.isdigit():
                while len(values) < int(index) - 1:
                    values.append("")
            data = cell.find(f"{_SS_NS}Data")
            values.append("" if data is None or data.text is None else data.text)
        rows.append(values)
    return rows


def _parse_asof_from_table(rows: list[list[str]]) -> Optional[str]:
    for row in rows[:20]:
        for i, cell in enumerate(row):
            if "as of" not in cell.lower():
                continue
            for value in row[i + 1 :]:
                parsed = _parse_asof_value(value)
                if parsed:
                    return parsed
    return None


def _find_header_row(rows: list[list[str]]) -> int:
    for i, row in enumerate(rows):
        cols = [c.lower().strip() for c in row]
        if "ticker" in cols and _weight_column_index(row) is not None:
            return i
    return -1


def _weight_column_index(headers: list[str]) -> Optional[int]:
    """Use market weights from current downloads, or the legacy Weight column."""
    for prefix in ("market weight", "weight"):
        for index, header in enumerate(headers):
            if header.lower().strip().startswith(prefix):
                return index
    return None


def _normalize_holdings_table(
    series_id: str,
    rows_in: list[list[str]],
    *,
    as_of: str,
    run_id: Optional[str],
    ingested_at: str,
    source: str,
) -> list[dict[str, Any]]:
    hi = _find_header_row(rows_in)
    if hi < 0:
        return []
    headers = rows_in[hi]
    cols = {c.lower().strip(): idx for idx, c in enumerate(headers)}
    tk_idx = cols.get("ticker")
    wt_idx = _weight_column_index(headers)
    if tk_idx is None or wt_idx is None:
        return []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rec in rows_in[hi + 1 :]:
        ticker = rec[tk_idx].strip().upper() if tk_idx < len(rec) else ""
        if ticker in _NON_EQUITY or ticker in seen:
            continue
        raw = rec[wt_idx] if wt_idx < len(rec) else None
        weight = parse_value(raw)
        if weight is None:
            continue
        seen.add(ticker)
        constituent_series = f"{series_id.upper()}:{ticker}"
        row = {
            "source": source,
            "series_id": constituent_series,
            "observation_date": as_of,
            "realtime_start": "",
            "realtime_end": "",
            "value": weight,
            "raw_value": None if raw is None else str(raw),
            "is_missing": False,
            "row_hash": _row_hash(constituent_series, as_of, "", raw),
            "ingested_at": ingested_at,
            "run_id": run_id,
        }
        rows.append(row)
    return rows


def normalize_ishares_holdings(
    series_id: str,
    payload: Any,
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "ishares",
) -> list[dict[str, Any]]:
    """Explode a holdings CSV into one weight row per constituent.

    ``payload`` is :meth:`ISharesClient.get_observations`'s envelope
    ``{"format": "csv", "etf": <ETF>, "text": <csv>}``. Emits
    ``series_id = <ETF>:<constituent-ticker>``, ``value = weight %``. A file
    with no parseable "as of" date or header yields no rows (logged) rather
    than guessing a date.
    """
    ingested_at = ingested_at or _utc_now_iso()
    if not isinstance(payload, dict):
        return []
    text = payload.get("text") or ""
    etf = (payload.get("etf") or series_id).upper()
    stripped = text.lstrip("\ufeff\r\n\t ")
    if stripped.startswith("<?xml") or "<ss:Workbook" in stripped[:500]:
        table_rows = _xml_spreadsheet_rows(stripped)
        as_of = _parse_asof_from_table(table_rows)
        if not as_of:
            log.warning("ishares %s: no parseable 'as of' date; skipping", etf)
            return []
        return _normalize_holdings_table(
            etf,
            table_rows,
            as_of=as_of,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )

    as_of = _parse_asof(text)
    if not as_of or not text.strip():
        log.warning("ishares %s: no parseable 'as of' date; skipping", etf)
        return []

    lines = text.splitlines()
    hi = _find_header(lines)
    if hi < 0:
        return []
    reader = csv.DictReader(io.StringIO("\n".join(lines[hi:])))
    cols = {c.lower().strip(): c for c in (reader.fieldnames or [])}
    tk_col = cols.get("ticker")
    weight_index = _weight_column_index(reader.fieldnames or [])
    wt_col = reader.fieldnames[weight_index] if weight_index is not None else None
    if not tk_col or not wt_col:
        return []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rec in reader:
        ticker = (rec.get(tk_col) or "").strip().upper()
        if ticker in _NON_EQUITY or ticker in seen:
            continue
        weight = parse_value(rec.get(wt_col))
        if weight is None:
            continue
        seen.add(ticker)
        constituent_series = f"{etf}:{ticker}"
        raw = rec.get(wt_col)
        row = {
            "source": source,
            "series_id": constituent_series,
            "observation_date": as_of,
            "realtime_start": "",
            "realtime_end": "",
            "value": weight,
            "raw_value": None if raw is None else str(raw),
            "is_missing": False,
            "row_hash": _row_hash(constituent_series, as_of, "", raw),
            "ingested_at": ingested_at,
            "run_id": run_id,
        }
        rows.append(row)
    return rows


class ISharesClient(HTTPSource):
    """Keyless ETF-holdings CSV client (iShares / State Street)."""

    source_name = "ISHARES"
    error_cls = SourceError

    def __init__(
        self,
        base_url: str = "https://www.ishares.com",
        *,
        holdings_urls: Optional[dict[str, str]] = None,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 30,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.holdings_urls = holdings_urls or dict(HOLDINGS_URLS)
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _resolve_url(self, series_id: str) -> str:
        etf = series_id.strip().upper()
        url = self.holdings_urls.get(etf)
        if not url:
            raise SourceError(
                f"ishares: no holdings URL registered for ETF {etf!r}; add it to "
                f"HOLDINGS_URLS or pass holdings_urls="
            )
        return url

    def observations_endpoint(self, series_id: str) -> str:
        return self._resolve_url(series_id)

    # ---- SourceClient contract ------------------------------------------

    def get_observations(self, series_id: str, **_ignored: Any) -> dict[str, Any]:
        """Fetch a fund's holdings CSV verbatim."""
        etf = series_id.strip().upper()
        url = self._resolve_url(series_id)
        text = self._request(url, {}, as_text=True)
        return {"format": "csv", "etf": etf, "text": text}

    def normalize(
        self,
        series_id: str,
        payload: Any,
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,
        source: str = "ishares",
        **_ignored: Any,
    ) -> list[dict[str, Any]]:
        return normalize_ishares_holdings(
            series_id, payload, run_id=run_id, source=source
        )


def build_equity_manifest(
    holdings_csv: str,
    *,
    name: str = "equity_stooq",
    field: str = "close",
    source: str = "stooq",
    active: bool = False,
    include_etf: Optional[str] = None,
) -> dict[str, Any]:
    """Generate a Stooq price manifest from an ETF holdings CSV — the symbol
    universe generator (analogous to ``sources.sec.build_sec_manifest``).

    Parses the holdings file for its constituent tickers and emits one manifest
    series per ticker (``<ticker>:<field>``), optionally prepending the ETF
    itself (``include_etf``) so the fund's own price is pulled too. Shipped
    ``active=False`` by default — review before enabling.
    """
    payload = {"format": "csv", "etf": include_etf or "ETF", "text": holdings_csv}
    holdings = normalize_ishares_holdings("_", payload, source="ishares")
    tickers = [r["series_id"].split(":", 1)[1] for r in holdings]
    if include_etf:
        tickers = [include_etf.upper(), *tickers]

    series = [
        {
            "series_id": f"{t}:{field}",
            "title": f"{t} daily {field}",
            "category": "equity",
            "frequency": "d",
            "units": "USD" if field != "volume" else "Shares",
            "source": source,
            "active": active,
            "vintage_enabled": False,
            "validation_profile": "standard",
            "downstream_use_case": "equity_price_return",
            "priority": 3,
            "tags": ["equity", field, source],
        }
        for t in tickers
    ]
    return {
        "name": name,
        "description": (
            f"Equity price manifest generated from an ETF holdings file "
            f"({len(series)} tickers, {field}). VERIFY tickers before activating."
        ),
        "version": 1,
        "series": series,
    }
