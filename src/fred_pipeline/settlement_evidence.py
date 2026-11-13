"""Narrow, offline settlement evidence; never infer a settlement calendar.

Only the historical Nasdaq notices listed here are covered. A reported date is
not proof that a particular trade settled and is not a point-in-time feed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Literal, TypeGuard

Status = Literal["REPORTED", "UNAVAILABLE", "INVALID_INPUT"]
_MARKET = "NASDAQ_US_EQUITIES"
_CONVENTION = "REGULAR_WAY"
_SOURCE_MODE = "CURATED_HISTORICAL_NOTICES"
_KNOWLEDGE = "CURRENT_VINTAGE"
_CONTRACT_VERSION = "settlement-evidence/1"
_CATALOG_VERSION = "nasdaq-mourning-notices/1"


@dataclass(frozen=True)
class NoticeEvidence:
    source: str
    source_url: str
    published_on: str
    reviewed_on: str
    publication_time: None = None
    first_seen_at: None = None


@dataclass(frozen=True)
class SettlementDayResult:
    status: Status
    market: str | None
    day: str | None
    is_settlement_day: bool | None
    evidence: NoticeEvidence | None
    reason: str | None
    contract_version: str = _CONTRACT_VERSION
    catalog_version: str = _CATALOG_VERSION
    source_mode: str = _SOURCE_MODE
    knowledge_basis: str = _KNOWLEDGE
    model_admission: str = "NOT_ADMITTED"


@dataclass(frozen=True)
class ReportedSettlementResult:
    status: Status
    market: str | None
    convention: str | None
    trade_date: str | None
    reported_settlement_date: str | None
    standard_cycle: str | None
    cycle_source_url: str | None
    evidence: NoticeEvidence | None
    reason: str | None
    execution_verified: bool = False
    contract_version: str = _CONTRACT_VERSION
    catalog_version: str = _CATALOG_VERSION
    source_mode: str = _SOURCE_MODE
    knowledge_basis: str = _KNOWLEDGE
    model_admission: str = "NOT_ADMITTED"


_BUSH_NOTICE = NoticeEvidence(
    source="Nasdaq",
    source_url="https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2018-99",
    published_on="2018-12-03",
    reviewed_on="2026-11-12",
)
_CARTER_NOTICE = NoticeEvidence(
    source="Nasdaq",
    source_url="https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2024-632",
    published_on="2024-12-31",
    reviewed_on="2026-11-12",
)
_SEC_T2 = "https://www.sec.gov/newsroom/press-releases/2017-68-0"
_SEC_T1 = "https://www.sec.gov/newsroom/press-releases/2024-62"

# Each value is explicitly stated by the linked notice. These are not a date
# range, market-wide calendar, live feed, or proof of an individual settlement.
_SETTLEMENT_DAYS = MappingProxyType(
    {"2018-12-05": _BUSH_NOTICE, "2025-01-09": _CARTER_NOTICE}
)
_REPORTED_TRADES = MappingProxyType(
    {
        "2018-12-03": ("2018-12-05", "T+2", _SEC_T2, _BUSH_NOTICE),
        "2018-12-04": ("2018-12-06", "T+2", _SEC_T2, _BUSH_NOTICE),
        "2018-12-06": ("2018-12-10", "T+2", _SEC_T2, _BUSH_NOTICE),
        "2025-01-08": ("2025-01-09", "T+1", _SEC_T1, _CARTER_NOTICE),
        "2025-01-10": ("2025-01-13", "T+1", _SEC_T1, _CARTER_NOTICE),
    }
)


def _valid_date(value: object) -> TypeGuard[str]:
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _valid_scope(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and 0 < len(value) <= 128 and bool(value.strip())


def _safe_text(value: object) -> str | None:
    return value if isinstance(value, str) and len(value) <= 128 else None


def lookup_settlement_day(*, market: object, day: object) -> SettlementDayResult:
    """Return a notice-backed day status only for the exact recorded scope/date."""
    if not _valid_date(day) or not _valid_scope(market):
        return SettlementDayResult(
            "INVALID_INPUT",
            _safe_text(market),
            _safe_text(day),
            None,
            None,
            "INVALID_REQUEST",
        )
    if market != _MARKET:
        return SettlementDayResult(
            "UNAVAILABLE", market, day, None, None, "UNSUPPORTED_SCOPE"
        )
    evidence = _SETTLEMENT_DAYS.get(day)
    if evidence is None:
        return SettlementDayResult(
            "UNAVAILABLE", market, day, None, None, "NO_REVIEWED_NOTICE"
        )
    return SettlementDayResult("REPORTED", market, day, True, evidence, None)


def lookup_reported_settlement(
    *, market: object, convention: object, trade_date: object
) -> ReportedSettlementResult:
    """Look up an exact reported regular-way date; never calculate a fallback."""
    if (
        not _valid_date(trade_date)
        or not _valid_scope(market)
        or not _valid_scope(convention)
    ):
        return ReportedSettlementResult(
            "INVALID_INPUT",
            _safe_text(market),
            _safe_text(convention),
            _safe_text(trade_date),
            None,
            None,
            None,
            None,
            "INVALID_REQUEST",
        )
    if market != _MARKET or convention != _CONVENTION:
        return ReportedSettlementResult(
            "UNAVAILABLE",
            market,
            convention,
            trade_date,
            None,
            None,
            None,
            None,
            "UNSUPPORTED_SCOPE",
        )
    result = _REPORTED_TRADES.get(trade_date)
    if result is None:
        return ReportedSettlementResult(
            "UNAVAILABLE",
            market,
            convention,
            trade_date,
            None,
            None,
            None,
            None,
            "NO_REVIEWED_NOTICE",
        )
    settlement_date, cycle, cycle_source_url, evidence = result
    return ReportedSettlementResult(
        "REPORTED",
        market,
        convention,
        trade_date,
        settlement_date,
        cycle,
        cycle_source_url,
        evidence,
        None,
    )
