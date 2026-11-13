"""Historical expectations come from the cited Nasdaq notices, not fixtures."""

from dataclasses import asdict

import pytest

from fred_pipeline.settlement_evidence import (
    lookup_reported_settlement,
    lookup_settlement_day,
)
from fred_pipeline.terminal_views import compute_market_calendar


@pytest.mark.parametrize(
    ("closed", "notice"),
    [
        ("2018-12-05", "ETA2018-99"),
        ("2025-01-09", "ECA2024-632"),
    ],
)
def test_trading_closure_can_still_be_a_reported_settlement_day(closed, notice):
    trading = compute_market_calendar(closed, closed, calendars=("NYSE",))[0]
    result = lookup_settlement_day(market="NASDAQ_US_EQUITIES", day=closed)
    assert trading["is_business_day"] is False
    assert result.status == "REPORTED"
    assert result.contract_version == "settlement-evidence/1"
    assert result.catalog_version == "nasdaq-mourning-notices/1"
    assert result.is_settlement_day is True
    assert notice in result.evidence.source_url
    assert result.evidence.publication_time is None
    assert result.evidence.first_seen_at is None
    assert result.knowledge_basis == "CURRENT_VINTAGE"
    assert result.model_admission == "NOT_ADMITTED"


@pytest.mark.parametrize(
    ("trade_date", "settlement_date", "cycle", "notice"),
    [
        ("2018-12-03", "2018-12-05", "T+2", "ETA2018-99"),
        ("2018-12-04", "2018-12-06", "T+2", "ETA2018-99"),
        ("2025-01-08", "2025-01-09", "T+1", "ECA2024-632"),
        ("2025-01-10", "2025-01-13", "T+1", "ECA2024-632"),
    ],
)
def test_reported_settlement_is_the_notice_value_not_nyse_arithmetic(
    trade_date, settlement_date, cycle, notice
):
    result = lookup_reported_settlement(
        market="NASDAQ_US_EQUITIES", convention="REGULAR_WAY", trade_date=trade_date
    )
    assert result.status == "REPORTED"
    assert result.contract_version == "settlement-evidence/1"
    assert result.catalog_version == "nasdaq-mourning-notices/1"
    assert result.reported_settlement_date == settlement_date
    assert result.standard_cycle == cycle
    assert notice in result.evidence.source_url
    assert result.execution_verified is False
    assert result.model_admission == "NOT_ADMITTED"
    assert asdict(result)["evidence"]["publication_time"] is None


def test_missing_evidence_and_unsupported_scope_return_no_settlement():
    unknown = lookup_settlement_day(market="NASDAQ_US_EQUITIES", day="2025-01-10")
    assert unknown.status == "UNAVAILABLE"
    assert unknown.is_settlement_day is None
    assert unknown.evidence is None
    for market, convention in [
        ("NYSE", "REGULAR_WAY"),
        ("NASDAQ_US_EQUITIES", "CUSTOM"),
    ]:
        result = lookup_reported_settlement(
            market=market, convention=convention, trade_date="2018-12-04"
        )
        assert result.status == "UNAVAILABLE"
        assert result.reported_settlement_date is None
        assert result.evidence is None
    unknown_trade = lookup_reported_settlement(
        market="NASDAQ_US_EQUITIES", convention="REGULAR_WAY", trade_date="2025-01-07"
    )
    assert unknown_trade.status == "UNAVAILABLE"
    assert unknown_trade.reported_settlement_date is None


@pytest.mark.parametrize(
    "bad_date", ["2025-02-30", "2025-1-09", "", "not-a-date", None]
)
def test_invalid_dates_never_become_reported_or_assumed(bad_date):
    result = lookup_settlement_day(market="NASDAQ_US_EQUITIES", day=bad_date)
    assert result.status == "INVALID_INPUT"
    assert result.is_settlement_day is None
    assert result.evidence is None


def test_powerbi_calendar_catalog_does_not_present_arithmetic_as_verified_settlement():
    from fred_pipeline.global_views import powerbi_catalog_rows

    entry = next(
        row for row in powerbi_catalog_rows() if row["object_name"] == "market_calendar"
    )
    assert "not a verified settlement" in entry["description"].lower()


def test_report_plan_excludes_legacy_settlement_arithmetic_from_calendar_query():
    from pathlib import Path

    plan = (
        Path(__file__).resolve().parents[1]
        / "docs/reporting/powerbi_report_build_plan.md"
    ).read_text()
    query = plan.split("-- qMarketCalendar", 1)[1].split("```", 1)[0]
    assert "t2_settle_date" not in query


def test_invalid_untrusted_inputs_produce_serializable_null_results():
    import json

    day = lookup_settlement_day(market={"untrusted": True}, day=object())
    trade = lookup_reported_settlement(
        market="NASDAQ_US_EQUITIES", convention=[], trade_date=object()
    )
    assert day.status == "INVALID_INPUT"
    assert day.market is None and day.day is None
    assert trade.status == "INVALID_INPUT"
    assert trade.convention is None and trade.trade_date is None
    json.dumps(asdict(day))
    json.dumps(asdict(trade))


def test_offline_notice_lookup_never_attempts_a_provider_connection(monkeypatch):
    import socket

    def fail(*args, **kwargs):
        raise AssertionError("settlement evidence lookup must not connect")

    monkeypatch.setattr(socket, "socket", fail)
    result = lookup_reported_settlement(
        market="NASDAQ_US_EQUITIES", convention="REGULAR_WAY", trade_date="2025-01-08"
    )
    assert result.status == "REPORTED"
    assert result.source_mode == "CURATED_HISTORICAL_NOTICES"
    assert result.evidence.first_seen_at is None


def test_nyse_two_day_arithmetic_disagrees_with_the_2018_nasdaq_notice():
    trading = compute_market_calendar("2018-12-04", "2018-12-04", calendars=("NYSE",))[
        0
    ]
    reported = lookup_reported_settlement(
        market="NASDAQ_US_EQUITIES", convention="REGULAR_WAY", trade_date="2018-12-04"
    )
    assert trading["t2_settle_date"] == "2018-12-07"
    assert reported.reported_settlement_date == "2018-12-06"


def test_local_gold_catalog_materializes_the_settlement_limitation(tmp_path):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    warehouse = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="unused-for-test"),
        db_path=str(tmp_path / "gold.db"),
    )
    try:
        warehouse.build_gold()
        rows = warehouse.query(
            "SELECT description FROM gold_powerbi_catalog WHERE object_name='market_calendar'"
        )
        assert len(rows) == 1
        assert "not a verified settlement" in rows[0]["description"].lower()
    finally:
        warehouse.close()
