"""NYSE closure behavior is based on exchange announcements, not synthetic data."""

import pytest

from fred_pipeline.catalogs.nyse_adhoc_closures import NYSE_ADHOC_CLOSURES
from fred_pipeline.terminal_views import compute_market_calendar


@pytest.mark.parametrize(
    ("closed", "previous", "following", "after_following"),
    [
        ("2018-12-05", "2018-12-04", "2018-12-06", "2018-12-07"),
        ("2025-01-09", "2025-01-08", "2025-01-10", "2025-01-13"),
    ],
)
def test_announced_nyse_mourning_closures_and_adjacent_sessions(
    closed, previous, following, after_following
):
    rows = compute_market_calendar(previous, following)
    by_key = {(row["calendar_name"], row["calendar_date"]): row for row in rows}
    closure = by_key[("NYSE", closed)]
    assert closure["is_business_day"] is False
    assert closure["is_holiday"] is True
    assert "Mourning" in closure["holiday_name"]
    assert closure["prior_business_day"] == previous
    assert closure["next_business_day"] == following
    assert by_key[("NYSE", previous)]["is_business_day"] is True
    assert by_key[("NYSE", following)]["is_business_day"] is True
    # This tests calendar arithmetic, not an exchange clearing schedule.
    assert by_key[("NYSE", previous)]["t2_settle_date"] == after_following
    # NYSE source notices do not establish closures on other calendars.
    assert by_key[("SIFMA", closed)]["is_business_day"] is True
    assert by_key[("FEDWIRE", closed)]["is_business_day"] is True


def test_narrow_calendar_window_still_accounts_for_nearby_closure():
    row = compute_market_calendar("2025-01-08", "2025-01-08", calendars=("NYSE",))[0]
    assert row["next_business_day"] == "2025-01-10"
    assert row["t2_settle_date"] == "2025-01-13"


def test_local_gold_build_materializes_the_reviewed_nyse_closure(tmp_path):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    warehouse = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="test"),
        db_path=str(tmp_path / "calendar.db"),
    )
    try:
        warehouse.merge_silver(
            [
                {
                    "source": "fred",
                    "series_id": "TEST",
                    "observation_date": day,
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": 1.0,
                    "raw_value": "1",
                    "is_missing": 0,
                    "row_hash": day,
                    "revision_number": 1,
                    "ingested_at": "2025-01-11T00:00:00+00:00",
                    "run_id": "test",
                }
                for day in ("2025-01-08", "2025-01-10")
            ]
        )
        warehouse.build_gold()
        rows = warehouse.query(
            "SELECT calendar_date, is_business_day, holiday_name "
            "FROM gold_market_calendar WHERE calendar_name = 'NYSE' "
            "ORDER BY calendar_date"
        )
        assert [row["calendar_date"] for row in rows] == [
            "2025-01-08",
            "2025-01-09",
            "2025-01-10",
        ]
        assert rows[1]["is_business_day"] == 0
        assert "Mourning" in rows[1]["holiday_name"]
    finally:
        warehouse.close()


def test_reviewed_closure_catalog_has_unique_dates_and_exchange_sources():
    dates = [closure.date for closure in NYSE_ADHOC_CLOSURES]
    assert len(dates) == len(set(dates))
    assert all(
        closure.name and closure.source_url.startswith("https://")
        for closure in NYSE_ADHOC_CLOSURES
    )
