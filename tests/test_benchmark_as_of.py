from datetime import date, datetime, timezone

import pytest

from fred_pipeline.benchmark_as_of import benchmark_as_of_date


def test_benchmark_default_uses_new_york_date(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2024, 1, 2, 2, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr("fred_pipeline.benchmark_as_of.datetime", FixedDatetime)
    assert benchmark_as_of_date() == date(2024, 1, 1)
    assert benchmark_as_of_date(date(2023, 12, 31)) == date(2023, 12, 31)
    with pytest.raises(ValueError, match="future"):
        benchmark_as_of_date(date(2024, 1, 2))
    with pytest.raises(ValueError, match="calendar date"):
        benchmark_as_of_date("2024-01-01")


def test_select_vintages_uses_capture_bound_and_discloses_unknown_start():
    from fred_pipeline.benchmark_as_of import select_benchmark_vintages

    def row(start, value, captured):
        return {
            "source": "fred",
            "series_id": "SOFR",
            "observation_date": "2024-01-02",
            "realtime_start": start,
            "value": value,
            "is_missing": False,
            "ingested_at": captured,
        }

    rows = [
        row("", 2.0, "2024-01-03T03:00:00+00:00"),
        row("2024-01-03", 4.0, "2024-01-06T00:00:00+00:00"),
        row("2024-01-06", 8.0, "2024-01-04T00:00:00+00:00"),
    ]
    # Jan 5 at 00:00 UTC is still Jan 4 in New York; the Jan 6 capture is too late.
    selected = select_benchmark_vintages(rows, date(2024, 1, 4))
    assert len(selected) == 1 and selected[0]["value"] == 2.0
    assert selected[0]["realtime_start"] == ""
    with pytest.raises(ValueError, match="capture timestamp"):
        select_benchmark_vintages(
            [row("", 1.0, "2024-01-02T12:00:00")], date(2024, 1, 4)
        )


def test_malformed_revision_date_fails_closed():
    from fred_pipeline.benchmark_as_of import select_benchmark_vintages

    rows = [
        {
            "source": "fred",
            "series_id": "SOFR",
            "observation_date": "2024-01-02",
            "realtime_start": "not-a-date",
            "ingested_at": "2024-01-02T12:00:00+00:00",
        }
    ]
    with pytest.raises(ValueError, match="Invalid benchmark date"):
        select_benchmark_vintages(rows, date(2024, 1, 5))


def test_gold_cli_rejects_future_cutoff_before_creating_database(tmp_path):
    from datetime import timedelta
    from fred_pipeline.cli import main

    future = (date.today() + timedelta(days=2)).isoformat()
    database = tmp_path / "must-not-exist.db"
    with pytest.raises(ValueError, match="future"):
        main(
            ["gold", "--local", "--db-path", str(database), "--benchmark-as-of", future]
        )
    assert not database.exists()
