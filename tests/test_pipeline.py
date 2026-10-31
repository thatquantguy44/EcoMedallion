import threading
from datetime import datetime, timedelta, timezone

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.fred_client import FredAPIError
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec, ValidationProfile
from fred_pipeline.pipeline import (
    BatchBufferLimitExceeded,
    _CADENCE_MIN_INTERVAL,
    FredPipeline,
    _extract_workers_for_source,
    _make_tiingo,
    _normalize_tiingo_keys,
    _rate_limit_for_source,
    _series_is_due,
)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _spec(series_id, **kw):
    kw.setdefault("title", series_id)
    kw.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kw)


def test_normalize_tiingo_keys_accepts_string_or_list():
    assert _normalize_tiingo_keys("abc") == ["abc"]
    assert _normalize_tiingo_keys("") == []
    assert _normalize_tiingo_keys(["a", "b"]) == ["a", "b"]
    assert _normalize_tiingo_keys(["a", "", "b"]) == ["a", "b"]
    assert _normalize_tiingo_keys(None) == []


def test_make_tiingo_wires_primary_and_backup_keys():
    config = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        tiingo_api_key=["primary", "backup1", "backup2"],
    )
    client = _make_tiingo(config)
    assert client.api_key == "primary"
    assert client._backup_keys == ["backup1", "backup2"]


def test_make_tiingo_still_accepts_a_single_string_key():
    config = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        tiingo_api_key="only",
    )
    client = _make_tiingo(config)
    assert client.api_key == "only"
    assert client._backup_keys == []


def test_run_without_spark_records_audit(observations_payload, fake_client_cls):
    client = fake_client_cls({"DGS10": observations_payload})
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run([_spec("DGS10")])

    assert run.status == RunStatus.SUCCEEDED
    assert run.series_total == 1
    assert run.series_succeeded == 1
    sr = run.series_runs[0]
    assert sr.status == RunStatus.SUCCEEDED
    assert sr.observations_extracted == 4
    assert sr.dq_passed is True
    assert sr.duration_seconds is not None


def test_concurrent_extraction_preserves_order_and_isolation(
    observations_payload, fake_client_cls
):
    """Threaded extraction (phase 2) must still yield deterministic,
    per-series-isolated results in original spec order once phase 3 replays
    them sequentially — regardless of worker count or completion order."""
    client = fake_client_cls(
        {sid: observations_payload for sid in ("A", "B", "C", "D", "BAD_E", "F")},
        errors={"BAD_E": FredAPIError("boom", 400)},
    )
    config = PipelineConfig(
        environment=Environment.DEV, fred_api_key="k", extract_workers=4
    )
    pipe = FredPipeline(config, client=client, spark=None, persist_audit=False)
    specs = [_spec(sid) for sid in ("A", "B", "C", "D", "BAD_E", "F")]
    run = pipe.run(specs)

    assert run.status == RunStatus.PARTIAL
    assert run.series_succeeded == 5
    assert run.series_failed == 1
    # audit rows preserve the original spec order regardless of thread timing
    assert [sr.series_id for sr in run.series_runs] == [
        "A",
        "B",
        "C",
        "D",
        "BAD_E",
        "F",
    ]
    bad = run.series_runs[4]
    assert bad.status == RunStatus.FAILED
    assert "boom" in bad.error_message
    for sid in ("A", "B", "C", "D", "F"):
        sr = next(s for s in run.series_runs if s.series_id == sid)
        assert sr.status == RunStatus.SUCCEEDED


def test_extraction_streams_completed_series_before_slow_future(
    observations_payload,
):
    fast_written = threading.Event()

    class BlockingClient:
        source_name = "fred"

        def get_observations(self, series_id, **kwargs):
            if series_id == "SLOW" and not fast_written.wait(timeout=2):
                raise AssertionError("SLOW completed before FAST was written")
            return observations_payload

    class StreamingWarehouse:
        supports_incremental_audit = True

        def __init__(self):
            self.series_audit = []
            self.run_states = []

        def restate_start(self, series_id, n):
            return None

        def write_bronze(self, rows):
            if rows[0]["series_id"] == "FAST":
                fast_written.set()
            return len(rows)

        def merge_silver(self, rows):
            return len(rows)

        def persist_dq(self, run_id, report):
            pass

        def persist_run_state(self, run):
            self.run_states.append(
                (run.status, run.series_succeeded, run.series_failed)
            )

        def persist_series_run(self, series_run):
            self.series_audit.append(series_run.series_id)

        def build_gold(self):
            return {}

        def write_release_calendar(self, rows):
            return len(rows)

        def persist_run(self, run):
            pass

    wh = StreamingWarehouse()
    cfg = PipelineConfig(
        environment=Environment.DEV, fred_api_key="k", extract_workers=2
    )
    pipe = FredPipeline(cfg, client=BlockingClient(), warehouse=wh)

    run = pipe.run([_spec("SLOW"), _spec("FAST")], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    assert fast_written.is_set()
    assert wh.series_audit[0] == "FAST"
    assert [sr.series_id for sr in run.series_runs] == ["SLOW", "FAST"]
    assert wh.run_states[0] == (RunStatus.RUNNING, 1, 0)


def test_series_failure_is_isolated(observations_payload, fake_client_cls):
    client = fake_client_cls(
        {"GOOD": observations_payload},
        errors={"BAD": FredAPIError("boom", 400)},
    )
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run([_spec("BAD"), _spec("GOOD")])

    assert run.status == RunStatus.PARTIAL
    assert run.series_succeeded == 1
    assert run.series_failed == 1
    bad = next(s for s in run.series_runs if s.series_id == "BAD")
    assert bad.status == RunStatus.FAILED
    assert "boom" in bad.error_message


def test_tiingo_incremental_plan_uses_adjclose_watermark():
    class Warehouse:
        def __init__(self):
            self.requested = []

        def restate_start(self, series_id, n):
            self.requested.append((series_id, n))
            return "2026-07-17"

    cfg = PipelineConfig(environment=Environment.DEV, tiingo_api_key="k")
    wh = Warehouse()
    pipe = FredPipeline(cfg, warehouse=wh, persist_audit=False)

    start, load_type = pipe._plan_extract(
        _spec("AAPL", source="tiingo", vintage_enabled=False)
    )

    assert start == "2026-07-17"
    assert load_type == f"restate_last_{cfg.restate_last_n}"
    assert wh.requested == [("AAPL:adjClose", cfg.restate_last_n)]


def test_strict_profile_fails_run_on_dq(fake_client_cls):
    all_missing = {
        "observations": [
            {
                "date": "2024-01-01",
                "value": ".",
                "realtime_start": "2024-01-02",
                "realtime_end": "9999-12-31",
            },
            {
                "date": "2024-01-02",
                "value": ".",
                "realtime_start": "2024-01-03",
                "realtime_end": "9999-12-31",
            },
        ]
    }
    client = fake_client_cls({"CPIAUCSL": all_missing})
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run([_spec("CPIAUCSL", validation_profile=ValidationProfile.STRICT)])

    assert run.status == RunStatus.FAILED
    sr = run.series_runs[0]
    assert sr.status == RunStatus.FAILED
    assert sr.dq_passed is False
    assert "Data quality failed" in sr.error_message


def test_run_batched_writes_each_data_layer_once_and_excludes_failed_dq(
    observations_payload, fake_client_cls
):
    all_missing = {
        "observations": [
            {
                "date": "2024-01-01",
                "value": ".",
                "realtime_start": "2024-01-02",
                "realtime_end": "9999-12-31",
            }
        ]
    }

    class BatchedWarehouse:
        supports_incremental_audit = True

        def __init__(self):
            self.bronze_batches = []
            self.silver_batches = []
            self.dq_batches = []
            self.incremental_audit_calls = 0

        def restate_start(self, series_id, n):
            return None

        def write_bronze(self, rows):
            self.bronze_batches.append(rows)
            return len(rows)

        def merge_silver(self, rows):
            self.silver_batches.append(rows)
            return len(rows)

        def persist_dq_reports(self, run_id, reports):
            self.dq_batches.append((run_id, reports))

        def persist_run_state(self, run):
            self.incremental_audit_calls += 1

        def persist_series_run(self, series_run):
            self.incremental_audit_calls += 1

        def persist_run(self, run):
            pass

    warehouse = BatchedWarehouse()
    config = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        persistence_write_mode="run_batched",
    )
    pipe = FredPipeline(
        config,
        client=fake_client_cls({"GOOD": observations_payload, "BAD": all_missing}),
        warehouse=warehouse,
    )

    run = pipe.run(
        [
            _spec("GOOD"),
            _spec("BAD", validation_profile=ValidationProfile.STRICT),
        ],
        build_gold_layer=False,
    )

    assert run.status == RunStatus.PARTIAL
    assert len(warehouse.bronze_batches) == 1
    assert {row["series_id"] for row in warehouse.bronze_batches[0]} == {"GOOD", "BAD"}
    assert len(warehouse.silver_batches) == 1
    assert {row["series_id"] for row in warehouse.silver_batches[0]} == {"GOOD"}
    assert len(warehouse.dq_batches) == 1
    assert {report.series_id for report in warehouse.dq_batches[0][1]} == {
        "GOOD",
        "BAD",
    }
    assert warehouse.incremental_audit_calls == 0

    good = next(sr for sr in run.series_runs if sr.series_id == "GOOD")
    bad = next(sr for sr in run.series_runs if sr.series_id == "BAD")
    assert good.status == RunStatus.SUCCEEDED
    assert good.rows_written_bronze == 1
    assert good.rows_merged_silver == len(warehouse.silver_batches[0])
    assert bad.status == RunStatus.FAILED
    assert bad.rows_written_bronze == 1
    assert bad.rows_merged_silver == 0
    assert bad.dq_passed is False


def test_run_batched_buffer_limit_fails_before_any_data_write(
    observations_payload, fake_client_cls
):
    class GuardedWarehouse:
        def __init__(self):
            self.write_attempted = False

        def restate_start(self, series_id, n):
            return None

        def write_bronze(self, rows):
            self.write_attempted = True
            return len(rows)

        def merge_silver(self, rows):
            self.write_attempted = True
            return len(rows)

        def persist_dq_reports(self, run_id, reports):
            self.write_attempted = True

    warehouse = GuardedWarehouse()
    config = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        persistence_write_mode="run_batched",
        persistence_batch_max_buffer_bytes=1,
    )
    pipe = FredPipeline(
        config,
        client=fake_client_cls({"TOO_LARGE": observations_payload}),
        warehouse=warehouse,
        persist_audit=False,
    )

    with pytest.raises(BatchBufferLimitExceeded, match="per_series"):
        pipe.run([_spec("TOO_LARGE")], build_gold_layer=False)

    assert warehouse.write_attempted is False


def test_run_batched_all_extraction_failures_do_not_issue_empty_writes(
    fake_client_cls,
):
    class NoEmptyWritesWarehouse:
        def __init__(self):
            self.write_attempted = False

        def restate_start(self, series_id, n):
            return None

        def write_bronze(self, rows):
            self.write_attempted = True
            raise AssertionError("an empty raw batch must not be written")

        def merge_silver(self, rows):
            self.write_attempted = True
            raise AssertionError("an empty cleaned batch must not be written")

    warehouse = NoEmptyWritesWarehouse()
    config = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        persistence_write_mode="run_batched",
    )
    pipe = FredPipeline(
        config,
        client=fake_client_cls({}, errors={"BROKEN": FredAPIError("boom", 400)}),
        warehouse=warehouse,
        persist_audit=False,
    )

    run = pipe.run([_spec("BROKEN")], build_gold_layer=False)

    assert run.status == RunStatus.FAILED
    assert run.series_runs[0].status == RunStatus.FAILED
    assert warehouse.write_attempted is False


def test_run_batched_writer_failure_completes_series_and_persists_failure_audit(
    observations_payload, fake_client_cls
):
    class FailingCleanedWarehouse:
        def __init__(self):
            self.bronze_batches = []
            self.persisted_runs = []

        def restate_start(self, series_id, n):
            return None

        def write_bronze(self, rows):
            self.bronze_batches.append(rows)
            return len(rows)

        def merge_silver(self, rows):
            raise RuntimeError("cleaned storage is unavailable")

        def persist_dq_reports(self, run_id, reports):
            raise AssertionError("quality phase must not run after cleaned failure")

        def persist_run(self, run):
            self.persisted_runs.append(run)

    warehouse = FailingCleanedWarehouse()
    config = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        persistence_write_mode="run_batched",
    )
    pipe = FredPipeline(
        config,
        client=fake_client_cls({"A": observations_payload, "B": observations_payload}),
        warehouse=warehouse,
    )

    with pytest.raises(RuntimeError, match="cleaned storage is unavailable"):
        pipe.run([_spec("A"), _spec("B")], build_gold_layer=False)

    assert len(warehouse.bronze_batches) == 1
    assert len(warehouse.persisted_runs) == 1
    failed_run = warehouse.persisted_runs[0]
    assert failed_run.status == RunStatus.FAILED
    assert all(sr.status == RunStatus.FAILED for sr in failed_run.series_runs)
    assert all(sr.rows_written_bronze == 1 for sr in failed_run.series_runs)
    assert all(sr.rows_merged_silver == 0 for sr in failed_run.series_runs)
    assert all(
        "Batch cleaned persistence failed" in sr.error_message
        for sr in failed_run.series_runs
    )


def test_vintage_series_requests_full_realtime_window(observations_payload):
    captured = {}

    class RecordingClient:
        def get_observations(self, series_id, **kwargs):
            captured.update(kwargs)
            return observations_payload

    pipe = FredPipeline(
        _config(), client=RecordingClient(), spark=None, persist_audit=False
    )
    pipe.run([_spec("GDP", vintage_enabled=True)])
    assert captured.get("realtime_start") == "1776-07-04"
    assert captured.get("realtime_end") == "9999-12-31"


def test_run_from_manifest_series_filter(observations_payload, fake_client_cls):
    client = fake_client_cls(
        {sid: observations_payload for sid in ("DGS10", "UNRATE", "GDP")}
    )
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run_from_manifest(
        "manifests",
        series=["DGS10", "GDP"],
        build_gold_layer=False,
    )
    assert {s.series_id for s in run.series_runs} == {"DGS10", "GDP"}
    assert set(client.requested) == {"DGS10", "GDP"}


def test_run_source_filters(observations_payload, fake_client_cls):
    fred_client = fake_client_cls({"DGS10": observations_payload})
    stooq_client = fake_client_cls({"SPY:close": observations_payload})
    tiingo_client = fake_client_cls({"SPY:adjClose": observations_payload})
    pipe = FredPipeline(
        _config(),
        clients={
            "fred": fred_client,
            "stooq": stooq_client,
            "tiingo": tiingo_client,
        },
        spark=None,
        persist_audit=False,
    )
    specs = [
        _spec("DGS10", source="fred"),
        _spec("SPY:close", source="stooq", vintage_enabled=False),
        _spec("SPY:adjClose", source="tiingo", vintage_enabled=False),
    ]

    run = pipe.run(specs, build_gold_layer=False)

    assert run.series_succeeded == 3
    assert fred_client.requested == ["DGS10"]
    assert stooq_client.requested == ["SPY:close"]
    assert tiingo_client.requested == ["SPY:adjClose"]


def test_run_from_manifest_source_include_exclude(
    tmp_path, observations_payload, fake_client_cls
):
    manifest = tmp_path / "sources.yml"
    manifest.write_text(
        "name: sources\n"
        "series:\n"
        "  - {series_id: DGS10, title: DGS10, category: rates, frequency: d, active: true}\n"
        "  - {series_id: SPY:close, title: SPY, category: equity, frequency: d, source: stooq, active: true, vintage_enabled: false}\n"
        "  - {series_id: SPY:adjClose, title: SPY TR, category: equity, frequency: d, source: tiingo, active: true, vintage_enabled: false}\n"
    )
    fred_client = fake_client_cls({"DGS10": observations_payload})
    stooq_client = fake_client_cls({"SPY:close": observations_payload})
    tiingo_client = fake_client_cls({"SPY:adjClose": observations_payload})
    pipe = FredPipeline(
        _config(),
        clients={
            "fred": fred_client,
            "stooq": stooq_client,
            "tiingo": tiingo_client,
        },
        spark=None,
        persist_audit=False,
    )

    run = pipe.run_from_manifest(
        str(manifest),
        sources=["fred", "stooq"],
        exclude_sources=["stooq"],
        build_gold_layer=False,
    )

    assert [sr.series_id for sr in run.series_runs] == ["DGS10"]
    assert fred_client.requested == ["DGS10"]
    assert stooq_client.requested == []
    assert tiingo_client.requested == []


def test_source_worker_defaults_and_overrides():
    cfg = PipelineConfig(
        environment=Environment.DEV, fred_api_key="k", extract_workers=16
    )
    assert _extract_workers_for_source(cfg, "fred") == 16
    assert _extract_workers_for_source(cfg, "tiingo") == 2
    assert _extract_workers_for_source(cfg, "stooq") == 2

    cfg = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        extract_workers=16,
        source_extract_workers="fred=12,tiingo=1,*=3",
    )
    assert _extract_workers_for_source(cfg, "fred") == 12
    assert _extract_workers_for_source(cfg, "tiingo") == 1
    assert _extract_workers_for_source(cfg, "worldbank") == 3


def test_source_rate_defaults_and_overrides():
    cfg = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        rate_limit_per_minute=90,
    )
    assert _rate_limit_for_source(cfg, "fred") == 90
    assert _rate_limit_for_source(cfg, "stooq") == 20
    assert _rate_limit_for_source(cfg, "tiingo") == 10
    assert _rate_limit_for_source(cfg, "worldbank") == 60

    cfg = PipelineConfig(
        environment=Environment.DEV,
        fred_api_key="k",
        rate_limit_per_minute=90,
        source_rate_limits="fred=60,stooq=15,tiingo=5,*=30",
    )
    assert _rate_limit_for_source(cfg, "fred") == 60
    assert _rate_limit_for_source(cfg, "stooq") == 15
    assert _rate_limit_for_source(cfg, "tiingo") == 5
    assert _rate_limit_for_source(cfg, "worldbank") == 30


def test_force_full_ignores_watermark(observations_payload):
    import os
    import tempfile

    from fred_pipeline.local_store import LocalWarehouse

    captured = []

    class RecordingClient:
        def get_observations(self, series_id, **kwargs):
            captured.append(kwargs)
            return observations_payload

    with tempfile.TemporaryDirectory() as d:
        cfg = PipelineConfig(
            environment=Environment.DEV, fred_api_key="k", restate_last_n=2
        )
        wh = LocalWarehouse(cfg, db_path=os.path.join(d, "f.db"))
        pipe = FredPipeline(cfg, client=RecordingClient(), warehouse=wh)
        pipe.run([_spec("DGS10")])  # first load (full)
        pipe.run([_spec("DGS10")], force_full=True)  # force full despite data
        assert all("observation_start" not in c for c in captured)
        wh.close()


def test_missing_source_keys():
    from fred_pipeline.pipeline import missing_source_keys

    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")  # no eia key
    # fred satisfied, bls keyless (never required), eia/stooq missing
    assert missing_source_keys(cfg, ["fred", "bls"]) == {}
    assert missing_source_keys(cfg, ["fred", "bls", "eia", "stooq"]) == {
        "eia": "eia_api_key",
        "stooq": "stooq_api_key",
    }

    cfg2 = PipelineConfig(
        environment=Environment.DEV, fred_api_key="", eia_api_key="e", stooq_api_key="s"
    )
    # fred required but empty; eia/stooq satisfied
    assert missing_source_keys(cfg2, ["fred", "eia", "stooq"]) == {
        "fred": "fred_api_key"
    }


def test_gold_failure_logs_the_real_error_not_noneType_none(
    observations_payload, fake_client_cls, caplog
):
    """Regression: log.exception() outside the tracker's except block used to
    print "NoneType: None" (no live exception context left to report) instead
    of the actual Gold-build failure. The stage tracker already captures
    error_type/error_message before swallowing -- the log line must use those.
    """

    class FailingGoldWarehouse:
        def restate_start(self, series_id, n):
            return None

        def write_bronze(self, rows):
            return len(rows)

        def merge_silver(self, rows):
            return len(rows)

        def persist_dq(self, run_id, report):
            pass

        def persist_run_state(self, run):
            pass

        def persist_series_run(self, series_run):
            pass

        def build_gold(self):
            raise ValueError("table gold_dim_date has no column named is_imm_date")

        def write_release_calendar(self, rows):
            return len(rows)

        def persist_run(self, run):
            pass

    client = fake_client_cls({"DGS10": observations_payload})
    pipe = FredPipeline(
        _config(), client=client, warehouse=FailingGoldWarehouse(), persist_audit=False
    )
    with caplog.at_level("ERROR"):
        pipe.run([_spec("DGS10")])

    gold_failures = [r for r in caplog.records if "Gold refresh failed" in r.message]
    assert len(gold_failures) == 1
    assert "NoneType: None" not in gold_failures[0].message
    assert "ValueError" in gold_failures[0].message
    assert "is_imm_date" in gold_failures[0].message


def test_config_table_naming():
    cfg = PipelineConfig(environment=Environment.PROD, fred_api_key="k")
    assert cfg.catalog == "macro_prod"
    assert (
        cfg.table("silver", "fred_observation") == "macro_prod.silver.fred_observation"
    )


# -- _series_is_due (spec007 due-date gating) --------------------------------


def test_series_is_due_when_never_pulled():
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    assert _series_is_due("monthly", None, now) is True


def test_series_is_due_for_missing_or_unrecognized_frequency():
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    recent = (now - timedelta(minutes=1)).isoformat()
    assert _series_is_due("", recent, now) is True
    assert _series_is_due("semiannual", recent, now) is True
    assert _series_is_due("not-a-real-cadence", recent, now) is True


def test_series_is_due_boundary_is_inclusive():
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    last = (now - timedelta(hours=20)).isoformat()
    assert _series_is_due("daily", last, now) is True


def test_series_is_due_just_inside_and_outside_interval_per_cadence():
    # Iterates the real _CADENCE_MIN_INTERVAL table rather than a duplicated
    # literal copy, so this test can't silently drift from the validated
    # values (spec007 §5/§10 item 5) the next time they're tightened.
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    for cadence, interval in _CADENCE_MIN_INTERVAL.items():
        not_yet_due = (now - (interval - timedelta(seconds=1))).isoformat()
        already_due = (now - (interval + timedelta(seconds=1))).isoformat()
        assert _series_is_due(cadence, not_yet_due, now) is False, cadence
        assert _series_is_due(cadence, already_due, now) is True, cadence


def test_series_is_due_treats_naive_timestamp_as_utc():
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    naive_last = (now - timedelta(days=1)).replace(tzinfo=None).isoformat()
    assert _series_is_due("daily", naive_last, now) is True


def test_series_is_due_weekend_adjacent_daily_case():
    # A daily series pulled Friday morning must not be treated as "not due"
    # by the time Saturday or Monday's routine run comes around -- the ~20h
    # interval is deliberately shorter than any weekend gap so daily series
    # are never silently held back over a weekend.
    friday_pull = datetime(2026, 9, 11, 9, 0, tzinfo=timezone.utc)  # a Friday
    saturday_run = datetime(2026, 9, 12, 9, 0, tzinfo=timezone.utc)
    monday_run = datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc)
    assert _series_is_due("daily", friday_pull.isoformat(), saturday_run) is True
    assert _series_is_due("daily", friday_pull.isoformat(), monday_run) is True
    # But a same-day re-run within the interval is correctly not due yet.
    same_day_rerun = friday_pull + timedelta(hours=2)
    assert _series_is_due("daily", friday_pull.isoformat(), same_day_rerun) is False


def _fresh_gate_payload(payload):
    import copy
    from datetime import date

    payload = copy.deepcopy(payload)
    delta = date.today() - max(
        date.fromisoformat(o["date"]) for o in payload["observations"]
    )
    for obs in payload["observations"]:
        obs["date"] = (date.fromisoformat(obs["date"]) + delta).isoformat()
        obs["realtime_start"] = date.today().isoformat()
    return payload


def test_stale_period_overrides_recent_changed_row_cadence():
    now = datetime(2026, 10, 31, tzinfo=timezone.utc)
    recent = (now - timedelta(hours=1)).isoformat()
    assert _series_is_due(
        "monthly", recent, now, latest_period="2026-08-01", observation_frequency="m"
    )
    assert not _series_is_due(
        "monthly", recent, now, latest_period="2026-09-01", observation_frequency="m"
    )


# -- Pipeline.run(skip_not_due=...) wiring (spec007) -------------------------


def test_skip_not_due_skips_a_recently_pulled_series(
    tmp_path, observations_payload, fake_client_cls
):
    observations_payload = _fresh_gate_payload(observations_payload)
    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    client = fake_client_cls({"CPIAUCSL": observations_payload})
    pipe = FredPipeline(_config(), client=client, warehouse=wh, persist_audit=False)
    spec = _spec("CPIAUCSL", expected_update_frequency="monthly")

    first = pipe.run([spec])
    assert first.series_succeeded == 1
    assert first.series_skipped_not_due == 0
    assert client.requested == ["CPIAUCSL"]

    second = pipe.run([spec], skip_not_due=True)
    assert second.series_total == 1
    assert second.series_skipped_not_due == 1
    assert second.series_runs[0].status == RunStatus.SKIPPED_NOT_DUE
    # Never re-requested: gating kept it out of extraction entirely.
    assert client.requested == ["CPIAUCSL"]
    wh.close()


def test_skip_not_due_never_holds_back_a_series_that_has_never_been_pulled(
    tmp_path, observations_payload, fake_client_cls
):
    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    client = fake_client_cls({"CPIAUCSL": observations_payload})
    pipe = FredPipeline(_config(), client=client, warehouse=wh, persist_audit=False)
    spec = _spec("CPIAUCSL", expected_update_frequency="monthly")

    run = pipe.run([spec], skip_not_due=True)
    assert run.series_skipped_not_due == 0
    assert run.series_succeeded == 1
    assert client.requested == ["CPIAUCSL"]
    wh.close()


def test_skip_not_due_leaves_a_failed_extraction_due_next_run(
    tmp_path, fake_client_cls
):
    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    client = fake_client_cls({}, errors={"CPIAUCSL": FredAPIError("boom")})
    pipe = FredPipeline(_config(), client=client, warehouse=wh, persist_audit=False)
    spec = _spec("CPIAUCSL", expected_update_frequency="monthly")

    first = pipe.run([spec])
    assert first.series_failed == 1

    second = pipe.run([spec], skip_not_due=True)
    assert second.series_skipped_not_due == 0
    assert second.series_failed == 1
    assert client.requested == ["CPIAUCSL", "CPIAUCSL"]
    wh.close()


def test_force_full_bypasses_skip_not_due(
    tmp_path, observations_payload, fake_client_cls
):
    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    client = fake_client_cls({"CPIAUCSL": observations_payload})
    pipe = FredPipeline(_config(), client=client, warehouse=wh, persist_audit=False)
    spec = _spec("CPIAUCSL", expected_update_frequency="monthly")

    pipe.run([spec])
    assert client.requested == ["CPIAUCSL"]

    run = pipe.run([spec], skip_not_due=True, force_full=True)
    assert run.series_skipped_not_due == 0
    assert client.requested == ["CPIAUCSL", "CPIAUCSL"]
    wh.close()


def test_skip_not_due_summary_counts_add_up(
    tmp_path, observations_payload, fake_client_cls
):
    observations_payload = _fresh_gate_payload(observations_payload)
    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    client = fake_client_cls(
        {"CPIAUCSL": observations_payload, "DGS10": observations_payload}
    )
    pipe = FredPipeline(_config(), client=client, warehouse=wh, persist_audit=False)
    due_now = _spec("CPIAUCSL", expected_update_frequency="monthly")

    pipe.run([due_now])
    client.requested.clear()

    not_yet_due = due_now
    never_pulled = _spec("DGS10", expected_update_frequency="daily")
    run = pipe.run([not_yet_due, never_pulled], skip_not_due=True)

    assert run.series_total == 2
    assert run.series_skipped_not_due == 1
    attempted = run.series_succeeded + run.series_failed
    assert attempted + run.series_skipped_not_due == run.series_total
    assert client.requested == ["DGS10"]
    wh.close()


def test_skip_not_due_does_nothing_when_warehouse_lacks_the_method(
    observations_payload, fake_client_cls
):
    client = fake_client_cls({"CPIAUCSL": observations_payload})
    pipe = FredPipeline(_config(), client=client, warehouse=None, persist_audit=False)
    spec = _spec("CPIAUCSL", expected_update_frequency="monthly")

    run = pipe.run([spec], skip_not_due=True)
    assert run.series_skipped_not_due == 0
    assert client.requested == ["CPIAUCSL"]


def test_skip_not_due_defaults_on_without_passing_the_flag(
    tmp_path, observations_payload, fake_client_cls
):
    """Gating is on by default as of 2026-09-14 (spec007's real timing
    benchmark justified flipping it) -- this guards against a silent
    regression back to opt-in-only, since every other test in this file
    passes skip_not_due explicitly and wouldn't catch that."""
    observations_payload = _fresh_gate_payload(observations_payload)
    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    client = fake_client_cls({"CPIAUCSL": observations_payload})
    pipe = FredPipeline(_config(), client=client, warehouse=wh, persist_audit=False)
    spec = _spec("CPIAUCSL", expected_update_frequency="monthly")

    pipe.run([spec])
    second = pipe.run([spec])
    assert second.series_skipped_not_due == 1
    assert second.series_runs[0].status == RunStatus.SKIPPED_NOT_DUE
    assert client.requested == ["CPIAUCSL"]
    wh.close()


def test_stale_series_is_polled_even_when_recently_ingested(
    tmp_path, observations_payload, fake_client_cls
):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "stale.db"))
    client = fake_client_cls({"CPIAUCSL": observations_payload})
    pipe = FredPipeline(_config(), client=client, warehouse=wh, persist_audit=False)
    spec = _spec("CPIAUCSL", expected_update_frequency="monthly", frequency="m")
    pipe.run([spec])
    second = pipe.run([spec], skip_not_due=True)
    assert second.series_skipped_not_due == 0
    assert client.requested == ["CPIAUCSL", "CPIAUCSL"]
    wh.close()
