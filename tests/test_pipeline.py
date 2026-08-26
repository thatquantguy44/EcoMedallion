import threading

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.fred_client import FredAPIError
from fred_pipeline.manifest import SeriesSpec, ValidationProfile
from fred_pipeline.pipeline import (
    FredPipeline,
    _extract_workers_for_source,
    _make_tiingo,
    _normalize_tiingo_keys,
    _rate_limit_for_source,
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
