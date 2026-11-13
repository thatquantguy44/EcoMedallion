"""Workloads use real isolated warehouse files and recorded source responses."""

import json
from pathlib import Path

import pytest

pytest.importorskip("dagster")


@pytest.fixture
def local_runtime(tmp_path, monkeypatch):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.io.local_store import LocalWarehouse
    from fred_pipeline.job_runtime import JobRuntime

    path = tmp_path / "warehouse.db"
    warehouse = LocalWarehouse(PipelineConfig(), str(path))
    warehouse.close()
    settings = tmp_path / "warehouse.yml"
    settings.write_text(
        f"default:\n  primary_backend: local\n  backends:\n    local:\n      db_path: {path}\n"
    )
    monkeypatch.setenv("FRED_WAREHOUSE_CONFIG", str(settings))
    monkeypatch.setenv("FRED_CONFIG_FILE", str(tmp_path / "absent.yml"))
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path / "performance"))
    return JobRuntime(), path


def test_read_only_open_does_not_initialize_or_write(local_runtime):
    runtime, path = local_runtime
    from fred_pipeline.orchestration.config import WarehouseHealthConfig

    before = path.read_bytes()
    with runtime.warehouse(WarehouseHealthConfig(), read_only=True) as (
        _,
        warehouse,
        _,
    ):
        assert warehouse.query("SELECT COUNT(*) AS n FROM audit_etl_run")[0]["n"] == 0
        with pytest.raises(Exception, match="readonly"):
            warehouse.conn.execute("DELETE FROM audit_etl_run")
    assert path.read_bytes() == before


def test_missing_warehouse_is_not_created_by_monitoring(tmp_path, monkeypatch):
    from fred_pipeline.job_runtime import JobRuntime
    from fred_pipeline.orchestration.config import WarehouseHealthConfig

    settings = tmp_path / "warehouse.yml"
    missing = tmp_path / "missing.db"
    settings.write_text(
        f"default:\n  primary_backend: local\n  backends:\n    local:\n      db_path: {missing}\n"
    )
    monkeypatch.setenv("FRED_WAREHOUSE_CONFIG", str(settings))
    with pytest.raises(Exception):
        JobRuntime().warehouse_health(WarehouseHealthConfig())
    assert not missing.exists()


def test_warehouse_health_requires_persisted_evidence(
    local_runtime, tmp_path, monkeypatch
):
    from fred_pipeline.orchestration.config import WarehouseHealthConfig

    runtime, _ = local_runtime
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    (manifests / "test.yml").write_text(
        "name: test\nseries:\n  - series_id: TEST\n    title: Test series\n    frequency: d\n    units: Index\n"
    )
    monkeypatch.setenv("FRED_DAGSTER_MANIFESTS", str(manifests))
    report = runtime.warehouse_health(WarehouseHealthConfig())
    assert report.verdict == "failure"
    assert report.details["latest_run_id"] is None


def test_standalone_fomc_refresh_uses_additive_shared_service(tmp_path, monkeypatch):
    import functools
    import re
    from datetime import date
    from fred_pipeline.job_runtime import JobRuntime
    from fred_pipeline.orchestration.definitions import fomc_refresh_job
    import fred_pipeline.catalogs.fomc_calendar as calendar
    import fred_pipeline.governance.fomc_calendar_check as check

    config = tmp_path / "fomc.yml"
    fixture = Path("tests/fixtures/fomccalendars.html").read_text()
    meetings = calendar.parse_fomc_calendar(fixture, warn=False)
    dates = {meeting.decision_date.isoformat() for meeting in meetings[:-1]}
    original = Path("config/fomc.yml").read_text()
    lines = []
    for line in original.splitlines(keepends=True):
        match = re.fullmatch(r'  - "(\d{4}-\d{2}-\d{2})"\s*', line)
        if not match or match[1] in dates:
            lines.append(line)
    config.write_text("".join(lines))
    monkeypatch.setenv("FRED_FOMC_CONFIG_FILE", str(config))
    monkeypatch.setattr(
        check,
        "check_fomc_calendar",
        functools.partial(check.check_fomc_calendar, today=date(2026, 10, 7)),
    )
    monkeypatch.setattr(calendar, "fetch_calendar_html_requests", lambda **_: fixture)
    result = fomc_refresh_job.execute_in_process(resources={"runtime": JobRuntime()})
    assert result.success
    details = result.output_for_node("fomc_refresh")
    assert details["refresh_requested"] is True
    assert details["refresh_attempted"] is True
    assert details["refresh_outcome"] == "updated"
    assert len(result.get_asset_materialization_events()) == 1
    assert str(config) not in json.dumps(details)


def test_pipeline_report_fails_for_non_ok_gold_result():
    from types import SimpleNamespace
    from fred_pipeline.job_execution import pipeline_report
    from fred_pipeline.governance.stages import RunStageTracker

    tracker = RunStageTracker()
    for name in (
        "plan",
        "extract",
        "fomc_calendar",
        "gold",
        "release_calendar",
        "persist",
    ):
        with tracker.stage(name) as stage:
            if name == "gold":
                stage.detail["tables_not_ok"] = "fred_latest_observation"
    pipeline = SimpleNamespace(_stage_tracker=tracker)
    run = SimpleNamespace(
        run_id="fixture", series_total=1, series_failed=0, series_succeeded=1
    )
    report = pipeline_report(pipeline, run, durable=True)
    assert report.verdict == "failure"
    assert not report.materializations


def test_macro_job_persists_real_data_without_gold_materialization(
    local_runtime,
    tmp_path,
    monkeypatch,
    observations_payload,
    fake_client_cls,
):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.orchestration.definitions import macro_pipeline_job
    import fred_pipeline.pipeline as pipeline_module
    import sqlite3

    runtime, path = local_runtime
    manifests = tmp_path / "test.yml"
    manifests.write_text(
        "name: test\nseries:\n  - series_id: DGS10\n    title: Ten year yield\n    frequency: d\n    units: Percent\n"
    )
    monkeypatch.setenv("FRED_DAGSTER_MANIFESTS", str(manifests))
    monkeypatch.setattr(
        runtime,
        "pipeline_config",
        lambda *_a, **_k: PipelineConfig(
            fred_api_key="fixture", rate_limit_per_minute=0
        ),
    )
    client = fake_client_cls({"DGS10": observations_payload})
    monkeypatch.setitem(pipeline_module.SOURCE_FACTORIES, "fred", lambda _: client)
    result = macro_pipeline_job.execute_in_process(
        run_config={
            "backend": "local",
            "build_gold": False,
            "refresh_fomc_calendar": False,
        },
        resources={"runtime": runtime},
    )
    assert result.success
    assert not result.get_asset_materialization_events()
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM silver_fred_observation"
            ).fetchone()[0]
            > 0
        )
        assert (
            connection.execute("SELECT status FROM audit_etl_run").fetchone()[0]
            == "succeeded"
        )


def test_gold_job_treats_table_failure_as_failed_execution(local_runtime, monkeypatch):
    from fred_pipeline.io.local_store import LocalWarehouse
    from fred_pipeline.orchestration.definitions import gold_rebuild_job
    from fred_pipeline.governance.fomc_calendar_check import FOMCCalendarCheck
    import fred_pipeline.governance.fomc_calendar_check as calendar

    runtime, _ = local_runtime
    monkeypatch.setattr(
        calendar,
        "check_fomc_calendar",
        lambda **kwargs: FOMCCalendarCheck("ok", "private"),
    )
    monkeypatch.setattr(
        LocalWarehouse, "build_gold", lambda *_a, **_k: {"test_table": "failed"}
    )
    result = gold_rebuild_job.execute_in_process(
        run_config={"backend": "local"},
        resources={"runtime": runtime},
        raise_on_error=False,
    )
    assert not result.success
    assert not result.get_asset_materialization_events()


def test_metadata_job_reconciles_fred_and_reads_stored_data(
    local_runtime, monkeypatch, tmp_path
):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.fred_client import FredClient
    from fred_pipeline.orchestration.definitions import metadata_reconciliation_job

    runtime, path = local_runtime
    manifests = tmp_path / "test.yml"
    manifests.write_text(
        "name: test\nseries:\n  - series_id: DGS10\n    title: Ten year yield\n    frequency: d\n    units: Percent\n"
    )
    monkeypatch.setenv("FRED_DAGSTER_MANIFESTS", str(manifests))
    monkeypatch.setattr(
        runtime,
        "pipeline_config",
        lambda *_a, **_k: PipelineConfig(
            fred_api_key="fixture", rate_limit_per_minute=0
        ),
    )
    monkeypatch.setattr(
        FredClient,
        "get_series_metadata",
        lambda *_: {
            "title": "Ten year yield",
            "frequency_short": "D",
            "units": "Percent",
            "observation_end": "2026-11-13",
            "last_updated": "2026-11-13",
        },
    )
    before = path.read_bytes()
    result = metadata_reconciliation_job.execute_in_process(
        run_config={"backend": "local"},
        resources={"runtime": runtime},
    )
    assert result.success
    details = result.output_for_node("metadata_reconciliation")
    assert details["metadata_source"] == "fred"
    assert details["stored_missing_series"] == 1
    assert path.read_bytes() == before


def test_job_error_event_does_not_include_provider_secret():
    from fred_pipeline.orchestration.definitions import macro_pipeline_job

    class Runtime:
        def macro_pipeline(self, _config):
            raise RuntimeError("postgresql://private:VERY_SECRET@private/warehouse")

    result = macro_pipeline_job.execute_in_process(
        resources={"runtime": Runtime()}, raise_on_error=False
    )
    assert not result.success
    assert "VERY_SECRET" not in str(result.all_events)
