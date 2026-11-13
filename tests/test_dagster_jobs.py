"""Manual job contracts, exercised through Dagster rather than CLI subprocesses."""

from pathlib import Path

import pytest

dagster = pytest.importorskip("dagster")


JOB_NAMES = {
    "macro_pipeline_job",
    "ecb_eligible_assets_job",
    "ecb_eligible_assets_health_job",
    "warehouse_health_job",
    "gold_rebuild_job",
    "metadata_reconciliation_job",
    "fomc_refresh_job",
}


def test_definitions_expose_independent_jobs_and_stopped_schedules():
    from fred_pipeline.orchestration.definitions import defs

    dagster.Definitions.validate_loadable(defs)
    assert {job.name for job in defs.resolve_all_job_defs()} >= JOB_NAMES
    assert all(
        s.default_status == dagster.DefaultScheduleStatus.STOPPED
        for s in defs.schedules
    )


def test_job_configuration_is_flat_and_defaults_are_workload_specific():
    from fred_pipeline.orchestration.config import (
        MacroPipelineConfig,
        FOMCRefreshConfig,
    )
    from fred_pipeline.orchestration.definitions import macro_pipeline_job

    assert MacroPipelineConfig().refresh_fomc_calendar is True
    assert FOMCRefreshConfig().refresh is True
    observed = []

    class Runtime:
        def macro_pipeline(self, config):
            from fred_pipeline.job_execution import JobReport

            observed.append(config)
            return JobReport("success", {"series_total": 0})

    result = macro_pipeline_job.execute_in_process(
        run_config={"environment": "dev", "backend": "postgres", "series": ["DGS10"]},
        resources={"runtime": Runtime()},
    )
    assert result.success
    assert observed[0].series == ["DGS10"]
    assert observed[0].backend == "postgres"
    assert not result.get_asset_materialization_events()


@pytest.mark.parametrize(
    "config",
    [
        {"environment": "invalid"},
        {"extract_workers": 0},
        {"fred_api_key": "never-accept-secrets"},
        {"command": "arbitrary"},
    ],
)
def test_invalid_launch_config_never_executes(config):
    from fred_pipeline.orchestration.definitions import macro_pipeline_job

    with pytest.raises(
        (
            dagster.DagsterInvalidConfigError,
            dagster.DagsterConfigMappingFunctionError,
            ValueError,
        )
    ):
        macro_pipeline_job.execute_in_process(run_config=config)


def test_partial_report_fails_without_materializing():
    from fred_pipeline.job_execution import JobReport
    from fred_pipeline.orchestration.definitions import macro_pipeline_job

    class Runtime:
        def macro_pipeline(self, config):
            return JobReport("partial", {"stages": {"gold": "failed"}})

    result = macro_pipeline_job.execute_in_process(
        resources={"runtime": Runtime()},
        raise_on_error=False,
    )
    assert not result.success
    assert not result.get_asset_materialization_events()
    assert any(
        event.event_type == dagster.DagsterEventType.ASSET_OBSERVATION
        for event in result.all_events
    )


def test_all_launch_examples_validate_without_execution():
    import yaml
    from fred_pipeline.orchestration.definitions import defs

    paths = list(Path("resources/dagster/jobs").glob("*.yaml"))
    assert len(paths) >= len(JOB_NAMES)
    for path in paths:
        config = yaml.safe_load(path.read_text())
        name = path.stem.split(".")[0]
        dagster.validate_run_config(defs.get_job_def(name), config)


def test_definitions_import_does_not_resolve_settings_or_open_connections():
    import subprocess
    import sys

    code = """
import requests
from fred_pipeline.config import PipelineConfig
from fred_pipeline.io.warehouse_factory import WarehouseFactory
from fred_pipeline.io.local_store import LocalWarehouse
from fred_pipeline.io.postgres_store import PostgresWarehouse
import fred_pipeline.network as network
def forbidden(*args, **kwargs):
    raise AssertionError('definitions import attempted external work')
PipelineConfig.resolve = forbidden
WarehouseFactory.build = forbidden
WarehouseFactory.build_exact = forbidden
LocalWarehouse.__init__ = forbidden
PostgresWarehouse.__init__ = forbidden
network.select_route = forbidden
requests.Session.request = forbidden
from fred_pipeline.orchestration.definitions import defs
from dagster import Definitions
Definitions.validate_loadable(defs)
"""
    subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True
    )


def test_instance_example_enforces_single_writer_pools(tmp_path):
    import shutil
    from dagster import DagsterInstance

    shutil.copy("resources/dagster/dagster.yaml", tmp_path / "dagster.yaml")
    with DagsterInstance.from_config(str(tmp_path)) as instance:
        assert instance.get_concurrency_config().pool_config.default_pool_limit == 1


def test_ecb_schedules_preserve_wall_clock_across_dst():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from fred_pipeline.orchestration.definitions import ecb_schedules
    from dagster._utils.schedules import schedule_execution_time_iterator

    for schedule in ecb_schedules:
        hour = int(schedule.cron_schedule.split()[1])
        minute = int(schedule.cron_schedule.split()[0])
        for month in (1, 7):
            start = datetime(2026, month, 5, tzinfo=ZoneInfo("Europe/Berlin"))
            tick = next(
                schedule_execution_time_iterator(
                    start.timestamp(),
                    schedule.cron_schedule,
                    schedule.execution_timezone,
                )
            )
            assert (tick.hour, tick.minute) == (hour, minute)
            assert tick.weekday() < 5
