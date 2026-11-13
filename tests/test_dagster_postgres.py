"""Run only against TEST_ECB_POSTGRES_DSN pointing at a disposable database."""

import os
import uuid

import pytest

dagster = pytest.importorskip("dagster")
psycopg = pytest.importorskip("psycopg")


@pytest.fixture
def pg_runtime(tmp_path, monkeypatch):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.io.postgres_store import PostgresWarehouse
    from fred_pipeline.job_runtime import JobRuntime

    admin_dsn = os.environ.get("TEST_ECB_POSTGRES_DSN")
    if not admin_dsn:
        pytest.skip(
            "TEST_ECB_POSTGRES_DSN must select a disposable PostgreSQL database"
        )
    name = f"fred_dagster_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(
            psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(name))
        )
    dsn = psycopg.conninfo.make_conninfo(admin_dsn, dbname=name)
    try:
        config = PipelineConfig()
        PostgresWarehouse(config, dsn=dsn).close()
        settings = tmp_path / "warehouse.yml"
        settings.write_text(
            "default:\n  primary_backend: postgres\n  ecb_assets_authoritative_backend: postgres\n  backends:\n    postgres:\n      dsn_env: FRED_DAGSTER_TEST_DSN\n"
        )
        monkeypatch.setenv("FRED_DAGSTER_TEST_DSN", dsn)
        monkeypatch.setenv("FRED_WAREHOUSE_CONFIG", str(settings))
        monkeypatch.setenv("FRED_CONFIG_FILE", str(tmp_path / "absent.yml"))
        monkeypatch.delenv("FRED_ECB_ASSETS_AUTHORITATIVE_BACKEND", raising=False)
        yield JobRuntime(), config, dsn
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            connection.execute(
                psycopg.sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    psycopg.sql.Identifier(name)
                )
            )


def test_postgres_monitoring_is_read_only_including_constructor(pg_runtime):
    from fred_pipeline.orchestration.config import WarehouseHealthConfig

    runtime, _, _ = pg_runtime
    with runtime.warehouse(WarehouseHealthConfig(), read_only=True) as (
        _,
        warehouse,
        _,
    ):
        assert (
            warehouse.query("SHOW transaction_read_only")[0]["transaction_read_only"]
            == "on"
        )
        assert (
            warehouse.query("SHOW transaction_isolation")[0]["transaction_isolation"]
            == "repeatable read"
        )
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            warehouse.conn.execute(
                "CREATE TABLE public.monitor_must_not_write (value integer)"
            )


@pytest.mark.parametrize("action", ["poll", "ingest", "promotion", "initialize"])
def test_real_postgres_contention_covers_all_live_entry_points(pg_runtime, action):
    from fred_pipeline.ecb_writer_lock import ECBWriterBusy, ECBWriterLock
    from fred_pipeline.ecb_eligible_assets import ECBEligibleAssetsPipeline
    from fred_pipeline.ecb_archive_promotion import ECBHistoricalPromoter
    from fred_pipeline.io.postgres_store import PostgresWarehouse

    _, config, dsn = pg_runtime
    warehouse = PostgresWarehouse(config, initialize=False, dsn=dsn)
    try:
        with psycopg.connect(dsn) as owner:
            with ECBWriterLock(owner) as lock:
                assert lock.acquired
                with pytest.raises(ECBWriterBusy):
                    if action == "initialize":
                        PostgresWarehouse(config, dsn=dsn)
                    elif action == "promotion":
                        ECBHistoricalPromoter(warehouse).promote(
                            "unused", acknowledge_quarantines=0
                        )
                    else:
                        # Any attempt to access this client would fail the test.
                        pipeline = ECBEligibleAssetsPipeline(warehouse, client=object())
                        if action == "poll":
                            pipeline.poll()
                        else:
                            pipeline.ingest_release("unused")
        # Ownership is released, and the same contender can acquire it next.
        with ECBWriterLock(warehouse._pg_conn) as successor:
            assert successor.acquired
    finally:
        warehouse.close()


def test_dagster_busy_poll_observes_without_contacting_source(pg_runtime, monkeypatch):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock
    from fred_pipeline.orchestration.definitions import ecb_eligible_assets_job

    runtime, _, dsn = pg_runtime
    monkeypatch.setattr(
        runtime,
        "http_session",
        lambda *_a, **_k: pytest.fail("busy writer must not probe or poll a source"),
    )
    with psycopg.connect(dsn) as owner:
        with ECBWriterLock(owner):
            result = ecb_eligible_assets_job.execute_in_process(
                resources={"runtime": runtime}
            )
    assert result.success
    assert result.output_for_node("ecb_eligible_assets")["outcome"] == "busy"
    assert not result.get_asset_materialization_events()


def test_nested_service_ownership_keeps_lock_across_commits(pg_runtime):
    from fred_pipeline.ecb_writer_lock import ECBWriterLock, ecb_writer_scope
    from fred_pipeline.orchestration.config import ECBEligibleAssetsConfig

    runtime, _, dsn = pg_runtime
    with runtime.warehouse(ECBEligibleAssetsConfig(), ecb=True) as (_, warehouse, _):
        with ecb_writer_scope(warehouse):
            warehouse.conn.commit()
        with psycopg.connect(dsn) as contender:
            with ECBWriterLock(contender) as lock:
                assert not lock.acquired
    with psycopg.connect(dsn) as successor:
        with ECBWriterLock(successor) as lock:
            assert lock.acquired


def test_main_and_gold_jobs_persist_to_postgres(
    pg_runtime,
    tmp_path,
    monkeypatch,
    observations_payload,
    fake_client_cls,
):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.orchestration.definitions import (
        macro_pipeline_job,
        gold_rebuild_job,
    )
    import fred_pipeline.pipeline as pipeline_module

    runtime, _, dsn = pg_runtime
    manifest = tmp_path / "manifest.yml"
    manifest.write_text(
        "name: dagster\nseries:\n  - series_id: DGS10\n    title: Ten year yield\n    frequency: d\n    units: Percent\n"
    )
    monkeypatch.setenv("FRED_DAGSTER_MANIFESTS", str(manifest))
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path / "performance"))
    monkeypatch.setattr(
        runtime,
        "pipeline_config",
        lambda *_a, **_k: PipelineConfig(
            fred_api_key="fixture", rate_limit_per_minute=0
        ),
    )
    monkeypatch.setitem(
        pipeline_module.SOURCE_FACTORIES,
        "fred",
        lambda _: fake_client_cls({"DGS10": observations_payload}),
    )
    main = macro_pipeline_job.execute_in_process(
        run_config={"backend": "postgres", "build_gold": False},
        resources={"runtime": runtime},
    )
    assert main.success
    assert not main.get_asset_materialization_events()
    monkeypatch.setitem(
        pipeline_module.SOURCE_FACTORIES,
        "fred",
        lambda _: pytest.fail("Gold must not extract sources"),
    )
    gold = gold_rebuild_job.execute_in_process(
        run_config={"backend": "postgres"},
        resources={"runtime": runtime},
    )
    assert gold.success
    assert len(gold.get_asset_materialization_events()) == 1
    with psycopg.connect(dsn) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM gold.fred_latest_observation WHERE series_id = 'DGS10'"
            ).fetchone()[0]
            > 0
        )


def test_ecb_poll_preserves_committed_events_even_when_freshness_fails(
    pg_runtime, monkeypatch
):
    from types import SimpleNamespace
    from fred_pipeline.ecb_eligible_assets import ECBEligibleAssetsPipeline
    from fred_pipeline.orchestration.definitions import ecb_eligible_assets_job
    import fred_pipeline.ecb_execution as execution
    import fred_pipeline.governance.ecb_alerting as alerting
    from tests.test_ecb_eligible_assets import _fixture_client, _verified

    runtime, _, _ = pg_runtime
    client, url, _ = _fixture_client()
    client.rss = (
        f"<rss><channel><item><title>List of eligible marketable assets</title><link>{url}</link>"
        "<pubDate>Mon, 01 Jan 2024 17:56:07 +0100</pubDate></item></channel></rss>"
    ).encode()
    execute = execution.execute_ecb_poll
    monkeypatch.setattr(
        execution,
        "execute_ecb_poll",
        lambda config, **kwargs: execute(
            config,
            **kwargs,
            pipeline_factory=lambda warehouse, _: ECBEligibleAssetsPipeline(
                warehouse, client=client, signature_verifier=_verified
            ),
        ),
    )
    monkeypatch.setattr(
        alerting,
        "send_ecb_assets_alert_result",
        lambda **_: SimpleNamespace(status="disabled"),
    )
    result = ecb_eligible_assets_job.execute_in_process(
        resources={"runtime": runtime}, raise_on_error=False
    )
    assert not result.success  # Recorded 2024 releases are stale today.
    assert len(result.get_asset_materialization_events()) == 1
    second = ecb_eligible_assets_job.execute_in_process(
        resources={"runtime": runtime}, raise_on_error=False
    )
    assert not second.success
    assert not second.get_asset_materialization_events()

    # The independent health job reads that same accepted frontier. Freeze only
    # its expected publication date to match the recorded source fixture.
    from datetime import date
    import fred_pipeline.ecb_health as health
    from fred_pipeline.orchestration.definitions import ecb_eligible_assets_health_job

    monkeypatch.setattr(health, "expected_release_date", lambda *_: date(2024, 1, 1))
    checked = ecb_eligible_assets_health_job.execute_in_process(
        resources={"runtime": runtime}
    )
    assert checked.success
    assert not checked.get_asset_materialization_events()
    assert set(
        checked.output_for_node("ecb_eligible_assets_health")["checks"].values()
    ) == {"pass"}


@pytest.mark.parametrize(
    "job_name", ["warehouse_health_job", "ecb_eligible_assets_health_job"]
)
def test_health_jobs_fail_for_missing_evidence_without_source_access(
    pg_runtime,
    tmp_path,
    monkeypatch,
    job_name,
):
    from fred_pipeline.orchestration.definitions import JOBS

    runtime, _, dsn = pg_runtime
    manifest = tmp_path / "manifest.yml"
    manifest.write_text(
        "name: health\nseries:\n  - series_id: HEALTH_TEST\n    title: Health test\n    frequency: d\n    units: Index\n"
    )
    monkeypatch.setenv("FRED_DAGSTER_MANIFESTS", str(manifest))
    monkeypatch.setattr(
        runtime,
        "http_session",
        lambda *_a, **_k: pytest.fail("health jobs must not fetch sources"),
    )
    result = JOBS[job_name].execute_in_process(
        resources={"runtime": runtime}, raise_on_error=False
    )
    assert not result.success
    assert not result.get_asset_materialization_events()
    assert any(
        event.event_type == dagster.DagsterEventType.ASSET_OBSERVATION
        for event in result.all_events
    )
    with psycopg.connect(dsn) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM audit.query_log").fetchone()[0]
            == 0
        )


def test_warehouse_health_job_passes_with_real_postgres_evidence_and_no_api_key(
    pg_runtime,
    tmp_path,
    monkeypatch,
):
    from datetime import datetime, timezone
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.orchestration.definitions import warehouse_health_job

    runtime, _, dsn = pg_runtime
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "INSERT INTO silver.fred_observation (series_id, observation_date, realtime_start, realtime_end, value, is_missing, ingested_at) VALUES (%s, %s, '2026-01-01', '9999-12-31', 1.0, 0, %s)",
            ("HEALTH_TEST", today, now.isoformat()),
        )
        connection.execute(
            "INSERT INTO gold.fred_latest_observation (series_id, observation_date, value) VALUES (%s, %s, 1.0)",
            ("HEALTH_TEST", today),
        )
        connection.execute(
            "INSERT INTO audit.etl_run (run_id, status, started_at, ended_at) VALUES ('health-test', 'succeeded', %s, %s)",
            (now.isoformat(), now.isoformat()),
        )
        connection.execute(
            "INSERT INTO audit.data_quality_result (run_id, series_id, check_name, passed, severity) VALUES ('health-test', 'HEALTH_TEST', 'complete', 1, 'error')"
        )
    manifest = tmp_path / "manifest.yml"
    manifest.write_text(
        "name: health\nseries:\n  - series_id: HEALTH_TEST\n    title: Health test\n    frequency: d\n    units: Index\n"
    )
    monkeypatch.setenv("FRED_DAGSTER_MANIFESTS", str(manifest))
    monkeypatch.setattr(
        runtime, "pipeline_config", lambda *_a, **_k: PipelineConfig(fred_api_key="")
    )
    monkeypatch.setattr(
        runtime,
        "http_session",
        lambda *_a, **_k: pytest.fail("health jobs must not fetch sources"),
    )
    result = warehouse_health_job.execute_in_process(resources={"runtime": runtime})
    assert result.success
    details = result.output_for_node("warehouse_health")
    assert details["latest_run_id"] == "health-test"
    assert details["dq_checks"] == 1
    assert (
        details["dq_errors"]
        == details["stale_series"]
        == details["missing_series"]
        == 0
    )
    assert not result.get_asset_materialization_events()
    missing_relation = warehouse_health_job.execute_in_process(
        run_config={
            "required_gold_relations": [
                "gold_missing_health_fixture",
                "gold_fred_latest_observation",
            ]
        },
        resources={"runtime": runtime},
        raise_on_error=False,
    )
    assert not missing_relation.success
    observations = [
        event.event_specific_data.asset_observation
        for event in missing_relation.all_events
        if event.event_type == dagster.DagsterEventType.ASSET_OBSERVATION
    ]
    assert observations[0].metadata["report"].value["gold_row_evidence"] == {
        "gold_missing_health_fixture": None,
        "gold_fred_latest_observation": 1,
    }
    with psycopg.connect(dsn) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM audit.query_log").fetchone()[0]
            == 0
        )
