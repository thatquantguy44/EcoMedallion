import uuid

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.io.local_store import LocalWarehouse
from fred_pipeline.io.postgres_store import PostgresWarehouse
from fred_pipeline.io.warehouse_factory import WarehouseConfig, WarehouseFactory
from fred_pipeline.warehouse import Warehouse


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _silver_row(
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float | None,
    revision_number: int,
    *,
    is_missing: bool = False,
    realtime_end: str = "9999-12-31",
    source: str = "fred",
    ingested_at: str = "2024-01-01T00:00:00+00:00",
):
    raw_value = "." if is_missing else str(value)
    return {
        "source": source,
        "series_id": series_id,
        "observation_date": observation_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_end,
        "value": value,
        "raw_value": raw_value,
        "is_missing": is_missing,
        "row_hash": f"h-{source}-{series_id}-{observation_date}-{realtime_start}",
        "revision_number": revision_number,
        "ingested_at": ingested_at,
        "run_id": "r",
    }


@pytest.fixture
def postgres_dsn():
    psycopg = pytest.importorskip("psycopg")
    admin_dsn = "postgresql://fred:fred@localhost:55432/postgres"
    db_name = f"fred_pipeline_test_{uuid.uuid4().hex[:10]}"
    try:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{db_name}"')
    except Exception as exc:  # noqa: BLE001 - unavailable local service should skip.
        pytest.skip(f"local Postgres is not available: {exc}")

    dsn = f"postgresql://fred:fred@localhost:55432/{db_name}"
    try:
        yield dsn
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (db_name,),
            )
            conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


def test_postgres_warehouse_builds_core_gold_like_local(tmp_path, postgres_dsn):
    rows = [
        _silver_row(
            "PAYEMS",
            "2024-01-01",
            "2024-02-01",
            100.0,
            1,
            realtime_end="2024-02-29",
        ),
        _silver_row("PAYEMS", "2024-01-01", "2024-03-01", 101.5, 2),
        _silver_row(
            "PAYEMS",
            "2024-02-01",
            "2024-02-01",
            None,
            1,
            is_missing=True,
        ),
        _silver_row("DGS10", "2024-01-02", "", 4.25, 1, realtime_end=""),
    ]

    local = LocalWarehouse(_config(), db_path=str(tmp_path / "local.db"))
    pg = PostgresWarehouse(_config(), dsn=postgres_dsn)
    assert isinstance(pg, Warehouse)

    try:
        object_counts = pg.query(
            """
            SELECT table_schema, COUNT(*) AS n
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema IN ('meta', 'audit', 'bronze', 'silver', 'gold')
            GROUP BY table_schema
            """
        )
        assert {row["table_schema"]: row["n"] for row in object_counts} == {
            "audit": 4,
            "bronze": 1,
            # 57, not 56: includes gold_incremental_checkpoint (spec003 Phase 3
            # checkpoint infra), added on a branch that merged in after this
            # count was first written.
            "gold": 57,
            "meta": 6,
            "silver": 1,
        }
        view_counts = pg.query(
            """
            SELECT table_schema, COUNT(*) AS n
            FROM information_schema.views
            WHERE table_schema = 'gold'
            GROUP BY table_schema
            """
        )
        # 7, not 6: includes gold.fred_point_in_time, the flat-name-translated
        # mirror of LocalWarehouse's gold_fred_point_in_time view, alongside
        # the pre-existing gold.v_point_in_time BI-facing alias.
        assert view_counts == [{"table_schema": "gold", "n": 7}]

        local.merge_silver(rows)
        pg.merge_silver(rows)

        local_result = local.build_gold()
        pg_result = pg.build_gold()
        assert pg_result == local_result

        latest_sql = """
            SELECT series_id, observation_date, value, realtime_start,
                   realtime_end, is_missing, revision_number, ingested_at
            FROM gold_fred_latest_observation
            ORDER BY series_id, observation_date
        """
        assert pg.query(latest_sql) == local.query(latest_sql)

        pit_sql = """
            SELECT series_id, observation_date, realtime_start, realtime_end,
                   value, revision_number, is_missing, ingested_at
            FROM gold_fred_point_in_time
            ORDER BY series_id, observation_date, realtime_start
        """
        assert pg.query(pit_sql) == local.query(pit_sql)
    finally:
        pg.close()
        local.close()


def test_postgres_warehouse_is_idempotent(postgres_dsn):
    row = _silver_row("DGS10", "2024-01-01", "", 4.0, 1, realtime_end="")
    wh = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        wh.merge_silver([row])
        wh.merge_silver([{**row, "value": 4.1}])
        got = wh.query("SELECT COUNT(*) AS c FROM silver_fred_observation")
        assert got[0]["c"] == 1
        value = wh.query("SELECT value FROM silver_fred_observation")[0]["value"]
        assert value == 4.1
    finally:
        wh.close()


def test_postgres_last_ingested_at_by_series_matches_local(postgres_dsn, tmp_path):
    rows = [
        _silver_row(
            "DGS10", "2024-01-01", "", 4.1, 1, ingested_at="2024-01-02T00:00:00+00:00"
        ),
        _silver_row(
            "DGS10", "2024-01-02", "", 4.2, 1, ingested_at="2024-01-03T00:00:00+00:00"
        ),
        _silver_row(
            "DGS2", "2024-01-01", "", 4.5, 1, ingested_at="2024-01-01T00:00:00+00:00"
        ),
    ]
    local = LocalWarehouse(_config(), db_path=str(tmp_path / "local.db"))
    pg = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        local.merge_silver(rows)
        pg.merge_silver(rows)
        assert pg.last_ingested_at_by_series() == local.last_ingested_at_by_series()
        assert pg.last_ingested_at_by_series() == {
            "DGS10": "2024-01-03T00:00:00+00:00",
            "DGS2": "2024-01-01T00:00:00+00:00",
        }
    finally:
        pg.close()
        local.close()


def test_warehouse_factory_builds_postgres(postgres_dsn):
    factory = WarehouseFactory(
        _config(),
        WarehouseConfig(
            primary_backend="postgres",
            backends={"postgres": {"dsn": postgres_dsn}},
        ),
    )
    wh = factory.build()
    try:
        assert isinstance(wh, PostgresWarehouse)
    finally:
        wh.close()
