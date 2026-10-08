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


# ---------------------------------------------------------------------------
# Upsert keys. A database seeded by scripts/copy_sqlite_to_postgres.py has the
# tables but none of their primary keys, so the first ON CONFLICT upsert failed
# with "there is no unique or exclusion constraint matching the ON CONFLICT
# specification" -- and then every later call failed with a different, useless
# error because the aborted transaction was never rolled back.
# ---------------------------------------------------------------------------


def _drop_all_keys(dsn: str) -> None:
    """Put the database in the state the copy script leaves it in: every upsert
    target present, none of them with a primary key or unique constraint."""
    import psycopg

    from fred_pipeline.io.postgres_store import _UPSERT_KEYS, _split_table_name

    with psycopg.connect(dsn, autocommit=True) as conn:
        for flat in _UPSERT_KEYS:
            schema, table = _split_table_name(flat)
            names = [
                r[0]
                for r in conn.execute(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid = %s::regclass AND contype IN ('p', 'u')",
                    (f"{schema}.{table}",),
                )
            ]
            for name in names:
                conn.execute(f'ALTER TABLE {schema}."{table}" DROP CONSTRAINT "{name}"')
            for (idx,) in list(
                conn.execute(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE schemaname = %s AND tablename = %s "
                    "AND indexdef LIKE 'CREATE UNIQUE%%'",
                    (schema, table),
                )
            ):
                conn.execute(f'DROP INDEX {schema}."{idx}"')


def _unique_key_exists(dsn: str, flat_table: str) -> bool:
    import psycopg

    from fred_pipeline.io.postgres_store import (
        _UPSERT_KEYS,
        PostgresWarehouse as PW,
        _split_table_name,
    )

    schema, table = _split_table_name(flat_table)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        return PW._has_unique_index(cur, schema, table, _UPSERT_KEYS[flat_table])


def test_upsert_keys_match_the_primary_keys_the_ddl_declares():
    """_UPSERT_KEYS is hand-written; the DDL is what actually defines each key.
    If they drift, the startup repair would build a key the table was never
    meant to have."""
    import re

    from fred_pipeline.io.postgres_store import _UPSERT_KEYS, _sqlite_table_defs

    bodies = dict(_sqlite_table_defs())
    for flat, columns in _UPSERT_KEYS.items():
        body = bodies[flat]
        composite = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", body)
        if composite:
            declared = tuple(c.strip() for c in composite.group(1).split(","))
        else:
            inline = re.search(r"(\w+)\s+\w+\s+PRIMARY KEY", body)
            assert inline, f"{flat} declares no primary key"
            declared = (inline.group(1),)
        assert declared == columns, flat


def test_a_database_without_keys_gets_them_back_and_can_upsert(postgres_dsn):
    PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    _drop_all_keys(postgres_dsn)
    assert not _unique_key_exists(postgres_dsn, "meta_fred_series")
    assert not _unique_key_exists(postgres_dsn, "silver_fred_observation")

    wh = PostgresWarehouse(_config(), dsn=postgres_dsn)  # the repair runs here
    try:
        from fred_pipeline.io.postgres_store import _UPSERT_KEYS

        for flat in _UPSERT_KEYS:
            assert _unique_key_exists(postgres_dsn, flat), flat
        # And the thing that was actually failing now works, twice, idempotently.
        keys = _UPSERT_KEYS["meta_fred_series"]
        wh._insert(
            "meta_fred_series", [{"series_id": "X", "title": "a"}], upsert_keys=keys
        )
        wh._insert(
            "meta_fred_series", [{"series_id": "X", "title": "b"}], upsert_keys=keys
        )
        rows = wh.query("SELECT title FROM meta.fred_series WHERE series_id = 'X'")
        assert [r["title"] for r in rows] == ["b"]
    finally:
        wh.close()


def test_the_repair_warns_once_and_a_healthy_database_is_left_alone(
    postgres_dsn, caplog
):
    PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    _drop_all_keys(postgres_dsn)

    with caplog.at_level("WARNING", logger="fred_pipeline"):
        PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    assert "has no unique key on (series_id)" in caplog.text
    assert "copy_sqlite_to_postgres.py" in caplog.text

    caplog.clear()
    with caplog.at_level("WARNING", logger="fred_pipeline"):
        PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    assert "no unique key" not in caplog.text, "second start must not rebuild"


def test_duplicate_rows_stop_the_repair_with_a_clear_error_and_change_nothing(
    postgres_dsn,
):
    import psycopg

    from fred_pipeline.io.postgres_store import PostgresSchemaError

    PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    _drop_all_keys(postgres_dsn)
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO meta.fred_series (series_id, title) "
            "VALUES ('DUP', 'one'), ('DUP', 'two')"
        )

    with pytest.raises(PostgresSchemaError) as exc:
        PostgresWarehouse(_config(), dsn=postgres_dsn)

    message = str(exc.value)
    assert '"meta"."fred_series"' in message  # quoted: pasteable into psql
    assert "duplicate rows" in message
    assert "HAVING count(*) > 1" in message  # the query to find them is included
    assert "Nothing was changed" in message
    assert not _unique_key_exists(postgres_dsn, "meta_fred_series")
    with psycopg.connect(postgres_dsn) as conn:
        n = conn.execute(
            "SELECT count(*) FROM meta.fred_series WHERE series_id = 'DUP'"
        ).fetchone()[0]
    assert n == 2  # the user's data is untouched


def test_a_failed_statement_does_not_poison_the_connection(postgres_dsn):
    """Regression for the cascade: one error used to leave the transaction
    aborted, so the NEXT, unrelated call failed with 'current transaction is
    aborted' and the real cause was gone."""
    import psycopg

    wh = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        with pytest.raises(psycopg.errors.UndefinedColumn):
            wh._insert(
                "meta_fred_series",
                [{"series_id": "A", "not_a_column": 1}],
                upsert_keys=("series_id",),
            )
        # The very next call must work.
        wh._insert(
            "meta_fred_series",
            [{"series_id": "B", "title": "ok"}],
            upsert_keys=("series_id",),
        )
        assert wh.query("SELECT series_id FROM meta.fred_series") == [
            {"series_id": "B"}
        ]
    finally:
        wh.close()
