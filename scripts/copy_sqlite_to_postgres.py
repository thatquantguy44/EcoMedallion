#!/usr/bin/env python
"""Copy a local SQLite medallion warehouse into Postgres.

This is intentionally dependency-light: it uses Python's stdlib ``sqlite3`` and
the ``psql`` client inside the local Postgres Docker container. Tables are
mapped from SQLite's flat names into real Postgres schemas:

    gold_fred_latest_observation -> gold.fred_latest_observation
    silver_fred_observation      -> silver.fred_observation

Rows are streamed through ``COPY``; no intermediate CSV files are written.
"""

from __future__ import annotations

import argparse
import csv
import io
import sqlite3
import subprocess
import sys
import time
from collections.abc import Iterable, Sequence
from typing import Any

MEDALLION_SCHEMAS = ("meta", "audit", "bronze", "silver", "gold")
POSTGRES_DEFAULT_DB = "macro_medallion"
POSTGRES_DEFAULT_CONTAINER = "fred-pipeline-postgres"
POSTGRES_DEFAULT_USER = "fred"
NULL_MARKER = r"\N"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stream-copy fred_local.db SQLite tables into Postgres."
    )
    parser.add_argument("--sqlite-db", default="fred_local.db")
    parser.add_argument("--postgres-db", default=POSTGRES_DEFAULT_DB)
    parser.add_argument("--container", default=POSTGRES_DEFAULT_CONTAINER)
    parser.add_argument("--user", default=POSTGRES_DEFAULT_USER)
    parser.add_argument(
        "--append",
        action="store_true",
        help="append into existing schemas instead of replacing them first",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="comma-separated SQLite table names to copy (default: all tables)",
    )
    parser.add_argument(
        "--skip-views",
        action="store_true",
        help="skip creating Postgres equivalents of local Gold views",
    )
    parser.add_argument(
        "--progress-rows",
        type=int,
        default=250_000,
        help="print progress every N rows per table (default: 250000)",
    )
    args = parser.parse_args()

    sqlite_conn = sqlite3.connect(args.sqlite_db)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_conn.execute("PRAGMA query_only = ON")

    tables = _selected_tables(sqlite_conn, args.tables)
    if not tables:
        print("No tables selected.", file=sys.stderr)
        return 2

    _ensure_database(args.container, args.user, args.postgres_db)
    _prepare_schemas(
        args.container,
        args.user,
        args.postgres_db,
        schemas=sorted({_split_table_name(t)[0] for t in tables}),
        replace=not args.append,
    )

    started = time.time()
    copied: dict[str, int] = {}
    for table in tables:
        rows = _copy_table(
            sqlite_conn,
            table,
            container=args.container,
            user=args.user,
            database=args.postgres_db,
            replace_table=not args.append,
            progress_rows=args.progress_rows,
        )
        copied[table] = rows

    _create_indexes(args.container, args.user, args.postgres_db, copied_tables=tables)
    if not args.skip_views:
        _create_views(args.container, args.user, args.postgres_db)

    elapsed = time.time() - started
    print(
        f"Copied {sum(copied.values()):,} rows across {len(copied)} tables "
        f"into Postgres database {args.postgres_db!r} in {elapsed:,.1f}s."
    )
    return 0


def _selected_tables(conn: sqlite3.Connection, raw: str | None) -> list[str]:
    if raw:
        return [name.strip() for name in raw.split(",") if name.strip()]
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        ORDER BY
            CASE
                WHEN name LIKE 'meta_%' THEN 1
                WHEN name LIKE 'audit_%' THEN 2
                WHEN name LIKE 'bronze_%' THEN 3
                WHEN name LIKE 'silver_%' THEN 4
                WHEN name LIKE 'gold_%' THEN 5
                ELSE 6
            END,
            name
        """
    )
    return [row["name"] for row in rows]


def _ensure_database(container: str, user: str, database: str) -> None:
    exists = _psql_capture(
        container,
        user,
        "postgres",
        f"SELECT 1 FROM pg_database WHERE datname = '{_sql_literal(database)}'",
    ).strip()
    if exists == "1":
        return
    _psql_exec(container, user, "postgres", f"CREATE DATABASE {_pg_ident(database)}")


def _prepare_schemas(
    container: str,
    user: str,
    database: str,
    *,
    schemas: Iterable[str],
    replace: bool,
) -> None:
    statements: list[str] = []
    for schema in schemas:
        if replace:
            statements.append(f"DROP SCHEMA IF EXISTS {_pg_ident(schema)} CASCADE")
        statements.append(f"CREATE SCHEMA IF NOT EXISTS {_pg_ident(schema)}")
    _psql_exec(container, user, database, ";\n".join(statements))


def _copy_table(
    conn: sqlite3.Connection,
    sqlite_table: str,
    *,
    container: str,
    user: str,
    database: str,
    replace_table: bool,
    progress_rows: int,
) -> int:
    schema, pg_table = _split_table_name(sqlite_table)
    columns = _sqlite_columns(conn, sqlite_table)
    if not columns:
        raise RuntimeError(f"SQLite table {sqlite_table!r} has no columns")

    _create_table(
        container,
        user,
        database,
        schema,
        pg_table,
        columns,
        replace=replace_table,
    )
    if not replace_table:
        _psql_exec(
            container,
            user,
            database,
            f"TRUNCATE TABLE {_pg_name(schema, pg_table)}",
        )

    copy_sql = (
        rf"\copy {_pg_name(schema, pg_table)} "
        f"({_pg_column_list([c.name for c in columns])}) "
        rf"FROM STDIN WITH (FORMAT csv, NULL '{NULL_MARKER}')"
    )
    cmd = [
        "docker",
        "exec",
        "-i",
        container,
        "psql",
        "-U",
        user,
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        copy_sql,
    ]

    print(f"Copying {sqlite_table} -> {schema}.{pg_table} ...", flush=True)
    cur = conn.execute(
        f"SELECT {_sqlite_column_list([c.name for c in columns])} "
        f"FROM {_sqlite_ident(sqlite_table)}"
    )
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdin is not None
    stdin = io.TextIOWrapper(proc.stdin, encoding="utf-8", newline="")
    writer = csv.writer(stdin, lineterminator="\n")
    count = 0
    try:
        for row in cur:
            writer.writerow([_copy_value(row[c.name]) for c in columns])
            count += 1
            if progress_rows and count % progress_rows == 0:
                print(f"  {sqlite_table}: {count:,} rows", flush=True)
    finally:
        stdin.close()

    returncode = proc.wait()
    stdout = proc.stdout.read() if proc.stdout is not None else b""
    stderr = proc.stderr.read() if proc.stderr is not None else b""
    if returncode != 0:
        raise RuntimeError(
            f"COPY failed for {sqlite_table}:\n"
            f"stdout={stdout.decode('utf-8', 'replace')}\n"
            f"stderr={stderr.decode('utf-8', 'replace')}"
        )
    print(f"Finished {sqlite_table}: {count:,} rows", flush=True)
    return count


def _create_table(
    container: str,
    user: str,
    database: str,
    schema: str,
    table: str,
    columns: Sequence[Column],
    *,
    replace: bool,
) -> None:
    statements = []
    if replace:
        statements.append(f"DROP TABLE IF EXISTS {_pg_name(schema, table)} CASCADE")
    column_sql = ",\n    ".join(
        f"{_pg_ident(col.name)} {_pg_type(col.sqlite_type)}" for col in columns
    )
    statements.append(
        f"CREATE TABLE IF NOT EXISTS {_pg_name(schema, table)} (\n    {column_sql}\n)"
    )
    _psql_exec(container, user, database, ";\n".join(statements))


def _create_indexes(
    container: str,
    user: str,
    database: str,
    *,
    copied_tables: Sequence[str],
) -> None:
    copied = set(copied_tables)
    statements = []
    if "silver_fred_observation" in copied:
        statements.extend(
            [
                """
                CREATE INDEX IF NOT EXISTS ix_silver_obs_sid_rt
                    ON silver.fred_observation(series_id, realtime_start DESC)
                """,
                """
                CREATE INDEX IF NOT EXISTS ix_silver_obs_sid_date
                    ON silver.fred_observation(series_id, observation_date)
                """,
            ]
        )
    if "gold_fred_latest_observation" in copied:
        statements.append(
            """
            CREATE INDEX IF NOT EXISTS ix_gold_latest_sid
                ON gold.fred_latest_observation(series_id)
            """
        )
    if "gold_macro_factor_scores" in copied:
        statements.append(
            """
            CREATE INDEX IF NOT EXISTS ix_factor_scores_date
                ON gold.macro_factor_scores(observation_date)
            """
        )
    if "gold_equity_factor_attribution" in copied:
        statements.append(
            """
            CREATE INDEX IF NOT EXISTS ix_equity_attr_ticker_window
                ON gold.equity_factor_attribution(ticker, "window")
            """
        )
    if statements:
        _psql_exec(container, user, database, ";\n".join(statements))


def _create_views(container: str, user: str, database: str) -> None:
    sql = """
    CREATE OR REPLACE VIEW gold.v_latest_revised AS
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY series_id, observation_date
            ORDER BY realtime_start DESC
        ) AS rn
        FROM silver.fred_observation
    )
    SELECT series_id, observation_date, value, realtime_start, realtime_end,
           is_missing, revision_number, ingested_at
    FROM ranked
    WHERE rn = 1;

    CREATE OR REPLACE VIEW gold.v_point_in_time AS
    SELECT series_id, observation_date, realtime_start, realtime_end, value,
           revision_number, is_missing, ingested_at
    FROM silver.fred_observation;

    CREATE OR REPLACE VIEW gold.v_series_latest_value AS
    WITH latest AS (
        SELECT series_id, observation_date, value,
            ROW_NUMBER() OVER (
                PARTITION BY series_id ORDER BY observation_date DESC
            ) AS rn
        FROM gold.v_latest_revised
        WHERE COALESCE(is_missing, 0) = 0
    )
    SELECT series_id, observation_date, value
    FROM latest
    WHERE rn = 1;

    CREATE OR REPLACE VIEW gold.v_series_revision_summary AS
    SELECT series_id,
        COUNT(*)               AS observation_count,
        AVG(revision_count)    AS avg_revision_count,
        MAX(revision_count)    AS max_revision_count,
        AVG(ABS(revision_pct)) AS avg_abs_revision_pct,
        MAX(ABS(revision_pct)) AS max_abs_revision_pct
    FROM gold.fred_revision_stats
    GROUP BY series_id;

    CREATE OR REPLACE VIEW gold.v_source_coverage AS
    WITH per_series AS (
        SELECT source, series_id,
               MAX(observation_date)            AS latest_observation_date,
               COUNT(DISTINCT observation_date) AS observation_count
        FROM silver.fred_observation
        GROUP BY source, series_id
    ),
    aged AS (
        SELECT p.source, p.series_id, m.category, m.frequency,
               p.latest_observation_date, p.observation_count,
               CURRENT_DATE - p.latest_observation_date::date AS days_since_last
        FROM per_series p
        LEFT JOIN meta.fred_series m ON m.series_id = p.series_id
    )
    SELECT source, series_id, category, frequency, latest_observation_date,
           observation_count, days_since_last,
           CASE
             WHEN frequency IN ('d','daily')       AND days_since_last > 10  THEN 1
             WHEN frequency IN ('w','weekly')      AND days_since_last > 21  THEN 1
             WHEN frequency IN ('bw','biweekly')   AND days_since_last > 30  THEN 1
             WHEN frequency IN ('m','monthly')     AND days_since_last > 75  THEN 1
             WHEN frequency IN ('q','quarterly')   AND days_since_last > 200 THEN 1
             WHEN frequency IN ('sa','semiannual') AND days_since_last > 380 THEN 1
             WHEN frequency IN ('a','annual')      AND days_since_last > 550 THEN 1
             ELSE 0
           END AS is_stale
    FROM aged;

    CREATE OR REPLACE VIEW gold.v_company_ratio_ranks AS
    SELECT cik, ratio_name, observation_date, value,
           PERCENT_RANK() OVER (
               PARTITION BY ratio_name, observation_date ORDER BY value
           ) AS pct_rank,
           ROW_NUMBER() OVER (
               PARTITION BY ratio_name, observation_date ORDER BY value DESC
           ) AS rank_desc
    FROM gold.fred_company_ratios;
    """
    _psql_exec(container, user, database, sql)


class Column(sqlite3.Row):
    name: str
    sqlite_type: str


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[Column]:
    rows = conn.execute(f"PRAGMA table_info({_sqlite_ident(table)})").fetchall()
    return [
        type(
            "ColumnValue",
            (),
            {"name": row["name"], "sqlite_type": row["type"] or "TEXT"},
        )()
        for row in rows
    ]


def _split_table_name(sqlite_table: str) -> tuple[str, str]:
    for schema in MEDALLION_SCHEMAS:
        prefix = f"{schema}_"
        if sqlite_table.startswith(prefix):
            return schema, sqlite_table[len(prefix) :]
    return "public", sqlite_table


def _copy_value(value: Any) -> Any:
    if value is None:
        return NULL_MARKER
    if isinstance(value, bytes):
        return "\\x" + value.hex()
    return value


def _pg_type(sqlite_type: str) -> str:
    normalized = sqlite_type.upper()
    if "INT" in normalized:
        return "BIGINT"
    if any(token in normalized for token in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE PRECISION"
    if "BLOB" in normalized:
        return "BYTEA"
    return "TEXT"


def _pg_name(schema: str, table: str) -> str:
    return f"{_pg_ident(schema)}.{_pg_ident(table)}"


def _pg_column_list(columns: Sequence[str]) -> str:
    return ", ".join(_pg_ident(col) for col in columns)


def _pg_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_column_list(columns: Sequence[str]) -> str:
    return ", ".join(_sqlite_ident(col) for col in columns)


def _sqlite_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _psql_exec(container: str, user: str, database: str, sql: str) -> None:
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container,
            "psql",
            "-U",
            user,
            "-d",
            database,
            "-v",
            "ON_ERROR_STOP=1",
            "-q",
            "-c",
            sql,
        ],
        check=True,
    )


def _psql_capture(container: str, user: str, database: str, sql: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container,
            "psql",
            "-U",
            user,
            "-d",
            database,
            "-v",
            "ON_ERROR_STOP=1",
            "-tAc",
            sql,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


if __name__ == "__main__":
    raise SystemExit(main())
