"""Postgres warehouse backend for the medallion pipeline.

This backend mirrors ``LocalWarehouse`` semantically but uses real Postgres
schemas (``meta``, ``audit``, ``bronze``, ``silver``, ``gold``) instead of
SQLite's flat ``gold_*`` table names.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # typing.Self doesn't exist until Python 3.11 (PEP 673), but this repo
    # supports >=3.10; TYPE_CHECKING-gated so it never actually executes at
    # runtime (annotations are lazily-stringified anyway, per the
    # __future__ import above) while still satisfying static type checkers
    # and ruff's PYI034 (__enter__ should return Self).
    from typing import Self

from fred_pipeline.audit import EtlRun, EtlSeriesRun
from fred_pipeline.config import PipelineConfig
from fred_pipeline.io.local_store import (
    _SCHEMA as _SQLITE_SCHEMA,
)
from fred_pipeline.io.local_store import (
    LocalWarehouse,
    _encode,
)
from fred_pipeline.io.postgres_config import resolve_postgres_settings
from fred_pipeline.manifest import Manifest
from fred_pipeline.meta import build_meta_rows
from fred_pipeline.quality import QualityReport
from fred_pipeline.warehouse import dq_rows

_MEDALLION_SCHEMAS = ("meta", "audit", "bronze", "silver", "gold")
_TABLE_RE = re.compile(
    r"CREATE TABLE IF NOT EXISTS\s+([A-Za-z0-9_]+)\s*\((.*?)\);",
    re.IGNORECASE | re.DOTALL,
)
_FLAT_TABLE_RE = re.compile(
    r"(?<![.\"])\b((?:meta|audit|bronze|silver|gold)_[A-Za-z0-9_]+)\b"
)
_NAMED_PARAM = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")


class _CursorResult:
    def __init__(self, cursor: Any):
        self._cursor = cursor
        self.rowcount = cursor.rowcount

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return list(self._cursor.fetchall())


class _PostgresConnAdapter:
    """Tiny compatibility shim for ``LocalWarehouse``'s SQL-heavy Gold flow."""

    def __init__(self, warehouse: PostgresWarehouse):
        self._warehouse = warehouse

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()):
        sql, params = self._warehouse._adapt_sql(sql, params)
        cur = self._warehouse._pg_conn.cursor()
        cur.execute(sql, params or None)
        return _CursorResult(cur)

    def executemany(
        self, sql: str, params_seq: Sequence[Sequence[Any] | Mapping[str, Any]]
    ) -> None:
        if not params_seq:
            return
        sql, _ = self._warehouse._adapt_sql(sql, params_seq[0])
        cur = self._warehouse._pg_conn.cursor()
        cur.executemany(sql, list(params_seq))

    def commit(self) -> None:
        self._warehouse._pg_conn.commit()

    def rollback(self) -> None:
        self._warehouse._pg_conn.rollback()

    def close(self) -> None:
        self._warehouse._pg_conn.close()


def _borrowed_from_local_warehouse(cls: type) -> type:
    """Bind LocalWarehouse's spec003 Phase 3 (incremental Gold) and spec007
    (due-date gating) methods onto this class, the same way
    ``_build_gold_inner`` below reuses LocalWarehouse's orchestration
    wholesale: these are pure Python over ``self.conn``/``self._insert``/
    ``self._load_checkpoints`` with no SQLite-specific API surface once
    ``_PostgresConnAdapter`` translates placeholders and flat table names.
    Without the spec003 methods, PostgresWarehouse.build_gold() raises
    AttributeError the moment ``_build_gold_inner`` reaches the checkpoint
    watermark/per-table incremental logic -- these were added to
    LocalWarehouse on a branch that merged in after PostgresWarehouse first
    shipped, and never got ported.
    """
    for name in (
        "_get_build_watermark",
        "_set_build_watermark",
        "_touched_series_since_watermark",
        "last_ingested_at_by_series",
        "_load_checkpoints",
        "_write_checkpoints_batch",
        "_clear_checkpoints",
        "_build_curve_spread_daily",
        "_build_credit_spread_daily",
        "_build_funding_tape_daily",
        "_build_funding_stress_daily",
        "_build_series_correlation",
    ):
        setattr(cls, name, getattr(LocalWarehouse, name))
    return cls


@_borrowed_from_local_warehouse
class PostgresWarehouse:
    """PostgreSQL implementation of the write-side Warehouse protocol."""

    supports_incremental_audit = True

    def __init__(self, config: PipelineConfig, **kwargs: Any):
        self.config = config
        self.settings = resolve_postgres_settings(kwargs)
        self._pg_conn = self._connect(self.settings.dsn)
        self.conn = _PostgresConnAdapter(self)
        self._defer_commits = False
        self._bootstrap_schema()

    def _connect(self, dsn: str) -> Any:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError(
                "psycopg required for Postgres warehouse backend. "
                "Install with: pip install -e '.[postgres]'"
            )

        return psycopg.connect(dsn, row_factory=dict_row)

    # ---- schema ---------------------------------------------------------

    def _bootstrap_schema(self) -> None:
        with self._pg_conn.cursor() as cur:
            for schema in _MEDALLION_SCHEMAS:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_pg_ident(schema)}")
            for table_name, body in _sqlite_table_defs():
                schema, table = _split_table_name(table_name)
                cur.execute(_postgres_create_table_sql(schema, table, body))
            for flat_table, column, coltype in self._ADDED_COLUMNS:
                schema, table = _split_table_name(flat_table)
                cur.execute(
                    f"ALTER TABLE {_pg_name(schema, table)} "
                    f"ADD COLUMN IF NOT EXISTS {_pg_ident(column)} {_pg_type(coltype)}"
                )
            for sql in _POSTGRES_INDEX_SQL:
                cur.execute(sql)
            cur.execute(_POSTGRES_VIEW_SQL)
        self._pg_conn.commit()

    _ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (
        ("gold_dim_series", "geo", "TEXT"),
        ("gold_dim_series", "metric", "TEXT"),
        ("gold_dim_date", "is_imm_date", "INTEGER"),
        ("gold_dim_date", "is_monthly_option_expiry", "INTEGER"),
        ("gold_dim_date", "is_triple_witching", "INTEGER"),
        ("audit_etl_run", "series_skipped_not_due", "INTEGER"),
    )

    # ---- low-level helpers --------------------------------------------

    def _adapt_sql(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> tuple[str, Sequence[Any] | Mapping[str, Any]]:
        translated = _translate_flat_tables(sql)
        if isinstance(params, Mapping):
            translated = _NAMED_PARAM.sub(r"%(\1)s", translated)
            return translated, params
        if params:
            translated = translated.replace("?", "%s")
        return translated, params

    def _insert(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        upsert_keys: Sequence[str] | None = None,
    ) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        rel = _relation_from_flat(table)
        collist = _pg_column_list(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {rel} ({collist}) VALUES ({placeholders})"
        if upsert_keys:
            updates = ", ".join(
                f"{_pg_ident(c)}=EXCLUDED.{_pg_ident(c)}"
                for c in cols
                if c not in upsert_keys
            )
            conflict = _pg_column_list(upsert_keys)
            sql += f" ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
            data = [tuple(_encode(r.get(c)) for c in cols) for r in rows]
            with self._pg_conn.cursor() as cur:
                cur.executemany(sql, data)
        else:
            self._copy_rows(rel, cols, ([r.get(c) for c in cols] for r in rows))
        if not self._defer_commits:
            self._pg_conn.commit()
        return len(rows)

    def _insert_frame(
        self,
        table: str,
        df: Any,
        upsert_keys: Sequence[str] | None = None,
    ) -> int:
        if df.is_empty():
            return 0
        if upsert_keys:
            return self._insert(table, df.to_dicts(), upsert_keys=upsert_keys)
        rel = _relation_from_flat(table)
        cols = list(df.columns)
        self._copy_rows(rel, cols, df.iter_rows())
        if not self._defer_commits:
            self._pg_conn.commit()
        return df.height

    def _copy_rows(
        self,
        relation: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
    ) -> None:
        copy_sql = f"COPY {relation} ({_pg_column_list(columns)}) FROM STDIN"
        with self._pg_conn.cursor() as cur, cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row([_encode(value) for value in row])

    def _read(self, table: str) -> list[dict[str, Any]]:
        with self._pg_conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {_relation_from_flat(table)}")
            return [dict(row) for row in cur.fetchall()]

    def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
        *,
        caller: str = "",
    ) -> list[dict[str, Any]]:
        if not self._defer_commits:
            self._log_query(sql, caller=caller)
        sql, params = self._adapt_sql(sql, params)
        with self._pg_conn.cursor() as cur:
            cur.execute(sql, params or None)
            return [dict(row) for row in cur.fetchall()]

    def _log_query(self, sql: str, *, caller: str = "") -> None:
        query_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        self._insert(
            "audit_query_log",
            [
                {
                    "queried_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                    "query_text_hash": query_hash,
                    "caller": caller,
                }
            ],
        )

    # ---- Warehouse surface --------------------------------------------

    def sync_meta(self, manifests: Iterable[Manifest]) -> dict[str, int]:
        rows = build_meta_rows(list(manifests))
        counts = {}
        counts["fred_series"] = self._insert(
            "meta_fred_series", rows["fred_series"], upsert_keys=["series_id"]
        )
        counts["fred_manifest"] = self._insert(
            "meta_fred_manifest", rows["fred_manifest"], upsert_keys=["manifest_name"]
        )
        counts["fred_series_manifest_map"] = self._insert(
            "meta_fred_series_manifest_map",
            rows["fred_series_manifest_map"],
            upsert_keys=["series_id", "manifest_name"],
        )
        return counts

    def restate_start(self, series_id: str, n: int) -> str | None:
        rows = self.query(
            """
            SELECT MIN(observation_date) AS start FROM (
                SELECT DISTINCT observation_date FROM silver_fred_observation
                WHERE series_id = %s
                ORDER BY observation_date DESC
                LIMIT %s
            ) s
            """,
            (series_id, int(n)),
        )
        return rows[0]["start"] if rows and rows[0]["start"] is not None else None

    def write_bronze(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("bronze_fred_api_response", rows)

    def read_bronze(self, series_ids: list[str] | None = None) -> list[dict[str, Any]]:
        sql = (
            "SELECT source, series_id, response_payload, run_id, ingested_at "
            "FROM bronze_fred_api_response"
        )
        params: tuple[Any, ...] = ()
        if series_ids:
            placeholders = ", ".join(["%s"] * len(series_ids))
            sql += f" WHERE series_id IN ({placeholders})"
            params = tuple(series_ids)
        sql += " ORDER BY ingested_at"
        return self.query(sql, params)

    def merge_silver(self, rows: list[dict[str, Any]]) -> int:
        return self._insert(
            "silver_fred_observation",
            rows,
            upsert_keys=["source", "series_id", "observation_date", "realtime_start"],
        )

    def build_gold(self) -> dict[str, str]:
        self.conn.execute("BEGIN")
        self._defer_commits = True
        try:
            result = self._build_gold_inner()
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            return result
        finally:
            self._defer_commits = False

    def _rebuild_gold_latest_observation_sql(self) -> int:
        with self._pg_conn.cursor() as cur:
            cur.execute("DELETE FROM gold.fred_latest_observation")
            cur.execute(
                """
                INSERT INTO gold.fred_latest_observation (
                    series_id, observation_date, value, realtime_start, realtime_end,
                    is_missing, revision_number, ingested_at
                )
                WITH ranked AS (
                    SELECT
                        series_id,
                        observation_date,
                        value,
                        realtime_start,
                        realtime_end,
                        is_missing,
                        revision_number,
                        ingested_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY series_id, observation_date
                            ORDER BY COALESCE(realtime_start, '') DESC, ctid DESC
                        ) AS rn
                    FROM silver.fred_observation
                )
                SELECT
                    series_id,
                    observation_date,
                    value,
                    realtime_start,
                    realtime_end,
                    is_missing,
                    revision_number,
                    ingested_at
                FROM ranked
                WHERE rn = 1
                ORDER BY series_id, observation_date
                """
            )
            return max(cur.rowcount, 0)

    def _build_gold_inner(self) -> dict[str, str]:
        # Reuse the SQLite backend's pure-Python Gold orchestration. The
        # low-level table operations above translate flat table names into
        # Postgres schemas, and the two largest relational transforms are
        # overridden with native Postgres SQL.
        from fred_pipeline.io.local_store import LocalWarehouse

        return LocalWarehouse._build_gold_inner(self)

    def point_in_time_features(self, as_of: str) -> list[dict[str, Any]]:
        from fred_pipeline.features import point_in_time_snapshot

        silver = self._read("silver_fred_observation")
        for row in silver:
            row["is_missing"] = bool(row.get("is_missing"))
        return point_in_time_snapshot(silver, as_of)

    def write_lifecycle(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_fred_series_lifecycle", rows)

    def write_drift(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_fred_series_drift", rows)

    def latest_observation_dates(
        self, series_ids: Sequence[str] | None = None
    ) -> dict[str, str]:
        if series_ids is not None and not series_ids:
            return {}
        sql = (
            "SELECT series_id, MAX(observation_date) AS latest "
            "FROM silver_fred_observation WHERE COALESCE(is_missing, 0) = 0"
        )
        params: list[Any] = []
        if series_ids:
            placeholders = ", ".join(["%s"] * len(series_ids))
            sql += f" AND series_id IN ({placeholders})"
            params.extend(series_ids)
        sql += " GROUP BY series_id"
        return {
            row["series_id"]: row["latest"]
            for row in self.query(sql, tuple(params))
            if row["latest"] is not None
        }

    def write_staleness(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_series_staleness", rows)

    def write_release_calendar(self, rows: list[dict[str, Any]]) -> int:
        with self._pg_conn.cursor() as cur:
            cur.execute("DELETE FROM gold.release_calendar")
        if not self._defer_commits:
            self._pg_conn.commit()
        return self._insert("gold_release_calendar", rows)

    def persist_run_state(self, run: EtlRun) -> None:
        self._insert("audit_etl_run", [run.to_row()], upsert_keys=["run_id"])

    def persist_series_run(self, series_run: EtlSeriesRun) -> None:
        with self._pg_conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM audit.etl_series_run
                WHERE run_id = %s AND series_id = %s
                """,
                (series_run.run_id, series_run.series_id),
            )
        self._insert("audit_etl_series_run", [series_run.to_row()])

    def persist_run(self, run: EtlRun) -> None:
        self.persist_run_state(run)
        for series_run in run.series_runs:
            self.persist_series_run(series_run)

    def persist_dq(self, run_id: str, report: QualityReport) -> None:
        self._insert("audit_data_quality_result", dq_rows(run_id, report))

    def close(self) -> None:
        self.conn.close()

    def tables(self) -> list[str]:
        rows = self.query(
            """
            SELECT table_schema || '.' || table_name AS name
            FROM information_schema.tables
            WHERE table_schema IN ('meta', 'audit', 'bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name
            """
        )
        return [row["name"] for row in rows]

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


def _sqlite_table_defs() -> list[tuple[str, str]]:
    return [
        (match.group(1), match.group(2)) for match in _TABLE_RE.finditer(_SQLITE_SCHEMA)
    ]


def _postgres_create_table_sql(schema: str, table: str, sqlite_body: str) -> str:
    parts = _split_sql_parts(_strip_sql_comments(sqlite_body))
    converted = []
    for part in parts:
        if not part:
            continue
        converted.append(_convert_table_part(part))
    return (
        f"CREATE TABLE IF NOT EXISTS {_pg_name(schema, table)} (\n    "
        + ",\n    ".join(converted)
        + "\n)"
    )


def _convert_table_part(part: str) -> str:
    text = " ".join(part.split())
    upper = text.upper()
    if upper.startswith("PRIMARY KEY"):
        cols = text[text.index("(") + 1 : text.rindex(")")]
        return f"PRIMARY KEY ({_pg_column_list([c.strip() for c in cols.split(',')])})"

    pieces = text.split(None, 2)
    name = pieces[0]
    sqlite_type = pieces[1] if len(pieces) > 1 else "TEXT"
    rest = pieces[2] if len(pieces) > 2 else ""
    pieces = (_pg_ident(name), _pg_type(sqlite_type), _quote_inline_constraints(rest))
    return " ".join(p for p in pieces if p)


def _quote_inline_constraints(rest: str) -> str:
    if not rest:
        return ""

    def quote_pk(match: re.Match[str]) -> str:
        columns = [c.strip() for c in match.group(1).split(",")]
        return f"PRIMARY KEY ({_pg_column_list(columns)})"

    return re.sub(
        r"PRIMARY KEY\s*\(([^)]+)\)",
        quote_pk,
        rest,
        flags=re.IGNORECASE,
    )


def _strip_sql_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _split_sql_parts(sql: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    in_quote = False
    for idx, char in enumerate(sql):
        if char == "'":
            in_quote = not in_quote
        elif not in_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append(sql[start:idx].strip())
                start = idx + 1
    tail = sql[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _translate_flat_tables(sql: str) -> str:
    return _FLAT_TABLE_RE.sub(lambda m: _relation_from_flat(m.group(1)), sql)


def _relation_from_flat(flat_table: str) -> str:
    schema, table = _split_table_name(flat_table)
    return _pg_name(schema, table)


def _split_table_name(flat_table: str) -> tuple[str, str]:
    for schema in _MEDALLION_SCHEMAS:
        prefix = f"{schema}_"
        if flat_table.startswith(prefix):
            return schema, flat_table[len(prefix) :]
    return "public", flat_table


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


_POSTGRES_INDEX_SQL = (
    """
    CREATE INDEX IF NOT EXISTS ix_silver_obs_sid_rt
        ON silver.fred_observation(series_id, realtime_start DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_silver_obs_sid_date
        ON silver.fred_observation(series_id, observation_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_gold_latest_sid
        ON gold.fred_latest_observation(series_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_factor_scores_date
        ON gold.macro_factor_scores(observation_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_equity_attr_ticker_window
        ON gold.equity_factor_attribution(ticker, "window")
    """,
    # spec003 Phase 3's touched-series watermark index and spec007's
    # per-series last-pull index -- both defined in LocalWarehouse's
    # _SCHEMA but never mirrored here, so lookups that need them fall back
    # to a full table scan on Postgres.
    """
    CREATE INDEX IF NOT EXISTS ix_silver_obs_ingested_at
        ON silver.fred_observation(ingested_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_silver_obs_sid_ingested
        ON silver.fred_observation(series_id, ingested_at)
    """,
)

_POSTGRES_VIEW_SQL = """
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

-- Mirrors LocalWarehouse's gold_fred_point_in_time (spec003 section 7: a
-- pure 1:1 view over Silver, not a materialized table). This is the name
-- the flat-table-name translation in _translate_flat_tables expects when
-- pipeline code reads "gold_fred_point_in_time" -- v_point_in_time above is
-- a separate, human-facing alias for BI tools and is kept for that reason,
-- not as a stand-in for this one.
CREATE OR REPLACE VIEW gold.fred_point_in_time AS
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
