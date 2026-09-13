"""Database connection factory: unified read access across all backends.

Provides a single interface to query any warehouse (SQLite, Databricks, DuckDB,
Postgres, etc.) without worrying about dialect differences or credentials.

Supports both simple queries (returning rows as dicts) and streaming for
large datasets.

Usage
-----
Connect to SQLite::

    db = DatabaseConnection.from_config(backend="local", db_path="./fred.db")
    rows = db.query("SELECT * FROM gold_fred_latest_observation LIMIT 10")
    db.close()

Connect to Databricks::

    db = DatabaseConnection.from_config(
        backend="databricks",
        workspace_url="https://my.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/abc123",
        catalog="macro_prod"
    )
    rows = db.query("SELECT * FROM gold.macro_indicator_dashboard")
    db.close()

Stream large result sets::

    db = DatabaseConnection.from_config(backend="local", db_path="fred.db")
    for batch in db.query_stream("SELECT * FROM gold_fred_latest_observation",
                                  batch_size=10000):
        print(f"Received {len(batch)} rows")
    db.close()

With context manager::

    with DatabaseConnection.from_config(backend="local", db_path="fred.db") as db:
        rows = db.query("SELECT COUNT(*) as n FROM gold_fred_latest_observation")
        print(rows[0]["n"])
"""

from __future__ import annotations

import logging
import re
from collections.abc import Generator, Mapping
from types import TracebackType
from typing import Any, Protocol, runtime_checkable

log = logging.getLogger("fred_pipeline")


@runtime_checkable
class DatabaseConnection(Protocol):
    """Protocol for database read access.

    Implementations should handle connection pooling, credential management,
    and dialect-specific query adaptation.
    """

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return all rows.

        Parameters
        ----------
        sql : str
            SQL query. Use :param syntax for parameters
            (e.g., "SELECT * WHERE id = :id")
        params : dict, optional
            Query parameters to bind.

        Returns
        -------
        list[dict]
            Rows as dictionaries with column names as keys.
        """
        ...

    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        batch_size: int = 10000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Execute a query and yield rows in batches.

        Useful for large result sets. Each batch is a list of dicts.

        Parameters
        ----------
        sql : str
            SQL query.
        params : dict, optional
            Query parameters to bind.
        batch_size : int
            Number of rows per batch (default 10000).

        Yields
        ------
        list[dict]
            Batches of rows.
        """
        ...

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> int:
        """Execute a write/update statement. Not all backends support this.

        Parameters
        ----------
        sql : str
            SQL statement (INSERT, UPDATE, DELETE, etc.)
        params : dict, optional
            Query parameters to bind.

        Returns
        -------
        int
            Number of rows affected.

        Raises
        ------
        NotImplementedError
            If the backend is read-only.
        """
        ...

    def table_names(self, schema: str | None = None) -> list[str]:
        """List table names in the database (or schema, if applicable).

        Parameters
        ----------
        schema : str, optional
            Schema name (for Databricks, DuckDB, PostgreSQL). Ignored for SQLite.

        Returns
        -------
        list[str]
            Table names.
        """
        ...

    def close(self) -> None:
        """Close the connection and release resources."""
        ...

    def __enter__(self) -> DatabaseConnection:  # noqa: PYI034 -- Self needs py3.11+, repo supports 3.10
        """Context manager entry."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit."""
        ...


class SQLiteConnection:
    """SQLite database connection."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        import sqlite3

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Return rows as dicts

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute query and return all rows."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        return [dict(row) for row in cursor.fetchall()]

    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        batch_size: int = 10000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Execute query and stream rows in batches."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(row) for row in rows]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> int:
        """Execute write statement (INSERT, UPDATE, DELETE)."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        self.conn.commit()
        return cursor.rowcount

    def table_names(self, schema: str | None = None) -> list[str]:
        """List table names in the database. Schema parameter is ignored."""
        rows = self.query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [r["name"] for r in rows]

    def close(self) -> None:
        """Close the connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> SQLiteConnection:  # noqa: PYI034 -- Self needs py3.11+, repo supports 3.10
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


class DatabricksConnection:
    """Databricks SQL connection via Python SQL connector."""

    def __init__(
        self,
        workspace_url: str,
        http_path: str,
        catalog: str | None = None,
        timeout: int = 60,
    ):
        self.workspace_url = workspace_url
        self.http_path = http_path
        self.catalog = catalog
        self.timeout = timeout
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        try:
            from databricks import sql
        except ImportError:
            raise ImportError(
                "databricks-sql-connector required for Databricks backend. "
                "Install with: pip install databricks-sql-connector"
            )

        # Credentials come from environment (DATABRICKS_HOST, DATABRICKS_TOKEN)
        # or Databricks CLI config
        self.conn = sql.connect(
            server_hostname=self.workspace_url.replace("https://", "").split("/")[0],
            http_path=self.http_path,
            catalog=self.catalog,
        )

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute query and return all rows."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        # Databricks cursor.fetchall() returns lists; convert to dicts
        description = cursor.description
        rows = cursor.fetchall()
        return [{desc[0]: val for desc, val in zip(description, row)} for row in rows]

    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        batch_size: int = 10000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Execute query and stream rows in batches."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        description = cursor.description
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [
                {desc[0]: val for desc, val in zip(description, row)} for row in rows
            ]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> int:
        """Execute write statement."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        return cursor.rowcount

    def table_names(self, schema: str | None = None) -> list[str]:
        """List table names. If schema is provided, list tables in that schema."""
        if schema:
            sql = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema"
            )
            rows = self.query(sql, {"schema": schema})
        else:
            sql = "SHOW TABLES"
            rows = self.query(sql)

        # SHOW TABLES returns differently; normalize
        if rows and "table_name" in rows[0]:
            return [r["table_name"] for r in rows]
        elif rows and "tableName" in rows[0]:
            return [r["tableName"] for r in rows]
        else:
            return []

    def close(self) -> None:
        """Close the connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> DatabricksConnection:  # noqa: PYI034 -- Self needs py3.11+, repo supports 3.10
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


class DuckDBConnection:
    """DuckDB database connection."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb required for DuckDB backend. Install with: pip install duckdb"
            )

        self.conn = duckdb.connect(self.db_path, read_only=True)

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute query and return all rows."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        if params:
            result = self.conn.execute(sql, params).fetchall()
        else:
            result = self.conn.execute(sql).fetchall()

        columns = self.conn.description
        if columns is None:
            return []

        return [{col[0]: val for col, val in zip(columns, row)} for row in result]

    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        batch_size: int = 10000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Execute query and stream rows in batches."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        if params:
            cursor = self.conn.execute(sql, params)
        else:
            cursor = self.conn.execute(sql)

        columns = cursor.description
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [{col[0]: val for col, val in zip(columns, row)} for row in rows]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> int:
        """Execute write statement. DuckDB read-only mode does not support writes."""
        raise NotImplementedError("DuckDB connection is read-only")

    def table_names(self, schema: str | None = None) -> list[str]:
        """List table names."""
        if schema:
            sql = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = ?"
            )
            rows = self.query(sql, (schema,))
        else:
            sql = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema != 'information_schema'"
            )
            rows = self.query(sql)

        return [r["table_name"] for r in rows]

    def close(self) -> None:
        """Close the connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> DuckDBConnection:  # noqa: PYI034 -- Self needs py3.11+, repo supports 3.10
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


_NAMED_PARAM = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")


def _adapt_psycopg_params(
    sql: str, params: Mapping[str, Any] | None
) -> tuple[str, Mapping[str, Any] | None]:
    """Convert the repo's :name query convention to psycopg's %(name)s."""
    if not params:
        return sql, params
    return _NAMED_PARAM.sub(r"%(\1)s", sql), params


class PostgresConnection:
    """PostgreSQL database connection via psycopg v3."""

    def __init__(self, **kwargs: Any):
        from fred_pipeline.io.postgres_config import resolve_postgres_settings

        self.settings = resolve_postgres_settings(kwargs)
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError(
                "psycopg required for Postgres backend. "
                "Install with: pip install -e '.[postgres]'"
            )

        self.conn = psycopg.connect(self.settings.dsn, row_factory=dict_row)

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute query and return all rows."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        sql, params = _adapt_psycopg_params(sql, params)
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        batch_size: int = 10000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Execute query and stream rows in batches."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        sql, params = _adapt_psycopg_params(sql, params)
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield list(rows)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> int:
        """Execute write statement."""
        if self.conn is None:
            raise RuntimeError("Connection closed")

        sql, params = _adapt_psycopg_params(sql, params)
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            self.conn.commit()
            return cursor.rowcount

    def table_names(self, schema: str | None = None) -> list[str]:
        """List table names, schema-qualified when no schema is supplied."""
        if schema:
            rows = self.query(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema
                ORDER BY table_name
                """,
                {"schema": schema},
            )
            return [r["table_name"] for r in rows]

        rows = self.query(
            """
            SELECT table_schema || '.' || table_name AS table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
            """
        )
        return [r["table_name"] for r in rows]

    def close(self) -> None:
        """Close the connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> PostgresConnection:  # noqa: PYI034 -- Self needs py3.11+, repo supports 3.10
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


class DatabaseConnectionFactory:
    """Factory for creating database connections."""

    @staticmethod
    def create(
        backend: str, **kwargs: Any
    ) -> (
        SQLiteConnection | DatabricksConnection | DuckDBConnection | PostgresConnection
    ):
        """Create a database connection for the specified backend.

        Parameters
        ----------
        backend : str
            Backend type: "local", "sqlite", "databricks", "duckdb", "postgres"
        **kwargs
            Backend-specific parameters (db_path, workspace_url, http_path, etc.)

        Returns
        -------
        DatabaseConnection
            A connection to the specified backend.

        Raises
        ------
        ValueError
            If backend is unknown.
        ImportError
            If required packages are not installed.
        """
        if backend in ("local", "sqlite"):
            db_path = kwargs.get("db_path")
            if not db_path:
                raise ValueError("db_path required for SQLite backend")
            return SQLiteConnection(db_path)

        elif backend == "databricks":
            workspace_url = kwargs.get("workspace_url")
            http_path = kwargs.get("http_path")
            catalog = kwargs.get("catalog")
            timeout = kwargs.get("timeout", 60)

            if not workspace_url or not http_path:
                raise ValueError("workspace_url and http_path required for Databricks")

            return DatabricksConnection(workspace_url, http_path, catalog, timeout)

        elif backend == "duckdb":
            db_path = kwargs.get("db_path")
            if not db_path:
                raise ValueError("db_path required for DuckDB backend")
            return DuckDBConnection(db_path)

        elif backend in ("postgres", "postgresql"):
            return PostgresConnection(**kwargs)

        else:
            raise ValueError(f"Unknown backend: {backend}")

    @staticmethod
    def from_warehouse_config(
        warehouse_config: Any, environment: str = "dev"
    ) -> (
        SQLiteConnection | DatabricksConnection | DuckDBConnection | PostgresConnection
    ):
        """Create a connection from a warehouse config object.

        Parameters
        ----------
        warehouse_config : WarehouseConfig
            Config loaded from warehouse.yml
        environment : str
            Environment name (dev/test/prod)

        Returns
        -------
        DatabaseConnection
            A connection to the primary backend in the config.
        """
        backend = warehouse_config.primary_backend
        backends = warehouse_config.backends or {}
        backend_config = backends.get(backend, {})

        return DatabaseConnectionFactory.create(backend, **backend_config)
