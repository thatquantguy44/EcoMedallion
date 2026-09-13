#!/usr/bin/env python
"""Example script: Query Gold layer using the database connection factory.

Usage
-----
Query SQLite locally::

    python scripts/query_gold_layer.py --backend local --db-path ./fred.db \\
        --query "SELECT COUNT(*) as n FROM gold_fred_latest_observation"

Query Databricks::

    python scripts/query_gold_layer.py --backend databricks \\
        --workspace-url https://my.cloud.databricks.com \\
        --http-path /sql/1.0/warehouses/abc123 \\
        --catalog macro_prod \\
        --query "SELECT COUNT(*) as n FROM gold.macro_indicator_dashboard"

Export a table to CSV::

    python scripts/query_gold_layer.py --backend local --db-path ./fred.db \\
        --export gold_global_inflation.csv \\
        --export-limit 100000

Stream large table in batches::

    python scripts/query_gold_layer.py --backend local --db-path ./fred.db \\
        --stream gold_fred_latest_observation --batch-size 50000

List all Gold tables::

    python scripts/query_gold_layer.py --backend local --db-path ./fred.db --list-tables
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fred_pipeline.io.database_connection import DatabaseConnectionFactory

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("query_gold_layer")


def query_database(args: argparse.Namespace) -> int:
    """Execute a single query and print results."""
    try:
        db = DatabaseConnectionFactory.create(args.backend, **_build_backend_args(args))
    except (ValueError, ImportError) as e:
        log.error(f"Failed to connect to {args.backend}: {e}")
        return 1

    try:
        log.info(f"Executing query on {args.backend}...")
        rows = db.query(args.query)
        log.info(f"Retrieved {len(rows)} rows")

        if not rows:
            print("No results")
            return 0

        print(json.dumps(rows, indent=2, default=str))
        return 0

    finally:
        db.close()


def stream_table(args: argparse.Namespace) -> int:
    """Stream a table in batches (for large tables)."""
    try:
        db = DatabaseConnectionFactory.create(args.backend, **_build_backend_args(args))
    except (ValueError, ImportError) as e:
        log.error(f"Failed to connect to {args.backend}: {e}")
        return 1

    try:
        log.info(
            f"Streaming {args.stream} from {args.backend} "
            f"(batch size: {args.batch_size})..."
        )

        total_rows = 0
        query = f"SELECT * FROM {args.stream}"
        for batch_num, batch in enumerate(
            db.query_stream(query, batch_size=args.batch_size), 1
        ):
            total_rows += len(batch)
            log.info(f"Batch {batch_num}: {len(batch)} rows (total: {total_rows})")

        log.info(f"Total rows streamed: {total_rows}")
        return 0

    finally:
        db.close()


def export_table(args: argparse.Namespace) -> int:
    """Export a table to CSV."""
    try:
        db = DatabaseConnectionFactory.create(args.backend, **_build_backend_args(args))
    except (ValueError, ImportError) as e:
        log.error(f"Failed to connect to {args.backend}: {e}")
        return 1

    try:
        output_path = Path(args.export)
        log.info(f"Exporting to {output_path}...")

        query = f"SELECT * FROM {args.export.replace('.csv', '')}"
        if args.export_limit:
            query += f" LIMIT {args.export_limit}"

        rows = db.query(query)
        log.info(f"Retrieved {len(rows)} rows")

        if not rows:
            log.warning("No rows to export")
            return 0

        # Write CSV
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        log.info(f"Exported {len(rows)} rows to {output_path}")
        return 0

    finally:
        db.close()


def list_tables(args: argparse.Namespace) -> int:
    """List all tables in the database."""
    try:
        db = DatabaseConnectionFactory.create(args.backend, **_build_backend_args(args))
    except (ValueError, ImportError) as e:
        log.error(f"Failed to connect to {args.backend}: {e}")
        return 1

    try:
        log.info(f"Listing tables in {args.backend}...")
        tables = db.table_names(schema=getattr(args, "schema", None))

        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table}")

        # Filter to Gold tables across flat SQLite names and schema-qualified
        # backends such as Postgres/Databricks.
        gold_tables = [t for t in tables if _is_gold_table(t, args)]
        if gold_tables:
            print(f"\nGold layer tables ({len(gold_tables)}):")
            for table in gold_tables:
                print(f"  - {table}")

        return 0

    finally:
        db.close()


def _build_backend_args(args: argparse.Namespace) -> dict[str, str]:
    """Build backend-specific arguments from parsed CLI args."""
    backend_args = {}

    if args.backend in ("local", "sqlite"):
        backend_args["db_path"] = args.db_path

    elif args.backend == "databricks":
        backend_args["workspace_url"] = args.workspace_url
        backend_args["http_path"] = args.http_path
        if args.catalog:
            backend_args["catalog"] = args.catalog

    elif args.backend == "duckdb":
        backend_args["db_path"] = args.db_path

    elif args.backend in ("postgres", "postgresql"):
        backend_args["target"] = args.postgres_target
        if args.postgres_dsn:
            backend_args["dsn"] = args.postgres_dsn
        if args.postgres_dsn_env:
            backend_args["dsn_env"] = args.postgres_dsn_env

    return backend_args


def _is_gold_table(table: str, args: argparse.Namespace) -> bool:
    if args.backend in ("local", "sqlite"):
        return table.startswith("gold_")
    if getattr(args, "schema", None) == "gold":
        return True
    return table == "gold" or table.startswith("gold.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Query the FRED pipeline's Gold layer using any backend "
            "(SQLite, Databricks, DuckDB, Postgres)."
        )
    )

    # Common arguments
    parser.add_argument(
        "--backend",
        choices=["local", "sqlite", "databricks", "duckdb", "postgres", "postgresql"],
        default="local",
        help="Database backend (default: local SQLite)",
    )

    # SQLite/DuckDB arguments
    parser.add_argument(
        "--db-path",
        default="./fred.db",
        help="Path to SQLite/DuckDB file (default: ./fred.db)",
    )

    # Databricks arguments
    parser.add_argument(
        "--workspace-url",
        help="Databricks workspace URL (e.g., https://my.cloud.databricks.com)",
    )
    parser.add_argument(
        "--http-path",
        help="Databricks SQL warehouse HTTP path (e.g., /sql/1.0/warehouses/abc123)",
    )
    parser.add_argument(
        "--catalog",
        help="Databricks catalog (e.g., macro_prod). Optional; defaults from config.",
    )
    parser.add_argument(
        "--postgres-target",
        choices=["local", "service"],
        default="local",
        help="Postgres target profile for DSN resolution (default: local)",
    )
    parser.add_argument(
        "--postgres-dsn",
        help="Postgres connection string; overrides target defaults/env lookup",
    )
    parser.add_argument(
        "--postgres-dsn-env",
        help="Environment variable containing the Postgres connection string",
    )

    # Operation arguments (mutually exclusive)
    op_group = parser.add_mutually_exclusive_group(required=True)
    op_group.add_argument("--query", help="SQL query to execute")
    op_group.add_argument(
        "--stream",
        help="Table name to stream in batches (for large tables)",
    )
    op_group.add_argument(
        "--export",
        help="Export table to CSV (specify CSV filename, table name derived)",
    )
    op_group.add_argument("--list-tables", action="store_true", help="List all tables")

    # Optional arguments
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for streaming (default: 10000)",
    )
    parser.add_argument(
        "--export-limit",
        type=int,
        help="Limit rows in export (optional)",
    )
    parser.add_argument(
        "--schema",
        help="Schema name (for Databricks/DuckDB/Postgres)",
    )

    args = parser.parse_args()

    # Route to appropriate operation
    if args.query:
        return query_database(args)
    elif args.stream:
        return stream_table(args)
    elif args.export:
        return export_table(args)
    elif args.list_tables:
        return list_tables(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
