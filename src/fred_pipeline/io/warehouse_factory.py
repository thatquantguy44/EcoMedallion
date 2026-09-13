"""Warehouse factory: configurable storage backend selection.

Resolves warehouse configuration to a concrete backend (LocalWarehouse,
SparkWarehouse, PostgresWarehouse, etc.). Supports tiered fallback: primary
backend, then fallback backends, then in-memory dry-run.

Configuration is environment-aware and file-based (config/warehouse.yml) or
explicit arguments.

Example config/warehouse.yml::

    default:
      primary_backend: local
      local:
        db_path: ./fred.db
      databricks:
        catalog: macro_dev
        workspace_url: https://my-workspace.cloud.databricks.com
        http_path: /sql/1.0/warehouses/abc123

    environments:
      prod:
        primary_backend: databricks
        databricks:
          catalog: macro_prod
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from fred_pipeline.config import PipelineConfig
from fred_pipeline.warehouse import Warehouse


@dataclass(frozen=True)
class WarehouseConfig:
    """Warehouse backend configuration."""

    # Primary backend: "local", "databricks", "postgres", "duckdb",
    # or None for dry-run
    primary_backend: str = "local"

    # Fallback backends to try if primary fails (e.g., ["duckdb", "local"])
    fallback_backends: list[str] = None

    # Backend-specific settings
    backends: dict[str, dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.fallback_backends is None:
            object.__setattr__(self, "fallback_backends", [])
        if self.backends is None:
            object.__setattr__(self, "backends", {})


def load_warehouse_config(
    path: str | None = None, environment: str = "dev"
) -> WarehouseConfig:
    """Load warehouse configuration from YAML file.

    Resolution: explicit path > FRED_WAREHOUSE_CONFIG env var >
    config/warehouse.yml > built-in defaults.

    Built-in default: primary_backend="local" with db_path="fred.db".
    """
    try:
        import yaml
    except ImportError:
        yaml = None

    resolved = path or os.environ.get("FRED_WAREHOUSE_CONFIG") or "config/warehouse.yml"

    if not resolved or not os.path.isfile(resolved):
        # Default: local SQLite
        return WarehouseConfig(
            primary_backend="local",
            backends={"local": {"db_path": "fred.db"}},
        )

    if yaml is None:
        raise RuntimeError("PyYAML required to load warehouse config")

    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    if not isinstance(data, dict):
        raise TypeError(f"Warehouse config {resolved} must be a mapping")

    # Merge default + environment-specific settings
    settings = dict(data.get("default") or {})
    env_section = (data.get("environments") or {}).get(environment)
    if env_section:
        settings.update(env_section)

    return WarehouseConfig(
        primary_backend=settings.get("primary_backend", "local"),
        fallback_backends=settings.get("fallback_backends", []),
        backends=settings.get("backends", {"local": {"db_path": "fred.db"}}),
    )


class WarehouseFactory:
    """Instantiate warehouse backends based on configuration.

    Tries primary backend, then fallbacks, then in-memory dry-run.
    """

    def __init__(self, config: PipelineConfig, warehouse_config: WarehouseConfig):
        self.config = config
        self.warehouse_config = warehouse_config

    def build(self, force_dry_run: bool = False) -> Warehouse | None:
        """Build a warehouse, with fallback on error.

        Parameters
        ----------
        force_dry_run : bool
            If True, skip all backends and return None (in-memory only).

        Returns
        -------
        Warehouse or None
            A warehouse instance, or None for in-memory dry-run.
        """
        if force_dry_run:
            return None

        backends_to_try = [self.warehouse_config.primary_backend]
        if self.warehouse_config.fallback_backends:
            backends_to_try.extend(self.warehouse_config.fallback_backends)

        for backend_name in backends_to_try:
            try:
                warehouse = self._build_backend(backend_name)
                if warehouse is not None:
                    return warehouse
            except Exception as e:  # noqa: BLE001 -- must survive any backend's
                # own exception type (Delta, psycopg, sqlite3, ...) to fall
                # through to the next backend rather than aborting the run.
                import logging

                log = logging.getLogger("fred_pipeline")
                log.warning(
                    f"Failed to initialize {backend_name} backend: {e}. "
                    f"Trying next fallback..."
                )

        # No backend succeeded; fall back to in-memory dry-run
        import logging

        log = logging.getLogger("fred_pipeline")
        log.warning("All warehouse backends failed. Falling back to in-memory dry-run.")
        return None

    def _build_backend(self, backend_name: str) -> Warehouse | None:
        """Build a single backend by name."""
        if not backend_name or backend_name == "none":
            return None

        backend_config = self.warehouse_config.backends.get(backend_name, {})

        if backend_name == "local":
            from fred_pipeline.local_store import LocalWarehouse

            db_path = backend_config.get("db_path", "fred.db")
            return LocalWarehouse(self.config, db_path=db_path)

        elif backend_name == "databricks":
            from fred_pipeline.warehouse import SparkWarehouse

            try:
                from fred_pipeline.spark_io import get_spark
            except ImportError:
                raise ImportError("Spark required for Databricks backend")

            # For Databricks, we may need to set up Spark with connection
            # details. This is a simplified version; in production you'd
            # handle auth/workspace details.
            spark = get_spark()
            if spark is None:
                raise RuntimeError("Could not initialize Spark for Databricks backend")

            return SparkWarehouse(self.config, spark)

        elif backend_name == "duckdb":
            # DuckDB backend (future expansion)
            raise NotImplementedError(
                "DuckDB backend not yet implemented. Use 'local' for now."
            )

        elif backend_name == "postgres":
            from fred_pipeline.io.postgres_config import (
                redact_postgres_dsn,
                resolve_postgres_settings,
            )

            settings = resolve_postgres_settings(backend_config)
            raise NotImplementedError(
                "Postgres warehouse write backend is not implemented yet "
                "(Spec004 Phase 1 pending). Config resolved target="
                f"{settings.target!r}, dsn={redact_postgres_dsn(settings.dsn)!r}."
            )

        else:
            raise ValueError(f"Unknown warehouse backend: {backend_name}")


def warehouse_from_config(
    pipeline_config: PipelineConfig,
    warehouse_config_path: str | None = None,
    force_dry_run: bool = False,
) -> Warehouse | None:
    """One-line convenience: load config and build warehouse.

    Returns None if dry_run is True or all backends fail.
    """
    warehouse_config = load_warehouse_config(
        path=warehouse_config_path, environment=pipeline_config.environment.value
    )
    factory = WarehouseFactory(pipeline_config, warehouse_config)
    return factory.build(force_dry_run=force_dry_run)
