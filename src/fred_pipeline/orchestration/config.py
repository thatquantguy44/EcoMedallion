"""Independent launch schemas; protected paths and secrets stay on the server."""

from typing import Literal

from dagster import Config
from pydantic import Field, field_validator


class EnvironmentConfig(Config):
    environment: Literal["dev", "test", "prod"] = "dev"


class WarehouseJobConfig(EnvironmentConfig):
    backend: Literal["configured", "postgres", "local", "databricks"] = "configured"


class MacroPipelineConfig(WarehouseJobConfig):
    series: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    exclude_sources: list[str] = Field(default_factory=list)
    full: bool = False
    skip_not_due: bool = True
    build_gold: bool = True
    refresh_fomc_calendar: bool = True
    dry_run: bool = False
    extract_workers: int | None = Field(default=None, ge=1, le=64)
    rate_limit_per_minute: int | None = Field(default=None, ge=1, le=10000)

    @field_validator("sources", "exclude_sources")
    @classmethod
    def normalize_sources(cls, values):
        return [value.strip().lower() for value in values if value.strip()]


class ECBEligibleAssetsConfig(WarehouseJobConfig):
    backend: Literal["configured", "postgres", "local"] = "postgres"
    allow_gap: bool = False


class ECBHealthConfig(WarehouseJobConfig):
    backend: Literal["configured", "postgres", "local"] = "postgres"


class WarehouseHealthConfig(WarehouseJobConfig):
    series: list[str] = Field(default_factory=list)
    max_missing_series: int = Field(default=0, ge=0)
    max_stale_series: int = Field(default=0, ge=0)
    max_capture_age_days: int = Field(default=7, ge=1)
    required_gold_relations: list[str] = Field(
        default_factory=lambda: ["gold_fred_latest_observation"]
    )
    min_gold_rows: int = Field(default=1, ge=0, le=10000)

    @field_validator("required_gold_relations")
    @classmethod
    def safe_relations(cls, values):
        import re

        if any(not re.fullmatch(r"gold_[a-z][a-z0-9_]*", value) for value in values):
            raise ValueError("required Gold relations must use gold_lowercase_name")
        return values


class GoldRebuildConfig(WarehouseJobConfig):
    full: bool = False
    benchmark_as_of: str | None = None


class MetadataReconciliationConfig(WarehouseJobConfig):
    series: list[str] = Field(default_factory=list)
    persist: bool = False
    fail_on_warning: bool = False


class FOMCRefreshConfig(EnvironmentConfig):
    refresh: bool = True
    timeout_seconds: int = Field(default=15, ge=1, le=120)
