"""Runtime configuration for the FRED pipeline.

Configuration is environment-aware (dev / test / prod) so the same code can be
promoted across Unity Catalog catalogs without edits. Settings can come from a
YAML file (``config/config.yaml`` by default), environment variables, explicit
arguments, or a Databricks secret scope.

Precedence (highest wins): explicit argument > environment variable >
config file > built-in default. The FRED API key additionally falls back to a
Databricks secret scope when it is not set by any of the above. Nothing here
hardcodes a secret, and the real ``config/config.yaml`` is git-ignored.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is a core dependency
    yaml = None  # type: ignore


class Environment(str, Enum):
    """Deployment environment. Maps 1:1 to a Unity Catalog catalog."""

    DEV = "dev"
    TEST = "test"
    PROD = "prod"

    @property
    def catalog(self) -> str:
        return f"macro_{self.value}"


# Schema names are stable across environments; only the catalog changes.
SCHEMAS = ("meta", "audit", "bronze", "silver", "gold", "sandbox")

# Default location for the (git-ignored) config file, relative to the CWD.
DEFAULT_CONFIG_PATH = "config/config.yaml"

# Config fields that may be set from file / env / overrides (not `environment`).
_SETTING_FIELDS = (
    "fred_api_key",
    "fred_base_url",
    "ecb_base_url",
    "bls_api_key",
    "eia_api_key",
    "bea_api_key",
    "census_api_key",
    "sec_user_agent",
    "bls_user_agent",
    "stooq_api_key",
    "tiingo_api_key",
    "secret_scope",
    "secret_key",
    "request_timeout_seconds",
    "max_retries",
    "rate_limit_per_minute",
    "source_extract_workers",
    "source_rate_limits",
    "raw_volume_path",
    "restate_last_n",
    "alert_webhook_url",
    "notify_on",
    "complete_vintage_history",
    "extract_workers",
)

# Environment-variable name for each setting (12-factor style overrides).
_ENV_OVERRIDES = {
    "fred_api_key": "FRED_API_KEY",
    "fred_base_url": "FRED_BASE_URL",
    "ecb_base_url": "ECB_BASE_URL",
    "bls_api_key": "BLS_API_KEY",
    "eia_api_key": "EIA_API_KEY",
    "bea_api_key": "BEA_API_KEY",
    "census_api_key": "CENSUS_API_KEY",
    "sec_user_agent": "SEC_USER_AGENT",
    "bls_user_agent": "BLS_USER_AGENT",
    "stooq_api_key": "STOOQ_API_KEY",
    "tiingo_api_key": "TIINGO_API_KEY",
    "secret_scope": "FRED_SECRET_SCOPE",
    "secret_key": "FRED_SECRET_KEY",
    "request_timeout_seconds": "FRED_REQUEST_TIMEOUT_SECONDS",
    "max_retries": "FRED_MAX_RETRIES",
    "rate_limit_per_minute": "FRED_RATE_LIMIT_PER_MINUTE",
    "source_extract_workers": "FRED_SOURCE_EXTRACT_WORKERS",
    "source_rate_limits": "FRED_SOURCE_RATE_LIMITS",
    "raw_volume_path": "FRED_RAW_VOLUME_PATH",
    "restate_last_n": "FRED_RESTATE_LAST_N",
    "alert_webhook_url": "FRED_ALERT_WEBHOOK_URL",
    "notify_on": "FRED_NOTIFY_ON",
    "complete_vintage_history": "FRED_COMPLETE_VINTAGE_HISTORY",
    "extract_workers": "FRED_EXTRACT_WORKERS",
}

_INT_FIELDS = {
    "request_timeout_seconds",
    "max_retries",
    "rate_limit_per_minute",
    "restate_last_n",
    "extract_workers",
}

_BOOL_FIELDS = {"complete_vintage_history"}
_TRUE_STRINGS = {"1", "true", "yes", "on"}


def load_config_file(
    path: str | None = None, environment: str = "dev"
) -> dict[str, Any]:
    """Load settings from a YAML config file, merged for the given environment.

    The file may be either a flat mapping of settings, or structured with a
    ``default:`` block plus per-environment overrides under ``environments:``::

        default:
          rate_limit_per_minute: 120
          secret_scope: fred
        environments:
          prod:
            rate_limit_per_minute: 60

    Resolution of the path: explicit ``path`` argument, else ``FRED_CONFIG_FILE``
    env var, else ``config/config.yaml``. A missing file yields ``{}`` (so the
    file is entirely optional). Only recognized setting keys are returned.
    """
    resolved = path or os.environ.get("FRED_CONFIG_FILE") or DEFAULT_CONFIG_PATH
    if not resolved or not os.path.isfile(resolved):
        return {}
    if yaml is None:  # pragma: no cover
        raise RuntimeError("PyYAML is required to read config files")
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise TypeError(f"Config file {resolved} must be a mapping at the top level")

    settings: dict[str, Any] = {}
    if "default" in data or "environments" in data:
        settings.update(data.get("default") or {})
        env_section = (data.get("environments") or {}).get(environment) or {}
        settings.update(env_section)
    else:
        settings.update(data)

    # Keep only recognized settings; ignore comments/unknown keys quietly.
    return {k: v for k, v in settings.items() if k in _SETTING_FIELDS}


def _env_var_settings() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field_name, env_name in _ENV_OVERRIDES.items():
        val = os.environ.get(env_name)
        if val is not None and val != "":
            out[field_name] = val
    return out


def _coerce_settings(settings: dict[str, Any]) -> dict[str, Any]:
    out = dict(settings)
    for name in _INT_FIELDS:
        if out.get(name) is not None:
            out[name] = int(out[name])
    for name in _BOOL_FIELDS:
        val = out.get(name)
        if isinstance(val, str):
            out[name] = val.strip().lower() in _TRUE_STRINGS
        elif val is not None:
            out[name] = bool(val)
    return out


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved configuration for a single pipeline invocation.

    Attributes
    ----------
    environment:
        Which catalog to target (dev/test/prod).
    fred_api_key:
        Resolved FRED API key. Never logged.
    fred_base_url:
        Base URL for the FRED REST API.
    secret_scope / secret_key:
        Databricks secret scope + key used to resolve the API key when one is
        not passed explicitly.
    request_timeout_seconds / max_retries / rate_limit_per_minute:
        HTTP client tuning knobs.
    raw_volume_path:
        Unity Catalog volume path where raw JSON payloads are archived.
    restate_last_n:
        Default number of most-recent observations to re-pull ("restate") on an
        incremental load, so recent revisions are captured. A series with no data
        yet is always loaded in full. Overridable per series via the manifest's
        ``restate_records``.
    """

    environment: Environment = Environment.DEV
    fred_api_key: str = field(repr=False, default="")
    fred_base_url: str = "https://api.stlouisfed.org/fred"
    ecb_base_url: str = "https://data-api.ecb.europa.eu/service"
    # Optional keys for additional sources (see fred_pipeline.sources). BLS and
    # Census work keyless at a lower quota; EIA and BEA require a key. SEC needs
    # a descriptive User-Agent (contact), not a key. Keys are never logged.
    bls_api_key: str = field(repr=False, default="")
    eia_api_key: str = field(repr=False, default="")
    bea_api_key: str = field(repr=False, default="")
    census_api_key: str = field(repr=False, default="")
    sec_user_agent: str = (
        "fred-bronze-to-gold-pipeline (set SEC_USER_AGENT to your contact)"
    )
    # Sent on requests to BLS's flat-file series-catalog server (used by
    # discover-bls; the v2 JSON API in fred_pipeline.sources.bls needs no UA).
    bls_user_agent: str = (
        "fred-bronze-to-gold-pipeline (set BLS_USER_AGENT to your contact)"
    )
    stooq_api_key: str = field(repr=False, default="")  # Stooq CSV download key
    tiingo_api_key: str = field(repr=False, default="")  # equity total return
    secret_scope: str = "fred"
    secret_key: str = "api_key"
    request_timeout_seconds: int = 30
    max_retries: int = 5
    rate_limit_per_minute: int = 120
    # Optional comma-separated per-source worker overrides, e.g.
    # ``fred=16,stooq=8,tiingo=1``. Unspecified sources use the default
    # source-aware caps in ``pipeline._extract_workers_for_source``.
    source_extract_workers: str = ""
    # Optional comma-separated per-source request-rate overrides, e.g.
    # ``fred=60,stooq=20,tiingo=10``. Unspecified sources use each client's
    # conservative default.
    source_rate_limits: str = ""
    raw_volume_path: str = ""
    restate_last_n: int = 90
    # Concurrent extraction workers (thread pool). Extraction is I/O-bound and
    # rate-limited (see rate_limit_per_minute), so this overlaps per-series
    # response wait / retry backoff rather than exceeding the aggregate call
    # rate — the shared RateLimiter still serializes actual request issuance.
    extract_workers: int = 8
    # Notifications: POST a run summary to this webhook (Slack-compatible JSON).
    # notify_on: "never" | "failure" (default) | "always".
    alert_webhook_url: str = field(repr=False, default="")
    notify_on: str = "failure"
    # When true, vintage_enabled series fetch their COMPLETE vintage history by
    # batching under FRED's 2000-vintage-date cap (more requests, full history).
    # When false, a single full-window request is made and, if it hits the cap,
    # the client falls back to a bounded recent window.
    complete_vintage_history: bool = False

    @property
    def catalog(self) -> str:
        return self.environment.catalog

    def table(self, schema: str, name: str) -> str:
        """Fully-qualified Unity Catalog table name: catalog.schema.name."""
        if schema not in SCHEMAS:
            raise ValueError(f"Unknown schema {schema!r}; expected one of {SCHEMAS}")
        return f"{self.catalog}.{schema}.{name}"

    @property
    def raw_archive_path(self) -> str:
        if self.raw_volume_path:
            return self.raw_volume_path
        return f"/Volumes/{self.catalog}/bronze/raw"

    @classmethod
    def resolve(
        cls,
        environment: Environment | str = Environment.DEV,
        fred_api_key: str | None = None,
        *,
        dbutils=None,
        config_file: str | None = None,
        **overrides,
    ) -> PipelineConfig:
        """Build a config from file + env + args + Databricks secrets.

        Precedence for every setting (highest wins):
          1. explicit keyword argument (``fred_api_key=`` or ``**overrides``)
          2. environment variable (see ``_ENV_OVERRIDES``)
          3. config file (``config_file`` / ``FRED_CONFIG_FILE`` /
             ``config/config.yaml``), merged for this environment
          4. built-in dataclass default

        The API key additionally falls back to the Databricks secret scope
        (whose name/key can themselves come from any layer above) when it is not
        provided by 1–3.
        """
        if isinstance(environment, str):
            environment = Environment(environment)

        # Layer the sources: file < env < explicit overrides.
        settings: dict[str, Any] = {}
        settings.update(load_config_file(config_file, environment.value))
        settings.update(_env_var_settings())
        settings.update({k: v for k, v in overrides.items() if v is not None})
        if fred_api_key is not None:
            settings["fred_api_key"] = fred_api_key
        settings = _coerce_settings(settings)

        # Resolve the API key, falling back to the Databricks secret scope.
        scope = settings.get("secret_scope", "fred")
        secret_key = settings.get("secret_key", "api_key")
        key = settings.get("fred_api_key")
        if not key and dbutils is not None:
            try:
                key = dbutils.secrets.get(scope=scope, key=secret_key)
            except (AttributeError, KeyError, RuntimeError):  # pragma: no cover
                key = None
        settings["fred_api_key"] = key or ""

        # Additional source keys fall back to the same secret scope, keyed by the
        # config field name (e.g. secrets/<scope>/eia_api_key — matching
        # resources/source_jobs.yml). Optional: a missing secret leaves the key
        # empty; the source's client only raises if such a series is run.
        for field_name in (
            "bls_api_key",
            "eia_api_key",
            "bea_api_key",
            "census_api_key",
            "stooq_api_key",
            "tiingo_api_key",
        ):
            if not settings.get(field_name) and dbutils is not None:
                try:
                    settings[field_name] = dbutils.secrets.get(
                        scope=scope, key=field_name
                    )
                except (AttributeError, KeyError, RuntimeError):  # pragma: no cover
                    continue

        # Only pass recognized fields to the (frozen) dataclass.
        kwargs = {k: v for k, v in settings.items() if k in _SETTING_FIELDS}
        return cls(environment=environment, **kwargs)
