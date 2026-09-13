"""Postgres target/DSN resolution shared by read and write backends.

The write-side ``PostgresWarehouse`` is still Spec004 work, but both that
backend and the read-only ``PostgresConnection`` need the same decision: is
this a local Postgres instance or a managed/service database?
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

DEFAULT_LOCAL_POSTGRES_DSN = "postgresql://fred:fred@localhost:55432/macro_medallion"

LOCAL_DSN_ENVS = ("FRED_POSTGRES_LOCAL_DSN", "FRED_POSTGRES_DSN")
SERVICE_DSN_ENVS = ("FRED_POSTGRES_SERVICE_DSN", "DATABASE_URL", "FRED_POSTGRES_DSN")

_TARGET_ALIASES = {
    "local": "local",
    "localhost": "local",
    "docker": "local",
    "compose": "local",
    "service": "service",
    "managed": "service",
    "remote": "service",
    "cloud": "service",
    "production": "service",
    "prod": "service",
}


@dataclass(frozen=True)
class PostgresSettings:
    """Resolved Postgres connection settings."""

    target: str
    dsn: str


def resolve_postgres_settings(
    config: Mapping[str, Any] | None = None,
) -> PostgresSettings:
    """Resolve Postgres target and DSN from backend config + environment.

    Supported config fields:
    - ``target`` or ``mode``: ``local`` or ``service`` (default: ``local``)
    - ``dsn``: explicit connection string
    - ``dsn_env``: environment variable name containing the connection string
    - discrete fields: ``host``, ``port``, ``database``/``dbname``, ``user``,
      ``password``

    Resolution intentionally keeps secrets out of tracked YAML. Local mode has
    a harmless localhost default; service mode must be configured explicitly.
    """
    cfg = dict(config or {})
    target = _normalize_target(cfg.get("target") or cfg.get("mode") or "local")

    dsn = _clean(cfg.get("dsn"))
    if dsn:
        return PostgresSettings(target=target, dsn=dsn)

    dsn_env = _clean(cfg.get("dsn_env"))
    if dsn_env:
        env_dsn = _clean(os.environ.get(dsn_env))
        if env_dsn:
            return PostgresSettings(target=target, dsn=env_dsn)

    field_dsn = _dsn_from_fields(cfg, target)
    if field_dsn:
        return PostgresSettings(target=target, dsn=field_dsn)

    for env_name in LOCAL_DSN_ENVS if target == "local" else SERVICE_DSN_ENVS:
        env_dsn = _clean(os.environ.get(env_name))
        if env_dsn:
            return PostgresSettings(target=target, dsn=env_dsn)

    if target == "local":
        return PostgresSettings(target=target, dsn=DEFAULT_LOCAL_POSTGRES_DSN)

    raise ValueError(
        "Postgres target 'service' requires a DSN. Set dsn, dsn_env, "
        "FRED_POSTGRES_SERVICE_DSN, DATABASE_URL, or FRED_POSTGRES_DSN."
    )


def redact_postgres_dsn(dsn: str) -> str:
    """Redact the password part of a Postgres DSN for logs/errors."""
    try:
        parts = urlsplit(dsn)
    except ValueError:
        return "<invalid-postgres-dsn>"

    if not parts.password:
        return dsn

    user = parts.username or ""
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    netloc = f"{user}:***@{host}{port}" if user else f"***@{host}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _normalize_target(value: Any) -> str:
    raw = _clean(value).lower()
    target = _TARGET_ALIASES.get(raw)
    if not target:
        valid = ", ".join(sorted(set(_TARGET_ALIASES.values())))
        raise ValueError(f"Unknown Postgres target {value!r}; expected one of: {valid}")
    return target


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.startswith("${") and text.endswith("}") and len(text) > 3:
        return os.environ.get(text[2:-1], "").strip()
    return text


def _dsn_from_fields(cfg: Mapping[str, Any], target: str) -> str:
    field_names = {"host", "port", "database", "dbname", "user", "password"}
    if not any(_clean(cfg.get(name)) for name in field_names):
        return ""

    host = _clean(cfg.get("host")) or ("localhost" if target == "local" else "")
    port = _clean(cfg.get("port")) or ("55432" if target == "local" else "5432")
    database = (
        _clean(cfg.get("database"))
        or _clean(cfg.get("dbname"))
        or ("macro_medallion" if target == "local" else "")
    )
    user = _clean(cfg.get("user")) or ("fred" if target == "local" else "")
    password = _clean(cfg.get("password")) or ("fred" if target == "local" else "")

    missing = [
        name
        for name, value in (("host", host), ("database", database), ("user", user))
        if not value
    ]
    if missing:
        raise ValueError(
            "Postgres discrete-field configuration is missing: " + ", ".join(missing)
        )

    auth = quote(user, safe="")
    if password:
        auth += f":{quote(password, safe='')}"
    return f"postgresql://{auth}@{host}:{port}/{quote(database, safe='')}"
