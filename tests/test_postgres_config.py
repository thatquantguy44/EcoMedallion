import pytest

from fred_pipeline.io.postgres_config import (
    DEFAULT_LOCAL_POSTGRES_DSN,
    redact_postgres_dsn,
    resolve_postgres_settings,
)


@pytest.fixture(autouse=True)
def clear_postgres_env(monkeypatch):
    for name in (
        "FRED_POSTGRES_LOCAL_DSN",
        "FRED_POSTGRES_SERVICE_DSN",
        "FRED_POSTGRES_DSN",
        "DATABASE_URL",
        "PG_TEST_DSN",
        "PG_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)


def test_local_target_defaults_to_localhost_dsn():
    settings = resolve_postgres_settings({"target": "local"})

    assert settings.target == "local"
    assert settings.dsn == DEFAULT_LOCAL_POSTGRES_DSN


def test_local_target_can_use_target_specific_env(monkeypatch):
    monkeypatch.setenv(
        "FRED_POSTGRES_LOCAL_DSN",
        "postgresql://fred:secret@localhost:5544/fred_test",
    )

    settings = resolve_postgres_settings({"target": "local"})

    assert settings.dsn == "postgresql://fred:secret@localhost:5544/fred_test"


def test_service_target_requires_configured_dsn():
    with pytest.raises(ValueError, match="target 'service' requires a DSN"):
        resolve_postgres_settings({"target": "service"})


def test_service_target_can_use_database_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://svc:secret@db.example.com/prod")

    settings = resolve_postgres_settings({"target": "service"})

    assert settings.target == "service"
    assert settings.dsn == "postgresql://svc:secret@db.example.com/prod"


def test_explicit_dsn_env_overrides_target_defaults(monkeypatch):
    monkeypatch.setenv("PG_TEST_DSN", "postgresql://u:p@localhost:5433/custom")

    settings = resolve_postgres_settings({"target": "local", "dsn_env": "PG_TEST_DSN"})

    assert settings.dsn == "postgresql://u:p@localhost:5433/custom"


def test_discrete_fields_build_a_dsn(monkeypatch):
    monkeypatch.setenv("PG_PASSWORD", "p ass")

    settings = resolve_postgres_settings(
        {
            "target": "service",
            "host": "db.example.com",
            "database": "macro prod",
            "user": "macro user",
            "password": "${PG_PASSWORD}",
        }
    )

    assert settings.dsn == (
        "postgresql://macro%20user:p%20ass@db.example.com:5432/macro%20prod"
    )


def test_invalid_target_is_rejected():
    with pytest.raises(ValueError, match="Unknown Postgres target"):
        resolve_postgres_settings({"target": "sqlite"})


def test_redact_postgres_dsn_hides_password():
    assert (
        redact_postgres_dsn("postgresql://user:secret@localhost:5432/fred")
        == "postgresql://user:***@localhost:5432/fred"
    )
