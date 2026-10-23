import textwrap

import pytest

from fred_pipeline.config import PipelineConfig, load_config_file

# Env vars that could leak in from the surrounding shell and skew precedence tests.
_ENV_VARS = [
    "FRED_API_KEY",
    "FRED_BASE_URL",
    "FRED_SECRET_SCOPE",
    "FRED_SECRET_KEY",
    "ECB_BASE_URL",
    "FRED_REQUEST_TIMEOUT_SECONDS",
    "FRED_MAX_RETRIES",
    "FRED_RATE_LIMIT_PER_MINUTE",
    "FRED_EXTRACT_WORKERS",
    "FRED_SOURCE_EXTRACT_WORKERS",
    "FRED_SOURCE_RATE_LIMITS",
    "FRED_RAW_VOLUME_PATH",
    "FRED_CONFIG_FILE",
    "BLS_API_KEY",
    "EIA_API_KEY",
    "BEA_API_KEY",
    "CENSUS_API_KEY",
    "SEC_USER_AGENT",
    "STOOQ_API_KEY",
    "TIINGO_API_KEY",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _write(tmp_path, text):
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(text))
    return str(p)


def test_missing_file_returns_empty(tmp_path):
    assert load_config_file(str(tmp_path / "nope.yaml")) == {}


def test_flat_layout(tmp_path):
    path = _write(
        tmp_path,
        """
        fred_api_key: from-file
        rate_limit_per_minute: 42
    """,
    )
    settings = load_config_file(path)
    assert settings["fred_api_key"] == "from-file"
    assert settings["rate_limit_per_minute"] == 42


def test_nested_layout_merges_environment(tmp_path):
    path = _write(
        tmp_path,
        """
        default:
          rate_limit_per_minute: 120
          secret_scope: fred
        environments:
          prod:
            rate_limit_per_minute: 60
    """,
    )
    dev = load_config_file(path, "dev")
    prod = load_config_file(path, "prod")
    assert dev["rate_limit_per_minute"] == 120
    assert prod["rate_limit_per_minute"] == 60  # env override wins
    assert prod["secret_scope"] == "fred"  # default carried through


def test_unknown_keys_ignored(tmp_path):
    path = _write(
        tmp_path,
        """
        fred_api_key: k
        not_a_real_setting: 1
    """,
    )
    settings = load_config_file(path)
    assert "not_a_real_setting" not in settings


def test_resolve_uses_config_file(tmp_path):
    path = _write(
        tmp_path,
        """
        fred_api_key: file-key
        rate_limit_per_minute: 30
        max_retries: 9
    """,
    )
    cfg = PipelineConfig.resolve(environment="dev", config_file=path)
    assert cfg.fred_api_key == "file-key"
    assert cfg.rate_limit_per_minute == 30
    assert cfg.max_retries == 9


def test_env_var_overrides_file(tmp_path, monkeypatch):
    path = _write(
        tmp_path,
        """
        fred_api_key: file-key
        rate_limit_per_minute: 30
        ecb_base_url: https://file-ecb.example/service
        ecb_mid_base_url: https://file-mid.example
        """,
    )
    monkeypatch.setenv("FRED_API_KEY", "env-key")
    monkeypatch.setenv("ECB_BASE_URL", "https://env-ecb.example/service")
    monkeypatch.setenv("ECB_MID_BASE_URL", "https://env-mid.example")
    monkeypatch.setenv("FRED_RATE_LIMIT_PER_MINUTE", "77")
    monkeypatch.setenv("FRED_EXTRACT_WORKERS", "12")
    monkeypatch.setenv("FRED_SOURCE_EXTRACT_WORKERS", "fred=12,tiingo=1")
    monkeypatch.setenv("FRED_SOURCE_RATE_LIMITS", "fred=60,tiingo=5")
    cfg = PipelineConfig.resolve(environment="dev", config_file=path)
    assert cfg.fred_api_key == "env-key"
    assert cfg.ecb_base_url == "https://env-ecb.example/service"
    assert cfg.ecb_mid_base_url == "https://env-mid.example"
    assert cfg.rate_limit_per_minute == 77  # coerced to int
    assert cfg.extract_workers == 12
    assert cfg.source_extract_workers == "fred=12,tiingo=1"
    assert cfg.source_rate_limits == "fred=60,tiingo=5"


def test_explicit_arg_overrides_everything(tmp_path, monkeypatch):
    path = _write(tmp_path, "fred_api_key: file-key\n")
    monkeypatch.setenv("FRED_API_KEY", "env-key")
    cfg = PipelineConfig.resolve(
        environment="dev", fred_api_key="arg-key", config_file=path
    )
    assert cfg.fred_api_key == "arg-key"


def test_fred_config_file_env_var(tmp_path, monkeypatch):
    path = _write(tmp_path, "fred_api_key: via-env-path\n")
    monkeypatch.setenv("FRED_CONFIG_FILE", path)
    cfg = PipelineConfig.resolve(environment="dev")
    assert cfg.fred_api_key == "via-env-path"


def test_secret_scope_fallback_when_no_key(tmp_path):
    path = _write(tmp_path, "secret_scope: myscope\nsecret_key: mykey\n")

    class FakeSecrets:
        def get(self, scope, key):
            if scope == "myscope" and key == "mykey":
                return "secret-key"
            raise RuntimeError("no such source secret")

    class FakeDbutils:
        secrets = FakeSecrets()

    cfg = PipelineConfig.resolve(
        environment="dev", config_file=path, dbutils=FakeDbutils()
    )
    assert cfg.fred_api_key == "secret-key"


def test_source_keys_fall_back_to_secret_scope():
    class FakeSecrets:
        def __init__(self):
            self._store = {
                ("fred", "bls_api_key"): "bls-from-secret",
                ("fred", "eia_api_key"): "eia-from-secret",
                ("fred", "stooq_api_key"): "stooq-from-secret",
            }

        def get(self, scope, key):
            return self._store[(scope, key)]  # KeyError if absent

    class FakeDbutils:
        secrets = FakeSecrets()

    cfg = PipelineConfig.resolve(
        environment="dev", config_file="nope.yaml", dbutils=FakeDbutils()
    )
    # keyed by the config field name in the default 'fred' scope
    assert cfg.bls_api_key == "bls-from-secret"
    assert cfg.eia_api_key == "eia-from-secret"
    assert cfg.stooq_api_key == "stooq-from-secret"


def test_explicit_source_key_beats_secret_scope(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "eia-from-env")

    class FakeSecrets:
        def get(self, scope, key):
            if key == "eia_api_key":
                raise AssertionError(
                    "EIA secret should not be consulted when env is set"
                )
            raise RuntimeError("no such secret")

    class FakeDbutils:
        secrets = FakeSecrets()

    cfg = PipelineConfig.resolve(
        environment="dev", config_file="nope.yaml", dbutils=FakeDbutils()
    )
    assert cfg.eia_api_key == "eia-from-env"


def test_missing_source_secret_leaves_key_empty():
    class FakeSecrets:
        def get(self, scope, key):
            raise RuntimeError("no such secret")  # simulate absent secret

    class FakeDbutils:
        secrets = FakeSecrets()

    cfg = PipelineConfig.resolve(
        environment="dev", config_file="nope.yaml", dbutils=FakeDbutils()
    )
    # non-fatal: BLS/EIA keys stay empty; a client only raises if actually used
    assert cfg.bls_api_key == "" and cfg.eia_api_key == ""


def test_defaults_when_no_file_no_env():
    cfg = PipelineConfig.resolve(environment="prod", config_file="does-not-exist.yaml")
    assert cfg.fred_api_key == ""
    assert cfg.rate_limit_per_minute == 120
    assert cfg.restate_last_n == 90
    assert cfg.catalog == "macro_prod"


def test_source_keys_from_env_reach_clients(monkeypatch):
    monkeypatch.setenv("BLS_API_KEY", "bls-secret")
    monkeypatch.setenv("EIA_API_KEY", "eia-secret")
    monkeypatch.setenv("STOOQ_API_KEY", "stooq-secret")
    cfg = PipelineConfig.resolve(environment="dev", config_file="nope.yaml")
    assert cfg.bls_api_key == "bls-secret"
    assert cfg.eia_api_key == "eia-secret"
    assert cfg.stooq_api_key == "stooq-secret"

    # and the pipeline factories feed them into the right clients
    from fred_pipeline.pipeline import _make_bls, _make_eia, _make_stooq

    assert _make_bls(cfg).api_key == "bls-secret"
    assert _make_eia(cfg).api_key == "eia-secret"
    assert _make_stooq(cfg).api_key == "stooq-secret"


def test_ecb_base_url_reaches_client(monkeypatch):
    monkeypatch.setenv("ECB_BASE_URL", "https://ecb-proxy.example/service")
    cfg = PipelineConfig.resolve(environment="dev", config_file="nope.yaml")

    from fred_pipeline.pipeline import _make_ecb

    assert _make_ecb(cfg).base_url == "https://ecb-proxy.example/service"


def test_source_keys_default_empty():
    cfg = PipelineConfig.resolve(environment="dev", config_file="nope.yaml")
    assert cfg.bls_api_key == ""
    assert cfg.eia_api_key == ""
    assert cfg.stooq_api_key == ""
    # secrets stay out of repr
    assert "bls-" not in repr(cfg) and "eia-" not in repr(cfg)


def test_restate_last_n_from_env(monkeypatch):
    monkeypatch.setenv("FRED_RESTATE_LAST_N", "15")
    cfg = PipelineConfig.resolve(environment="dev", config_file="nope.yaml")
    assert cfg.restate_last_n == 15  # coerced to int


def test_shipped_example_config_is_valid():
    # The committed template must parse and only contain known settings.
    settings = load_config_file("config/config.example.yaml", "prod")
    assert "rate_limit_per_minute" in settings
