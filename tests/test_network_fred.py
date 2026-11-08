"""Managed FRED routing is selected once per pipeline invocation."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from fred_pipeline.config import PipelineConfig
from fred_pipeline.cli import main
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.network import (
    NetworkConfigError,
    NetworkPreflightError,
    select_route,
)
from fred_pipeline.pipeline import FredPipeline


@pytest.fixture(autouse=True)
def clean_proxy_environment(monkeypatch):
    for name in (
        "FRED_NETWORK_MODE",
        "FRED_NETWORK_PREFERENCE",
        "FRED_HTTP_PROXY",
        "FRED_HTTPS_PROXY",
        "FRED_NO_PROXY",
        "FRED_CA_BUNDLE",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "REQUEST_METHOD",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
    ):
        monkeypatch.delenv(name, raising=False)


def test_config_layers_network_fields_and_rejects_unknown_settings(
    tmp_path, monkeypatch
):
    path = tmp_path / "config.yaml"
    path.write_text(
        "default:\n  network:\n    mode: auto\n    preference: direct_first\n"
        "    https_proxy_env: FRED_TEST_PROXY\n"
        "environments:\n  prod:\n    network:\n      preference: proxy_first\n"
    )
    monkeypatch.setenv("FRED_NETWORK_PREFERENCE", "direct_first")
    cfg = PipelineConfig.resolve("prod", config_file=str(path))
    assert cfg.network.mode == "auto"
    assert cfg.network.preference == "direct_first"
    assert cfg.network.https_proxy_env == "FRED_TEST_PROXY"
    path.write_text("network:\n  mode: auto\n  prefernce: direct_first\n")
    with pytest.raises(NetworkConfigError, match="prefernce"):
        PipelineConfig.resolve(config_file=str(path))


@pytest.mark.parametrize(
    ("preference", "failed_route", "expected"),
    [("proxy_first", "proxy", "direct"), ("direct_first", "direct", "proxy")],
)
def test_auto_selects_other_route_once_on_connection_failure(
    monkeypatch, preference, failed_route, expected
):
    attempts = []

    def probe(_url, route, _settings):
        attempts.append(route.kind)
        if route.kind == failed_route:
            raise requests.ConnectionError("unreachable")

    monkeypatch.setattr("fred_pipeline.network._probe", probe)
    cfg = PipelineConfig.resolve(
        config_file="/nonexistent",
        network={"mode": "auto", "preference": preference},
    )
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")
    route = select_route(cfg.network, "https://api.stlouisfed.org/fred")
    assert route.kind == expected
    assert attempts == [failed_route, expected]


@pytest.mark.parametrize(
    "error", [requests.exceptions.SSLError, requests.exceptions.ProxyError]
)
def test_security_errors_do_not_fall_back(monkeypatch, error):
    attempts = []

    def probe(_url, route, _settings):
        attempts.append(route.kind)
        raise error("407 proxy authentication failed")

    monkeypatch.setattr("fred_pipeline.network._probe", probe)
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://user:secret@proxy.example:8080")
    cfg = PipelineConfig.resolve(config_file="/nonexistent", network={"mode": "auto"})
    with pytest.raises(NetworkPreflightError) as exc:
        select_route(cfg.network, "https://api.stlouisfed.org/fred")
    assert attempts == ["proxy"]
    assert "secret" not in str(exc.value)


def test_bypass_is_direct_only_even_when_proxy_is_required(monkeypatch):
    attempts = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda _url, route, _settings: attempts.append(route.kind),
    )
    cfg = PipelineConfig.resolve(
        config_file="/nonexistent",
        network={"mode": "proxy", "no_proxy": [".stlouisfed.org"]},
    )
    route = select_route(cfg.network, "https://api.stlouisfed.org/fred")
    assert route.kind == "direct"
    assert attempts == ["direct"]


def test_bypass_normalizes_ipv6_literals(monkeypatch):
    monkeypatch.setattr("fred_pipeline.network._probe", lambda *_args: None)
    cfg = PipelineConfig.resolve(
        config_file="/nonexistent", network={"mode": "proxy", "no_proxy": ["::1"]}
    )
    assert select_route(cfg.network, "http://[0:0:0:0:0:0:0:1]/fred").kind == "direct"


def test_empty_run_never_probes_the_release_calendar(monkeypatch):
    monkeypatch.setattr(
        "fred_pipeline.network._probe", lambda *_args: pytest.fail("no outbound work")
    )
    cfg = PipelineConfig.resolve(
        fred_api_key="key", config_file="/nonexistent", network={"mode": "direct"}
    )
    pipe = FredPipeline(cfg, warehouse=object(), persist_audit=False)
    with pipe._run_network([], include_calendar=True):
        pass


def test_unavailable_advisory_fred_route_does_not_escape_directly(monkeypatch):
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_args: (_ for _ in ()).throw(requests.ConnectionError("offline")),
    )
    cfg = PipelineConfig.resolve(
        fred_api_key="key", config_file="/nonexistent", network={"mode": "direct"}
    )
    pipe = FredPipeline(
        cfg, warehouse=object(), clients={"bea": object()}, persist_audit=False
    )
    specs = [SeriesSpec(series_id="A", title="A", frequency="d", source="bea")]
    with pipe._run_network(specs, include_calendar=True):
        with pytest.raises(NetworkPreflightError, match="no selected route"):
            pipe._client_for_source("fred")


@pytest.mark.parametrize("mode", ["direct", "proxy"])
def test_strict_modes_do_not_try_an_alternate_route(monkeypatch, mode):
    attempts = []
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")

    def probe(_url, route, _settings):
        attempts.append(route.kind)
        raise requests.ConnectionError("offline")

    monkeypatch.setattr("fred_pipeline.network._probe", probe)
    cfg = PipelineConfig.resolve(config_file="/nonexistent", network={"mode": mode})
    with pytest.raises(NetworkPreflightError):
        select_route(cfg.network, "https://api.stlouisfed.org/fred")
    assert attempts == [mode]


def test_direct_ignores_invalid_environment_proxy(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "socks5://proxy.example:1080")
    monkeypatch.setattr("fred_pipeline.network._probe", lambda *_args: None)
    cfg = PipelineConfig.resolve(config_file="/nonexistent", network={"mode": "direct"})
    assert select_route(cfg.network, "https://api.stlouisfed.org/fred").kind == "direct"


def test_environment_precedence_cgi_and_ca_bundle(monkeypatch):
    seen = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda _url, route, _settings: seen.append(route),
    )
    monkeypatch.setenv("HTTP_PROXY", "http://unsafe.example:1")
    monkeypatch.setenv("REQUEST_METHOD", "GET")
    monkeypatch.setenv("all_proxy", "http://fallback.example:2")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/tmp/corporate-ca.pem")
    cfg = PipelineConfig.resolve(config_file="/nonexistent", network={"mode": "auto"})
    selected = select_route(cfg.network, "http://api.example/fred")
    assert selected.proxy_url == "http://fallback.example:2"
    assert selected.ca_bundle == "/tmp/corporate-ca.pem"
    assert len(seen) == 1


def test_yaml_network_secret_values_and_unsupported_schemes_are_rejected(monkeypatch):
    with pytest.raises(NetworkConfigError, match="unknown network"):
        PipelineConfig.resolve(
            config_file="/nonexistent",
            network={"mode": "proxy", "https_proxy": "http://user:pass@example"},
        )
    monkeypatch.setenv("FRED_HTTPS_PROXY", "socks5://proxy.example:1080")
    cfg = PipelineConfig.resolve(config_file="/nonexistent", network={"mode": "proxy"})
    with pytest.raises(NetworkConfigError, match="proxy"):
        select_route(cfg.network, "https://api.stlouisfed.org/fred")


def test_override_precedence_for_bypass_and_per_origin_modes(monkeypatch):
    seen = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda _url, route, _settings: seen.append(route.kind),
    )
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("FRED_NO_PROXY", "api.stlouisfed.org:443")
    cfg = PipelineConfig.resolve(
        config_file="/nonexistent",
        network={
            "mode": "auto",
            "origins": {"https://api.stlouisfed.org:443": {"mode": "proxy"}},
        },
    )
    assert select_route(cfg.network, "https://api.stlouisfed.org/fred").kind == "direct"
    assert seen == ["direct"]


def test_probe_omits_configured_url_query(monkeypatch):
    urls = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe", lambda url, *_args: urls.append(url)
    )
    cfg = PipelineConfig.resolve(config_file="/nonexistent", network={"mode": "direct"})
    select_route(
        cfg.network, "https://api.stlouisfed.org/fred?api_key=private#fragment"
    )
    assert urls == ["https://api.stlouisfed.org/fred"]


def test_managed_route_redacts_proxy_credentials_from_source_errors(monkeypatch):
    monkeypatch.setattr("fred_pipeline.network._probe", lambda *_args: None)
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://user:secret@proxy.example:8080")

    def send(*_args, **_kwargs):
        raise requests.ConnectionError("http://user:secret@proxy.example:8080")

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    cfg = PipelineConfig.resolve(
        fred_api_key="key",
        config_file="/nonexistent",
        network={"mode": "auto"},
        max_retries=0,
        rate_limit_per_minute=0,
    )
    run = FredPipeline(cfg, persist_audit=False).run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert run.series_failed == 1
    assert "secret" not in run.series_runs[0].error_message


def test_default_mode_does_not_preflight_injected_client(
    monkeypatch, fake_client_cls, observations_payload
):
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_args: pytest.fail("legacy mode must not probe"),
    )
    cfg = PipelineConfig.resolve(fred_api_key="key", config_file="/nonexistent")
    pipe = FredPipeline(
        cfg, client=fake_client_cls({"A": observations_payload}), persist_audit=False
    )
    result = pipe.run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert result.series_succeeded == 1


def test_default_mode_does_not_preflight_builtin_client(
    monkeypatch, observations_payload
):
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_args: pytest.fail("legacy mode must not probe"),
    )

    def send(_adapter, request, **_kwargs):
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(observations_payload).encode()
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    cfg = PipelineConfig.resolve(
        fred_api_key="key", config_file="/nonexistent", rate_limit_per_minute=0
    )
    result = FredPipeline(cfg, persist_audit=False).run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert result.series_succeeded == 1


def test_managed_mode_does_not_probe_injected_client(
    monkeypatch, fake_client_cls, observations_payload
):
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_args: pytest.fail("injected clients own their networking"),
    )
    cfg = PipelineConfig.resolve(
        fred_api_key="key", config_file="/nonexistent", network={"mode": "direct"}
    )
    pipe = FredPipeline(
        cfg, client=fake_client_cls({"A": observations_payload}), persist_audit=False
    )
    result = pipe.run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert result.series_succeeded == 1


def test_redirect_to_unregistered_origin_is_blocked(monkeypatch):
    calls = []
    monkeypatch.setattr("fred_pipeline.network._probe", lambda *_args: None)

    def send(_adapter, request, **_kwargs):
        calls.append(request.url)
        response = requests.Response()
        response.status_code = 302
        response.headers["Location"] = "https://other.example/steal"
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    cfg = PipelineConfig.resolve(
        fred_api_key="key",
        config_file="/nonexistent",
        network={"mode": "direct"},
        max_retries=0,
        rate_limit_per_minute=0,
    )
    result = FredPipeline(cfg, persist_audit=False).run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert result.series_failed == 1
    assert len(calls) == 1
    assert "other.example" not in calls[0]


@pytest.fixture
def http_server():
    class Handler(BaseHTTPRequestHandler):
        calls = []
        response_status = 200
        payload = {}

        def _respond(self):
            self.calls.append((self.command, self.path))
            self.send_response(self.response_status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(json.dumps(self.payload).encode())

        do_HEAD = _respond
        do_GET = _respond

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server, Handler
    server.shutdown()
    thread.join()
    server.server_close()


def test_actual_http_proxy_carries_probe_and_source_requests(
    monkeypatch, http_server, observations_payload
):
    server, handler = http_server
    handler.payload = observations_payload
    monkeypatch.setenv("FRED_HTTP_PROXY", f"http://127.0.0.1:{server.server_port}")
    cfg = PipelineConfig.resolve(
        fred_api_key="key",
        fred_base_url="http://fred.example/fred",
        config_file="/nonexistent",
        network={"mode": "auto"},
        rate_limit_per_minute=0,
    )
    result = FredPipeline(cfg, persist_audit=False).run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert result.series_succeeded == 1
    assert [method for method, _ in handler.calls].count("HEAD") == 1
    assert all(path.startswith("http://fred.example/") for _, path in handler.calls)
    assert "api_key" not in handler.calls[0][1]


def test_cli_dry_run_uses_configured_proxy(
    monkeypatch, http_server, observations_payload, capsys
):
    server, handler = http_server
    handler.payload = observations_payload
    monkeypatch.setenv("FRED_API_KEY", "key")
    monkeypatch.setenv("FRED_BASE_URL", "http://fred.example/fred")
    monkeypatch.setenv("FRED_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("FRED_NETWORK_MODE", "auto")
    monkeypatch.setenv("FRED_HTTP_PROXY", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("FRED_RATE_LIMIT_PER_MINUTE", "0")
    assert main(["run", "--dry-run", "--source", "fred", "--series", "DGS10"]) == 0
    assert '"series_succeeded": 1' in capsys.readouterr().out
    assert [method for method, _ in handler.calls].count("HEAD") == 1
    assert all(path.startswith("http://fred.example/") for _, path in handler.calls)


def test_cli_reports_required_preflight_failure(monkeypatch, capsys):
    monkeypatch.setenv("FRED_API_KEY", "key")
    monkeypatch.setenv("FRED_BASE_URL", "http://127.0.0.1:1/fred")
    monkeypatch.setenv("FRED_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("FRED_NETWORK_MODE", "direct")
    assert main(["run", "--dry-run", "--source", "fred", "--series", "DGS10"]) == 1
    assert "no permitted FRED route" in capsys.readouterr().err


def test_proxy_connection_failure_falls_back_to_actual_direct_origin(
    monkeypatch, http_server, observations_payload
):
    server, handler = http_server
    handler.payload = observations_payload
    monkeypatch.setenv("FRED_HTTP_PROXY", "http://127.0.0.1:1")
    cfg = PipelineConfig.resolve(
        fred_api_key="key",
        fred_base_url=f"http://127.0.0.1:{server.server_port}/fred",
        config_file="/nonexistent",
        network={"mode": "auto"},
        rate_limit_per_minute=0,
    )
    result = FredPipeline(cfg, persist_audit=False).run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert result.series_succeeded == 1
    assert [method for method, _ in handler.calls].count("HEAD") == 1
    assert all(path.startswith("/fred") for _, path in handler.calls)


def test_direct_connection_failure_falls_back_to_actual_http_proxy(
    monkeypatch, http_server, observations_payload
):
    server, handler = http_server
    handler.payload = observations_payload
    monkeypatch.setenv("FRED_HTTP_PROXY", f"http://127.0.0.1:{server.server_port}")
    cfg = PipelineConfig.resolve(
        fred_api_key="key",
        fred_base_url="http://127.0.0.1:1/fred",
        config_file="/nonexistent",
        network={"mode": "auto", "preference": "direct_first"},
        rate_limit_per_minute=0,
    )
    result = FredPipeline(cfg, persist_audit=False).run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert result.series_succeeded == 1
    assert [method for method, _ in handler.calls].count("HEAD") == 1
    assert all(path.startswith("http://127.0.0.1:1/") for _, path in handler.calls)


def test_origin_http_errors_never_trigger_another_route(monkeypatch, http_server):
    server, handler = http_server
    handler.response_status = 403
    monkeypatch.setenv("FRED_HTTP_PROXY", "http://127.0.0.1:1")
    cfg = PipelineConfig.resolve(
        fred_api_key="key",
        fred_base_url=f"http://127.0.0.1:{server.server_port}/fred",
        config_file="/nonexistent",
        network={"mode": "auto", "preference": "direct_first"},
        rate_limit_per_minute=0,
        max_retries=0,
    )
    run = FredPipeline(cfg, persist_audit=False).run(
        [SeriesSpec(series_id="A", title="A", frequency="d")], build_gold_layer=False
    )
    assert run.series_failed == 1
    assert [method for method, _ in handler.calls] == ["HEAD", "GET"]


def test_pipeline_selects_once_per_run_and_uses_selected_route(
    monkeypatch, observations_payload
):
    probes = []
    requests_seen = []
    closed = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda _url, route, _settings: probes.append(route.kind),
    )
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")

    def send(_adapter, request, **kwargs):
        requests_seen.append(kwargs.get("proxies"))
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(observations_payload).encode()
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    original_close = requests.Session.close

    def close(session):
        closed.append(session)
        original_close(session)

    monkeypatch.setattr("fred_pipeline.network._OriginSession.close", close)
    cfg = PipelineConfig.resolve(
        fred_api_key="key",
        config_file="/nonexistent",
        network={"mode": "auto"},
        rate_limit_per_minute=0,
        extract_workers=2,
    )
    pipe = FredPipeline(cfg, persist_audit=False)
    specs = [SeriesSpec(series_id=sid, title=sid, frequency="d") for sid in ("A", "B")]
    for _ in range(2):
        run = pipe.run(specs, build_gold_layer=False)
        assert run.series_succeeded == 2
    assert probes == ["proxy", "proxy"]
    assert len(requests_seen) >= 4
    assert all(
        item.get("https") == "http://proxy.example:8080" for item in requests_seen
    )
    assert len(closed) >= 2
    assert pipe._run_sessions is None


def test_preflight_failure_precedes_manifest_metadata_writes(monkeypatch):
    calls = []

    class Warehouse:
        def sync_meta(self, _manifests):
            calls.append("sync")

    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_args: (_ for _ in ()).throw(requests.ConnectionError("offline")),
    )
    cfg = PipelineConfig.resolve(
        fred_api_key="key", config_file="/nonexistent", network={"mode": "direct"}
    )
    pipe = FredPipeline(cfg, warehouse=Warehouse())
    with pytest.raises(NetworkPreflightError):
        pipe.run_from_manifest("manifests", sources=["fred"])
    assert calls == []
