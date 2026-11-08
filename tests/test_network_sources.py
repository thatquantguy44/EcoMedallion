"""The main pipeline selects managed routes per source origin."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import pytest
import requests

from fred_pipeline.cli import build_parser
from fred_pipeline.config import PipelineConfig
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.network import NetworkPreflightError, Route, RunSessions
from fred_pipeline.pipeline import SOURCE_FACTORIES, FredPipeline


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


def _spec(series_id, source):
    return SeriesSpec(
        series_id=series_id, title=series_id, frequency="m", source=source
    )


def test_mixed_sources_select_and_reuse_routes_per_origin(
    monkeypatch, observations_payload
):
    probes = []
    requests_seen = []
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda url, route, _settings: probes.append(
            (urlsplit(url).hostname, route.kind)
        ),
    )

    def send(_adapter, request, **kwargs):
        host = urlsplit(request.url).hostname
        requests_seen.append((host, kwargs.get("proxies", {})))
        payload = (
            observations_payload
            if host == "api.stlouisfed.org"
            else {
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "series": [
                        {
                            "seriesID": "BLS1",
                            "data": [{"year": "2024", "period": "M12", "value": "3"}],
                        }
                    ]
                },
            }
        )
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    config = PipelineConfig.resolve(
        fred_api_key="key",
        config_file="/nonexistent",
        rate_limit_per_minute=0,
        network={
            "mode": "auto",
            "origins": {"https://api.bls.gov": {"mode": "direct"}},
        },
    )
    result = FredPipeline(config, persist_audit=False).run(
        [_spec("FRED1", "fred"), _spec("BLS1", "bls")], build_gold_layer=False
    )
    assert result.series_succeeded == 2
    assert sorted(probes) == [
        ("api.bls.gov", "direct"),
        ("api.stlouisfed.org", "proxy"),
    ]
    assert all(
        p.get("https") == "http://proxy.example:8080"
        for h, p in requests_seen
        if h == "api.stlouisfed.org"
    )
    assert all(not p for h, p in requests_seen if h == "api.bls.gov")


def test_non_fred_required_preflight_failure_aborts_before_metadata(monkeypatch):
    calls = []

    class Warehouse:
        def sync_meta(self, _manifests):
            calls.append("sync")

    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_args: (_ for _ in ()).throw(requests.ConnectionError("offline")),
    )
    config = PipelineConfig.resolve(
        config_file="/nonexistent", network={"mode": "direct"}
    )
    pipe = FredPipeline(config, warehouse=Warehouse())
    with pytest.raises(NetworkPreflightError):
        pipe.run_from_manifest("manifests", sources=["bls"])
    assert calls == []


def test_ishares_selects_only_the_requested_holdings_origins(monkeypatch):
    probes = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda url, _route, _settings: probes.append(urlsplit(url).hostname),
    )
    config = PipelineConfig.resolve(
        config_file="/nonexistent", network={"mode": "direct"}
    )
    pipe = FredPipeline(config, persist_audit=False)
    with pipe._run_network([_spec("IVV", "ishares"), _spec("SPY", "ishares")]):
        client = pipe._client_for_source("ishares")
        assert client.source_name == "ISHARES"
    assert sorted(probes) == ["www.blackrock.com", "www.ssga.com"]


def test_cli_fomc_refresh_is_enabled_by_default_with_opt_out():
    parser = build_parser()
    assert parser.parse_args(["run"]).refresh_fomc_calendar is True
    assert (
        parser.parse_args(["run", "--refresh-fomc-calendar"]).refresh_fomc_calendar
        is True
    )
    assert (
        parser.parse_args(["run", "--no-refresh-fomc-calendar"]).refresh_fomc_calendar
        is False
    )


@pytest.mark.parametrize("source", sorted(SOURCE_FACTORIES))
def test_each_builtin_source_registers_its_actual_origin(monkeypatch, source):
    probes = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda url, _route, _settings: probes.append(urlsplit(url).hostname),
    )
    config = PipelineConfig(
        fred_api_key="key",
        eia_api_key="key",
        bea_api_key="key",
        stooq_api_key="key",
        tiingo_api_key="key",
        network={"mode": "direct"},
    )
    pipe = FredPipeline(config, persist_audit=False)
    with pipe._run_network([_spec("IVV" if source == "ishares" else "X", source)]):
        client = pipe._client_for_source(source)
        assert isinstance(client._session, RunSessions)
        expected = (
            "www.blackrock.com"
            if source == "ishares"
            else urlsplit(client.base_url).hostname
        )
        assert probes == [expected]


def test_source_and_calendar_on_same_origin_are_probed_once(monkeypatch):
    probes = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe", lambda url, *_args: probes.append(url)
    )
    config = PipelineConfig(
        fred_api_key="key",
        fred_base_url="https://www.federalreserve.gov/fred",
        network={"mode": "direct"},
    )
    pipe = FredPipeline(config, warehouse=object(), persist_audit=False)
    with pipe._run_network(
        [_spec("X", "fred")], include_calendar=True, refresh_fomc=True
    ):
        pass
    assert len(probes) == 1
    assert urlsplit(probes[0]).hostname == "www.federalreserve.gov"


def test_all_source_probes_use_one_environment_snapshot(monkeypatch):
    import fred_pipeline.pipeline as pipeline_module

    seen = []
    original = pipeline_module.select_route
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://first.example:8080")
    monkeypatch.setattr("fred_pipeline.network._probe", lambda *_args: None)

    def select(settings, url, **kwargs):
        monkeypatch.setenv("FRED_HTTPS_PROXY", "http://changed.example:8080")
        route = original(settings, url, **kwargs)
        seen.append(route.proxy_url)
        return route

    monkeypatch.setattr(pipeline_module, "select_route", select)
    config = PipelineConfig(fred_api_key="key", network={"mode": "auto"})
    pipe = FredPipeline(config, persist_audit=False)
    with pipe._run_network([_spec("X", "fred"), _spec("Y", "bls")]):
        pass
    assert seen == ["http://first.example:8080", "http://first.example:8080"]


@pytest.mark.parametrize(
    "first_kind,second_kind", [("proxy", "direct"), ("direct", "proxy")]
)
def test_registered_cross_origin_redirect_switches_route_without_proxy_header_leak(
    monkeypatch, first_kind, second_kind
):
    from fred_pipeline.network import _origin

    first = "https://one.example/path"
    second = "https://two.example/path"
    proxy = "http://user:secret@proxy.example:8080"
    routes = {
        _origin(first): Route(
            first_kind, _origin(first), proxy if first_kind == "proxy" else ""
        ),
        _origin(second): Route(
            second_kind, _origin(second), proxy if second_kind == "proxy" else ""
        ),
    }
    sent = []

    def send(_adapter, request, **kwargs):
        sent.append((request.url, dict(request.headers), kwargs.get("proxies", {})))
        response = requests.Response()
        response.status_code = 302 if len(sent) == 1 else 200
        response._content = b"ok"
        if len(sent) == 1:
            response.headers["Location"] = second
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    sessions = RunSessions(routes)
    try:
        response = sessions.get(first, headers={"Proxy-Authorization": "secret-header"})
    finally:
        sessions.close()
    assert response.status_code == 200
    assert [bool(item[2]) for item in sent] == [
        first_kind == "proxy",
        second_kind == "proxy",
    ]
    assert all("Proxy-Authorization" not in item[1] for item in sent)


def test_sec_preflight_retains_its_configured_user_agent(monkeypatch):
    headers = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda _url, route, _settings: headers.append(route.probe_headers),
    )
    config = PipelineConfig(
        sec_user_agent="research operator@example.com", network={"mode": "direct"}
    )
    with FredPipeline(config)._run_network([_spec("X", "sec")]):
        pass
    assert headers[0]["User-Agent"] == "research operator@example.com"


def test_mixed_run_uses_actual_direct_origin_and_http_proxy(
    monkeypatch, observations_payload
):
    import fred_pipeline.pipeline as pipeline_module
    from fred_pipeline.sources.bls import BLSClient

    servers = []

    def start_server(payload):
        class Handler(BaseHTTPRequestHandler):
            calls = []

            def handle_request(self):
                self.calls.append((self.command, self.path))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                if self.command == "GET":
                    self.wfile.write(json.dumps(payload).encode())

            do_HEAD = handle_request
            do_GET = handle_request

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return server, Handler

    bls_payload = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "BLS1",
                    "data": [{"year": "2024", "period": "M12", "value": "3"}],
                }
            ]
        },
    }
    try:
        origin, fred_handler = start_server(observations_payload)
        proxy, proxy_handler = start_server(bls_payload)
        monkeypatch.setenv("FRED_HTTP_PROXY", f"http://127.0.0.1:{proxy.server_port}")
        monkeypatch.setattr(
            pipeline_module,
            "BLSClient",
            lambda **kwargs: BLSClient(base_url="http://bls.example/api", **kwargs),
        )
        config = PipelineConfig.resolve(
            fred_api_key="key",
            fred_base_url=f"http://127.0.0.1:{origin.server_port}/fred",
            config_file="/nonexistent",
            rate_limit_per_minute=0,
            network={"mode": "auto", "no_proxy": [f"127.0.0.1:{origin.server_port}"]},
        )
        result = FredPipeline(config, persist_audit=False).run(
            [_spec("FRED1", "fred"), _spec("BLS1", "bls")], build_gold_layer=False
        )
        assert result.series_succeeded == 2
        assert sum(method == "HEAD" for method, _ in fred_handler.calls) == 1
        assert sum(method == "HEAD" for method, _ in proxy_handler.calls) == 1
        assert all(path.startswith("/fred") for _, path in fred_handler.calls)
        assert all(
            path.startswith("http://bls.example/") for _, path in proxy_handler.calls
        )
    finally:
        for server, thread in servers:
            server.shutdown()
            thread.join()
            server.server_close()
