"""Managed pipeline plans include email HTTP targets only when enabled."""

from urllib.parse import urlsplit

import requests

from fred_pipeline.config import PipelineConfig
from fred_pipeline.governance.alerting_config import load_alerting_config
from fred_pipeline.governance.email_transport import EmailMessageSpec, build_transport
from fred_pipeline.pipeline import FredPipeline


def test_graph_routes_are_selected_once_and_restricted(monkeypatch, tmp_path):
    config_path = tmp_path / "alerting.yml"
    config_path.write_text("""enabled: true
policy: failure
from_address: sender@example.com
transport:
  kind: microsoft_graph
  tenant_id: tenant
  client_id: client
  client_secret_env: GRAPH_SECRET
routes:
  failure: {to: ops@example.com}
""")
    monkeypatch.setenv("FRED_ALERTING_FILE", str(config_path))
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")
    probes = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda url, route, _settings: probes.append(
            (urlsplit(url).hostname, route.kind)
        ),
    )
    config = PipelineConfig.resolve(
        config_file="/nonexistent", network={"mode": "auto"}
    )
    pipeline = FredPipeline(config, persist_audit=False)
    with pipeline._run_network([]):
        assert sorted(probes) == [
            ("graph.microsoft.com", "proxy"),
            ("login.microsoftonline.com", "proxy"),
        ]
        assert pipeline._alert_sessions is not None
        assert len(pipeline._alert_sessions._routes) == 2
    assert pipeline._alert_sessions is None


def test_disabled_alerts_do_not_trigger_email_preflight(monkeypatch):
    monkeypatch.setenv("FRED_ALERTING_FILE", "/nonexistent")
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_a: (_ for _ in ()).throw(AssertionError("unexpected probe")),
    )
    config = PipelineConfig.resolve(
        config_file="/nonexistent", network={"mode": "auto"}
    )
    with FredPipeline(config, persist_audit=False)._run_network([]):
        pass


def test_graph_token_and_send_reuse_startup_proxy(monkeypatch, tmp_path):
    path = tmp_path / "alerts.yml"
    path.write_text("""enabled: true
from_address: sender@example.com
transport: {kind: microsoft_graph, tenant_id: tenant, client_id: client, client_secret_env: GRAPH_SECRET}
routes: {failure: {to: ops@example.com}}
""")
    monkeypatch.setenv("FRED_ALERTING_FILE", str(path))
    monkeypatch.setenv("GRAPH_SECRET", "secret")
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")
    probes, calls = [], []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda url, *_a: probes.append(urlsplit(url).hostname),
    )

    def send(_adapter, request, **kwargs):
        calls.append((urlsplit(request.url).hostname, kwargs["proxies"]))
        response = requests.Response()
        response.status_code = (
            200 if "login.microsoftonline.com" in request.url else 202
        )
        response._content = b'{"access_token":"private-token"}'
        response.url, response.request = request.url, request
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    pipeline = FredPipeline(
        PipelineConfig.resolve(config_file="/nonexistent", network={"mode": "auto"}),
        persist_audit=False,
    )
    message = EmailMessageSpec(
        subject="test",
        text_body="x",
        html_body="x",
        from_address="sender@example.com",
        to=["ops@example.com"],
    )
    with pipeline._run_network([]):
        build_transport(
            load_alerting_config(str(path)),
            verdict="failure",
            http_client=pipeline._alert_sessions,
        ).send(message)
    assert sorted(probes) == ["graph.microsoft.com", "login.microsoftonline.com"]
    assert [host for host, _ in calls] == [
        "login.microsoftonline.com",
        "graph.microsoft.com",
    ]
    assert all(proxies["https"] == "http://proxy.example:8080" for _, proxies in calls)
