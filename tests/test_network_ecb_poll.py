"""Managed routing for ECB MID polling entry points."""

import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import pytest
import requests

from fred_pipeline import cli
from fred_pipeline.ecb_eligible_assets import SignatureResult
from tests.test_ecb_eligible_assets import _fixture_client, _verified


@pytest.fixture(autouse=True)
def clean_network_environment(monkeypatch):
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
        "FRED_CONFIG_FILE",
        "FRED_API_KEY",
        "FRED_BASE_URL",
        "ECB_MID_BASE_URL",
        "FRED_RATE_LIMIT_PER_MINUTE",
        "FRED_MAX_RETRIES",
    ):
        monkeypatch.delenv(name, raising=False)


def _rss(release_url):
    return f"""<rss><channel><item>
      <title>List of eligible marketable assets</title>
      <link>{release_url}</link>
      <pubDate>Mon, 01 Jan 2024 17:56:07 +0100</pubDate>
      </item></channel></rss>""".encode()


def _config(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(
        "default:\n  network:\n    mode: auto\n  ecb_mid_base_url: https://example\n"
    )
    monkeypatch.setenv("FRED_HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("FRED_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("FRED_MAX_RETRIES", "0")
    return path


def _install_fixture_transport(
    monkeypatch, observations_payload, *, release_override=None, verifier=_verified
):
    import fred_pipeline.ecb_eligible_assets as ecb

    fixture, release_url, _ = _fixture_client()
    payloads = {
        "https://example/rss/mid.xml": _rss(release_override or release_url),
        **fixture.releases,
        **fixture.artifacts,
    }
    probes, sends, events = [], [], []

    def probe(url, route, _settings):
        probes.append((urlsplit(url).hostname, route.kind))
        events.append("probe:" + urlsplit(url).hostname)

    def send(_adapter, request, **kwargs):
        url = request.url.split("?", 1)[0]
        sends.append((url, kwargs.get("proxies", {})))
        response = requests.Response()
        response.status_code = 200
        response._content = (
            json.dumps(observations_payload).encode()
            if urlsplit(url).hostname == "api.stlouisfed.org"
            else payloads[url]
        )
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr("fred_pipeline.network._probe", probe)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    original = ecb.ECBEligibleAssetsPipeline
    monkeypatch.setattr(
        ecb,
        "ECBEligibleAssetsPipeline",
        lambda warehouse, *, client: original(
            warehouse, client=client, signature_verifier=verifier
        ),
    )
    monkeypatch.setattr(
        cli, "_send_ecb_assets_health_alert", lambda *_args, **_kwargs: True
    )
    return probes, sends, events


def test_standalone_poll_selects_once_and_fetches_signed_artifacts(
    tmp_path, monkeypatch, capsys, observations_payload
):
    config = _config(tmp_path, monkeypatch)
    probes, sends, _ = _install_fixture_transport(monkeypatch, observations_payload)
    database = tmp_path / "ecb.db"
    result = cli.main(
        [
            "ecb-assets",
            "poll",
            "--local",
            "--db-path",
            str(database),
            "--config",
            str(config),
        ]
    )
    assert result == 0
    assert json.loads(capsys.readouterr().out)[0]["status"] == "accepted"
    assert probes == [("example", "proxy")]
    assert len(sends) == 7  # RSS, release, and five attachments
    assert all(
        proxies.get("https") == "http://proxy.example:8080" for _, proxies in sends
    )
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM silver_ecb_eligible_asset_version"
            ).fetchone()[0]
            == 2
        )


def test_poll_sends_one_alert_from_the_shared_service(
    tmp_path, monkeypatch, capsys, observations_payload
):
    import fred_pipeline.ecb_execution as execution

    config = _config(tmp_path, monkeypatch)
    _install_fixture_transport(monkeypatch, observations_payload)
    alerts = []
    monkeypatch.setattr(
        cli, "_send_ecb_assets_health_alert", lambda *_a, **kw: alerts.append(kw)
    )
    original = execution.execute_ecb_poll

    def checked(*args, **kwargs):
        report = original(*args, **kwargs)
        assert len(alerts) == 1
        assert alerts[0]["results"][0]["status"] == "accepted"
        return report

    monkeypatch.setattr(execution, "execute_ecb_poll", checked)
    assert (
        cli.main(
            [
                "ecb-assets",
                "poll",
                "--local",
                "--db-path",
                str(tmp_path / "ecb.db"),
                "--config",
                str(config),
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert len(alerts) == 1


def test_poll_failure_sends_one_alert_from_the_shared_service(
    tmp_path, monkeypatch, capsys, observations_payload
):
    config = _config(tmp_path, monkeypatch)
    _install_fixture_transport(
        monkeypatch,
        observations_payload,
        verifier=lambda *_: SignatureResult(
            False, "digest_mismatch", detail="tampered"
        ),
    )
    alerts = []
    monkeypatch.setattr(
        cli, "_send_ecb_assets_health_alert", lambda *_a, **kw: alerts.append(kw)
    )
    assert (
        cli.main(
            [
                "ecb-assets",
                "poll",
                "--local",
                "--db-path",
                str(tmp_path / "ecb.db"),
                "--config",
                str(config),
            ]
        )
        == 1
    )
    assert "signature failed" in capsys.readouterr().err
    assert len(alerts) == 1
    assert "signature failed" in alerts[0]["error"]


def test_main_run_selects_ecb_route_before_metadata_and_reuses_it_for_hook(
    tmp_path, monkeypatch, capsys, observations_payload
):
    from fred_pipeline.io.local_store import LocalWarehouse

    config = _config(tmp_path, monkeypatch)
    monkeypatch.setenv("FRED_API_KEY", "key")
    probes, sends, events = _install_fixture_transport(
        monkeypatch, observations_payload
    )
    original_sync = LocalWarehouse.sync_meta

    def sync(warehouse, manifests):
        events.append("sync_meta")
        return original_sync(warehouse, manifests)

    monkeypatch.setattr(LocalWarehouse, "sync_meta", sync)
    database = tmp_path / "run.db"
    result = cli.main(
        [
            "run",
            "--local",
            "--db-path",
            str(database),
            "--config",
            str(config),
            "--manifests",
            "manifests/rates.yml",
            "--series",
            "DGS10",
            "--no-gold",
        ]
    )
    assert result == 0
    assert '"ecb_assets"' in capsys.readouterr().out
    assert sorted(probes) == [("api.stlouisfed.org", "proxy"), ("example", "proxy")]
    assert events.index("probe:example") < events.index("sync_meta")
    assert len([url for url, _ in sends if urlsplit(url).hostname == "example"]) == 7
    assert all(
        proxies.get("https") == "http://proxy.example:8080" for _, proxies in sends
    )
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM silver_ecb_eligible_asset_version"
            ).fetchone()[0]
            == 2
        )


def test_main_poll_sends_one_alert_from_the_shared_service(
    tmp_path, monkeypatch, capsys, observations_payload
):
    import fred_pipeline.ecb_execution as execution

    config = _config(tmp_path, monkeypatch)
    monkeypatch.setenv("FRED_API_KEY", "key")
    _install_fixture_transport(monkeypatch, observations_payload)
    alerts = []
    monkeypatch.setattr(
        cli, "_send_ecb_assets_health_alert", lambda *_a, **kw: alerts.append(kw)
    )
    original = execution.execute_ecb_poll

    def checked(*args, **kwargs):
        report = original(*args, **kwargs)
        assert len(alerts) == 1
        assert alerts[0]["results"][0]["status"] == "accepted"
        return report

    monkeypatch.setattr(execution, "execute_ecb_poll", checked)
    assert (
        cli.main(
            [
                "run",
                "--local",
                "--db-path",
                str(tmp_path / "run.db"),
                "--config",
                str(config),
                "--manifests",
                "manifests/rates.yml",
                "--series",
                "DGS10",
                "--no-gold",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert len(alerts) == 1


def test_standalone_preflight_failure_sends_alert_and_downloads_nothing(
    tmp_path, monkeypatch, capsys
):
    config = _config(tmp_path, monkeypatch)
    events = []
    monkeypatch.setattr(
        "fred_pipeline.network._probe",
        lambda *_args: (_ for _ in ()).throw(requests.ConnectionError("offline")),
    )
    monkeypatch.setattr(
        requests.adapters.HTTPAdapter,
        "send",
        lambda *_args, **_kw: pytest.fail("unexpected request"),
    )
    monkeypatch.setattr(
        cli, "_send_ecb_assets_health_alert", lambda *_a, **kw: events.append(kw)
    )
    database = tmp_path / "ecb.db"
    assert (
        cli.main(
            [
                "ecb-assets",
                "poll",
                "--local",
                "--db-path",
                str(database),
                "--config",
                str(config),
            ]
        )
        == 1
    )
    assert "ECB MID" in capsys.readouterr().err
    assert events and "ECB MID" in events[0]["error"]
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM bronze_ecb_eligible_asset_release"
            ).fetchone()[0]
            == 0
        )


def test_other_ecb_actions_do_not_preflight(tmp_path, monkeypatch, capsys):
    config = _config(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "fred_pipeline.network._probe", lambda *_args: pytest.fail("unexpected probe")
    )
    assert (
        cli.main(
            [
                "ecb-assets",
                "status",
                "--local",
                "--db-path",
                str(tmp_path / "db"),
                "--config",
                str(config),
            ]
        )
        == 0
    )
    assert "latest_accepted" in json.loads(capsys.readouterr().out)


def test_main_ecb_preflight_failure_preserves_observations(
    tmp_path, monkeypatch, capsys, observations_payload
):
    config = _config(tmp_path, monkeypatch)
    monkeypatch.setenv("FRED_API_KEY", "key")
    _, sends, _ = _install_fixture_transport(monkeypatch, observations_payload)
    alerts = []

    def probe(url, _route, _settings):
        if urlsplit(url).hostname == "example":
            raise requests.ConnectionError("offline")

    monkeypatch.setattr("fred_pipeline.network._probe", probe)
    monkeypatch.setattr(
        cli, "_send_ecb_assets_health_alert", lambda *_a, **kw: alerts.append(kw)
    )
    database = tmp_path / "run.db"
    assert (
        cli.main(
            [
                "run",
                "--local",
                "--db-path",
                str(database),
                "--config",
                str(config),
                "--manifests",
                "manifests/rates.yml",
                "--series",
                "DGS10",
                "--no-gold",
            ]
        )
        == 1
    )
    stdout = capsys.readouterr().out
    output, _ = json.JSONDecoder().raw_decode(stdout[stdout.index("{") :])
    assert output["ecb_assets"]["status"] == "failed"
    assert alerts and "ECB MID" in alerts[0]["error"]
    assert sends and all(
        urlsplit(url).hostname == "api.stlouisfed.org" for url, _ in sends
    )
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute("SELECT count(*) FROM silver_fred_observation").fetchone()[0]
            > 0
        )


def test_mid_feed_cannot_use_another_registered_source_origin(
    tmp_path, monkeypatch, capsys, observations_payload
):
    config = _config(tmp_path, monkeypatch)
    monkeypatch.setenv("FRED_API_KEY", "key")
    _, sends, _ = _install_fixture_transport(
        monkeypatch,
        observations_payload,
        release_override="https://api.stlouisfed.org/steal",
    )
    assert (
        cli.main(
            [
                "run",
                "--local",
                "--db-path",
                str(tmp_path / "run.db"),
                "--config",
                str(config),
                "--manifests",
                "manifests/rates.yml",
                "--series",
                "DGS10",
                "--no-gold",
            ]
        )
        == 1
    )
    stdout = capsys.readouterr().out
    output, _ = json.JSONDecoder().raw_decode(stdout[stdout.index("{") :])
    assert output["ecb_assets"]["status"] == "failed"
    assert not any(url.endswith("/steal") for url, _ in sends)


def test_main_opt_out_does_not_probe_ecb(
    tmp_path, monkeypatch, capsys, observations_payload
):
    config = _config(tmp_path, monkeypatch)
    monkeypatch.setenv("FRED_API_KEY", "key")
    probes, sends, _ = _install_fixture_transport(monkeypatch, observations_payload)
    assert (
        cli.main(
            [
                "run",
                "--local",
                "--db-path",
                str(tmp_path / "run.db"),
                "--config",
                str(config),
                "--manifests",
                "manifests/rates.yml",
                "--series",
                "DGS10",
                "--no-gold",
                "--no-ecb-assets",
            ]
        )
        == 0
    )
    assert probes == [("api.stlouisfed.org", "proxy")]
    assert all(urlsplit(url).hostname != "example" for url, _ in sends)
    assert '"status": "skipped"' in capsys.readouterr().out


def test_signature_failure_still_quarantines_managed_poll(
    tmp_path, monkeypatch, capsys, observations_payload
):
    config = _config(tmp_path, monkeypatch)
    _install_fixture_transport(
        monkeypatch,
        observations_payload,
        verifier=lambda *_args: SignatureResult(
            False, "digest_mismatch", detail="tampered"
        ),
    )
    database = tmp_path / "ecb.db"
    assert (
        cli.main(
            [
                "ecb-assets",
                "poll",
                "--local",
                "--db-path",
                str(database),
                "--config",
                str(config),
            ]
        )
        == 1
    )
    assert "signature" in capsys.readouterr().err
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM silver_ecb_eligible_asset_release"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM bronze_ecb_eligible_asset_release WHERE status = 'quarantined'"
            ).fetchone()[0]
            == 1
        )


@pytest.mark.parametrize("via_proxy", [False, True])
def test_poll_uses_actual_selected_http_route(tmp_path, monkeypatch, via_proxy, capsys):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            calls.append(("HEAD", self.path))
            self.send_response(200)
            self.end_headers()

        def do_GET(self):
            calls.append(("GET", self.path))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<rss><channel></channel></rss>")

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = f"http://127.0.0.1:{server.server_port}"
        base_url = "http://mid.example" if via_proxy else endpoint
        proxy_url = endpoint if via_proxy else "http://127.0.0.1:1"
        monkeypatch.setenv("FRED_HTTP_PROXY", proxy_url)
        monkeypatch.setenv("FRED_RATE_LIMIT_PER_MINUTE", "0")
        monkeypatch.setenv("FRED_MAX_RETRIES", "0")
        config = tmp_path / "config.yaml"
        config.write_text(
            f"default:\n  network:\n    mode: auto\n  ecb_mid_base_url: {base_url}\n"
        )
        assert (
            cli.main(
                [
                    "ecb-assets",
                    "poll",
                    "--local",
                    "--db-path",
                    str(tmp_path / "db"),
                    "--config",
                    str(config),
                ]
            )
            == 0
        )
        assert json.loads(capsys.readouterr().out) == []
        path = f"{base_url}/rss/mid.xml" if via_proxy else "/rss/mid.xml"
        assert calls == [("HEAD", path), ("GET", path)]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
