"""SMTP route selection happens once, before credentials or message data."""

import smtplib
import select
import shutil
import socket
import socketserver
import ssl
import subprocess
import threading

import pytest

from fred_pipeline.governance.alerting_config import load_alerting_config
from fred_pipeline.governance.email_transport import (
    EmailMessageSpec,
    EmailSendError,
    SmtpEmailTransport,
)
from fred_pipeline.governance.smtp_network import plan_smtp_route


def _transport(tmp_path):
    path = tmp_path / "alerting.yml"
    path.write_text("""enabled: true
from_address: sender@example.com
transport:
  kind: smtp
  host: smtp.example.com
  port: 25
  tls: none
  auth: none
  network:
    mode: auto
    preference: proxy_first
    proxy_url_env: SMTP_PROXY
""")
    return load_alerting_config(str(path)).transport


def test_smtp_auto_falls_back_at_preflight_and_reuses_route(tmp_path, monkeypatch):
    transport = _transport(tmp_path)
    monkeypatch.setenv("SMTP_PROXY", "http://proxy.example:8080")
    events = []

    def connect(config, route):
        events.append(("connect", route.kind))
        if route.kind == "proxy":
            raise ConnectionRefusedError()

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_a):
                pass

            def ehlo(self):
                events.append(("ehlo", route.kind))

            def send_message(self, *_a, **_kw):
                events.append(("message", route.kind))
                return {}

        return Connection()

    monkeypatch.setattr(
        "fred_pipeline.governance.smtp_network.smtp_connection", connect
    )
    route = plan_smtp_route(transport)
    assert events == [("connect", "proxy"), ("connect", "direct"), ("ehlo", "direct")]
    SmtpEmailTransport(transport, smtp_route=route).send(
        EmailMessageSpec(
            subject="x",
            text_body="x",
            html_body="x",
            to=["ops@example.com"],
            from_address="sender@example.com",
        )
    )
    assert events[-3:] == [
        ("connect", "direct"),
        ("ehlo", "direct"),
        ("message", "direct"),
    ]


def test_strict_proxy_policy_does_not_fallback(tmp_path, monkeypatch):
    from fred_pipeline.governance.smtp_network import SmtpProxyPolicyError

    transport = _transport(tmp_path)
    monkeypatch.setenv("SMTP_PROXY", "http://proxy.example:8080")
    events = []

    def blocked(config, route):
        events.append(route.kind)
        raise SmtpProxyPolicyError("proxy rejected CONNECT")

    monkeypatch.setattr(
        "fred_pipeline.governance.smtp_network.smtp_connection", blocked
    )
    with pytest.raises(EmailSendError, match="policy"):
        plan_smtp_route(transport)
    assert events == ["proxy"]


def test_smtp_tls_failure_never_triggers_direct_fallback(tmp_path, monkeypatch):
    transport = _transport(tmp_path)
    monkeypatch.setenv("SMTP_PROXY", "http://proxy.example:8080")
    events = []

    def invalid_tls(_config, route):
        events.append(route.kind)
        raise ssl.SSLError("certificate failure")

    monkeypatch.setattr(
        "fred_pipeline.governance.smtp_network.smtp_connection", invalid_tls
    )
    with pytest.raises(EmailSendError, match="TLS"):
        plan_smtp_route(transport)
    assert events == ["proxy"]


def test_disconnected_proxy_is_treated_as_connectivity_failure(tmp_path, monkeypatch):
    transport = _transport(tmp_path)
    monkeypatch.setenv("SMTP_PROXY", "http://proxy.example:8080")
    events = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ehlo(self):
            return (250, b"ok")

    def connect(_config, route):
        events.append(route.kind)
        if route.kind == "proxy":
            raise smtplib.SMTPServerDisconnected("connection lost")
        return Connection()

    monkeypatch.setattr(
        "fred_pipeline.governance.smtp_network.smtp_connection", connect
    )
    assert plan_smtp_route(transport).kind == "direct"
    assert events == ["proxy", "direct"]


def test_direct_first_can_fall_back_to_proxy(tmp_path, monkeypatch):
    from dataclasses import replace

    original = _transport(tmp_path)
    transport = replace(
        original, network=replace(original.network, preference="direct_first")
    )
    monkeypatch.setenv("SMTP_PROXY", "http://proxy.example:8080")
    events = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ehlo(self):
            return (250, b"ok")

    def connect(_config, route):
        events.append(route.kind)
        if route.kind == "direct":
            raise ConnectionRefusedError()
        return Connection()

    monkeypatch.setattr(
        "fred_pipeline.governance.smtp_network.smtp_connection", connect
    )
    assert plan_smtp_route(transport).kind == "proxy"
    assert events == ["direct", "proxy"]


def test_smtp_bypass_stays_direct_even_in_proxy_mode(tmp_path, monkeypatch):
    from dataclasses import replace

    original = _transport(tmp_path)
    transport = replace(
        original,
        network=replace(original.network, mode="proxy", no_proxy=(".example.com",)),
    )
    monkeypatch.setenv("SMTP_PROXY", "http://proxy.example:8080")
    events = []

    def connect(_config, route):
        events.append(route.kind)
        raise ConnectionRefusedError()

    monkeypatch.setattr(
        "fred_pipeline.governance.smtp_network.smtp_connection", connect
    )
    with pytest.raises(EmailSendError):
        plan_smtp_route(transport)
    assert events == ["direct"]


def test_smtp_managed_config_cannot_send_without_startup_plan(tmp_path, monkeypatch):
    transport = _transport(tmp_path)
    monkeypatch.setattr(
        smtplib, "SMTP", lambda *_a, **_kw: pytest.fail("unexpected connection")
    )
    with pytest.raises(EmailSendError, match="preflight"):
        SmtpEmailTransport(transport).send(
            EmailMessageSpec(
                subject="x",
                text_body="x",
                html_body="x",
                to=["ops@example.com"],
                from_address="sender@example.com",
            )
        )


def test_smtp_network_rejects_non_string_mode(tmp_path):
    from fred_pipeline.governance.alerting_config import AlertingConfigError

    path = tmp_path / "alerting.yml"
    path.write_text(
        "from_address: sender@example.com\ntransport:\n  kind: smtp\n  host: smtp.example.com\n  network: {mode: [auto]}\n"
    )
    with pytest.raises(AlertingConfigError, match="mode"):
        load_alerting_config(str(path))


def test_pipeline_selects_smtp_route_once_even_in_http_env_mode(tmp_path, monkeypatch):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.governance.smtp_network import SmtpRoute
    from fred_pipeline.pipeline import FredPipeline

    path = tmp_path / "alerting.yml"
    path.write_text("""enabled: true
from_address: sender@example.com
transport:
  kind: smtp
  host: smtp.example.com
  tls: none
  network: {mode: auto, proxy_url_env: SMTP_PROXY}
routes: {failure: {to: ops@example.com}, partial: {to: ops@example.com}}
""")
    monkeypatch.setenv("FRED_ALERTING_FILE", str(path))
    events = []
    monkeypatch.setattr(
        "fred_pipeline.governance.smtp_network.plan_smtp_route",
        lambda cfg: events.append(cfg) or SmtpRoute("direct"),
    )
    pipeline = FredPipeline(
        PipelineConfig.resolve(config_file="/nonexistent"), persist_audit=False
    )
    with pipeline._run_network([]):
        assert len(events) == 1
        assert list(pipeline._smtp_routes.values())[0].kind == "direct"
    assert pipeline._smtp_routes is None


def test_real_connect_proxy_preflights_without_sending_message(tmp_path, monkeypatch):
    events = []

    class SmtpHandler(socketserver.StreamRequestHandler):
        def handle(self):
            self.wfile.write(b"220 smtp.test ready\r\n")
            while line := self.rfile.readline():
                command = line.decode("ascii", "replace").strip()
                events.append(command)
                if command.upper().startswith("EHLO"):
                    self.wfile.write(b"250 smtp.test\r\n")
                elif command.upper().startswith(("MAIL FROM", "RCPT TO")):
                    self.wfile.write(b"250 ok\r\n")
                elif command.upper() == "DATA":
                    self.wfile.write(b"354 send data\r\n")
                    while self.rfile.readline() != b".\r\n":
                        pass
                    self.wfile.write(b"250 accepted\r\n")
                elif command.upper() == "QUIT":
                    self.wfile.write(b"221 bye\r\n")
                    return
                else:
                    self.wfile.write(b"250 ok\r\n")

    class ProxyHandler(socketserver.StreamRequestHandler):
        def handle(self):
            first = self.rfile.readline().decode("ascii").strip()
            events.append(first)
            while self.rfile.readline() not in (b"\r\n", b""):
                pass
            with socket.create_connection(smtp.server_address, timeout=2) as upstream:
                self.wfile.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                self.wfile.flush()
                while True:
                    readable, _, _ = select.select(
                        [self.connection, upstream], [], [], 2
                    )
                    if not readable:
                        return
                    for source in readable:
                        data = source.recv(8192)
                        if not data:
                            return
                        (
                            upstream if source is self.connection else self.connection
                        ).sendall(data)

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    smtp = Server(("127.0.0.1", 0), SmtpHandler)
    proxy = Server(("127.0.0.1", 0), ProxyHandler)
    threads = [
        threading.Thread(target=s.serve_forever, daemon=True) for s in (smtp, proxy)
    ]
    for thread in threads:
        thread.start()
    try:
        monkeypatch.setenv("SMTP_PROXY", f"http://127.0.0.1:{proxy.server_address[1]}")
        path = tmp_path / "alerts.yml"
        path.write_text(f"""enabled: true
from_address: sender@example.com
transport:
  kind: smtp
  host: smtp.example.com
  port: {smtp.server_address[1]}
  tls: none
  auth: none
  network: {{mode: proxy, proxy_url_env: SMTP_PROXY}}
""")
        config = load_alerting_config(str(path)).transport
        route = plan_smtp_route(config)
        assert not any(e.upper().startswith(("MAIL", "RCPT", "DATA")) for e in events)
        SmtpEmailTransport(config, smtp_route=route).send(
            EmailMessageSpec(
                subject="test",
                text_body="text",
                html_body="<p>text</p>",
                to=["ops@example.com"],
                bcc=["audit@example.com"],
                from_address="sender@example.com",
            )
        )
        assert len([e for e in events if e.startswith("CONNECT")]) == 2
        assert len([e for e in events if e.upper().startswith("MAIL")]) == 1
        assert len([e for e in events if e.upper() == "DATA"]) == 1
        assert any(
            "audit@example.com" in e for e in events if e.upper().startswith("RCPT")
        )
    finally:
        for server in (proxy, smtp):
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()


@pytest.mark.parametrize("tls", ["starttls", "implicit_tls"])
def test_real_smtp_tls_verifies_hostname_and_custom_ca(tmp_path, monkeypatch, tls):
    if not shutil.which("openssl"):
        pytest.skip("openssl is needed to generate a local test certificate")
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    events = []

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            sock = self.request
            try:
                if tls == "implicit_tls":
                    sock = context.wrap_socket(sock, server_side=True)
                sock.sendall(b"220 smtp.test ready\r\n")
                reader = sock.makefile("rb")
                while line := reader.readline():
                    cmd = line.decode("ascii", "replace").strip().upper()
                    events.append(cmd)
                    if cmd.startswith("EHLO"):
                        sock.sendall(b"250-smtp.test\r\n250 STARTTLS\r\n")
                    elif cmd == "STARTTLS":
                        sock.sendall(b"220 begin tls\r\n")
                        reader.close()
                        sock = context.wrap_socket(sock, server_side=True)
                        reader = sock.makefile("rb")
                    elif cmd == "QUIT":
                        sock.sendall(b"221 bye\r\n")
                        return
                    else:
                        sock.sendall(b"250 ok\r\n")
            except (OSError, ssl.SSLError):
                return
            finally:
                sock.close()

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv("SMTP_CA", str(cert))
        path = tmp_path / "alerts.yml"
        path.write_text(f"""enabled: true
from_address: sender@example.com
transport:
  kind: smtp
  host: localhost
  port: {server.server_address[1]}
  tls: {tls}
  auth: none
  network: {{mode: direct, ca_bundle_env: SMTP_CA}}
""")
        config = load_alerting_config(str(path)).transport
        assert plan_smtp_route(config).kind == "direct"
        if tls == "starttls":
            assert "STARTTLS" in events
        path.write_text(path.read_text().replace("host: localhost", "host: 127.0.0.1"))
        with pytest.raises(EmailSendError, match="TLS"):
            plan_smtp_route(load_alerting_config(str(path)).transport)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
