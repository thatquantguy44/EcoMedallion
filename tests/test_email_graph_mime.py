"""Graph preserves MIME alternatives without exposing blind recipients."""

import base64
from dataclasses import replace
from email import policy
from email.parser import BytesParser
from types import SimpleNamespace

import pytest
import requests

from fred_pipeline.governance.alerting_config import AlertingConfig, TransportConfig
from fred_pipeline.governance.email_delivery import deliver_message
from fred_pipeline.governance.email_transport import (
    EmailMessageSpec,
    MicrosoftGraphTransport,
)


def _message(*, bcc=()):
    return EmailMessageSpec(
        subject="Résumé – alert",
        text_body="Plain text: café",
        html_body="<p>HTML: café</p>",
        to=("to@example.com",),
        cc=("cc@example.com",),
        bcc=bcc,
        from_address="sender@example.com",
        from_name="Pipeline",
        attachments=(("résumé.json", b'\x00{"ok":true}\xff', "application/json"),),
        correlation_id="a" * 32,
    )


class FakeGraph:
    def __init__(self, response=202):
        self.response = response
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if url.endswith("/token"):
            return SimpleNamespace(
                status_code=200, json=lambda: {"access_token": "secret-token"}
            )
        if isinstance(self.response, Exception):
            raise self.response
        return SimpleNamespace(
            status_code=self.response, text="sensitive provider response"
        )


def _transport(auth, http, monkeypatch):
    if auth == "delegated":
        monkeypatch.setattr(
            "fred_pipeline.governance.email_oauth.delegated_token",
            lambda *_args, **_kwargs: "delegated-token",
        )
        config = TransportConfig(
            kind="microsoft_graph",
            auth="delegated",
            tenant_id="consumers",
            client_id="client",
            token_cache_path_env="CACHE",
        )
    else:
        monkeypatch.setenv("GRAPH_SECRET", "secret")
        config = TransportConfig(
            kind="microsoft_graph",
            tenant_id="tenant",
            client_id="client",
            client_secret_env="GRAPH_SECRET",
        )
    return MicrosoftGraphTransport(config, http_client=http)


@pytest.mark.parametrize("auth", ["app_only", "delegated"])
def test_graph_without_bcc_sends_both_bodies_and_attachment_as_mime(auth, monkeypatch):
    http = FakeGraph()
    _transport(auth, http, monkeypatch).send(_message())
    url, request = http.calls[-1]
    assert url.endswith(
        "/me/sendMail"
        if auth == "delegated"
        else "/users/sender%40example.com/sendMail"
    )
    assert request["headers"]["Content-Type"] == "text/plain"
    assert request["headers"]["Authorization"] == (
        "Bearer delegated-token" if auth == "delegated" else "Bearer secret-token"
    )
    assert request["allow_redirects"] is False
    assert "json" not in request
    assert len([url for url, _ in http.calls if url.endswith("/sendMail")]) == 1
    mime = BytesParser(policy=policy.default).parsebytes(
        base64.b64decode(request["data"], validate=True)
    )
    assert mime["Bcc"] is None
    assert mime["To"] == "to@example.com"
    assert mime["Cc"] == "cc@example.com"
    assert mime["Subject"] == "Résumé – alert"
    assert "a" * 32 in mime["Message-ID"]
    assert mime["X-Fred-Correlation-Id"] == "a" * 32
    assert (
        mime.get_body(preferencelist=("plain",)).get_content().strip()
        == "Plain text: café"
    )
    assert (
        mime.get_body(preferencelist=("html",)).get_content().strip()
        == "<p>HTML: café</p>"
    )
    attachments = list(mime.iter_attachments())
    assert len(attachments) == 1
    assert attachments[0].get_filename() == "résumé.json"
    assert attachments[0].get_content_type() == "application/json"
    assert attachments[0].get_payload(decode=True) == b'\x00{"ok":true}\xff'


@pytest.mark.parametrize("auth", ["app_only", "delegated"])
def test_graph_with_bcc_uses_structured_recipients(auth, monkeypatch):
    http = FakeGraph()
    _transport(auth, http, monkeypatch).send(
        _message(bcc=("hidden@example.com", "second@example.com"))
    )
    _, request = http.calls[-1]
    assert "data" not in request
    assert request["headers"]["Authorization"] == (
        "Bearer delegated-token" if auth == "delegated" else "Bearer secret-token"
    )
    assert request["json"]["saveToSentItems"] is True
    payload = request["json"]["message"]
    assert payload["bccRecipients"] == [
        {"emailAddress": {"address": "hidden@example.com"}},
        {"emailAddress": {"address": "second@example.com"}},
    ]
    assert "hidden@example.com" not in str(payload["toRecipients"])
    assert "hidden@example.com" not in str(payload["ccRecipients"])
    assert "hidden@example.com" not in payload["body"]["content"]
    assert payload["internetMessageHeaders"] == [
        {"name": "x-fred-correlation-id", "value": "a" * 32}
    ]
    assert (
        base64.b64decode(payload["attachments"][0]["contentBytes"])
        == b'\x00{"ok":true}\xff'
    )


@pytest.mark.parametrize("bcc", [(), ("hidden@example.com",)])
@pytest.mark.parametrize("auth", ["app_only", "delegated"])
@pytest.mark.parametrize(
    "response,status,category",
    [
        (202, "accepted", ""),
        (400, "failed", "provider"),
        (503, "unknown", "provider"),
        (200, "unknown", "provider"),
        (requests.Timeout("secret detail"), "unknown", "network"),
    ],
)
def test_graph_delivery_outcomes_never_replay_and_do_not_expose_secrets(
    response, status, category, bcc, auth, monkeypatch, caplog
):
    http = FakeGraph(response)
    transport = _transport(auth, http, monkeypatch)
    result = deliver_message(
        _message(bcc=bcc), AlertingConfig(), "failure", transport=transport
    )
    assert (result.status, result.error_category) == (status, category)
    assert len([url for url, _ in http.calls if url.endswith("/sendMail")]) == 1
    assert "secret" not in caplog.text
    assert "hidden@example.com" not in caplog.text


@pytest.mark.parametrize("bcc", [(), ("hidden@example.com",)])
def test_invalid_message_fails_before_graph_send(bcc, monkeypatch, caplog):
    http = FakeGraph()
    transport = _transport("app_only", http, monkeypatch)
    message = replace(
        _message(bcc=bcc), attachments=(("bad", "sensitive", "application/json"),)
    )
    result = deliver_message(message, AlertingConfig(), "failure", transport=transport)
    assert (result.status, result.error_category) == ("failed", "message")
    assert not any(url.endswith("/sendMail") for url, _ in http.calls)
    assert "sensitive" not in caplog.text
