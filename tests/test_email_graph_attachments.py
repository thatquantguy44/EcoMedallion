"""App-only Graph messages retain attachments and never echo server bodies."""

import base64

import pytest
import requests

from fred_pipeline.governance.alerting_config import TransportConfig
from fred_pipeline.governance.email_transport import (
    EmailMessageSpec,
    EmailSendError,
    MicrosoftGraphTransport,
)


def test_graph_sends_attachments_and_sanitizes_response(monkeypatch):
    requests_seen = []

    class Response:
        def __init__(self, code, data=None):
            self.status_code = code
            self.text = "sensitive response body"
            self.data = data or {}

        def json(self):
            return self.data

    def post(url, **kwargs):
        requests_seen.append((url, kwargs))
        return (
            Response(200, {"access_token": "sensitive-token"})
            if "/token" in url
            else Response(400)
        )

    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setenv("GRAPH_SECRET", "sensitive-secret")
    transport = MicrosoftGraphTransport(
        TransportConfig(
            kind="microsoft_graph",
            tenant_id="tenant",
            client_id="client",
            client_secret_env="GRAPH_SECRET",
        )
    )
    message = EmailMessageSpec(
        subject="test",
        text_body="text",
        html_body="<p>text</p>",
        to=["ops@example.com"],
        from_address="sender@example.com",
        attachments=[("run.json", b"{}", "application/json")],
    )
    with pytest.raises(EmailSendError) as exc:
        transport.send(message)
    assert "sensitive" not in str(exc.value)
    attachment = requests_seen[1][1]["json"]["message"]["attachments"][0]
    assert attachment["@odata.type"] == "#microsoft.graph.fileAttachment"
    assert base64.b64decode(attachment["contentBytes"]) == b"{}"
