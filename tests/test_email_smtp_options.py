"""Configurable SMTP security and delivery behavior."""

import smtplib

import pytest

from fred_pipeline.governance.alerting_config import (
    AlertingConfigError,
    load_alerting_config,
)
from fred_pipeline.governance.email_transport import (
    EmailMessageSpec,
    EmailSendError,
    SmtpEmailTransport,
)


def _config(tmp_path, transport):
    path = tmp_path / "alerting.yml"
    path.write_text(
        "enabled: true\nfrom_address: sender@example.com\ntransport:\n" + transport
    )
    return load_alerting_config(str(path)).transport


def _message():
    return EmailMessageSpec(
        subject="test",
        text_body="text",
        html_body="<p>text</p>",
        to=["ops@example.com"],
        bcc=["audit@example.com"],
        from_address="sender@example.com",
    )


def test_implicit_tls_password_delivery(tmp_path, monkeypatch):
    config = _config(
        tmp_path,
        "  kind: smtp\n  host: smtp.example.com\n  port: 465\n  tls: implicit_tls\n  auth: password\n  username: sender@example.com\n  password_env: TEST_MAIL_PASSWORD\n",
    )
    monkeypatch.setenv("TEST_MAIL_PASSWORD", "secret")
    calls = []

    class SMTP:
        def __init__(self, host, port, *, timeout, context):
            calls.append((host, port, context.check_hostname, context.verify_mode))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ehlo(self):
            pass

        def login(self, username, password):
            calls.append((username, password))

        def send_message(self, message, from_addr, to_addrs):
            assert message["Bcc"] is None
            assert "audit@example.com" in to_addrs
            return {}

    monkeypatch.setattr(smtplib, "SMTP_SSL", SMTP)
    SmtpEmailTransport(config).send(_message())
    assert calls[0][:3] == ("smtp.example.com", 465, True)
    assert calls[1] == ("sender@example.com", "secret")


@pytest.mark.parametrize(
    "options",
    [
        "  tls: none\n  auth: password\n  username: sender@example.com\n  password_env: PW\n",
        "  use_starttls: false\n  username: sender@example.com\n  password_env: PW\n",
        "  tls: starttls\n  use_starttls: true\n",
        "  auth: password\n  username: sender@example.com\n",
    ],
)
def test_unsafe_or_ambiguous_settings_are_rejected(tmp_path, options):
    with pytest.raises(AlertingConfigError):
        _config(tmp_path, "  kind: smtp\n  host: smtp.example.com\n" + options)


def test_partial_recipient_refusal_is_not_success(tmp_path, monkeypatch):
    config = _config(
        tmp_path, "  kind: smtp\n  host: smtp.example.com\n  tls: none\n  auth: none\n"
    )

    class SMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ehlo(self):
            pass

        def send_message(self, *_args, **_kwargs):
            return {"audit@example.com": (550, b"recipient refused")}

    monkeypatch.setattr(smtplib, "SMTP", SMTP)
    with pytest.raises(EmailSendError, match="recipient"):
        SmtpEmailTransport(config).send(_message())


def test_smtp_timeout_during_send_reports_unknown_without_replay(tmp_path, monkeypatch):
    config = _config(
        tmp_path, "  kind: smtp\n  host: smtp.example.com\n  tls: none\n  auth: none\n"
    )
    calls = []

    class SMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def ehlo(self):
            pass

        def send_message(self, *_args, **_kwargs):
            calls.append("send")
            raise TimeoutError("potentially accepted")

    monkeypatch.setattr(smtplib, "SMTP", SMTP)
    with pytest.raises(EmailSendError) as exc:
        SmtpEmailTransport(config).send(_message())
    assert exc.value.uncertain and calls == ["send"]
