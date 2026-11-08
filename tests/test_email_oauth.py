"""Delegated Microsoft mail: secure cache, account binding and XOAUTH2."""

import os
import json
import stat
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

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


def _profile(tmp_path, monkeypatch, kind="microsoft_graph"):
    cache_dir = tmp_path / "private"
    cache_dir.mkdir(mode=0o700)
    monkeypatch.setenv("TEST_OAUTH_CACHE", str(cache_dir / "tokens.json"))
    path = tmp_path / "alerting.yml"
    path.write_text(f"""enabled: true
default_profile: personal
profiles:
  personal:
    from_address: person@outlook.com
    transport:
      kind: {kind}
      auth: delegated
      client_id: public-client
      tenant_id: consumers
      token_cache_path_env: TEST_OAUTH_CACHE
routes:
  failure: {{to: ops@example.com}}
""")
    return load_alerting_config(str(path)).profile_for("failure")


class FakeCache:
    def __init__(self):
        self.has_state_changed = False

    def deserialize(self, text):
        self.has_state_changed = False

    def serialize(self):
        return '{"credential":"private"}'


class FakeApp:
    accounts = [{"username": "person@outlook.com"}]
    token = {"access_token": "access-value"}
    created = []
    scopes = []

    def __init__(self, client_id, **kwargs):
        self.cache = kwargs["token_cache"]
        FakeApp.created.append(kwargs)

    def get_accounts(self, username=None):
        return self.accounts

    def acquire_token_silent(self, scopes, account):
        self.scopes.append(scopes)
        self.cache.has_state_changed = True
        return self.token

    def acquire_token_interactive(self, scopes, **kwargs):
        self.cache.has_state_changed = True
        return self.token


@pytest.fixture
def fake_msal(monkeypatch):
    FakeApp.accounts = [{"username": "person@outlook.com"}]
    FakeApp.token = {"access_token": "access-value"}
    FakeApp.created = []
    FakeApp.scopes = []
    monkeypatch.setitem(
        sys.modules,
        "msal",
        SimpleNamespace(
            SerializableTokenCache=FakeCache, PublicClientApplication=FakeApp
        ),
    )


def test_oauth_refresh_uses_private_atomic_cache_and_bound_account(
    tmp_path, monkeypatch, fake_msal
):
    from fred_pipeline.governance.email_oauth import delegated_token

    profile = _profile(tmp_path, monkeypatch)
    http = object()
    assert (
        delegated_token(profile.transport, profile.from_address, http_client=http)
        == "access-value"
    )
    cache = tmp_path / "private" / "tokens.json"
    assert stat.S_IMODE(cache.stat().st_mode) == 0o600
    assert FakeApp.created[0]["http_client"].inner is http
    assert FakeApp.scopes == [["https://graph.microsoft.com/Mail.Send"]]
    assert '"credential"' in cache.read_text()


def test_oauth_rejects_wrong_account_or_unsafe_cache(tmp_path, monkeypatch, fake_msal):
    from fred_pipeline.governance.email_oauth import delegated_token

    profile = _profile(tmp_path, monkeypatch)
    FakeApp.accounts = [{"username": "other@outlook.com"}]
    with pytest.raises(EmailSendError, match="account"):
        delegated_token(profile.transport, profile.from_address)
    os.chmod(tmp_path / "private", 0o755)
    with pytest.raises(EmailSendError, match="private"):
        delegated_token(profile.transport, profile.from_address)


def test_revoked_consent_requires_explicit_reauthentication(
    tmp_path, monkeypatch, fake_msal
):
    from fred_pipeline.governance.email_oauth import delegated_token

    profile = _profile(tmp_path, monkeypatch)
    FakeApp.token = {
        "error": "interaction_required",
        "error_description": "secret details",
    }
    with pytest.raises(EmailSendError, match="auth") as exc:
        delegated_token(profile.transport, profile.from_address)
    assert "secret details" not in str(exc.value)


def test_smtp_oauth_requests_a_distinct_outlook_scope(tmp_path, monkeypatch, fake_msal):
    from dataclasses import replace
    from fred_pipeline.governance.email_oauth import delegated_token

    profile = _profile(tmp_path, monkeypatch)
    config = replace(
        profile.transport,
        kind="outlook_com_smtp",
        host="smtp-mail.outlook.com",
        username=profile.from_address,
        auth="oauth2",
    )
    assert delegated_token(config, profile.from_address) == "access-value"
    assert FakeApp.scopes == [["https://outlook.office.com/SMTP.Send"]]


def test_outlook_smtp_preset_requires_oauth_and_uses_documented_endpoint(
    tmp_path, monkeypatch
):
    cache_dir = tmp_path / "private"
    cache_dir.mkdir(mode=0o700)
    monkeypatch.setenv("TEST_OAUTH_CACHE", str(cache_dir / "tokens.json"))
    path = tmp_path / "alerts.yml"
    path.write_text("""enabled: true
from_address: person@outlook.com
transport:
  kind: outlook_com_smtp
  username: person@outlook.com
  client_id: public-client
  token_cache_path_env: TEST_OAUTH_CACHE
""")
    cfg = load_alerting_config(str(path)).transport
    assert (cfg.host, cfg.port, cfg.smtp_tls, cfg.smtp_auth) == (
        "smtp-mail.outlook.com",
        587,
        "starttls",
        "oauth2",
    )
    monkeypatch.setattr(
        "fred_pipeline.governance.email_oauth.delegated_token",
        lambda *_a, **_kw: "access-value",
    )
    seen = []

    class SMTP:
        def __init__(self, *_a, **_kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            pass

        def ehlo(self):
            pass

        def starttls(self, **_kw):
            pass

        def auth(self, mechanism, authobject):
            seen.append((mechanism, authobject()))

        def send_message(self, *_a, **_kw):
            return {}

    monkeypatch.setattr("smtplib.SMTP", SMTP)
    message = EmailMessageSpec(
        subject="test",
        text_body="x",
        html_body="x",
        to=["ops@example.com"],
        from_address="person@outlook.com",
    )
    SmtpEmailTransport(cfg).send(message)
    assert seen == [
        ("XOAUTH2", "user=person@outlook.com\x01auth=Bearer access-value\x01\x01")
    ]


def test_delegated_graph_requires_named_cache(tmp_path):
    path = tmp_path / "alerts.yml"
    path.write_text("""enabled: true
from_address: person@outlook.com
transport: {kind: microsoft_graph, auth: delegated, tenant_id: consumers, client_id: client}
""")
    with pytest.raises(AlertingConfigError, match="token_cache_path_env"):
        load_alerting_config(str(path))


def test_smtp_oauth_rejects_sender_username_mismatch_at_load(tmp_path):
    path = tmp_path / "alerts.yml"
    path.write_text("""enabled: true
from_address: person@outlook.com
transport:
  kind: outlook_com_smtp
  username: other@outlook.com
  client_id: client
  token_cache_path_env: CACHE_FILE
""")
    with pytest.raises(AlertingConfigError, match="sender"):
        load_alerting_config(str(path))


def test_delegated_graph_uses_me_and_the_managed_http_client(
    tmp_path, monkeypatch, fake_msal
):
    from fred_pipeline.governance.email_transport import MicrosoftGraphTransport

    profile = _profile(tmp_path, monkeypatch)
    sent = []

    class Http:
        def post(self, url, **kwargs):
            sent.append((url, kwargs))
            return SimpleNamespace(status_code=202)

    http = Http()
    message = EmailMessageSpec(
        subject="test",
        text_body="x",
        html_body="x",
        to=["ops@example.com"],
        from_address=profile.from_address,
    )
    MicrosoftGraphTransport(profile.transport, http_client=http).send(message)
    assert sent[0][0] == "https://graph.microsoft.com/v1.0/me/sendMail"
    assert sent[0][1]["headers"]["Authorization"] == "Bearer access-value"
    assert FakeApp.created[0]["http_client"].inner is http


def test_oauth_cache_rejects_symlink_file(tmp_path, monkeypatch, fake_msal):
    from fred_pipeline.governance.email_oauth import delegated_token

    profile = _profile(tmp_path, monkeypatch)
    target = tmp_path / "target"
    target.write_text("private value")
    (tmp_path / "private" / "tokens.json").symlink_to(target)
    with pytest.raises(EmailSendError):
        delegated_token(profile.transport, profile.from_address)
    assert target.read_text() == "private value"


def test_oauth_cache_serializes_concurrent_writers(tmp_path, monkeypatch, fake_msal):
    from fred_pipeline.governance.email_oauth import _locked_cache

    class CountingCache:
        def __init__(self):
            self.value = 0
            self.has_state_changed = False

        def deserialize(self, text):
            self.value = json.loads(text)["value"]

        def serialize(self):
            return json.dumps({"value": self.value})

    monkeypatch.setitem(
        sys.modules, "msal", SimpleNamespace(SerializableTokenCache=CountingCache)
    )
    profile = _profile(tmp_path, monkeypatch)

    def update(_):
        with _locked_cache(profile.transport) as (cache, _path):
            cache.value += 1
            cache.has_state_changed = True

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(update, range(12)))
    assert json.loads((tmp_path / "private" / "tokens.json").read_text())["value"] == 12


def test_identity_client_blocks_cross_origin_and_redirects():
    from fred_pipeline.governance.email_oauth import _IdentityHttpClient

    seen = []

    class Http:
        def post(self, url, **kwargs):
            seen.append((url, kwargs))
            return None

    client = _IdentityHttpClient(Http())
    client.post(
        "https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
        allow_redirects=True,
    )
    assert seen[0][1]["allow_redirects"] is False
    with pytest.raises(EmailSendError, match="authority"):
        client.post("https://graph.microsoft.com/token")
    with pytest.raises(EmailSendError, match="authority"):
        client.post("https://user@login.microsoftonline.com/token")


def test_basic_alerting_does_not_require_msal(tmp_path, monkeypatch):
    from fred_pipeline.governance.email_oauth import delegated_token
    from fred_pipeline.governance.email_transport import ConsoleEmailTransport

    profile = _profile(tmp_path, monkeypatch)
    monkeypatch.setitem(sys.modules, "msal", None)
    ConsoleEmailTransport().send(
        EmailMessageSpec(
            subject="test", text_body="x", html_body="x", to=["ops@example.com"]
        )
    )
    with pytest.raises(EmailSendError, match="email-oauth"):
        delegated_token(profile.transport, profile.from_address)


def test_real_msal_uses_injected_identity_client_without_network(tmp_path, monkeypatch):
    pytest.importorskip("msal")
    from fred_pipeline.governance.email_oauth import delegated_token

    profile = _profile(tmp_path, monkeypatch)
    seen = []

    class Http:
        def get(self, url, **kwargs):
            seen.append((url, kwargs))
            if "/discovery/instance" in url:
                return SimpleNamespace(
                    status_code=200,
                    text=json.dumps(
                        {
                            "metadata": [
                                {
                                    "aliases": ["login.microsoftonline.com"],
                                    "preferred_network": "login.microsoftonline.com",
                                    "preferred_cache": "login.microsoftonline.com",
                                }
                            ]
                        }
                    ),
                )
            return SimpleNamespace(
                status_code=200,
                text=json.dumps(
                    {
                        "authorization_endpoint": "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize",
                        "token_endpoint": "https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
                        "issuer": "https://login.microsoftonline.com/consumers/v2.0",
                    }
                ),
            )

    with pytest.raises(EmailSendError, match="account"):
        delegated_token(profile.transport, profile.from_address, http_client=Http())
    assert seen and all(item[1]["allow_redirects"] is False for item in seen)
