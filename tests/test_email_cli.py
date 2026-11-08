"""Explicit OAuth bootstrap never occurs on a disabled alert config."""

from fred_pipeline import cli
import pytest


def test_auth_is_explicit_and_respects_disabled_alerting(tmp_path, monkeypatch, capsys):
    alerting = tmp_path / "alerts.yml"
    alerting.write_text("""enabled: false
default_profile: personal
profiles:
  personal:
    from_address: person@outlook.com
    transport:
      kind: microsoft_graph
      auth: delegated
      tenant_id: consumers
      client_id: client
      token_cache_path_env: OUTLOOK_CACHE
""")
    called = []
    monkeypatch.setattr(
        "fred_pipeline.governance.email_oauth.delegated_token",
        lambda *_a, **kw: called.append(kw),
    )
    args = [
        "alerts",
        "auth",
        "--profile",
        "personal",
        "--alerting-file",
        str(alerting),
        "--config",
        "/nonexistent",
    ]
    assert cli.main(args) == 1
    assert not called
    alerting.write_text(alerting.read_text().replace("enabled: false", "enabled: true"))
    assert cli.main(args) == 0
    assert called[0]["interactive"] is True
    assert "token" not in capsys.readouterr().out.lower()


def test_test_command_previews_by_default_and_requires_explicit_send(tmp_path, capsys):
    alerting = tmp_path / "alerts.yml"
    alerting.write_text(f"""enabled: true
default_profile: file
profiles:
  file:
    from_address: sender@example.com
    transport: {{kind: file, output_dir: {tmp_path / "sent"}}}
routes: {{failure: {{to: production@example.com}}}}
""")
    args = [
        "alerts",
        "test",
        "--profile",
        "file",
        "--to",
        "test@example.com",
        "--alerting-file",
        str(alerting),
        "--config",
        "/nonexistent",
    ]
    assert cli.main(args) == 0
    assert not (tmp_path / "sent").exists()
    assert "test@example.com" in capsys.readouterr().out
    assert cli.main([*args, "--send"]) == 0
    assert list((tmp_path / "sent").glob("*.html"))


def test_check_rejects_missing_secret_without_network(tmp_path, monkeypatch, capsys):
    alerting = tmp_path / "alerts.yml"
    alerting.write_text("""enabled: true
default_profile: smtp
profiles:
  smtp:
    from_address: sender@example.com
    transport:
      kind: smtp
      host: smtp.example.com
      auth: password
      username: sender@example.com
      password_env: MISSING_TEST_PASSWORD
""")
    monkeypatch.delenv("MISSING_TEST_PASSWORD", raising=False)
    monkeypatch.setattr(
        "smtplib.SMTP",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            AssertionError("unexpected connection")
        ),
    )
    assert (
        cli.main(
            [
                "alerts",
                "check",
                "--profile",
                "smtp",
                "--alerting-file",
                str(alerting),
                "--config",
                "/nonexistent",
            ]
        )
        == 1
    )
    assert "MISSING_TEST_PASSWORD" in capsys.readouterr().err


def test_check_rejects_cache_for_a_different_account(tmp_path, monkeypatch, capsys):
    import json

    pytest.importorskip("msal")
    cache_dir = tmp_path / "private"
    cache_dir.mkdir(mode=0o700)
    cache = cache_dir / "cache.json"
    cache.write_text(
        json.dumps(
            {
                "Account": {
                    "entry": {
                        "username": "other@outlook.com",
                        "home_account_id": "id",
                        "environment": "login.microsoftonline.com",
                    }
                }
            }
        )
    )
    cache.chmod(0o600)
    monkeypatch.setenv("OUTLOOK_CACHE", str(cache))
    alerting = tmp_path / "alerts.yml"
    alerting.write_text("""enabled: true
default_profile: personal
profiles:
  personal:
    from_address: person@outlook.com
    transport:
      kind: microsoft_graph
      auth: delegated
      tenant_id: consumers
      client_id: client
      token_cache_path_env: OUTLOOK_CACHE
""")
    assert (
        cli.main(
            [
                "alerts",
                "check",
                "--profile",
                "personal",
                "--alerting-file",
                str(alerting),
                "--config",
                "/nonexistent",
            ]
        )
        == 1
    )
    assert "account" in capsys.readouterr().err.lower()


def test_invalid_alert_yaml_returns_config_error_without_echoing_content(
    tmp_path, capsys
):
    path = tmp_path / "broken.yml"
    path.write_text("profiles: [secret-value\n")
    assert (
        cli.main(["alerts", "check", "--profile", "x", "--alerting-file", str(path)])
        == 2
    )
    assert "secret-value" not in capsys.readouterr().err
