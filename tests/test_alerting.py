"""Stage tracking and the run-summary email.

The behaviour worth protecting here is the reason the modules exist: a run can
ingest every series correctly and still fail to rebuild Gold, because
``FredPipeline.run()`` catches Gold errors and ``RunStatus`` is computed from
series outcomes alone. Before stage tracking that run reported SUCCEEDED while
every Power BI report served stale tables.

Nothing here touches a network or a mailbox: transports are injected, and the
SMTP path is exercised against a fake ``smtplib.SMTP``.
"""

from __future__ import annotations

import smtplib
from datetime import datetime, timezone

import pytest

from fred_pipeline.governance.alerting import build_message, send_run_alert
from fred_pipeline.governance.alerting_config import (
    AlertingConfig,
    AlertingConfigError,
    RouteConfig,
    TransportConfig,
    load_alerting_config,
)
from fred_pipeline.governance.email_report import (
    RunSummary,
    render_html,
    render_subject,
    render_text,
)
from fred_pipeline.governance.email_transport import (
    ConsoleEmailTransport,
    EmailMessageSpec,
    EmailSendError,
    FileEmailTransport,
    SmtpEmailTransport,
    build_transport,
    redact,
)
from fred_pipeline.governance.stages import (
    RunStageTracker,
    StageStatus,
    overall_verdict,
)


# ---- stage tracking ---------------------------------------------------------


def test_stage_records_success_with_duration_and_detail():
    tracker = RunStageTracker()
    with tracker.stage("extract") as stage:
        stage.detail["series_succeeded"] = 2800
    record = tracker.get("extract")
    assert record.status is StageStatus.SUCCEEDED
    assert record.duration_seconds is not None
    assert record.detail["series_succeeded"] == 2800


def test_stage_records_failure_and_reraises_by_default():
    tracker = RunStageTracker()
    with pytest.raises(ValueError):
        with tracker.stage("plan"):
            raise ValueError("warehouse unreachable")
    record = tracker.get("plan")
    assert record.status is StageStatus.FAILED
    assert record.error_type == "ValueError"
    assert "warehouse unreachable" in record.error_message


def test_stage_can_swallow_so_a_gold_failure_does_not_abort_the_run():
    """The Gold stage uses swallow=True: a Gold failure must be reported without
    discarding a run whose ingestion succeeded."""
    tracker = RunStageTracker()
    with tracker.stage("gold", swallow=True):
        raise RuntimeError("Delta table locked")
    assert tracker.get("gold").status is StageStatus.FAILED
    assert "Delta table locked" in tracker.get("gold").error_message


def test_declared_stages_that_never_ran_are_visible_as_not_run():
    """A phase skipped by a bad flag looks identical to success if you only
    look at the stages that reported."""
    tracker = RunStageTracker()
    with tracker.stage("plan"):
        pass
    names = {r.name: r.status for r in tracker.records()}
    assert names["extract"] is StageStatus.NOT_RUN
    assert names["gold"] is StageStatus.NOT_RUN


def test_gold_failure_is_a_run_failure_even_when_every_series_succeeded():
    """THE regression this module exists for."""
    tracker = RunStageTracker()
    for name in ("plan", "extract", "persist"):
        with tracker.stage(name):
            pass
    with tracker.stage("gold", swallow=True):
        raise RuntimeError("build_gold blew up")
    tracker.skip("release_calendar", "not requested")

    verdict, reason = overall_verdict(
        tracker.records(), series_failed=0, series_total=2800
    )
    assert verdict == "failure"
    assert "gold" in reason


def test_optional_stage_failure_is_partial_not_failure():
    tracker = RunStageTracker()
    for name in ("plan", "extract", "gold", "persist"):
        with tracker.stage(name):
            pass
    with tracker.stage("release_calendar", swallow=True):
        raise RuntimeError("FRED releases endpoint 503")
    verdict, reason = overall_verdict(tracker.records())
    assert verdict == "partial"
    assert "release_calendar" in reason


@pytest.mark.parametrize(
    "failed,total,expected",
    [
        (0, 100, "success"),
        (3, 100, "partial"),
        (100, 100, "failure"),
    ],
)
def test_series_counts_drive_the_verdict_when_stages_are_clean(failed, total, expected):
    tracker = RunStageTracker()
    for name in ("plan", "extract", "gold", "release_calendar", "persist"):
        with tracker.stage(name):
            pass
    verdict, _ = overall_verdict(
        tracker.records(), series_failed=failed, series_total=total
    )
    assert verdict == expected


# ---- config -----------------------------------------------------------------


def _write(tmp_path, text):
    path = tmp_path / "alerting.yml"
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_missing_config_disables_alerting_quietly(tmp_path):
    """Alerting is opt-in; a pipeline with no alerting file must still run."""
    config = load_alerting_config(str(tmp_path / "nope.yml"))
    assert config.enabled is False
    assert config.should_send("failure") is False


def test_repo_alerting_config_parses():
    config = load_alerting_config("config/alerting.yml")
    assert config.enabled is False  # shipped off
    assert "data-oncall@example.com" in config.route_for("failure").to


def test_inline_secret_is_rejected(tmp_path):
    """config/alerting.yml is in git. A password in it is a leaked mailbox."""
    path = _write(
        tmp_path,
        """
enabled: true
from_address: a@b.com
transport:
  kind: smtp
  host: smtp.example.com
  password: hunter2
""",
    )
    with pytest.raises(AlertingConfigError, match="inline secret"):
        load_alerting_config(path)


def test_env_interpolation_is_rejected_rather_than_silently_ignored(tmp_path):
    path = _write(
        tmp_path,
        """
enabled: true
from_address: a@b.com
transport:
  kind: smtp
  host: smtp.example.com
  username: "${SMTP_USER}"
""",
    )
    with pytest.raises(AlertingConfigError, match=r"\$\{"):
        load_alerting_config(path)


@pytest.mark.parametrize("address", ["not-an-email", "missing@domain", "@example.com"])
def test_malformed_recipient_is_rejected(tmp_path, address):
    """A typo'd address means alerts go nowhere while you believe you are
    covered -- worse than having no alerting at all."""
    # Quoted: "@example.com" is not a valid YAML token unquoted, and we want
    # the loader's validation to be what rejects it, not the YAML scanner.
    path = _write(
        tmp_path,
        f"""
enabled: true
from_address: a@b.com
routes:
  failure:
    to: ["{address}"]
""",
    )
    with pytest.raises(AlertingConfigError, match="not a valid email"):
        load_alerting_config(path)


def test_unknown_keys_are_rejected(tmp_path):
    path = _write(
        tmp_path, "enabled: true\nfrom_address: a@b.com\nrecipients: x@y.com\n"
    )
    with pytest.raises(AlertingConfigError, match="unknown top-level key"):
        load_alerting_config(path)


def test_enabled_without_from_address_is_rejected(tmp_path):
    path = _write(tmp_path, "enabled: true\n")
    with pytest.raises(AlertingConfigError, match="from_address"):
        load_alerting_config(path)


def test_transport_requires_its_own_fields(tmp_path):
    path = _write(
        tmp_path,
        """
enabled: true
from_address: a@b.com
transport:
  kind: microsoft_graph
  tenant_id: t
""",
    )
    with pytest.raises(AlertingConfigError, match="microsoft_graph"):
        load_alerting_config(path)


def test_secret_is_read_from_the_environment_not_the_file(monkeypatch):
    transport = TransportConfig(
        kind="smtp", host="h", username="u", password_env="MY_SMTP_PW"
    )
    assert transport.resolve_secret() == ""
    monkeypatch.setenv("MY_SMTP_PW", "s3cret")
    assert transport.resolve_secret() == "s3cret"


@pytest.mark.parametrize(
    "policy,verdict,expected",
    [
        ("never", "failure", False),
        ("failure", "success", False),
        ("failure", "failure", True),
        ("failure", "partial", True),
        ("always", "success", True),
    ],
)
def test_policy_controls_sending(policy, verdict, expected):
    config = AlertingConfig(
        enabled=True,
        policy=policy,
        from_address="a@b.com",
        routes={"default": RouteConfig(to=("ops@example.com",))},
    )
    assert config.should_send(verdict) is expected


def test_environment_gate(tmp_path):
    config = AlertingConfig(
        enabled=True,
        policy="always",
        from_address="a@b.com",
        environments=("prod",),
        routes={"default": RouteConfig(to=("ops@example.com",))},
    )
    assert config.should_send("failure", "prod") is True
    assert config.should_send("failure", "dev") is False


def test_no_recipients_means_no_send():
    config = AlertingConfig(enabled=True, policy="always", from_address="a@b.com")
    assert config.should_send("failure") is False


# ---- rendering --------------------------------------------------------------


def _summary(verdict="failure", reason="required stage(s) failed: gold"):
    tracker = RunStageTracker()
    for name in ("plan", "extract", "persist"):
        with tracker.stage(name) as stage:
            stage.detail["note"] = name
    with tracker.stage("gold", swallow=True):
        raise RuntimeError("Delta commit conflict")
    return RunSummary(
        run_id="abc123def456",
        environment="prod",
        verdict=verdict,
        reason=reason,
        stages=tracker.records(),
        series_total=2820,
        series_succeeded=2820,
        series_failed=0,
        duration_seconds=754.2,
        started_at=datetime(2026, 8, 13, 6, 0, tzinfo=timezone.utc),
        failures=[("BGCRRATE", "series does not exist")],
    )


def test_subject_is_actionable_on_its_own():
    subject = render_subject(_summary())
    assert "[FRED][prod]" in subject
    assert "FAILURE" in subject
    assert "gold" in subject
    assert "2820/2820 series ok" in subject  # the tell-tale of the Gold case


def test_text_body_lists_every_stage_with_the_error():
    body = render_text(_summary())
    for stage in ("plan", "extract", "gold", "persist"):
        assert stage in body
    assert "Delta commit conflict" in body
    assert "RuntimeError" in body


def test_html_escapes_error_text():
    tracker = RunStageTracker()
    with tracker.stage("gold", swallow=True):
        raise RuntimeError("<script>alert(1)</script>")
    summary = RunSummary(
        run_id="r",
        environment="dev",
        verdict="failure",
        reason="boom",
        stages=tracker.records(),
    )
    html_body = render_html(summary)
    assert "<script>alert(1)</script>" not in html_body
    assert "&lt;script&gt;" in html_body


def test_html_uses_tables_and_inline_styles_for_outlook():
    html_body = render_html(_summary())
    assert "<table" in html_body
    assert "style=" in html_body


# ---- transports -------------------------------------------------------------


class _FakeSMTP:
    instances: list["_FakeSMTP"] = []

    def __init__(self, host, port, timeout=None):
        self.host, self.port = host, port
        self.started_tls = False
        self.logged_in_as = None
        self.sent = []
        _FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def ehlo(self):
        pass

    def starttls(self, *, context=None):
        assert context is not None and context.check_hostname
        self.started_tls = True

    def login(self, user, password):
        self.logged_in_as = (user, password)

    def send_message(self, mime, from_addr=None, to_addrs=None):
        self.sent.append((mime, from_addr, tuple(to_addrs or ())))


def _message(**kw):
    defaults = dict(
        subject="[FRED][prod] FAILURE — gold",
        text_body="text",
        html_body="<p>html</p>",
        to=("ops@example.com",),
        cc=("lead@example.com",),
        bcc=("audit@example.com",),
        from_address="pipeline@example.com",
        from_name="FRED Pipeline",
    )
    defaults.update(kw)
    return EmailMessageSpec(**defaults)


def test_smtp_transport_uses_starttls_and_logs_in(monkeypatch):
    _FakeSMTP.instances.clear()
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    monkeypatch.setenv("PW", "secret")
    transport = SmtpEmailTransport(
        TransportConfig(
            kind="outlook_smtp",
            host="smtp.office365.com",
            port=587,
            username="pipeline@example.com",
            password_env="PW",
        )
    )
    transport.send(_message())
    smtp = _FakeSMTP.instances[0]
    assert (smtp.host, smtp.port) == ("smtp.office365.com", 587)
    assert smtp.started_tls
    assert smtp.logged_in_as == ("pipeline@example.com", "secret")


def test_bcc_is_an_envelope_recipient_not_a_header(monkeypatch):
    """A Bcc header would show every recipient the bcc list."""
    _FakeSMTP.instances.clear()
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    monkeypatch.setenv("PW", "secret")
    SmtpEmailTransport(
        TransportConfig(kind="smtp", host="h", username="u", password_env="PW")
    ).send(_message())
    mime, _from, to_addrs = _FakeSMTP.instances[0].sent[0]
    assert mime["Bcc"] is None
    assert "audit@example.com" in to_addrs


def test_smtp_without_the_password_env_set_fails_with_a_clear_message(monkeypatch):
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    monkeypatch.delenv("MISSING_PW", raising=False)
    transport = SmtpEmailTransport(
        TransportConfig(kind="smtp", host="h", username="u", password_env="MISSING_PW")
    )
    with pytest.raises(EmailSendError, match="MISSING_PW"):
        transport.send(_message())


def test_smtp_auth_failure_points_at_the_graph_transport(monkeypatch):
    class _AuthFail(_FakeSMTP):
        def login(self, user, password):
            raise smtplib.SMTPAuthenticationError(535, b"disabled")

    monkeypatch.setattr(smtplib, "SMTP", _AuthFail)
    monkeypatch.setenv("PW", "x")
    transport = SmtpEmailTransport(
        TransportConfig(
            kind="outlook_smtp",
            host="smtp.office365.com",
            username="u",
            password_env="PW",
        )
    )
    with pytest.raises(EmailSendError, match="microsoft_graph"):
        transport.send(_message())


def test_file_transport_writes_both_parts(tmp_path):
    FileEmailTransport(TransportConfig(kind="file", output_dir=str(tmp_path))).send(
        _message()
    )
    written = {p.suffix for p in tmp_path.iterdir()}
    assert written == {".html", ".txt"}


def test_build_transport_selects_by_kind():
    assert isinstance(
        build_transport(AlertingConfig(transport=TransportConfig(kind="console"))),
        ConsoleEmailTransport,
    )
    assert isinstance(
        build_transport(
            AlertingConfig(transport=TransportConfig(kind="outlook_smtp", host="h"))
        ),
        SmtpEmailTransport,
    )


def test_redact_never_echoes_a_credential():
    assert "hunter2" not in redact("hunter2")
    assert redact("") == "(unset)"


# ---- end to end -------------------------------------------------------------


class _CapturingTransport:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)


class _FakeRun:
    run_id = "abc123"
    environment = "prod"
    series_total = 2820
    series_succeeded = 2820
    series_failed = 0
    duration_seconds = 700.0
    started_at = datetime(2026, 8, 13, 6, 0, tzinfo=timezone.utc)
    ended_at = datetime(2026, 8, 13, 6, 12, tzinfo=timezone.utc)
    triggered_by = "databricks-job"
    manifest_path = "manifests"
    series_runs: list = []


def test_send_run_alert_reports_gold_failure_to_the_failure_route():
    tracker = RunStageTracker()
    for name in ("plan", "extract", "persist"):
        with tracker.stage(name):
            pass
    with tracker.stage("gold", swallow=True):
        raise RuntimeError("Delta commit conflict")
    tracker.skip("release_calendar", "n/a")

    transport = _CapturingTransport()
    config = AlertingConfig(
        enabled=True,
        policy="failure",
        from_address="pipeline@example.com",
        routes={"failure": RouteConfig(to=("oncall@example.com",))},
    )
    sent = send_run_alert(
        _FakeRun(), tracker, config=config, environment="prod", transport=transport
    )
    assert sent is True
    (message,) = transport.sent
    assert message.to == ("oncall@example.com",)
    assert "FAILURE" in message.subject
    # the point: series all fine, run still a failure
    assert "2820/2820 series ok" in message.subject
    assert "Delta commit conflict" in message.text_body


def test_send_run_alert_is_silent_on_a_clean_run_under_the_failure_policy():
    tracker = RunStageTracker()
    for name in ("plan", "extract", "gold", "release_calendar", "persist"):
        with tracker.stage(name):
            pass
    transport = _CapturingTransport()
    config = AlertingConfig(
        enabled=True,
        policy="failure",
        from_address="a@b.com",
        routes={"default": RouteConfig(to=("ops@example.com",))},
    )
    assert (
        send_run_alert(_FakeRun(), tracker, config=config, transport=transport) is False
    )
    assert transport.sent == []


def test_a_transport_failure_never_fails_the_run():
    class _Broken:
        def send(self, message):
            raise EmailSendError("smtp down")

    tracker = RunStageTracker()
    with tracker.stage("gold", swallow=True):
        raise RuntimeError("boom")
    config = AlertingConfig(
        enabled=True,
        policy="always",
        from_address="a@b.com",
        routes={"default": RouteConfig(to=("ops@example.com",))},
    )
    # returns False rather than raising
    assert (
        send_run_alert(_FakeRun(), tracker, config=config, transport=_Broken()) is False
    )


def test_attach_run_json_carries_the_stage_detail():
    tracker = RunStageTracker()
    with tracker.stage("extract") as stage:
        stage.detail["series_succeeded"] = 10
    config = AlertingConfig(
        enabled=True,
        policy="always",
        from_address="a@b.com",
        attach_run_json=True,
        routes={"default": RouteConfig(to=("ops@example.com",))},
    )
    summary = RunSummary(
        run_id="r1",
        environment="dev",
        verdict="success",
        reason="ok",
        stages=tracker.records(),
    )
    message = build_message(summary, config)
    (name, data, mime_type) = message.attachments[0]
    assert name.startswith("run-") and name.endswith(".json")
    assert mime_type == "application/json"
    assert b"series_succeeded" in data


# ---- wired into the real pipeline ------------------------------------------


def test_pipeline_run_reports_failure_when_gold_breaks_but_series_succeed(
    tmp_path, observations_payload, fake_client_cls
):
    """End-to-end through FredPipeline.run(), not a hand-built tracker.

    Before stage tracking this run reported RunStatus.SUCCEEDED and the webhook
    said so, while gold.* stayed stale. The audit status is unchanged (it is a
    statement about series); the ALERT now says failure, and names the stage.
    """
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse
    from fred_pipeline.manifest import SeriesSpec
    from fred_pipeline.pipeline import FredPipeline

    config = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    warehouse = LocalWarehouse(config, db_path=str(tmp_path / "alert.db"))

    def _explode():
        raise RuntimeError("Delta commit conflict")

    warehouse.build_gold = _explode  # type: ignore[method-assign]

    transport = _CapturingTransport()
    pipeline = FredPipeline(
        config,
        client=fake_client_cls({"UNRATE": observations_payload}),
        warehouse=warehouse,
        alert_transport=transport,
    )
    alerting = AlertingConfig(
        enabled=True,
        policy="failure",
        from_address="pipeline@example.com",
        routes={"failure": RouteConfig(to=("oncall@example.com",))},
    )
    import fred_pipeline.governance.alerting as alerting_mod

    original = alerting_mod.load_alerting_config
    alerting_mod.load_alerting_config = lambda *a, **k: alerting
    try:
        run = pipeline.run(
            [SeriesSpec(series_id="UNRATE", title="UNRATE", frequency="m")],
            # the release calendar needs a live FRED client; not what this
            # test is about, and its failure is an optional stage anyway
            build_gold_layer=True,
        )
    finally:
        alerting_mod.load_alerting_config = original

    # The series-level audit verdict is unchanged -- it is not wrong, it is
    # answering a different question.
    assert run.series_failed == 0
    # ... but the run alert calls it what it is.
    assert transport.sent, "no alert was sent for a failed Gold build"
    (message,) = transport.sent
    assert "FAILURE" in message.subject
    assert "gold" in message.subject
    assert "Delta commit conflict" in message.text_body

    tracker = pipeline._stage_tracker
    assert tracker.get("gold").status is StageStatus.FAILED
    assert tracker.get("extract").status is StageStatus.SUCCEEDED
    assert tracker.get("persist").status is StageStatus.SUCCEEDED
