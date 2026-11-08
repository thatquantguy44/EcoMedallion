"""Alert delivery results distinguish previews, acceptance and uncertainty."""

from datetime import datetime, timezone

from fred_pipeline.governance.alerting import send_run_alert_result
from fred_pipeline.governance.alerting_config import (
    AlertingConfig,
    RouteConfig,
    TransportConfig,
)
from fred_pipeline.governance.ecb_alerting import send_ecb_assets_alert_result
from fred_pipeline.governance.email_transport import EmailSendError
from fred_pipeline.governance.stages import RunStageTracker


class Run:
    run_id = "run-123"
    series_failed = 1
    series_total = 1
    series_succeeded = 0
    started_at = datetime.now(timezone.utc)
    ended_at = started_at
    series_runs = []


def _config():
    return AlertingConfig(
        enabled=True,
        policy="always",
        from_address="sender@example.com",
        transport=TransportConfig(kind="console"),
        routes={"default": RouteConfig(to=("ops@example.com",))},
    )


def test_console_is_previewed_and_ecb_disabled_is_distinct():
    result = send_run_alert_result(
        Run(), RunStageTracker(), config=_config(), environment="dev"
    )
    assert result.status == "previewed"
    assert result.profile_id == "legacy"
    assert result.correlation_id
    result = send_ecb_assets_alert_result(
        action="poll", config=AlertingConfig(), error="failure"
    )
    assert result.status == "disabled"


def test_ambiguous_transport_failure_is_reported_without_replay():
    class Failed:
        calls = 0

        def send(self, _message):
            self.calls += 1
            raise EmailSendError("send timed out", category="network", uncertain=True)

    sender = Failed()
    result = send_run_alert_result(
        Run(), RunStageTracker(), config=_config(), transport=sender
    )
    assert (result.status, result.error_category, sender.calls) == (
        "unknown",
        "network",
        1,
    )


def test_reused_pipeline_records_each_runs_alert(
    monkeypatch, observations_payload, fake_client_cls
):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.manifest import SeriesSpec
    from fred_pipeline.pipeline import FredPipeline

    class Capturing:
        def __init__(self):
            self.messages = []

        def send(self, message):
            self.messages.append(message)

    sender = Capturing()
    monkeypatch.setattr(
        "fred_pipeline.governance.alerting.load_alerting_config",
        lambda *_a, **_kw: _config(),
    )
    pipeline = FredPipeline(
        PipelineConfig.resolve(
            config_file="/nonexistent", fred_api_key="key", rate_limit_per_minute=0
        ),
        client=fake_client_cls({"UNRATE": observations_payload}),
        persist_audit=False,
        alert_transport=sender,
    )
    spec = SeriesSpec(series_id="UNRATE", title="Unemployment", frequency="m")
    pipeline.run([spec], build_gold_layer=False)
    first = pipeline._last_alert_result
    pipeline.run([spec], build_gold_layer=False)
    assert len(sender.messages) == 2
    assert first.status == pipeline._last_alert_result.status == "accepted"
    assert first.correlation_id != pipeline._last_alert_result.correlation_id
