"""Replay checked-in alert cases through the real pure renderers and adapter."""

import json

import pytest

from fred_pipeline.governance.alerting import build_message
from fred_pipeline.governance.alerting_config import AlertingConfig, RouteConfig
from fred_pipeline.governance.email_report import (
    render_html,
    render_subject,
    render_text,
)
from fred_pipeline.governance.stages import overall_verdict
from fred_pipeline.performance import PerformanceCollector
from fred_pipeline.sources.fred import FredClient


@pytest.mark.parametrize(
    "name,expected",
    [("success", "success"), ("partial", "partial"), ("early_failure", "failure")],
)
def test_saved_case_verdict_and_attachment_round_trip(
    name, expected, performance_summary
):
    summary = performance_summary(name)
    verdict, _ = overall_verdict(
        summary.stages,
        series_total=summary.series_total,
        series_failed=summary.series_failed,
    )
    assert verdict == summary.verdict == expected
    config = AlertingConfig(
        enabled=True,
        policy="always",
        from_address="pipeline@example.invalid",
        routes={"default": RouteConfig(to=("operations@example.invalid",))},
        attach_run_json=True,
    )
    message = build_message(summary, config)
    assert message.subject == render_subject(summary)
    assert expected.upper() in message.subject
    assert expected.upper() in message.text_body
    assert expected.upper() in message.html_body
    attachment = json.loads(message.attachments[0][1])
    assert attachment["run_id"] == summary.run_id
    assert attachment["verdict"] == expected
    assert attachment["performance"] == summary.performance
    assert len(attachment["stages"]) == len(summary.stages)
    assert message.attachments[0][2] == "application/json"


def test_failure_case_shows_unreached_work_and_incomplete_telemetry(
    performance_summary,
):
    summary = performance_summary("early_failure")
    for rendered in (render_text(summary), render_html(summary)):
        assert "not_run" in rendered
        assert "Incomplete telemetry" in rendered
        assert "1 unfinished" in rendered
        assert "No comparable baseline" in rendered


def test_successful_case_has_one_source_row_with_real_window_and_latency(
    performance_summary,
):
    text = render_text(performance_summary("success"))
    assert text.count(" bea:") == 1
    assert "window 1m 00s" in text
    assert "p50 1.00s / p95 3.00s" in text
    assert "retry 1" in text
    assert "+20.0% vs median" in text
    assert "skipped 1" in text


@pytest.mark.parametrize("name", ["success", "partial", "early_failure"])
def test_saved_case_html_remains_safe_and_self_contained(name, performance_summary):
    rendered = render_html(performance_summary(name))
    assert '<meta charset="utf-8">' in rendered
    assert "<script" not in rendered.lower()
    assert "<img" not in rendered.lower()
    assert "<svg" not in rendered.lower()
    assert "<canvas" not in rendered.lower()
    assert "display:flex" not in rendered.replace(" ", "")
    if name != "early_failure":
        assert "&lt;script&gt;" in rendered


def test_saved_retry_case_exercises_real_transport_and_normalizer(
    tmp_path,
    manual_clock,
    performance_fixture,
    fake_session_cls,
    fake_response_cls,
):
    case = performance_fixture("http_retry")
    responses = []
    for response in case["responses"]:
        item = fake_response_cls(response["body"], status_code=response["status"])
        item.headers = response["headers"]
        responses.append(item)
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        manual_clock.advance(seconds)

    client = FredClient(
        api_key="fixture-do-not-record",
        session=fake_session_cls(responses),
        max_retries=1,
        rate_limit_per_minute=0,
        sleep=sleep,
    )
    collector = PerformanceCollector(
        clock=manual_clock, output_dir=tmp_path, detailed=True
    )
    with collector.activate():
        payload = client.get_observations("EXAMPLE")
        rows = client.normalize("EXAMPLE", payload)
    report = collector.finish("success")
    aggregate = {row["name"]: row for row in report["aggregates"]}
    assert waits == [2]
    assert [row["value"] for row in rows] == [4.25, 4.30]
    assert report["processing_seconds"] == 2
    assert aggregate["http.request"]["count"] == 2
    assert aggregate["http.request"]["failed"] == 1
    assert aggregate["http.retry_wait"]["worker_seconds"] == 2
    assert "fixture-do-not-record" not in str(report)
    assert "api.stlouisfed.org" not in str(report)
