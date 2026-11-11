"""Performance reports must remain useful without images or JavaScript."""

import json

from fred_pipeline.governance.email_report import RunSummary, render_html, render_text
from fred_pipeline.governance.alerting import build_message
from fred_pipeline.governance.alerting_config import AlertingConfig, RouteConfig


def report():
    return {
        "processing_seconds": 120,
        "dropped_events": 0,
        "telemetry_errors": [],
        "stages": [
            {
                "name": "extract",
                "status": "succeeded",
                "offset_seconds": 10,
                "duration_seconds": 80,
            }
        ],
        "aggregates": [
            {
                "name": "http.request",
                "source": "bea",
                "count": 5,
                "failed": 1,
                "worker_seconds": 10,
                "p50_seconds": 1,
                "p95_seconds": 3,
                "sample_count": 5,
                "sampled": False,
                "counters": {},
            },
            {
                "name": "http.retry_wait",
                "source": "bea",
                "count": 1,
                "worker_seconds": 2,
                "counters": {"retries": 1},
            },
            {
                "name": "http.rate_limit",
                "source": "bea",
                "count": 5,
                "worker_seconds": 7,
                "counters": {},
            },
            {
                "name": "extract.normalize",
                "source": "bea",
                "count": 2,
                "worker_seconds": 1,
                "counters": {"rows": 100},
            },
        ],
        "slowest": [
            {
                "name": "gold.engine",
                "status": "succeeded",
                "duration_seconds": 20,
                "attributes": {"table": "<script>alert(1)</script>"},
            }
        ],
        "baseline": {
            "count": 3,
            "median_seconds": 100,
            "previous_seconds": [90, 100, 110],
        },
    }


def summary(performance=None):
    return RunSummary(
        run_id="test-run",
        environment="dev",
        verdict="success",
        reason="all done",
        stages=[],
        series_total=2,
        series_succeeded=2,
        performance=performance or report(),
    )


def test_email_has_timeline_sources_percentiles_regressions_and_escaped_slow_operations():
    rendered = render_html(summary())
    for label in (
        "Performance",
        "Timeline",
        "Source performance",
        "p95",
        "Slowest operations",
        "Recent comparable runs",
        "+20.0%",
        "100",
    ):
        assert label in rendered
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert "<svg" not in rendered and "<canvas" not in rendered
    assert "width=" in rendered and "style=" in rendered
    text = render_text(summary())
    for label in ("PERFORMANCE", "bea", "p95", "retry", "worker", "+20.0%"):
        assert label in text


def test_missing_baseline_and_partial_telemetry_are_explicit():
    performance = report()
    performance.update(
        baseline={"count": 1}, dropped_events=8, telemetry_errors=["storage"]
    )
    for rendered in (
        render_text(summary(performance)),
        render_html(summary(performance)),
    ):
        assert "No comparable baseline" in rendered
        assert "Incomplete telemetry" in rendered
        assert "8" in rendered and "storage" in rendered


def test_json_attachment_includes_performance_data():
    config = AlertingConfig(
        enabled=True,
        from_address="pipeline@example.invalid",
        routes={"default": RouteConfig(to=("test@example.invalid",))},
        attach_run_json=True,
    )
    message = build_message(summary(), config)
    assert message is not None
    payload = json.loads(message.attachments[0][1])
    assert payload["performance"]["processing_seconds"] == 120


def test_delivery_timings_are_separate_from_processing_timeline():
    performance = report()
    performance["stages"].append(
        {
            "name": "report.delivery",
            "status": "succeeded",
            "offset_seconds": 120,
            "duration_seconds": 40,
        }
    )
    performance["aggregates"].append(
        {
            "name": "report.delivery",
            "source": "",
            "count": 1,
            "worker_seconds": 40,
            "counters": {},
        }
    )
    text = render_text(summary(performance))
    assert "report.delivery" not in text.partition("After processing")[0]
    assert "report.delivery" in text.partition("After processing")[2]
    assert '<meta charset="utf-8">' in render_html(summary(performance))
