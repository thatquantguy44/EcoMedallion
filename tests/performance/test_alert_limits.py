"""Large failures remain bounded while rendering useful, escaped diagnostics."""

from dataclasses import replace
from types import SimpleNamespace

from fred_pipeline.governance.email_report import (
    build_summary,
    render_html,
    render_text,
)


def test_large_failure_lists_are_truncated_in_both_email_parts(performance_summary):
    summary = replace(
        performance_summary("partial"),
        duration_seconds=3665,
        performance=None,
        failures=[(f"SERIES_{index}", "<untrusted>") for index in range(10)],
        dq_failures=[("DQ_EXAMPLE", "<unsafe check>")],
    )
    text = render_text(summary, max_failures=3)
    html = render_html(summary, max_failures=3)
    for rendered in (text, html):
        assert "SERIES_0" in rendered and "SERIES_2" in rendered
        assert "SERIES_9" not in rendered
        assert "7 more" in rendered
        assert "1h 1m" in rendered
    assert "&lt;untrusted&gt;" in html and "<untrusted>" not in html
    assert "DATA-QUALITY FAILURES" in text and "DQ_EXAMPLE" in text


def test_summary_adapter_accepts_string_statuses_and_caps_retained_failure_details(
    performance_fixture,
):
    performance = performance_fixture("partial")["performance"]
    run = SimpleNamespace(
        run_id="adapter-fixture",
        environment="test",
        series_total=12,
        series_failed=12,
        series_succeeded=0,
        duration_seconds=999,
        series_runs=[
            SimpleNamespace(
                series_id=f"SERIES_{index}",
                status="failed",
                error_message="fixture error",
                dq_passed=False,
            )
            for index in range(12)
        ],
    )
    summary = build_summary(
        run,
        [],
        verdict="failure",
        reason="fixture failure",
        performance=performance,
        max_failures=2,
    )
    assert summary.duration_seconds == 120
    assert len(summary.failures) == len(summary.dq_failures) == 4
    assert summary.series_failed == 12
    assert summary.started_at.isoformat() == performance["started_at"]
    assert summary.ended_at.isoformat() == performance["ended_at"]
