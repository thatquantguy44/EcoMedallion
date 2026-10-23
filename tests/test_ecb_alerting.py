from datetime import datetime, timezone

import pytest

from fred_pipeline.governance.alerting_config import (
    AlertingConfig,
    ECBAssetsAlertConfig,
    RouteConfig,
)
from fred_pipeline.governance.ecb_alerting import (
    assess_ecb_assets,
    expected_release_date,
    send_ecb_assets_alert,
)


LATEST = {
    "release_id": "release-one",
    "publication_at_utc": "2026-10-22T15:56:07+00:00",
    "coverage_status": "complete",
    "reconciliation_status": "matched",
}


def _status(**overrides):
    value = {
        "latest_accepted": dict(LATEST),
        "current_eligible_count": 31_331,
        "quarantined": [],
    }
    value.update(overrides)
    return value


def test_expected_release_date_respects_cutoff_weekends_and_target_holidays():
    settings = ECBAssetsAlertConfig(grace_period_minutes=180)
    # Friday before the 21:15 Frankfurt cutoff still expects Thursday.
    assert (
        expected_release_date(
            datetime(2026, 10, 23, 18, 0, tzinfo=timezone.utc), settings
        ).isoformat()
        == "2026-10-22"
    )
    # Saturday rolls back to Friday.
    assert (
        expected_release_date(
            datetime(2026, 10, 24, 20, 0, tzinfo=timezone.utc), settings
        ).isoformat()
        == "2026-10-23"
    )
    # Good Friday is a TARGET closing day, so Thursday remains expected.
    assert (
        expected_release_date(
            datetime(2026, 4, 3, 21, 0, tzinfo=timezone.utc), settings
        ).isoformat()
        == "2026-04-02"
    )


def test_fresh_feed_without_a_new_item_is_success():
    assessment = assess_ecb_assets(
        action="poll",
        status=_status(),
        now=datetime(2026, 10, 23, 2, 0, tzinfo=timezone.utc),
    )
    assert assessment.verdict == "success"
    assert "no new release" in assessment.reason


def test_late_feed_is_partial_and_names_expected_date():
    assessment = assess_ecb_assets(
        action="poll",
        status=_status(),
        now=datetime(2026, 10, 23, 21, 30, tzinfo=timezone.utc),
    )
    assert assessment.verdict == "partial"
    assert "2026-10-23" in assessment.reason


def test_unresolved_quarantine_is_failure():
    quarantined = [
        {
            "release_id": "release-two",
            "publication_at_utc": "2026-10-23T15:56:07+00:00",
            "error_detail": "signature failed",
        }
    ]
    assessment = assess_ecb_assets(
        action="poll",
        status=_status(quarantined=quarantined),
        now=datetime(2026, 10, 23, 17, 0, tzinfo=timezone.utc),
    )
    assert assessment.verdict == "failure"
    assert "quarantined" in assessment.reason
    assert assessment.details["latest_quarantine_error"] == "signature failed"


@pytest.mark.parametrize(
    ("result", "reason_fragment"),
    [
        ({"coverage_status": "gapped"}, "gapped"),
        (
            {
                "coverage_status": "complete",
                "full_count": 100,
                "new_count": 10,
                "updated_count": 15,
                "deleted_count": 5,
            },
            "change volume",
        ),
    ],
)
def test_gap_and_large_change_are_partial(result, reason_fragment):
    assessment = assess_ecb_assets(
        action="poll",
        results=[result],
        status=_status(
            latest_accepted={
                **LATEST,
                "publication_at_utc": "2026-10-23T15:56:07+00:00",
                "coverage_status": result["coverage_status"],
            }
        ),
        now=datetime(2026, 10, 23, 21, 30, tzinfo=timezone.utc),
        settings=ECBAssetsAlertConfig(max_change_percent=20.0),
    )
    assert assessment.verdict == "partial"
    assert reason_fragment in assessment.reason


class _CapturingTransport:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)


def test_failure_alert_uses_ecb_subject_and_existing_failure_route():
    transport = _CapturingTransport()
    config = AlertingConfig(
        enabled=True,
        policy="failure",
        from_address="pipeline@example.com",
        routes={"failure": RouteConfig(to=("oncall@example.com",))},
    )
    assert send_ecb_assets_alert(
        action="poll",
        error="MID unavailable",
        environment="prod",
        config=config,
        transport=transport,
    )
    (message,) = transport.sent
    assert "[ECB][prod] FAILURE" in message.subject
    assert "MID unavailable" in message.text_body
    assert message.to == ("oncall@example.com",)
