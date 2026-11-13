from datetime import datetime, timezone

import pytest

from fred_pipeline.config import PipelineConfig
from fred_pipeline.ecb_eligible_assets import ECBEligibleAssetsPipeline
from fred_pipeline.governance.alerting_config import ECBAssetsAlertConfig
from fred_pipeline.io.local_store import LocalWarehouse
from tests.test_ecb_eligible_assets import _fixture_client, _verified


@pytest.fixture
def accepted(tmp_path):
    warehouse = LocalWarehouse(PipelineConfig(), str(tmp_path / "ecb.db"))
    client, url, _ = _fixture_client()
    ECBEligibleAssetsPipeline(
        warehouse, client=client, signature_verifier=_verified
    ).ingest_release(url)
    try:
        yield warehouse
    finally:
        warehouse.close()


def test_ecb_health_checks_accepted_frontier_without_payloads(accepted):
    from fred_pipeline.ecb_health import ecb_health_snapshot

    verdict, details = ecb_health_snapshot(
        accepted,
        ECBAssetsAlertConfig(),
        now=datetime(2024, 1, 1, 18, tzinfo=timezone.utc),
    )
    assert verdict == "success"
    assert len(details["checks"]) == 7
    assert details["limited_history"] is True
    assert "payload" not in str(details)


@pytest.mark.parametrize(
    ("sql", "check"),
    [
        (
            "UPDATE bronze_ecb_eligible_asset_artifact SET signature_status = 'invalid'",
            "accepted_release_validated",
        ),
        (
            "DELETE FROM silver_ecb_eligible_asset_version WHERE asset_key = (SELECT MIN(asset_key) FROM silver_ecb_eligible_asset_version)",
            "current_matches_release",
        ),
        (
            "UPDATE silver_ecb_eligible_asset_version SET valid_to_utc = valid_from_utc",
            "history_intervals_valid",
        ),
        (
            "UPDATE silver_ecb_eligible_asset_release SET coverage_status = 'unknown'",
            "coverage_declared",
        ),
        (
            "UPDATE silver_ecb_eligible_asset_change SET change_origin = 'source_noop'",
            "source_noop_auditable",
        ),
        (
            "UPDATE bronze_ecb_eligible_asset_release SET status = 'quarantined', publication_at_utc = '2030-01-01'",
            "no_unresolved_quarantine",
        ),
    ],
)
def test_ecb_health_detects_persisted_corruption(accepted, sql, check):
    from fred_pipeline.ecb_health import ecb_health_snapshot

    accepted.conn.execute(sql)
    verdict, details = ecb_health_snapshot(accepted, ECBAssetsAlertConfig())
    assert verdict == "failure"
    assert details["checks"][check] == "fail"


def test_missing_accepted_release_is_failure(tmp_path):
    from fred_pipeline.ecb_health import ecb_health_snapshot

    warehouse = LocalWarehouse(PipelineConfig(), str(tmp_path / "empty.db"))
    try:
        verdict, details = ecb_health_snapshot(warehouse, ECBAssetsAlertConfig())
    finally:
        warehouse.close()
    assert verdict == "failure"
    assert details["latest_release_id"] is None
