"""Shared ECB polling must enforce authority and preserve caller ownership."""

import pytest

from fred_pipeline.config import PipelineConfig
from fred_pipeline.ecb_eligible_assets import (
    ECBEligibleAssetRepository,
    ECBEligibleAssetsPipeline,
    ECBReleaseValidationError,
    SignatureResult,
)
from fred_pipeline.io.local_store import LocalWarehouse
from tests.test_ecb_eligible_assets import _fixture_client, _verified


def test_shared_poll_ingests_signed_releases_and_borrows_warehouse(tmp_path):
    from fred_pipeline.ecb_execution import execute_ecb_poll

    config = PipelineConfig.resolve(config_file="/nonexistent", rate_limit_per_minute=0)
    warehouse = LocalWarehouse(config, db_path=str(tmp_path / "ecb.db"))
    fixture, release_url, _ = _fixture_client()
    fixture.rss = f"""<rss><channel><item>
    <title>List of eligible marketable assets</title><link>{release_url}</link>
    <pubDate>Mon, 01 Jan 2024 17:56:07 +0100</pubDate>
    </item></channel></rss>""".encode()

    def factory(w, client):
        return ECBEligibleAssetsPipeline(w, client=client, signature_verifier=_verified)

    try:
        report = execute_ecb_poll(
            config,
            warehouse=warehouse,
            selected_backend="local",
            authoritative_backend="local",
            client=fixture,
            pipeline_factory=factory,
        )
        assert report.outcome == "accepted"
        assert len(report.releases) == 1
        assert report.releases[0]["status"] == "accepted"
        assert (
            warehouse.conn.execute(
                "SELECT count(*) FROM silver_ecb_eligible_asset_version"
            ).fetchone()[0]
            == 2
        )
        assert (
            execute_ecb_poll(
                config,
                warehouse=warehouse,
                selected_backend="local",
                authoritative_backend="local",
                client=fixture,
                pipeline_factory=factory,
            ).outcome
            == "no_new_release"
        )
    finally:
        warehouse.close()


@pytest.mark.parametrize(
    "selected,authoritative",
    [("local", "disabled"), ("local", "postgres"), ("spark", "spark")],
)
def test_service_rejects_non_authoritative_writes_before_source_calls(
    selected, authoritative
):
    from fred_pipeline.ecb_execution import ECBAuthorityError, execute_ecb_poll

    class Client:
        def fetch_rss(self):
            pytest.fail("source must not be contacted")

    with pytest.raises(ECBAuthorityError):
        execute_ecb_poll(
            PipelineConfig.resolve(config_file="/nonexistent"),
            warehouse=object(),
            selected_backend=selected,
            authoritative_backend=authoritative,
            client=Client(),
        )


def test_managed_mode_requires_run_selected_session():
    from fred_pipeline.ecb_execution import ECBExecutionError, execute_ecb_poll

    class Warehouse:
        conn = object()

        def _insert(self):
            pass

    config = PipelineConfig.resolve(
        config_file="/nonexistent", network={"mode": "auto"}
    )
    with pytest.raises(ECBExecutionError, match="route"):
        execute_ecb_poll(
            config,
            warehouse=Warehouse(),
            selected_backend="local",
            authoritative_backend="local",
        )


def test_shared_poll_preserves_quarantine_and_borrowed_warehouse_on_failure(tmp_path):
    from fred_pipeline.ecb_execution import execute_ecb_poll

    config = PipelineConfig.resolve(config_file="/nonexistent", rate_limit_per_minute=0)
    warehouse = LocalWarehouse(config, db_path=str(tmp_path / "ecb.db"))
    client, release_url, _ = _fixture_client()
    client.rss = f"""<rss><channel><item>
    <title>List of eligible marketable assets</title><link>{release_url}</link>
    <pubDate>Mon, 01 Jan 2024 17:56:07 +0100</pubDate>
    </item></channel></rss>""".encode()

    def factory(w, fixture):
        return ECBEligibleAssetsPipeline(
            w,
            client=fixture,
            signature_verifier=lambda _payload, _signature: SignatureResult(
                False, "digest_mismatch", detail="tampered"
            ),
        )

    try:
        with pytest.raises(ECBReleaseValidationError, match="signature failed"):
            execute_ecb_poll(
                config,
                warehouse=warehouse,
                selected_backend="local",
                authoritative_backend="local",
                client=client,
                pipeline_factory=factory,
            )
        status = ECBEligibleAssetRepository(warehouse).status()
        assert status["quarantined"]
        assert status["current_eligible_count"] == 0
    finally:
        warehouse.close()
