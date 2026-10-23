from __future__ import annotations

from datetime import date

import pytest

from fred_pipeline import cli
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.ecb_archive_backfill import ECBHistoricalStagingRebuilder
from fred_pipeline.ecb_archive_promotion import (
    ECBArchivePromotionError,
    ECBHistoricalPromoter,
)
from fred_pipeline.local_store import LocalWarehouse
from tests.test_ecb_archive_backfill import _three_snapshots


def _warehouse(tmp_path):
    config = PipelineConfig(environment=Environment.TEST, fred_api_key="")
    return LocalWarehouse(config, db_path=str(tmp_path / "promotion.db"))


def _seed_signed_live_boundary(warehouse, run_id: str) -> str:
    release_id = "signed-live-boundary"
    publication = "2026-06-17T15:56:07+00:00"
    current = warehouse.query(
        "SELECT * FROM silver_ecb_eligible_asset_version_stage "
        "WHERE run_id = ? AND valid_to_utc IS NULL AND is_eligible = 1",
        (run_id,),
    )
    warehouse._insert(
        "silver_ecb_eligible_asset_release",
        [
            {
                "release_id": release_id,
                "release_url": "https://example/signed-live",
                "publication_at_utc": publication,
                "full_count": len(current),
                "new_count": len(current),
                "updated_count": 0,
                "deleted_count": 0,
                "coverage_status": "starts_here",
                "timestamp_precision": "exact",
                "status": "accepted",
                "accepted_at": publication,
                "run_id": "forward-live",
            }
        ],
    )
    version_columns = (
        "asset_key",
        "isin_code",
        "other_reg_number",
        "is_eligible",
        "haircut_category",
        "asset_type",
        "reference_market",
        "denomination",
        "issuance_date",
        "maturity_date",
        "issuer_csd",
        "coupon_definition",
        "coupon_rate",
        "issuer_name",
        "issuer_group",
        "issuer_residence",
        "guarantor_name",
        "guarantor_group",
        "guarantor_residence",
        "non_own_use_haircut",
        "own_use_haircut",
        "potentially_own_usable_covered_bond",
        "climate_factor",
        "source_fields_json",
        "source_schema_version",
        "row_hash",
    )
    versions = []
    changes = []
    for row in current:
        versions.append(
            {
                **{column: row.get(column) for column in version_columns},
                "dictionary_hash": "dictionary-live",
                "change_type": "N",
                "change_origin": "full_snapshot",
                "valid_from_release_id": release_id,
                "valid_from_utc": publication,
                "valid_to_release_id": None,
                "valid_to_utc": None,
                "ingested_at": publication,
                "run_id": "forward-live",
            }
        )
        changes.append(
            {
                "release_id": release_id,
                "asset_key": row["asset_key"],
                "computed_change_type": "N",
                "new_row_hash": row["row_hash"],
                "reconciliation_status": "matched",
                "change_origin": "full_snapshot",
                "publication_at_utc": publication,
                "run_id": "forward-live",
            }
        )
    warehouse._insert("silver_ecb_eligible_asset_version", versions)
    warehouse._insert("silver_ecb_eligible_asset_change", changes)
    return release_id


def test_promotion_requires_exact_ack_and_atomically_maps_signed_boundary(tmp_path):
    warehouse = _warehouse(tmp_path)
    run_id = "promotion-history"
    try:
        staged = ECBHistoricalStagingRebuilder(warehouse).rebuild(
            _three_snapshots(),
            run_id=run_id,
            from_date=date(2026, 6, 12),
            to_date=date(2026, 6, 17),
        )
        assert staged["status"] == "staged"
        live_release_id = _seed_signed_live_boundary(warehouse, run_id)
        promoter = ECBHistoricalPromoter(warehouse)

        with pytest.raises(ECBArchivePromotionError, match="requires"):
            promoter.promote(run_id, acknowledge_quarantines=None)
        with pytest.raises(ECBArchivePromotionError, match="does not match"):
            promoter.promote(run_id, acknowledge_quarantines=1)
        assert (
            warehouse.query(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version"
            )[0]["n"]
            == 3
        )

        result = promoter.promote(run_id, acknowledge_quarantines=0)
        assert result["status"] == "promoted"
        assert result["idempotent"] is False
        assert result["current_asset_count"] == 3
        assert result["version_count"] == staged["version_count"]
        assert result["change_count"] == staged["change_count"]

        releases = warehouse.query(
            "SELECT release_id FROM silver_ecb_eligible_asset_release "
            "ORDER BY publication_at_utc"
        )
        assert releases[-1]["release_id"] == live_release_id
        assert "archive:2026-06-17" not in {row["release_id"] for row in releases}
        boundary_changes = warehouse.query(
            "SELECT computed_change_type FROM silver_ecb_eligible_asset_change "
            "WHERE release_id = ?",
            (live_release_id,),
        )
        assert [row["computed_change_type"] for row in boundary_changes] == ["N"]
        assert (
            warehouse.query(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version "
                "WHERE valid_to_utc IS NULL AND dictionary_hash = 'dictionary-live'"
            )[0]["n"]
            == 3
        )

        again = promoter.promote(run_id, acknowledge_quarantines=0)
        assert again["status"] == "promoted"
        assert again["idempotent"] is True
    finally:
        warehouse.close()


def test_cli_accepts_promotion_acknowledgement():
    args = cli.build_parser().parse_args(
        [
            "ecb-assets",
            "backfill-promote",
            "--backfill-run-id",
            "history",
            "--acknowledge-quarantines",
            "8",
        ]
    )
    assert args.action == "backfill-promote"
    assert args.acknowledge_quarantines == 8
