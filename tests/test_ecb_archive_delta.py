from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from fred_pipeline.ecb_archive_delta import (
    ECBDeltaPromoter,
    ECBDeltaStageWriter,
    ECBDeltaWriteError,
)


class _Config:
    @staticmethod
    def table(schema: str, name: str) -> str:
        return f"macro_test.{schema}.{name}"


class _RecordingSpark:
    def __init__(self):
        self.statements: list[str] = []

    def sql(self, statement: str):
        self.statements.append(statement)
        return SimpleNamespace(collect=lambda: [])


def test_delta_stage_tables_are_declared_in_silver_ddl():
    ddl = Path("sql/40_silver.sql").read_text(encoding="utf-8")
    for name in (
        "ecb_eligible_asset_backfill_run",
        "ecb_eligible_asset_release_stage",
        "ecb_eligible_asset_version_stage",
        "ecb_eligible_asset_change_stage",
    ):
        assert f"CREATE TABLE IF NOT EXISTS silver.{name}" in ddl
    assert ddl.count("PARTITIONED BY (run_id)") >= 3


def test_empty_delta_partition_is_deleted_without_dataframe_inference():
    spark = _RecordingSpark()
    writer = ECBDeltaStageWriter(spark, _Config())
    assert writer._replace_partition("ecb_eligible_asset_change_stage", "r'1", []) == 0
    assert spark.statements == [
        "DELETE FROM macro_test.silver.ecb_eligible_asset_change_stage "
        "WHERE run_id = 'r''1'"
    ]


def test_delta_stage_writer_rejects_nonstaged_control_row_before_writes():
    writer = ECBDeltaStageWriter(_RecordingSpark(), _Config())
    with pytest.raises(ECBDeltaWriteError, match="status 'staged'"):
        writer.replace_run(
            run_id="history",
            run_row={"run_id": "history", "status": "promoted"},
            releases=[],
            versions=[],
            changes=[],
        )


def test_delta_promotion_is_one_catalog_transaction():
    promoter = ECBDeltaPromoter(_RecordingSpark(), _Config())
    sql = promoter.atomic_sql(
        {
            "run_id": "history",
            "stage_boundary_release_id": "archive:2026-10-22",
            "live_boundary_release_id": "signed-boundary",
            "live_publication_at_utc": datetime(
                2026, 10, 22, 15, 56, 7, tzinfo=timezone.utc
            ),
            "dictionary_hash": "dictionary-v1",
        }
    )

    assert sql.startswith("BEGIN ATOMIC\n")
    assert sql.endswith("\nEND")
    assert sql.count("BEGIN ATOMIC") == 1
    assert "INSERT OVERWRITE TABLE macro_test.silver.ecb_eligible_asset_version" in sql
    assert "INSERT OVERWRITE TABLE macro_test.silver.ecb_eligible_asset_change" in sql
    assert "historical_boundary_signed_full" in sql
    assert "status = 'promoted'" in sql


def test_delta_promotion_quotes_run_and_release_ids():
    promoter = ECBDeltaPromoter(_RecordingSpark(), _Config())
    sql = promoter.atomic_sql(
        {
            "run_id": "run'o",
            "stage_boundary_release_id": "archive:'boundary",
            "live_boundary_release_id": "live'id",
            "live_publication_at_utc": "2026-10-22T15:56:07+00:00",
            "dictionary_hash": None,
        }
    )
    assert "run''o" in sql
    assert "archive:''boundary" in sql
    assert "live''id" in sql
    assert "THEN NULL" in sql
