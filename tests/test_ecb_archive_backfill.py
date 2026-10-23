from __future__ import annotations

import hashlib
import io
import tarfile
import zipfile
from dataclasses import replace
from datetime import date

import pytest

from fred_pipeline import cli
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.ecb_archive_backfill import (
    ECBArchiveBackfillError,
    ECBHistoricalStageRepository,
    ECBHistoricalStagingRebuilder,
    HistoricalSnapshot,
    iter_archive_zip,
)
from fred_pipeline.ecb_asset_schema_registry import load_schema_registry
from fred_pipeline.local_store import LocalWarehouse


def _warehouse(tmp_path):
    config = PipelineConfig(environment=Environment.TEST, fred_api_key="")
    return LocalWarehouse(config, db_path=str(tmp_path / "history.db"))


def _payload(columns, rows):
    lines = ["\t".join(columns)]
    for values in rows:
        lines.append("\t".join(str(values.get(column, "")) for column in columns))
    return ("\n".join(lines) + "\n").encode("utf-16")


def _row(isin, *, haircut="1.0", issuer="Issuer"):
    return {
        "ISIN_CODE": isin,
        "HAIRCUT_CATEGORY": "L1A",
        "TYPE": "AT01",
        "REFERENCE_MARKET": "RMDE01",
        "DENOMINATION": "EUR",
        "ISSUANCE_DATE": "01/01/2024 00:00:00",
        "MATURITY_DATE": "01/01/2030 00:00:00",
        "ISSUER_CSD": "CLDE01",
        "COUPON_RATE (%)": "2.5",
        "ISSUER_NAME": issuer,
        "ISSUER_RESIDENCE": "IRDE",
        "ISSUER_GROUP": "IG1",
        "COUPON_DEFINITION": "CD4",
        "HAIRCUT": haircut,
        "POTENTIALLY_OWN_USABLE_COVERED_BOND": "N",
        "CLIMATE_FACTOR": "N",
    }


def _snapshot(snapshot_date, columns, rows):
    payload = _payload(columns, rows)
    return HistoricalSnapshot(
        snapshot_date=snapshot_date,
        source_url=f"https://example/{snapshot_date}.csv.gz",
        entry_path=f"ea_csv_{snapshot_date.strftime('%y%m%d')}.csv.gz",
        entry_sha256=hashlib.sha256(payload).hexdigest(),
        archive_sha256="a" * 64,
        payload=payload,
    )


def _three_snapshots():
    columns = load_schema_registry().schemas[-1].columns
    return [
        _snapshot(
            date(2026, 6, 12),
            columns,
            [_row("DE0001"), _row("DE0002")],
        ),
        _snapshot(
            date(2026, 6, 15),
            columns,
            [_row("DE0001", haircut="1.5"), _row("DE0003")],
        ),
        _snapshot(
            date(2026, 6, 17),
            columns,
            [
                _row("DE0001", haircut="1.5"),
                _row("DE0002"),
                _row("DE0003"),
            ],
        ),
    ]


def test_stage_rebuild_is_compact_gap_aware_and_isolated_from_live(tmp_path):
    warehouse = _warehouse(tmp_path)
    try:
        result = ECBHistoricalStagingRebuilder(warehouse).rebuild(
            _three_snapshots(),
            run_id="test-history",
            from_date=date(2026, 6, 12),
            to_date=date(2026, 6, 17),
        )

        assert result["status"] == "staged"
        assert result["snapshot_count"] == 3
        assert result["missing_count"] == 1
        assert result["quarantined_count"] == 0
        assert result["version_count"] == 6
        assert result["change_count"] == 6
        assert result["final_asset_count"] == 3

        releases = warehouse.query(
            "SELECT * FROM silver_ecb_eligible_asset_release_stage "
            "WHERE run_id = ? ORDER BY snapshot_date",
            ("test-history",),
        )
        assert [row["status"] for row in releases] == [
            "accepted",
            "accepted",
            "missing",
            "accepted",
        ]
        assert releases[-1]["coverage_status"] == "gapped"
        assert releases[-1]["reconciliation_status"] == "derived_full_diff_gap"
        assert releases[0]["timestamp_precision"] == "date"
        assert releases[0]["publication_at_utc"].endswith("21:59:59+00:00")

        open_eligible = warehouse.query(
            "SELECT asset_key FROM silver_ecb_eligible_asset_version_stage "
            "WHERE run_id = ? AND valid_to_utc IS NULL AND is_eligible = 1 "
            "ORDER BY asset_key",
            ("test-history",),
        )
        assert [row["asset_key"] for row in open_eligible] == [
            "ISIN:DE0001",
            "ISIN:DE0002",
            "ISIN:DE0003",
        ]
        assert (
            warehouse.query(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_release"
            )[0]["n"]
            == 0
        )
        assert (
            warehouse.query(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version"
            )[0]["n"]
            == 0
        )
    finally:
        warehouse.close()


def test_failed_run_resumes_from_committed_snapshot(tmp_path):
    warehouse = _warehouse(tmp_path)
    snapshots = _three_snapshots()

    def interrupted():
        yield snapshots[0]
        yield snapshots[1]
        raise RuntimeError("simulated interruption")

    try:
        rebuilder = ECBHistoricalStagingRebuilder(warehouse)
        with pytest.raises(RuntimeError, match="simulated interruption"):
            rebuilder.rebuild(
                interrupted(),
                run_id="resume-history",
                from_date=date(2026, 6, 12),
                to_date=date(2026, 6, 17),
            )
        repository = ECBHistoricalStageRepository(warehouse)
        assert repository.status("resume-history")["status"] == "failed"
        assert repository.status("resume-history")["snapshot_count"] == 2

        result = rebuilder.rebuild(
            snapshots,
            run_id="resume-history",
            from_date=date(2026, 6, 12),
            to_date=date(2026, 6, 17),
        )
        assert result["status"] == "staged"
        assert result["snapshot_count"] == 3
        assert result["final_asset_count"] == 3
    finally:
        warehouse.close()


def test_unknown_schema_is_quarantined_without_live_or_stage_versions(tmp_path):
    warehouse = _warehouse(tmp_path)
    columns = [*load_schema_registry().schemas[-1].columns, "SURPRISE"]
    snapshot = _snapshot(date(2026, 6, 12), columns, [_row("DE0001")])
    try:
        with pytest.raises(ECBArchiveBackfillError, match="unregistered"):
            ECBHistoricalStagingRebuilder(warehouse).rebuild(
                [snapshot],
                run_id="bad-history",
                from_date=date(2026, 6, 12),
                to_date=date(2026, 6, 12),
            )
        releases = warehouse.query(
            "SELECT status, error_detail "
            "FROM silver_ecb_eligible_asset_release_stage WHERE run_id = ?",
            ("bad-history",),
        )
        assert releases[0]["status"] == "quarantined"
        assert "unregistered" in releases[0]["error_detail"]
        assert (
            warehouse.query(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version_stage "
                "WHERE run_id = ?",
                ("bad-history",),
            )[0]["n"]
            == 0
        )
    finally:
        warehouse.close()


def test_middle_quarantine_is_bridged_by_next_authoritative_full_snapshot(tmp_path):
    warehouse = _warehouse(tmp_path)
    columns = load_schema_registry().schemas[-1].columns
    bad_columns = [*columns, "SURPRISE"]
    snapshots = [
        _snapshot(date(2026, 6, 12), columns, [_row("DE0001")]),
        _snapshot(date(2026, 6, 15), bad_columns, [_row("DE0001")]),
        _snapshot(
            date(2026, 6, 16),
            columns,
            [_row("DE0001"), _row("DE0002")],
        ),
    ]
    try:
        result = ECBHistoricalStagingRebuilder(warehouse).rebuild(
            snapshots,
            run_id="quarantine-gap-history",
            from_date=date(2026, 6, 12),
            to_date=date(2026, 6, 16),
        )

        assert result["status"] == "staged"
        assert result["snapshot_count"] == 2
        assert result["quarantined_count"] == 1
        releases = warehouse.query(
            "SELECT snapshot_date, status, coverage_status, is_consecutive "
            "FROM silver_ecb_eligible_asset_release_stage WHERE run_id = ? "
            "ORDER BY snapshot_date",
            ("quarantine-gap-history",),
        )
        assert [(row["snapshot_date"], row["status"]) for row in releases] == [
            ("2026-06-12", "accepted"),
            ("2026-06-15", "quarantined"),
            ("2026-06-16", "accepted"),
        ]
        assert releases[-1]["coverage_status"] == "gapped"
        assert releases[-1]["is_consecutive"] == 0
    finally:
        warehouse.close()


def test_resume_rejects_changed_bytes_for_an_accepted_snapshot(tmp_path):
    warehouse = _warehouse(tmp_path)
    snapshots = _three_snapshots()

    def interrupted():
        yield snapshots[0]
        raise RuntimeError("stop after checkpoint")

    try:
        rebuilder = ECBHistoricalStagingRebuilder(warehouse)
        with pytest.raises(RuntimeError, match="checkpoint"):
            rebuilder.rebuild(
                interrupted(),
                run_id="drift-history",
                from_date=date(2026, 6, 12),
                to_date=date(2026, 6, 17),
            )
        changed = replace(snapshots[0], entry_sha256="f" * 64)
        with pytest.raises(ECBArchiveBackfillError, match="source bytes changed"):
            rebuilder.rebuild(
                [changed, *snapshots[1:]],
                run_id="drift-history",
                from_date=date(2026, 6, 12),
                to_date=date(2026, 6, 17),
            )
    finally:
        warehouse.close()


def test_iter_archive_zip_unwraps_legacy_tgz(tmp_path):
    registry = load_schema_registry()
    columns = registry.schemas[0].columns
    payload = _payload(columns, [_row("DE0001")])
    nested = io.BytesIO()
    with tarfile.open(fileobj=nested, mode="w:gz") as archive:
        info = tarfile.TarInfo("ea_all_100408.txt")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
    archive_path = tmp_path / "2010.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("2010/ea_all_100408.tgz", nested.getvalue())

    snapshots = list(iter_archive_zip(archive_path, year=2010))
    assert len(snapshots) == 1
    assert snapshots[0].snapshot_date == date(2010, 4, 8)
    assert snapshots[0].payload.startswith((b"\xff\xfe", b"\xfe\xff"))


def test_backfill_stage_cli_options_parse():
    args = cli.build_parser().parse_args(
        [
            "ecb-assets",
            "backfill-stage",
            "--from-year",
            "2010",
            "--to-year",
            "2015",
            "--archive-dir",
            "data/ecb",
            "--backfill-run-id",
            "review-1",
            "--restart",
        ]
    )
    assert args.action == "backfill-stage"
    assert args.archive_dir == "data/ecb"
    assert args.backfill_run_id == "review-1"
    assert args.restart is True
