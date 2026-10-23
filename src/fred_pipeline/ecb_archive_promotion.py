"""Atomic promotion of a reconciled ECB historical staging run.

The archive represents publication time at date precision (end of day), while
the forward MID release has an exact signed publication timestamp.  Promotion
therefore maps the final archive release onto the matching signed live release
instead of publishing two overlapping versions for the same cutoff.
"""

from __future__ import annotations

from typing import Any, Sequence

from fred_pipeline.ecb_eligible_assets import _utc_now


class ECBArchivePromotionError(RuntimeError):
    """A staged history cannot be promoted without violating invariants."""


class ECBHistoricalPromoter:
    """Validate and atomically merge one staged history into live Silver."""

    def __init__(self, warehouse: Any):
        if not hasattr(warehouse, "conn") or not hasattr(warehouse, "_insert"):
            raise TypeError("ECB promotion requires a SQL warehouse")
        self.warehouse = warehouse

    def _rows(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        return [
            dict(row) for row in self.warehouse.conn.execute(sql, params).fetchall()
        ]

    def _scalar(self, sql: str, params: Sequence[Any] = ()) -> int:
        row = self._rows(sql, params)[0]
        return int(next(iter(row.values())) or 0)

    def preflight(
        self, run_id: str, *, acknowledge_quarantines: int | None
    ) -> dict[str, Any]:
        runs = self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_backfill_run WHERE run_id = ?",
            (run_id,),
        )
        if not runs:
            raise ECBArchivePromotionError(f"unknown staging run {run_id!r}")
        run = runs[0]
        if run["status"] not in {"staged", "promoted"}:
            raise ECBArchivePromotionError(
                f"staging run {run_id!r} has status {run['status']!r}, not 'staged'"
            )
        quarantine_count = self._scalar(
            "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_release_stage "
            "WHERE run_id = ? AND status = 'quarantined'",
            (run_id,),
        )
        if acknowledge_quarantines is None:
            raise ECBArchivePromotionError(
                "promotion requires --acknowledge-quarantines COUNT "
                f"(this run has {quarantine_count})"
            )
        if acknowledge_quarantines != quarantine_count:
            raise ECBArchivePromotionError(
                f"quarantine acknowledgement {acknowledge_quarantines} does not "
                f"match staged count {quarantine_count}"
            )

        boundary_rows = self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_release_stage "
            "WHERE run_id = ? AND status = 'accepted' "
            "ORDER BY snapshot_date DESC LIMIT 1",
            (run_id,),
        )
        if not boundary_rows:
            raise ECBArchivePromotionError("staging run has no accepted boundary")
        boundary = boundary_rows[0]
        live_rows = self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_release "
            "WHERE status = 'accepted' AND substr(publication_at_utc, 1, 10) = ?",
            (boundary["snapshot_date"],),
        )
        if len(live_rows) != 1:
            raise ECBArchivePromotionError(
                "expected exactly one signed live release at staged boundary "
                f"{boundary['snapshot_date']}; found {len(live_rows)}"
            )
        live = live_rows[0]
        later_live = self._scalar(
            "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_release "
            "WHERE status = 'accepted' AND publication_at_utc > ?",
            (live["publication_at_utc"],),
        )
        if later_live:
            raise ECBArchivePromotionError(
                "live releases exist after the staging boundary; rebuild staging "
                "through the latest signed release before promotion"
            )

        duplicate_open = self._scalar(
            "SELECT COUNT(*) AS n FROM ("
            "SELECT asset_key FROM silver_ecb_eligible_asset_version_stage "
            "WHERE run_id = ? AND valid_to_utc IS NULL GROUP BY asset_key "
            "HAVING COUNT(*) > 1) duplicate_open",
            (run_id,),
        )
        invalid_intervals = self._scalar(
            "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version_stage "
            "WHERE run_id = ? AND valid_to_utc IS NOT NULL "
            "AND valid_to_utc <= valid_from_utc",
            (run_id,),
        )
        stage_only = self._scalar(
            "SELECT COUNT(*) AS n FROM ("
            "SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version_stage "
            "WHERE run_id = ? AND valid_to_utc IS NULL AND is_eligible = 1 "
            "EXCEPT SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version "
            "WHERE valid_to_utc IS NULL AND is_eligible = 1) stage_only",
            (run_id,),
        )
        live_only = self._scalar(
            "SELECT COUNT(*) AS n FROM ("
            "SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version "
            "WHERE valid_to_utc IS NULL AND is_eligible = 1 "
            "EXCEPT SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version_stage "
            "WHERE run_id = ? AND valid_to_utc IS NULL AND is_eligible = 1) live_only",
            (run_id,),
        )
        if duplicate_open or invalid_intervals or stage_only or live_only:
            raise ECBArchivePromotionError(
                "promotion preflight failed: "
                f"duplicate_open={duplicate_open} "
                f"invalid_intervals={invalid_intervals} "
                f"stage_only={stage_only} live_only={live_only}"
            )

        if run["status"] != "promoted":
            live_release_count = self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_release"
            )
            nonboundary_versions = self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version "
                "WHERE valid_from_release_id <> ?",
                (live["release_id"],),
            )
            nonboundary_changes = self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_change "
                "WHERE release_id <> ?",
                (live["release_id"],),
            )
            if live_release_count != 1 or nonboundary_versions or nonboundary_changes:
                raise ECBArchivePromotionError(
                    "live Silver is not the expected single-release bootstrap state"
                )

        return {
            "run_id": run_id,
            "status": run["status"],
            "quarantined_count": quarantine_count,
            "boundary_snapshot_date": boundary["snapshot_date"],
            "stage_boundary_release_id": boundary["release_id"],
            "live_boundary_release_id": live["release_id"],
            "live_publication_at_utc": live["publication_at_utc"],
            "stage_only": stage_only,
            "live_only": live_only,
            "duplicate_open": duplicate_open,
            "invalid_intervals": invalid_intervals,
        }

    def promote(
        self, run_id: str, *, acknowledge_quarantines: int | None
    ) -> dict[str, Any]:
        check = self.preflight(run_id, acknowledge_quarantines=acknowledge_quarantines)
        if check["status"] == "promoted":
            return {**check, "status": "promoted", "idempotent": True}

        stage_boundary = check["stage_boundary_release_id"]
        live_boundary = check["live_boundary_release_id"]
        live_time = check["live_publication_at_utc"]
        prior_defer = bool(getattr(self.warehouse, "_defer_commits", False))
        self.warehouse._defer_commits = True
        try:
            self.warehouse.conn.execute(
                "DROP TABLE IF EXISTS temp_ecb_live_boundary_version"
            )
            self.warehouse.conn.execute(
                "CREATE TEMP TABLE temp_ecb_live_boundary_version AS "
                "SELECT * FROM silver_ecb_eligible_asset_version"
            )
            self.warehouse.conn.execute("DELETE FROM silver_ecb_eligible_asset_change")
            self.warehouse.conn.execute("DELETE FROM silver_ecb_eligible_asset_version")

            self.warehouse.conn.execute(
                """
                INSERT INTO silver_ecb_eligible_asset_release (
                    release_id, release_url, release_at_local,
                    publication_at_local, publication_at_utc, rss_pub_at,
                    prior_release_id, is_consecutive, full_count, update_count,
                    new_count, updated_count, deleted_count, parser_version,
                    source_schema_hash, dictionary_hash, reconciliation_status,
                    coverage_status, timestamp_precision, status, accepted_at, run_id
                )
                SELECT release_id, source_url, publication_at_local,
                       publication_at_local, publication_at_utc, NULL,
                       prior_release_id, is_consecutive, full_count, NULL,
                       new_count, updated_count, deleted_count, parser_version,
                       source_schema_hash, NULL, reconciliation_status,
                       coverage_status, timestamp_precision, status, staged_at, run_id
                FROM silver_ecb_eligible_asset_release_stage
                WHERE run_id = ? AND release_id <> ?
                """,
                (run_id, stage_boundary),
            )
            self.warehouse.conn.execute(
                """
                UPDATE silver_ecb_eligible_asset_release
                SET prior_release_id = (
                        SELECT prior_release_id
                        FROM silver_ecb_eligible_asset_release_stage
                        WHERE run_id = ? AND release_id = ?
                    ),
                    is_consecutive = (
                        SELECT is_consecutive
                        FROM silver_ecb_eligible_asset_release_stage
                        WHERE run_id = ? AND release_id = ?
                    ),
                    full_count = (
                        SELECT full_count
                        FROM silver_ecb_eligible_asset_release_stage
                        WHERE run_id = ? AND release_id = ?
                    ),
                    new_count = (
                        SELECT new_count
                        FROM silver_ecb_eligible_asset_release_stage
                        WHERE run_id = ? AND release_id = ?
                    ),
                    updated_count = (
                        SELECT updated_count
                        FROM silver_ecb_eligible_asset_release_stage
                        WHERE run_id = ? AND release_id = ?
                    ),
                    deleted_count = (
                        SELECT deleted_count
                        FROM silver_ecb_eligible_asset_release_stage
                        WHERE run_id = ? AND release_id = ?
                    ),
                    coverage_status = 'complete'
                WHERE release_id = ?
                """,
                (
                    run_id,
                    stage_boundary,
                    run_id,
                    stage_boundary,
                    run_id,
                    stage_boundary,
                    run_id,
                    stage_boundary,
                    run_id,
                    stage_boundary,
                    run_id,
                    stage_boundary,
                    live_boundary,
                ),
            )
            self.warehouse.conn.execute(
                """
                INSERT INTO silver_ecb_eligible_asset_version (
                    asset_key, isin_code, other_reg_number, is_eligible,
                    haircut_category, asset_type, reference_market, denomination,
                    issuance_date, maturity_date, issuer_csd, coupon_definition,
                    coupon_rate, issuer_name, issuer_group, issuer_residence,
                    guarantor_name, guarantor_group, guarantor_residence,
                    non_own_use_haircut, own_use_haircut,
                    potentially_own_usable_covered_bond, climate_factor,
                    source_fields_json, source_schema_version, dictionary_hash,
                    row_hash, change_type, change_origin, valid_from_release_id,
                    valid_from_utc, valid_to_release_id, valid_to_utc,
                    ingested_at, run_id
                )
                SELECT s.asset_key, s.isin_code, s.other_reg_number, s.is_eligible,
                       s.haircut_category, s.asset_type, s.reference_market,
                       s.denomination, s.issuance_date, s.maturity_date, s.issuer_csd,
                       s.coupon_definition, s.coupon_rate, s.issuer_name,
                       s.issuer_group, s.issuer_residence, s.guarantor_name,
                       s.guarantor_group, s.guarantor_residence,
                       s.non_own_use_haircut, s.own_use_haircut,
                       s.potentially_own_usable_covered_bond, s.climate_factor,
                       CASE WHEN s.valid_from_release_id = ? AND b.asset_key IS NOT NULL
                            THEN b.source_fields_json ELSE s.source_fields_json END,
                       CASE WHEN s.valid_from_release_id = ? AND b.asset_key IS NOT NULL
                            THEN b.source_schema_version ELSE s.source_schema_version END,
                       CASE WHEN s.valid_to_utc IS NULL AND s.is_eligible = 1
                            THEN b.dictionary_hash ELSE NULL END,
                       s.row_hash, s.change_type,
                       CASE WHEN s.valid_from_release_id = ?
                            THEN 'historical_boundary_signed_full'
                            ELSE s.change_origin END,
                       CASE WHEN s.valid_from_release_id = ? THEN ?
                            ELSE s.valid_from_release_id END,
                       CASE WHEN s.valid_from_release_id = ? THEN ?
                            ELSE s.valid_from_utc END,
                       CASE WHEN s.valid_to_release_id = ? THEN ?
                            ELSE s.valid_to_release_id END,
                       CASE WHEN s.valid_to_release_id = ? THEN ?
                            ELSE s.valid_to_utc END,
                       s.staged_at, s.run_id
                FROM silver_ecb_eligible_asset_version_stage s
                LEFT JOIN temp_ecb_live_boundary_version b
                  ON b.asset_key = s.asset_key AND b.valid_to_utc IS NULL
                WHERE s.run_id = ?
                """,
                (
                    stage_boundary,
                    stage_boundary,
                    stage_boundary,
                    stage_boundary,
                    live_boundary,
                    stage_boundary,
                    live_time,
                    stage_boundary,
                    live_boundary,
                    stage_boundary,
                    live_time,
                    run_id,
                ),
            )
            self.warehouse.conn.execute(
                """
                INSERT INTO silver_ecb_eligible_asset_change (
                    release_id, asset_key, declared_change_type,
                    computed_change_type, prior_row_hash, new_row_hash,
                    reconciliation_status, change_origin, publication_at_utc, run_id
                )
                SELECT CASE WHEN release_id = ? THEN ? ELSE release_id END,
                       asset_key, NULL, computed_change_type, prior_row_hash,
                       new_row_hash, reconciliation_status,
                       CASE WHEN release_id = ?
                            THEN 'historical_boundary_signed_full'
                            ELSE change_origin END,
                       CASE WHEN release_id = ? THEN ? ELSE publication_at_utc END,
                       run_id
                FROM silver_ecb_eligible_asset_change_stage
                WHERE run_id = ?
                """,
                (
                    stage_boundary,
                    live_boundary,
                    stage_boundary,
                    stage_boundary,
                    live_time,
                    run_id,
                ),
            )

            duplicate_open = self._scalar(
                "SELECT COUNT(*) AS n FROM ("
                "SELECT asset_key FROM silver_ecb_eligible_asset_version "
                "WHERE valid_to_utc IS NULL GROUP BY asset_key HAVING COUNT(*) > 1) d"
            )
            invalid_intervals = self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version "
                "WHERE valid_to_utc IS NOT NULL AND valid_to_utc <= valid_from_utc"
            )
            stage_only = self._scalar(
                "SELECT COUNT(*) AS n FROM ("
                "SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version_stage "
                "WHERE run_id = ? AND valid_to_utc IS NULL AND is_eligible = 1 "
                "EXCEPT SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version "
                "WHERE valid_to_utc IS NULL AND is_eligible = 1) x",
                (run_id,),
            )
            live_only = self._scalar(
                "SELECT COUNT(*) AS n FROM ("
                "SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version "
                "WHERE valid_to_utc IS NULL AND is_eligible = 1 "
                "EXCEPT SELECT asset_key, row_hash FROM silver_ecb_eligible_asset_version_stage "
                "WHERE run_id = ? AND valid_to_utc IS NULL AND is_eligible = 1) x",
                (run_id,),
            )
            if duplicate_open or invalid_intervals or stage_only or live_only:
                raise ECBArchivePromotionError(
                    "post-promotion invariant failed: "
                    f"duplicate_open={duplicate_open} "
                    f"invalid_intervals={invalid_intervals} "
                    f"stage_only={stage_only} live_only={live_only}"
                )
            self.warehouse.conn.execute(
                "UPDATE silver_ecb_eligible_asset_backfill_run "
                "SET status = 'promoted', updated_at = ?, error_detail = NULL "
                "WHERE run_id = ?",
                (_utc_now(), run_id),
            )
            self.warehouse.conn.commit()
        except Exception:
            self.warehouse.conn.rollback()
            raise
        finally:
            self.warehouse._defer_commits = prior_defer

        return {
            **check,
            "status": "promoted",
            "idempotent": False,
            "release_count": self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_release"
            ),
            "version_count": self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version"
            ),
            "change_count": self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_change"
            ),
            "current_asset_count": self._scalar(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version "
                "WHERE valid_to_utc IS NULL AND is_eligible = 1"
            ),
        }
