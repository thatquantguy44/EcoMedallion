"""Bounded, payload-free checks of the accepted ECB security history.

Callers provide a read snapshot or hold the live writer lock for the entire
assessment. Full signature re-verification remains the explicit verify command.
"""

from datetime import datetime, timezone

from fred_pipeline.ecb_eligible_assets import ECB_ZONE
from fred_pipeline.governance.ecb_alerting import expected_release_date


def ecb_health_snapshot(warehouse, settings, *, now=None):
    now = now or datetime.now(timezone.utc)

    def rows(sql, params=()):
        return [dict(row) for row in warehouse.conn.execute(sql, params).fetchall()]

    def scalar(sql, params=()):
        return next(iter(rows(sql, params)[0].values())) or 0

    latest_rows = rows(
        "SELECT release_id, publication_at_utc, full_count, coverage_status, reconciliation_status "
        "FROM silver_ecb_eligible_asset_release WHERE status = 'accepted' "
        "ORDER BY publication_at_utc DESC LIMIT 1"
    )
    if not latest_rows:
        return "failure", {
            "latest_release_id": None,
            "checks": {"accepted_release_present": "fail"},
        }
    latest = latest_rows[0]
    release_id = latest["release_id"]
    publication = datetime.fromisoformat(str(latest["publication_at_utc"]))
    if publication.tzinfo is None:
        publication = publication.replace(tzinfo=timezone.utc)
    current = scalar(
        "SELECT COUNT(*) FROM silver_ecb_eligible_asset_version WHERE valid_to_utc IS NULL AND is_eligible = 1"
    )
    unique = scalar(
        "SELECT COUNT(DISTINCT asset_key) FROM silver_ecb_eligible_asset_version WHERE valid_to_utc IS NULL AND is_eligible = 1"
    )
    quarantined = scalar(
        "SELECT COUNT(*) FROM bronze_ecb_eligible_asset_release WHERE status = 'quarantined' "
        "AND (publication_at_utc IS NULL OR publication_at_utc > ?)",
        (latest["publication_at_utc"],),
    )
    artifacts = rows(
        "SELECT artifact_kind, signature_status FROM bronze_ecb_eligible_asset_artifact WHERE release_id = ?",
        (release_id,),
    )
    kinds = {"full_xml", "full_csv", "update_xml", "update_csv", "dictionary_xml"}
    validated = {row["artifact_kind"] for row in artifacts} == kinds and all(
        row["signature_status"] == "verified" for row in artifacts
    )
    coverage = latest["coverage_status"]
    reconciliation = latest["reconciliation_status"]
    coverage_valid = reconciliation in {
        "complete": {"matched", "matched_with_source_noops"},
        "starts_here": {"initial_snapshot"},
        "gapped": {"derived_full_diff"},
    }.get(coverage, set())
    invalid_intervals = scalar(
        "SELECT COUNT(*) FROM (SELECT valid_from_utc, valid_to_utc, "
        "LEAD(valid_from_utc) OVER (PARTITION BY asset_key ORDER BY valid_from_utc) AS next_start "
        "FROM silver_ecb_eligible_asset_version) intervals "
        "WHERE (valid_to_utc IS NOT NULL AND valid_to_utc <= valid_from_utc) "
        "OR (next_start IS NOT NULL AND (valid_to_utc IS NULL OR valid_to_utc > next_start))"
    )
    invalid_noops = scalar(
        "SELECT COUNT(*) FROM silver_ecb_eligible_asset_change c WHERE change_origin = 'source_noop' AND ("
        "declared_change_type <> 'U' OR declared_change_type IS NULL OR computed_change_type IS NOT NULL "
        "OR prior_row_hash IS NULL OR new_row_hash IS NULL OR prior_row_hash <> new_row_hash OR EXISTS ("
        "SELECT 1 FROM silver_ecb_eligible_asset_version v WHERE v.asset_key = c.asset_key "
        "AND v.valid_from_release_id = c.release_id))"
    )
    checks = {
        "expected_release_present": "pass"
        if publication.astimezone(ECB_ZONE).date()
        >= expected_release_date(now, settings)
        else "warn",
        "no_unresolved_quarantine": "fail" if quarantined else "pass",
        "accepted_release_validated": "pass"
        if validated and coverage_valid
        else "fail",
        "current_matches_release": "pass"
        if current == unique == latest["full_count"] and current > 0
        else "fail",
        "history_intervals_valid": "fail" if invalid_intervals else "pass",
        "coverage_declared": "fail"
        if not coverage_valid
        else "warn"
        if coverage == "gapped"
        else "pass",
        "source_noop_auditable": "fail" if invalid_noops else "pass",
    }
    verdict = (
        "failure"
        if "fail" in checks.values()
        else "partial"
        if "warn" in checks.values()
        else "success"
    )
    return verdict, {
        "latest_release_id": release_id,
        "publication_at_utc": str(latest["publication_at_utc"]),
        "current_eligible_count": current,
        "limited_history": coverage == "starts_here",
        "checks": checks,
    }
