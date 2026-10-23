"""Spark/Delta persistence helpers for spec011 historical staging/promotion.

Historical parsing and diffing remain backend-neutral.  This writer replaces a
single run partition in each Delta staging table, making retries idempotent and
keeping incomplete work isolated from the live consumer tables.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


DELTA_STAGE_TABLES = (
    "ecb_eligible_asset_backfill_run",
    "ecb_eligible_asset_release_stage",
    "ecb_eligible_asset_version_stage",
    "ecb_eligible_asset_change_stage",
)


class ECBDeltaWriteError(RuntimeError):
    """Delta staging input or post-write validation is invalid."""


def _quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class ECBDeltaStageWriter:
    """Idempotently persist a complete run image to run-partitioned Delta."""

    def __init__(self, spark: Any, config: Any):
        self.spark = spark
        self.config = config

    def _table(self, name: str) -> str:
        return self.config.table("silver", name)

    def _source_frame(self, rows: Sequence[Mapping[str, Any]]) -> Any:
        """Build an inference-free frame so all-NULL columns remain valid."""
        from pyspark.sql.types import StringType, StructField, StructType

        columns = sorted({key for row in rows for key in row})
        schema = StructType(
            [StructField(column, StringType(), True) for column in columns]
        )

        def text_value(value: Any) -> str | None:
            if value is None:
                return None
            if isinstance(value, bool):
                return "true" if value else "false"
            if isinstance(value, (date, datetime)):
                return value.isoformat()
            return str(value)

        normalized = [
            {column: text_value(row.get(column)) for column in columns} for row in rows
        ]
        return self.spark.createDataFrame(normalized, schema=schema)

    def _replace_partition(
        self, table_name: str, run_id: str, rows: Sequence[Mapping[str, Any]]
    ) -> int:
        table = self._table(table_name)
        if not rows:
            self.spark.sql(f"DELETE FROM {table} WHERE run_id = {_quoted(run_id)}")
            return 0
        target = self.spark.table(table)
        source = self._source_frame(rows)
        from pyspark.sql import functions as F

        expressions = []
        for field in target.schema.fields:
            column = F.col(field.name) if field.name in source.columns else F.lit(None)
            expressions.append(column.cast(field.dataType).alias(field.name))
        prepared = source.select(*expressions)
        predicate = f"run_id = {_quoted(run_id)}"
        (
            prepared.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", predicate)
            .saveAsTable(table)
        )
        return len(rows)

    def replace_run(
        self,
        *,
        run_id: str,
        run_row: Mapping[str, Any],
        releases: Sequence[Mapping[str, Any]],
        versions: Sequence[Mapping[str, Any]],
        changes: Sequence[Mapping[str, Any]],
    ) -> dict[str, int]:
        if run_row.get("run_id") != run_id:
            raise ECBDeltaWriteError("run ledger row does not match run_id")
        if run_row.get("status") != "staged":
            raise ECBDeltaWriteError("run ledger row must have status 'staged'")
        groups = {
            "ecb_eligible_asset_release_stage": releases,
            "ecb_eligible_asset_version_stage": versions,
            "ecb_eligible_asset_change_stage": changes,
        }
        for name, rows in groups.items():
            if any(row.get("run_id") != run_id for row in rows):
                raise ECBDeltaWriteError(f"{name} contains a different run_id")

        # Invalidate the prior control record before touching evidence. The run
        # ledger is then written last, so a failure cannot make a partial retry
        # appear promotable.
        self.spark.sql(
            "DELETE FROM "
            f"{self._table('ecb_eligible_asset_backfill_run')} "
            f"WHERE run_id = {_quoted(run_id)}"
        )
        counts = {
            name: self._replace_partition(name, run_id, rows)
            for name, rows in groups.items()
        }
        counts["ecb_eligible_asset_backfill_run"] = self._replace_partition(
            "ecb_eligible_asset_backfill_run", run_id, [run_row]
        )
        return counts

    def replace_run_batches(
        self,
        *,
        run_id: str,
        run_row: Mapping[str, Any],
        releases: Iterable[Mapping[str, Any]],
        versions: Iterable[Mapping[str, Any]],
        changes: Iterable[Mapping[str, Any]],
        batch_size: int = 50_000,
    ) -> dict[str, int]:
        """Stream a large run image into Delta without driver-wide buffering."""
        if batch_size <= 0:
            raise ECBDeltaWriteError("batch_size must be greater than zero")
        if run_row.get("run_id") != run_id or run_row.get("status") != "staged":
            raise ECBDeltaWriteError("run ledger row must match run_id and be staged")
        ledger = self._table("ecb_eligible_asset_backfill_run")
        self.spark.sql(f"DELETE FROM {ledger} WHERE run_id = {_quoted(run_id)}")
        counts: dict[str, int] = {}
        for name, source in (
            ("ecb_eligible_asset_release_stage", releases),
            ("ecb_eligible_asset_version_stage", versions),
            ("ecb_eligible_asset_change_stage", changes),
        ):
            counts[name] = self._replace_iterable(name, run_id, source, batch_size)
        counts["ecb_eligible_asset_backfill_run"] = self._replace_partition(
            "ecb_eligible_asset_backfill_run", run_id, [run_row]
        )
        return counts

    def _replace_iterable(
        self,
        table_name: str,
        run_id: str,
        rows: Iterable[Mapping[str, Any]],
        batch_size: int,
    ) -> int:
        iterator = iter(rows)
        batch: list[Mapping[str, Any]] = []
        total = 0
        first = True
        table = self._table(table_name)
        target = self.spark.table(table)
        from pyspark.sql import functions as F

        while True:
            try:
                while len(batch) < batch_size:
                    row = next(iterator)
                    if row.get("run_id") != run_id:
                        raise ECBDeltaWriteError(
                            f"{table_name} contains a different run_id"
                        )
                    batch.append(row)
            except StopIteration:
                pass
            if not batch:
                if first:
                    self.spark.sql(
                        f"DELETE FROM {table} WHERE run_id = {_quoted(run_id)}"
                    )
                break
            source = self._source_frame(batch)
            prepared = source.select(
                *[
                    (F.col(field.name) if field.name in source.columns else F.lit(None))
                    .cast(field.dataType)
                    .alias(field.name)
                    for field in target.schema.fields
                ]
            )
            writer = prepared.write.format("delta")
            if first:
                writer = writer.mode("overwrite").option(
                    "replaceWhere", f"run_id = {_quoted(run_id)}"
                )
            else:
                writer = writer.mode("append")
            writer.saveAsTable(table)
            total += len(batch)
            batch = []
            first = False
        return total

    def parity_summary(self, run_id: str) -> dict[str, int]:
        stage_versions = self._table("ecb_eligible_asset_version_stage")
        live_versions = self._table("ecb_eligible_asset_version")
        run = _quoted(run_id)

        def scalar(sql: str) -> int:
            row = self.spark.sql(sql).collect()[0]
            return int(row[0] or 0)

        return {
            "stage_only": scalar(
                "SELECT COUNT(*) FROM ("
                f"SELECT asset_key, row_hash FROM {stage_versions} "
                f"WHERE run_id = {run} AND valid_to_utc IS NULL AND is_eligible = true "
                "EXCEPT SELECT asset_key, row_hash FROM "
                f"{live_versions} WHERE valid_to_utc IS NULL AND is_eligible = true)"
            ),
            "live_only": scalar(
                "SELECT COUNT(*) FROM ("
                f"SELECT asset_key, row_hash FROM {live_versions} "
                "WHERE valid_to_utc IS NULL AND is_eligible = true EXCEPT "
                f"SELECT asset_key, row_hash FROM {stage_versions} "
                f"WHERE run_id = {run} AND valid_to_utc IS NULL AND is_eligible = true)"
            ),
            "duplicate_open": scalar(
                "SELECT COUNT(*) FROM (SELECT asset_key FROM "
                f"{stage_versions} WHERE run_id = {run} AND valid_to_utc IS NULL "
                "GROUP BY asset_key HAVING COUNT(*) > 1)"
            ),
            "invalid_intervals": scalar(
                f"SELECT COUNT(*) FROM {stage_versions} WHERE run_id = {run} "
                "AND valid_to_utc IS NOT NULL AND valid_to_utc <= valid_from_utc"
            ),
        }


class ECBDeltaHistoricalStagingRebuilder:
    """Build history on bounded local disk, then stream it into Delta.

    Snapshot normalization/diffing is intentionally shared with the proven SQL
    implementation. A temporary SQLite spool avoids retaining the multi-million
    row history in driver memory; rows are transferred to Delta in batches.
    """

    def __init__(
        self,
        warehouse: Any,
        *,
        registry: Any | None = None,
        registry_path: str | Path,
        batch_size: int = 50_000,
    ):
        if not hasattr(warehouse, "spark"):
            raise TypeError("Delta historical staging requires SparkWarehouse")
        self.warehouse = warehouse
        self.registry = registry
        self.registry_path = Path(registry_path)
        self.batch_size = batch_size

    @staticmethod
    def _iter_rows(
        warehouse: Any, table: str, run_id: str, *, batch_size: int
    ) -> Iterable[dict[str, Any]]:
        cursor = warehouse.conn.execute(
            f"SELECT * FROM {table} WHERE run_id = ?", (run_id,)
        )
        while rows := cursor.fetchmany(batch_size):
            for row in rows:
                yield dict(row)

    def rebuild(
        self,
        snapshots: Iterable[Any],
        *,
        run_id: str,
        from_date: date,
        to_date: date,
        restart: bool = False,
    ) -> dict[str, Any]:
        from fred_pipeline.ecb_archive_backfill import (
            ECBHistoricalStagingRebuilder,
        )
        from fred_pipeline.io.local_store import LocalWarehouse

        with TemporaryDirectory(prefix="ecb-delta-stage-") as temporary:
            local = LocalWarehouse(
                self.warehouse.config,
                db_path=str(Path(temporary) / "history.sqlite"),
            )
            try:
                builder = ECBHistoricalStagingRebuilder(
                    local,
                    registry=self.registry,
                    registry_path=self.registry_path,
                )
                result = builder.rebuild(
                    snapshots,
                    run_id=run_id,
                    from_date=from_date,
                    to_date=to_date,
                    restart=restart,
                )
                run_row = local.query(
                    "SELECT * FROM silver_ecb_eligible_asset_backfill_run "
                    "WHERE run_id = ?",
                    (run_id,),
                )[0]
                writer = ECBDeltaStageWriter(
                    self.warehouse.spark, self.warehouse.config
                )
                counts = writer.replace_run_batches(
                    run_id=run_id,
                    run_row=run_row,
                    releases=self._iter_rows(
                        local,
                        "silver_ecb_eligible_asset_release_stage",
                        run_id,
                        batch_size=self.batch_size,
                    ),
                    versions=self._iter_rows(
                        local,
                        "silver_ecb_eligible_asset_version_stage",
                        run_id,
                        batch_size=self.batch_size,
                    ),
                    changes=self._iter_rows(
                        local,
                        "silver_ecb_eligible_asset_change_stage",
                        run_id,
                        batch_size=self.batch_size,
                    ),
                    batch_size=self.batch_size,
                )
                return {**result, "delta_stage_counts": counts}
            finally:
                local.close()


class ECBDeltaEligibleAssetReader:
    """Read the four spec011 consumer shapes from Spark/Delta."""

    def __init__(self, spark: Any, config: Any):
        self.spark = spark
        self.config = config

    def _table(self, name: str) -> str:
        return self.config.table("silver", name)

    def _rows(self, sql: str) -> list[dict[str, Any]]:
        return [row.asDict(recursive=True) for row in self.spark.sql(sql).collect()]

    def history(self, *, asset_key: str | None = None) -> list[dict[str, Any]]:
        where = ""
        if asset_key is not None:
            where = f" WHERE asset_key = {_quoted(asset_key)}"
        return self._rows(
            f"SELECT * FROM {self._table('ecb_eligible_asset_version')}"
            f"{where} ORDER BY asset_key, valid_from_utc"
        )

    def current(self) -> list[dict[str, Any]]:
        return self._rows(
            f"SELECT * FROM {self._table('ecb_eligible_asset_version')} "
            "WHERE valid_to_utc IS NULL AND is_eligible = true ORDER BY asset_key"
        )

    def changes(self) -> list[dict[str, Any]]:
        return self._rows(
            f"SELECT * FROM {self._table('ecb_eligible_asset_change')} "
            "ORDER BY publication_at_utc, asset_key"
        )

    def as_of(self, timestamp: str) -> list[dict[str, Any]]:
        cutoff = _quoted(timestamp)
        release = self._table("ecb_eligible_asset_release")
        version = self._table("ecb_eligible_asset_version")
        # Keep the selected timestamp inside Spark SQL. Collecting it into a
        # Python datetime and interpolating it back can silently shift it when
        # the Spark session time zone is not UTC.
        return self._rows(
            "SELECT v.*, r.release_id AS selected_release_id, "
            "r.publication_at_utc AS selected_release_publication_at_utc, "
            f"r.coverage_status FROM {version} v CROSS JOIN ("
            "SELECT release_id, publication_at_utc, coverage_status "
            f"FROM {release} WHERE status = 'accepted' "
            f"AND publication_at_utc <= CAST({cutoff} AS TIMESTAMP) "
            "ORDER BY publication_at_utc DESC LIMIT 1) r "
            "WHERE v.valid_from_utc <= r.publication_at_utc "
            "AND (v.valid_to_utc IS NULL OR r.publication_at_utc < v.valid_to_utc) "
            "AND v.is_eligible = true ORDER BY v.asset_key"
        )


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (date, datetime)):
        value = value.isoformat()
    return _quoted(str(value))


class ECBDeltaPromoter:
    """Promote one staged run using a Databricks multi-table transaction.

    Open-source Delta provides table-level transactions only. Production
    promotion therefore deliberately uses ``BEGIN ATOMIC``, which requires
    Unity Catalog managed tables with catalog commits and supported Databricks
    compute. Unsupported runtimes reject the statement before any write.
    """

    def __init__(self, spark: Any, config: Any):
        self.spark = spark
        self.config = config

    def _table(self, name: str) -> str:
        return self.config.table("silver", name)

    def _rows(self, sql: str) -> list[dict[str, Any]]:
        return [row.asDict(recursive=True) for row in self.spark.sql(sql).collect()]

    def _scalar(self, sql: str) -> int:
        rows = self.spark.sql(sql).collect()
        return int(rows[0][0] or 0)

    def preflight(
        self, run_id: str, *, acknowledge_quarantines: int | None
    ) -> dict[str, Any]:
        run = _quoted(run_id)
        ledger = self._table("ecb_eligible_asset_backfill_run")
        release_stage = self._table("ecb_eligible_asset_release_stage")
        live_release = self._table("ecb_eligible_asset_release")
        live_version = self._table("ecb_eligible_asset_version")
        live_change = self._table("ecb_eligible_asset_change")
        runs = self._rows(f"SELECT * FROM {ledger} WHERE run_id = {run}")
        if not runs:
            raise ECBDeltaWriteError(f"unknown staging run {run_id!r}")
        run_row = runs[0]
        if run_row["status"] not in {"staged", "promoted"}:
            raise ECBDeltaWriteError(
                f"staging run {run_id!r} has status {run_row['status']!r}"
            )
        quarantines = self._scalar(
            f"SELECT COUNT(*) FROM {release_stage} "
            f"WHERE run_id = {run} AND status = 'quarantined'"
        )
        if acknowledge_quarantines is None or acknowledge_quarantines != quarantines:
            raise ECBDeltaWriteError(
                "promotion requires the exact quarantine acknowledgement "
                f"({quarantines})"
            )
        boundaries = self._rows(
            f"SELECT * FROM {release_stage} WHERE run_id = {run} "
            "AND status = 'accepted' ORDER BY snapshot_date DESC LIMIT 1"
        )
        if not boundaries:
            raise ECBDeltaWriteError("staging run has no accepted boundary")
        boundary = boundaries[0]
        snapshot_day = _sql_literal(boundary["snapshot_date"])
        live = self._rows(
            f"SELECT * FROM {live_release} WHERE status = 'accepted' "
            f"AND CAST(publication_at_utc AS DATE) = {snapshot_day}"
        )
        if len(live) != 1:
            raise ECBDeltaWriteError(
                "expected exactly one signed live release at staged boundary; "
                f"found {len(live)}"
            )
        live_boundary = live[0]
        live_time = _sql_literal(live_boundary["publication_at_utc"])
        later = self._scalar(
            f"SELECT COUNT(*) FROM {live_release} "
            f"WHERE status = 'accepted' AND publication_at_utc > {live_time}"
        )
        parity = ECBDeltaStageWriter(self.spark, self.config).parity_summary(run_id)
        if later or any(parity.values()):
            raise ECBDeltaWriteError(
                f"promotion preflight failed: later_live={later}, parity={parity}"
            )
        if run_row["status"] != "promoted":
            live_release_count = self._scalar(f"SELECT COUNT(*) FROM {live_release}")
            live_id = _sql_literal(live_boundary["release_id"])
            nonboundary_versions = self._scalar(
                f"SELECT COUNT(*) FROM {live_version} "
                f"WHERE valid_from_release_id <> {live_id}"
            )
            nonboundary_changes = self._scalar(
                f"SELECT COUNT(*) FROM {live_change} WHERE release_id <> {live_id}"
            )
            if live_release_count != 1 or nonboundary_versions or nonboundary_changes:
                raise ECBDeltaWriteError(
                    "live Silver is not the expected single-release bootstrap state"
                )
        dictionary_rows = self._rows(
            f"SELECT DISTINCT dictionary_hash FROM {live_version} "
            "WHERE valid_to_utc IS NULL AND is_eligible = true"
        )
        if len(dictionary_rows) != 1 or not dictionary_rows[0].get("dictionary_hash"):
            raise ECBDeltaWriteError(
                "signed live boundary must have exactly one dictionary hash"
            )
        dictionary_hash = dictionary_rows[0]["dictionary_hash"]
        return {
            "run_id": run_id,
            "status": run_row["status"],
            "quarantined_count": quarantines,
            "stage_boundary_release_id": boundary["release_id"],
            "live_boundary_release_id": live_boundary["release_id"],
            "live_publication_at_utc": live_boundary["publication_at_utc"],
            "dictionary_hash": dictionary_hash,
            **parity,
        }

    def atomic_sql(self, check: Mapping[str, Any]) -> str:
        """Return the single transactional statement used for promotion."""
        run = _sql_literal(check["run_id"])
        stage_boundary = _sql_literal(check["stage_boundary_release_id"])
        live_boundary = _sql_literal(check["live_boundary_release_id"])
        live_time = _sql_literal(check["live_publication_at_utc"])
        dictionary_hash = _sql_literal(check.get("dictionary_hash"))
        ledger = self._table("ecb_eligible_asset_backfill_run")
        release_stage = self._table("ecb_eligible_asset_release_stage")
        version_stage = self._table("ecb_eligible_asset_version_stage")
        change_stage = self._table("ecb_eligible_asset_change_stage")
        live_release = self._table("ecb_eligible_asset_release")
        live_version = self._table("ecb_eligible_asset_version")
        live_change = self._table("ecb_eligible_asset_change")
        return f"""BEGIN ATOMIC
  DELETE FROM {live_release} WHERE release_id <> {live_boundary};
  INSERT INTO {live_release}
  SELECT release_id, source_url, publication_at_local, publication_at_local,
         publication_at_utc, NULL, prior_release_id, is_consecutive, full_count,
         NULL, new_count, updated_count, deleted_count, parser_version,
         source_schema_hash, NULL, reconciliation_status, coverage_status,
         timestamp_precision, status, staged_at, run_id
  FROM {release_stage}
  WHERE run_id = {run} AND release_id <> {stage_boundary};
  MERGE INTO {live_release} AS target
  USING (SELECT * FROM {release_stage}
         WHERE run_id = {run} AND release_id = {stage_boundary}) AS source
  ON target.release_id = {live_boundary}
  WHEN MATCHED THEN UPDATE SET
    target.prior_release_id = source.prior_release_id,
    target.is_consecutive = source.is_consecutive,
    target.full_count = source.full_count,
    target.new_count = source.new_count,
    target.updated_count = source.updated_count,
    target.deleted_count = source.deleted_count,
    target.coverage_status = 'complete';
  INSERT OVERWRITE TABLE {live_version}
  SELECT asset_key, isin_code, other_reg_number, is_eligible, haircut_category,
         asset_type, reference_market, denomination, issuance_date, maturity_date,
         issuer_csd, coupon_definition, coupon_rate, issuer_name, issuer_group,
         issuer_residence, guarantor_name, guarantor_group, guarantor_residence,
         non_own_use_haircut, own_use_haircut,
         potentially_own_usable_covered_bond, climate_factor, source_fields_json,
         source_schema_version,
         CASE WHEN valid_to_utc IS NULL AND is_eligible THEN {dictionary_hash}
              ELSE NULL END,
         row_hash, change_type,
         CASE WHEN valid_from_release_id = {stage_boundary}
              THEN 'historical_boundary_signed_full' ELSE change_origin END,
         CASE WHEN valid_from_release_id = {stage_boundary}
              THEN {live_boundary} ELSE valid_from_release_id END,
         CASE WHEN valid_from_release_id = {stage_boundary}
              THEN {live_time} ELSE valid_from_utc END,
         CASE WHEN valid_to_release_id = {stage_boundary}
              THEN {live_boundary} ELSE valid_to_release_id END,
         CASE WHEN valid_to_release_id = {stage_boundary}
              THEN {live_time} ELSE valid_to_utc END,
         staged_at, run_id
  FROM {version_stage} WHERE run_id = {run};
  INSERT OVERWRITE TABLE {live_change}
  SELECT CASE WHEN release_id = {stage_boundary}
              THEN {live_boundary} ELSE release_id END,
         asset_key, NULL, computed_change_type, prior_row_hash, new_row_hash,
         reconciliation_status,
         CASE WHEN release_id = {stage_boundary}
              THEN 'historical_boundary_signed_full' ELSE change_origin END,
         CASE WHEN release_id = {stage_boundary}
              THEN {live_time} ELSE publication_at_utc END,
         run_id
  FROM {change_stage} WHERE run_id = {run};
  UPDATE {ledger} SET status = 'promoted', updated_at = current_timestamp(),
         error_detail = NULL WHERE run_id = {run};
END"""

    def promote(
        self, run_id: str, *, acknowledge_quarantines: int | None
    ) -> dict[str, Any]:
        check = self.preflight(run_id, acknowledge_quarantines=acknowledge_quarantines)
        if check["status"] == "promoted":
            return {**check, "status": "promoted", "idempotent": True}
        try:
            self.spark.sql(self.atomic_sql(check)).collect()
        except Exception as exc:
            raise ECBDeltaWriteError(
                "atomic Delta promotion failed; use Databricks Runtime 18.0+ "
                "with Unity Catalog managed tables and catalog commits enabled"
            ) from exc
        post = ECBDeltaStageWriter(self.spark, self.config).parity_summary(run_id)
        if any(post.values()):
            raise ECBDeltaWriteError(f"post-promotion parity failed: {post}")
        return {**check, **post, "status": "promoted", "idempotent": False}
