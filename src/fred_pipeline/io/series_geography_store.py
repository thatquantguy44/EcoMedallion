"""PostgreSQL persistence for reviewed series-geography associations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from fred_pipeline.catalogs.manifest import Manifest
from fred_pipeline.catalogs.series_geography import (
    SeriesGeographyCandidate,
    SeriesGeographyError,
    validate_manifest_source,
)


class PostgresSeriesGeographyRepository:
    """Writes and reviews geography candidates in PostgreSQL only.

    The parent catalog cannot enforce source-aware identity because it is keyed
    only by ``series_id``. Every write and review therefore validates the
    candidate against the supplied manifest collection before mutating storage.
    """

    def __init__(self, warehouse: Any, manifests: Iterable[Manifest]):
        self.warehouse = warehouse
        self._manifests = tuple(manifests)

    def seed_candidates(
        self, candidates: Iterable[SeriesGeographyCandidate]
    ) -> list[dict[str, Any]]:
        """Idempotently persist validated candidate evidence records."""
        candidate_list = tuple(candidates)
        for candidate in candidate_list:
            self._validate_candidate(candidate)

        now = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        try:
            with self.warehouse._pg_conn.cursor() as cur:
                for candidate in candidate_list:
                    self._assert_cataloged_series(cur, candidate.series_id)
                    cur.execute(
                        """
                        INSERT INTO meta.series_geography (
                            mapping_id, source, series_id, geo_type, geo_code,
                            country_iso3, status, evidence_kind, evidence_ref,
                            evidence_hash, observed_at_utc, mapping_version,
                            created_at_utc
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, 'candidate', %s, %s, %s,
                            %s, %s, %s
                        )
                        ON CONFLICT (
                            source, series_id, geo_type, geo_code, evidence_kind,
                            evidence_hash
                        ) DO NOTHING
                        RETURNING *
                        """,
                        (
                            str(uuid4()),
                            candidate.source,
                            candidate.series_id,
                            candidate.geo_type,
                            candidate.geo_code,
                            candidate.country_iso3,
                            candidate.evidence_kind,
                            candidate.evidence_ref,
                            candidate.evidence_hash,
                            now,
                            self._next_mapping_version(cur, candidate),
                            now,
                        ),
                    )
                    row = cur.fetchone()
                    if row is None:
                        cur.execute(
                            """
                            SELECT * FROM meta.series_geography
                            WHERE source = %s AND series_id = %s
                              AND geo_type = %s AND geo_code = %s
                              AND evidence_kind = %s AND evidence_hash = %s
                            """,
                            (
                                candidate.source,
                                candidate.series_id,
                                candidate.geo_type,
                                candidate.geo_code,
                                candidate.evidence_kind,
                                candidate.evidence_hash,
                            ),
                        )
                        row = cur.fetchone()
                    rows.append(dict(row))
            self.warehouse._pg_conn.commit()
        except Exception:
            self.warehouse._pg_conn.rollback()
            raise
        return rows

    def review_candidate(
        self, mapping_id: str, *, status: str, reviewed_by: str
    ) -> dict[str, Any]:
        """Approve or reject one candidate, superseding a same-country approval."""
        if status not in {"approved", "rejected"}:
            raise SeriesGeographyError("review status must be 'approved' or 'rejected'")
        if not reviewed_by.strip():
            raise SeriesGeographyError("reviewed_by must be non-empty")

        now = datetime.now(timezone.utc)
        try:
            with self.warehouse._pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM meta.series_geography WHERE mapping_id = %s FOR UPDATE",
                    (mapping_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise SeriesGeographyError(
                        f"unknown geography mapping {mapping_id!r}"
                    )
                record = dict(row)
                if record["status"] != "candidate":
                    raise SeriesGeographyError(
                        f"mapping {mapping_id!r} is already {record['status']!r}"
                    )
                validate_manifest_source(
                    self._manifests,
                    series_id=record["series_id"],
                    expected_source=record["source"],
                )
                self._assert_cataloged_series(cur, record["series_id"])

                if status == "approved":
                    self._supersede_prior_approval(cur, record, now)
                cur.execute(
                    """
                    UPDATE meta.series_geography
                    SET status = %s, reviewed_at_utc = %s, reviewed_by = %s
                    WHERE mapping_id = %s
                    RETURNING *
                    """,
                    (status, now, reviewed_by, mapping_id),
                )
                reviewed = dict(cur.fetchone())
            self.warehouse._pg_conn.commit()
        except Exception:
            self.warehouse._pg_conn.rollback()
            raise
        return reviewed

    def approved_countries(self) -> list[dict[str, Any]]:
        """Return the read-only approved-country query contract."""
        with self.warehouse._pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, series_id, country_iso3, evidence_kind, evidence_ref,
                       mapping_version, reviewed_at_utc
                FROM meta.v_approved_series_country
                ORDER BY source, series_id
                """
            )
            return [dict(row) for row in cur.fetchall()]

    def _validate_candidate(self, candidate: SeriesGeographyCandidate) -> None:
        if candidate.status != "candidate":
            raise SeriesGeographyError("only candidate records may be seeded")
        if candidate.geo_type != "country":
            raise SeriesGeographyError(
                "first-release seeds must have geo_type='country'"
            )
        if candidate.country_iso3 != candidate.geo_code:
            raise SeriesGeographyError(
                "country candidate country_iso3 must equal geo_code"
            )
        validate_manifest_source(
            self._manifests,
            series_id=candidate.series_id,
            expected_source=candidate.source,
        )

    @staticmethod
    def _next_mapping_version(cur: Any, candidate: SeriesGeographyCandidate) -> int:
        cur.execute(
            """
            SELECT COALESCE(MAX(mapping_version), 0) + 1 AS next_version
            FROM meta.series_geography
            WHERE source = %s AND series_id = %s
              AND geo_type = %s AND geo_code = %s
            """,
            (
                candidate.source,
                candidate.series_id,
                candidate.geo_type,
                candidate.geo_code,
            ),
        )
        return int(cur.fetchone()["next_version"])

    @staticmethod
    def _assert_cataloged_series(cur: Any, series_id: str) -> None:
        cur.execute(
            "SELECT 1 FROM meta.fred_series WHERE series_id = %s",
            (series_id,),
        )
        if cur.fetchone() is None:
            raise SeriesGeographyError(
                f"series_id {series_id!r} is not registered in meta.fred_series"
            )

    @staticmethod
    def _supersede_prior_approval(
        cur: Any, record: dict[str, Any], now: datetime
    ) -> None:
        cur.execute(
            """
            SELECT mapping_id, country_iso3
            FROM meta.series_geography
            WHERE source = %s AND series_id = %s
              AND geo_type = 'country' AND status = 'approved'
            FOR UPDATE
            """,
            (record["source"], record["series_id"]),
        )
        prior = cur.fetchone()
        if prior is None:
            return
        if prior["country_iso3"] != record["country_iso3"]:
            raise SeriesGeographyError(
                f"mapping {record['mapping_id']!r} country {record['country_iso3']!r} "
                f"conflicts with approved country {prior['country_iso3']!r}"
            )
        cur.execute(
            """
            UPDATE meta.series_geography
            SET status = 'superseded', superseded_at_utc = %s
            WHERE mapping_id = %s
            """,
            (now, prior["mapping_id"]),
        )
