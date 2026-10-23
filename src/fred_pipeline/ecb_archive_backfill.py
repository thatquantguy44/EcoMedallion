"""Resumable, stage-only historical rebuild for ECB eligible assets.

The rebuild intentionally writes to dedicated ``*_stage`` tables.  It never
promotes into the live forward-ingestion tables; promotion is a later Phase 2
step that must first reconcile the staged boundary to a signed live snapshot.
"""

from __future__ import annotations

import hashlib
import logging
import re
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping, Sequence

from fred_pipeline.ecb_archive_inventory import (
    ARCHIVE_URL_TEMPLATE,
    DAILY_URL_TEMPLATE,
    DEFAULT_MAX_ZIP_UNCOMPRESSED_BYTES,
    DOWNLOAD_PAGE_URL,
    MIN_ARCHIVE_YEAR,
    ECBArchiveInventoryClient,
    _date_range,
    _safe_member_name,
    _snapshot_date,
)
from fred_pipeline.ecb_asset_schema_registry import (
    DEFAULT_REGISTRY_PATH,
    HistoricalSchemaRegistry,
    load_schema_registry,
    parse_historical_assets_csv,
)
from fred_pipeline.ecb_calendar import is_target_operating_day
from fred_pipeline.ecb_eligible_assets import (
    ECB_ZONE,
    _VERSION_COLUMNS,
    _canonical_hash,
    _utc_now,
    diff_assets,
)

log = logging.getLogger("fred_pipeline.ecb_archive_backfill")

HISTORICAL_PARSER_VERSION = "spec011-history-v1"
_ARCHIVE_MEMBER_RE = re.compile(
    r"^ea_(?:all|csv)_(?P<stamp>\d{6})(?P<suffix>\.tgz|\.tar\.gz|\.csv\.gz|\.txt\.gz)$",
    re.IGNORECASE,
)


class ECBArchiveBackfillError(RuntimeError):
    """The historical staging rebuild cannot safely continue."""


@dataclass(frozen=True)
class HistoricalSnapshot:
    snapshot_date: date
    source_url: str
    entry_path: str
    entry_sha256: str
    archive_sha256: str | None
    payload: bytes


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _unwrap_snapshot(entry_name: str, entry_bytes: bytes) -> bytes:
    lower = entry_name.lower()
    if lower.endswith((".csv.gz", ".txt.gz")):
        return entry_bytes
    if not lower.endswith((".tgz", ".tar.gz")):
        raise ECBArchiveBackfillError(f"unsupported snapshot {entry_name!r}")
    with tarfile.open(fileobj=BytesIO(entry_bytes), mode="r:gz") as archive:
        payloads = []
        for member in archive.getmembers():
            if not _safe_member_name(member.name):
                raise ECBArchiveBackfillError(
                    f"unsafe path in nested archive: {member.name!r}"
                )
            if member.isfile() and member.name.lower().endswith((".txt", ".csv")):
                payloads.append(member)
        if len(payloads) != 1:
            raise ECBArchiveBackfillError(
                f"nested archive {entry_name!r} contains {len(payloads)} data files"
            )
        stream = archive.extractfile(payloads[0])
        if stream is None:
            raise ECBArchiveBackfillError(
                f"cannot extract {payloads[0].name!r} from {entry_name!r}"
            )
        return stream.read()


def iter_archive_zip(
    path: str | Path,
    *,
    year: int,
    source_url: str | None = None,
    archive_sha256: str | None = None,
    max_uncompressed_bytes: int = DEFAULT_MAX_ZIP_UNCOMPRESSED_BYTES,
) -> Iterator[HistoricalSnapshot]:
    """Yield one annual ZIP's snapshots in strict date order."""
    archive_path = Path(path)
    archive_hash = archive_sha256 or _sha256_file(archive_path)
    url = source_url or ARCHIVE_URL_TEMPLATE.format(year=year)
    with zipfile.ZipFile(archive_path) as archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]
        unsafe = [
            info.filename for info in infos if not _safe_member_name(info.filename)
        ]
        if unsafe:
            raise ECBArchiveBackfillError(
                f"archive contains unsafe paths: {unsafe[:10]}"
            )
        declared = sum(info.file_size for info in infos)
        if declared > max_uncompressed_bytes:
            raise ECBArchiveBackfillError(
                f"archive declares {declared} bytes above the safety limit"
            )
        dated: list[tuple[date, zipfile.ZipInfo]] = []
        for info in infos:
            match = _ARCHIVE_MEMBER_RE.match(PurePosixPath(info.filename).name)
            if match:
                dated.append((_snapshot_date(match.group("stamp"), year), info))
        dated.sort(key=lambda value: (value[0], value[1].filename))
        for snapshot_day, info in dated:
            entry_bytes = archive.read(info)
            yield HistoricalSnapshot(
                snapshot_date=snapshot_day,
                source_url=f"{url}#{info.filename}",
                entry_path=info.filename,
                entry_sha256=hashlib.sha256(entry_bytes).hexdigest(),
                archive_sha256=archive_hash,
                payload=_unwrap_snapshot(info.filename, entry_bytes),
            )


class ECBArchiveSnapshotSource:
    """Stream annual archives one at a time, optionally using a local cache."""

    def __init__(
        self,
        client: ECBArchiveInventoryClient,
        *,
        archive_dir: str | Path | None = None,
    ):
        self.client = client
        self.archive_dir = Path(archive_dir) if archive_dir else None

    def _cached_annual_path(self, year: int) -> Path | None:
        if self.archive_dir is None:
            return None
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        target = self.archive_dir / f"{year}.zip"
        if target.exists():
            if target.stat().st_size > self.client.max_archive_bytes:
                raise ECBArchiveBackfillError(
                    f"cached archive {target} exceeds the configured size limit"
                )
            return target
        temporary = self.archive_dir / f".{year}.zip.partial"
        try:
            self.client._download(  # internal transport shared with inventory
                ARCHIVE_URL_TEMPLATE.format(year=year), temporary
            )
            temporary.replace(target)
        finally:
            if temporary.exists():
                temporary.unlink()
        return target

    def _iter_completed_year(self, year: int) -> Iterator[HistoricalSnapshot]:
        cached = self._cached_annual_path(year)
        if cached is not None:
            yield from iter_archive_zip(cached, year=year)
            return
        url = ARCHIVE_URL_TEMPLATE.format(year=year)
        with tempfile.TemporaryDirectory(prefix=f"ecb-backfill-{year}-") as tmp:
            downloaded = self.client._download(url, Path(tmp) / f"{year}.zip")
            yield from iter_archive_zip(
                downloaded.path,
                year=year,
                source_url=url,
                archive_sha256=downloaded.sha256,
            )

    def _daily_cache_path(self, candidate: date) -> Path | None:
        if self.archive_dir is None:
            return None
        directory = self.archive_dir / str(candidate.year) / candidate.strftime("%m")
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"ea_csv_{candidate.strftime('%y%m%d')}.csv.gz"

    def _iter_current_year(self, year: int) -> Iterator[HistoricalSnapshot]:
        page_result = self.client._fetch_bytes(DOWNLOAD_PAGE_URL)
        if page_result is None:  # pragma: no cover - allow_missing is false
            raise ECBArchiveBackfillError("ECB download page was not returned")
        page, _headers = page_result
        stamps = [
            value.decode("ascii")
            for value in re.findall(rb"ea_csv_(\d{6})\.csv(?:\.gz)?", page)
            if value.startswith(f"{year % 100:02d}".encode("ascii"))
        ]
        if not stamps:
            raise ECBArchiveBackfillError(
                f"download page exposes no current snapshot for {year}"
            )
        latest = _snapshot_date(max(stamps), year)
        for candidate in _date_range(date(year, 1, 1), latest):
            if not is_target_operating_day(candidate):
                continue
            stamp = candidate.strftime("%y%m%d")
            url = DAILY_URL_TEMPLATE.format(
                year=year, month=candidate.strftime("%m"), stamp=stamp
            )
            cached = self._daily_cache_path(candidate)
            if cached is not None and cached.exists():
                if cached.stat().st_size > self.client.max_archive_bytes:
                    raise ECBArchiveBackfillError(
                        f"cached snapshot {cached} exceeds the configured size limit"
                    )
                payload = cached.read_bytes()
            else:
                fetched = self.client._fetch_bytes(url, allow_missing=True)
                if fetched is None:
                    continue
                payload, _headers = fetched
                if cached is not None:
                    temporary = cached.with_suffix(cached.suffix + ".partial")
                    temporary.write_bytes(payload)
                    temporary.replace(cached)
            yield HistoricalSnapshot(
                snapshot_date=candidate,
                source_url=url,
                entry_path=url,
                entry_sha256=hashlib.sha256(payload).hexdigest(),
                archive_sha256=None,
                payload=payload,
            )

    def iter_snapshots(
        self, *, from_year: int = MIN_ARCHIVE_YEAR, to_year: int | None = None
    ) -> Iterator[HistoricalSnapshot]:
        current_year = datetime.now(timezone.utc).year
        to_year = to_year or current_year
        if from_year < MIN_ARCHIVE_YEAR or to_year < from_year:
            raise ECBArchiveBackfillError("invalid archive year range")
        if to_year > current_year:
            raise ECBArchiveBackfillError(
                f"to_year must be <= current year {current_year}"
            )
        for year in range(from_year, to_year + 1):
            log.info("Streaming ECB archive snapshots for %d", year)
            if year == current_year:
                yield from self._iter_current_year(year)
            else:
                yield from self._iter_completed_year(year)


def _publication_times(snapshot_date: date) -> tuple[str, str]:
    local = datetime.combine(snapshot_date, time(23, 59, 59), tzinfo=ECB_ZONE)
    return local.isoformat(), local.astimezone(timezone.utc).isoformat()


def _registry_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ECBHistoricalStageRepository:
    """Transactional persistence for an isolated historical rebuild run."""

    def __init__(self, warehouse: Any):
        if not hasattr(warehouse, "conn") or not hasattr(warehouse, "_insert"):
            raise TypeError("historical staging requires SQLite or PostgreSQL")
        self.warehouse = warehouse

    def _rows(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        result = self.warehouse.conn.execute(sql, params)
        return [dict(row) for row in result.fetchall()]

    def restart(self, run_id: str) -> None:
        prior_defer = bool(getattr(self.warehouse, "_defer_commits", False))
        self.warehouse._defer_commits = True
        try:
            for table in (
                "silver_ecb_eligible_asset_change_stage",
                "silver_ecb_eligible_asset_version_stage",
                "silver_ecb_eligible_asset_release_stage",
                "silver_ecb_eligible_asset_backfill_run",
            ):
                self.warehouse.conn.execute(
                    f"DELETE FROM {table} WHERE run_id = ?", (run_id,)
                )
            self.warehouse.conn.commit()
        except Exception:
            self.warehouse.conn.rollback()
            raise
        finally:
            self.warehouse._defer_commits = prior_defer

    def begin(
        self,
        *,
        run_id: str,
        from_date: date,
        to_date: date,
        registry_sha256: str,
    ) -> dict[str, Any]:
        existing = self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_backfill_run WHERE run_id = ?",
            (run_id,),
        )
        if existing:
            row = existing[0]
            if (
                row["requested_from_date"] != from_date.isoformat()
                or row["registry_sha256"] != registry_sha256
                or to_date < date.fromisoformat(row["requested_to_date"])
            ):
                raise ECBArchiveBackfillError(
                    f"run {run_id!r} parameters changed; use --restart or a new run id"
                )
            if to_date > date.fromisoformat(row["requested_to_date"]):
                self.warehouse.conn.execute(
                    "UPDATE silver_ecb_eligible_asset_backfill_run SET "
                    "requested_to_date = ?, status = 'running', updated_at = ?, "
                    "completed_at = NULL WHERE run_id = ?",
                    (to_date.isoformat(), _utc_now(), run_id),
                )
                self.warehouse.conn.commit()
                return self._rows(
                    "SELECT * FROM silver_ecb_eligible_asset_backfill_run "
                    "WHERE run_id = ?",
                    (run_id,),
                )[0]
            return row
        now = _utc_now()
        self.warehouse._insert(
            "silver_ecb_eligible_asset_backfill_run",
            [
                {
                    "run_id": run_id,
                    "requested_from_date": from_date.isoformat(),
                    "requested_to_date": to_date.isoformat(),
                    "registry_sha256": registry_sha256,
                    "status": "running",
                    "snapshot_count": 0,
                    "missing_count": 0,
                    "quarantined_count": 0,
                    "version_count": 0,
                    "change_count": 0,
                    "final_asset_count": 0,
                    "started_at": now,
                    "updated_at": now,
                    "completed_at": None,
                    "error_detail": None,
                }
            ],
        )
        return self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_backfill_run WHERE run_id = ?",
            (run_id,),
        )[0]

    def releases(self, run_id: str) -> dict[str, dict[str, Any]]:
        return {
            row["snapshot_date"]: row
            for row in self._rows(
                "SELECT * FROM silver_ecb_eligible_asset_release_stage "
                "WHERE run_id = ? ORDER BY snapshot_date",
                (run_id,),
            )
        }

    def latest_accepted(self, run_id: str) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_release_stage "
            "WHERE run_id = ? AND status = 'accepted' "
            "ORDER BY snapshot_date DESC LIMIT 1",
            (run_id,),
        )
        return rows[0] if rows else None

    def current_assets(self, run_id: str) -> dict[str, dict[str, Any]]:
        return {
            row["asset_key"]: row
            for row in self._rows(
                "SELECT * FROM silver_ecb_eligible_asset_version_stage "
                "WHERE run_id = ? AND valid_to_utc IS NULL AND is_eligible = 1",
                (run_id,),
            )
        }

    def delete_nonaccepted_release(self, run_id: str, release_id: str) -> None:
        self.warehouse.conn.execute(
            "DELETE FROM silver_ecb_eligible_asset_release_stage "
            "WHERE run_id = ? AND release_id = ? AND status <> 'accepted'",
            (run_id, release_id),
        )
        self.warehouse.conn.commit()

    def stage_missing(
        self, run_id: str, missing_date: date, *, prior_release_id: str | None
    ) -> None:
        release_id = f"archive-missing:{missing_date.isoformat()}"
        local, utc = _publication_times(missing_date)
        self.warehouse._insert(
            "silver_ecb_eligible_asset_release_stage",
            [
                {
                    "run_id": run_id,
                    "release_id": release_id,
                    "snapshot_date": missing_date.isoformat(),
                    "publication_at_local": local,
                    "publication_at_utc": utc,
                    "timestamp_precision": "date",
                    "prior_release_id": prior_release_id,
                    "is_consecutive": False,
                    "full_count": None,
                    "new_count": None,
                    "updated_count": None,
                    "deleted_count": None,
                    "parser_version": HISTORICAL_PARSER_VERSION,
                    "source_schema_hash": None,
                    "reconciliation_status": "not_available",
                    "coverage_status": "missing",
                    "status": "missing",
                    "source_url": None,
                    "source_entry_path": None,
                    "source_entry_sha256": None,
                    "archive_sha256": None,
                    "error_detail": "expected TARGET operating-day snapshot absent",
                    "staged_at": _utc_now(),
                }
            ],
        )

    def quarantine(self, run_id: str, snapshot: HistoricalSnapshot, error: str) -> None:
        release_id = f"archive:{snapshot.snapshot_date.isoformat()}"
        local, utc = _publication_times(snapshot.snapshot_date)
        self.delete_nonaccepted_release(run_id, release_id)
        self.warehouse._insert(
            "silver_ecb_eligible_asset_release_stage",
            [
                {
                    "run_id": run_id,
                    "release_id": release_id,
                    "snapshot_date": snapshot.snapshot_date.isoformat(),
                    "publication_at_local": local,
                    "publication_at_utc": utc,
                    "timestamp_precision": "date",
                    "prior_release_id": None,
                    "is_consecutive": False,
                    "full_count": None,
                    "new_count": None,
                    "updated_count": None,
                    "deleted_count": None,
                    "parser_version": HISTORICAL_PARSER_VERSION,
                    "source_schema_hash": None,
                    "reconciliation_status": "failed",
                    "coverage_status": "quarantined",
                    "status": "quarantined",
                    "source_url": snapshot.source_url,
                    "source_entry_path": snapshot.entry_path,
                    "source_entry_sha256": snapshot.entry_sha256,
                    "archive_sha256": snapshot.archive_sha256,
                    "error_detail": error,
                    "staged_at": _utc_now(),
                }
            ],
        )
        self.fail(run_id, error)

    def publish(
        self,
        *,
        run_id: str,
        snapshot: HistoricalSnapshot,
        current: Mapping[str, Mapping[str, Any]],
        prior: Mapping[str, Mapping[str, Any]],
        changes: Mapping[str, str],
        prior_release: Mapping[str, Any] | None,
        has_gap: bool,
        schema_hash: str,
    ) -> None:
        release_id = f"archive:{snapshot.snapshot_date.isoformat()}"
        local, utc = _publication_times(snapshot.snapshot_date)
        now = _utc_now()
        origin = "historical_full_diff_gap" if has_gap else "historical_full_diff"
        reconciliation = "derived_full_diff_gap" if has_gap else "derived_full_diff"
        coverage = (
            "starts_here"
            if prior_release is None
            else ("gapped" if has_gap else "complete")
        )
        counts = {
            code: sum(value == code for value in changes.values())
            for code in ("N", "U", "D")
        }
        version_rows: list[dict[str, Any]] = []
        change_rows: list[dict[str, Any]] = []
        for asset_key, change_type in sorted(changes.items()):
            source = dict(current.get(asset_key) or prior.get(asset_key) or {})
            eligible = change_type != "D"
            row_hash = (
                source.get("row_hash")
                if eligible
                else _canonical_hash(source, is_eligible=False)
            )
            version_rows.append(
                {
                    "run_id": run_id,
                    "asset_key": asset_key,
                    **{column: source.get(column) for column in _VERSION_COLUMNS},
                    "is_eligible": eligible,
                    "source_fields_json": source.get("source_fields_json"),
                    "source_schema_version": source.get("source_schema_version"),
                    "row_hash": row_hash,
                    "change_type": change_type,
                    "change_origin": origin,
                    "valid_from_release_id": release_id,
                    "valid_from_utc": utc,
                    "valid_to_release_id": None,
                    "valid_to_utc": None,
                    "staged_at": now,
                }
            )
            change_rows.append(
                {
                    "run_id": run_id,
                    "release_id": release_id,
                    "asset_key": asset_key,
                    "computed_change_type": change_type,
                    "prior_row_hash": prior.get(asset_key, {}).get("row_hash"),
                    "new_row_hash": current.get(asset_key, {}).get("row_hash"),
                    "reconciliation_status": reconciliation,
                    "change_origin": origin,
                    "publication_at_utc": utc,
                    "staged_at": now,
                }
            )

        prior_defer = bool(getattr(self.warehouse, "_defer_commits", False))
        self.warehouse._defer_commits = True
        try:
            closing = [(release_id, utc, run_id, asset_key) for asset_key in changes]
            self.warehouse.conn.executemany(
                "UPDATE silver_ecb_eligible_asset_version_stage "
                "SET valid_to_release_id = ?, valid_to_utc = ? "
                "WHERE run_id = ? AND asset_key = ? AND valid_to_utc IS NULL",
                closing,
            )
            self.warehouse._insert(
                "silver_ecb_eligible_asset_version_stage", version_rows
            )
            self.warehouse._insert(
                "silver_ecb_eligible_asset_change_stage", change_rows
            )
            self.warehouse._insert(
                "silver_ecb_eligible_asset_release_stage",
                [
                    {
                        "run_id": run_id,
                        "release_id": release_id,
                        "snapshot_date": snapshot.snapshot_date.isoformat(),
                        "publication_at_local": local,
                        "publication_at_utc": utc,
                        "timestamp_precision": "date",
                        "prior_release_id": (
                            prior_release.get("release_id") if prior_release else None
                        ),
                        "is_consecutive": prior_release is not None and not has_gap,
                        "full_count": len(current),
                        "new_count": counts["N"],
                        "updated_count": counts["U"],
                        "deleted_count": counts["D"],
                        "parser_version": HISTORICAL_PARSER_VERSION,
                        "source_schema_hash": schema_hash,
                        "reconciliation_status": reconciliation,
                        "coverage_status": coverage,
                        "status": "accepted",
                        "source_url": snapshot.source_url,
                        "source_entry_path": snapshot.entry_path,
                        "source_entry_sha256": snapshot.entry_sha256,
                        "archive_sha256": snapshot.archive_sha256,
                        "error_detail": None,
                        "staged_at": now,
                    }
                ],
            )
            self.warehouse.conn.execute(
                "UPDATE silver_ecb_eligible_asset_backfill_run "
                "SET status = 'running', snapshot_count = snapshot_count + 1, "
                "updated_at = ?, error_detail = NULL WHERE run_id = ?",
                (now, run_id),
            )
            self.warehouse.conn.commit()
        except Exception:
            self.warehouse.conn.rollback()
            raise
        finally:
            self.warehouse._defer_commits = prior_defer

    def fail(self, run_id: str, error: str) -> None:
        self.warehouse.conn.execute(
            "UPDATE silver_ecb_eligible_asset_backfill_run "
            "SET status = 'failed', updated_at = ?, error_detail = ? "
            "WHERE run_id = ?",
            (_utc_now(), error, run_id),
        )
        self.warehouse.conn.commit()

    def validate_and_complete(
        self,
        run_id: str,
        expected_current: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        duplicate_open = self._rows(
            "SELECT asset_key, COUNT(*) AS n "
            "FROM silver_ecb_eligible_asset_version_stage "
            "WHERE run_id = ? AND valid_to_utc IS NULL GROUP BY asset_key "
            "HAVING COUNT(*) > 1 LIMIT 1",
            (run_id,),
        )
        invalid_interval = self._rows(
            "SELECT asset_key FROM silver_ecb_eligible_asset_version_stage "
            "WHERE run_id = ? AND valid_to_utc IS NOT NULL "
            "AND valid_to_utc <= valid_from_utc LIMIT 1",
            (run_id,),
        )
        actual_current = self.current_assets(run_id)
        expected_hashes = {
            key: row.get("row_hash") for key, row in expected_current.items()
        }
        actual_hashes = {
            key: row.get("row_hash") for key, row in actual_current.items()
        }
        if duplicate_open or invalid_interval or actual_hashes != expected_hashes:
            detail = (
                f"stage invariant failed: duplicate_open={bool(duplicate_open)} "
                f"invalid_interval={bool(invalid_interval)} "
                f"missing_current={len(set(expected_hashes) - set(actual_hashes))} "
                f"extra_current={len(set(actual_hashes) - set(expected_hashes))} "
                f"hash_mismatch={sum(expected_hashes.get(k) != actual_hashes.get(k) for k in set(expected_hashes) & set(actual_hashes))}"
            )
            self.fail(run_id, detail)
            raise ECBArchiveBackfillError(detail)

        counts = self._rows(
            "SELECT "
            "SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) AS accepted_count, "
            "SUM(CASE WHEN status = 'missing' THEN 1 ELSE 0 END) AS missing_count, "
            "SUM(CASE WHEN status = 'quarantined' THEN 1 ELSE 0 END) AS quarantined_count "
            "FROM silver_ecb_eligible_asset_release_stage WHERE run_id = ?",
            (run_id,),
        )[0]
        version_count = int(
            self._rows(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_version_stage "
                "WHERE run_id = ?",
                (run_id,),
            )[0]["n"]
        )
        change_count = int(
            self._rows(
                "SELECT COUNT(*) AS n FROM silver_ecb_eligible_asset_change_stage "
                "WHERE run_id = ?",
                (run_id,),
            )[0]["n"]
        )
        now = _utc_now()
        self.warehouse.conn.execute(
            "UPDATE silver_ecb_eligible_asset_backfill_run SET "
            "status = 'staged', snapshot_count = ?, missing_count = ?, "
            "quarantined_count = ?, version_count = ?, change_count = ?, "
            "final_asset_count = ?, updated_at = ?, completed_at = ?, "
            "error_detail = NULL WHERE run_id = ?",
            (
                int(counts.get("accepted_count") or 0),
                int(counts.get("missing_count") or 0),
                int(counts.get("quarantined_count") or 0),
                version_count,
                change_count,
                len(actual_current),
                now,
                now,
                run_id,
            ),
        )
        self.warehouse.conn.commit()
        return self.status(run_id)

    def status(self, run_id: str) -> dict[str, Any]:
        rows = self._rows(
            "SELECT * FROM silver_ecb_eligible_asset_backfill_run WHERE run_id = ?",
            (run_id,),
        )
        if not rows:
            raise ECBArchiveBackfillError(f"unknown backfill run {run_id!r}")
        return rows[0]


class ECBHistoricalStagingRebuilder:
    """Chronologically diff registered snapshots into compact staging history."""

    def __init__(
        self,
        warehouse: Any,
        *,
        registry: HistoricalSchemaRegistry | None = None,
        registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    ):
        self.registry_path = Path(registry_path)
        self.registry = registry or load_schema_registry(self.registry_path)
        self.repository = ECBHistoricalStageRepository(warehouse)

    @staticmethod
    def _missing_between(previous: date, current: date) -> list[date]:
        return [
            value
            for value in _date_range(previous, current)
            if previous < value < current and is_target_operating_day(value)
        ]

    def rebuild(
        self,
        snapshots: Iterable[HistoricalSnapshot],
        *,
        run_id: str,
        from_date: date,
        to_date: date,
        restart: bool = False,
    ) -> dict[str, Any]:
        if to_date < from_date:
            raise ECBArchiveBackfillError("to_date must be >= from_date")
        if restart:
            self.repository.restart(run_id)
        run = self.repository.begin(
            run_id=run_id,
            from_date=from_date,
            to_date=to_date,
            registry_sha256=_registry_hash(self.registry_path),
        )
        if run["status"] == "staged":
            return run

        processed = self.repository.releases(run_id)
        prior_release = self.repository.latest_accepted(run_id)
        prior = self.repository.current_assets(run_id)
        latest_accepted_date = (
            date.fromisoformat(prior_release["snapshot_date"])
            if prior_release
            else None
        )
        last_source_date: date | None = None
        saw_snapshot = bool(prior_release)

        try:
            for snapshot in snapshots:
                if snapshot.snapshot_date < from_date:
                    continue
                if snapshot.snapshot_date > to_date:
                    break
                if (
                    last_source_date is not None
                    and snapshot.snapshot_date <= last_source_date
                ):
                    raise ECBArchiveBackfillError(
                        "archive snapshots are not strictly chronological: "
                        f"{snapshot.snapshot_date.isoformat()}"
                    )
                last_source_date = snapshot.snapshot_date
                existing = processed.get(snapshot.snapshot_date.isoformat())
                if existing and existing["status"] == "accepted":
                    if existing["source_entry_sha256"] != snapshot.entry_sha256:
                        raise ECBArchiveBackfillError(
                            f"accepted source bytes changed for {snapshot.snapshot_date}; "
                            "restart is required"
                        )
                    continue
                if (
                    existing
                    and latest_accepted_date is not None
                    and (snapshot.snapshot_date < latest_accepted_date)
                ):
                    raise ECBArchiveBackfillError(
                        f"snapshot {snapshot.snapshot_date} fills an earlier staged gap; "
                        "restart is required to rebuild intervals chronologically"
                    )
                if existing:
                    self.repository.delete_nonaccepted_release(
                        run_id, str(existing["release_id"])
                    )

                missing = (
                    self._missing_between(latest_accepted_date, snapshot.snapshot_date)
                    if latest_accepted_date is not None
                    else []
                )
                for missing_date in missing:
                    if missing_date.isoformat() not in processed:
                        self.repository.stage_missing(
                            run_id,
                            missing_date,
                            prior_release_id=(
                                str(prior_release["release_id"])
                                if prior_release
                                else None
                            ),
                        )
                        processed[missing_date.isoformat()] = {
                            "status": "missing",
                            "release_id": f"archive-missing:{missing_date.isoformat()}",
                        }

                try:
                    parsed = parse_historical_assets_csv(
                        snapshot.payload,
                        snapshot_date=snapshot.snapshot_date,
                        registry=self.registry,
                    )
                except Exception as exc:
                    detail = f"{type(exc).__name__}: {exc}"
                    self.repository.quarantine(run_id, snapshot, detail)
                    processed[snapshot.snapshot_date.isoformat()] = {
                        "status": "quarantined",
                        "release_id": f"archive:{snapshot.snapshot_date.isoformat()}",
                        "source_entry_sha256": snapshot.entry_sha256,
                    }
                    # A later historical full snapshot is authoritative, so a
                    # malformed snapshot after the initial accepted state can
                    # be represented as an explicit coverage gap and safely
                    # bridged by the next valid full diff.  Never guess which
                    # duplicate/conflicting row to keep.  The first snapshot
                    # is still fatal because there is no prior state to carry.
                    if prior_release is None:
                        raise ECBArchiveBackfillError(detail) from exc
                    log.warning(
                        "Quarantined ECB snapshot %s without advancing state: %s",
                        snapshot.snapshot_date,
                        detail,
                    )
                    continue
                changes = diff_assets(prior, parsed.rows)
                self.repository.publish(
                    run_id=run_id,
                    snapshot=snapshot,
                    current=parsed.rows,
                    prior=prior,
                    changes=changes,
                    prior_release=prior_release,
                    has_gap=bool(missing),
                    schema_hash=parsed.schema_hash,
                )
                prior = parsed.rows
                prior_release = {
                    "release_id": f"archive:{snapshot.snapshot_date.isoformat()}",
                    "snapshot_date": snapshot.snapshot_date.isoformat(),
                }
                latest_accepted_date = snapshot.snapshot_date
                processed[snapshot.snapshot_date.isoformat()] = {
                    "status": "accepted",
                    "release_id": prior_release["release_id"],
                    "source_entry_sha256": snapshot.entry_sha256,
                }
                saw_snapshot = True
                log.info(
                    "Staged ECB snapshot %s: %s assets, %s changes%s",
                    snapshot.snapshot_date,
                    f"{len(parsed.rows):,}",
                    f"{len(changes):,}",
                    " across a gap" if missing else "",
                )
        except Exception as exc:
            if not isinstance(exc, ECBArchiveBackfillError) or "quarantined" not in str(
                exc
            ):
                self.repository.fail(run_id, str(exc))
            raise
        if not saw_snapshot:
            detail = "no ECB archive snapshots found in requested range"
            self.repository.fail(run_id, detail)
            raise ECBArchiveBackfillError(detail)
        if latest_accepted_date is not None:
            for missing_date in _date_range(latest_accepted_date, to_date):
                if (
                    missing_date > latest_accepted_date
                    and is_target_operating_day(missing_date)
                    and missing_date.isoformat() not in processed
                ):
                    self.repository.stage_missing(
                        run_id,
                        missing_date,
                        prior_release_id=(
                            str(prior_release["release_id"]) if prior_release else None
                        ),
                    )
                    processed[missing_date.isoformat()] = {
                        "status": "missing",
                        "release_id": f"archive-missing:{missing_date.isoformat()}",
                    }
        return self.repository.validate_and_complete(run_id, prior)
