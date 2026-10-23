"""Read-only inventory of the ECB eligible-assets annual archive ZIPs."""

from __future__ import annotations

import csv
import codecs
import gzip
import hashlib
import json
import logging
import re
import tarfile
import tempfile
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from itertools import chain
from pathlib import Path, PurePosixPath
from typing import IO, Any, Callable, Iterable, Iterator, Protocol
from urllib.parse import urlparse

import requests

from fred_pipeline.ecb_calendar import is_target_operating_day

log = logging.getLogger("fred_pipeline.ecb_archive_inventory")

ARCHIVE_URL_TEMPLATE = (
    "https://www.ecb.europa.eu/paym/coll/assets/shared/data/archive/{year}.zip"
)
DOWNLOAD_PAGE_URL = "https://www.ecb.europa.eu/mopo/coll/assets/html/list-MID.en.html"
DAILY_URL_TEMPLATE = (
    "https://www.ecb.europa.eu/paym/coll/assets/shared/data/EMA/"
    "{year}/{month}/ea_csv_{stamp}.csv.gz"
)
INVENTORY_VERSION = "spec011-archive-inventory-v1"
MIN_ARCHIVE_YEAR = 2010
DEFAULT_MAX_ARCHIVE_BYTES = 1_073_741_824  # 1 GiB compressed
DEFAULT_MAX_ZIP_UNCOMPRESSED_BYTES = 21_474_836_480  # 20 GiB
_SNAPSHOT_RE = re.compile(
    r"^ea_(?P<family>all|csv)_(?P<stamp>\d{6})"
    r"(?P<suffix>\.tgz|\.tar\.gz|\.csv\.gz|\.txt\.gz)$",
    re.IGNORECASE,
)


class ECBArchiveInventoryError(RuntimeError):
    """An annual archive could not be downloaded or safely inventoried."""


class _ReadableBytes(Protocol):
    def read(self, size: int = -1) -> bytes: ...


@dataclass(frozen=True)
class DownloadedArchive:
    path: Path
    byte_count: int
    sha256: str
    content_type: str
    last_modified: str


def _safe_member_name(name: str) -> bool:
    path = PurePosixPath(name)
    return not path.is_absolute() and ".." not in path.parts


def _snapshot_date(stamp: str, expected_year: int) -> date:
    try:
        value = date(2000 + int(stamp[:2]), int(stamp[2:4]), int(stamp[4:6]))
    except ValueError as exc:
        raise ECBArchiveInventoryError(f"invalid snapshot date {stamp!r}") from exc
    if value.year != expected_year:
        raise ECBArchiveInventoryError(
            f"snapshot date {value.isoformat()} does not match archive {expected_year}"
        )
    return value


def _detect_encoding(head: bytes) -> str:
    if head.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    if b"\x00" in head[:200]:
        return "utf-16le"
    return "utf-8-sig"


def _decoded_lines(
    prefix: bytes, stream: _ReadableBytes, encoding: str
) -> Iterator[str]:
    decoder = codecs.getincrementaldecoder(encoding)()
    pending = ""
    chunks = iter(lambda: stream.read(1024 * 1024), b"")
    for chunk in chain((prefix,), chunks):
        decoded = pending + decoder.decode(chunk)
        lines = decoded.splitlines(keepends=True)
        if lines and not lines[-1].endswith(("\n", "\r")):
            pending = lines.pop()
        else:
            pending = ""
        yield from lines
    tail = pending + decoder.decode(b"", final=True)
    if tail:
        yield tail


def _inspect_delimited_stream(
    stream: _ReadableBytes, *, deep: bool
) -> tuple[str, str, tuple[str, ...], int | None]:
    head = stream.read(4096)
    encoding = _detect_encoding(head)
    lines = _decoded_lines(head, stream, encoding)
    first = next(lines, "")
    if not first:
        raise ECBArchiveInventoryError("snapshot payload is empty")
    delimiter = max(("\t", ";", ","), key=first.count)
    columns = tuple(
        value.strip().lstrip("\ufeff")
        for value in next(csv.reader([first], delimiter=delimiter))
    )
    if not columns or not any(columns):
        raise ECBArchiveInventoryError("snapshot payload has no header columns")
    row_count: int | None = None
    if deep:
        row_count = sum(1 for row in csv.reader(lines, delimiter=delimiter) if any(row))
    return encoding, delimiter, columns, row_count


def _inspect_tgz(
    stream: IO[bytes], *, deep: bool
) -> tuple[str, int, str, str, tuple[str, ...], int | None]:
    with tarfile.open(fileobj=stream, mode="r|gz") as archive:
        for member in archive:
            if not _safe_member_name(member.name):
                raise ECBArchiveInventoryError(
                    f"unsafe path in nested archive: {member.name!r}"
                )
            if not member.isfile() or not member.name.lower().endswith(
                (".txt", ".csv")
            ):
                continue
            payload = archive.extractfile(member)
            if payload is None:
                continue
            encoding, delimiter, columns, row_count = _inspect_delimited_stream(
                payload, deep=deep
            )
            return (
                member.name,
                member.size,
                encoding,
                delimiter,
                columns,
                row_count,
            )
    raise ECBArchiveInventoryError("nested tgz has no .txt or .csv snapshot")


def _inspect_snapshot(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    *,
    year: int,
    deep: bool,
) -> dict[str, Any]:
    match = _SNAPSHOT_RE.match(PurePosixPath(info.filename).name)
    if match is None:
        raise ECBArchiveInventoryError(
            f"unsupported snapshot filename {info.filename!r}"
        )
    snapshot_date = _snapshot_date(match.group("stamp"), year)
    suffix = match.group("suffix").lower()
    result: dict[str, Any] = {
        "snapshot_date": snapshot_date.isoformat(),
        "entry_path": info.filename,
        "entry_format": "tgz" if suffix in {".tgz", ".tar.gz"} else "csv.gz",
        "zip_crc32": f"{info.CRC:08x}",
        "compressed_bytes": info.compress_size,
        "entry_bytes": info.file_size,
        "payload_name": None,
        "payload_bytes": None,
        "encoding": None,
        "delimiter": None,
        "columns": [],
        "schema_hash": None,
        "row_count": None,
        "error": None,
    }
    try:
        with archive.open(info, "r") as member:
            if result["entry_format"] == "tgz":
                (
                    payload_name,
                    payload_bytes,
                    encoding,
                    delimiter,
                    columns,
                    row_count,
                ) = _inspect_tgz(member, deep=deep)
            else:
                with gzip.GzipFile(fileobj=member, mode="rb") as payload:
                    encoding, delimiter, columns, row_count = _inspect_delimited_stream(
                        payload, deep=deep
                    )
                payload_name = PurePosixPath(info.filename).name.removesuffix(".gz")
                payload_bytes = None
        result.update(
            {
                "payload_name": payload_name,
                "payload_bytes": payload_bytes,
                "encoding": encoding,
                "delimiter": {"\t": "tab", ";": "semicolon", ",": "comma"}[delimiter],
                "columns": list(columns),
                "schema_hash": hashlib.sha256(
                    json.dumps(
                        columns,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
                "row_count": row_count,
            }
        )
    except Exception as exc:  # preserve per-snapshot evidence and continue
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _build_year_report(
    *,
    year: int,
    source_url: str,
    delivery_mode: str,
    snapshots: list[dict[str, Any]],
    include_snapshots: bool,
    source_bytes: int,
    source_sha256: str | None,
    content_type: str,
    last_modified: str,
    member_count: int,
    unsupported_members: list[str] | None = None,
    missing_expected_dates: list[str] | None = None,
) -> dict[str, Any]:
    snapshots.sort(key=lambda row: (row["snapshot_date"], row["entry_path"]))
    dates = [date.fromisoformat(row["snapshot_date"]) for row in snapshots]
    unique_dates = set(dates)
    duplicate_dates = sorted(
        value.isoformat() for value in unique_dates if dates.count(value) > 1
    )
    if missing_expected_dates is None:
        missing_expected_dates = []
        if dates:
            missing_expected_dates = [
                value.isoformat()
                for value in _date_range(min(dates), max(dates))
                if is_target_operating_day(value) and value not in unique_dates
            ]

    schema_groups: dict[str, dict[str, Any]] = {}
    for row in snapshots:
        schema_hash = row.get("schema_hash")
        if not schema_hash:
            continue
        group = schema_groups.setdefault(
            schema_hash,
            {
                "schema_hash": schema_hash,
                "columns": row["columns"],
                "encodings": set(),
                "formats": set(),
                "snapshot_count": 0,
                "first_snapshot_date": row["snapshot_date"],
                "last_snapshot_date": row["snapshot_date"],
            },
        )
        group["encodings"].add(row["encoding"])
        group["formats"].add(row["entry_format"])
        group["snapshot_count"] += 1
        group["last_snapshot_date"] = row["snapshot_date"]
    schemas = []
    for group in schema_groups.values():
        group["encodings"] = sorted(group["encodings"])
        group["formats"] = sorted(group["formats"])
        schemas.append(group)
    schemas.sort(key=lambda row: (row["first_snapshot_date"], row["schema_hash"]))

    errors = [row for row in snapshots if row.get("error")]
    report: dict[str, Any] = {
        "year": year,
        "archive_url": source_url,
        "delivery_mode": delivery_mode,
        "status": "partial" if errors else "ok",
        "archive_sha256": source_sha256,
        "archive_bytes": source_bytes,
        "content_type": content_type,
        "last_modified": last_modified,
        "zip_member_count": member_count if delivery_mode == "annual_zip" else None,
        "snapshot_count": len(snapshots),
        "first_snapshot_date": min(dates).isoformat() if dates else None,
        "last_snapshot_date": max(dates).isoformat() if dates else None,
        "missing_expected_dates": missing_expected_dates,
        "duplicate_snapshot_dates": duplicate_dates,
        "unsupported_members": unsupported_members or [],
        "schema_count": len(schemas),
        "schemas": schemas,
        "snapshot_error_count": len(errors),
    }
    if include_snapshots:
        report["snapshots"] = snapshots
    return report


def inspect_archive(
    path: str | Path,
    *,
    year: int,
    archive_url: str,
    archive_sha256: str | None = None,
    archive_bytes: int | None = None,
    content_type: str = "application/zip",
    last_modified: str = "",
    deep: bool = False,
    include_snapshots: bool = True,
    max_uncompressed_bytes: int = DEFAULT_MAX_ZIP_UNCOMPRESSED_BYTES,
) -> dict[str, Any]:
    """Inspect one annual ZIP without extracting files to the filesystem."""
    path = Path(path)
    if archive_sha256 is None or archive_bytes is None:
        digest = hashlib.sha256()
        byte_count = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
                byte_count += len(chunk)
        archive_sha256 = digest.hexdigest()
        archive_bytes = byte_count

    snapshots: list[dict[str, Any]] = []
    unsupported_members: list[str] = []
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        unsafe = [
            info.filename for info in infos if not _safe_member_name(info.filename)
        ]
        if unsafe:
            raise ECBArchiveInventoryError(
                f"archive contains unsafe paths: {unsafe[:10]}"
            )
        total_uncompressed = sum(info.file_size for info in infos)
        if total_uncompressed > max_uncompressed_bytes:
            raise ECBArchiveInventoryError(
                f"archive declares {total_uncompressed} uncompressed bytes, above "
                f"the {max_uncompressed_bytes} safety limit"
            )
        for info in infos:
            if info.is_dir():
                continue
            if _SNAPSHOT_RE.match(PurePosixPath(info.filename).name):
                snapshots.append(_inspect_snapshot(archive, info, year=year, deep=deep))
            else:
                unsupported_members.append(info.filename)

    return _build_year_report(
        year=year,
        source_url=archive_url,
        delivery_mode="annual_zip",
        snapshots=snapshots,
        include_snapshots=include_snapshots,
        source_bytes=archive_bytes,
        source_sha256=archive_sha256,
        content_type=content_type,
        last_modified=last_modified,
        member_count=len(snapshots) + len(unsupported_members),
        unsupported_members=unsupported_members,
    )


class ECBArchiveInventoryClient:
    """Stream annual ECB ZIPs to bounded temporary storage and inspect them."""

    def __init__(
        self,
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
        rate_limit_per_minute: int = 60,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if rate_limit_per_minute < 1:
            raise ValueError("rate_limit_per_minute must be >= 1")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_archive_bytes = max_archive_bytes
        self.sleep = sleep
        self._request_interval = 60.0 / rate_limit_per_minute
        self._last_request_at = 0.0

    def _pace(self) -> None:
        wait = self._request_interval - (time.monotonic() - self._last_request_at)
        if wait > 0:
            self.sleep(wait)
        self._last_request_at = time.monotonic()

    def _download(self, url: str, target: Path) -> DownloadedArchive:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            response = None
            try:
                self._pace()
                response = self.session.get(url, stream=True, timeout=self.timeout)
                status = int(getattr(response, "status_code", 200))
                if status != 200:
                    if (
                        status in {408, 429, 500, 502, 503, 504}
                        and attempt < self.max_retries
                    ):
                        self.sleep(min(2**attempt, 30))
                        continue
                    raise ECBArchiveInventoryError(f"HTTP {status} downloading {url}")
                content_length = int(response.headers.get("Content-Length") or 0)
                if content_length > self.max_archive_bytes:
                    raise ECBArchiveInventoryError(
                        f"archive Content-Length {content_length} exceeds "
                        f"the {self.max_archive_bytes} byte safety limit"
                    )
                digest = hashlib.sha256()
                byte_count = 0
                with target.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        byte_count += len(chunk)
                        if byte_count > self.max_archive_bytes:
                            raise ECBArchiveInventoryError(
                                f"archive exceeded the {self.max_archive_bytes} "
                                "byte safety limit while downloading"
                            )
                        digest.update(chunk)
                        output.write(chunk)
                return DownloadedArchive(
                    path=target,
                    byte_count=byte_count,
                    sha256=digest.hexdigest(),
                    content_type=response.headers.get("Content-Type", ""),
                    last_modified=response.headers.get("Last-Modified", ""),
                )
            except ECBArchiveInventoryError:
                raise
            except Exception as exc:  # network error
                last_error = exc
                if attempt < self.max_retries:
                    self.sleep(min(2**attempt, 30))
            finally:
                if response is not None and hasattr(response, "close"):
                    response.close()
        raise ECBArchiveInventoryError(
            f"download failed after {self.max_retries + 1} attempts: {last_error}"
        )

    def _fetch_bytes(
        self, url: str, *, allow_missing: bool = False
    ) -> tuple[bytes, dict[str, str]] | None:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            response = None
            try:
                self._pace()
                response = self.session.get(url, stream=True, timeout=self.timeout)
                status = int(getattr(response, "status_code", 200))
                if status == 404 and allow_missing:
                    return None
                if status != 200:
                    if (
                        status in {408, 429, 500, 502, 503, 504}
                        and attempt < self.max_retries
                    ):
                        self.sleep(min(2**attempt, 30))
                        continue
                    raise ECBArchiveInventoryError(f"HTTP {status} downloading {url}")
                chunks: list[bytes] = []
                byte_count = 0
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    byte_count += len(chunk)
                    if byte_count > self.max_archive_bytes:
                        raise ECBArchiveInventoryError(
                            f"response exceeded the {self.max_archive_bytes} "
                            "byte safety limit"
                        )
                    chunks.append(chunk)
                headers = {
                    str(key): str(value)
                    for key, value in getattr(response, "headers", {}).items()
                }
                return b"".join(chunks), headers
            except ECBArchiveInventoryError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self.sleep(min(2**attempt, 30))
            finally:
                if response is not None and hasattr(response, "close"):
                    response.close()
        raise ECBArchiveInventoryError(
            f"download failed after {self.max_retries + 1} attempts: {last_error}"
        )

    @staticmethod
    def _inspect_daily_payload(
        payload: bytes, *, snapshot_date: date, url: str, deep: bool
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "snapshot_date": snapshot_date.isoformat(),
            "entry_path": urlparse(url).path,
            "source_url": url,
            "entry_format": "csv.gz",
            "sha256": hashlib.sha256(payload).hexdigest(),
            "zip_crc32": None,
            "compressed_bytes": len(payload),
            "entry_bytes": len(payload),
            "payload_name": PurePosixPath(urlparse(url).path).name.removesuffix(".gz"),
            "payload_bytes": None,
            "encoding": None,
            "delimiter": None,
            "columns": [],
            "schema_hash": None,
            "row_count": None,
            "error": None,
        }
        try:
            with gzip.GzipFile(fileobj=BytesIO(payload), mode="rb") as stream:
                encoding, delimiter, columns, row_count = _inspect_delimited_stream(
                    stream, deep=deep
                )
            result.update(
                {
                    "encoding": encoding,
                    "delimiter": {
                        "\t": "tab",
                        ";": "semicolon",
                        ",": "comma",
                    }[delimiter],
                    "columns": list(columns),
                    "schema_hash": hashlib.sha256(
                        json.dumps(
                            columns,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ).hexdigest(),
                    "row_count": row_count,
                }
            )
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    def inventory_current_year(
        self,
        year: int,
        *,
        deep: bool = False,
        include_snapshots: bool = True,
    ) -> dict[str, Any]:
        page_result = self._fetch_bytes(DOWNLOAD_PAGE_URL)
        if page_result is None:  # pragma: no cover - allow_missing is false
            raise ECBArchiveInventoryError("ECB download page was not returned")
        page, page_headers = page_result
        stamps = [
            value.decode("ascii")
            for value in re.findall(rb"ea_csv_(\d{6})\.csv(?:\.gz)?", page)
            if value.startswith(f"{year % 100:02d}".encode("ascii"))
        ]
        if not stamps:
            raise ECBArchiveInventoryError(
                f"download page exposes no current snapshot for {year}"
            )
        latest = _snapshot_date(max(stamps), year)
        snapshots: list[dict[str, Any]] = []
        missing: list[str] = []
        total_bytes = 0
        last_modified = ""
        attempted = 0
        for candidate in _date_range(date(year, 1, 1), latest):
            if not is_target_operating_day(candidate):
                continue
            attempted += 1
            stamp = candidate.strftime("%y%m%d")
            url = DAILY_URL_TEMPLATE.format(
                year=year,
                month=candidate.strftime("%m"),
                stamp=stamp,
            )
            fetched = self._fetch_bytes(url, allow_missing=True)
            if fetched is None:
                missing.append(candidate.isoformat())
                if attempted % 25 == 0:
                    log.info(
                        "ECB current-year inventory: %d dates checked, "
                        "%d found, %d missing",
                        attempted,
                        len(snapshots),
                        len(missing),
                    )
                continue
            payload, headers = fetched
            total_bytes += len(payload)
            last_modified = headers.get("Last-Modified", last_modified)
            snapshots.append(
                self._inspect_daily_payload(
                    payload,
                    snapshot_date=candidate,
                    url=url,
                    deep=deep,
                )
            )
            if attempted % 25 == 0:
                log.info(
                    "ECB current-year inventory: %d dates checked, %d found, %d missing",
                    attempted,
                    len(snapshots),
                    len(missing),
                )
        return _build_year_report(
            year=year,
            source_url=DOWNLOAD_PAGE_URL,
            delivery_mode="daily_files",
            snapshots=snapshots,
            include_snapshots=include_snapshots,
            source_bytes=total_bytes,
            source_sha256=None,
            content_type=page_headers.get("Content-Type", "text/html"),
            last_modified=last_modified,
            member_count=len(snapshots),
            missing_expected_dates=missing,
        )

    def inventory_year(
        self,
        year: int,
        *,
        deep: bool = False,
        include_snapshots: bool = True,
    ) -> dict[str, Any]:
        if year == datetime.now(timezone.utc).year:
            return self.inventory_current_year(
                year,
                deep=deep,
                include_snapshots=include_snapshots,
            )
        url = ARCHIVE_URL_TEMPLATE.format(year=year)
        with tempfile.TemporaryDirectory(prefix=f"ecb-archive-{year}-") as tmp:
            downloaded = self._download(url, Path(tmp) / f"{year}.zip")
            return inspect_archive(
                downloaded.path,
                year=year,
                archive_url=url,
                archive_sha256=downloaded.sha256,
                archive_bytes=downloaded.byte_count,
                content_type=downloaded.content_type,
                last_modified=downloaded.last_modified,
                deep=deep,
                include_snapshots=include_snapshots,
            )

    def inventory(
        self,
        *,
        from_year: int = MIN_ARCHIVE_YEAR,
        to_year: int | None = None,
        deep: bool = False,
        include_snapshots: bool = True,
    ) -> dict[str, Any]:
        to_year = to_year or datetime.now(timezone.utc).year
        if from_year < MIN_ARCHIVE_YEAR:
            raise ECBArchiveInventoryError(f"from_year must be >= {MIN_ARCHIVE_YEAR}")
        if to_year < from_year:
            raise ECBArchiveInventoryError("to_year must be >= from_year")
        current_year = datetime.now(timezone.utc).year
        if to_year > current_year:
            raise ECBArchiveInventoryError(
                f"to_year must be <= current year {current_year}"
            )

        years: list[dict[str, Any]] = []
        for year in range(from_year, to_year + 1):
            log.info("Inventorying ECB eligible-assets archive for %d", year)
            try:
                annual = self.inventory_year(
                    year,
                    deep=deep,
                    include_snapshots=include_snapshots,
                )
                years.append(annual)
                log.info(
                    "ECB archive %d: %s, %d snapshots, %d schemas",
                    year,
                    annual["status"],
                    annual["snapshot_count"],
                    annual["schema_count"],
                )
            except Exception as exc:
                log.error("ECB archive %d inventory failed: %s", year, exc)
                years.append(
                    {
                        "year": year,
                        "archive_url": ARCHIVE_URL_TEMPLATE.format(year=year),
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        schema_hashes = {
            schema["schema_hash"]
            for annual in years
            for schema in annual.get("schemas", [])
        }
        return {
            "inventory_version": INVENTORY_VERSION,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "ECB eligible marketable assets annual archives",
            "requested": {
                "from_year": from_year,
                "to_year": to_year,
                "deep": deep,
                "include_snapshots": include_snapshots,
            },
            "summary": {
                "year_count": len(years),
                "ok_year_count": sum(row.get("status") == "ok" for row in years),
                "partial_year_count": sum(
                    row.get("status") == "partial" for row in years
                ),
                "failed_year_count": sum(
                    row.get("status") == "failed" for row in years
                ),
                "snapshot_count": sum(
                    int(row.get("snapshot_count") or 0) for row in years
                ),
                "schema_count": len(schema_hashes),
                "missing_expected_date_count": sum(
                    len(row.get("missing_expected_dates") or []) for row in years
                ),
            },
            "years": years,
        }
