from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
import zipfile

import pytest

from fred_pipeline import cli
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.ecb_archive_inventory import (
    DAILY_URL_TEMPLATE,
    DOWNLOAD_PAGE_URL,
    ECBArchiveInventoryClient,
    ECBArchiveInventoryError,
    inspect_archive,
)


def _utf16_table(columns, rows):
    lines = ["\t".join(columns), *("\t".join(row) for row in rows)]
    return ("\n".join(lines) + "\n").encode("utf-16")


def _tgz(name, payload):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        info = tarfile.TarInfo(name)
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
        metadata = b"Format: ECB eligible assets\n"
        info = tarfile.TarInfo(name.removesuffix(".txt") + ".lsm")
        info.size = len(metadata)
        archive.addfile(info, io.BytesIO(metadata))
    return output.getvalue()


def _zip(path, entries):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)


def test_inventory_reads_legacy_tgz_schema_and_target_date_gaps(tmp_path):
    columns = ["ISIN_CODE", "OTHER_REG_NUMBER", "LIQUIDITY_CLASS"]
    rows = [["DE0001", "", "L1A"], ["DE0002", "", "L1C"]]
    archive_path = tmp_path / "2010.zip"
    _zip(
        archive_path,
        {
            "2010/ea_all_100408.tgz": _tgz(
                "ea_all_100408.txt", _utf16_table(columns, rows)
            ),
            "2010/ea_all_100409.tgz": _tgz(
                "ea_all_100409.txt", _utf16_table(columns, rows)
            ),
            "2010/ea_all_100413.tgz": _tgz(
                "ea_all_100413.txt", _utf16_table(columns, rows)
            ),
        },
    )

    report = inspect_archive(
        archive_path,
        year=2010,
        archive_url="https://example/2010.zip",
        deep=True,
    )

    assert report["status"] == "ok", report
    assert report["snapshot_count"] == 3
    assert report["schema_count"] == 1
    assert report["schemas"][0]["columns"] == columns
    assert report["schemas"][0]["snapshot_count"] == 3
    assert report["snapshots"][0]["entry_format"] == "tgz"
    assert report["snapshots"][0]["encoding"] == "utf-16"
    assert report["snapshots"][0]["row_count"] == 2
    assert report["row_count_snapshot_count"] == 3
    assert report["row_count_total"] == 6
    assert report["schemas"][0]["row_count_min"] == 2
    assert report["schemas"][0]["row_count_max"] == 2
    assert report["missing_expected_dates"] == ["2010-04-12"]


def test_inventory_reads_modern_csv_gzip_and_groups_schema_versions(tmp_path):
    original = ["ISIN_CODE", "HAIRCUT_CATEGORY"]
    changed = [*original, "CLIMATE_FACTOR"]
    archive_path = tmp_path / "2025.zip"
    _zip(
        archive_path,
        {
            "2025/ea_csv_250102.csv.gz": gzip.compress(
                _utf16_table(original, [["DE0001", "L1A"]])
            ),
            "2025/ea_csv_250103.csv.gz": gzip.compress(
                _utf16_table(changed, [["DE0001", "L1A", "N"]])
            ),
            "2025/readme.txt": b"source notes",
        },
    )

    report = inspect_archive(
        archive_path,
        year=2025,
        archive_url="https://example/2025.zip",
        deep=False,
        include_snapshots=False,
    )

    assert (
        report["archive_sha256"]
        == hashlib.sha256(archive_path.read_bytes()).hexdigest()
    )
    assert report["snapshot_count"] == 2
    assert report["schema_count"] == 2
    assert report["unsupported_members"] == ["2025/readme.txt"]
    assert "snapshots" not in report
    assert report["row_count_total"] is None
    assert all(schema["encodings"] == ["utf-16"] for schema in report["schemas"])


def test_inventory_rejects_zip_path_traversal(tmp_path):
    archive_path = tmp_path / "unsafe.zip"
    _zip(
        archive_path,
        {
            "../ea_csv_250102.csv.gz": gzip.compress(
                _utf16_table(["ISIN_CODE"], [["DE0001"]])
            )
        },
    )
    with pytest.raises(ECBArchiveInventoryError, match="unsafe paths"):
        inspect_archive(
            archive_path,
            year=2025,
            archive_url="https://example/2025.zip",
        )


class _Response:
    def __init__(self, payload, *, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "application/zip"}
        self.closed = False

    def iter_content(self, chunk_size):
        del chunk_size
        yield self.payload

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, payload):
        self.payload = payload

    def get(self, url, stream, timeout):
        assert url.endswith("/2010.zip")
        assert stream is True
        assert timeout == 30
        return _Response(self.payload)


def test_inventory_client_enforces_download_size_limit(tmp_path):
    client = ECBArchiveInventoryClient(
        session=_Session(b"too large"),
        max_archive_bytes=3,
        max_retries=0,
    )
    with pytest.raises(ECBArchiveInventoryError, match="safety limit"):
        client._download("https://example/2010.zip", tmp_path / "2010.zip")


def test_current_year_uses_daily_files_and_records_missing_dates():
    daily_payload = gzip.compress(
        _utf16_table(["ISIN_CODE", "HAIRCUT_CATEGORY"], [["DE0001", "L1A"]])
    )
    jan_2 = DAILY_URL_TEMPLATE.format(year=2026, month="01", stamp="260102")
    jan_5 = DAILY_URL_TEMPLATE.format(year=2026, month="01", stamp="260105")

    class _RoutingSession:
        def get(self, url, stream, timeout):
            assert stream is True and timeout == 30
            if url == DOWNLOAD_PAGE_URL:
                return _Response(b'<a href="ea_csv_260105.csv.gz">latest</a>')
            if url == jan_2:
                return _Response(
                    daily_payload, headers={"Content-Type": "application/x-gzip"}
                )
            assert url == jan_5
            return _Response(b"", status_code=404)

    report = ECBArchiveInventoryClient(
        session=_RoutingSession(),
        max_retries=0,
        rate_limit_per_minute=1_000_000,
    ).inventory_current_year(2026, include_snapshots=True)

    assert report["delivery_mode"] == "daily_files"
    assert report["status"] == "ok"
    assert report["snapshot_count"] == 1
    assert report["first_snapshot_date"] == "2026-01-02"
    assert report["missing_expected_dates"] == ["2026-01-05"]
    assert report["schemas"][0]["columns"] == [
        "ISIN_CODE",
        "HAIRCUT_CATEGORY",
    ]


def test_inventory_cli_does_not_initialize_a_warehouse(monkeypatch, tmp_path, capsys):
    config = PipelineConfig(environment=Environment.DEV, fred_api_key="")
    monkeypatch.setattr(
        cli.PipelineConfig, "resolve", staticmethod(lambda **_kwargs: config)
    )

    report = {
        "inventory_version": "test",
        "summary": {
            "failed_year_count": 0,
            "partial_year_count": 0,
            "snapshot_count": 190,
        },
        "years": [],
    }

    class _InventoryClient:
        def __init__(self, **kwargs):
            assert kwargs["max_archive_bytes"] == 64 * 1024 * 1024
            assert kwargs["rate_limit_per_minute"] == 60

        def inventory(self, **kwargs):
            assert kwargs == {
                "from_year": 2010,
                "to_year": 2010,
                "deep": False,
                "include_snapshots": False,
            }
            return report

    import fred_pipeline.ecb_archive_inventory as inventory_module

    monkeypatch.setattr(inventory_module, "ECBArchiveInventoryClient", _InventoryClient)
    output_path = tmp_path / "inventory.json"
    args = cli.build_parser().parse_args(
        [
            "ecb-assets",
            "inventory",
            "--from-year",
            "2010",
            "--to-year",
            "2010",
            "--summary-only",
            "--max-archive-mb",
            "64",
            "--output",
            str(output_path),
        ]
    )

    assert cli._cmd_ecb_assets(args) == 0
    assert json.loads(output_path.read_text())["summary"]["snapshot_count"] == 190
    assert json.loads(capsys.readouterr().out)["output"] == str(output_path)
