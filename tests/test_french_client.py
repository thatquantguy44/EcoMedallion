"""Tests for the Kenneth French Data Library client.

Fixtures here are **hand-built to the library's long-documented CSV layout**,
not recorded from a live fetch — unlike `test_oecd.py`'s recorded SDMX-CSV,
this session's environment blocks egress to mba.tuck.dartmouth.edu (see
`config/data_licensing.yml`'s `french` entry), so the exact byte-for-byte
shape of a real file has not been confirmed. A human should replace this
fixture with a real recorded response before activating anything in
`manifests/french_factors.yml`. The offline-fixture discipline itself
matches `test_oecd.py`/`test_bls_discovery.py`/`test_ecb_discovery.py`: the
suite never touches the network either way.
"""

from __future__ import annotations

import io
import zipfile

import pytest

from fred_pipeline.sources.french import (
    DATASET_FILES,
    FrenchAPIError,
    FrenchClient,
    _dataset_name,
    _extract_csv_member,
    _find_monthly_block,
    _month_to_date,
    normalize_french_observations,
)

# Hand-built to match the library's documented layout: a one-line preamble,
# a blank line, the monthly table (header + YYYYMM-keyed rows), a blank
# line, then an "Annual Factors" table this client must NOT read.
FACTORS_CSV = (
    "This file was created by CMPT_ME_BEME_RETS using the 202506 CRSP "
    "database. The 1-month TBill return is from Ibbotson and Associates, "
    "Inc. Copyright 2026 Kenneth R. French\n"
    "\n"
    ",Mkt-RF,SMB,HML,RF\n"
    "192607,   2.96,  -2.30,  -2.87,   0.22\n"
    "192608,   2.64,  -1.40,   4.19,   0.25\n"
    "192609,   0.36,  -1.32,   0.01,   0.23\n"
    "202505,  -99.99,  -99.99,  -99.99,   0.35\n"
    "\n"
    " Annual Factors: January-December\n"
    "\n"
    ",Mkt-RF,SMB,HML,RF\n"
    "1926,   5.94,  -5.19,   0.19,   2.62\n"
)


def _zip_bytes(member_name: str, text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(member_name, text)
    return buf.getvalue()


class _Response:
    def __init__(self, content: bytes):
        self.status_code = 200
        self.content = content


class _Session:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self._response


# ---- dataset name resolution --------------------------------------------


def test_dataset_name_strips_stray_factor_suffix():
    assert _dataset_name("F-F_Research_Data_Factors:Mkt-RF") == "F-F_Research_Data_Factors"
    assert _dataset_name("F-F_Research_Data_Factors") == "F-F_Research_Data_Factors"


def test_dataset_name_rejects_unknown_dataset():
    with pytest.raises(FrenchAPIError):
        _dataset_name("Not_A_Real_Dataset")


# ---- date parsing ---------------------------------------------------------


@pytest.mark.parametrize(
    "token,expected",
    [
        ("192607", "1926-07-01"),
        ("202512", "2025-12-01"),
        ("2025", None),  # annual rows must not be mistaken for monthly
        ("202513", None),  # invalid month
        ("", None),
        ("abcdef", None),
    ],
)
def test_month_to_date(token, expected):
    assert _month_to_date(token) == expected


# ---- CSV block parsing -----------------------------------------------------


def test_find_monthly_block_stops_before_annual_table():
    block = _find_monthly_block(FACTORS_CSV)
    header, data = block[0], block[1:]
    assert header == ["", "Mkt-RF", "SMB", "HML", "RF"]
    assert [row[0] for row in data] == ["192607", "192608", "192609", "202505"]


def test_find_monthly_block_empty_text_returns_empty():
    assert _find_monthly_block("") == []


def test_find_monthly_block_no_header_returns_empty():
    assert _find_monthly_block("just,some,text\nwith,no,dates\n") == []


# ---- zip extraction ---------------------------------------------------------


def test_extract_csv_member_reads_the_csv_inside_the_zip():
    blob = _zip_bytes("F-F_Research_Data_Factors.CSV", FACTORS_CSV)
    assert _extract_csv_member(blob, "F-F_Research_Data_Factors") == FACTORS_CSV


def test_extract_csv_member_rejects_non_zip_bytes():
    with pytest.raises(FrenchAPIError):
        _extract_csv_member(b"not a zip file", "F-F_Research_Data_Factors")


def test_extract_csv_member_rejects_zip_with_no_csv():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no csv here")
    with pytest.raises(FrenchAPIError):
        _extract_csv_member(buf.getvalue(), "F-F_Research_Data_Factors")


# ---- normalization -----------------------------------------------------


def test_normalize_explodes_one_row_per_factor_per_month():
    payload = {"dataset": "F-F_Research_Data_Factors", "text": FACTORS_CSV}
    rows = normalize_french_observations("F-F_Research_Data_Factors", payload, run_id="r1")
    # 4 months * 4 factors = 16 rows
    assert len(rows) == 16
    series_ids = {r["series_id"] for r in rows}
    assert series_ids == {
        "F-F_Research_Data_Factors:Mkt-RF",
        "F-F_Research_Data_Factors:SMB",
        "F-F_Research_Data_Factors:HML",
        "F-F_Research_Data_Factors:RF",
    }


def test_normalize_maps_values_and_dates():
    payload = {"dataset": "F-F_Research_Data_Factors", "text": FACTORS_CSV}
    rows = normalize_french_observations("F-F_Research_Data_Factors", payload)
    mkt_rf_jul = next(
        r for r in rows
        if r["series_id"] == "F-F_Research_Data_Factors:Mkt-RF"
        and r["observation_date"] == "1926-07-01"
    )
    assert mkt_rf_jul["value"] == pytest.approx(2.96)
    assert mkt_rf_jul["source"] == "french"
    assert mkt_rf_jul["is_missing"] is False


def test_normalize_flags_missing_sentinel_as_missing():
    payload = {"dataset": "F-F_Research_Data_Factors", "text": FACTORS_CSV}
    rows = normalize_french_observations("F-F_Research_Data_Factors", payload)
    missing = [
        r for r in rows
        if r["observation_date"] == "2025-05-01" and r["series_id"].endswith(":Mkt-RF")
    ]
    assert len(missing) == 1
    assert missing[0]["value"] is None
    assert missing[0]["is_missing"] is True
    # RF was not sentineled in the fixture and must survive untouched
    rf_same_month = next(
        r for r in rows
        if r["observation_date"] == "2025-05-01" and r["series_id"].endswith(":RF")
    )
    assert rf_same_month["value"] == pytest.approx(0.35)
    assert rf_same_month["is_missing"] is False


def test_normalize_leaves_vintage_columns_empty():
    payload = {"dataset": "F-F_Research_Data_Factors", "text": FACTORS_CSV}
    for track in (True, False):
        rows = normalize_french_observations(
            "F-F_Research_Data_Factors", payload, track_vintage=track
        )
        assert all(r["realtime_start"] == "" for r in rows)
        assert all(r["realtime_end"] == "" for r in rows)


def test_normalize_only_explodes_registered_factor_columns():
    """Momentum dataset only has Mom -- it must not also emit Mkt-RF/etc.
    even if a stray column happened to share a header name."""
    mom_csv = (
        "This file was created by CMPT_ME_RETS. Copyright 2026 Kenneth R. French\n"
        "\n"
        ",Mom   \n"
        "192701,   3.42\n"
        "\n"
        " Annual Factors: January-December\n"
        "\n"
        ",Mom   \n"
        "1927,   9.11\n"
    )
    payload = {"dataset": "F-F_Momentum_Factor", "text": mom_csv}
    rows = normalize_french_observations("F-F_Momentum_Factor", payload)
    assert {r["series_id"] for r in rows} == {"F-F_Momentum_Factor:Mom"}
    assert rows[0]["value"] == pytest.approx(3.42)


def test_normalize_non_dict_payload_returns_no_rows():
    assert normalize_french_observations("F-F_Research_Data_Factors", "not a dict") == []


def test_normalize_empty_text_returns_no_rows():
    payload = {"dataset": "F-F_Research_Data_Factors", "text": ""}
    assert normalize_french_observations("F-F_Research_Data_Factors", payload) == []


# ---- client ------------------------------------------------------------


def test_observations_endpoint_maps_dataset_to_zip_path():
    client = FrenchClient(session=_Session(_Response(b"")), sleep=lambda _s: None)
    assert client.observations_endpoint("F-F_Research_Data_Factors") == (
        "/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip"
    )


def test_get_observations_downloads_and_unzips():
    blob = _zip_bytes("F-F_Research_Data_Factors.CSV", FACTORS_CSV)
    session = _Session(_Response(blob))
    client = FrenchClient(session=session, sleep=lambda _s: None)

    payload = client.get_observations("F-F_Research_Data_Factors")

    assert payload["format"] == "french-csv"
    assert payload["dataset"] == "F-F_Research_Data_Factors"
    assert payload["text"] == FACTORS_CSV
    assert session.calls[0]["url"].endswith("F-F_Research_Data_Factors_CSV.zip")


def test_get_observations_rejects_unknown_dataset():
    client = FrenchClient(session=_Session(_Response(b"")), sleep=lambda _s: None)
    with pytest.raises(FrenchAPIError):
        client.get_observations("Not_A_Real_Dataset")


def test_client_normalize_round_trips_through_contract():
    blob = _zip_bytes("F-F_Research_Data_Factors.CSV", FACTORS_CSV)
    client = FrenchClient(session=_Session(_Response(blob)), sleep=lambda _s: None)
    rows = client.normalize(
        "F-F_Research_Data_Factors", client.get_observations("F-F_Research_Data_Factors")
    )
    assert len(rows) == 16
    assert all(r["source"] == "french" for r in rows)


def test_dataset_files_cover_the_first_build_slice():
    """Locks in the three datasets this slice ships -- daily/industry-
    portfolio variants are a deliberate follow-up, not silently dropped."""
    assert set(DATASET_FILES) == {
        "F-F_Research_Data_Factors",
        "F-F_Research_Data_5_Factors_2x3",
        "F-F_Momentum_Factor",
    }
