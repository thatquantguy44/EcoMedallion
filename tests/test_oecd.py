"""Tests for the OECD SDMX client.

Fixtures are recorded SDMX-CSV, so the suite never touches the network — the
same offline discipline `test_bls_discovery.py` and `test_ecb_discovery.py`
already follow.
"""

import pytest

from fred_pipeline.sources.oecd import (
    OECDAPIError,
    OECDClient,
    _oecd_period_from_date,
    _oecd_period_to_date,
    normalize_oecd_observations,
    parse_oecd_series_id,
)

# Recorded verbatim from the live API on 2026-09-12:
#   data/OECD.SDD.STES,DSD_STES@DF_CLI,/USA.M.LI...AA...H
CLI_CSV = (
    "DATAFLOW,REF_AREA,FREQ,MEASURE,UNIT_MEASURE,ACTIVITY,ADJUSTMENT,"
    "TRANSFORMATION,TIME_HORIZ,METHODOLOGY,TIME_PERIOD,OBS_VALUE,OBS_STATUS,"
    "UNIT_MULT,DECIMALS,BASE_PER\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),USA,M,LI,IX,_Z,AA,IX,_Z,H,"
    "2025-06,99.54102,A,0,2,\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),USA,M,LI,IX,_Z,AA,IX,_Z,H,"
    "2025-05,99.52384,A,0,2,\n"
    "OECD.SDD.STES:DSD_STES@DF_CLI(4.1),USA,M,LI,IX,_Z,AA,IX,_Z,H,"
    "2025-04,,A,0,2,\n"
)

SERIES_ID = "OECD:OECD.SDD.STES:DSD_STES@DF_CLI:USA.M.LI...AA...H"


class _Response:
    def __init__(self, text=""):
        self.status_code = 200
        self.text = text


class _Session:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append(
            {"url": url, "params": params, "timeout": timeout, "headers": headers}
        )
        return self._response


# ---- series id parsing -------------------------------------------------


def test_parse_series_id_with_and_without_source_prefix():
    assert parse_oecd_series_id(SERIES_ID) == (
        "OECD.SDD.STES",
        "DSD_STES@DF_CLI",
        "USA.M.LI...AA...H",
    )
    # bare coordinates (no OECD: prefix) parse identically
    assert parse_oecd_series_id("OECD.SDD.STES:DSD_STES@DF_CLI:USA.M.LI...AA...H") == (
        "OECD.SDD.STES",
        "DSD_STES@DF_CLI",
        "USA.M.LI...AA...H",
    )


@pytest.mark.parametrize("bad", ["", "   ", "OECD", "agency:dataflow", "a:b:c:d:e"])
def test_parse_series_id_rejects_malformed(bad):
    with pytest.raises(OECDAPIError):
        parse_oecd_series_id(bad)


def test_agency_is_not_assumed_to_be_oecd():
    """The catalogue serves ESTAT- and IAEG-SDGs-owned flows too, so the
    agency must come from the id rather than being hardcoded."""
    agency, dataflow, key = parse_oecd_series_id("OECD:ESTAT:SEEA_AEA_A:FR.A")
    assert agency == "ESTAT"
    assert dataflow == "SEEA_AEA_A"
    assert key == "FR.A"


# ---- period mapping ----------------------------------------------------


@pytest.mark.parametrize(
    "period,expected",
    [
        ("2025-06", "2025-06-01"),
        ("2024", "2024-01-01"),
        ("2025-Q2", "2025-04-01"),
        ("2025-S2", "2025-07-01"),
        ("2025-06-17", "2025-06-17"),
        ("", None),
        (None, None),
        ("not-a-date", None),
    ],
)
def test_period_to_date(period, expected):
    assert _oecd_period_to_date(period) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2025-01-01", "2025-01"),
        ("2025-01", "2025-01"),
        ("2025", "2025"),
        (None, None),
        ("", None),
    ],
)
def test_period_from_date(value, expected):
    assert _oecd_period_from_date(value) == expected


# ---- normalization -----------------------------------------------------


def test_normalize_maps_csv_to_canonical_silver_rows():
    rows = normalize_oecd_observations(SERIES_ID, {"data": CLI_CSV}, run_id="r1")
    assert len(rows) == 3

    first = rows[0]
    assert first["series_id"] == SERIES_ID
    assert first["source"] == "oecd"
    assert first["observation_date"] == "2025-06-01"
    assert first["value"] == pytest.approx(99.54102)
    assert first["is_missing"] is False
    assert first["run_id"] == "r1"


def test_normalize_flags_blank_observations_as_missing():
    rows = normalize_oecd_observations(SERIES_ID, {"data": CLI_CSV})
    blank = next(r for r in rows if r["observation_date"] == "2025-04-01")
    assert blank["value"] is None
    assert blank["is_missing"] is True


def test_normalize_leaves_vintage_columns_empty():
    """OECD publishes no VALID_FROM/VALID_TO, so there is no vintage to record.
    track_vintage is accepted for contract parity but must not invent one."""
    for track in (True, False):
        rows = normalize_oecd_observations(
            SERIES_ID, {"data": CLI_CSV}, track_vintage=track
        )
        assert all(r["realtime_start"] == "" for r in rows)
        assert all(r["realtime_end"] == "" for r in rows)


def test_normalize_skips_deleted_rows():
    csv_with_delete = CLI_CSV.replace(
        "DATAFLOW,REF_AREA", "ACTION,DATAFLOW,REF_AREA"
    ).replace("OECD.SDD.STES:DSD_STES@DF_CLI(4.1),USA", "delete,X,USA", 1)
    rows = normalize_oecd_observations(SERIES_ID, {"data": csv_with_delete})
    assert all(r["observation_date"] != "2025-06-01" for r in rows)


def test_normalize_empty_payload_returns_no_rows():
    assert normalize_oecd_observations(SERIES_ID, {"data": ""}) == []


# ---- client ------------------------------------------------------------


def test_endpoint_sends_empty_version_segment():
    """The trailing comma leaves the version open so OECD serves the current
    one -- pinning would 404 when the dataflow version rolls."""
    client = OECDClient(session=_Session(_Response()), sleep=lambda _s: None)
    assert (
        client.observations_endpoint(SERIES_ID)
        == "data/OECD.SDD.STES,DSD_STES@DF_CLI,/USA.M.LI...AA...H"
    )


def test_get_observations_requests_sdmx_csv_with_period_window():
    session = _Session(_Response(CLI_CSV))
    client = OECDClient(session=session, sleep=lambda _s: None)

    payload = client.get_observations(
        SERIES_ID, observation_start="2025-01-01", observation_end="2025-06-30"
    )

    assert payload["meta"]["agency"] == "OECD.SDD.STES"
    assert payload["meta"]["format"] == "sdmx-csv"
    call = session.calls[0]
    assert call["params"]["startPeriod"] == "2025-01"
    assert call["params"]["endPeriod"] == "2025-06"
    assert "sdmx.data+csv" in call["headers"]["Accept"]


def test_get_observations_omits_period_params_when_unset():
    session = _Session(_Response(CLI_CSV))
    client = OECDClient(session=session, sleep=lambda _s: None)
    client.get_observations(SERIES_ID)
    assert session.calls[0]["params"] == {}


def test_client_normalize_round_trips_through_contract():
    session = _Session(_Response(CLI_CSV))
    client = OECDClient(session=session, sleep=lambda _s: None)
    rows = client.normalize(SERIES_ID, client.get_observations(SERIES_ID))
    assert [r["observation_date"] for r in rows] == [
        "2025-06-01",
        "2025-05-01",
        "2025-04-01",
    ]
