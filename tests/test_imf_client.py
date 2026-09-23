"""Tests for the IMF SDMX 3.0 client.

⚠️ Unlike `test_oecd.py`'s fixture (recorded verbatim from a live response),
the fixture here is **hand-built to the public SDMX-JSON 2.0.0 data-message
specification** -- no live IMF response has ever been captured (see
`sources/imf.py`'s module docstring and `scripts/probe_imf_dataflows.py`).
These tests prove the decoding LOGIC is internally consistent with that
public spec; they are not evidence it matches IMF's actual dialect. The
offline-fixture discipline itself matches every other source test in this
repo -- no network either way.
"""

from __future__ import annotations

import pytest

from fred_pipeline.sources.imf import (
    IMFAPIError,
    IMFClient,
    _imf_period_to_date,
    normalize_imf_observations,
    parse_imf_series_id,
)

SERIES_ID = "IMF:IMF.STA:COFER:USA.Q.RES_USD"

# Hand-built to the public SDMX-JSON 2.0.0 data-message shape: one series
# (already pinned by the query key), three quarterly observations, one null.
COFER_PAYLOAD = {
    "data": {
        "dataSets": [
            {
                "series": {
                    "0:0": {
                        "observations": {
                            "0": [95.2],
                            "1": [96.1],
                            "2": [None],
                        }
                    }
                }
            }
        ],
        "structures": [
            {
                "dimensions": {
                    "series": [
                        {"id": "REF_AREA", "values": [{"id": "USA"}]},
                        {"id": "INDICATOR", "values": [{"id": "RES_USD"}]},
                    ],
                    "observation": [
                        {
                            "id": "TIME_PERIOD",
                            "values": [
                                {"id": "2024-Q1"},
                                {"id": "2024-Q2"},
                                {"id": "2024-Q3"},
                            ],
                        }
                    ],
                },
            }
        ],
    }
}


class _Response:
    def __init__(self, payload, status=200):
        self.status_code = status
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


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
    assert parse_imf_series_id(SERIES_ID) == ("IMF.STA", "COFER", "USA.Q.RES_USD")
    assert parse_imf_series_id("IMF.STA:COFER:USA.Q.RES_USD") == (
        "IMF.STA", "COFER", "USA.Q.RES_USD",
    )


@pytest.mark.parametrize("bad", ["", "   ", "IMF", "agency:dataflow", "a:b:c:d:e"])
def test_parse_series_id_rejects_malformed(bad):
    with pytest.raises(IMFAPIError):
        parse_imf_series_id(bad)


def test_agency_is_not_assumed_to_be_imf():
    """Mirrors OECD's own finding (spec006 §5.1): the maintaining agency for
    a flow served under IMF's catalogue is not always literally 'IMF'."""
    agency, dataflow, key = parse_imf_series_id("IMF:IMF.RES:BOP:USA.Q.CA")
    assert agency == "IMF.RES"
    assert dataflow == "BOP"
    assert key == "USA.Q.CA"


# ---- period mapping ------------------------------------------------------


@pytest.mark.parametrize(
    "period,expected",
    [
        ("2024", "2024-01-01"),
        ("2024-Q1", "2024-01-01"),
        ("2024-Q3", "2024-07-01"),
        ("2024-S2", "2024-07-01"),
        ("2024-06", "2024-06-01"),
        ("2024-06-15", "2024-06-15"),
        ("", None),
        (None, None),
        ("not-a-period", None),
    ],
)
def test_period_to_date(period, expected):
    assert _imf_period_to_date(period) == expected


# ---- normalization -----------------------------------------------------


def test_normalize_maps_json_to_canonical_silver_rows():
    rows = normalize_imf_observations(SERIES_ID, COFER_PAYLOAD, run_id="r1")
    assert len(rows) == 3

    first = rows[0]
    assert first["series_id"] == SERIES_ID
    assert first["source"] == "imf"
    assert first["observation_date"] == "2024-01-01"
    assert first["value"] == pytest.approx(95.2)
    assert first["is_missing"] is False
    assert first["run_id"] == "r1"


def test_normalize_flags_null_observations_as_missing():
    rows = normalize_imf_observations(SERIES_ID, COFER_PAYLOAD)
    blank = next(r for r in rows if r["observation_date"] == "2024-07-01")
    assert blank["value"] is None
    assert blank["is_missing"] is True


def test_normalize_leaves_vintage_columns_empty():
    for track in (True, False):
        rows = normalize_imf_observations(SERIES_ID, COFER_PAYLOAD, track_vintage=track)
        assert all(r["realtime_start"] == "" for r in rows)
        assert all(r["realtime_end"] == "" for r in rows)


def test_normalize_rejects_missing_data_key():
    with pytest.raises(IMFAPIError):
        normalize_imf_observations(SERIES_ID, {"not_data": {}})


def test_normalize_rejects_missing_structures():
    with pytest.raises(IMFAPIError):
        normalize_imf_observations(SERIES_ID, {"data": {"dataSets": []}})


def test_normalize_rejects_multiple_observation_dimensions():
    payload = {
        "data": {
            "dataSets": [{"series": {"0:0": {"observations": {"0": [1.0]}}}}],
            "structures": [
                {
                    "dimensions": {
                        "series": [],
                        "observation": [
                            {"id": "TIME_PERIOD", "values": [{"id": "2024"}]},
                            {"id": "FREQ", "values": [{"id": "Q"}]},
                        ],
                    }
                }
            ],
        }
    }
    with pytest.raises(IMFAPIError):
        normalize_imf_observations(SERIES_ID, payload)


def test_normalize_rejects_more_than_one_series():
    payload = {
        "data": {
            "dataSets": [
                {
                    "series": {
                        "0:0": {"observations": {"0": [1.0]}},
                        "0:1": {"observations": {"0": [2.0]}},
                    }
                }
            ],
            "structures": [
                {
                    "dimensions": {
                        "series": [],
                        "observation": [
                            {"id": "TIME_PERIOD", "values": [{"id": "2024"}]}
                        ],
                    }
                }
            ],
        }
    }
    with pytest.raises(IMFAPIError):
        normalize_imf_observations(SERIES_ID, payload)


def test_normalize_tolerates_singular_structure_key():
    """SDMX-JSON 1.0.0 used 'structure' (singular), not 'structures' (2.0.0's
    array) -- tolerate both dialects rather than assuming one."""
    payload = {
        "data": {
            "dataSets": [{"series": {"0": {"observations": {"0": [7.5]}}}}],
            "structure": {
                "dimensions": {
                    "series": [],
                    "observation": [
                        {"id": "TIME_PERIOD", "values": [{"id": "2024"}]}
                    ],
                }
            },
        }
    }
    rows = normalize_imf_observations(SERIES_ID, payload)
    assert rows[0]["value"] == pytest.approx(7.5)


# ---- client ------------------------------------------------------------


def test_observations_endpoint_wildcards_version():
    client = IMFClient(session=_Session(_Response({})), sleep=lambda _s: None)
    assert (
        client.observations_endpoint(SERIES_ID)
        == "data/dataflow/IMF.STA/COFER/+/USA.Q.RES_USD"
    )


def test_get_observations_sends_the_json_accept_header():
    session = _Session(_Response(COFER_PAYLOAD))
    client = IMFClient(session=session, sleep=lambda _s: None)

    payload = client.get_observations(SERIES_ID)

    assert payload["meta"]["agency"] == "IMF.STA"
    assert payload["meta"]["dataflow"] == "COFER"
    assert payload["meta"]["format"] == "sdmx-json"
    call = session.calls[0]
    assert "sdmx.data+json" in call["headers"]["Accept"]


def test_client_normalize_round_trips_through_contract():
    session = _Session(_Response(COFER_PAYLOAD))
    client = IMFClient(session=session, sleep=lambda _s: None)
    rows = client.normalize(SERIES_ID, client.get_observations(SERIES_ID))
    assert [r["observation_date"] for r in rows] == [
        "2024-01-01", "2024-04-01", "2024-07-01",
    ]  # fmt: skip
    assert all(r["source"] == "imf" for r in rows)
