"""Tests for the ECB Data Portal source client."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import Manifest, SeriesSpec
from fred_pipeline.pipeline import (
    SOURCE_FACTORIES,
    SOURCE_KEY_REQUIREMENTS,
    FredPipeline,
)
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.replay import replay_from_bronze
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.ecb import (
    ECB_ACCEPT,
    ECBAPIError,
    ECBClient,
    _ecb_period_to_date,
    normalize_ecb_observations,
    parse_ecb_series_id,
)
from fred_pipeline.transform import SILVER_COLUMNS

SID = "ECB:EXR:D.USD.EUR.SP00.A"

CURRENT_CSV = (
    "KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,"
    "OBS_STATUS\n"
    "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-02,1.0956,A\n"
    "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-03,1.0919,A\n"
)

HISTORY_CSV = (
    "KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,"
    "ACTION,VALID_FROM,VALID_TO\n"
    "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-02,1.0956,Replace,"
    "2024-01-02T15:57:01.000+01:00,\n"
    "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-03,1.0919,Replace,"
    "2024-01-03T15:56:59.000+01:00,\n"
)


def _payload(csv_text=CURRENT_CSV, include_history=False):
    return {
        "data": csv_text,
        "meta": {
            "series_id": SID,
            "flow_ref": "EXR",
            "key": "D.USD.EUR.SP00.A",
            "format": "sdmx-csv",
            "include_history": include_history,
        },
    }


def _client(session, **kw):
    return ECBClient(session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _manifest(spec):
    return Manifest.from_dict({"name": "t", "series": [spec.to_dict()]})


# ---- transport / parsing / errors ---------------------------------------


def test_parse_ecb_series_id_accepts_flow_and_key():
    assert parse_ecb_series_id(SID) == ("EXR", "D.USD.EUR.SP00.A")
    assert parse_ecb_series_id("ECB:ECB,EXR,1.0:D.USD.EUR.SP00.A") == (
        "ECB,EXR,1.0",
        "D.USD.EUR.SP00.A",
    )


@pytest.mark.parametrize("series_id", ["EXR:D.USD.EUR.SP00.A", "ECB:EXR", "ECB::"])
def test_parse_ecb_series_id_rejects_bad_shape(series_id):
    with pytest.raises(ECBAPIError):
        parse_ecb_series_id(series_id)


def test_observations_endpoint_builds_data_endpoint():
    assert ECBClient(session=object()).observations_endpoint(SID) == (
        "data/EXR/D.USD.EUR.SP00.A"
    )


def test_get_observations_requests_csv_and_period_bounds(
    fake_session_cls, fake_response_cls
):
    session = fake_session_cls([fake_response_cls({}, text=CURRENT_CSV)])
    payload = _client(session).get_observations(
        SID, observation_start="2024-01-02", observation_end="2024-01-03"
    )

    call = session.calls[0]
    assert call["url"].endswith("/data/EXR/D.USD.EUR.SP00.A")
    assert call["params"] == {
        "format": "csvdata",
        "detail": "full",
        "startPeriod": "2024-01-02",
        "endPeriod": "2024-01-03",
    }
    assert call["headers"]["Accept"] == ECB_ACCEPT
    assert payload["data"] == CURRENT_CSV
    assert payload["meta"]["include_history"] is False


def test_get_observations_sets_include_history_when_vintage_kwargs_present(
    fake_session_cls, fake_response_cls
):
    session = fake_session_cls([fake_response_cls({}, text=HISTORY_CSV)])
    payload = _client(session).get_observations(
        SID, realtime_start="1776-07-04", realtime_end="9999-12-31"
    )

    params = session.calls[0]["params"]
    assert params["includeHistory"] == "true"
    assert params["detail"] == "full"
    assert payload["meta"]["include_history"] is True


def test_error_detail_reads_response_text(fake_session_cls, fake_response_cls):
    session = fake_session_cls(
        [fake_response_cls({}, status_code=404, text="No results found")]
    )
    with pytest.raises(ECBAPIError) as exc:
        _client(session).get_observations(SID)
    assert "No results found" in str(exc.value)


# ---- period mapping / normalization -------------------------------------


@pytest.mark.parametrize(
    "period, expected",
    [
        ("2024", "2024-01-01"),
        ("2024-S2", "2024-07-01"),
        ("2024-Q3", "2024-07-01"),
        ("2024Q1", "2024-01-01"),
        ("2024-W01", "2024-01-01"),
        ("2024-06", "2024-06-01"),
        ("202406", "2024-06-01"),
        ("2024-06-15", "2024-06-15"),
        ("20240615", "2024-06-15"),
        ("nonsense", None),
    ],
)
def test_ecb_period_mapping(period, expected):
    assert _ecb_period_to_date(period) == expected


def test_normalize_current_csv_matches_canonical_silver_schema():
    rows = normalize_ecb_observations(SID, _payload(), run_id="r", track_vintage=False)
    assert len(rows) == 2
    for row in rows:
        assert set(SILVER_COLUMNS).issubset(row)
        assert row["source"] == "ecb"
        assert row["realtime_start"] == ""
        assert row["realtime_end"] == ""
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-01-02"]["value"] == pytest.approx(1.0956)
    assert by_date["2024-01-02"]["raw_value"] == "1.0956"


def test_normalize_history_csv_maps_valid_from_to_realtime_fields():
    rows = normalize_ecb_observations(
        SID, _payload(HISTORY_CSV, include_history=True), run_id="r"
    )
    assert rows[0]["realtime_start"] == "2024-01-02"
    assert rows[0]["realtime_end"] == ""
    assert rows[1]["realtime_start"] == "2024-01-03"


def test_blank_value_is_missing():
    payload = _payload("KEY,TIME_PERIOD,OBS_VALUE\nEXR.D.USD.EUR.SP00.A,2024-01-02,\n")
    rows = normalize_ecb_observations(SID, payload, track_vintage=False)
    assert rows[0]["is_missing"] is True
    assert rows[0]["value"] is None


def test_delete_action_is_retained_as_a_tombstone():
    payload = _payload(
        "KEY,TIME_PERIOD,OBS_VALUE,ACTION,VALID_FROM,VALID_TO\n"
        "EXR.D.USD.EUR.SP00.A,2015-07-27,,Delete,,"
        "2015-07-27T15:25:34.000+02:00\n"
        "EXR.D.USD.EUR.SP00.A,2015-07-27,1.1058,Replace,"
        "2015-07-28T15:16:04.000+02:00,\n",
        include_history=True,
    )

    rows = normalize_ecb_observations(SID, payload)

    assert len(rows) == 2
    deleted, replacement = rows
    assert deleted["observation_date"] == "2015-07-27"
    assert deleted["is_deleted"] is True
    assert deleted["value"] is None
    assert replacement["value"] == pytest.approx(1.1058)
    assert replacement["realtime_start"] == "2015-07-28"


def test_exr_metadata_and_exact_value_are_retained_and_hashed():
    rows = normalize_ecb_observations(
        SID,
        _payload(
            "KEY,CURRENCY,CURRENCY_DENOM,UNIT_MULT,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
            "EXR.D.USD.EUR.SP00.A,USD,EUR,0,2024-01-02,"
            "1.123456789012345678900,A\n"
        ),
    )
    row = rows[0]
    assert row["base_currency"] == "EUR"
    assert row["quote_currency"] == "USD"
    assert row["exact_value"] == "1.123456789012345678900"
    assert row["unit_multiplier"] == 0
    assert row["observation_status"] == "A"

    changed_status = normalize_ecb_observations(
        SID,
        _payload(
            "KEY,CURRENCY,CURRENCY_DENOM,UNIT_MULT,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
            "EXR.D.USD.EUR.SP00.A,USD,EUR,0,2024-01-02,"
            "1.123456789012345678900,E\n"
        ),
    )[0]
    assert changed_status["row_hash"] != row["row_hash"]


def test_exr_rejects_conflicting_currency_or_nonzero_scale():
    currency_payload = _payload(
        "KEY,CURRENCY,CURRENCY_DENOM,TIME_PERIOD,OBS_VALUE\n"
        "EXR.D.USD.EUR.SP00.A,JPY,EUR,2024-01-02,1.1\n"
    )
    with pytest.raises(ECBAPIError, match="currency mismatch"):
        normalize_ecb_observations(SID, currency_payload)

    scale_payload = _payload(
        "KEY,CURRENCY,CURRENCY_DENOM,UNIT_MULT,TIME_PERIOD,OBS_VALUE\n"
        "EXR.D.USD.EUR.SP00.A,USD,EUR,3,2024-01-02,1.1\n"
    )
    with pytest.raises(ECBAPIError, match="UNIT_MULT"):
        normalize_ecb_observations(SID, scale_payload)


# ---- downstream + orchestrator routing ----------------------------------


def test_ecb_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_ecb_observations(SID, _payload(), run_id="r", track_vintage=False)
    report = run_quality_checks(
        SID, rows, profile="standard", frequency="d", min_value=0
    )
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_local_gold_persists_ecb_fx_reference_rates(tmp_path):
    usd_csv = (
        "KEY,CURRENCY,CURRENCY_DENOM,UNIT_MULT,TIME_PERIOD,OBS_VALUE\n"
        "EXR.D.USD.EUR.SP00.A,USD,EUR,0,2024-01-02,1.25\n"
    )
    jpy_csv = (
        "KEY,CURRENCY,CURRENCY_DENOM,UNIT_MULT,TIME_PERIOD,OBS_VALUE\n"
        "EXR.D.JPY.EUR.SP00.A,JPY,EUR,0,2024-01-02,150\n"
    )
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "fx.db"))
    wh.merge_silver(normalize_ecb_observations(SID, _payload(usd_csv), run_id="r"))
    jpy_sid = "ECB:EXR:D.JPY.EUR.SP00.A"
    wh.merge_silver(normalize_ecb_observations(jpy_sid, _payload(jpy_csv), run_id="r"))
    wh.build_gold()
    rows = wh.query(
        "SELECT rate_decimal, native_series_ids FROM gold_ecb_fx_reference_rate "
        "WHERE observation_date='2024-01-02' AND base_currency='USD' "
        "AND quote_currency='JPY'"
    )
    assert rows == [
        {
            "rate_decimal": "120",
            "native_series_ids": '["ECB:EXR:D.JPY.EUR.SP00.A","ECB:EXR:D.USD.EUR.SP00.A"]',
        }
    ]
    wh.close()


def test_pipeline_routes_ecb_end_to_end(tmp_path, fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls({}, text=CURRENT_CSV)])
    client = ECBClient(session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(
        _config(), clients={"ecb": client}, warehouse=wh, persist_audit=False
    )

    spec = SeriesSpec(
        series_id=SID,
        title="USD per EUR",
        frequency="d",
        source="ecb",
        vintage_enabled=False,
    )
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    assert session.calls[0]["url"].endswith("/data/EXR/D.USD.EUR.SP00.A")
    rows = wh.query(
        "SELECT source, count(*) c FROM silver_fred_observation GROUP BY source"
    )
    assert rows == [{"source": "ecb", "c": 2}]
    wh.close()


def test_replay_routes_ecb_normalizer(tmp_path, fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls({}, text=HISTORY_CSV)])
    client = ECBClient(session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(
        _config(), clients={"ecb": client}, warehouse=wh, persist_audit=False
    )
    spec = SeriesSpec(
        series_id=SID,
        title="USD per EUR",
        frequency="d",
        source="ecb",
        vintage_enabled=True,
    )
    pipe.run([spec], build_gold_layer=False)
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2

    wh.conn.execute("DELETE FROM silver_fred_observation")
    wh.conn.commit()
    result = replay_from_bronze(_config(), [_manifest(spec)], wh, rebuild_gold=False)

    assert result["bronze_payloads_replayed"] == 1
    rows = wh.query(
        "SELECT source, min(realtime_start) first_rt, count(*) c "
        "FROM silver_fred_observation GROUP BY source"
    )
    assert rows == [{"source": "ecb", "first_rt": "2024-01-02", "c": 2}]
    wh.close()


def test_ecb_client_satisfies_source_protocol():
    assert isinstance(ECBClient(session=object()), SourceClient)


def test_ecb_source_is_registered_and_keyless():
    assert "ecb" in SOURCE_FACTORIES
    assert "ecb" not in SOURCE_KEY_REQUIREMENTS
