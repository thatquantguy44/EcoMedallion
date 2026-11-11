"""Tests for the US Census source client."""

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.census import (
    CensusClient,
    normalize_census_observations,
)
from fred_pipeline.transform import SILVER_COLUMNS

SID = "timeseries/eits/marts:category_code=44X72,data_type_code=SM,seasonally_adj=yes"


def _payload(rows):
    return [["cell_value", "time"], *rows]


ROWS = [["625000", "2024-11"], ["631000", "2024-12"]]


def _client(session, **kw):
    return CensusClient(session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- transport / predicates / key ----------------------------------------


def test_get_observations_builds_predicates(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(ROWS))])
    _client(session, api_key="census-key").get_observations(
        SID, observation_start="2020-01-01"
    )
    call = session.calls[0]
    assert call["url"].endswith("/timeseries/eits/marts")
    params = call["params"]
    # Census returns time from its required predicate; it is not a get variable.
    assert params["get"] == "cell_value"
    assert params["category_code"] == "44X72"
    assert params["data_type_code"] == "SM"
    assert params["seasonally_adj"] == "yes"
    assert params["time"] == "from 2020"
    assert params["key"] == "census-key"


def test_get_observations_requires_time_without_date_bounds(
    fake_session_cls,
    fake_response_cls,
):
    session = fake_session_cls([fake_response_cls(_payload(ROWS))])
    _client(session).get_observations(SID)
    assert session.calls[0]["params"].get("time")


def test_get_observations_includes_end_year_in_time_range(
    fake_session_cls,
    fake_response_cls,
):
    session = fake_session_cls([fake_response_cls(_payload(ROWS))])
    _client(session).get_observations(
        SID, observation_start="2020-01-01", observation_end="2024-12-31"
    )
    assert session.calls[0]["params"]["time"] == "from 2020 to 2024"


def test_get_observations_includes_end_year_without_start(
    fake_session_cls,
    fake_response_cls,
):
    session = fake_session_cls([fake_response_cls(_payload(ROWS))])
    _client(session).get_observations(SID, observation_end="2024-12-31")
    assert "to 2024" in session.calls[0]["params"].get("time", "")


def test_keyless_omits_key(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(ROWS))])
    _client(session).get_observations(SID)
    assert "key" not in session.calls[0]["params"]


# ---- normalization -------------------------------------------------------


def test_normalize_maps_array_to_schema():
    rows = normalize_census_observations(SID, _payload(ROWS), run_id="r")
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "census"
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-11-01"]["value"] == 625000
    assert by_date["2024-12-01"]["value"] == 631000


def test_column_order_independent():
    # normalize must locate columns by header name, not position
    payload = [["time", "cell_value"], ["2024-01", "700000"]]
    rows = normalize_census_observations(SID, payload)
    assert rows[0]["observation_date"] == "2024-01-01"
    assert rows[0]["value"] == 700000


# ---- downstream + orchestrator routing -----------------------------------


def test_census_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_census_observations(SID, _payload(ROWS), run_id="r")
    report = run_quality_checks(
        SID, rows, profile="standard", frequency="m", min_value=0
    )
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_pipeline_routes_census_end_to_end(
    tmp_path, fake_session_cls, fake_response_cls
):
    session = fake_session_cls([fake_response_cls(_payload(ROWS))])
    client = CensusClient(session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(
        _config(), clients={"census": client}, warehouse=wh, persist_audit=False
    )

    spec = SeriesSpec(
        series_id=SID,
        title="Retail Sales",
        frequency="m",
        source="census",
        vintage_enabled=False,
    )
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    assert session.calls[0]["url"].endswith("/timeseries/eits/marts")
    rows = wh.query(
        "SELECT source, count(*) c FROM silver_fred_observation GROUP BY source"
    )
    assert rows == [{"source": "census", "c": 2}]
    wh.close()


def test_census_client_satisfies_source_protocol():
    assert isinstance(CensusClient(session=object()), SourceClient)
