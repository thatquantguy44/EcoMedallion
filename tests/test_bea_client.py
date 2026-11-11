"""Tests for the BEA source client."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.bea import BEAAPIError, BEAClient, normalize_bea_observations
from fred_pipeline.transform import SILVER_COLUMNS

SID = "NIPA:T10101:1:Q"


def _payload(data):
    return {"BEAAPI": {"Results": {"Statistic": "GDP", "Data": data}}}


DATA = [
    {
        "TableName": "T10101",
        "LineNumber": "1",
        "TimePeriod": "2024Q1",
        "DataValue": "1.6",
    },
    {
        "TableName": "T10101",
        "LineNumber": "1",
        "TimePeriod": "2024Q2",
        "DataValue": "3.0",
    },
    {
        "TableName": "T10101",
        "LineNumber": "2",
        "TimePeriod": "2024Q1",
        "DataValue": "9.9",
    },
]


def _client(session, **kw):
    return BEAClient(api_key="bea-key", session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- transport / auth / errors -------------------------------------------


def test_requires_api_key():
    with pytest.raises(BEAAPIError):
        BEAClient(api_key="", session=object())


def test_get_observations_builds_params(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(DATA))])
    _client(session).get_observations(SID)
    params = session.calls[0]["params"]
    assert params["UserID"] == "bea-key"
    assert params["datasetname"] == "NIPA"
    assert params["TableName"] == "T10101"
    assert params["Frequency"] == "Q"
    assert params["method"] == "GetData"
    assert params["Year"] == "ALL"


def test_get_observations_fetches_shared_table_once_and_normalizes_each_line(
    fake_session_cls,
    fake_response_cls,
):
    body = _payload(
        [
            {
                "TableName": "U20404",
                "LineNumber": "1",
                "TimePeriod": "2025M01",
                "DataValue": "120.5",
            },
            {
                "TableName": "U20404",
                "LineNumber": "10",
                "TimePeriod": "2025M01",
                "DataValue": "115.3",
            },
        ]
    )
    session = fake_session_cls([fake_response_cls(body), fake_response_cls(body)])
    client = _client(session)

    first_id = "NIUnderlyingDetail:U20404:1:M"
    second_id = "NIUnderlyingDetail:U20404:10:M"
    bounds = {"observation_start": "2025-01-01", "observation_end": "2025-12-31"}
    first_rows = client.normalize(first_id, client.get_observations(first_id, **bounds))
    second_rows = client.normalize(
        second_id, client.get_observations(second_id, **bounds)
    )

    assert [
        (r["series_id"], r["observation_date"], r["value"]) for r in first_rows
    ] == [(first_id, "2025-01-01", 120.5)]
    assert [
        (r["series_id"], r["observation_date"], r["value"]) for r in second_rows
    ] == [(second_id, "2025-01-01", 115.3)]
    assert len(session.calls) == 1


@pytest.mark.parametrize(
    "other_id",
    [
        "NIUnderlyingDetail:T10101:2:Q",  # different dataset
        "NIPA:T10102:2:Q",  # different table
        "NIPA:T10101:2:A",  # different frequency
    ],
    ids=["dataset", "table", "frequency"],
)
def test_get_observations_does_not_share_different_table_requests(
    other_id,
    fake_session_cls,
    fake_response_cls,
):
    first_body = _payload(DATA)
    second_body = _payload(
        [
            {"LineNumber": "2", "TimePeriod": "2024", "DataValue": "12.5"},
        ]
    )
    session = fake_session_cls(
        [
            fake_response_cls(first_body),
            fake_response_cls(second_body),
        ]
    )
    client = _client(session)

    bounds = {"observation_start": "2025-01-01", "observation_end": "2025-12-31"}
    assert client.get_observations(SID, **bounds) == first_body
    assert client.get_observations(other_id, **bounds) == second_body
    assert len(session.calls) == 2


def test_get_observations_does_not_share_tables_between_clients(
    fake_session_cls,
    fake_response_cls,
):
    first_body = _payload(DATA)
    newer_body = _payload(
        [
            {"LineNumber": "1", "TimePeriod": "2024Q1", "DataValue": "2.0"},
        ]
    )
    session = fake_session_cls(
        [
            fake_response_cls(first_body),
            fake_response_cls(newer_body),
        ]
    )

    assert _client(session).get_observations(SID) == first_body
    assert _client(session).get_observations(SID) == newer_body
    assert len(session.calls) == 2


def test_error_block_raises(fake_session_cls, fake_response_cls):
    body = {"BEAAPI": {"Error": {"APIErrorDescription": "Invalid table name"}}}
    session = fake_session_cls([fake_response_cls(body)])
    with pytest.raises(BEAAPIError) as exc:
        _client(session).get_observations(SID)
    assert "Invalid table name" in str(exc.value)


def test_get_observations_does_not_cache_bea_error_payload(
    fake_session_cls,
    fake_response_cls,
):
    error_body = {"BEAAPI": {"Error": {"APIErrorDescription": "Temporary limit"}}}
    success_body = _payload(DATA)
    session = fake_session_cls(
        [
            fake_response_cls(error_body),
            fake_response_cls(success_body),
        ]
    )
    client = _client(session)

    with pytest.raises(BEAAPIError, match="Temporary limit"):
        client.get_observations(SID)
    # The next line needs the same request and must be able to fetch it again.
    assert client.get_observations("NIPA:T10101:2:Q") == success_body
    assert len(session.calls) == 2


@pytest.mark.parametrize(
    "bounds,first_year,uses_metadata",
    [
        ({}, 2004, True),
        (
            {"observation_start": "2010-04-03", "observation_end": "2025-08-31"},
            2010,
            False,
        ),
    ],
    ids=["full_history", "explicit_bounds"],
)
def test_monthly_underlying_detail_batches_years_and_preserves_provenance(
    bounds,
    first_year,
    uses_metadata,
    fake_response_cls,
):
    class FakeSession:
        def __init__(self):
            self.calls = []
            self.raw_rows = []
            self.batches = []

        def get(self, url, params=None, timeout=None, headers=None):
            self.calls.append(dict(params))
            if params["method"] == "GetParameterValues":
                return fake_response_cls(
                    {
                        "BEAAPI": {
                            "Results": {
                                "ParamValue": [
                                    {
                                        "TableName": "U20100",
                                        "FirstMonthlyYear": "1959",
                                        "LastMonthlyYear": "2026",
                                    },
                                    {
                                        "TableName": "U20404",
                                        "FirstMonthlyYear": "2004",
                                        "LastMonthlyYear": "2025",
                                    },
                                ]
                            }
                        }
                    }
                )
            years = (
                list(range(2004, 2026))
                if params["Year"] == "ALL"
                else [int(year) for year in params["Year"].split(",")]
            )
            data = [
                {
                    "TableName": "U20404",
                    "LineNumber": line,
                    "TimePeriod": f"{year}M01",
                    "DataValue": str(year + int(line)),
                    "CL_UNIT": "Index",
                }
                for year in years
                for line in ("1", "10")
            ]
            production_time = f"2026-11-11T00:00:{len(self.calls):02d}"
            body = _payload(data)
            body["BEAAPI"]["Results"]["UTCProductionTime"] = production_time
            self.raw_rows.extend(dict(row) for row in data)
            self.batches.append(
                {
                    "Year": params["Year"],
                    "RowCount": len(data),
                    "UTCProductionTime": production_time,
                }
            )
            return fake_response_cls(body)

    session = FakeSession()
    client = _client(session)
    first_id = "NIUnderlyingDetail:U20404:1:M"
    second_id = "NIUnderlyingDetail:U20404:10:M"

    payload = client.get_observations(first_id, **bounds)
    second_payload = client.get_observations(second_id, **bounds)

    metadata_calls = [p for p in session.calls if p["method"] == "GetParameterValues"]
    assert len(metadata_calls) == int(uses_metadata)
    if uses_metadata:
        assert session.calls[0]["method"] == "GetParameterValues"
        assert metadata_calls[0]["datasetname"] == "NIUnderlyingDetail"
        assert metadata_calls[0]["ParameterName"] == "Year"
    data_calls = [p for p in session.calls if p["method"] == "GetData"]
    assert all(
        p["datasetname"] == "NIUnderlyingDetail"
        and p["TableName"] == "U20404"
        and p["Frequency"] == "M"
        for p in data_calls
    )
    assert all(p["Year"] != "ALL" for p in data_calls)
    batch_years = [[int(year) for year in p["Year"].split(",")] for p in data_calls]
    assert all(1 <= len(years) <= 10 for years in batch_years)
    assert [year for years in batch_years for year in years] == list(
        range(first_year, 2026)
    )
    assert payload["BEAAPI"]["Results"]["Data"] == session.raw_rows
    assert payload["BEAAPI"]["RequestBatches"] == session.batches
    for series_id, body, line in (
        (first_id, payload, "1"),
        (second_id, second_payload, "10"),
    ):
        rows = client.normalize(series_id, body)
        assert [(r["observation_date"], r["value"]) for r in rows] == [
            (f"{year}-01-01", float(year + int(line)))
            for year in range(first_year, 2026)
        ]


def test_monthly_underlying_detail_cache_distinguishes_year_windows(
    fake_session_cls,
    fake_response_cls,
):
    windows = ((2024, 2024), (2025, 2025), (2024, 2025))
    bodies = [
        _payload(
            [
                {
                    "TableName": "U20404",
                    "LineNumber": "1",
                    "TimePeriod": f"{year}M01",
                    "DataValue": str(year),
                }
                for year in range(start, end + 1)
            ]
        )
        for start, end in windows
    ]
    session = fake_session_cls([fake_response_cls(body) for body in bodies])
    client = _client(session)
    series_id = "NIUnderlyingDetail:U20404:1:M"

    for (start, end), body in zip(windows, bodies):
        payload = client.get_observations(
            series_id,
            observation_start=f"{start}-01-01",
            observation_end=f"{end}-12-31",
        )
        assert payload["BEAAPI"]["Results"]["Data"] == body["BEAAPI"]["Results"]["Data"]
    assert [call["params"]["Year"] for call in session.calls] == [
        "2024",
        "2025",
        "2024,2025",
    ]


@pytest.mark.parametrize(
    "error_body",
    [
        {"BEAAPI": {"Error": {"APIErrorDescription": "Temporary limit"}}},
        {"BEAAPI": {"Results": {"Error": {"APIErrorDescription": "Temporary limit"}}}},
    ],
    ids=["top_level", "nested_results"],
)
def test_monthly_underlying_detail_retries_complete_capture_after_batch_error(
    error_body,
    fake_session_cls,
    fake_response_cls,
):
    def data_row(year, value):
        return {
            "TableName": "U20404",
            "LineNumber": "1",
            "TimePeriod": f"{year}M01",
            "DataValue": value,
        }

    fresh_rows = [data_row(2000, "1.1"), data_row(2010, "2.0")]
    session = fake_session_cls(
        [
            fake_response_cls(_payload([data_row(2000, "1.0")])),
            fake_response_cls(error_body),
            fake_response_cls(_payload(fresh_rows[:1])),
            fake_response_cls(_payload(fresh_rows[1:])),
        ]
    )
    client = _client(session)
    series_id = "NIUnderlyingDetail:U20404:1:M"
    bounds = {"observation_start": "2000-01-01", "observation_end": "2010-12-31"}

    with pytest.raises(BEAAPIError, match="Temporary limit"):
        client.get_observations(series_id, **bounds)
    payload = client.get_observations(series_id, **bounds)

    assert payload["BEAAPI"]["Results"]["Data"] == fresh_rows
    assert [r["value"] for r in client.normalize(series_id, payload)] == [1.1, 2.0]
    assert len(session.calls) == 4
    assert session.calls[0]["params"]["Year"] == session.calls[2]["params"]["Year"]
    assert session.calls[1]["params"]["Year"] == session.calls[3]["params"]["Year"]


def test_get_observations_preserves_list_shaped_results(
    fake_session_cls,
    fake_response_cls,
):
    body = {"BEAAPI": {"Results": [{"Statistic": "GDP", "Data": DATA}]}}
    session = fake_session_cls([fake_response_cls(body)])
    client = _client(session)

    payload = client.get_observations(SID)

    assert payload == body
    assert [row["value"] for row in client.normalize(SID, payload)] == [1.6, 3.0]


# ---- normalization -------------------------------------------------------


def test_normalize_selects_line_and_maps_schema():
    rows = normalize_bea_observations(SID, _payload(DATA), run_id="r")
    # only LineNumber == 1 rows for this series (line 2 dropped)
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "bea"
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-01-01"]["value"] == 1.6  # Q1 -> Jan
    assert by_date["2024-04-01"]["value"] == 3.0  # Q2 -> Apr


def test_bad_series_id_raises():
    with pytest.raises(BEAAPIError):
        normalize_bea_observations("NIPA:T10101", {"BEAAPI": {}})


# ---- downstream + orchestrator routing -----------------------------------


def test_bea_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_bea_observations(SID, _payload(DATA), run_id="r")
    report = run_quality_checks(SID, rows, profile="standard", frequency="q")
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)  # idempotent
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_pipeline_routes_bea_end_to_end(tmp_path, fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(DATA))])
    client = BEAClient(api_key="bea-key", session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(
        _config(), clients={"bea": client}, warehouse=wh, persist_audit=False
    )

    spec = SeriesSpec(
        series_id=SID,
        title="Real GDP",
        frequency="q",
        source="bea",
        vintage_enabled=False,
    )
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    rows = wh.query(
        "SELECT source, count(*) c FROM silver_fred_observation GROUP BY source"
    )
    assert rows == [{"source": "bea", "c": 2}]
    wh.close()


def test_pipeline_fetches_fresh_bea_table_on_each_run(
    tmp_path,
    fake_session_cls,
    fake_response_cls,
):
    session = fake_session_cls(
        [
            fake_response_cls(
                _payload(
                    [
                        {
                            "TableName": "T10101",
                            "LineNumber": "1",
                            "TimePeriod": "2024Q1",
                            "DataValue": value,
                        },
                    ]
                )
            )
            for value in ("1.0", "2.0")
        ]
    )
    client = _client(session)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(
        _config(), clients={"bea": client}, warehouse=wh, persist_audit=False
    )
    spec = SeriesSpec(
        series_id=SID,
        title="Real GDP",
        frequency="q",
        source="bea",
        vintage_enabled=False,
    )

    try:
        for expected_value in (1.0, 2.0):
            run = pipe.run([spec], skip_not_due=False, build_gold_layer=False)
            assert run.status == RunStatus.SUCCEEDED
            assert wh.query(
                "SELECT value FROM silver_fred_observation "
                "WHERE source = 'bea' AND series_id = ?",
                (SID,),
            ) == [{"value": expected_value}]
        assert len(session.calls) == 2
    finally:
        wh.close()


def test_bea_client_satisfies_source_protocol():
    assert isinstance(BEAClient(api_key="k", session=object()), SourceClient)
