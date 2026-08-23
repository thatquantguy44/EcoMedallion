"""Tests for the SEC EDGAR source client + manifest generator."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import Manifest, SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.sec import (
    SECAPIError,
    SECClient,
    build_sec_manifest,
    build_sec_series_id,
    normalize_sec_observations,
    sec_cik,
)
from fred_pipeline.transform import SILVER_COLUMNS

SID = "CIK0000320193:us-gaap/Assets:USD"


def _payload(entries):
    return {
        "cik": 320193,
        "taxonomy": "us-gaap",
        "tag": "Assets",
        "units": {"USD": entries},
    }


# two filings report the same period end (a restatement) + one later period
ENTRIES = [
    {"end": "2023-07-01", "val": 335038000000, "form": "10-Q", "filed": "2023-08-04"},
    {"end": "2023-07-01", "val": 335100000000, "form": "10-K", "filed": "2023-11-03"},
    {"end": "2023-09-30", "val": 352583000000, "form": "10-K", "filed": "2023-11-03"},
]


def _client(session, **kw):
    return SECClient(
        user_agent="test-agent (a@b.com)", session=session, sleep=lambda _s: None, **kw
    )


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- id helpers / generator ----------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        (320193, "CIK0000320193"),
        ("320193", "CIK0000320193"),
        ("CIK0000320193", "CIK0000320193"),
    ],
)
def test_sec_cik_normalization(raw, expected):
    assert sec_cik(raw) == expected


def test_build_sec_manifest_is_valid():
    data = build_sec_manifest(
        [(320193, "Apple"), ("0000789019", "Microsoft")],
        [("us-gaap", "Assets", "USD", "Total Assets")],
    )
    man = Manifest.from_dict(data)  # validates via SeriesSpec
    ids = {s.series_id for s in man.series}
    assert build_sec_series_id(320193, "us-gaap", "Assets") in ids
    assert all(s.source == "sec" and s.vintage_enabled for s in man.series)


# ---- transport -----------------------------------------------------------


def test_get_observations_sends_user_agent(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(ENTRIES))])
    _client(session).get_observations(SID)
    call = session.calls[0]
    assert call["url"].endswith(
        "/api/xbrl/companyconcept/CIK0000320193/us-gaap/Assets.json"
    )
    assert call["headers"]["User-Agent"] == "test-agent (a@b.com)"


def test_bad_series_id_raises():
    with pytest.raises(SECAPIError):
        normalize_sec_observations("CIK123:Assets", {"units": {}})


# ---- normalization: vintages ---------------------------------------------


def test_normalize_captures_filings_as_vintages():
    rows = normalize_sec_observations(SID, _payload(ENTRIES), run_id="r")
    assert len(rows) == 3
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "sec"
    # the two filings for 2023-07-01 are distinct vintages by filed date
    jul = [r for r in rows if r["observation_date"] == "2023-07-01"]
    assert {r["realtime_start"] for r in jul} == {"2023-08-04", "2023-11-03"}


def test_normalize_dedupes_same_period_and_filing_date():
    entries = [
        {
            "end": "2023-07-01",
            "val": 335038000000,
            "form": "10-Q",
            "filed": "2023-08-04",
        },
        {
            "end": "2023-07-01",
            "val": 335100000000,
            "form": "10-Q/A",
            "filed": "2023-08-04",
        },
    ]
    rows = normalize_sec_observations(SID, _payload(entries), run_id="r")
    assert len(rows) == 1
    assert rows[0]["observation_date"] == "2023-07-01"
    assert rows[0]["realtime_start"] == "2023-08-04"
    assert rows[0]["value"] == 335100000000.0
    report = run_quality_checks(SID, rows, profile="standard", frequency="q")
    assert report.passed, [f.message for f in report.failures]


def test_normalize_vintage_off_blanks_realtime():
    rows = normalize_sec_observations(SID, _payload(ENTRIES), track_vintage=False)
    assert all(r["realtime_start"] == "" for r in rows)


# ---- downstream + orchestrator routing -----------------------------------


def test_sec_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_sec_observations(SID, _payload(ENTRIES), run_id="r")
    report = run_quality_checks(
        SID, rows, profile="standard", frequency="q", min_value=0
    )
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    # three distinct (source, series_id, end, filed) vintage rows
    assert wh.merge_silver(rows) == 3
    wh.merge_silver(rows)  # idempotent
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 3
    wh.close()


def test_pipeline_routes_sec_end_to_end(tmp_path, fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(ENTRIES))])
    client = SECClient(
        user_agent="test-agent (a@b.com)", session=session, sleep=lambda _s: None
    )
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(
        _config(), clients={"sec": client}, warehouse=wh, persist_audit=False
    )

    spec = SeriesSpec(
        series_id=SID,
        title="Apple Assets",
        frequency="q",
        source="sec",
        vintage_enabled=True,
    )
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    # vintages preserved: 2 rows for the restated period end
    jul = wh.query(
        "SELECT realtime_start FROM silver_fred_observation "
        "WHERE observation_date = '2023-07-01' ORDER BY realtime_start"
    )
    assert [r["realtime_start"] for r in jul] == ["2023-08-04", "2023-11-03"]
    wh.close()


def test_sec_client_satisfies_source_protocol():
    assert isinstance(SECClient(session=object()), SourceClient)


# ---- duration disambiguation (income-statement concepts) -----------------

_NI_SID = "CIK0000320193:us-gaap/NetIncomeLoss:USD"

# For period end 2023-09-30, one 10-Q reports BOTH the quarterly (~92d) and the
# 9-month YTD (~273d) value; a 10-K reports the annual (~365d) value.
_NI_ENTRIES = [
    {
        "start": "2023-07-01",
        "end": "2023-09-30",
        "val": 23000,
        "form": "10-Q",
        "filed": "2023-11-03",
    },  # quarterly
    {
        "start": "2023-01-01",
        "end": "2023-09-30",
        "val": 74000,
        "form": "10-Q",
        "filed": "2023-11-03",
    },  # 9-month YTD
    {
        "start": "2022-10-01",
        "end": "2023-09-30",
        "val": 97000,
        "form": "10-K",
        "filed": "2023-11-03",
    },  # annual
]


def _ni_payload():
    return {"units": {"USD": _NI_ENTRIES}}


def test_duration_filter_keeps_quarterly_only():
    rows = normalize_sec_observations(_NI_SID, _ni_payload(), period="quarterly")
    # only the ~3-month fact survives (no natural-key collision)
    assert [r["value"] for r in rows] == [23000.0]


def test_duration_filter_annual_mode():
    rows = normalize_sec_observations(_NI_SID, _ni_payload(), period="annual")
    assert [r["value"] for r in rows] == [97000.0]


def test_instant_facts_always_kept_regardless_of_period():
    # Assets entries have no `start` (instant) -> kept under any period target
    rows_q = normalize_sec_observations(SID, _payload(ENTRIES), period="quarterly")
    rows_a = normalize_sec_observations(SID, _payload(ENTRIES), period="annual")
    assert len(rows_q) == len(rows_a) == 3


# A full fiscal year (start 2022-10-01, end 2023-09-30): Q1-Q3 reported as
# quarterly facts; the 10-K gives the FY figure and the Q3 10-Q the 9-month YTD.
_FY_ENTRIES = [
    {
        "start": "2022-10-01",
        "end": "2022-12-31",
        "val": 30000,
        "filed": "2023-02-01",
    },  # Q1 (~91d)
    {
        "start": "2023-01-01",
        "end": "2023-03-31",
        "val": 20000,
        "filed": "2023-05-01",
    },  # Q2 (~89d)
    {
        "start": "2023-04-01",
        "end": "2023-06-30",
        "val": 25000,
        "filed": "2023-08-01",
    },  # Q3 (~90d)
    {
        "start": "2022-10-01",
        "end": "2023-06-30",
        "val": 75000,
        "filed": "2023-08-01",
    },  # 9mo YTD (~272d)
    {
        "start": "2022-10-01",
        "end": "2023-09-30",
        "val": 100000,
        "filed": "2023-11-01",
    },  # FY (~364d)
]


def test_q4_is_synthesized_from_fy_minus_ytd():
    rows = normalize_sec_observations(
        _NI_SID, {"units": {"USD": _FY_ENTRIES}}, period="quarterly"
    )
    by_date = {r["observation_date"]: r["value"] for r in rows}
    # Q1-Q3 direct, Q4 synthesized = FY(100000) - 9moYTD(75000) = 25000
    assert by_date == {
        "2022-12-31": 30000.0,
        "2023-03-31": 20000.0,
        "2023-06-30": 25000.0,
        "2023-09-30": 25000.0,
    }
    # the synthetic Q4 is dated at FY end and known as of the 10-K filing
    q4 = next(r for r in rows if r["observation_date"] == "2023-09-30")
    assert q4["realtime_start"] == "2023-11-01"


def test_q4_not_synthesized_without_matching_ytd():
    # FY present but no 9-month YTD sharing its start -> no Q4
    entries = [
        e
        for e in _FY_ENTRIES
        if not (e["start"] == "2022-10-01" and e["end"] == "2023-06-30")
    ]
    rows = normalize_sec_observations(
        _NI_SID, {"units": {"USD": entries}}, period="quarterly"
    )
    assert "2023-09-30" not in {r["observation_date"] for r in rows}


def test_annual_mode_keeps_fy_not_q4():
    rows = normalize_sec_observations(
        _NI_SID, {"units": {"USD": _FY_ENTRIES}}, period="annual"
    )
    # annual mode keeps the FY figure (100000), no de-cumulation
    assert [r["value"] for r in rows] == [100000.0]


def test_client_period_from_env(monkeypatch):
    from fred_pipeline.sources.sec import resolve_sec_period

    monkeypatch.setenv("SEC_PERIOD", "annual")
    assert resolve_sec_period() == "annual"
    monkeypatch.setenv("SEC_PERIOD", "bogus")
    assert resolve_sec_period() == "quarterly"  # unknown falls back
    monkeypatch.delenv("SEC_PERIOD", raising=False)
    assert resolve_sec_period() == "quarterly"
