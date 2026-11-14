"""Equity slice: Stooq CSV client, iShares holdings ingester, the symbol-
universe generator, and the equity Gold engines."""

from __future__ import annotations

import pytest

from fred_pipeline.sources.ishares import (
    ISharesClient,
    build_equity_manifest,
    normalize_ishares_holdings,
)
from fred_pipeline.sources.stooq import (
    STOOQ_KEY_HELP,
    StooqAPIError,
    _parse_series_id,
    _stooq_symbol,
    normalize_stooq_observations,
)
from fred_pipeline.equity_views import (
    compute_equal_weight_participation,
    compute_equity_price_reconciliation,
    compute_equity_return_daily,
    compute_index_constituents,
    compute_realized_volatility,
    select_canonical_equity_price_rows,
)


def test_equal_weight_participation_uses_matched_total_return_sessions():
    rows = [
        {"ticker": "RSP", "observation_date": "2024-01-02", "total_return_index": 100.0},
        {"ticker": "SPY", "observation_date": "2024-01-02", "total_return_index": 100.0},
        {"ticker": "RSP", "observation_date": "2024-01-03", "total_return_index": 103.0},
        # SPY's missing January 3 session must not enter either return window.
        {"ticker": "RSP", "observation_date": "2024-01-04", "total_return_index": 106.0},
        {"ticker": "SPY", "observation_date": "2024-01-04", "total_return_index": 102.0},
        {"ticker": "RSP", "observation_date": "2024-01-05", "total_return_index": 110.0},
        {"ticker": "SPY", "observation_date": "2024-01-05", "total_return_index": 104.0},
    ]
    out = compute_equal_weight_participation(rows, window_sessions=2)
    assert len(out) == 1
    assert out[0]["observation_date"] == "2024-01-05"
    assert out[0]["rsp_total_return_pct"] == pytest.approx(10.0)
    assert out[0]["spy_total_return_pct"] == pytest.approx(4.0)
    assert out[0]["excess_return_pp"] == pytest.approx(6.0)
from fred_pipeline.equity_reconciliation_config import (
    EquityReconciliationConfig,
    EquityReconciliationConfigError,
    load_equity_reconciliation_config,
)


# ---- Stooq ------------------------------------------------------------------


def test_stooq_series_id_parsing_and_symbol():
    assert _parse_series_id("AAPL") == ("AAPL", "close")
    assert _parse_series_id("aapl:volume") == ("AAPL", "volume")
    assert _stooq_symbol("AAPL") == "aapl.us"
    assert _stooq_symbol("CBA.au") == "cba.au"  # already-suffixed left alone
    with pytest.raises(StooqAPIError):
        _parse_series_id(":close")
    with pytest.raises(StooqAPIError):
        _parse_series_id("AAPL:adjclose")  # unsupported field


_STOOQ_CSV = (
    "Date,Open,High,Low,Close,Volume\n"
    "2024-01-02,185.0,186.0,184.0,185.64,50000\n"
    "2024-01-03,184.0,185.5,183.0,184.25,60000\n"
    "2024-01-04,184.5,188.0,184.2,187.10,72000\n"
)


def test_stooq_normalize_selects_field():
    payload = {"format": "csv", "field": "close", "text": _STOOQ_CSV}
    rows = normalize_stooq_observations("AAPL:close", payload)
    assert [r["observation_date"] for r in rows] == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]
    assert [r["value"] for r in rows] == pytest.approx([185.64, 184.25, 187.10])
    assert all(r["source"] == "stooq" and r["realtime_start"] == "" for r in rows)
    # volume field selects the Volume column
    vol = normalize_stooq_observations(
        "AAPL:volume", {"format": "csv", "field": "volume", "text": _STOOQ_CSV}
    )
    assert [r["value"] for r in vol] == pytest.approx([50000, 60000, 72000])


def test_stooq_normalize_tolerates_bad_body():
    for bad in ("N/D", "", "No data\n", "garbage,header\n1,2\n"):
        assert (
            normalize_stooq_observations(
                "AAPL:close", {"format": "csv", "field": "close", "text": bad}
            )
            == []
        )
    assert normalize_stooq_observations("AAPL:close", None) == []


def test_stooq_get_observations_uses_text_transport():
    class FakeResp:
        status_code = 200
        text = _STOOQ_CSV

        def json(self):  # must NOT be called for CSV
            raise AssertionError("json() called on a CSV response")

    class FakeSession:
        def __init__(self):
            self.last = None

        def get(self, url, params=None, timeout=None, headers=None):
            self.last = (url, params)
            return FakeResp()

    from fred_pipeline.sources.stooq import StooqClient

    sess = FakeSession()
    client = StooqClient(session=sess, sleep=lambda _s: None)
    payload = client.get_observations("AAPL:close", observation_start="2024-01-01")
    assert payload["field"] == "close" and "185.64" in payload["text"]
    url, params = sess.last
    assert params["s"] == "aapl.us" and params["d1"] == "20240101"
    rows = client.normalize("AAPL:close", payload)
    assert len(rows) == 3
    assert url.endswith("/q/d/l/")


def test_stooq_get_observations_includes_api_key():
    class FakeResp:
        status_code = 200
        text = _STOOQ_CSV

        def json(self):
            raise AssertionError("json() called on a CSV response")

    class FakeSession:
        def __init__(self):
            self.last = None

        def get(self, url, params=None, timeout=None, headers=None):
            self.last = {"url": url, "params": params, "headers": headers}
            return FakeResp()

    from fred_pipeline.sources.stooq import StooqClient

    sess = FakeSession()
    client = StooqClient(api_key="stooq-key", session=sess, sleep=lambda _s: None)
    client.get_observations("SPY:close")
    assert sess.last["params"]["apikey"] == "stooq-key"
    assert "Mozilla/5.0" in sess.last["headers"]["User-Agent"]


def test_stooq_get_observations_raises_on_key_required_page(fake_response_cls):
    class FakeSession:
        def get(self, url, params=None, timeout=None, headers=None):
            return fake_response_cls(
                None,
                text="<html><body>Get your apikey: open Stooq</body></html>",
            )

    from fred_pipeline.sources.stooq import StooqClient

    client = StooqClient(session=FakeSession(), sleep=lambda _s: None)
    with pytest.raises(StooqAPIError, match=STOOQ_KEY_HELP):
        client.get_observations("SPY:close")


# ---- iShares holdings -------------------------------------------------------

_IVV_CSV = (
    "iShares Core S&P 500 ETF\n"
    '"Fund Holdings as of","Jul 31, 2024"\n'
    '"Inception Date","May 15, 2000"\n'
    "\n"
    "Ticker,Name,Sector,Asset Class,Weight (%),Shares\n"
    "AAPL,APPLE INC,Information Technology,Equity,6.95,1000000\n"
    "MSFT,MICROSOFT CORP,Information Technology,Equity,6.55,500000\n"
    "NVDA,NVIDIA CORP,Information Technology,Equity,5.10,300000\n"
    "USD,US DOLLAR,Cash and/or Derivatives,Cash,0.05,0\n"
)


_IVV_XML = """<?xml version="1.0"?>
<ss:Workbook xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
<ss:Worksheet ss:Name="Disclaimers">
<ss:Table>
<ss:Row><ss:Cell><ss:Data ss:Type="String">AT&T disclaimer text</ss:Data></ss:Cell></ss:Row>
</ss:Table>
</ss:Worksheet>
<ss:Worksheet ss:Name="Holdings">
<ss:Table>
<ss:Row><ss:Cell><ss:Data ss:Type="String">Fund Holdings as of</ss:Data></ss:Cell><ss:Cell><ss:Data ss:Type="String">Jul 16, 2026</ss:Data></ss:Cell></ss:Row>
<ss:Row><ss:Cell><ss:Data ss:Type="String"></ss:Data></ss:Cell></ss:Row>
<ss:Row>
<ss:Cell><ss:Data ss:Type="String">Ticker</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Name</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Sector</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Asset Class</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Market Value</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Weight (%)</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Notional Value</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Quantity</ss:Data></ss:Cell>
</ss:Row>
<ss:Row>
<ss:Cell><ss:Data ss:Type="String">NVDA</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">NVIDIA CORP</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Information Technology</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Equity</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">68846652955</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">7.7417</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">68846652955</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">382000000</ss:Data></ss:Cell>
</ss:Row>
<ss:Row>
<ss:Cell><ss:Data ss:Type="String">USD</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">US DOLLAR</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Cash</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Cash</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">1000</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">0.01</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">1000</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">1000</ss:Data></ss:Cell>
</ss:Row>
</ss:Table>
</ss:Worksheet>
</ss:Workbook>
"""


def test_ishares_normalize_explodes_constituents():
    payload = {"format": "csv", "etf": "IVV", "text": _IVV_CSV}
    rows = normalize_ishares_holdings("IVV", payload)
    assert [r["series_id"] for r in rows] == [
        "IVV:AAPL",
        "IVV:MSFT",
        "IVV:NVDA",
    ]  # USD cash row dropped
    assert all(r["observation_date"] == "2024-07-31" for r in rows)
    assert rows[0]["value"] == pytest.approx(6.95)
    assert "shares" not in rows[0]
    assert rows[0]["source"] == "ishares"


def test_ishares_normalize_explodes_xml_workbook():
    payload = {"format": "xml", "etf": "IVV", "text": _IVV_XML}
    rows = normalize_ishares_holdings("IVV", payload)
    assert [r["series_id"] for r in rows] == ["IVV:NVDA"]
    assert rows[0]["observation_date"] == "2026-07-16"
    assert rows[0]["value"] == pytest.approx(7.7417)
    assert "shares" not in rows[0]


@pytest.mark.parametrize(
    "format,text",
    [
        (
            "csv",
            (
                '"Fund Holdings as of","Jul 16, 2026"\n'
                "Ticker,Name,Notional Weight,Market Weight\n"
                "NVDA,NVIDIA CORP,7.74,7.75\n"
            ),
        ),
        (
            "xml",
            """<?xml version="1.0"?>
<ss:Workbook xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
<ss:Worksheet ss:Name="Holdings"><ss:Table>
<ss:Row>
<ss:Cell><ss:Data ss:Type="String">Fund Holdings as of</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Jul 16, 2026</ss:Data></ss:Cell>
</ss:Row>
<ss:Row>
<ss:Cell><ss:Data ss:Type="String">Ticker</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Name</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Notional Weight</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">Market Weight</ss:Data></ss:Cell>
</ss:Row>
<ss:Row>
<ss:Cell><ss:Data ss:Type="String">NVDA</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="String">NVIDIA CORP</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">7.74</ss:Data></ss:Cell>
<ss:Cell><ss:Data ss:Type="Number">7.75</ss:Data></ss:Cell>
</ss:Row>
</ss:Table></ss:Worksheet>
</ss:Workbook>
""",
        ),
    ],
    ids=["csv", "xml"],
)
def test_ishares_normalize_prefers_market_weight_to_notional_weight(format, text):
    rows = normalize_ishares_holdings(
        "IVV", {"format": format, "etf": "IVV", "text": text}
    )
    assert [r["series_id"] for r in rows] == ["IVV:NVDA"]
    assert rows[0]["observation_date"] == "2026-07-16"
    assert rows[0]["value"] == pytest.approx(7.75)
    assert rows[0]["raw_value"] == "7.75"


def test_ishares_normalize_requires_asof_and_header():
    # no "as of" line -> skip (won't guess a date)
    no_date = _IVV_CSV.replace('"Fund Holdings as of","Jul 31, 2024"\n', "")
    assert (
        normalize_ishares_holdings(
            "IVV", {"format": "csv", "etf": "IVV", "text": no_date}
        )
        == []
    )
    # no header row -> skip
    assert (
        normalize_ishares_holdings(
            "IVV",
            {
                "format": "csv",
                "etf": "IVV",
                "text": '"Fund Holdings as of","Jul 31, 2024"\nfoo,bar\n1,2\n',
            },
        )
        == []
    )


def test_ishares_get_observations_uses_absolute_url(fake_response_cls):
    class FakeSession:
        def __init__(self):
            self.last = None

        def get(self, url, params=None, timeout=None, headers=None):
            self.last = (url, params)
            return fake_response_cls(None, text=_IVV_CSV)

    url = "https://files.example.test/IVV_holdings.csv?download=1"
    sess = FakeSession()
    client = ISharesClient(
        holdings_urls={"IVV": url},
        session=sess,
        sleep=lambda _s: None,
    )
    payload = client.get_observations("IVV")
    assert payload["text"] == _IVV_CSV
    assert sess.last == (url, {})


def test_build_equity_manifest_from_holdings():
    man = build_equity_manifest(_IVV_CSV, include_etf="IVV")
    ids = [s["series_id"] for s in man["series"]]
    assert ids == ["IVV:close", "AAPL:close", "MSFT:close", "NVDA:close"]
    assert all(s["source"] == "stooq" and s["active"] is False for s in man["series"])
    assert man["name"] == "equity_stooq"


# ---- Gold engines -----------------------------------------------------------


def _close_row(ticker, date, close, is_missing=False):
    return {
        "series_id": f"{ticker}:close",
        "observation_date": date,
        "value": close,
        "is_missing": is_missing,
    }


def test_equity_return_daily_and_index():
    rows = [
        _close_row("AAPL", "2024-01-02", 100.0),
        _close_row("AAPL", "2024-01-03", 101.0),  # +1%
        _close_row("AAPL", "2024-01-04", 99.99),  # -1% -> back near 100
        _close_row("SPY", "2024-01-02", 470.0),
    ]
    out = compute_equity_return_daily(rows)
    aapl = [r for r in out if r["ticker"] == "AAPL"]
    assert aapl[0]["price_return"] is None and aapl[0]["price_return_index"] == 100.0
    assert aapl[1]["price_return"] == pytest.approx(0.01)
    assert aapl[1]["price_return_index"] == pytest.approx(101.0)
    assert aapl[2]["price_return"] == pytest.approx((99.99 - 101.0) / 101.0)
    assert aapl[2]["price_return_index"] == pytest.approx(
        100.0 * 1.01 * (99.99 / 101.0)
    )
    # SPY single obs -> one row, null return
    spy = [r for r in out if r["ticker"] == "SPY"]
    assert len(spy) == 1 and spy[0]["price_return"] is None


def test_equity_return_ignores_non_close_and_missing():
    rows = [
        _close_row("AAPL", "2024-01-02", 100.0),
        {
            "series_id": "AAPL:volume",
            "observation_date": "2024-01-02",
            "value": 5000.0,
            "is_missing": False,
        },  # not a :close series
        _close_row("AAPL", "2024-01-03", 200.0, is_missing=True),  # missing
    ]
    out = compute_equity_return_daily(rows)
    assert [r["ticker"] for r in out] == ["AAPL"]  # one valid close row
    assert out[0]["close"] == pytest.approx(100.0)


def test_select_canonical_equity_price_rows_falls_back_to_tiingo_adjclose():
    stooq_rows = [
        {
            "source": "stooq",
            "series_id": "AAPL:close",
            "observation_date": "2024-01-02",
            "value": 100.0,
            "is_missing": False,
        },
    ]
    tiingo_rows = [
        {
            "source": "tiingo",
            "series_id": "AAPL:adjClose",
            "observation_date": "2024-01-02",
            "value": 101.0,
            "is_missing": False,
        },
        {
            "source": "tiingo",
            "series_id": "MSFT:adjClose",
            "observation_date": "2024-01-02",
            "value": 200.0,
            "is_missing": False,
        },
        {
            "source": "tiingo",
            "series_id": "MSFT:close",
            "observation_date": "2024-01-02",
            "value": 201.0,
            "is_missing": False,
        },
    ]

    rows = select_canonical_equity_price_rows(stooq_rows, tiingo_rows)

    assert [(r["source"], r["series_id"], r["value"]) for r in rows] == [
        ("stooq", "AAPL:close", 100.0),
        ("tiingo", "MSFT:close", 200.0),
    ]


def _weight_row(etf, constituent, date, weight):
    return {
        "series_id": f"{etf}:{constituent}",
        "observation_date": date,
        "value": weight,
        "is_missing": False,
    }


def test_index_constituents_ranks_and_latest_flag():
    rows = [
        _weight_row("IVV", "AAPL", "2024-06-30", 6.5),
        _weight_row("IVV", "MSFT", "2024-06-30", 6.9),
        _weight_row("IVV", "AAPL", "2024-07-31", 7.0),  # newer snapshot
        _weight_row("IVV", "MSFT", "2024-07-31", 6.6),
    ]
    out = compute_index_constituents(rows)
    jul = [r for r in out if r["observation_date"] == "2024-07-31"]
    jun = [r for r in out if r["observation_date"] == "2024-06-30"]
    # ranked by weight within snapshot
    assert [(r["constituent"], r["weight_rank"]) for r in jul] == [
        ("AAPL", 1),
        ("MSFT", 2),
    ]
    assert [(r["constituent"], r["weight_rank"]) for r in jun] == [
        ("MSFT", 1),
        ("AAPL", 2),
    ]
    # only the newest snapshot is flagged latest
    assert all(r["is_latest_snapshot"] for r in jul)
    assert not any(r["is_latest_snapshot"] for r in jun)


def test_index_constituents_bounds_by_etf_set():
    # a FOO:close price series must not be mistaken for a constituent
    rows = [
        _weight_row("IVV", "AAPL", "2024-07-31", 7.0),
        {
            "series_id": "FOO:close",
            "observation_date": "2024-07-31",
            "value": 12.0,
            "is_missing": False,
        },
    ]
    out = compute_index_constituents(rows, etfs=("IVV",))
    assert [r["constituent"] for r in out] == ["AAPL"]


# ---- manifests + registry ---------------------------------------------------


def test_equity_manifests_parse_and_register():
    from fred_pipeline.manifest import load_manifests
    from fred_pipeline.pipeline import SOURCE_FACTORIES

    assert "stooq" in SOURCE_FACTORIES and "ishares" in SOURCE_FACTORIES
    mans = load_manifests("manifests")
    by_name = {m.name: m for m in mans}
    assert "equity_stooq" in by_name and "etf_holdings" in by_name
    stooq_series = by_name["equity_stooq"].series
    assert all(s.source == "stooq" for s in stooq_series)
    assert any(s.series_id == "SPY:close" for s in stooq_series)


def test_equity_local_build_gold(tmp_path):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "eq.db"),
    )
    silver = []
    for i, px in enumerate([100.0, 101.0, 99.99]):
        silver.append(
            {
                "source": "stooq",
                "series_id": "AAPL:close",
                "observation_date": f"2024-01-0{i + 2}",
                "realtime_start": "",
                "realtime_end": "",
                "value": px,
                "raw_value": str(px),
                "is_missing": 0,
                "row_hash": f"a{i}",
                "revision_number": 1,
                "ingested_at": "2024-02-01T00:00:00",
                "run_id": "r1",
            }
        )
    for tk, wt in (("AAPL", 7.0), ("MSFT", 6.6)):
        silver.append(
            {
                "source": "ishares",
                "series_id": f"IVV:{tk}",
                "observation_date": "2024-07-31",
                "realtime_start": "",
                "realtime_end": "",
                "value": wt,
                "raw_value": str(wt),
                "is_missing": 0,
                "row_hash": f"h{tk}",
                "revision_number": 1,
                "ingested_at": "2024-08-01T00:00:00",
                "run_id": "r1",
            }
        )
    wh.merge_silver(silver)
    results = wh.build_gold()
    assert results["equity_return_daily"] == "ok"
    assert results["index_constituents"] == "ok"

    ret = wh.query("SELECT * FROM gold_equity_return_daily ORDER BY observation_date")
    assert [r["ticker"] for r in ret] == ["AAPL", "AAPL", "AAPL"]
    assert ret[1]["price_return"] == pytest.approx(0.01)
    cons = wh.query(
        "SELECT * FROM gold_index_constituents "
        "WHERE is_latest_snapshot = 1 ORDER BY weight_rank"
    )
    assert [r["constituent"] for r in cons] == ["AAPL", "MSFT"]
    wh.close()


def test_equity_local_build_gold_uses_tiingo_when_stooq_absent(tmp_path):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "eq_tiingo.db"),
    )
    silver = []
    for i, (close, adj_close) in enumerate([(100.0, 100.0), (101.0, 102.0)]):
        date = f"2024-01-0{i + 2}"
        for field, value in (
            ("close", close),
            ("adjClose", adj_close),
            ("divCash", 0.0),
            ("splitFactor", 1.0),
        ):
            silver.append(
                {
                    "source": "tiingo",
                    "series_id": f"AAPL:{field}",
                    "observation_date": date,
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": value,
                    "raw_value": str(value),
                    "is_missing": 0,
                    "row_hash": f"tg-{field}-{i}",
                    "revision_number": 1,
                    "ingested_at": "2024-02-01T00:00:00",
                    "run_id": "r1",
                }
            )
    wh.merge_silver(silver)
    results = wh.build_gold()
    assert results["equity_return_daily"] == "ok"
    assert results["equity_total_return_index"] == "ok"

    ret = wh.query("SELECT * FROM gold_equity_return_daily ORDER BY observation_date")
    assert [r["ticker"] for r in ret] == ["AAPL", "AAPL"]
    assert ret[0]["close"] == pytest.approx(100.0)
    assert ret[1]["close"] == pytest.approx(102.0)
    assert ret[1]["price_return"] == pytest.approx(0.02)
    wh.close()


def test_equal_weight_participation_local_gold_build(tmp_path):
    from datetime import date, timedelta

    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "participation.db"),
    )
    sessions = [date(2024, 1, 1) + timedelta(days=i) for i in range(90)]
    sessions = [d for d in sessions if d.weekday() < 5][:64]
    silver = []
    for i, day in enumerate(sessions):
        for ticker, daily_step in (("RSP", 0.001), ("SPY", 0.0005)):
            for field, value in (("close", 100 * (1 + daily_step * i)),
                                 ("divCash", 0.0), ("splitFactor", 1.0)):
                silver.append({
                    "source": "tiingo", "series_id": f"{ticker}:{field}",
                    "observation_date": day.isoformat(), "realtime_start": "",
                    "realtime_end": "", "value": value, "raw_value": str(value),
                    "is_missing": False, "row_hash": f"{ticker}-{field}-{i}",
                    "revision_number": 1, "ingested_at": "2024-06-01T00:00:00",
                    "run_id": "r1",
                })
    wh.merge_silver(silver)
    result = wh.build_gold()
    assert result["equal_weight_participation_daily"] == "ok"
    rows = wh.query("SELECT * FROM gold_equal_weight_participation_daily")
    assert len(rows) == 1
    assert rows[0]["observation_date"] == sessions[-1].isoformat()
    assert rows[0]["window_sessions"] == 63
    assert rows[0]["excess_return_pp"] == pytest.approx(3.15)
    wh.close()


# ---- realized volatility ----------------------------------------------------


def test_realized_volatility_hand_computed_window():
    import math

    closes = [100.0, 105.0, 98.0, 102.0]
    rows = [_close_row("AAPL", f"2024-01-0{i + 2}", c) for i, c in enumerate(closes)]
    out = compute_realized_volatility(rows, windows=(3,))

    r = [math.log(closes[i + 1] / closes[i]) for i in range(3)]
    mean = sum(r) / 3
    var = sum(x * x for x in r) / 3 - mean**2
    expected_pct = (var**0.5) * math.sqrt(252) * 100.0

    assert len(out) == 1
    row = out[0]
    assert row["ticker"] == "AAPL"
    assert row["window"] == 3
    assert row["observation_date"] == "2024-01-05"
    assert row["realized_vol_pct"] == pytest.approx(expected_pct)


def test_realized_volatility_requires_full_window():
    rows = [
        _close_row("AAPL", "2024-01-02", 100.0),
        _close_row("AAPL", "2024-01-03", 101.0),  # only 1 log return
    ]
    assert compute_realized_volatility(rows, windows=(21, 63)) == []


def test_realized_volatility_isolates_tickers():
    rows = [
        _close_row("AAPL", "2024-01-02", 100.0),
        _close_row("AAPL", "2024-01-03", 105.0),
        _close_row("AAPL", "2024-01-04", 98.0),
        _close_row("SPY", "2024-01-02", 470.0),
        _close_row("SPY", "2024-01-03", 471.0),  # only 1 log return -> no window=2 row
    ]
    out = compute_realized_volatility(rows, windows=(2,))
    assert {r["ticker"] for r in out} == {"AAPL"}


def test_equity_local_build_gold_writes_realized_volatility(tmp_path):
    import datetime as dt

    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "vol.db"),
    )
    silver = []
    start = dt.date(2024, 1, 2)
    price = 100.0
    for i in range(25):
        d = start + dt.timedelta(days=i)
        price *= 1.001 if i % 2 == 0 else 0.999
        silver.append(
            {
                "source": "stooq",
                "series_id": "AAPL:close",
                "observation_date": d.isoformat(),
                "realtime_start": "",
                "realtime_end": "",
                "value": price,
                "raw_value": str(price),
                "is_missing": 0,
                "row_hash": f"v{i}",
                "revision_number": 1,
                "ingested_at": "2024-02-01T00:00:00",
                "run_id": "r1",
            }
        )
    wh.merge_silver(silver)
    results = wh.build_gold()
    assert results["realized_volatility"] == "ok"

    rows = wh.query(
        "SELECT * FROM gold_realized_volatility ORDER BY observation_date, window"
    )
    windows_seen = {r["window"] for r in rows}
    assert 21 in windows_seen  # 24 returns available -> window=21 populated
    assert 63 not in windows_seen  # not enough history for a 63-day window
    assert all(r["ticker"] == "AAPL" for r in rows)
    assert all(r["realized_vol_pct"] > 0 for r in rows)
    wh.close()


# ---- equity price reconciliation -------------------------------------------


def _stooq_close(ticker, date, close):
    return {
        "source": "stooq",
        "series_id": f"{ticker}:close",
        "observation_date": date,
        "value": close,
        "is_missing": False,
    }


def _tiingo_adj_close(ticker, date, adj_close):
    return {
        "source": "tiingo",
        "series_id": f"{ticker}:adjClose",
        "observation_date": date,
        "value": adj_close,
        "is_missing": False,
    }


def _recon_cfg(*tickers, tol=2.0):
    return EquityReconciliationConfig(tickers=tuple(tickers), tolerance_pct=tol)


def test_equity_recon_agrees():
    sq = [
        _stooq_close("AAPL", "2024-01-02", 193.0),
        _stooq_close("AAPL", "2024-01-03", 195.0),
    ]
    tg = [
        _tiingo_adj_close("AAPL", "2024-01-02", 192.5),
        _tiingo_adj_close("AAPL", "2024-01-03", 194.6),
    ]
    out = compute_equity_price_reconciliation(sq, tg, _recon_cfg("AAPL"))
    assert len(out) == 2
    r = out[0]
    assert r["ticker"] == "AAPL" and r["observation_date"] == "2024-01-02"
    assert r["stooq_close"] == pytest.approx(193.0)
    assert r["tiingo_adj_close"] == pytest.approx(192.5)
    assert r["abs_diff"] == pytest.approx(0.5)
    assert r["pct_diff"] == pytest.approx(0.5 / 192.5)
    # |pct_diff| * 100 ~= 0.26% < 2% -> not diverged
    assert not r["diverged"]
    assert not out[1]["diverged"]


def test_equity_recon_flags_divergence():
    sq = [_stooq_close("SPY", "2024-03-01", 500.0)]
    # Tiingo has a clearly wrong price on this date
    tg = [_tiingo_adj_close("SPY", "2024-03-01", 400.0)]
    out = compute_equity_price_reconciliation(sq, tg, _recon_cfg("SPY", tol=2.0))
    assert len(out) == 1
    assert out[0]["diverged"]
    assert out[0]["pct_diff"] == pytest.approx((500.0 - 400.0) / 400.0)


def test_equity_recon_skips_missing_source():
    # Tiingo has no data -> no rows
    sq = [_stooq_close("AAPL", "2024-01-02", 193.0)]
    assert compute_equity_price_reconciliation(sq, [], _recon_cfg("AAPL")) == []
    # Stooq has no data -> no rows
    tg = [_tiingo_adj_close("AAPL", "2024-01-02", 192.5)]
    assert compute_equity_price_reconciliation([], tg, _recon_cfg("AAPL")) == []


def test_equity_recon_filters_to_cfg_tickers():
    sq = [
        _stooq_close("AAPL", "2024-01-02", 193.0),
        _stooq_close("MSFT", "2024-01-02", 375.0),
    ]
    tg = [
        _tiingo_adj_close("AAPL", "2024-01-02", 192.5),
        _tiingo_adj_close("MSFT", "2024-01-02", 374.0),
    ]
    # Only AAPL in config -> MSFT rows silently excluded
    out = compute_equity_price_reconciliation(sq, tg, _recon_cfg("AAPL"))
    assert [r["ticker"] for r in out] == ["AAPL"]


def test_equity_recon_skips_is_missing():
    sq = [
        {
            "source": "stooq",
            "series_id": "AAPL:close",
            "observation_date": "2024-01-02",
            "value": 193.0,
            "is_missing": True,
        }
    ]
    tg = [_tiingo_adj_close("AAPL", "2024-01-02", 192.5)]
    assert compute_equity_price_reconciliation(sq, tg, _recon_cfg("AAPL")) == []


def test_equity_recon_config_loader_missing(tmp_path):
    cfg = load_equity_reconciliation_config(str(tmp_path / "nope.yml"))
    assert cfg.tickers == () and cfg.tolerance_pct == 2.0


def test_equity_recon_config_loader_repo_file():
    cfg = load_equity_reconciliation_config("config/equity_reconciliations.yml")
    assert "SPY" in cfg.tickers and "AAPL" in cfg.tickers
    assert cfg.tolerance_pct == pytest.approx(2.0)


def test_equity_recon_config_loader_rejects_malformed(tmp_path):
    bad = tmp_path / "bad.yml"
    # negative tolerance
    bad.write_text("equity_reconciliations:\n  tolerance_pct: -1\n  tickers: [AAPL]\n")
    with pytest.raises(EquityReconciliationConfigError):
        load_equity_reconciliation_config(str(bad))
    # unknown key
    bad.write_text("equity_reconciliations:\n  tickers: [AAPL]\n  bogus: 1\n")
    with pytest.raises(EquityReconciliationConfigError):
        load_equity_reconciliation_config(str(bad))
    # duplicate tickers
    bad.write_text("equity_reconciliations:\n  tickers: [AAPL, AAPL]\n")
    with pytest.raises(EquityReconciliationConfigError):
        load_equity_reconciliation_config(str(bad))


def test_equity_recon_end_to_end(tmp_path):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "recon.db"),
    )
    silver = []
    for i, (sq, adj) in enumerate([(193.0, 192.5), (195.0, 194.0)]):
        silver.append(
            {
                "source": "stooq",
                "series_id": "AAPL:close",
                "observation_date": f"2024-01-0{i + 2}",
                "realtime_start": "",
                "realtime_end": "",
                "value": sq,
                "raw_value": str(sq),
                "is_missing": 0,
                "row_hash": f"sq{i}",
                "revision_number": 1,
                "ingested_at": "2024-02-01T00:00:00",
                "run_id": "r1",
            }
        )
        silver.append(
            {
                "source": "tiingo",
                "series_id": "AAPL:adjClose",
                "observation_date": f"2024-01-0{i + 2}",
                "realtime_start": "",
                "realtime_end": "",
                "value": adj,
                "raw_value": str(adj),
                "is_missing": 0,
                "row_hash": f"tg{i}",
                "revision_number": 1,
                "ingested_at": "2024-02-01T00:00:00",
                "run_id": "r1",
            }
        )
    wh.merge_silver(silver)
    results = wh.build_gold()
    assert results["equity_price_reconciliation"] == "ok"

    rows = wh.query(
        "SELECT * FROM gold_equity_price_reconciliation ORDER BY observation_date"
    )
    # AAPL is in the default config
    assert len(rows) == 2
    assert rows[0]["stooq_close"] == pytest.approx(193.0)
    assert rows[0]["tiingo_adj_close"] == pytest.approx(192.5)
    assert not rows[0]["diverged"]
    wh.close()
