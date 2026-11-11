"""Tiingo total-return slice: the keyed client, the raw-input total-return
engine, and source isolation from Stooq's shared <ticker>:close namespace."""

from __future__ import annotations

import pytest

from fred_pipeline.equity_views import compute_equity_total_return_index
from fred_pipeline.sources.tiingo import (
    TiingoAPIError,
    TiingoClient,
    normalize_tiingo_observations,
)


# ---- client -----------------------------------------------------------------

_TIINGO_JSON = [
    {
        "date": "2024-01-02T00:00:00.000Z",
        "close": 100.0,
        "divCash": 0.0,
        "splitFactor": 1.0,
        "adjClose": 99.0,
    },
    {
        "date": "2024-01-03T00:00:00.000Z",
        "close": 101.0,
        "divCash": 0.5,
        "splitFactor": 1.0,
        "adjClose": 100.5,
    },
]


def test_tiingo_requires_key():
    with pytest.raises(TiingoAPIError):
        TiingoClient(api_key="")


def test_tiingo_normalize_explodes_fields():
    payload = {"format": "tiingo", "ticker": "AAPL", "data": _TIINGO_JSON}
    rows = normalize_tiingo_observations("AAPL", payload)
    by_sid: dict[str, list] = {}
    for r in rows:
        by_sid.setdefault(r["series_id"], []).append(r)
    assert set(by_sid) == {
        "AAPL:close",
        "AAPL:divCash",
        "AAPL:splitFactor",
        "AAPL:adjClose",
    }
    assert [r["value"] for r in by_sid["AAPL:close"]] == pytest.approx([100.0, 101.0])
    assert by_sid["AAPL:divCash"][1]["value"] == pytest.approx(0.5)
    assert all(r["source"] == "tiingo" for r in rows)
    # a record missing a field simply omits that series' row (not "missing")
    partial = {
        "format": "tiingo",
        "ticker": "X",
        "data": [{"date": "2024-01-02", "close": 5.0}],
    }
    prows = normalize_tiingo_observations("X", partial)
    assert {r["series_id"] for r in prows} == {"X:close"}


def test_tiingo_get_observations_calls_endpoint_and_token():
    class FakeResp:
        status_code = 200

        def json(self):
            return _TIINGO_JSON

    class FakeSession:
        def __init__(self):
            self.last = None

        def get(self, url, params=None, timeout=None, headers=None):
            self.last = (url, params)
            return FakeResp()

    sess = FakeSession()
    client = TiingoClient(api_key="tok", session=sess, sleep=lambda _s: None)
    payload = client.get_observations("AAPL", observation_start="2020-01-01")
    url, params = sess.last
    assert url.endswith("/tiingo/daily/aapl/prices")
    assert params["token"] == "tok" and params["startDate"] == "2020-01-01"
    assert payload["ticker"] == "AAPL" and len(payload["data"]) == 2


def test_tiingo_get_observations_defaults_to_full_history_without_start():
    """Tiingo's API (unlike FRED's) returns only the latest day when
    startDate is omitted, so a full load (observation_start=None) must still
    send an explicit, far-past startDate rather than no date param at all."""

    class FakeResp:
        status_code = 200

        def json(self):
            return _TIINGO_JSON

    class FakeSession:
        def __init__(self):
            self.last = None

        def get(self, url, params=None, timeout=None, headers=None):
            self.last = (url, params)
            return FakeResp()

    sess = FakeSession()
    client = TiingoClient(api_key="tok", session=sess, sleep=lambda _s: None)
    client.get_observations("AAPL")
    _url, params = sess.last
    assert params["startDate"] == "1900-01-01"


def test_tiingo_rotates_to_backup_key_on_quota_exhaustion():
    """tiingo_api_key may be a list of accounts to rotate through (Tiingo's
    hourly quota is tracked per key). A 429 on the active key should switch
    to the next backup and retry, rather than failing the whole call."""

    class FakeResp:
        def __init__(self, status_code, body):
            self.status_code = status_code
            self._body = body

        def json(self):
            return self._body

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, timeout=None, headers=None):
            self.calls.append(params["token"])
            if params["token"] == "primary":
                return FakeResp(429, {"detail": "hourly request allocation"})
            return FakeResp(200, _TIINGO_JSON)

    sess = FakeSession()
    client = TiingoClient(
        api_key="primary",
        backup_api_keys=["backup"],
        session=sess,
        sleep=lambda _s: None,
        max_retries=0,
    )
    payload = client.get_observations("AAPL")
    assert sess.calls == ["primary", "backup"]
    assert client.api_key == "backup"
    assert payload["ticker"] == "AAPL" and len(payload["data"]) == 2


def test_tiingo_retries_current_key_when_inflight_request_used_exhausted_key(
    fake_response_cls,
):
    class FakeSession:
        def __init__(self):
            self.tokens = []

        def get(self, url, params=None, timeout=None, headers=None):
            self.tokens.append(params["token"])
            if len(self.tokens) == 1:
                # Another request rotates keys while this primary request is
                # in flight. Its eventual 429 must not exhaust the active key.
                client.api_key = client._backup_keys.pop(0)
                assert client._backup_keys == []
                return fake_response_cls(
                    {"detail": "hourly request allocation"}, status_code=429
                )
            return fake_response_cls(_TIINGO_JSON)

    session = FakeSession()
    client = TiingoClient(
        api_key="primary",
        backup_api_keys=["backup"],
        session=session,
        sleep=lambda _s: None,
        max_retries=0,
    )

    payload = client.get_observations("EWG")

    assert session.tokens == ["primary", "backup"]
    assert client.api_key == "backup"
    assert client._backup_keys == []
    assert payload == {"format": "tiingo", "ticker": "EWG", "data": _TIINGO_JSON}


def test_tiingo_raises_when_all_keys_exhausted():
    class FakeResp:
        status_code = 429

        def json(self):
            return {"detail": "hourly request allocation"}

    class FakeSession:
        def get(self, url, params=None, timeout=None, headers=None):
            return FakeResp()

    client = TiingoClient(
        api_key="primary",
        backup_api_keys=["backup"],
        session=FakeSession(),
        sleep=lambda _s: None,
        max_retries=0,
    )
    with pytest.raises(TiingoAPIError):
        client.get_observations("AAPL")
    # both keys tried and exhausted; nothing left to rotate to
    assert client._backup_keys == []


# ---- total-return engine ----------------------------------------------------


def _tiingo_rows(ticker, records):
    """records: list of (date, close, div, split) -> exploded Silver rows."""
    rows = []
    for d, close, div, split in records:
        for field, val in (("close", close), ("divCash", div), ("splitFactor", split)):
            rows.append(
                {
                    "source": "tiingo",
                    "series_id": f"{ticker}:{field}",
                    "observation_date": d,
                    "value": val,
                    "is_missing": False,
                }
            )
    return rows


def test_total_return_dividend_reinvested():
    # flat price, one $2 dividend on a $100 stock -> TR beats PR by ~2%.
    rows = _tiingo_rows(
        "AAPL",
        [
            ("2024-01-02", 100.0, 0.0, 1.0),
            ("2024-01-03", 100.0, 0.0, 1.0),
            ("2024-01-04", 100.0, 2.0, 1.0),  # ex-div $2
            ("2024-01-05", 100.0, 0.0, 1.0),
        ],
    )
    out = compute_equity_total_return_index(rows)
    last = out[-1]
    # price flat -> PR index stays 100
    assert last["price_return_index"] == pytest.approx(100.0)
    # TR index picks up the 2% dividend
    assert last["total_return_index"] == pytest.approx(102.0)
    div_day = out[2]
    assert div_day["total_return"] == pytest.approx(0.02)
    assert div_day["price_return"] == pytest.approx(0.0)
    assert div_day["dividend"] == pytest.approx(2.0)


def test_total_return_split_is_neutral():
    # 2:1 split: raw close halves, splitFactor 2 -> returns ~0 across it.
    rows = _tiingo_rows(
        "AAPL",
        [
            ("2024-01-02", 100.0, 0.0, 1.0),
            ("2024-01-03", 50.0, 0.0, 2.0),  # split day
            ("2024-01-04", 50.0, 0.0, 1.0),
        ],
    )
    out = compute_equity_total_return_index(rows)
    split_day = out[1]
    assert split_day["price_return"] == pytest.approx(0.0, abs=1e-9)
    assert split_day["total_return"] == pytest.approx(0.0, abs=1e-9)
    assert out[-1]["total_return_index"] == pytest.approx(100.0)
    assert out[-1]["price_return_index"] == pytest.approx(100.0)


def test_total_return_trailing_dividend_and_yield():
    # two $1 dividends within a year on a $100 close -> ttm 2.0, yield 2%.
    rows = _tiingo_rows(
        "KO",
        [
            ("2023-03-01", 100.0, 1.0, 1.0),
            ("2023-09-01", 100.0, 1.0, 1.0),
            ("2024-02-01", 100.0, 0.0, 1.0),  # within 365d of both
        ],
    )
    out = compute_equity_total_return_index(rows)
    last = out[-1]
    assert last["trailing_12m_dividend"] == pytest.approx(2.0)
    assert last["dividend_yield_pct"] == pytest.approx(2.0)
    # a dividend older than 365d rolls off
    rows_old = _tiingo_rows(
        "KO",
        [
            ("2022-01-01", 100.0, 1.0, 1.0),
            ("2024-02-01", 100.0, 0.0, 1.0),
        ],
    )
    assert compute_equity_total_return_index(rows_old)[-1][
        "trailing_12m_dividend"
    ] == pytest.approx(0.0)


def test_total_return_first_row_and_missing():
    rows = _tiingo_rows("AAPL", [("2024-01-02", 100.0, 0.0, 1.0)])
    (r,) = compute_equity_total_return_index(rows)
    assert r["price_return"] is None and r["total_return"] is None
    assert r["total_return_index"] == pytest.approx(100.0)
    assert compute_equity_total_return_index([]) == []


# ---- source isolation (Stooq vs Tiingo share <ticker>:close) ----------------


def test_stooq_and_tiingo_close_do_not_collide(tmp_path):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "eq.db"),
    )
    silver = []
    # Stooq AAPL:close (price return) and Tiingo AAPL:close (total return) on
    # the SAME dates but DIFFERENT prices — must not overwrite each other.
    for i, (sq, tg) in enumerate([(200.0, 100.0), (202.0, 101.0)]):
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
                "row_hash": f"s{i}",
                "revision_number": 1,
                "ingested_at": "2024-02-01T00:00:00",
                "run_id": "r1",
            }
        )
        for field, val in (("close", tg), ("divCash", 0.0), ("splitFactor", 1.0)):
            silver.append(
                {
                    "source": "tiingo",
                    "series_id": f"AAPL:{field}",
                    "observation_date": f"2024-01-0{i + 2}",
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": val,
                    "raw_value": str(val),
                    "is_missing": 0,
                    "row_hash": f"t{i}{field}",
                    "revision_number": 1,
                    "ingested_at": "2024-02-01T00:00:00",
                    "run_id": "r1",
                }
            )
    wh.merge_silver(silver)
    results = wh.build_gold()
    assert results["equity_total_return_index"] == "ok"

    # price-return table prefers the available Stooq close (200/202)
    pr = wh.query("SELECT * FROM gold_equity_return_daily ORDER BY observation_date")
    assert [r["close"] for r in pr] == pytest.approx([200.0, 202.0])
    # total-return table sees ONLY the Tiingo close (100/101)
    tr = wh.query(
        "SELECT * FROM gold_equity_total_return_index ORDER BY observation_date"
    )
    assert [r["close"] for r in tr] == pytest.approx([100.0, 101.0])
    assert tr[1]["total_return"] == pytest.approx(0.01)
    wh.close()


def test_tiingo_key_gating_and_registry():
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.pipeline import SOURCE_FACTORIES, missing_source_keys

    assert "tiingo" in SOURCE_FACTORIES
    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    # tiingo active but no key -> flagged
    assert missing_source_keys(cfg, ["tiingo"]) == {"tiingo": "tiingo_api_key"}
    cfg_keyed = PipelineConfig(
        environment=Environment.DEV, fred_api_key="k", tiingo_api_key="t"
    )
    assert missing_source_keys(cfg_keyed, ["tiingo"]) == {}


def test_equity_tiingo_manifest_parses():
    from fred_pipeline.manifest import load_manifests

    by_name = {m.name: m for m in load_manifests("manifests")}
    assert "equity_tiingo" in by_name
    series = by_name["equity_tiingo"].series
    assert all(s.source == "tiingo" for s in series)
    assert any(s.series_id == "SPY" for s in series)  # bare ticker
