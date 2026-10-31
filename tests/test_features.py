from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.features import (
    compute_ecb_fx_reference_rates,
    compute_curve_spreads,
    compute_feature_transforms,
    compute_revision_stats,
    point_in_time_snapshot,
)
from fred_pipeline.local_store import LocalWarehouse


def _monthly(series_id, start_year=2023):
    # 13 month-start points: value 100..112
    rows = []
    for i in range(13):
        month = (i % 12) + 1
        year = start_year + (i // 12)
        rows.append(
            {
                "series_id": series_id,
                "observation_date": f"{year}-{month:02d}-01",
                "value": 100.0 + i,
                "is_missing": False,
            }
        )
    return rows


def test_feature_transforms_mom_diff_yoy():
    out = compute_feature_transforms(_monthly("X"))
    by_date = {r["observation_date"]: r for r in out}

    # second point: 101 vs 100
    assert by_date["2023-02-01"]["diff"] == 1.0
    assert abs(by_date["2023-02-01"]["mom"] - 0.01) < 1e-9
    assert by_date["2023-01-01"]["mom"] is None  # no prior

    # YoY at 2024-01-01 (112) vs 2023-01-01 (100) = 0.12
    assert abs(by_date["2024-01-01"]["yoy"] - 0.12) < 1e-9
    # z-score is populated
    assert by_date["2024-01-01"]["zscore"] is not None


def test_curve_spreads():
    rows = [
        {
            "series_id": "DGS10",
            "observation_date": "2024-01-01",
            "value": 4.0,
            "is_missing": False,
        },
        {
            "series_id": "DGS2",
            "observation_date": "2024-01-01",
            "value": 3.0,
            "is_missing": False,
        },
        {
            "series_id": "DGS2",
            "observation_date": "2024-01-02",
            "value": 3.1,
            "is_missing": False,
        },  # no DGS10 on this date -> no spread
    ]
    out = compute_curve_spreads(rows)
    t10y2y = [r for r in out if r["spread_name"] == "T10Y2Y"]
    assert len(t10y2y) == 1
    assert t10y2y[0]["observation_date"] == "2024-01-01"
    assert abs(t10y2y[0]["value"] - 1.0) < 1e-9


def test_curve_spreads_ratio_op_and_zero_guard():
    from fred_pipeline.spread_config import SpreadDef

    rows = [
        {
            "series_id": "A",
            "observation_date": "2024-01-01",
            "value": 10.0,
            "is_missing": False,
        },
        {
            "series_id": "B",
            "observation_date": "2024-01-01",
            "value": 4.0,
            "is_missing": False,
        },
        {
            "series_id": "A",
            "observation_date": "2024-01-02",
            "value": 10.0,
            "is_missing": False,
        },
        {
            "series_id": "B",
            "observation_date": "2024-01-02",
            "value": 0.0,
            "is_missing": False,
        },
    ]
    defs = [SpreadDef(name="A_OVER_B", long_leg="A", short_leg="B", op="ratio")]
    out = compute_curve_spreads(rows, defs)
    # zero short leg on 01-02 must be skipped entirely, not emitted as null/inf
    assert len(out) == 1
    assert out[0]["observation_date"] == "2024-01-01"
    assert abs(out[0]["value"] - 2.5) < 1e-9


def _vintage_silver():
    return [
        {
            "series_id": "G",
            "observation_date": "2024-01-01",
            "value": 100.0,
            "realtime_start": "2024-02-01",
            "realtime_end": "9999-12-31",
            "is_missing": False,
            "revision_number": 1,
        },
        {
            "series_id": "G",
            "observation_date": "2024-01-01",
            "value": 101.5,
            "realtime_start": "2024-03-01",
            "realtime_end": "9999-12-31",
            "is_missing": False,
            "revision_number": 2,
        },
        {
            "series_id": "G",
            "observation_date": "2024-02-01",
            "value": 102.0,
            "realtime_start": "2024-03-15",
            "realtime_end": "9999-12-31",
            "is_missing": False,
            "revision_number": 1,
        },
    ]


def test_revision_stats():
    out = compute_revision_stats(_vintage_silver())
    by_date = {r["observation_date"]: r for r in out}

    revised = by_date["2024-01-01"]
    assert revised["revision_count"] == 2
    assert revised["first_value"] == 100.0
    assert revised["first_realtime_start"] == "2024-02-01"
    assert revised["latest_value"] == 101.5
    assert revised["latest_realtime_start"] == "2024-03-01"
    assert abs(revised["revision_delta"] - 1.5) < 1e-9
    assert abs(revised["revision_pct"] - 0.015) < 1e-9

    unrevised = by_date["2024-02-01"]
    assert unrevised["revision_count"] == 1
    assert unrevised["first_value"] == unrevised["latest_value"] == 102.0
    assert unrevised["revision_delta"] == 0.0


def test_revision_stats_non_vintage_series_always_one_revision():
    # Non-vintage series: blank realtime_start, revision_number stamped 1.
    rows = [
        {
            "series_id": "NV",
            "observation_date": "2024-01-01",
            "value": 5.0,
            "realtime_start": "",
            "realtime_end": "",
            "is_missing": False,
            "revision_number": 1,
        },
    ]
    out = compute_revision_stats(rows)
    assert len(out) == 1
    assert out[0]["revision_count"] == 1
    assert out[0]["revision_delta"] == 0.0


def test_point_in_time_snapshot_respects_vintage():
    # As of mid-Feb, only the first release is known.
    early = point_in_time_snapshot(_vintage_silver(), "2024-02-15")
    assert early == [
        {
            "as_of_date": "2024-02-15",
            "series_id": "G",
            "observation_date": "2024-01-01",
            "value": 100.0,
        }
    ]

    # By late March, the newer observation (Feb) is known and wins.
    late = point_in_time_snapshot(_vintage_silver(), "2024-03-20")
    assert late[0]["observation_date"] == "2024-02-01"
    assert late[0]["value"] == 102.0


def test_ecb_fx_rates_use_exact_same_day_native_inputs_without_forward_fill():
    rows = [
        {
            "source": "ecb",
            "series_id": "ECB:EXR:D.USD.EUR.SP00.A",
            "observation_date": "2024-01-02",
            "exact_value": "1.250000000000000000",
            "base_currency": "EUR",
            "quote_currency": "USD",
            "is_missing": False,
            "is_deleted": False,
            "provider_valid_from": "2024-01-02T16:00:00+01:00",
            "observation_event_id": "usd-event",
        },
        {
            "source": "ecb",
            "series_id": "ECB:EXR:D.JPY.EUR.SP00.A",
            "observation_date": "2024-01-02",
            "exact_value": "150.000000000000000000",
            "base_currency": "EUR",
            "quote_currency": "JPY",
            "is_missing": False,
            "is_deleted": False,
            "provider_valid_from": "2024-01-02T16:00:00+01:00",
            "observation_event_id": "jpy-event",
        },
        {
            "source": "ecb",
            "series_id": "ECB:EXR:D.GBP.EUR.SP00.A",
            "observation_date": "2024-01-03",
            "exact_value": "0.800000000000000000",
            "base_currency": "EUR",
            "quote_currency": "GBP",
            "is_missing": False,
            "is_deleted": False,
            "provider_valid_from": "2024-01-03T16:00:00+01:00",
            "observation_event_id": "gbp-event",
        },
    ]
    output = compute_ecb_fx_reference_rates(rows)
    jpy_per_usd = next(
        row
        for row in output
        if row["observation_date"] == "2024-01-02"
        and row["base_currency"] == "USD"
        and row["quote_currency"] == "JPY"
    )
    assert jpy_per_usd["rate_decimal"] == "120"
    assert jpy_per_usd["rounded_rate_decimal"] == "120.000000000000"
    assert jpy_per_usd["input_event_ids"] == '["jpy-event","usd-event"]'
    assert not any(
        row["observation_date"] == "2024-01-03"
        and {row["base_currency"], row["quote_currency"]} == {"GBP", "USD"}
        for row in output
    )


def test_local_backend_builds_feature_tables(tmp_path):
    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    rows = [
        {
            "series_id": "DGS10",
            "observation_date": "2024-01-01",
            "realtime_start": "",
            "realtime_end": "",
            "value": 4.0,
            "raw_value": "4.0",
            "is_missing": False,
            "row_hash": "h1",
            "revision_number": 1,
            "ingested_at": "2024-01-02",
            "run_id": "r",
        },
        {
            "series_id": "DGS2",
            "observation_date": "2024-01-01",
            "realtime_start": "",
            "realtime_end": "",
            "value": 3.0,
            "raw_value": "3.0",
            "is_missing": False,
            "row_hash": "h2",
            "revision_number": 1,
            "ingested_at": "2024-01-02",
            "run_id": "r",
        },
    ]
    wh.merge_silver(rows)
    wh.build_gold()

    spreads = wh.query(
        "SELECT * FROM gold_fred_curve_spread WHERE spread_name='T10Y2Y'"
    )
    assert len(spreads) == 1 and abs(spreads[0]["value"] - 1.0) < 1e-9
    transforms = wh.query("SELECT * FROM gold_fred_feature_transforms")
    assert len(transforms) == 2
    revisions = wh.query("SELECT * FROM gold_fred_revision_stats ORDER BY series_id")
    assert len(revisions) == 2
    assert {r["series_id"] for r in revisions} == {"DGS10", "DGS2"}
    assert all(r["revision_count"] == 1 for r in revisions)  # single vintage each
    snap = wh.point_in_time_features("2024-06-01")
    assert {r["series_id"] for r in snap} == {"DGS10", "DGS2"}
    wh.close()
