"""Tests for frequency-aware, N-leg cross-series Gold features."""

import textwrap

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.cross_series_config import (
    CrossSeriesConfigError,
    CrossSeriesDef,
    load_cross_series_defs,
)
from fred_pipeline.features import (
    compute_cross_series_features,
    compute_cross_series_features_pit,
)
from fred_pipeline.local_store import LocalWarehouse


def _row(series_id, date, value, missing=False):
    return {"series_id": series_id, "observation_date": date, "value": value,
            "is_missing": missing}


def _def(name, op, freq, legs):
    return CrossSeriesDef(name=name, op=op, frequency=freq, legs=tuple(legs))


# ---- config loader -------------------------------------------------------

def _write(tmp_path, text):
    p = tmp_path / "cross.yml"
    p.write_text(textwrap.dedent(text))
    return str(p)


def test_loader_parses_ops_and_weights(tmp_path):
    path = _write(tmp_path, """
        features:
          - name: ry
            op: spread
            frequency: d
            legs: [DGS10, T10YIE]
          - name: dgdp
            op: ratio
            frequency: q
            legs:
              - series_id: DEBT
              - series_id: GDP
          - name: idx
            op: composite
            frequency: m
            legs:
              - {series_id: A, weight: 0.5}
              - {series_id: B, weight: 0.25}
              - C
    """)
    defs = {d.name: d for d in load_cross_series_defs(path)}
    assert defs["ry"].legs == (("DGS10", 1.0), ("T10YIE", 1.0))
    assert defs["dgdp"].op == "ratio" and defs["dgdp"].frequency == "q"
    assert defs["idx"].legs == (("A", 0.5), ("B", 0.25), ("C", 1.0))


def test_loader_missing_file_is_empty():
    assert load_cross_series_defs("does-not-exist.yml") == []


def test_marketindicators_configured_spreads_use_active_series_ids():
    defs = {d.name: d for d in load_cross_series_defs()}
    assert defs["vix_term_inversion"].legs == (("VIXCLS", 1.0), ("VXVCLS", 1.0))
    assert defs["real_yield_10y_5y"].legs == (("DFII10", 1.0), ("DFII5", 1.0))
    rows = [_row("VIXCLS", "2024-01-02", 18.0),
            _row("VXVCLS", "2024-01-02", 16.0),
            _row("DFII10", "2024-01-02", 2.1),
            _row("DFII5", "2024-01-02", 1.8)]
    out = compute_cross_series_features(rows, list(defs.values()))
    values = {r["feature_name"]: r["value"] for r in out if r["observation_date"] == "2024-01-02"}
    assert values["vix_term_inversion"] == pytest.approx(2.0)
    assert values["real_yield_10y_5y"] == pytest.approx(0.3)


@pytest.mark.parametrize("legs, op", [(["A", "B", "C"], "spread"),
                                      (["A", "B", "C"], "ratio")])
def test_spread_ratio_require_two_legs(legs, op):
    with pytest.raises(CrossSeriesConfigError):
        _def("x", op, "d", [(s, 1.0) for s in legs])


def test_invalid_op_and_frequency():
    with pytest.raises(CrossSeriesConfigError):
        _def("x", "bogus", "d", [("A", 1.0), ("B", 1.0)])
    with pytest.raises(CrossSeriesConfigError):
        _def("x", "spread", "zzz", [("A", 1.0), ("B", 1.0)])


def test_loader_rejects_duplicate_and_unknown(tmp_path):
    dup = _write(tmp_path, """
        features:
          - {name: x, op: spread, frequency: d, legs: [A, B]}
          - {name: x, op: spread, frequency: d, legs: [A, B]}
    """)
    with pytest.raises(CrossSeriesConfigError):
        load_cross_series_defs(dup)


# ---- engine --------------------------------------------------------------

def test_same_frequency_spread():
    rows = [_row("A", "2024-01-01", 10), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-01", 3), _row("B", "2024-01-02", 4)]
    out = compute_cross_series_features(rows, [_def("s", "spread", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    assert [(r["observation_date"], r["value"]) for r in out] == [
        ("2024-01-01", 7.0), ("2024-01-02", 8.0)]
    assert all(r["op"] == "spread" and r["feature_name"] == "s" for r in out)


def test_ratio_zero_guard():
    rows = [_row("A", "2024-01-01", 10), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-01", 0), _row("B", "2024-01-02", 4)]
    out = compute_cross_series_features(rows, [_def("r", "ratio", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    # 2024-01-01 dropped (zero denominator); 2024-01-02 -> 3.0
    assert [(r["observation_date"], r["value"]) for r in out] == [("2024-01-02", 3.0)]


def test_composite_weighted_sum():
    rows = [_row("A", "2024-03-31", 100), _row("B", "2024-03-31", 200)]
    out = compute_cross_series_features(rows, [_def("c", "composite", "m",
                                                    [("A", 0.5), ("B", 0.25)])])
    assert out == [{"feature_name": "c", "op": "composite",
                    "observation_date": "2024-03-01", "value": 100.0}]  # 0.5*100+0.25*200


def test_four_quarter_change_requires_same_quarter_prior_year():
    definition = _def("credit_delta", "four_quarter_change", "q", [("DR", 1.0)])
    rows = [_row("DR", "2023-01-01", 1.2),
            _row("DR", "2023-04-01", 1.3),
            _row("DR", "2024-01-01", 1.8),
            _row("DR", "2024-07-01", 2.0)]
    out = compute_cross_series_features(rows, [definition])
    assert [(r["observation_date"], r["value"]) for r in out] == [
        ("2024-01-01", pytest.approx(0.6))]


def test_four_quarter_change_uses_known_vintages_as_of_date():
    definition = _def("credit_delta", "four_quarter_change", "q", [("DR", 1.0)])
    silver = [_srow("DR", "2023-01-01", 1.2, "2023-05-01"),
              _srow("DR", "2024-01-01", 1.8, "2024-05-01"),
              _srow("DR", "2024-01-01", 2.0, "2024-08-01")]
    assert compute_cross_series_features_pit(silver, [definition], as_of="2024-04-30") == []
    early = compute_cross_series_features_pit(silver, [definition], as_of="2024-06-01")
    late = compute_cross_series_features_pit(silver, [definition], as_of="2024-09-01")
    assert early[0]["value"] == pytest.approx(0.6)
    assert late[0]["value"] == pytest.approx(0.8)


def test_credit_deterioration_config_uses_quarterly_rate_levels():
    defs = {d.name: d for d in load_cross_series_defs()}
    assert defs["business_loan_delinquency_4q_change"].legs == (("DRBLACBN", 1.0),)
    assert defs["credit_card_delinquency_4q_change"].legs == (("DRCCLACBS", 1.0),)
    assert defs["mortgage_delinquency_4q_change"].legs == (("DRSFRMACBS", 1.0),)
    assert all(defs[name].op == "four_quarter_change" and defs[name].frequency == "q"
               for name in ("business_loan_delinquency_4q_change",
                            "credit_card_delinquency_4q_change",
                            "mortgage_delinquency_4q_change"))


def test_four_quarter_change_rejects_nonquarterly_or_multiple_legs():
    with pytest.raises(CrossSeriesConfigError):
        _def("bad", "four_quarter_change", "m", [("DR", 1.0)])
    with pytest.raises(CrossSeriesConfigError):
        _def("bad", "four_quarter_change", "q", [("A", 1.0), ("B", 1.0)])


def test_cross_frequency_asof_downsample():
    # daily A downsampled to quarterly = last obs in each quarter
    rows = [
        _row("A", "2024-01-15", 100), _row("A", "2024-02-20", 110),
        _row("A", "2024-03-31", 120),                      # Q1 last -> 120
        _row("A", "2024-04-10", 130),                      # Q2 last -> 130
        _row("B", "2024-01-01", 1000), _row("B", "2024-04-01", 1100),  # quarterly
    ]
    out = compute_cross_series_features(rows, [_def("dg", "ratio", "q",
                                                    [("A", 1.0), ("B", 1.0)])])
    by_date = {r["observation_date"]: r["value"] for r in out}
    assert by_date["2024-01-01"] == pytest.approx(120 / 1000)   # Q1: last-in-quarter
    assert by_date["2024-04-01"] == pytest.approx(130 / 1100)   # Q2


def test_missing_leg_yields_no_rows():
    rows = [_row("A", "2024-01-01", 10)]  # B absent
    out = compute_cross_series_features(rows, [_def("s", "spread", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    assert out == []


def test_missing_values_are_skipped():
    rows = [_row("A", "2024-01-01", None, missing=True), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-02", 4)]
    out = compute_cross_series_features(rows, [_def("s", "spread", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    assert [(r["observation_date"], r["value"]) for r in out] == [("2024-01-02", 8.0)]


# ---- point-in-time (realtime_start-aligned) variant ----------------------

def _srow(series_id, date, value, rt, missing=False):
    """A raw Silver-shaped row with a vintage (realtime_start)."""
    return {"series_id": series_id, "observation_date": date, "value": value,
            "realtime_start": rt, "is_missing": missing}


# A is revised (first print 10 on 2024-02-01, revised to 12 on 2024-05-01);
# B is non-vintage (blank realtime_start).
_PIT_SILVER = [
    _srow("A", "2024-01-01", 10.0, "2024-02-01"),
    _srow("A", "2024-01-01", 12.0, "2024-05-01"),
    _srow("B", "2024-01-01", 3.0, ""),
]
_PIT_DEF = [_def("s", "spread", "d", [("A", 1.0), ("B", 1.0)])]


def test_pit_first_report_uses_earliest_vintage():
    out = compute_cross_series_features_pit(_PIT_SILVER, _PIT_DEF)
    assert out == [{"feature_name": "s", "op": "spread",
                    "observation_date": "2024-01-01", "value": 7.0,  # 10 (first) - 3
                    "basis": "first_report"}]


def test_pit_differs_from_latest_revised():
    # latest-revised uses A=12 -> 9; point-in-time uses A=10 (first print) -> 7
    latest = [_row("A", "2024-01-01", 12.0), _row("B", "2024-01-01", 3.0)]
    assert compute_cross_series_features(latest, _PIT_DEF)[0]["value"] == 9.0
    assert compute_cross_series_features_pit(_PIT_SILVER, _PIT_DEF)[0]["value"] == 7.0


def test_pit_as_of_selects_vintage_known_by_date():
    # as of 2024-03-01 only the first print (10) is known -> 7
    early = compute_cross_series_features_pit(_PIT_SILVER, _PIT_DEF, as_of="2024-03-01")
    assert early[0]["value"] == 7.0 and early[0]["basis"] == "2024-03-01"
    # as of 2024-06-01 the revision (12) is known -> 9
    late = compute_cross_series_features_pit(_PIT_SILVER, _PIT_DEF, as_of="2024-06-01")
    assert late[0]["value"] == 9.0


def test_pit_as_of_before_first_publication_is_empty():
    # nothing for A was published by 2024-01-15 -> no common period
    out = compute_cross_series_features_pit(_PIT_SILVER, _PIT_DEF, as_of="2024-01-15")
    assert out == []


def test_pit_non_vintage_matches_latest():
    # both legs non-vintage (blank realtime) -> PIT == latest-revised
    silver = [_srow("A", "2024-01-01", 10.0, ""), _srow("B", "2024-01-01", 3.0, "")]
    latest = [_row("A", "2024-01-01", 10.0), _row("B", "2024-01-01", 3.0)]
    pit = compute_cross_series_features_pit(silver, _PIT_DEF)
    assert pit[0]["value"] == compute_cross_series_features(latest, _PIT_DEF)[0]["value"]


# ---- polars parity (skipped if polars absent) ----------------------------

def test_polars_parity():
    pytest.importorskip("polars")
    from fred_pipeline.gold_polars import compute_cross_series_features_pl

    rows = [_row("A", "2024-01-01", 10), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-01", 3), _row("B", "2024-01-02", 4)]
    defs = [_def("s", "spread", "d", [("A", 1.0), ("B", 1.0)])]
    assert compute_cross_series_features_pl(rows, defs) == \
        compute_cross_series_features(rows, defs)


# ---- local backend end-to-end -------------------------------------------

def test_local_build_gold_populates_cross_series(tmp_path, monkeypatch):
    cfg_path = _write(tmp_path, """
        features:
          - name: a_minus_b
            op: spread
            frequency: d
            legs: [A, B]
    """)
    monkeypatch.setenv("FRED_CROSS_SERIES_FILE", cfg_path)

    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    wh.merge_silver([
        {"source": "fred", "series_id": "A", "observation_date": "2024-01-01",
         "realtime_start": "", "realtime_end": "", "value": 10.0, "raw_value": "10",
         "is_missing": False, "row_hash": "h1", "revision_number": 1,
         "ingested_at": "t", "run_id": "r"},
        {"source": "fred", "series_id": "B", "observation_date": "2024-01-01",
         "realtime_start": "", "realtime_end": "", "value": 3.0, "raw_value": "3",
         "is_missing": False, "row_hash": "h2", "revision_number": 1,
         "ingested_at": "t", "run_id": "r"},
    ])
    wh.build_gold()
    got = wh.query("SELECT feature_name, op, observation_date, value "
                   "FROM gold_fred_cross_series_feature")
    assert got == [{"feature_name": "a_minus_b", "op": "spread",
                    "observation_date": "2024-01-01", "value": 7.0}]
    wh.close()


def test_local_build_gold_pit_vs_latest(tmp_path, monkeypatch):
    cfg_path = _write(tmp_path, """
        features:
          - name: a_minus_b
            op: spread
            frequency: d
            legs: [A, B]
    """)
    monkeypatch.setenv("FRED_CROSS_SERIES_FILE", cfg_path)

    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))

    def _sv(series_id, value, rt):
        return {"source": "fred", "series_id": series_id,
                "observation_date": "2024-01-01", "realtime_start": rt,
                "realtime_end": "", "value": value, "raw_value": str(value),
                "is_missing": False, "row_hash": f"{series_id}{rt}",
                "revision_number": 1, "ingested_at": "t", "run_id": "r"}

    # A revised (10 first, 12 later); B non-vintage (3)
    wh.merge_silver([_sv("A", 10.0, "2024-02-01"), _sv("A", 12.0, "2024-05-01"),
                     _sv("B", 3.0, "")])
    wh.build_gold()

    latest = wh.query("SELECT value FROM gold_fred_cross_series_feature")
    pit = wh.query("SELECT value, basis FROM gold_fred_cross_series_feature_pit")
    assert latest == [{"value": 9.0}]                       # latest-revised: 12-3
    assert pit == [{"value": 7.0, "basis": "first_report"}]  # first print: 10-3
    wh.close()
