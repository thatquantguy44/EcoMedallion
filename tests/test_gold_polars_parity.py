"""Parity checks: polars Gold-layer helpers must match the pure-Python spec.

fred_pipeline.transform/features are the tested reference implementations
used by both the local SQLite backend (previously) and as the documented
"pure-Python parity of Gold" spec. gold_polars re-expresses the same three
functions as vectorized polars for speed at hundreds/thousands of series.
Every case here runs both implementations over identical input and asserts
identical output, so a polars rewrite can never silently drift from spec.
"""

from __future__ import annotations

import pytest

pytest.importorskip("polars")

from fred_pipeline.features import (
    compute_curve_spreads,
    compute_feature_transforms,
    compute_revision_stats,
)
from fred_pipeline.gold_polars import (
    compute_curve_spreads_pl,
    compute_feature_transforms_pl,
    compute_revision_stats_pl,
    daily_feature_matrix_pl,
)
from fred_pipeline.transform import daily_feature_matrix


def _row(series_id, obs_date, value, is_missing=False):
    return {
        "series_id": series_id,
        "observation_date": obs_date,
        "value": value,
        "is_missing": is_missing,
    }


def _sort_key(d: dict) -> tuple:
    return tuple(sorted(d.items(), key=lambda kv: kv[0]))


def _assert_same_rows(actual: list[dict], expected: list[dict]) -> None:
    assert len(actual) == len(expected)
    a_sorted = sorted(
        actual,
        key=lambda r: (
            r.get("series_id", ""),
            r.get("observation_date") or r.get("as_of_date"),
        ),
    )
    e_sorted = sorted(
        expected,
        key=lambda r: (
            r.get("series_id", ""),
            r.get("observation_date") or r.get("as_of_date"),
        ),
    )
    for a, e in zip(a_sorted, e_sorted):
        assert set(a) == set(e), (a, e)
        for k in e:
            av, ev = a[k], e[k]
            if isinstance(ev, float) and isinstance(av, float):
                assert av == pytest.approx(ev, abs=1e-9), (k, a, e)
            else:
                assert av == ev, (k, a, e)


# ---- daily_feature_matrix ---------------------------------------------------


def _daily_matrix_fixture():
    return [
        _row("A", "2024-01-01", 1.0),
        _row("A", "2024-01-03", 2.0),
        _row("A", "2024-01-04", None, is_missing=True),  # excluded upstream
        _row("B", "2024-01-02", 10.0),
        _row("B", "2024-01-05", 12.0),
        _row("C", "2023-12-31", -5.0),  # widens the global range earlier
    ]


def test_daily_feature_matrix_parity():
    rows = _daily_matrix_fixture()
    _assert_same_rows(daily_feature_matrix_pl(rows), daily_feature_matrix(rows))


def test_daily_feature_matrix_parity_empty():
    assert daily_feature_matrix_pl([]) == daily_feature_matrix([]) == []
    all_missing = [_row("A", "2024-01-01", 1.0, is_missing=True)]
    assert (
        daily_feature_matrix_pl(all_missing) == daily_feature_matrix(all_missing) == []
    )


def test_daily_feature_matrix_parity_single_series_single_point():
    rows = [_row("A", "2024-06-15", 42.0)]
    _assert_same_rows(daily_feature_matrix_pl(rows), daily_feature_matrix(rows))


def test_daily_feature_matrix_batch_can_use_global_max_date():
    from fred_pipeline.gold_polars import daily_feature_matrix_frame

    rows = [_row("A", "2024-01-01", 1.0), _row("A", "2024-01-03", 2.0)]
    got = daily_feature_matrix_frame(rows, max_date="2024-01-05").to_dicts()
    assert got[-1] == {
        "as_of_date": "2024-01-05",
        "series_id": "A",
        "raw_value": None,
        "value": 2.0,
    }


# ---- compute_feature_transforms --------------------------------------------


def _transforms_fixture():
    rows = []
    # Regular monthly series over 3 years to exercise mom/diff/yoy/zscore,
    # including a point exactly at the 40-day YoY tolerance boundary.
    values = [
        100,
        102,
        101,
        105,
        103,
        110,
        108,
        112,
        115,
        111,
        109,
        120,
        121,
        119,
        125,
        123,
        130,
        128,
        132,
        135,
        131,
        129,
        140,
        141,
    ]
    dates = [
        "2022-01-15",
        "2022-02-15",
        "2022-03-15",
        "2022-04-15",
        "2022-05-15",
        "2022-06-15",
        "2022-07-15",
        "2022-08-15",
        "2022-09-15",
        "2022-10-15",
        "2022-11-15",
        "2022-12-15",
        "2023-01-15",
        "2023-02-15",
        "2023-03-15",
        "2023-04-15",
        "2023-05-15",
        "2023-06-15",
        "2023-07-15",
        "2023-08-15",
        "2023-09-15",
        "2023-10-15",
        "2023-11-15",
        "2023-12-15",
    ]
    for d, v in zip(dates, values):
        rows.append(_row("MONTHLY", d, float(v)))

    # A series whose next "year ago" point is just past the 40-day tolerance
    # (one row per (series_id, observation_date), as latest_by_observation
    # guarantees for real input).
    rows += [
        _row("EDGE_OUT", "2022-01-01", 50.0),
        _row("EDGE_OUT", "2023-03-01", 55.0),  # ~59 days past +365d -> null yoy
    ]
    rows += [
        _row("EDGE_IN", "2022-01-01", 50.0),
        _row("EDGE_IN", "2023-02-05", 60.0),  # ~36 days -> within tolerance -> real yoy
    ]

    # Constant series: std == 0 -> zscore must be null for every row.
    rows += [_row("FLAT", d, 7.0) for d in ("2024-01-01", "2024-02-01", "2024-03-01")]

    # Single-point series: no prior value, no year-ago, std == 0.
    rows += [_row("ONE", "2024-05-01", 9.5)]

    # Missing / None values must be excluded upstream, same as the reference.
    rows += [_row("MONTHLY", "2024-01-15", None, is_missing=True)]
    return rows


def test_compute_feature_transforms_parity():
    rows = _transforms_fixture()
    _assert_same_rows(
        compute_feature_transforms_pl(rows), compute_feature_transforms(rows)
    )


def test_compute_feature_transforms_parity_empty():
    assert compute_feature_transforms_pl([]) == compute_feature_transforms([]) == []


def test_compute_feature_transforms_parity_large_magnitude_zscore():
    """The expanding z-score uses Welford's algorithm in Python (numerically
    stable) vs. E[X^2] - E[X]^2 via cumulative sums in polars (faster, more
    float-cancellation-prone). Large-magnitude, low-relative-variance series
    (e.g. GDP in billions) are exactly where that cancellation would show up
    first — assert the two stay in agreement well beyond float noise."""
    import random

    random.seed(42)
    rows = []
    val = 20000.0  # ~$20T in billions, like GDP
    for i in range(300):
        val *= 1 + random.uniform(-0.005, 0.02)
        rows.append(
            _row("GDP_LIKE", f"20{(i // 12) % 100:02d}-{(i % 12) + 1:02d}-01", val)
        )

    py = {r["observation_date"]: r["zscore"] for r in compute_feature_transforms(rows)}
    pl_ = {
        r["observation_date"]: r["zscore"] for r in compute_feature_transforms_pl(rows)
    }
    assert set(py) == set(pl_)
    for d, a in py.items():
        b = pl_[d]
        if a is None or b is None:
            assert a == b
        else:
            assert a == pytest.approx(b, abs=1e-6)


# ---- compute_curve_spreads ---------------------------------------------------


def _spreads_fixture():
    return [
        _row("DGS10", "2024-01-01", 4.0),
        _row("DGS10", "2024-01-02", 4.1),
        _row("DGS10", "2024-01-03", 4.2),  # no matching DGS2 on this date
        _row("DGS2", "2024-01-01", 4.5),
        _row("DGS2", "2024-01-02", 4.4),
        _row("DGS3MO", "2024-01-01", 5.0),
        # DGS30/DGS10 spread: no DGS30 rows at all -> spread should be absent
    ]


def test_compute_curve_spreads_parity():
    rows = _spreads_fixture()
    _assert_same_rows(compute_curve_spreads_pl(rows), compute_curve_spreads(rows))


def test_compute_curve_spreads_parity_empty():
    assert compute_curve_spreads_pl([]) == compute_curve_spreads([]) == []


def test_compute_curve_spreads_parity_ratio_op_and_zero_guard():
    from fred_pipeline.spread_config import SpreadDef

    rows = [
        _row("A", "2024-01-01", 10.0),
        _row("B", "2024-01-01", 4.0),
        _row("A", "2024-01-02", 10.0),
        _row("B", "2024-01-02", 0.0),  # zero short leg -> must be skipped by both
    ]
    defs = [SpreadDef(name="A_OVER_B", long_leg="A", short_leg="B", op="ratio")]
    _assert_same_rows(
        compute_curve_spreads_pl(rows, defs), compute_curve_spreads(rows, defs)
    )


# ---- compute_revision_stats --------------------------------------------------


def _revision_stats_fixture():
    return [
        # revised twice
        {
            "series_id": "G",
            "observation_date": "2024-01-01",
            "value": 100.0,
            "realtime_start": "2024-02-01",
            "is_missing": False,
            "revision_number": 1,
        },
        {
            "series_id": "G",
            "observation_date": "2024-01-01",
            "value": 101.5,
            "realtime_start": "2024-03-01",
            "is_missing": False,
            "revision_number": 2,
        },
        {
            "series_id": "G",
            "observation_date": "2024-01-01",
            "value": 99.0,
            "realtime_start": "2024-04-01",
            "is_missing": False,
            "revision_number": 3,
        },
        # never revised
        {
            "series_id": "G",
            "observation_date": "2024-02-01",
            "value": 102.0,
            "realtime_start": "2024-03-15",
            "is_missing": False,
            "revision_number": 1,
        },
        # non-vintage series (blank realtime_start)
        {
            "series_id": "NV",
            "observation_date": "2024-01-01",
            "value": 5.0,
            "realtime_start": "",
            "is_missing": False,
            "revision_number": 1,
        },
        # first value is exactly zero -> revision_pct must be null, not div/0
        {
            "series_id": "Z",
            "observation_date": "2024-01-01",
            "value": 0.0,
            "realtime_start": "2024-02-01",
            "is_missing": False,
            "revision_number": 1,
        },
        {
            "series_id": "Z",
            "observation_date": "2024-01-01",
            "value": 2.0,
            "realtime_start": "2024-03-01",
            "is_missing": False,
            "revision_number": 2,
        },
        # missing/None rows must be excluded
        {
            "series_id": "G",
            "observation_date": "2024-03-01",
            "value": None,
            "realtime_start": "2024-04-01",
            "is_missing": True,
            "revision_number": 1,
        },
    ]


def test_compute_revision_stats_parity():
    rows = _revision_stats_fixture()
    _assert_same_rows(compute_revision_stats_pl(rows), compute_revision_stats(rows))


def test_compute_revision_stats_parity_empty():
    assert compute_revision_stats_pl([]) == compute_revision_stats([]) == []


def test_compute_revision_stats_zero_first_value_no_div_by_zero():
    rows = [r for r in _revision_stats_fixture() if r["series_id"] == "Z"]
    py = compute_revision_stats(rows)[0]
    pl_ = compute_revision_stats_pl(rows)[0]
    assert py["revision_pct"] is None
    assert pl_["revision_pct"] is None
    assert py["revision_delta"] == pl_["revision_delta"] == 2.0
