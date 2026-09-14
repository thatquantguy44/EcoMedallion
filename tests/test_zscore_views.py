"""Tests for fred_pipeline.zscore_views (rolling z-score + heatmap tables)."""

from __future__ import annotations

import pytest

from fred_pipeline.zscore_views import (
    ZSCORE_WINDOWS,
    _expanding_percentile,
    _rolling_stats,
    compute_fred_series_zscore_rolling,
    compute_zscore_heatmap,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ft_rows(series_id: str, values: list[float], start_year: int = 2000) -> list[dict]:
    """Build synthetic feature_transform_rows for one series."""
    rows = []
    year, month = start_year, 1
    for v in values:
        rows.append(
            {
                "series_id": series_id,
                "observation_date": f"{year:04d}-{month:02d}-01",
                "value": v,
                "mom": None,
                "diff": None,
                "yoy": None,
                "zscore": None,
            }
        )
        month += 1
        if month > 12:
            month = 1
            year += 1
    return rows


def _ft_rows_with_z(
    series_id: str, values: list[float], start_year: int = 2000
) -> list[dict]:
    """Build synthetic feature_transform_rows with pre-computed expanding z-scores."""
    rows = _ft_rows(series_id, values, start_year)
    # Compute expanding z-scores to simulate what fred_feature_transforms provides.
    mean = 0.0
    m2 = 0.0
    for n, (r, v) in enumerate(zip(rows, values), start=1):
        delta = v - mean
        mean += delta / n
        m2 += delta * (v - mean)
        std = (m2 / n) ** 0.5 if n > 1 else 0.0
        r["zscore"] = ((v - mean) / std) if std > 1e-12 else None
    return rows


# ---------------------------------------------------------------------------
# _rolling_stats
# ---------------------------------------------------------------------------


def test_rolling_stats_all_none_below_window():
    values = [1.0, 2.0, 3.0]
    stats = _rolling_stats(values, window=4)
    assert all(s is None for s in stats)


def test_rolling_stats_first_valid_at_window():
    values = list(range(15))
    window = 12
    stats = _rolling_stats(values, window)
    # First None-free entry at index window (0-indexed).
    assert all(s is None for s in stats[:window])
    assert stats[window] is not None


def test_rolling_stats_change():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    stats = _rolling_stats(values, window=3)
    # At i=3, base = values[0] = 1.0, v = 4.0 → change = 3.0
    change, _, _, _ = stats[3]
    assert change == pytest.approx(3.0)


def test_rolling_stats_pct_change():
    values = [2.0, 4.0, 6.0, 8.0]
    stats = _rolling_stats(values, window=2)
    # At i=2: base = values[0] = 2.0, v = 6.0 → pct = (6-2)/2 = 2.0
    _, pct, _, _ = stats[2]
    assert pct == pytest.approx(2.0)


def test_rolling_stats_pct_change_zero_base():
    values = [0.0, 1.0, 2.0, 3.0]
    stats = _rolling_stats(values, window=2)
    # At i=2: base = values[0] = 0.0 → pct_change is None
    _, pct, _, _ = stats[2]
    assert pct is None


def test_rolling_stats_zscore_constant():
    """Constant series → std=0 → zscore=None."""
    values = [5.0] * 20
    stats = _rolling_stats(values, window=12)
    for s in stats[12:]:
        assert s is not None
        _, _, z, _ = s
        assert z is None


def test_rolling_stats_zscore_sign():
    """Value above the window mean → positive z-score."""
    # Rising series: current value is above trailing mean.
    values = [float(i) for i in range(20)]
    stats = _rolling_stats(values, window=10)
    for s in stats[10:]:
        assert s is not None
        _, _, z, _ = s
        assert z is not None and z > 0


def test_rolling_stats_percentile_bounds():
    values = [float(i) for i in range(30)]
    stats = _rolling_stats(values, window=10)
    for s in stats:
        if s is not None:
            _, _, _, pct = s
            assert 0.0 <= pct <= 100.0


def test_rolling_stats_percentile_max():
    """Strictly increasing series → always highest in window → percentile=100."""
    values = list(range(20))
    stats = _rolling_stats(values, window=5)
    for s in stats[5:]:
        _, _, _, pct = s
        assert pct == pytest.approx(100.0)


def test_rolling_stats_percentile_min():
    """Strictly decreasing series → always lowest in window → percentile = 1/w * 100."""
    values = list(reversed(range(20)))
    stats = _rolling_stats(values, window=5)
    for s in stats[5:]:
        _, _, _, pct = s
        # rank=1 (only the current value is ≤ itself in a decreasing series)
        assert pct == pytest.approx(1 / 5 * 100.0)


# ---------------------------------------------------------------------------
# _expanding_percentile
# ---------------------------------------------------------------------------


def test_expanding_percentile_first_is_100():
    pct = _expanding_percentile([42.0])
    assert pct[0] == pytest.approx(100.0)


def test_expanding_percentile_bounds():
    values = [3.0, 1.0, 4.0, 1.0, 5.0, 9.0]
    pct = _expanding_percentile(values)
    for p in pct:
        assert 0.0 < p <= 100.0


def test_expanding_percentile_strictly_increasing():
    """Each new value is the largest seen → percentile = 100 each time."""
    values = list(range(1, 10))
    pct = _expanding_percentile(values)
    for p in pct:
        assert p == pytest.approx(100.0)


def test_expanding_percentile_length():
    values = [1.0, 2.0, 3.0, 4.0]
    assert len(_expanding_percentile(values)) == 4


def _naive_expanding_percentile(values: list[float]) -> list[float]:
    """The original O(n^2) implementation, kept only as an equivalence
    oracle for the bisect/insort rewrite below -- never call this on real
    data, it's the exact thing that made zscore_heatmap the single slowest
    stage (46% of a full Gold rebuild) before the rewrite."""
    out = []
    for i, v in enumerate(values):
        rank = sum(1 for x in values[: i + 1] if x <= v)
        out.append(rank / (i + 1) * 100.0)
    return out


@pytest.mark.parametrize("seed", range(20))
def test_expanding_percentile_matches_naive_rescan(seed):
    """Property test: the bisect/insort rewrite must match the original
    O(n^2) rescan-and-count exactly, including on ties (repeated values),
    which is the case most likely to break a rank-based rewrite."""
    import random

    rng = random.Random(seed)
    n = rng.randint(1, 200)
    # Deliberately narrow value range to force frequent ties.
    values = [float(rng.randint(0, 10)) for _ in range(n)]
    assert _expanding_percentile(values) == _naive_expanding_percentile(values)


def test_expanding_percentile_matches_naive_rescan_all_identical():
    values = [7.0] * 50
    assert _expanding_percentile(values) == _naive_expanding_percentile(values)


# ---------------------------------------------------------------------------
# compute_fred_series_zscore_rolling
# ---------------------------------------------------------------------------


def test_rolling_empty_input():
    assert compute_fred_series_zscore_rolling([]) == []


def test_rolling_series_too_short_for_window():
    rows = _ft_rows("AAA", [1.0, 2.0, 3.0])
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    assert result == []


def test_rolling_output_schema():
    rows = _ft_rows("AAA", [float(i) for i in range(20)])
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    assert len(result) > 0
    expected_keys = {
        "series_id",
        "observation_date",
        "window",
        "value",
        "change",
        "pct_change",
        "zscore",
        "percentile",
    }
    assert set(result[0].keys()) == expected_keys


def test_rolling_window_column_values():
    rows = _ft_rows("AAA", [float(i) for i in range(30)])
    windows = (12, 24)
    result = compute_fred_series_zscore_rolling(rows, windows=windows)
    found_windows = {r["window"] for r in result}
    assert found_windows == set(windows)


def test_rolling_series_id_preserved():
    rows = _ft_rows("UNRATE", [float(i) for i in range(20)])
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    assert all(r["series_id"] == "UNRATE" for r in result)


def test_rolling_multiple_series_independent():
    rows = _ft_rows("AAA", [float(i) for i in range(20)]) + _ft_rows(
        "BBB", [float(i) * 2 for i in range(20)]
    )
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    sids = {r["series_id"] for r in result}
    assert sids == {"AAA", "BBB"}
    aaa_rows = [r for r in result if r["series_id"] == "AAA"]
    bbb_rows = [r for r in result if r["series_id"] == "BBB"]
    assert len(aaa_rows) == len(bbb_rows)


def test_rolling_observation_dates_are_strings():
    rows = _ft_rows("X", [float(i) for i in range(20)])
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    for r in result:
        assert isinstance(r["observation_date"], str)


def test_rolling_sorted_by_series_then_date():
    rows = _ft_rows("ZZZ", [float(i) for i in range(20)]) + _ft_rows(
        "AAA", [float(i) for i in range(20)]
    )
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    keys = [(r["series_id"], r["observation_date"]) for r in result]
    assert keys == sorted(keys)


def test_rolling_value_matches_source():
    values = [float(i + 1) for i in range(20)]
    rows = _ft_rows("X", values)
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    # Value at each result row should match the source values.
    for r in result:
        obs = r["observation_date"]
        expected = next(
            float(ro["value"]) for ro in rows if ro["observation_date"] == obs
        )
        assert r["value"] == pytest.approx(expected)


def test_rolling_percentile_in_bounds():
    rows = _ft_rows("X", [float(i) for i in range(40)])
    result = compute_fred_series_zscore_rolling(rows, windows=(12,))
    for r in result:
        assert 0.0 <= r["percentile"] <= 100.0


def test_rolling_default_windows():
    """Default windows match the module constant."""
    rows = _ft_rows("X", [float(i) for i in range(130)])
    result = compute_fred_series_zscore_rolling(rows)
    found = {r["window"] for r in result}
    assert found == set(ZSCORE_WINDOWS)


def test_rolling_zscore_manual_verification():
    """Manually verify zscore against statistics.stdev for a small window."""
    values = [1.0, 3.0, 2.0, 5.0, 4.0, 6.0, 7.0, 3.0, 5.0, 8.0, 2.0, 4.0, 6.0]
    rows = _ft_rows("X", values)
    result = compute_fred_series_zscore_rolling(rows, windows=(5,))
    # Find the last row (i=12, window=5 → window values [6,7,3,5,8] wait no...)
    # For i=12 (0-indexed), window=5: values[8:13] = [5,8,2,4,6]
    last = [r for r in result if r["window"] == 5][-1]
    w_vals = values[8:13]  # indices 8..12 = [5,8,2,4,6]
    mean = sum(w_vals) / 5
    # Population variance (matches the prefix-sum formula).
    var = sum((x - mean) ** 2 for x in w_vals) / 5
    expected_z = (values[12] - mean) / var**0.5 if var > 0 else None
    assert last["zscore"] == pytest.approx(expected_z, abs=1e-9)


# ---------------------------------------------------------------------------
# compute_zscore_heatmap
# ---------------------------------------------------------------------------


def test_heatmap_empty_input():
    assert compute_zscore_heatmap([]) == []


def test_heatmap_output_schema_default_windows():
    rows = _ft_rows_with_z("AAA", [float(i) for i in range(20)])
    result = compute_zscore_heatmap(rows)
    assert len(result) > 0
    r = result[0]
    base_keys = {
        "series_id",
        "observation_date",
        "value",
        "zscore_expanding",
        "percentile_expanding",
    }
    window_keys = {f"zscore_{w}" for w in ZSCORE_WINDOWS} | {
        f"percentile_{w}" for w in ZSCORE_WINDOWS
    }
    assert base_keys | window_keys == set(r.keys())


def test_heatmap_one_row_per_series_per_date():
    rows = _ft_rows_with_z("AAA", [float(i) for i in range(20)])
    result = compute_zscore_heatmap(rows, windows=(12,))
    # Should have 20 rows (one per observation date).
    assert len(result) == 20


def test_heatmap_early_rolling_columns_are_none():
    rows = _ft_rows_with_z("AAA", [float(i) for i in range(15)])
    result = compute_zscore_heatmap(rows, windows=(12,))
    early = result[:12]
    for r in early:
        assert r["zscore_12"] is None
        assert r["percentile_12"] is None


def test_heatmap_late_rolling_columns_populated():
    rows = _ft_rows_with_z("AAA", [float(i) for i in range(20)])
    result = compute_zscore_heatmap(rows, windows=(12,))
    late = result[12:]
    for r in late:
        # Strictly rising series → z-score is well-defined (std > 0 after first window).
        assert r["zscore_12"] is not None
        assert r["percentile_12"] is not None


def test_heatmap_expanding_zscore_from_source():
    """zscore_expanding should match the pre-computed value in source rows."""
    values = [float(i) for i in range(20)]
    rows = _ft_rows_with_z("AAA", values)
    result = compute_zscore_heatmap(rows, windows=(12,))
    for r_out, r_in in zip(result, rows):
        stored = r_in["zscore"]
        if stored is not None:
            assert r_out["zscore_expanding"] == pytest.approx(stored, abs=1e-9)


def test_heatmap_expanding_percentile_bounds():
    rows = _ft_rows_with_z("AAA", [float(i) for i in range(20)])
    result = compute_zscore_heatmap(rows, windows=(12,))
    for r in result:
        assert 0.0 < r["percentile_expanding"] <= 100.0


def test_heatmap_rolling_percentile_bounds():
    rows = _ft_rows_with_z("AAA", [float(i) for i in range(30)])
    result = compute_zscore_heatmap(rows, windows=(12,))
    for r in result:
        pct = r["percentile_12"]
        if pct is not None:
            assert 0.0 <= pct <= 100.0


def test_heatmap_multiple_series():
    rows = _ft_rows_with_z("SERIES_A", [float(i) for i in range(15)]) + _ft_rows_with_z(
        "SERIES_B", [float(i) * 2 for i in range(15)]
    )
    result = compute_zscore_heatmap(rows, windows=(12,))
    assert len(result) == 30
    sids = {r["series_id"] for r in result}
    assert sids == {"SERIES_A", "SERIES_B"}


def test_heatmap_sorted_by_series_then_date():
    rows = _ft_rows_with_z("ZZZ", [float(i) for i in range(15)]) + _ft_rows_with_z(
        "AAA", [float(i) for i in range(15)]
    )
    result = compute_zscore_heatmap(rows, windows=(12,))
    keys = [(r["series_id"], r["observation_date"]) for r in result]
    assert keys == sorted(keys)


def test_heatmap_value_matches_source():
    values = [float(i + 1) for i in range(15)]
    rows = _ft_rows_with_z("X", values)
    result = compute_zscore_heatmap(rows, windows=(12,))
    for r_out, v in zip(result, values):
        assert r_out["value"] == pytest.approx(v)


def test_heatmap_custom_windows():
    rows = _ft_rows_with_z("X", [float(i) for i in range(50)])
    result = compute_zscore_heatmap(rows, windows=(6, 24))
    r = result[0]
    assert "zscore_6" in r
    assert "zscore_24" in r
    assert "percentile_6" in r
    assert "percentile_24" in r
    # Default windows should NOT be present when custom windows specified.
    assert "zscore_12" not in r
    assert "zscore_120" not in r
