"""Historical z-score analysis views (pure Python, shared by both backends).

Two Gold tables:

  * :func:`compute_fred_series_zscore_rolling` → ``gold.fred_series_zscore_rolling``:
    Long-format (series × date × window) trailing z-score, percentile, change,
    and pct-change for every ingested FRED macro series at observation-count
    windows [12, 36, 60, 120] (≈ 1 / 3 / 5 / 10 years of monthly data).
    Only emitted once the series has accumulated *window* observations.

  * :func:`compute_zscore_heatmap` → ``gold.zscore_heatmap``:
    Wide-format (series × date) cross-sectional snapshot combining the
    expanding z-score (from ``gold.fred_feature_transforms``) with each
    rolling-window z-score and percentile rank in a single row.  Filter to
    any date in Power BI for a cross-category heat matrix; keep all dates
    for per-series multi-window fan charts.
"""

from __future__ import annotations

from bisect import bisect_right, insort
from typing import Any

# Observation-count windows for monthly FRED series: 1y / 3y / 5y / 10y.
ZSCORE_WINDOWS: tuple[int, ...] = (12, 36, 60, 120)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _group_by_series(
    rows: list[dict[str, Any]],
) -> dict[str, list[tuple[str, float]]]:
    """Return {series_id: sorted [(observation_date_str, value)]}."""
    by_series: dict[str, list[tuple[str, float]]] = {}
    for r in rows:
        sid = r.get("series_id")
        d = r.get("observation_date")
        v = r.get("value")
        if sid is None or d is None or v is None:
            continue
        by_series.setdefault(str(sid), []).append((str(d), float(v)))
    for pairs in by_series.values():
        pairs.sort(key=lambda t: t[0])
    return by_series


def _rolling_stats(
    values: list[float],
    window: int,
) -> list[tuple[float, float | None, float | None, float] | None]:
    """Per observation: (change, pct_change, zscore, percentile) for *window*.

    Returns None for the first *window* rows where the window is not yet fully
    populated.  Uses prefix sums for O(n) mean/variance; percentile is O(n×w)
    via linear scan.

    The rolling window for observation *i* covers values[i-w+1 .. i] (w values,
    including the current); the base for *change* is values[i-w] (the
    observation that just rolled off), so *change* is the w-observation delta.
    """
    n = len(values)
    s = [0.0] * (n + 1)
    s2 = [0.0] * (n + 1)
    for i, v in enumerate(values):
        s[i + 1] = s[i] + v
        s2[i + 1] = s2[i] + v * v

    out: list[tuple | None] = []
    for i in range(n):
        if i < window:
            out.append(None)
            continue
        v = values[i]
        base = values[i - window]
        mean = (s[i + 1] - s[i + 1 - window]) / window
        var = max((s2[i + 1] - s2[i + 1 - window]) / window - mean * mean, 0.0)
        std = var**0.5
        change = v - base
        pct_change = (change / base) if base != 0.0 else None
        zscore = ((v - mean) / std) if std > 1e-12 else None
        window_vals = values[i + 1 - window : i + 1]
        rank = sum(1 for x in window_vals if x <= v)
        percentile = rank / window * 100.0
        out.append((change, pct_change, zscore, percentile))
    return out


def _expanding_percentile(values: list[float]) -> list[float]:
    """Expanding percentile: rank of current value within all values up to now.

    O(n log n)-ish via a running sorted list (`bisect`/`insort`) instead of
    an O(n²) rescan-and-count per point -- at real scale (a 62K-observation
    daily series is not unusual here) the naive version costs tens of
    seconds per series; this is ~100x faster for a series that long.
    `bisect_right` after inserting `v` counts every element <= v including
    `v` itself, which is exactly `sum(1 for x in values[:i+1] if x <= v)`
    (`values[:i+1]` includes the current point).
    """
    out = []
    sorted_values: list[float] = []
    for v in values:
        insort(sorted_values, v)
        rank = bisect_right(sorted_values, v)
        out.append(rank / len(sorted_values) * 100.0)
    return out


# ---------------------------------------------------------------------------
# Gold engines
# ---------------------------------------------------------------------------


def compute_fred_series_zscore_rolling(
    feature_transform_rows: list[dict[str, Any]],
    windows: tuple[int, ...] = ZSCORE_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.fred_series_zscore_rolling``: long-format rolling z-score history.

    One row per (series_id, observation_date, window).  Rows are only emitted
    once the series accumulates at least *window* observations — no partial-window
    stats.  Windows are observation counts, so for monthly FRED series
    window=12 ≈ 1 year, window=120 ≈ 10 years.
    """
    by_series = _group_by_series(feature_transform_rows)
    out: list[dict[str, Any]] = []
    for sid in sorted(by_series):
        pairs = by_series[sid]
        values = [v for _d, v in pairs]
        for w in windows:
            stats = _rolling_stats(values, w)
            for i, stat in enumerate(stats):
                if stat is None:
                    continue
                change, pct_change, zscore, pct = stat
                out.append(
                    {
                        "series_id": sid,
                        "observation_date": pairs[i][0],
                        "window": w,
                        "value": values[i],
                        "change": change,
                        "pct_change": pct_change,
                        "zscore": zscore,
                        "percentile": pct,
                    }
                )
    return out


def compute_zscore_heatmap(
    feature_transform_rows: list[dict[str, Any]],
    windows: tuple[int, ...] = ZSCORE_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.zscore_heatmap``: wide-format z-score snapshot at every date.

    One row per (series_id, observation_date) with the expanding z-score
    (carried from ``gold.fred_feature_transforms``) and each configured
    rolling-window z-score / percentile rank as fixed columns.  NULL when
    the window is not yet fully populated.

    Power BI usage:
    - Filter to a single date → cross-series heat matrix.
    - Filter to a single series → multi-window z-score fan chart over time.
    """
    # Index the pre-computed expanding z-scores from the source table.
    exp_z: dict[tuple[str, str], float | None] = {}
    for r in feature_transform_rows:
        sid = r.get("series_id")
        d = r.get("observation_date")
        if sid and d:
            exp_z[(str(sid), str(d))] = r.get("zscore")

    by_series = _group_by_series(feature_transform_rows)
    out: list[dict[str, Any]] = []

    for sid in sorted(by_series):
        pairs = by_series[sid]
        values = [v for _d, v in pairs]

        # Expanding percentile (not stored in feature_transform_rows).
        exp_pct = _expanding_percentile(values)

        # Rolling stats per configured window.
        rolling: dict[int, list] = {w: _rolling_stats(values, w) for w in windows}

        for i, (d, v) in enumerate(pairs):
            row: dict[str, Any] = {
                "series_id": sid,
                "observation_date": d,
                "value": v,
                "zscore_expanding": exp_z.get((sid, d)),
                "percentile_expanding": exp_pct[i],
            }
            for w in windows:
                stat = rolling[w][i]
                if stat is None:
                    row[f"zscore_{w}"] = None
                    row[f"percentile_{w}"] = None
                else:
                    _, _, zscore, pct = stat
                    row[f"zscore_{w}"] = zscore
                    row[f"percentile_{w}"] = pct
            out.append(row)

    return out
