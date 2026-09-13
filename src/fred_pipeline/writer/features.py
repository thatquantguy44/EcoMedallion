"""Quant feature engineering (pure Python).

Derived Gold features for research / optimizer / ML inputs, computed from the
latest-revision series and the full vintage history:

  * **transforms** — period-over-period % change (MoM-style), first difference,
    year-over-year % change (date-based), and an **expanding, point-in-time
    safe** z-score, per series;
  * **curve spreads / ratios** — cross-series features (e.g. 10Y-2Y) defined in
    a reviewable YAML config (see :mod:`fred_pipeline.spread_config`);
  * **as-of-date point-in-time snapshot** — each series' value *as it was known*
    on a given date (leakage-free feature vector for backtests).

Kept pure so the Local (SQLite) and Databricks backends share the same tested
logic; the Spark equivalents live in :mod:`fred_pipeline.gold`.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from fred_pipeline.cross_series_config import CrossSeriesDef, load_cross_series_defs
from fred_pipeline.reconciliation_config import (
    ReconciliationDef,
    load_reconciliation_defs,
)
from fred_pipeline.spread_config import SpreadDef, load_spread_defs

# How far back a "year ago" match may be before YoY is left null (daily series
# won't have an exact −365d point).
YOY_TOLERANCE_DAYS = 40


def _parse(d: Any) -> date | None:
    try:
        return date.fromisoformat(str(d)[:10])
    except (ValueError, TypeError):
        return None


def _pct_change(cur: float | None, base: float | None) -> float | None:
    if cur is None or base is None or base == 0:
        return None
    return (cur - base) / base


def _group_sorted(
    rows: Iterable[dict[str, Any]],
) -> dict[str, list[tuple[date, float]]]:
    by_series: dict[str, list[tuple[date, float]]] = {}
    for r in rows:
        if r.get("is_missing"):
            continue
        v = r.get("value")
        d = _parse(r.get("observation_date"))
        if v is None or d is None:
            continue
        by_series.setdefault(r["series_id"], []).append((d, float(v)))
    for series in by_series.values():
        series.sort(key=lambda t: t[0])
    return by_series


def _year_ago_value(dates: list[date], values: list[float], i: int) -> float | None:
    """Value at the observation nearest on-or-before ~1 year before dates[i]."""
    target = dates[i] - timedelta(days=365)
    pos = bisect_right(dates, target) - 1  # latest date <= target
    if pos < 0:
        return None
    if (target - dates[pos]).days > YOY_TOLERANCE_DAYS:
        return None
    return values[pos]


def _expanding_mean_std(values: list[float]) -> tuple[list[float], list[float]]:
    """Expanding (point-in-time safe) population mean/std, via Welford's
    online algorithm.

    ``means[i]``/``stds[i]`` are computed from ``values[0..i]`` only — never
    later values — so a z-score built from them can't leak future
    information into earlier rows the way a full-sample mean/std would.
    """
    means: list[float] = []
    stds: list[float] = []
    mean = 0.0
    m2 = 0.0
    for n, v in enumerate(values, start=1):
        delta = v - mean
        mean += delta / n
        m2 += delta * (v - mean)
        means.append(mean)
        stds.append((m2 / n) ** 0.5 if n > 1 else 0.0)
    return means, stds


# ---- spec003 Phase 3: checkpointed/resumable expanding accumulators --------
#
# Same PIT-safe expanding math as `_expanding_mean_std`/`_expanding_percentile`
# above, but split into (state, new_values) -> (new_state, new_outputs) so an
# incremental Gold rebuild can extend a per-entity checkpoint forward with
# only the rows past its frontier, instead of rescanning that entity's full
# history on every run. The correctness contract is exact, not approximate:
# for any split point k, `resume(resume(init(), xs[:k]), xs[k:])` must
# reproduce the non-resumable function's output for `xs` bit-for-bit (see
# tests/test_features_resumable.py). Both state dicts are plain JSON-
# serializable (for gold_incremental_checkpoint.state_json).


def init_expanding_mean_std_state() -> dict[str, float]:
    return {"n": 0, "mean": 0.0, "m2": 0.0}


def resume_expanding_mean_std(
    state: dict[str, float], new_values: list[float]
) -> tuple[dict[str, float], list[float], list[float]]:
    """Welford's algorithm is inherently streaming, so this is an exact
    resume: the loop body is identical to `_expanding_mean_std`, just seeded
    from a prior (n, mean, m2) instead of always starting at (0, 0.0, 0.0).
    Returns the updated state and the *new* points' means/stds only."""
    n = state["n"]
    mean = state["mean"]
    m2 = state["m2"]
    means: list[float] = []
    stds: list[float] = []
    for v in new_values:
        n += 1
        delta = v - mean
        mean += delta / n
        m2 += delta * (v - mean)
        means.append(mean)
        stds.append((m2 / n) ** 0.5 if n > 1 else 0.0)
    return {"n": n, "mean": mean, "m2": m2}, means, stds


def init_expanding_percentile_state() -> dict[str, list[float]]:
    return {"sorted_values": []}


def resume_expanding_percentile(
    state: dict[str, list[float]], new_values: list[float]
) -> tuple[dict[str, list[float]], list[float | None]]:
    """Exact (not approximate) resume of `_expanding_percentile`: the state
    is the sorted list of every prior value, so a new point's rank is a
    `bisect_left` against exactly the history the non-resumable version
    would have linearly rescanned. `_expanding_percentile`'s `below = sum(1
    for x in values[:i+1] if x < v)` never counts `v` against itself (`x < v`
    excludes equal elements), so it equals `bisect_left(sorted(values[:i]),
    v)` -- computed here against `sorted_values` *before* inserting `v`."""
    sorted_values = state["sorted_values"]
    out: list[float | None] = []
    for v in new_values:
        n = len(sorted_values)
        out.append(bisect_left(sorted_values, v) / n if n else None)
        insort(sorted_values, v)
    return {"sorted_values": sorted_values}, out


def compute_feature_transforms(
    latest_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Per-series transforms from latest-revision rows (one per obs date)."""
    grouped = _group_sorted(latest_rows)
    out: list[dict[str, Any]] = []
    for series_id, series in grouped.items():
        dates = [d for d, _ in series]
        values = [v for _, v in series]
        exp_means, exp_stds = _expanding_mean_std(values)
        for i, (d, v) in enumerate(series):
            prev = values[i - 1] if i > 0 else None
            year_ago = _year_ago_value(dates, values, i)
            std = exp_stds[i]
            out.append(
                {
                    "series_id": series_id,
                    "observation_date": d.isoformat(),
                    "value": v,
                    "mom": _pct_change(v, prev),
                    "diff": (v - prev) if prev is not None else None,
                    "yoy": _pct_change(v, year_ago),
                    "zscore": ((v - exp_means[i]) / std) if std else None,
                }
            )
    return out


def compute_curve_spreads(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Iterable[SpreadDef] | None = None,
) -> list[dict[str, Any]]:
    """Compute spread (long−short) or ratio (long/short) series per ``spreads``.

    ``spreads`` defaults to :func:`fred_pipeline.spread_config.load_spread_defs`
    (``config/spreads.yml``), resolved fresh on every call so edits to that
    reviewable YAML take effect without a code change. A date is only emitted
    when both legs have a non-missing value (and, for a ratio, the short leg
    is nonzero) — there's no partial/null row for an undefined spread.
    """
    if spreads is None:
        spreads = load_spread_defs()

    values: dict[tuple[str, str], float] = {}
    for r in latest_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        values[(r["series_id"], str(r["observation_date"])[:10])] = float(r["value"])

    dates_by_series: dict[str, set[str]] = {}
    for sid, d in values:
        dates_by_series.setdefault(sid, set()).add(d)

    out: list[dict[str, Any]] = []
    for sd in spreads:
        common = dates_by_series.get(sd.long_leg, set()) & dates_by_series.get(
            sd.short_leg, set()
        )
        for d in sorted(common):
            long_v = values[(sd.long_leg, d)]
            short_v = values[(sd.short_leg, d)]
            if sd.op == "ratio":
                if short_v == 0:
                    continue
                value = long_v / short_v
            else:
                value = long_v - short_v
            out.append(
                {
                    "spread_name": sd.name,
                    "observation_date": d,
                    "long_leg": sd.long_leg,
                    "short_leg": sd.short_leg,
                    "value": value,
                }
            )
    return out


# ---- cross-series features (frequency-aware, N-leg) ------------------------


def _period_start(d: date, freq: str) -> str:
    """Canonical ISO period-start date for ``d`` at target ``freq``."""
    if freq == "w":
        iso = d.isocalendar()
        return date.fromisocalendar(iso[0], iso[1], 1).isoformat()
    if freq == "m":
        return date(d.year, d.month, 1).isoformat()
    if freq == "q":
        return date(d.year, ((d.month - 1) // 3) * 3 + 1, 1).isoformat()
    if freq == "a":
        return date(d.year, 1, 1).isoformat()
    return d.isoformat()  # daily (or unknown) -> the date itself


def _downsample_asof(series: list[tuple[date, float]], freq: str) -> dict[str, float]:
    """Align a date-sorted series to ``freq``: the last observation within each
    period, keyed by the period-start ISO date (as-of downsampling)."""
    out: dict[str, float] = {}
    for d, v in series:  # ascending → the latest date in each period wins
        out[_period_start(d, freq)] = v
    return out


def _combine_cross_series(
    by_series: dict[str, list[tuple[date, float]]],
    defs: Iterable[CrossSeriesDef],
    *,
    basis: str | None = None,
) -> list[dict[str, Any]]:
    """Align each leg to its target frequency and combine per ``op``.

    ``by_series`` maps series_id → date-sorted ``[(date, value)]`` (already
    reduced to one value per date — latest-revised or point-in-time, per caller).
    When ``basis`` is set it is stamped on every row (used by the PIT variant).
    """
    out: list[dict[str, Any]] = []
    for cd in defs:
        aligned: list[tuple[float, dict[str, float]]] | None = []
        for sid, weight in cd.legs:
            s = by_series.get(sid)
            if not s:
                aligned = None  # a missing leg → the whole feature is undefined
                break
            aligned.append((weight, _downsample_asof(s, cd.frequency)))
        if not aligned:
            continue

        common = set(aligned[0][1])
        for _w, m in aligned[1:]:
            common &= set(m)

        for period in sorted(common):
            vals = [m[period] for _w, m in aligned]
            if cd.op == "spread":
                value = vals[0] - vals[1]
            elif cd.op == "ratio":
                if vals[1] == 0:
                    continue
                value = vals[0] / vals[1]
            else:  # composite: weighted sum
                value = sum(w * v for (w, _m), v in zip(aligned, vals))
            row = {
                "feature_name": cd.name,
                "op": cd.op,
                "observation_date": period,
                "value": value,
            }
            if basis is not None:
                row["basis"] = basis
            out.append(row)
    return sorted(out, key=lambda r: (r["feature_name"], r["observation_date"]))


def compute_cross_series_features(
    latest_rows: Iterable[dict[str, Any]],
    defs: Iterable[CrossSeriesDef] | None = None,
) -> list[dict[str, Any]]:
    """Compute frequency-aware, N-leg cross-series features per ``defs``.

    ``defs`` defaults to ``config/cross_series.yml`` (see
    :func:`fred_pipeline.cross_series_config.load_cross_series_defs`). Each leg is
    aligned as-of to the feature's target frequency, then combined by ``op``
    (``spread`` = a−b, ``ratio`` = a/b, ``composite`` = Σ wᵢ·legᵢ). A period is
    emitted only when *every* leg has an aligned value (ratio also requires a
    nonzero denominator). Uses **latest-revised** values. Returns rows
    ``(feature_name, op, observation_date, value)``.
    """
    if defs is None:
        defs = load_cross_series_defs()
    return _combine_cross_series(_group_sorted(latest_rows), defs)


def _select_vintage(
    vintages: list[tuple[str, float]], as_of: str | None
) -> float | None:
    """Pick one value from an observation's vintages (``(realtime_start, value)``).

    ``as_of=None`` → **first report** (earliest ``realtime_start``). ``as_of=D`` →
    the value **known as of D** (latest ``realtime_start`` ≤ D); ``None`` if the
    observation wasn't published by D. A blank ``realtime_start`` (non-vintage
    series) is treated as always-known.
    """
    if as_of is None:
        return min(vintages, key=lambda t: t[0])[1]
    eligible = [t for t in vintages if t[0] == "" or t[0] <= as_of]
    if not eligible:
        return None
    return max(eligible, key=lambda t: t[0])[1]


def _pit_by_series(
    silver_rows: Iterable[dict[str, Any]], as_of: str | None
) -> dict[str, list[tuple[date, float]]]:
    """Reduce raw Silver (all vintages) to one point-in-time value per
    (series, observation_date), selected by :func:`_select_vintage`."""
    groups: dict[tuple[str, str], list[tuple[str, float]]] = {}
    for r in silver_rows:
        if r.get("is_missing"):
            continue
        v = r.get("value")
        od = r.get("observation_date")
        if v is None or not od:
            continue
        rt = r.get("realtime_start") or ""
        groups.setdefault((r["series_id"], str(od)[:10]), []).append(
            (str(rt), float(v))
        )

    by_series: dict[str, list[tuple[date, float]]] = {}
    for (sid, od), vintages in groups.items():
        chosen = _select_vintage(vintages, as_of)
        d = _parse(od)
        if chosen is None or d is None:
            continue
        by_series.setdefault(sid, []).append((d, chosen))
    for s in by_series.values():
        s.sort(key=lambda t: t[0])
    return by_series


def compute_cross_series_features_pit(
    silver_rows: Iterable[dict[str, Any]],
    defs: Iterable[CrossSeriesDef] | None = None,
    *,
    as_of: str | None = None,
) -> list[dict[str, Any]]:
    """Point-in-time (``realtime_start``-aligned) cross-series features.

    Same alignment/combination as :func:`compute_cross_series_features`, but each
    leg contributes the value that was **actually known** rather than the
    latest-revised one — so the resulting feature series is leak-free for
    backtests. Reads raw **Silver** (all vintages).

    ``as_of=None`` (default) builds the *as-first-reported* series (each
    observation's earliest vintage). ``as_of=D`` builds the series as it stood on
    date ``D`` (only vintages published by ``D``). Rows carry a ``basis`` column
    (``"first_report"`` or the as-of date). For non-vintage series this is
    identical to the latest-revised feature.
    """
    if defs is None:
        defs = load_cross_series_defs()
    basis = as_of if as_of else "first_report"
    return _combine_cross_series(_pit_by_series(silver_rows, as_of), defs, basis=basis)


def compute_source_reconciliation(
    latest_rows: Iterable[dict[str, Any]],
    defs: Iterable[ReconciliationDef] | None = None,
) -> list[dict[str, Any]]:
    """Compare same-concept series from different sources per ``defs``.

    ``defs`` defaults to ``config/reconciliations.yml``. Each pair is aligned
    as-of the target frequency, then for every common period the two values,
    their difference, percent difference (vs. ``series_b``), and a ``diverged``
    flag (``|pct| > tolerance_pct``) are emitted. Rows appear only when both
    series are loaded.
    """
    if defs is None:
        defs = load_reconciliation_defs()
    by_series = _group_sorted(latest_rows)

    out: list[dict[str, Any]] = []
    for rc in defs:
        a = by_series.get(rc.series_a)
        b = by_series.get(rc.series_b)
        if not a or not b:
            continue
        am = _downsample_asof(a, rc.frequency)
        bm = _downsample_asof(b, rc.frequency)
        for period in sorted(set(am) & set(bm)):
            va, vb = am[period], bm[period]
            abs_diff = va - vb
            pct_diff = (abs_diff / vb) if vb != 0 else None
            diverged = pct_diff is not None and abs(pct_diff) * 100.0 > rc.tolerance_pct
            out.append(
                {
                    "name": rc.name,
                    "observation_date": period,
                    "series_a": rc.series_a,
                    "value_a": va,
                    "series_b": rc.series_b,
                    "value_b": vb,
                    "abs_diff": abs_diff,
                    "pct_diff": pct_diff,
                    "diverged": diverged,
                }
            )
    return sorted(out, key=lambda r: (r["name"], r["observation_date"]))


def compute_revision_stats(
    silver_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """How much each observation moved between its first print and today.

    Unlike the other transforms (which read *latest-revision* rows),
    this reads raw **Silver** rows — every vintage — since it exists to
    measure revision behavior itself. Groups by ``(series_id,
    observation_date)`` and compares the vintage with the lowest
    ``revision_number`` (first release) against the one with the highest
    (latest as of this run).

    Non-vintage series (``vintage_enabled: false``) always have
    ``revision_count == 1`` — no vintage history is tracked for them, so
    there's nothing to compare; that's a legitimate "not revised" signal,
    not a data gap.
    """
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in silver_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        key = (r["series_id"], str(r["observation_date"])[:10])
        groups.setdefault(key, []).append(r)

    out: list[dict[str, Any]] = []
    for (series_id, obs_date), rows in groups.items():
        rows.sort(key=lambda r: r.get("revision_number") or 0)
        first, latest = rows[0], rows[-1]
        first_value = float(first["value"])
        latest_value = float(latest["value"])
        delta = latest_value - first_value
        out.append(
            {
                "series_id": series_id,
                "observation_date": obs_date,
                "revision_count": len(rows),
                "first_value": first_value,
                "first_realtime_start": first.get("realtime_start") or "",
                "latest_value": latest_value,
                "latest_realtime_start": latest.get("realtime_start") or "",
                "revision_delta": delta,
                "revision_pct": (delta / first_value) if first_value else None,
            }
        )
    return sorted(out, key=lambda r: (r["series_id"], r["observation_date"]))


def point_in_time_snapshot(
    silver_rows: Iterable[dict[str, Any]], as_of: str
) -> list[dict[str, Any]]:
    """Each series' latest value *known as of* ``as_of`` (leakage-free vector).

    Uses the vintage windows: a row is "known" when
    ``realtime_start <= as_of`` and (``realtime_end`` empty/open or ``> as_of``).
    Among known rows per series, the latest ``observation_date`` wins, and among
    that date's vintages, the latest ``realtime_start``. Non-vintage rows
    (blank realtime) are always considered known.
    """
    as_of = str(as_of)[:10]
    best: dict[str, dict[str, Any]] = {}
    for r in silver_rows:
        if r.get("is_missing"):
            continue
        rt_start = (r.get("realtime_start") or "")[:10]
        rt_end = (r.get("realtime_end") or "")[:10]
        if rt_start and rt_start > as_of:
            continue  # not yet known
        if rt_end and rt_end not in ("", "9999-12-31") and rt_end <= as_of:
            continue  # superseded before as_of
        sid = r["series_id"]
        obs = str(r["observation_date"])[:10]
        cur = best.get(sid)
        key = (obs, rt_start)
        if cur is None or key > (cur["observation_date"], cur.get("_rt", "")):
            best[sid] = {
                "as_of_date": as_of,
                "series_id": sid,
                "observation_date": obs,
                "value": r.get("value"),
                "_rt": rt_start,
            }
    result = [{k: v for k, v in row.items() if k != "_rt"} for row in best.values()]
    return sorted(result, key=lambda r: r["series_id"])
