"""Polars-accelerated Gold-layer feature computation for the local (SQLite) backend.

``transform.daily_feature_matrix`` and ``features.compute_feature_transforms`` /
``compute_curve_spreads`` are pure-Python "spec" implementations — correct,
unit-tested, and the reference every backend must match. At a few dozen series
they're fast enough. At hundreds/thousands of series (the local backend has no
Spark cluster to fall back on), the dense daily panel in particular
(``series_count x calendar_days``) is large enough that Python-level loops
dominate the run, so this module re-expresses the same semantics as vectorized
polars expressions.

Every function here must be output-identical (row-for-row, value-for-value) to
its pure-Python counterpart — see ``tests/test_gold_polars_parity.py``, which
runs both implementations over the same synthetic data and asserts equality.
Only ``local_store.LocalWarehouse.build_gold`` calls these; the Spark path
(``gold.py``) uses its own SQL and is untouched.

Each computation has two entry points:

* ``..._pl(rows) -> list[dict]`` — the parity-tested "spec" surface, matching
  the pure-Python function signature. Used by tests and any caller that wants
  plain rows.
* ``..._frame(rows) -> pl.DataFrame`` — the same computation, but stopping one
  step short of materializing Python dicts. ``list[dict]`` construction (one
  dict alloc + string key lookup per cell) turned out to dominate wall-clock
  time at large row counts — profiling on a synthetic 300-series/11.5M-row
  daily matrix showed the vectorized compute itself takes ~0.75s, but
  ``iter_rows(named=True)`` + dict-building took ~4.9s on top of it, erasing
  the win over pure Python. ``LocalWarehouse.build_gold`` uses the ``_frame``
  variants and inserts straight from ``df.iter_rows()`` (plain tuples, no dict
  overhead) to actually realize the speedup for large series universes.
"""

from __future__ import annotations

import warnings
from datetime import date, datetime
from typing import Any, Iterable, Optional

import polars as pl

from fred_pipeline.features import YOY_TOLERANCE_DAYS
from fred_pipeline.spread_config import SpreadDef, load_spread_defs


def _parse_date_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    return df.with_columns(
        pl.col(col)
        .cast(pl.Utf8)
        .str.slice(0, 10)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("_date")
    )


def daily_feature_matrix_frame(
    latest_rows: Iterable[dict[str, Any]], *, max_date: str | date | None = None
) -> pl.DataFrame:
    """Polars parity of :func:`fred_pipeline.transform.daily_feature_matrix`.

    Returns a DataFrame with columns ``(as_of_date, series_id, raw_value,
    value)`` — ``as_of_date`` already cast to ``Utf8`` (ISO date strings), so
    ``df.iter_rows()`` yields SQLite-ready tuples with no further conversion.
    """
    rows = [r for r in latest_rows if not r.get("is_missing", False)]
    if not rows:
        return pl.DataFrame(
            schema={
                "as_of_date": pl.Utf8,
                "series_id": pl.Utf8,
                "raw_value": pl.Float64,
                "value": pl.Float64,
            }
        )

    df = pl.DataFrame(
        {
            "series_id": [r["series_id"] for r in rows],
            "observation_date": [str(r["observation_date"]) for r in rows],
            "value": [r.get("value") for r in rows],
        }
    )
    df = _parse_date_col(df, "observation_date").drop_nulls("_date")
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "as_of_date": pl.Utf8,
                "series_id": pl.Utf8,
                "raw_value": pl.Float64,
                "value": pl.Float64,
            }
        )

    if max_date is None:
        max_d = df["_date"].max()
    elif isinstance(max_date, date):
        max_d = max_date
    else:
        max_d = datetime.strptime(str(max_date)[:10], "%Y-%m-%d").date()

    # Per-series daily calendar: each series spans from ITS OWN first
    # observation to the global max date, not the global min. Using a global
    # min cross-joined a centuries-long calendar (a single 1600s-era series
    # forces ~300y of daily rows) onto every series, producing hundreds of
    # millions of all-NULL leading rows (forward_fill leaves pre-start rows
    # NULL). Bounding per series is output-identical for every non-leading row
    # while cutting the row count ~7x on the full universe.
    panel = (
        df.group_by("series_id")
        .agg(pl.col("_date").min().alias("_start"))
        .with_columns(pl.date_ranges(pl.col("_start"), max_d, "1d").alias("as_of_date"))
        .explode("as_of_date")
        .select("series_id", "as_of_date")
    )

    native = df.select(
        pl.col("series_id"),
        pl.col("_date").alias("as_of_date"),
        pl.col("value").alias("raw_value"),
    )

    out = (
        panel.join(native, on=["series_id", "as_of_date"], how="left")
        .sort(["series_id", "as_of_date"])
        .with_columns(
            pl.col("raw_value").forward_fill().over("series_id").alias("value"),
            pl.col("as_of_date").cast(pl.Utf8),
        )
        .select("as_of_date", "series_id", "raw_value", "value")
        .sort(["series_id", "as_of_date"])
    )
    return out


def daily_feature_matrix_pl(
    latest_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Dict-returning spec surface — see module docstring. Prefer
    :func:`daily_feature_matrix_frame` + direct DataFrame insert for bulk loads."""
    return daily_feature_matrix_frame(latest_rows).to_dicts()


def compute_feature_transforms_frame(
    latest_rows: Iterable[dict[str, Any]],
) -> pl.DataFrame:
    """Polars parity of :func:`fred_pipeline.features.compute_feature_transforms`.

    Returns a DataFrame with columns ``(series_id, observation_date, value,
    mom, diff, yoy, zscore)`` — ``observation_date`` already cast to ``Utf8``.
    """
    schema = {
        "series_id": pl.Utf8,
        "observation_date": pl.Utf8,
        "value": pl.Float64,
        "mom": pl.Float64,
        "diff": pl.Float64,
        "yoy": pl.Float64,
        "zscore": pl.Float64,
    }
    rows = [
        r for r in latest_rows if not r.get("is_missing") and r.get("value") is not None
    ]
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(
        {
            "series_id": [r["series_id"] for r in rows],
            "observation_date": [str(r["observation_date"]) for r in rows],
            "value": [float(r["value"]) for r in rows],
        }
    )
    df = _parse_date_col(df, "observation_date").drop_nulls("_date")
    if df.is_empty():
        return pl.DataFrame(schema=schema)
    df = df.sort(["series_id", "_date"])

    # Expanding (point-in-time safe) population mean/std via
    # Var(X) = E[X^2] - E[X]^2, using per-group cumulative sums — matches
    # fred_pipeline.features._expanding_mean_std (Welford) up to float noise.
    # Both use only values[0..i], so a z-score built from them can't leak
    # future information into earlier rows the way a full-sample std would.
    df = df.with_columns(
        pl.col("value").shift(1).over("series_id").alias("_prev"),
        pl.col("value").cum_count().over("series_id").alias("_n"),
        pl.col("value").cum_sum().over("series_id").alias("_cum_sum"),
        (pl.col("value") ** 2).cum_sum().over("series_id").alias("_cum_sum_sq"),
    )
    df = df.with_columns(
        (pl.col("_cum_sum") / pl.col("_n")).alias("_mean"),
    )
    df = df.with_columns(
        (pl.col("_cum_sum_sq") / pl.col("_n") - pl.col("_mean") ** 2)
        .clip(lower_bound=0)  # guard tiny negative values from fp cancellation
        .sqrt()
        .alias("_std"),
    )

    # year-ago match: nearest observation at/before (date - 365d), within
    # YOY_TOLERANCE_DAYS — an as-of backward join per series.
    target = df.select(
        pl.col("series_id"),
        pl.col("_date"),
        (pl.col("_date") - pl.duration(days=365)).alias("_target"),
    ).sort(["series_id", "_target"])
    lookup = df.select("series_id", "_date", pl.col("value").alias("_year_ago")).sort(
        ["series_id", "_date"]
    )
    # Both frames are pre-sorted by (series_id, key) above; polars just can't
    # cheaply verify per-group sortedness when `by` is used, hence the warning.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Sortedness of columns cannot be checked"
        )
        yoy_lookup = target.join_asof(
            lookup,
            left_on="_target",
            right_on="_date",
            by="series_id",
            strategy="backward",
            tolerance=f"{YOY_TOLERANCE_DAYS}d",
        ).select("series_id", pl.col("_date").alias("_date_key"), "_year_ago")

    df = df.join(
        yoy_lookup,
        left_on=["series_id", "_date"],
        right_on=["series_id", "_date_key"],
        how="left",
    ).sort(["series_id", "_date"])

    df = df.with_columns(
        pl.when(pl.col("_prev").is_not_null())
        .then((pl.col("value") - pl.col("_prev")) / pl.col("_prev"))
        .otherwise(None)
        .alias("mom"),
        pl.when(pl.col("_prev").is_not_null())
        .then(pl.col("value") - pl.col("_prev"))
        .otherwise(None)
        .alias("diff"),
        pl.when(pl.col("_year_ago").is_not_null() & (pl.col("_year_ago") != 0))
        .then((pl.col("value") - pl.col("_year_ago")) / pl.col("_year_ago"))
        .otherwise(None)
        .alias("yoy"),
        pl.when(pl.col("_std") > 0)
        .then((pl.col("value") - pl.col("_mean")) / pl.col("_std"))
        .otherwise(None)
        .alias("zscore"),
        pl.col("_date").cast(pl.Utf8).alias("observation_date"),
    )

    return df.select(
        "series_id", "observation_date", "value", "mom", "diff", "yoy", "zscore"
    )


def compute_feature_transforms_pl(
    latest_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Dict-returning spec surface — see module docstring. Prefer
    :func:`compute_feature_transforms_frame` + direct DataFrame insert for bulk loads."""
    return compute_feature_transforms_frame(latest_rows).to_dicts()


def compute_curve_spreads_frame(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Optional[Iterable[SpreadDef]] = None,
) -> pl.DataFrame:
    """Polars parity of :func:`fred_pipeline.features.compute_curve_spreads`.

    ``spreads`` defaults to ``config/spreads.yml`` (see
    :func:`fred_pipeline.spread_config.load_spread_defs`), resolved fresh on
    every call. Returns a DataFrame with columns ``(spread_name,
    observation_date, long_leg, short_leg, value)``.
    """
    if spreads is None:
        spreads = load_spread_defs()

    schema = {
        "spread_name": pl.Utf8,
        "observation_date": pl.Utf8,
        "long_leg": pl.Utf8,
        "short_leg": pl.Utf8,
        "value": pl.Float64,
    }
    rows = [
        r for r in latest_rows if not r.get("is_missing") and r.get("value") is not None
    ]
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(
        {
            "series_id": [r["series_id"] for r in rows],
            "observation_date": [str(r["observation_date"])[:10] for r in rows],
            "value": [float(r["value"]) for r in rows],
        }
    )

    out_frames: list[pl.DataFrame] = []
    for sd in spreads:
        long_df = df.filter(pl.col("series_id") == sd.long_leg).select(
            "observation_date", pl.col("value").alias("_long")
        )
        short_df = df.filter(pl.col("series_id") == sd.short_leg).select(
            "observation_date", pl.col("value").alias("_short")
        )
        if long_df.is_empty() or short_df.is_empty():
            continue
        joined = long_df.join(short_df, on="observation_date", how="inner")
        if sd.op == "ratio":
            joined = joined.filter(pl.col("_short") != 0)
        if joined.is_empty():
            continue
        value_expr = (
            (pl.col("_long") / pl.col("_short"))
            if sd.op == "ratio"
            else (pl.col("_long") - pl.col("_short"))
        )
        out_frames.append(
            joined.select(
                pl.lit(sd.name).alias("spread_name"),
                pl.col("observation_date"),
                pl.lit(sd.long_leg).alias("long_leg"),
                pl.lit(sd.short_leg).alias("short_leg"),
                value_expr.alias("value"),
            )
        )

    if not out_frames:
        return pl.DataFrame(schema=schema)
    return pl.concat(out_frames).sort(["spread_name", "observation_date"])


def compute_curve_spreads_pl(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Optional[Iterable[SpreadDef]] = None,
) -> list[dict[str, Any]]:
    """Dict-returning spec surface — see module docstring. Prefer
    :func:`compute_curve_spreads_frame` + direct DataFrame insert for bulk loads."""
    return compute_curve_spreads_frame(latest_rows, spreads).to_dicts()


def compute_revision_stats_frame(silver_rows: Iterable[dict[str, Any]]) -> pl.DataFrame:
    """Polars parity of :func:`fred_pipeline.features.compute_revision_stats`.

    Reads raw Silver rows (every vintage), not latest-revision rows. Returns
    a DataFrame with columns ``(series_id, observation_date, revision_count,
    first_value, first_realtime_start, latest_value, latest_realtime_start,
    revision_delta, revision_pct)``.
    """
    schema = {
        "series_id": pl.Utf8,
        "observation_date": pl.Utf8,
        "revision_count": pl.UInt32,
        "first_value": pl.Float64,
        "first_realtime_start": pl.Utf8,
        "latest_value": pl.Float64,
        "latest_realtime_start": pl.Utf8,
        "revision_delta": pl.Float64,
        "revision_pct": pl.Float64,
    }
    rows = [
        r for r in silver_rows if not r.get("is_missing") and r.get("value") is not None
    ]
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(
        {
            "series_id": [r["series_id"] for r in rows],
            "observation_date": [str(r["observation_date"])[:10] for r in rows],
            "value": [float(r["value"]) for r in rows],
            "realtime_start": [r.get("realtime_start") or "" for r in rows],
            "revision_number": [r.get("revision_number") or 0 for r in rows],
        }
    )
    df = df.sort(["series_id", "observation_date", "revision_number"])

    agg = df.group_by(["series_id", "observation_date"], maintain_order=True).agg(
        pl.col("value").first().alias("first_value"),
        pl.col("realtime_start").first().alias("first_realtime_start"),
        pl.col("value").last().alias("latest_value"),
        pl.col("realtime_start").last().alias("latest_realtime_start"),
        pl.len().alias("revision_count"),
    )
    agg = agg.with_columns(
        (pl.col("latest_value") - pl.col("first_value")).alias("revision_delta"),
    )
    agg = agg.with_columns(
        pl.when(pl.col("first_value") != 0)
        .then(pl.col("revision_delta") / pl.col("first_value"))
        .otherwise(None)
        .alias("revision_pct"),
    )
    return agg.select(
        "series_id",
        "observation_date",
        "revision_count",
        "first_value",
        "first_realtime_start",
        "latest_value",
        "latest_realtime_start",
        "revision_delta",
        "revision_pct",
    ).sort(["series_id", "observation_date"])


def compute_revision_stats_pl(
    silver_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Dict-returning spec surface — see module docstring. Prefer
    :func:`compute_revision_stats_frame` + direct DataFrame insert for bulk loads."""
    return compute_revision_stats_frame(silver_rows).to_dicts()


def compute_cross_series_features_frame(
    latest_rows: Iterable[dict[str, Any]],
    defs: Optional[Iterable[Any]] = None,
) -> pl.DataFrame:
    """Polars-frame surface for cross-series features.

    The computation is inherently small (a few features over a few legs), so this
    delegates to the pure-Python reference
    (:func:`fred_pipeline.features.compute_cross_series_features`) and wraps the
    result — guaranteeing parity by construction — then returns a DataFrame for
    the fast ``_insert_frame`` path. Columns: ``(feature_name, op,
    observation_date, value)``.
    """
    from fred_pipeline.features import compute_cross_series_features

    schema = {
        "feature_name": pl.Utf8,
        "op": pl.Utf8,
        "observation_date": pl.Utf8,
        "value": pl.Float64,
    }
    rows = compute_cross_series_features(latest_rows, defs)
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


def compute_cross_series_features_pl(
    latest_rows: Iterable[dict[str, Any]],
    defs: Optional[Iterable[Any]] = None,
) -> list[dict[str, Any]]:
    """Dict-returning surface (parity with the pure-Python reference)."""
    return compute_cross_series_features_frame(latest_rows, defs).to_dicts()
