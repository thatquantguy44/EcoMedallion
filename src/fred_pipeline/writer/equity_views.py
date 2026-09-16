"""Equity Gold engines (pure Python), shared by both backends.

Equity sub-plan (handoff.md → "Equity Price & Total Return — Two-Source
Sub-Plan"):

  * :func:`select_canonical_equity_price_rows` +
    :func:`compute_equity_return_daily` → ``gold.equity_return_daily``: daily
    return per ticker from the canonical close series. Stooq ``:close`` is
    preferred when present; otherwise Tiingo ``:adjClose`` is mapped into the
    same shape so Gold does not depend on Stooq availability.
  * :func:`compute_index_constituents` → ``gold.index_constituents``: the
    per-snapshot ETF constituent weights from ``<ETF>:<constituent>`` series.
  * :func:`compute_equity_total_return_index` →
    ``gold.equity_total_return_index``: true total return per ticker from the
    Tiingo raw inputs (``close`` + ``divCash`` + ``splitFactor``), dividends
    reinvested — derived from raw so it survives dividend restatements.
  * :func:`compute_equity_price_reconciliation` →
    ``gold.equity_price_reconciliation``: cross-vendor close comparison for
    governance — Stooq split-adjusted close vs Tiingo ``adjClose`` per ticker,
    flagging dates where both sources diverge beyond a tolerance.
  * :func:`compute_realized_volatility` → ``gold.realized_volatility``:
    annualized trailing realized volatility per ticker (docs/handoffs/
    asset_class_expansion.md item 1a) — the free, no-new-source half of the
    volatility gap; implied vol/options chains remain a separate, paid-vendor
    decision.

**Source isolation.** Stooq and Tiingo both name a ``<ticker>:close`` series,
so the callers pass **source-filtered** Silver rows rather than the merged
latest-observation table — which would otherwise collapse vendor rows for the
same ticker/date onto one row. The engines themselves stay source-agnostic
(they just consume the rows handed to them).
"""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Iterable
from datetime import date, timedelta
from math import log, sqrt
from typing import Any

from fred_pipeline.equity_reconciliation_config import (
    EquityReconciliationConfig,
    load_equity_reconciliation_config,
)
from fred_pipeline.features import _parse, _pct_change

# Silver series_id suffixes carrying close-like price values.
CLOSE_FIELD = "close"
ADJ_CLOSE_FIELD = "adjClose"


def _close_ticker(series_id: str) -> str | None:
    """``AAPL:close`` → ``AAPL``; anything not a ``:close`` series → None."""
    ticker, sep, field = series_id.partition(":")
    if sep and field == CLOSE_FIELD and ticker:
        return ticker
    return None


def _field_ticker(series_id: str, wanted_field: str) -> str | None:
    ticker, sep, field = series_id.partition(":")
    if sep and field == wanted_field and ticker:
        return ticker.upper()
    return None


def select_canonical_equity_price_rows(
    stooq_rows: Iterable[dict[str, Any]],
    tiingo_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Choose the rows used by ``gold.equity_return_daily``.

    Stooq remains the preferred price-return source for tickers where it has
    data. For tickers with no Stooq ``:close`` rows, Tiingo ``:adjClose`` rows
    are converted into ``<ticker>:close`` rows so the existing return engine can
    build the Gold table from the keyed Tiingo feed.
    """
    out: list[dict[str, Any]] = []
    stooq_tickers: set[str] = set()
    for row in stooq_rows:
        ticker = _field_ticker(row.get("series_id", ""), CLOSE_FIELD)
        if ticker is None:
            continue
        stooq_tickers.add(ticker)
        out.append(dict(row))

    for row in tiingo_rows:
        ticker = _field_ticker(row.get("series_id", ""), ADJ_CLOSE_FIELD)
        if ticker is None or ticker in stooq_tickers:
            continue
        mapped = dict(row)
        mapped["series_id"] = f"{ticker}:close"
        mapped["source"] = "tiingo"
        out.append(mapped)
    return out


def compute_equity_return_daily(
    latest_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """``gold.equity_return_daily``: one row per ticker × date from the
    ``<ticker>:close`` row stream — the close, the day-over-day price change
    and simple return, and a cumulative return index (=100 at each ticker's
    first observation). Callers should pass
    :func:`select_canonical_equity_price_rows` when multiple vendors are
    available. A ticker with a single observation emits that row with null
    return."""
    by_ticker: dict[str, list[tuple[date, float]]] = {}
    for r in latest_rows:
        ticker = _close_ticker(r.get("series_id", ""))
        if ticker is None or r.get("is_missing"):
            continue
        v, d = r.get("value"), _parse(r.get("observation_date"))
        if v is None or d is None:
            continue
        by_ticker.setdefault(ticker, []).append((d, float(v)))

    out: list[dict[str, Any]] = []
    for ticker in sorted(by_ticker):
        series = sorted(by_ticker[ticker], key=lambda t: t[0])
        index = 100.0
        for i, (d, close) in enumerate(series):
            prev = series[i - 1][1] if i > 0 else None
            ret = _pct_change(close, prev)
            if ret is not None:
                index *= 1.0 + ret
            out.append(
                {
                    "ticker": ticker,
                    "observation_date": d.isoformat(),
                    "close": close,
                    "price_change": (close - prev) if prev is not None else None,
                    "price_return": ret,
                    "price_return_index": index,
                }
            )
    return out


def compute_index_constituents(
    latest_rows: Iterable[dict[str, Any]],
    etfs: Iterable[str] = ("IVV", "SPY"),
) -> list[dict[str, Any]]:
    """``gold.index_constituents``: one row per ETF × constituent × snapshot
    date from the ``<ETF>:<constituent>`` holdings weight series — the weight
    (percent), a within-snapshot weight rank, and an ``is_latest_snapshot``
    flag on the most recent as-of date per ETF (the row set a report filters to
    for "current membership"). ``etfs`` bounds which prefixes are treated as
    holdings series so a normal ``FOO:close`` price series is never mistaken
    for a constituent."""
    etf_set = {e.upper() for e in etfs}
    # etf -> obs_date -> list[(constituent, weight)]
    snapshots: dict[str, dict[str, list[tuple[str, float]]]] = {}
    for r in latest_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        sid = r.get("series_id", "")
        etf, sep, constituent = sid.partition(":")
        if not sep or etf.upper() not in etf_set or not constituent:
            continue
        d = _parse(r.get("observation_date"))
        if d is None:
            continue
        snapshots.setdefault(etf.upper(), {}).setdefault(d.isoformat(), []).append(
            (constituent, float(r["value"]))
        )

    out: list[dict[str, Any]] = []
    for etf in sorted(snapshots):
        by_date = snapshots[etf]
        latest_date = max(by_date)
        for obs_date in sorted(by_date):
            ranked = sorted(by_date[obs_date], key=lambda t: -t[1])
            for rank, (constituent, weight) in enumerate(ranked, start=1):
                out.append(
                    {
                        "index_etf": etf,
                        "constituent": constituent,
                        "observation_date": obs_date,
                        "weight_pct": weight,
                        "weight_rank": rank,
                        "is_latest_snapshot": obs_date == latest_date,
                    }
                )
    return out


# ---- Tiingo total return ---------------------------------------------------


def _field_maps(
    tiingo_rows: Iterable[dict[str, Any]],
) -> dict[str, dict[str, dict[date, float]]]:
    """Group exploded Tiingo rows into ``ticker -> field -> {date: value}``
    for the ``close`` / ``divCash`` / ``splitFactor`` fields."""
    wanted = {"close", "divCash", "splitFactor"}
    out: dict[str, dict[str, dict[date, float]]] = {}
    for r in tiingo_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        ticker, sep, field = r.get("series_id", "").partition(":")
        if not sep or field not in wanted:
            continue
        d = _parse(r.get("observation_date"))
        if d is None:
            continue
        out.setdefault(ticker, {}).setdefault(field, {})[d] = float(r["value"])
    return out


def compute_equity_total_return_index(
    tiingo_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """``gold.equity_total_return_index``: true total return per ticker × date
    reconstructed from the Tiingo raw inputs (``close`` + ``divCash`` +
    ``splitFactor``), dividends reinvested.

    Per day, using the split-adjusted convention (``close`` is the raw close;
    ``splitFactor`` is that day's split ratio; ``divCash`` the per-share cash
    dividend):

        price_return_t = close_t / close_{t-1} × splitFactor_t − 1
        total_return_t = (close_t + divCash_t) / close_{t-1} × splitFactor_t − 1

    A split day (price halves, factor 2) nets ~0; a dividend day adds
    ``div/close_{t-1}``. ``total_return_index`` / ``price_return_index`` are
    cumulative (=100 at each ticker's first date); their gap is the reinvested
    income. ``trailing_12m_dividend`` sums ``divCash`` over the trailing 365
    days and ``dividend_yield_pct`` divides it by the close. Deriving from raw
    inputs (not Tiingo's ``adjClose``) means the series can be rebuilt and
    diffed when a dividend is restated. Same-day split+dividend (rare) uses the
    formula as written.
    """
    maps = _field_maps(tiingo_rows)
    out: list[dict[str, Any]] = []
    for ticker in sorted(maps):
        closes = maps[ticker].get("close")
        if not closes:
            continue
        divs = maps[ticker].get("divCash", {})
        splits = maps[ticker].get("splitFactor", {})
        dates = sorted(closes)
        div_dates = sorted(divs)
        tr_index = pr_index = 100.0
        prev_close: float | None = None
        for d in dates:
            close = closes[d]
            div = divs.get(d, 0.0)
            split = splits.get(d, 1.0) or 1.0
            if prev_close and prev_close > 0:
                pr = close / prev_close * split - 1.0
                tr = (close + div) / prev_close * split - 1.0
                pr_index *= 1.0 + pr
                tr_index *= 1.0 + tr
            else:
                pr = tr = None
            # trailing-365d dividend sum (inclusive), via the sorted div dates.
            cutoff = d - timedelta(days=365)
            ttm_div = sum(
                divs[dd]
                for dd in div_dates[bisect_left(div_dates, cutoff) :]
                if dd <= d
            )
            out.append(
                {
                    "ticker": ticker,
                    "observation_date": d.isoformat(),
                    "close": close,
                    "dividend": div,
                    "split_factor": split,
                    "price_return": pr,
                    "total_return": tr,
                    "price_return_index": pr_index,
                    "total_return_index": tr_index,
                    "trailing_12m_dividend": ttm_div,
                    "dividend_yield_pct": (ttm_div / close * 100.0) if close else None,
                }
            )
            prev_close = close
    return out


def resume_equity_total_return_index(
    tiingo_rows: Iterable[dict[str, Any]],
    checkpoints: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Resumable version of compute_equity_total_return_index for Phase 3 incrementality.

    Processes only new dates > checkpoint frontier for each ticker. Returns updated
    checkpoints and only new rows (to be appended, not inserted wholesale).

    Checkpoints are {ticker: {frontier_date, price_return_index, total_return_index,
    prev_close}} — enough state to resume the cumulative calculation.
    """
    from bisect import bisect_left
    from datetime import date as date_type
    from datetime import timedelta

    maps = _field_maps(tiingo_rows)
    out: list[dict[str, Any]] = []
    updated_checkpoints = dict(checkpoints)

    for ticker in sorted(maps):
        closes = maps[ticker].get("close", {})
        if not closes:
            continue

        divs = maps[ticker].get("divCash", {})
        splits = maps[ticker].get("splitFactor", {})
        dates = sorted(closes)
        div_dates = sorted(divs)

        # Load or initialize checkpoint for this ticker
        cp = checkpoints.get(ticker, {})
        frontier = cp.get("frontier_date")
        frontier_date = date_type.fromisoformat(frontier) if frontier else None
        tr_index = cp.get("total_return_index", 100.0)
        pr_index = cp.get("price_return_index", 100.0)
        prev_close = cp.get("prev_close")

        # Process only dates > frontier
        new_dates = [d for d in dates if frontier_date is None or d > frontier_date]
        if not new_dates:
            continue

        for d in new_dates:
            close = closes[d]
            div = divs.get(d, 0.0)
            split = splits.get(d, 1.0) or 1.0
            if prev_close and prev_close > 0:
                pr = close / prev_close * split - 1.0
                tr = (close + div) / prev_close * split - 1.0
                pr_index *= 1.0 + pr
                tr_index *= 1.0 + tr
            else:
                pr = tr = None

            cutoff = d - timedelta(days=365)
            ttm_div = sum(
                divs[dd]
                for dd in div_dates[bisect_left(div_dates, cutoff) :]
                if dd <= d
            )

            out.append(
                {
                    "ticker": ticker,
                    "observation_date": d.isoformat(),
                    "close": close,
                    "dividend": div,
                    "split_factor": split,
                    "price_return": pr,
                    "total_return": tr,
                    "price_return_index": pr_index,
                    "total_return_index": tr_index,
                    "trailing_12m_dividend": ttm_div,
                    "dividend_yield_pct": (ttm_div / close * 100.0) if close else None,
                }
            )
            prev_close = close

        # Update checkpoint to the last processed date
        if new_dates:
            updated_checkpoints[ticker] = {
                "frontier_date": new_dates[-1].isoformat(),
                "price_return_index": pr_index,
                "total_return_index": tr_index,
                "prev_close": prev_close,
            }

    return updated_checkpoints, out


# ---- Cross-source price reconciliation ------------------------------------


def compute_equity_price_reconciliation(
    stooq_rows: Iterable[dict[str, Any]],
    tiingo_rows: Iterable[dict[str, Any]],
    cfg: EquityReconciliationConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.equity_price_reconciliation``: per-ticker daily diff between
    Stooq's split-adjusted close and Tiingo's ``adjClose`` (split+div adjusted).

    Both vendor adjustments diverge over time by the cumulative dividend
    component; ``tolerance_pct`` (default 2 %) is wide enough to absorb that
    drift while still catching genuine data errors (wrong prices, stale feeds).

    Rows are emitted only when both sources carry a value for the same
    (ticker, date); tickers not listed in ``cfg`` are silently skipped so the
    table only covers the curated reconciliation universe.
    """
    if cfg is None:
        cfg = load_equity_reconciliation_config()

    wanted: frozenset[str] = frozenset(cfg.tickers)

    # Stooq: <ticker>:close -> {ticker: {date: close}}
    stooq_by_ticker: dict[str, dict[str, float]] = {}
    for r in stooq_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        sid = r.get("series_id", "")
        ticker, sep, field = sid.partition(":")
        if not sep or field != "close" or ticker.upper() not in wanted:
            continue
        d = _parse(r.get("observation_date"))
        if d is None:
            continue
        stooq_by_ticker.setdefault(ticker.upper(), {})[d.isoformat()] = float(
            r["value"]
        )

    # Tiingo: <ticker>:adjClose -> {ticker: {date: adjClose}}
    tiingo_adj_by_ticker: dict[str, dict[str, float]] = {}
    for r in tiingo_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        sid = r.get("series_id", "")
        ticker, sep, field = sid.partition(":")
        if not sep or field != "adjClose" or ticker.upper() not in wanted:
            continue
        d = _parse(r.get("observation_date"))
        if d is None:
            continue
        tiingo_adj_by_ticker.setdefault(ticker.upper(), {})[d.isoformat()] = float(
            r["value"]
        )

    out: list[dict[str, Any]] = []
    for ticker in sorted(wanted):
        stooq_dates = stooq_by_ticker.get(ticker, {})
        tiingo_dates = tiingo_adj_by_ticker.get(ticker, {})
        common = sorted(set(stooq_dates) & set(tiingo_dates))
        for obs_date in common:
            va = stooq_dates[obs_date]
            vb = tiingo_dates[obs_date]
            abs_diff = va - vb
            pct_diff = (abs_diff / vb) if vb != 0.0 else None
            diverged = (
                pct_diff is not None and abs(pct_diff) * 100.0 > cfg.tolerance_pct
            )
            out.append(
                {
                    "ticker": ticker,
                    "observation_date": obs_date,
                    "stooq_close": va,
                    "tiingo_adj_close": vb,
                    "abs_diff": abs_diff,
                    "pct_diff": pct_diff,
                    "diverged": diverged,
                }
            )
    return out


# ---- Realized volatility ---------------------------------------------------

# Trailing observation-count windows for realized vol (~1mo, 1qtr, 2qtr, 1yr
# in trading days). Narrower than terminal_views.ROLLING_WINDOWS: a stdev
# over a 1- or 5-day window is too noisy to be a meaningful vol estimate.
VOL_WINDOWS = (21, 63, 126, 252)

TRADING_DAYS_PER_YEAR = 252


def compute_realized_volatility(
    canonical_price_rows: Iterable[dict[str, Any]],
    windows: tuple[int, ...] = VOL_WINDOWS,
    trading_days_per_year: int = TRADING_DAYS_PER_YEAR,
) -> list[dict[str, Any]]:
    """``gold.realized_volatility`` (docs/handoffs/asset_class_expansion.md
    item 1a): per ticker x date x window, the annualized realized volatility
    of daily **log** returns -- the trailing sample stdev, scaled by
    ``sqrt(trading_days_per_year)``, expressed as a percent.

    Log returns (``ln(close_t / close_{t-1})``), not the simple returns in
    :func:`compute_equity_return_daily`: log returns are time-additive, which
    is what makes the sqrt(time) annualization scaling valid. Computed with
    prefix sums (O(n x windows), not O(n x w)) -- same technique as
    ``terminal_views._rolling_window_rows``. A (ticker, window) row is
    emitted only once the window is fully populated (no partial-window
    stats), matching the rolling-companion convention used elsewhere in
    Gold. Pass :func:`select_canonical_equity_price_rows` output, the same
    input :func:`compute_equity_return_daily` takes.
    """
    by_ticker: dict[str, list[tuple[date, float]]] = {}
    for r in canonical_price_rows:
        ticker = _close_ticker(r.get("series_id", ""))
        if ticker is None or r.get("is_missing"):
            continue
        v, d = r.get("value"), _parse(r.get("observation_date"))
        if v is None or d is None or v <= 0:
            continue
        by_ticker.setdefault(ticker, []).append((d, float(v)))

    scale = sqrt(trading_days_per_year) * 100.0
    out: list[dict[str, Any]] = []
    for ticker in sorted(by_ticker):
        series = sorted(by_ticker[ticker], key=lambda t: t[0])
        log_returns: list[tuple[date, float]] = []
        for i in range(1, len(series)):
            prev_close = series[i - 1][1]
            close = series[i][1]
            if prev_close <= 0:
                continue
            log_returns.append((series[i][0], log(close / prev_close)))

        n = len(log_returns)
        values = [v for _d, v in log_returns]
        s = [0.0] * (n + 1)  # prefix sums of x and x^2
        s2 = [0.0] * (n + 1)
        for i, v in enumerate(values):
            s[i + 1] = s[i] + v
            s2[i + 1] = s2[i] + v * v

        for i in range(n):
            d, _v = log_returns[i]
            for w in windows:
                if i + 1 < w:
                    continue
                mean = (s[i + 1] - s[i + 1 - w]) / w
                var = max((s2[i + 1] - s2[i + 1 - w]) / w - mean * mean, 0.0)
                out.append(
                    {
                        "ticker": ticker,
                        "observation_date": d.isoformat(),
                        "window": w,
                        "realized_vol_pct": (var**0.5) * scale,
                    }
                )
    return out
