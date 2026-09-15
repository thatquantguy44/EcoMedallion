"""Phase ML-3: Recession Probability Model (expanding IRLS logistic regression).

Pure Python — shared by both backends.

Re-estimated at each new USREC print.  Requires USREC to be ingested;
returns no rows otherwise.  Features from config/recession_model.yml are
assembled PIT-safe: only values on-or-before the estimation date enter the
design matrix.
"""

from __future__ import annotations

import calendar
import math
import os
from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Numerics
# ---------------------------------------------------------------------------


def _sigmoid(z: float) -> float:
    """Numerically stable sigmoid: avoids exp overflow in both directions."""
    if z >= 0.0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def _solve(a: list[list[float]], b: list[float]) -> list[float] | None:
    """Gaussian elimination with partial pivoting. Returns None if singular."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-12:
            return None
        m[col], m[piv] = m[piv], m[col]
        for r in range(col + 1, n):
            f = m[r][col] / m[col][col]
            for c in range(col, n + 1):
                m[r][c] -= f * m[col][c]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (m[r][n] - sum(m[r][c] * x[c] for c in range(r + 1, n))) / m[r][r]
    return x


def _irls(
    X: list[list[float]],
    y: list[float],
    l2_lambda: float,
    max_iter: int,
    tol: float,
    warm_start: list[float] | None = None,
) -> list[float] | None:
    """IRLS logistic regression with L2 ridge penalty.

    X: n × k design matrix (first column is 1.0 for intercept).
    y: n binary labels (0.0 / 1.0).
    Returns beta (length k), or None if the normal-equation solver fails.
    """
    n = len(y)
    if n == 0:
        return None
    k = len(X[0])
    beta: list[float] = warm_start[:] if warm_start else [0.0] * k

    for _ in range(max_iter):
        eta = [sum(beta[j] * X[i][j] for j in range(k)) for i in range(n)]
        mu = [_sigmoid(e) for e in eta]
        w = [max(m * (1.0 - m), 1e-8) for m in mu]

        # X^T W X + λI
        XtWX = [
            [sum(w[i] * X[i][j] * X[i][l] for i in range(n)) for l in range(k)]
            for j in range(k)
        ]
        for j in range(k):
            XtWX[j][j] += l2_lambda

        # X^T W z  (working response z_i = η_i + (y_i − μ_i)/w_i)
        XtWz = [
            sum(w[i] * X[i][j] * (eta[i] + (y[i] - mu[i]) / w[i]) for i in range(n))
            for j in range(k)
        ]

        new_beta = _solve(XtWX, XtWz)
        if new_beta is None:
            return None

        delta = math.sqrt(sum((new_beta[j] - beta[j]) ** 2 for j in range(k)))
        beta = new_beta
        if delta < tol:
            break

    return beta


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------


def _add_months(d: date, months: int) -> date:
    """Return d + months calendar months, clamping to end-of-month."""
    total = d.month - 1 + months
    year = d.year + total // 12
    month = total % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return d.replace(year=year, month=month, day=min(d.day, last_day))


def _asof(pairs: list[tuple[date, float]], d: date) -> float | None:
    """Return the most recent value on or before d using binary search."""
    pos = bisect_right(pairs, (d, float("inf"))) - 1
    if pos < 0:
        return None
    return pairs[pos][1]


# ---------------------------------------------------------------------------
# Forward label construction
# ---------------------------------------------------------------------------


def _forward_labels(
    usrec_pairs: list[tuple[date, float]],
    h: int,
) -> dict[date, float]:
    """y_h(t) = 1.0 if any USREC=1 in the half-open window (t, t+h].

    Only dates where add_months(t, h) ≤ last USREC date get a label —
    the forward window is not yet observed for more-recent dates and
    those rows are excluded from training to avoid leakage.
    """
    if not usrec_pairs:
        return {}
    usrec_dates = [d for d, _ in usrec_pairs]
    usrec_val = {d: v for d, v in usrec_pairs}
    last_date = usrec_dates[-1]
    labels: dict[date, float] = {}
    for t, _ in usrec_pairs:
        if _add_months(t, h) > last_date:
            continue
        lo = bisect_right(usrec_dates, t)
        hi = bisect_right(usrec_dates, _add_months(t, h))
        label = (
            1.0 if any(usrec_val[usrec_dates[i]] == 1.0 for i in range(lo, hi)) else 0.0
        )
        labels[t] = label
    return labels


# ---------------------------------------------------------------------------
# Feature extraction helpers
# ---------------------------------------------------------------------------


def _feature_pairs(
    rows: list[dict[str, Any]],
    series_id: str,
    field: str,
) -> list[tuple[date, float]]:
    """Extract (date, value) pairs for a given series_id and column."""
    out: list[tuple[date, float]] = []
    for r in rows:
        if r.get("series_id") != series_id:
            continue
        d_str = r.get("observation_date")
        v = r.get(field)
        if d_str is None or v is None:
            continue
        try:
            out.append((date.fromisoformat(str(d_str)), float(v)))
        except (ValueError, TypeError):
            continue
    return sorted(out)


def _dated_pairs(
    rows: list[dict[str, Any]],
    date_field: str,
    value_field: str,
    filter_key: str | None = None,
    filter_val: str | None = None,
) -> list[tuple[date, float]]:
    """Generic (date, value) extractor with optional single-key filter."""
    out: list[tuple[date, float]] = []
    for r in rows:
        if filter_key is not None and r.get(filter_key) != filter_val:
            continue
        d_str = r.get(date_field)
        v = r.get(value_field)
        if d_str is None or v is None:
            continue
        try:
            out.append((date.fromisoformat(str(d_str)), float(v)))
        except (ValueError, TypeError):
            continue
    return sorted(out)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "config", "recession_model.yml"
)


@dataclass(frozen=True)
class RecessionModelConfig:
    min_obs: int
    l2_lambda: float
    max_iter: int
    tol: float
    horizon_months: tuple[int, ...]
    ns_slope: bool
    unrate_mom: bool
    indpro_mom: bool
    hy_oas_zscore: bool
    hy_oas_instrument: str
    funding_stress: bool
    regime_composite: bool


def load_recession_model_config(
    path: str | None = None,
) -> RecessionModelConfig:
    """Load config/recession_model.yml."""
    p = path or _DEFAULT_CONFIG_PATH
    with open(p) as fh:
        d = yaml.safe_load(fh) or {}
    feats = d.get("features", {})
    return RecessionModelConfig(
        min_obs=int(d.get("min_obs", 60)),
        l2_lambda=float(d.get("l2_lambda", 0.01)),
        max_iter=int(d.get("max_iter", 25)),
        tol=float(d.get("tol", 1e-7)),
        horizon_months=tuple(int(h) for h in d.get("horizon_months", [3, 6, 12])),
        ns_slope=bool(feats.get("ns_slope", True)),
        unrate_mom=bool(feats.get("unrate_mom", True)),
        indpro_mom=bool(feats.get("indpro_mom", True)),
        hy_oas_zscore=bool(feats.get("hy_oas_zscore", True)),
        hy_oas_instrument=str(feats.get("hy_oas_instrument", "BAMLH0A0HYM2")),
        funding_stress=bool(feats.get("funding_stress", True)),
        regime_composite=bool(feats.get("regime_composite", True)),
    )


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------


def compute_recession_probability(
    latest_rows: Iterable[dict[str, Any]],
    *,
    ns_factor_rows: list[dict[str, Any]],
    feature_transform_rows: list[dict[str, Any]],
    credit_spread_rows: list[dict[str, Any]],
    funding_stress_rows: list[dict[str, Any]],
    regime_rows: list[dict[str, Any]],
    cfg: RecessionModelConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.recession_probability_daily``: one row per USREC date.

    Expanding IRLS logistic regression — re-estimated at each new USREC print.
    Features assembled PIT-safe: as-of value on-or-before each estimation date.

    Returns [] when USREC is not ingested or has no rows.
    """
    if cfg is None:
        cfg = load_recession_model_config()

    # 1. Extract USREC (monthly, 0/1).
    latest_list = list(latest_rows)
    usrec_pairs = _dated_pairs(
        [r for r in latest_list if r.get("series_id") == "USREC"],
        date_field="observation_date",
        value_field="value",
    )
    if not usrec_pairs:
        return []

    # 2. Build named feature time series (PIT-safe as-of lookup lists).
    feature_sources: dict[str, list[tuple[date, float]]] = {}

    if cfg.ns_slope:
        feature_sources["ns_slope"] = _dated_pairs(
            ns_factor_rows, "observation_date", "beta1"
        )
    if cfg.unrate_mom:
        feature_sources["unrate_mom"] = _feature_pairs(
            feature_transform_rows, "UNRATE", "mom"
        )
    if cfg.indpro_mom:
        feature_sources["indpro_mom"] = _feature_pairs(
            feature_transform_rows, "INDPRO", "mom"
        )
    if cfg.hy_oas_zscore:
        feature_sources["hy_oas_zscore"] = _dated_pairs(
            credit_spread_rows,
            "observation_date",
            "zscore",
            filter_key="instrument",
            filter_val=cfg.hy_oas_instrument,
        )
    if cfg.funding_stress:
        feature_sources["funding_stress"] = _dated_pairs(
            funding_stress_rows, "observation_date", "stress_score"
        )
    if cfg.regime_composite:
        feature_sources["regime_composite"] = _dated_pairs(
            regime_rows, "observation_date", "composite_score"
        )

    feature_names = list(feature_sources.keys())
    k = len(feature_names) + 1  # +1 for intercept column

    # 3. Forward labels for each horizon.
    labels_by_h = {h: _forward_labels(usrec_pairs, h) for h in cfg.horizon_months}

    # 4. Expand IRLS over each USREC date (monthly cadence).
    usrec_dates = [d for d, _ in usrec_pairs]
    last_beta: dict[int, list[float] | None] = {h: None for h in cfg.horizon_months}
    out: list[dict[str, Any]] = []

    for obs_date in usrec_dates:
        # Build inference feature row (as-of obs_date); missing → 0.0.
        x_row = [1.0] + [
            (_asof(feature_sources[f], obs_date) or 0.0) for f in feature_names
        ]
        n_features = sum(
            1 for f in feature_names if _asof(feature_sources[f], obs_date) is not None
        )

        probs: dict[int, float | None] = {}
        logits: dict[int, float | None] = {}
        n_obs_by_h: dict[int, int] = {}

        for h in cfg.horizon_months:
            labels = labels_by_h[h]
            train_dates = [d for d in usrec_dates if d <= obs_date and d in labels]
            n_obs = len(train_dates)
            n_obs_by_h[h] = n_obs

            if n_obs < cfg.min_obs:
                probs[h] = None
                logits[h] = None
                continue

            X_train = [
                [1.0] + [(_asof(feature_sources[f], d) or 0.0) for f in feature_names]
                for d in train_dates
            ]
            y_train = [labels[d] for d in train_dates]

            beta = _irls(
                X_train,
                y_train,
                l2_lambda=cfg.l2_lambda,
                max_iter=cfg.max_iter,
                tol=cfg.tol,
                warm_start=last_beta[h],
            )
            if beta is not None:
                last_beta[h] = beta
                logit = sum(beta[j] * x_row[j] for j in range(k))
                logits[h] = logit
                probs[h] = _sigmoid(logit)
            else:
                probs[h] = None
                logits[h] = None

        # Primary horizon is the first (shortest) — drives headline fields.
        primary_h = cfg.horizon_months[0]
        is_backfilled = n_obs_by_h[primary_h] < cfg.min_obs

        row: dict[str, Any] = {
            "observation_date": obs_date.isoformat(),
            "recession_prob": probs[primary_h],
            "logit_score": logits[primary_h],
            "n_features": n_features,
            "n_obs_training": n_obs_by_h[primary_h],
            "model_vintage": obs_date.isoformat(),
            "is_backfilled": int(is_backfilled),
        }
        # Per-horizon probability fields.
        for h in cfg.horizon_months:
            row[f"prob_recession_{h}m"] = probs[h]

        out.append(row)

    return out


def resume_recession_probability(
    latest_rows: Iterable[dict[str, Any]],
    *,
    ns_factor_rows: list[dict[str, Any]],
    feature_transform_rows: list[dict[str, Any]],
    credit_spread_rows: list[dict[str, Any]],
    funding_stress_rows: list[dict[str, Any]],
    regime_rows: list[dict[str, Any]],
    checkpoints: dict[str, Any],
    cfg: RecessionModelConfig | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Resumable version of compute_recession_probability for Phase 3 incrementality.

    Processes only USREC dates > checkpoint frontier. Returns updated checkpoint
    and only new rows (to be appended, not inserted wholesale).

    Checkpoint: {last_usrec_date, model_weights_by_horizon} where model_weights_by_horizon
    is {horizon: beta_list} to warm-start the IRLS on next build.
    """
    if cfg is None:
        cfg = load_recession_model_config()

    latest_list = list(latest_rows)
    usrec_pairs = _dated_pairs(
        [r for r in latest_list if r.get("series_id") == "USREC"],
        date_field="observation_date",
        value_field="value",
    )
    if not usrec_pairs:
        return checkpoints, []

    # Load checkpoint: frontier date and prior model weights
    frontier_str = checkpoints.get("last_usrec_date")
    frontier_date = date.fromisoformat(frontier_str) if frontier_str else None
    last_beta_ckpt = checkpoints.get("model_weights_by_horizon", {})

    # Build feature time series (same as full compute)
    feature_sources: dict[str, list[tuple[date, float]]] = {}
    if cfg.ns_slope:
        feature_sources["ns_slope"] = _dated_pairs(
            ns_factor_rows, "observation_date", "beta1"
        )
    if cfg.unrate_mom:
        feature_sources["unrate_mom"] = _feature_pairs(
            feature_transform_rows, "UNRATE", "mom"
        )
    if cfg.indpro_mom:
        feature_sources["indpro_mom"] = _feature_pairs(
            feature_transform_rows, "INDPRO", "mom"
        )
    if cfg.hy_oas_zscore:
        feature_sources["hy_oas_zscore"] = _dated_pairs(
            credit_spread_rows,
            "observation_date",
            "zscore",
            filter_key="instrument",
            filter_val=cfg.hy_oas_instrument,
        )
    if cfg.funding_stress:
        feature_sources["funding_stress"] = _dated_pairs(
            funding_stress_rows, "observation_date", "stress_score"
        )
    if cfg.regime_composite:
        feature_sources["regime_composite"] = _dated_pairs(
            regime_rows, "observation_date", "composite_score"
        )

    feature_names = list(feature_sources.keys())
    k = len(feature_names) + 1  # +1 for intercept

    # Forward labels for each horizon
    labels_by_h = {h: _forward_labels(usrec_pairs, h) for h in cfg.horizon_months}

    # Process only USREC dates > frontier
    usrec_dates = [d for d, _ in usrec_pairs]
    new_dates = [d for d in usrec_dates if frontier_date is None or d > frontier_date]
    if not new_dates:
        return checkpoints, []

    # Warm-start from checkpoint weights
    last_beta: dict[int, list[float] | None] = {
        h: last_beta_ckpt.get(str(h)) for h in cfg.horizon_months
    }

    out: list[dict[str, Any]] = []

    # Recompute from frontier forward (IRLS warm-starts from prior beta)
    for obs_date in new_dates:
        x_row = [1.0] + [
            (_asof(feature_sources[f], obs_date) or 0.0) for f in feature_names
        ]
        n_features = sum(
            1 for f in feature_names if _asof(feature_sources[f], obs_date) is not None
        )

        probs: dict[int, float | None] = {}
        logits: dict[int, float | None] = {}
        n_obs_by_h: dict[int, int] = {}

        for h in cfg.horizon_months:
            labels = labels_by_h[h]
            train_dates = [d for d in usrec_dates if d <= obs_date and d in labels]
            n_obs = len(train_dates)
            n_obs_by_h[h] = n_obs

            if n_obs < cfg.min_obs:
                probs[h] = None
                logits[h] = None
                continue

            X_train = [
                [1.0] + [(_asof(feature_sources[f], d) or 0.0) for f in feature_names]
                for d in train_dates
            ]
            y_train = [labels[d] for d in train_dates]

            beta = _irls(
                X_train,
                y_train,
                l2_lambda=cfg.l2_lambda,
                max_iter=cfg.max_iter,
                tol=cfg.tol,
                warm_start=last_beta[h],
            )
            if beta is not None:
                last_beta[h] = beta
                logit = sum(beta[j] * x_row[j] for j in range(k))
                logits[h] = logit
                probs[h] = _sigmoid(logit)
            else:
                probs[h] = None
                logits[h] = None

        primary_h = cfg.horizon_months[0]
        is_backfilled = n_obs_by_h[primary_h] < cfg.min_obs

        row: dict[str, Any] = {
            "observation_date": obs_date.isoformat(),
            "recession_prob": probs[primary_h],
            "logit_score": logits[primary_h],
            "n_features": n_features,
            "n_obs_training": n_obs_by_h[primary_h],
            "model_vintage": obs_date.isoformat(),
            "is_backfilled": int(is_backfilled),
        }
        for h in cfg.horizon_months:
            row[f"prob_recession_{h}m"] = probs[h]

        out.append(row)

    # Update checkpoint: frontier is now the last new date processed
    updated_checkpoints = {
        "last_usrec_date": new_dates[-1].isoformat(),
        "model_weights_by_horizon": {str(h): last_beta[h] for h in cfg.horizon_months},
    }

    return updated_checkpoints, out
