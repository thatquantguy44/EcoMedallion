"""Phase-5 engines: the macro regime playbook and the statistical lab.

Pure Python (dict-in → dict-out), shared verbatim by the SQLite and Spark
backends like :mod:`fred_pipeline.terminal_views`:

  * :func:`compute_macro_regime` → ``gold.macro_regime_daily``: the five
    pillar scores (growth / inflation / liquidity / credit / policy), each a
    weighted blend of direction-adjusted **expanding** z-scores of its input
    series carried as-of each date, the signed composite, and the named
    regime from the ordered rule table in ``config/regime.yml``.
  * :func:`compute_series_correlation` → ``gold.series_correlation``:
    rolling / expanding Pearson correlation per configured pair
    (``config/stats_pairs.yml``), on transformed (default first-differenced)
    aligned series.
  * :func:`compute_series_lead_lag` → ``gold.series_lead_lag``:
    cross-correlation at lags −K..+K (positive lag = ``series_a`` leads) and
    a two-direction Granger F-test with a pure-Python p-value (regularized
    incomplete beta — no SciPy dependency).

Everything is trailing/expanding-window only, so nothing leaks future
information into historical rows.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from collections.abc import Iterable
from datetime import date
from typing import Any

from fred_pipeline.features import (
    _expanding_mean_std,
    _group_sorted,
    _pct_change,
    _year_ago_value,
)
from fred_pipeline.regime_stats_config import (
    RegimeConfig,
    StatsConfig,
    load_regime_config,
    load_stats_config,
)

# ---- shared: series transforms -------------------------------------------------


def _transformed(
    series: list[tuple[date, float]], transform: str
) -> list[tuple[date, float]]:
    """Apply a config transform to a date-sorted series. ``level`` is the
    identity; ``diff``/``mom`` consume one leading observation; ``yoy`` is
    date-based (same tolerance as the feature engine) and keeps only dates
    with a year-ago match."""
    if transform == "level":
        return list(series)
    if transform in ("diff", "mom"):
        out = []
        for i in range(1, len(series)):
            prev, cur = series[i - 1][1], series[i][1]
            if transform == "diff":
                out.append((series[i][0], cur - prev))
            else:
                pc = _pct_change(cur, prev)
                if pc is not None:
                    out.append((series[i][0], pc))
        return out
    if transform == "yoy":
        dates = [d for d, _v in series]
        values = [v for _d, v in series]
        out = []
        for i in range(len(series)):
            base = _year_ago_value(dates, values, i)
            pc = _pct_change(values[i], base)
            if pc is not None:
                out.append((dates[i], pc))
        return out
    raise ValueError(f"unknown transform {transform!r}")


# ---- macro regime playbook -----------------------------------------------------


def compute_macro_regime(
    latest_rows: Iterable[dict[str, Any]],
    cfg: RegimeConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.macro_regime_daily``: one row per emission date (the union of
    the inputs' post-transform observation dates), from the first date every
    pillar has at least one live input.

    Per input: transform → expanding (PIT-safe) z-score on its native dates →
    ``direction × z``. Per date: each input contributes its last value
    on-or-before the date unless staler than ``max_staleness_days``; pillar
    score = Σwᵢzᵢ/Σwᵢ over live inputs. The composite is the
    ``composite_weight``-signed mean; the regime name is the first matching
    rule (all conditions must hold), else the default. ``regime_confidence``
    is the smallest margin (in z units) by which the matched rule's
    conditions clear their thresholds (``None`` for the default regime).
    """
    if cfg is None:
        cfg = load_regime_config()
    if not cfg.pillars:
        return []
    wanted = {i.series_id for p in cfg.pillars for i in p.inputs}
    by_series = _group_sorted(r for r in latest_rows if r.get("series_id") in wanted)

    # (pillar, input) -> date-sorted [(date, direction-adjusted z)]
    z_series: dict[tuple[str, str], list[tuple[date, float]]] = {}
    all_dates: set[date] = set()
    for p in cfg.pillars:
        for inp in p.inputs:
            base = by_series.get(inp.series_id)
            if not base:
                continue
            t = _transformed(base, inp.transform)
            if not t:
                continue
            values = [v for _d, v in t]
            means, stds = _expanding_mean_std(values)
            zs = [
                (t[i][0], inp.direction * (values[i] - means[i]) / stds[i])
                for i in range(len(t))
                if stds[i]
            ]
            if zs:
                z_series[(p.name, inp.series_id)] = zs
                all_dates.update(d for d, _z in zs)

    def _asof(key: tuple[str, str], d: date) -> float | None:
        s = z_series.get(key)
        if not s:
            return None
        pos = bisect_right(s, (d, float("inf"))) - 1
        if pos < 0 or (d - s[pos][0]).days > cfg.max_staleness_days:
            return None
        return s[pos][1]

    total_cw = sum(abs(p.composite_weight) for p in cfg.pillars) or 1.0
    out: list[dict[str, Any]] = []
    for d in sorted(all_dates):
        scores: dict[str, float] = {}
        for p in cfg.pillars:
            num = den = 0.0
            for inp in p.inputs:
                z = _asof((p.name, inp.series_id), d)
                if z is not None:
                    num += inp.weight * z
                    den += inp.weight
            if den:
                scores[p.name] = num / den
        if len(scores) < len(cfg.pillars):
            continue  # not every pillar live yet

        regime_name = cfg.default_regime
        confidence: float | None = None
        for rule in cfg.rules:
            if all(c.matches(scores[c.pillar]) for c in rule.conditions):
                regime_name = rule.name
                confidence = min(
                    abs(scores[c.pillar] - c.threshold) for c in rule.conditions
                )
                break
        composite = (
            sum(p.composite_weight * scores[p.name] for p in cfg.pillars) / total_cw
        )
        out.append(
            {
                "observation_date": d.isoformat(),
                "growth_score": scores["growth"],
                "inflation_score": scores["inflation"],
                "liquidity_score": scores["liquidity"],
                "credit_score": scores["credit"],
                "policy_score": scores["policy"],
                "composite_score": composite,
                "regime_name": regime_name,
                "regime_confidence": confidence,
            }
        )
    return out


# ---- correlation lab -------------------------------------------------------------


def _aligned(
    by_series: dict[str, list[tuple[date, float]]], pair: Any
) -> tuple[list[date], list[float], list[float]]:
    """Inner-join the pair's transformed series on common dates."""
    a = dict(_transformed(by_series.get(pair.series_a, []), pair.transform_a))
    b = dict(_transformed(by_series.get(pair.series_b, []), pair.transform_b))
    common = sorted(set(a) & set(b))
    return common, [a[d] for d in common], [b[d] for d in common]


def _corr_from_sums(n, sx, sy, sxx, syy, sxy) -> float | None:
    cov = sxy - sx * sy / n
    vx = sxx - sx * sx / n
    vy = syy - sy * sy / n
    if vx <= 1e-12 or vy <= 1e-12:
        return None
    return max(-1.0, min(1.0, cov / math.sqrt(vx * vy)))


def compute_series_correlation(
    latest_rows: Iterable[dict[str, Any]],
    cfg: StatsConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.series_correlation``: per pair × window × date, the Pearson
    correlation of the transformed, date-aligned series over the trailing
    ``window`` observations (window 0 = expanding full sample to date, from
    the third common observation). Rolling windows emit only once fully
    populated. Prefix sums keep this O(n × windows)."""
    if cfg is None:
        cfg = load_stats_config()
    if not cfg.pairs:
        return []
    wanted = {s for p in cfg.pairs for s in (p.series_a, p.series_b)}
    by_series = _group_sorted(r for r in latest_rows if r.get("series_id") in wanted)

    out: list[dict[str, Any]] = []
    for pair in cfg.pairs:
        dates, xs, ys = _aligned(by_series, pair)
        n = len(dates)
        if n < 3:
            continue
        px = [0.0] * (n + 1)
        py = [0.0] * (n + 1)
        pxx = [0.0] * (n + 1)
        pyy = [0.0] * (n + 1)
        pxy = [0.0] * (n + 1)
        for i in range(n):
            px[i + 1] = px[i] + xs[i]
            py[i + 1] = py[i] + ys[i]
            pxx[i + 1] = pxx[i] + xs[i] * xs[i]
            pyy[i + 1] = pyy[i] + ys[i] * ys[i]
            pxy[i + 1] = pxy[i] + xs[i] * ys[i]
        for w in cfg.windows:
            for i in range(n):
                lo = 0 if w == 0 else i + 1 - w
                if w == 0:
                    if i < 2:
                        continue  # expanding: need >= 3 obs
                elif lo < 0:
                    continue  # rolling: window not yet full
                m = i + 1 - lo
                corr = _corr_from_sums(
                    m,
                    px[i + 1] - px[lo],
                    py[i + 1] - py[lo],
                    pxx[i + 1] - pxx[lo],
                    pyy[i + 1] - pyy[lo],
                    pxy[i + 1] - pxy[lo],
                )
                out.append(
                    {
                        "series_a": pair.series_a,
                        "series_b": pair.series_b,
                        "transform_a": pair.transform_a,
                        "transform_b": pair.transform_b,
                        "window": w,
                        "observation_date": dates[i].isoformat(),
                        "correlation": corr,
                        "n_obs": m,
                    }
                )
    return out


def init_series_correlation_state() -> dict[str, Any]:
    return {
        "n": 0,
        "sx": 0.0,
        "sy": 0.0,
        "sxx": 0.0,
        "syy": 0.0,
        "sxy": 0.0,
        "history": [],
    }


def resume_series_correlation(
    series_a: str,
    series_b: str,
    transform_a: str,
    transform_b: str,
    new_points: list[tuple[date, float, float]],
    state: dict[str, Any] | None,
    windows: tuple[int, ...],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """spec003 Phase 3: checkpointed/resumable body of
    :func:`compute_series_correlation`, for one pair at a time.

    ``new_points`` must already be exactly the aligned ``(date, x, y)``
    triples past this pair's checkpoint frontier, sorted by date -- callers
    get these from :func:`_aligned` (same transform+inner-join the original
    function uses) restricted to dates after the frontier.

    The correctness-critical design point: the original computes rolling
    windows via prefix-sum subtraction (``px[i+1] - px[lo]``), not by
    directly summing the window's raw values. Those two are mathematically
    equal but not bit-identical (different floating-point cancellation), so
    a resume built on a ring buffer of *raw values* would satisfy the same
    correlation formula while silently failing the bit-for-bit parity
    contract every other resumable engine in this codebase holds to. This
    keeps a bounded ring buffer of *cumulative-sum snapshots* instead
    (``history``, capped at ``max(windows)``) and reconstructs a window's
    sums via subtraction from exactly ``w`` steps back -- the identical
    arithmetic the original prefix-sum array performs, just windowed rather
    than materializing the whole array.
    """
    max_window = max((w for w in windows if w > 0), default=0)
    if state is None:
        n = 0
        sx = sy = sxx = syy = sxy = 0.0
        history: list[tuple[int, float, float, float, float, float]] = []
    else:
        n = state["n"]
        sx, sy, sxx, syy, sxy = (
            state["sx"],
            state["sy"],
            state["sxx"],
            state["syy"],
            state["sxy"],
        )
        history = [tuple(s) for s in state["history"]]

    out: list[dict[str, Any]] = []
    for d, x, y in new_points:
        history.append((n, sx, sy, sxx, syy, sxy))
        if max_window and len(history) > max_window:
            history.pop(0)

        n += 1
        sx += x
        sy += y
        sxx += x * x
        syy += y * y
        sxy += x * y

        for w in windows:
            if w == 0:
                if n < 3:
                    continue
                corr = _corr_from_sums(n, sx, sy, sxx, syy, sxy)
                m = n
            else:
                if n < w:
                    continue
                lo = history[-w]
                m = w
                corr = _corr_from_sums(
                    m,
                    sx - lo[1],
                    sy - lo[2],
                    sxx - lo[3],
                    syy - lo[4],
                    sxy - lo[5],
                )
            out.append(
                {
                    "series_a": series_a,
                    "series_b": series_b,
                    "transform_a": transform_a,
                    "transform_b": transform_b,
                    "window": w,
                    "observation_date": d.isoformat(),
                    "correlation": corr,
                    "n_obs": m,
                }
            )

    new_state = {
        "n": n,
        "sx": sx,
        "sy": sy,
        "sxx": sxx,
        "syy": syy,
        "sxy": sxy,
        "history": history,
    }
    return new_state, out


# ---- lead-lag + Granger -----------------------------------------------------------


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta (Lentz's algorithm)."""
    tiny = 1e-30
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    h = d
    for m in range(1, 200):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + aa / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + aa / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-12:
            break
    return h


def _betainc(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    front = math.exp(
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log(1.0 - x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def _f_sf(f: float, d1: int, d2: int) -> float:
    """Survival function (p-value) of the F(d1, d2) distribution."""
    if f <= 0:
        return 1.0
    return _betainc(d2 / 2.0, d1 / 2.0, d2 / (d2 + d1 * f))


def _solve(a: list[list[float]], b: list[float]) -> list[float] | None:
    """Gaussian elimination with partial pivoting; None if singular."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-12:
            return None
        m[col], m[piv] = m[piv], m[col]
        for r in range(col + 1, n):
            factor = m[r][col] / m[col][col]
            for c in range(col, n + 1):
                m[r][c] -= factor * m[col][c]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (m[r][n] - sum(m[r][c] * x[c] for c in range(r + 1, n))) / m[r][r]
    return x


def _ols_rss(rows_x: list[list[float]], y: list[float]) -> float | None:
    """Residual sum of squares of OLS y ~ X (normal equations).

    A singular X'X (exactly collinear regressors — e.g. one series is a
    perfect lag of the other) falls back to a tiny ridge (λ ∝ trace/k), which
    leaves well-conditioned problems untouched but keeps the F-test defined
    on degenerate ones.
    """
    k = len(rows_x[0])
    xtx = [[sum(r[i] * r[j] for r in rows_x) for j in range(k)] for i in range(k)]
    xty = [sum(r[i] * yv for r, yv in zip(rows_x, y)) for i in range(k)]
    theta = _solve(xtx, xty)
    if theta is None:
        lam = 1e-8 * (sum(xtx[i][i] for i in range(k)) / k or 1.0)
        ridged = [
            [xtx[i][j] + (lam if i == j else 0.0) for j in range(k)] for i in range(k)
        ]
        theta = _solve(ridged, xty)
        if theta is None:
            return None
    return sum(
        (yv - sum(t * xv for t, xv in zip(theta, r))) ** 2 for r, yv in zip(rows_x, y)
    )


def _granger(
    cause: list[float], effect: list[float], p: int
) -> tuple[float | None, float | None]:
    """Granger F-test: do lags of ``cause`` improve the AR(p) of ``effect``?

    Restricted: effect_t ~ 1 + effect_{t−1..t−p}; unrestricted adds
    cause_{t−1..t−p}. Returns ``(F, p_value)``; ``(None, None)`` when there
    aren't enough observations or the design matrix is singular.
    """
    n = len(effect)
    k_u = 2 * p + 1
    m = n - p
    if m <= k_u:
        return None, None
    y = effect[p:]
    xr = [[1.0] + [effect[t - j] for j in range(1, p + 1)] for t in range(p, n)]
    xu = [
        row + [cause[t - j] for j in range(1, p + 1)] for row, t in zip(xr, range(p, n))
    ]
    rss_r, rss_u = _ols_rss(xr, y), _ols_rss(xu, y)
    if rss_r is None or rss_u is None:
        return None, None
    if rss_r <= 1e-12:
        return None, None  # effect is already perfectly self-predicted
    # A (near-)perfect unrestricted fit is overwhelming evidence, not missing
    # data — floor the residual so F comes out huge-but-finite instead of
    # dividing by zero.
    d2 = m - k_u
    f = ((rss_r - rss_u) / p) / (max(rss_u, 1e-12) / d2)
    if f < 0:
        f = 0.0
    return f, _f_sf(f, p, d2)


def _ols_fit(
    xs: list[float], ys: list[float]
) -> tuple[list[float], list[float], float] | None:
    """OLS y ~ 1 + x.  Returns (coeffs, residuals, rss) or None if singular."""
    n = len(ys)
    if n < 2:
        return None
    X = [[1.0, x] for x in xs]
    k = 2
    xtx = [[sum(r[i] * r[j] for r in X) for j in range(k)] for i in range(k)]
    xty = [sum(r[i] * yv for r, yv in zip(X, ys)) for i in range(k)]
    theta = _solve(xtx, xty)
    if theta is None:
        lam = 1e-8 * (sum(xtx[i][i] for i in range(k)) / k or 1.0)
        ridged = [
            [xtx[i][j] + (lam if i == j else 0) for j in range(k)] for i in range(k)
        ]
        theta = _solve(ridged, xty)
    if theta is None:
        return None
    resid = [ys[i] - sum(theta[j] * X[i][j] for j in range(k)) for i in range(n)]
    rss = sum(r * r for r in resid)
    return theta, resid, rss


def _chow_f_at(xs: list[float], ys: list[float], tau: int) -> float | None:
    """Chow F-statistic for breakpoint at index tau (y ~ 1 + x both sides)."""
    n = len(xs)
    k = 2
    if tau < k or (n - tau) < k or (n - 2 * k) <= 0:
        return None
    full = _ols_fit(xs, ys)
    left = _ols_fit(xs[:tau], ys[:tau])
    right = _ols_fit(xs[tau:], ys[tau:])
    if full is None or left is None or right is None:
        return None
    rss_u = left[2] + right[2]
    # If full-sample RSS is also negligible, the test is degenerate.
    if full[2] < 1e-12 and rss_u < 1e-12:
        return None
    # Both segments fit perfectly but full doesn't → overwhelming evidence of
    # a break; use a relative floor so F is finite but large.
    rss_u = max(rss_u, 1e-12 * max(full[2], 1.0))
    f = ((full[2] - rss_u) / k) / (rss_u / (n - 2 * k))
    return max(0.0, f)


def _chow_scan(
    dates: list[date],
    xs: list[float],
    ys: list[float],
    min_segment: int = 20,
) -> tuple[date | None, float | None, float | None, int, int]:
    """Scan all candidate break dates and return the one with the highest F.

    Returns (break_date, f_stat, p_value, pre_n, post_n).
    """
    n = len(dates)
    trim = max(min_segment, int(0.15 * n))
    k = 2

    # Full-sample fit once (identical to the value _chow_f_at recomputed on
    # every iteration); handles the singular/ridge case internally.
    full = _ols_fit(xs, ys)
    if full is None:
        return None, None, None, 0, 0
    full_rss = full[2]

    # Prefix sums make each candidate break's segment RSS O(1) instead of a
    # fresh O(n) OLS fit, turning the whole scan from O(n^2) into O(n). For a
    # non-singular segment the closed-form RSS = Syy - b0*Sy - b1*Sxy is
    # algebraically identical to summing squared residuals; near-constant
    # (singular) segments fall back to the exact _ols_fit ridge path.
    px = [0.0] * (n + 1)
    py = [0.0] * (n + 1)
    pxx = [0.0] * (n + 1)
    pxy = [0.0] * (n + 1)
    pyy = [0.0] * (n + 1)
    for i in range(n):
        xi, yi = xs[i], ys[i]
        px[i + 1] = px[i] + xi
        py[i + 1] = py[i] + yi
        pxx[i + 1] = pxx[i] + xi * xi
        pxy[i + 1] = pxy[i] + xi * yi
        pyy[i + 1] = pyy[i] + yi * yi

    def _seg_rss(a: int, b: int) -> float | None:
        m = b - a
        if m < k:
            return None
        sx = px[b] - px[a]
        sy = py[b] - py[a]
        sxx = pxx[b] - pxx[a]
        sxy = pxy[b] - pxy[a]
        syy = pyy[b] - pyy[a]
        det = m * sxx - sx * sx
        if abs(det) <= 1e-12 * (abs(m * sxx) + 1.0):
            fit = _ols_fit(xs[a:b], ys[a:b])  # singular → exact ridge path
            return None if fit is None else fit[2]
        b1 = (m * sxy - sx * sy) / det
        b0 = (sy - b1 * sx) / m
        rss = syy - b0 * sy - b1 * sxy
        return max(0.0, rss)

    best_tau: int | None = None
    best_f: float | None = None
    for tau in range(trim, n - trim + 1):
        if tau < k or (n - tau) < k or (n - 2 * k) <= 0:
            continue
        left_rss = _seg_rss(0, tau)
        right_rss = _seg_rss(tau, n)
        if left_rss is None or right_rss is None:
            continue
        rss_u = left_rss + right_rss
        if full_rss < 1e-12 and rss_u < 1e-12:
            continue
        rss_u = max(rss_u, 1e-12 * max(full_rss, 1.0))
        f = ((full_rss - rss_u) / k) / (rss_u / (n - 2 * k))
        f = max(0.0, f)
        if best_f is None or f > best_f:
            best_f = f
            best_tau = tau
    if best_tau is None or best_f is None:
        return None, None, None, 0, 0
    d2 = n - 4
    p = _f_sf(best_f, k, d2) if d2 > 0 else None
    return dates[best_tau - 1], best_f, p, best_tau, n - best_tau


def _cusum_scan(
    dates: list[date],
    xs: list[float],
    ys: list[float],
) -> tuple[date | None, float, float]:
    """CUSUM of full-sample OLS residuals (Brown-Durbin-Evans proxy).

    Normalises residuals by the residual standard deviation, accumulates
    the CUSUM, and uses the Kolmogorov-Smirnov survival function
    ``p ≈ 2·exp(−2·(max|CUSUM|/√n)²)`` as the p-value approximation.
    The 5% boundary is max|CUSUM| / √n > 1.358.

    Returns (break_date, cusum_max, p_value) where break_date is the first
    5%-boundary crossing (or the peak |CUSUM| date if there is no crossing).
    """
    n = len(dates)
    fit = _ols_fit(xs, ys)
    if fit is None:
        return None, 0.0, 1.0
    _, resid, rss = fit
    sigma = math.sqrt(rss / max(n - 2, 1))
    if sigma < 1e-12:
        return None, 0.0, 1.0

    norm = [r / sigma for r in resid]
    boundary = 1.358 * math.sqrt(n)
    cusum = 0.0
    cusum_max = 0.0
    first_cross: int | None = None
    peak_idx = 0
    for i, w in enumerate(norm):
        cusum += w
        if abs(cusum) > cusum_max:
            cusum_max = abs(cusum)
            peak_idx = i
        if first_cross is None and abs(cusum) > boundary:
            first_cross = i

    stat = cusum_max / math.sqrt(n)
    p = min(1.0, 2.0 * math.exp(-2.0 * stat * stat)) if stat > 0 else 1.0
    chosen_idx = first_cross if first_cross is not None else peak_idx
    return dates[chosen_idx], cusum_max, p


def compute_series_structural_breaks(
    latest_rows: Iterable[dict[str, Any]],
    cfg: StatsConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.series_structural_breaks``: Chow and CUSUM structural-break
    tests on the aligned, transformed series for every configured pair.

    Two rows are emitted per pair:

    * ``test_type='chow'`` — scans all candidate break dates (trimming 15%
      from each end) and reports the break date with the highest F-statistic
      under the simple regression ``series_b ~ 1 + series_a``.
    * ``test_type='cusum'`` — CUSUM of full-sample OLS residuals (Brown-
      Durbin-Evans proxy); reports the first 5%-boundary crossing or the
      peak |CUSUM| date when there is no crossing.

    Both tests use the same ``transform_a`` / ``transform_b`` alignment as
    ``gold.series_correlation`` and ``gold.series_lead_lag``.

    ``break_date = NULL`` means the test could not be run (too few obs).
    ``is_significant = 1`` means p-value < 0.05.
    """
    if cfg is None:
        cfg = load_stats_config()
    if not cfg.pairs:
        return []
    wanted = {s for p in cfg.pairs for s in (p.series_a, p.series_b)}
    by_series = _group_sorted(r for r in latest_rows if r.get("series_id") in wanted)

    out: list[dict[str, Any]] = []
    for pair in cfg.pairs:
        dates, xs, ys = _aligned(by_series, pair)
        n = len(dates)
        if n < 10:
            continue
        as_of = dates[-1].isoformat()
        mean_a_full = sum(xs) / n if n else None
        mean_b_full = sum(ys) / n if n else None

        # ---- Chow test -------------------------------------------------------
        bd, f, p, pre_n, post_n = _chow_scan(dates, xs, ys)
        if bd is not None:
            tau = pre_n
            pre_a = sum(xs[:tau]) / tau if tau else mean_a_full
            post_a = sum(xs[tau:]) / post_n if post_n else mean_a_full
            pre_b = sum(ys[:tau]) / tau if tau else mean_b_full
            post_b = sum(ys[tau:]) / post_n if post_n else mean_b_full
        else:
            pre_a = post_a = mean_a_full
            pre_b = post_b = mean_b_full
        out.append(
            {
                "series_a": pair.series_a,
                "series_b": pair.series_b,
                "transform_a": pair.transform_a,
                "transform_b": pair.transform_b,
                "test_type": "chow",
                "break_date": bd.isoformat() if bd else None,
                "f_stat": f,
                "p_value": p,
                "pre_n": pre_n,
                "post_n": post_n,
                "pre_mean_a": pre_a,
                "post_mean_a": post_a,
                "pre_mean_b": pre_b,
                "post_mean_b": post_b,
                "cusum_max": None,
                "is_significant": int(p < 0.05) if p is not None else 0,
                "as_of_date": as_of,
            }
        )

        # ---- CUSUM test ------------------------------------------------------
        cd, cusum_max, cp = _cusum_scan(dates, xs, ys)
        if cd is not None:
            ci = dates.index(cd)
            c_pre_n = ci + 1
            c_post_n = n - c_pre_n
            c_pre_a = sum(xs[:c_pre_n]) / c_pre_n if c_pre_n else mean_a_full
            c_post_a = sum(xs[c_pre_n:]) / c_post_n if c_post_n else mean_a_full
            c_pre_b = sum(ys[:c_pre_n]) / c_pre_n if c_pre_n else mean_b_full
            c_post_b = sum(ys[c_pre_n:]) / c_post_n if c_post_n else mean_b_full
        else:
            c_pre_n = c_post_n = 0
            c_pre_a = c_post_a = mean_a_full
            c_pre_b = c_post_b = mean_b_full
        out.append(
            {
                "series_a": pair.series_a,
                "series_b": pair.series_b,
                "transform_a": pair.transform_a,
                "transform_b": pair.transform_b,
                "test_type": "cusum",
                "break_date": cd.isoformat() if cd else None,
                "f_stat": None,
                "p_value": cp,
                "pre_n": c_pre_n,
                "post_n": c_post_n,
                "pre_mean_a": c_pre_a,
                "post_mean_a": c_post_a,
                "pre_mean_b": c_pre_b,
                "post_mean_b": c_post_b,
                "cusum_max": cusum_max,
                "is_significant": int(cp < 0.05) if cp is not None else 0,
                "as_of_date": as_of,
            }
        )
    return out


def compute_series_lead_lag(
    latest_rows: Iterable[dict[str, Any]],
    cfg: StatsConfig | None = None,
) -> list[dict[str, Any]]:
    """``gold.series_lead_lag``: per pair, the full-sample cross-correlation
    at lags −max_lag..+max_lag (positive lag = ``series_a`` leads ``series_b``
    by that many observations) with the pair's ``best_lag`` (largest |CCF|)
    and both Granger directions (``granger_f_ab``: a→b) denormalized onto
    every row. ``as_of_date`` is the last common observation."""
    if cfg is None:
        cfg = load_stats_config()
    if not cfg.pairs:
        return []
    wanted = {s for p in cfg.pairs for s in (p.series_a, p.series_b)}
    by_series = _group_sorted(r for r in latest_rows if r.get("series_id") in wanted)

    out: list[dict[str, Any]] = []
    for pair in cfg.pairs:
        dates, xs, ys = _aligned(by_series, pair)
        n = len(dates)
        if n < max(cfg.max_lag + 3, 2 * cfg.granger_lags + 2):
            continue
        ccf: dict[int, tuple[float | None, int]] = {}
        for lag in range(-cfg.max_lag, cfg.max_lag + 1):
            # positive lag: a_t vs b_{t+lag}  (a leads b)
            if lag >= 0:
                a_seg, b_seg = xs[: n - lag], ys[lag:]
            else:
                a_seg, b_seg = xs[-lag:], ys[: n + lag]
            m = len(a_seg)
            corr = (
                _corr_from_sums(
                    m,
                    sum(a_seg),
                    sum(b_seg),
                    sum(v * v for v in a_seg),
                    sum(v * v for v in b_seg),
                    sum(a * b for a, b in zip(a_seg, b_seg)),
                )
                if m >= 3
                else None
            )
            ccf[lag] = (corr, m)
        defined = {k: v for k, (v, _m) in ccf.items() if v is not None}
        best_lag = max(defined, key=lambda k: abs(defined[k])) if defined else None
        f_ab, p_ab = _granger(xs, ys, cfg.granger_lags)
        f_ba, p_ba = _granger(ys, xs, cfg.granger_lags)
        for lag in sorted(ccf):
            corr, m = ccf[lag]
            out.append(
                {
                    "series_a": pair.series_a,
                    "series_b": pair.series_b,
                    "transform_a": pair.transform_a,
                    "transform_b": pair.transform_b,
                    "lag": lag,
                    "cross_correlation": corr,
                    "n_obs": m,
                    "best_lag": best_lag,
                    "granger_f_ab": f_ab,
                    "granger_p_ab": p_ab,
                    "granger_f_ba": f_ba,
                    "granger_p_ba": p_ba,
                    "as_of_date": dates[-1].isoformat(),
                }
            )
    return out
