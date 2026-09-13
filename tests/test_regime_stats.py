"""Phase-5 regime playbook + statistical lab: config loaders, the pillar/rule
engine, rolling/expanding correlation, cross-correlation lead-lag, and the
pure-Python Granger F-test machinery."""

from __future__ import annotations

import random

import pytest

from fred_pipeline.features import _group_sorted
from fred_pipeline.regime_stats import (
    _aligned,
    _f_sf,
    _granger,
    _transformed,
    compute_macro_regime,
    compute_series_correlation,
    compute_series_lead_lag,
    init_series_correlation_state,
    resume_series_correlation,
)
from fred_pipeline.regime_stats_config import (
    RegimeCondition,
    RegimeConfig,
    RegimeInputDef,
    RegimePillarDef,
    RegimeRuleDef,
    RegimeStatsConfigError,
    StatsConfig,
    StatsPairDef,
    load_regime_config,
    load_stats_config,
)
from tests.test_terminal_views import _monthly

# ---- config loaders ---------------------------------------------------------


def test_regime_loader_missing_and_repo_file(tmp_path):
    assert load_regime_config(str(tmp_path / "nope.yml")).pillars == ()
    cfg = load_regime_config("config/regime.yml")
    assert {p.name for p in cfg.pillars} == {
        "growth",
        "inflation",
        "liquidity",
        "credit",
        "policy",
    }
    assert cfg.rules[0].name == "Liquidity-Squeeze"  # rules are ordered
    assert cfg.default_regime == "Neutral"
    # NFCI enters liquidity inverted
    liq = next(p for p in cfg.pillars if p.name == "liquidity")
    nfci = next(i for i in liq.inputs if i.series_id == "NFCI")
    assert nfci.direction == -1


@pytest.mark.parametrize(
    "body",
    [
        # missing a pillar
        ("pillars:\n  growth: {inputs: [{series_id: X}]}\n"),
        # unknown pillar
        (
            "pillars:\n  growth: {inputs: [{series_id: X}]}\n"
            "  inflation: {inputs: [{series_id: X}]}\n"
            "  liquidity: {inputs: [{series_id: X}]}\n"
            "  credit: {inputs: [{series_id: X}]}\n"
            "  policy: {inputs: [{series_id: X}]}\n"
            "  vibes: {inputs: [{series_id: X}]}\n"
        ),
        # malformed condition
        (
            "pillars:\n  growth: {inputs: [{series_id: X}]}\n"
            "  inflation: {inputs: [{series_id: X}]}\n"
            "  liquidity: {inputs: [{series_id: X}]}\n"
            "  credit: {inputs: [{series_id: X}]}\n"
            "  policy: {inputs: [{series_id: X}]}\n"
            "rules:\n  - {name: R, when: {growth: 'about zero'}}\n"
        ),
    ],
)
def test_regime_loader_rejects_malformed(tmp_path, body):
    p = tmp_path / "regime.yml"
    p.write_text(body)
    with pytest.raises(RegimeStatsConfigError):
        load_regime_config(str(p))


def test_stats_loader_missing_and_repo_file(tmp_path):
    assert load_stats_config(str(tmp_path / "nope.yml")).pairs == ()
    cfg = load_stats_config("config/stats_pairs.yml")
    assert any(p.series_a == "PERMIT" and p.series_b == "HOUST" for p in cfg.pairs)
    assert 0 in cfg.windows  # expanding window configured
    assert cfg.max_lag >= 1 and cfg.granger_lags >= 1


def test_stats_loader_rejects_self_pair_and_duplicates(tmp_path):
    p = tmp_path / "pairs.yml"
    p.write_text("pairs:\n  - {series_a: X, series_b: X}\n")
    with pytest.raises(RegimeStatsConfigError):
        load_stats_config(str(p))
    p.write_text(
        "pairs:\n  - {series_a: X, series_b: Y}\n"
        "  - {series_a: Y, series_b: X}\n"  # reversed duplicate
    )
    with pytest.raises(RegimeStatsConfigError):
        load_stats_config(str(p))


# ---- transforms ----------------------------------------------------------------


def test_transformed_variants():
    from datetime import date

    series = [(date(2024, m, 1), float(100 + m)) for m in range(1, 6)]
    assert _transformed(series, "level") == series
    diffs = _transformed(series, "diff")
    assert len(diffs) == 4 and all(v == pytest.approx(1.0) for _d, v in diffs)
    moms = _transformed(series, "mom")
    assert moms[0][1] == pytest.approx(1.0 / 101.0)
    with pytest.raises(ValueError):
        _transformed(series, "wat")


# ---- regime engine --------------------------------------------------------------


def _one_input_cfg(rules=(), default="Neutral"):
    """Every pillar driven by one level-transform series named after it."""
    pillars = tuple(
        RegimePillarDef(
            name=n,
            inputs=(RegimeInputDef(series_id=n.upper(), transform="level"),),
            composite_weight={
                "growth": 1,
                "inflation": -1,
                "liquidity": 1,
                "credit": -1,
                "policy": -1,
            }[n],
        )
        for n in ("growth", "inflation", "liquidity", "credit", "policy")
    )
    return RegimeConfig(
        pillars=pillars,
        rules=tuple(rules),
        default_regime=default,
        max_staleness_days=200,
    )


def _pillar_rows(**final_values):
    """24 months of neutral history per pillar series, then one final month
    steering each pillar's expanding z where the test wants it."""
    rows = []
    for name, final in final_values.items():
        base = [100.0, 101.0] * 12  # oscillating -> nonzero expanding std
        rows += _monthly(name.upper(), 2022, base + [final])
    return rows


def test_macro_regime_scores_and_rules():
    rules = (
        RegimeRuleDef(
            "Stagflation",
            (
                RegimeCondition("growth", "<", -0.25),
                RegimeCondition("inflation", ">", 0.5),
            ),
        ),
        RegimeRuleDef(
            "Goldilocks",
            (
                RegimeCondition("growth", ">", 0.25),
                RegimeCondition("inflation", "<", 0.0),
            ),
        ),
    )
    # growth high, inflation low, everything else neutral-ish -> Goldilocks
    rows = _pillar_rows(
        growth=110.0, inflation=95.0, liquidity=100.5, credit=100.5, policy=100.5
    )
    out = compute_macro_regime(rows, _one_input_cfg(rules))
    last = out[-1]
    assert last["growth_score"] > 0.25 and last["inflation_score"] < 0
    assert last["regime_name"] == "Goldilocks"
    assert last["regime_confidence"] is not None
    # composite is the signed mean: growth up + inflation down -> positive
    assert last["composite_score"] > 0
    # flip growth down / inflation hot -> Stagflation
    rows = _pillar_rows(
        growth=90.0, inflation=110.0, liquidity=100.5, credit=100.5, policy=100.5
    )
    out = compute_macro_regime(rows, _one_input_cfg(rules))
    assert out[-1]["regime_name"] == "Stagflation"


def test_macro_regime_rule_order_and_default():
    rules = (
        RegimeRuleDef("First", (RegimeCondition("growth", ">", -99),)),
        RegimeRuleDef("Never", (RegimeCondition("growth", ">", -99),)),
    )
    rows = _pillar_rows(
        growth=110.0, inflation=100.5, liquidity=100.5, credit=100.5, policy=100.5
    )
    out = compute_macro_regime(rows, _one_input_cfg(rules))
    assert out[-1]["regime_name"] == "First"  # ordered, first match wins
    out = compute_macro_regime(rows, _one_input_cfg(()))
    assert out[-1]["regime_name"] == "Neutral"  # no rules -> default
    assert out[-1]["regime_confidence"] is None


def test_macro_regime_requires_every_pillar():
    # 4 of 5 pillar series ingested -> no rows at all
    rows = _pillar_rows(
        growth=110.0, inflation=95.0, liquidity=100.5, credit=100.5, policy=100.5
    )
    rows = [r for r in rows if r["series_id"] != "POLICY"]
    assert compute_macro_regime(rows, _one_input_cfg(())) == []


def test_macro_regime_staleness_drops_input():
    # policy series stops printing in 2022-06; by the final date it's stale
    # (> 200 days) -> its pillar has no live input -> row not emitted.
    rows = _pillar_rows(
        growth=110.0, inflation=95.0, liquidity=100.5, credit=100.5, policy=100.5
    )
    rows = [
        r
        for r in rows
        if not (r["series_id"] == "POLICY" and r["observation_date"] > "2022-06-01")
    ]
    out = compute_macro_regime(rows, _one_input_cfg(()))
    assert out  # rows exist while policy was fresh
    assert max(r["observation_date"] for r in out) < "2023-06-01"


# ---- correlation lab -------------------------------------------------------------


def test_series_correlation_rolling_and_expanding():
    # y = 2x on common dates -> correlation 1 after differencing noise-free.
    xs = _monthly("A", 2023, [1, 2, 4, 7, 11, 16, 22])
    ys = _monthly("B", 2023, [2, 4, 8, 14, 22, 32, 44])
    cfg = StatsConfig(
        pairs=(StatsPairDef("A", "B"),), windows=(3, 0), max_lag=2, granger_lags=1
    )
    out = compute_series_correlation(xs + ys, cfg)
    w3 = [r for r in out if r["window"] == 3]
    exp = [r for r in out if r["window"] == 0]
    assert all(r["correlation"] == pytest.approx(1.0) for r in w3)
    # rolling emits once the window is full (3rd diff obs)
    assert w3[0]["observation_date"] == "2023-04-01" and w3[0]["n_obs"] == 3
    # expanding starts at 3 obs and grows
    assert [r["n_obs"] for r in exp] == [3, 4, 5, 6]
    # constant series -> undefined correlation (None), not a crash
    flat = _monthly("B", 2023, [5, 5, 5, 5, 5, 5, 5])
    out = compute_series_correlation(xs + flat, cfg)
    assert all(r["correlation"] is None for r in out)


# ---- spec003 Phase 3: resumable series correlation --------------------------


def _aligned_triples(rows, pair):
    """(date, x, y) triples exactly as compute_series_correlation would see
    them for one pair -- the input shape resume_series_correlation expects."""
    by_series = _group_sorted(rows)
    dates, xs, ys = _aligned(by_series, pair)
    return list(zip(dates, xs, ys))


def test_resume_series_correlation_matches_full_recompute_in_one_shot():
    xs = _monthly("A", 2023, [1, 2, 4, 7, 11, 16, 22])
    ys = _monthly("B", 2023, [2, 4, 8, 14, 22, 32, 44])
    pair = StatsPairDef("A", "B")
    cfg = StatsConfig(pairs=(pair,), windows=(3, 0))
    expected = compute_series_correlation(xs + ys, cfg)

    triples = _aligned_triples(xs + ys, pair)
    state = init_series_correlation_state()
    _state, out = resume_series_correlation(
        "A", "B", "diff", "diff", triples, state, cfg.windows
    )
    key = lambda r: (r["window"], r["observation_date"])
    assert sorted(out, key=key) == sorted(expected, key=key)


@pytest.mark.parametrize("k", [0, 1, 2, 3, 4, 5, 6, 7])
def test_resume_series_correlation_is_exact_across_a_split_point(k):
    xs = _monthly("A", 2023, [1, 2, 4, 7, 11, 16, 22])
    ys = _monthly("B", 2023, [2, 4, 8, 14, 22, 32, 44])
    pair = StatsPairDef("A", "B")
    cfg = StatsConfig(pairs=(pair,), windows=(3, 0))
    expected = compute_series_correlation(xs + ys, cfg)

    triples = _aligned_triples(xs + ys, pair)
    state = init_series_correlation_state()
    state, out1 = resume_series_correlation(
        "A", "B", "diff", "diff", triples[:k], state, cfg.windows
    )
    _state, out2 = resume_series_correlation(
        "A", "B", "diff", "diff", triples[k:], state, cfg.windows
    )
    key = lambda r: (r["window"], r["observation_date"])
    assert sorted(out1 + out2, key=key) == sorted(expected, key=key)


def test_resume_series_correlation_random_property_bit_for_bit():
    """The critical property test for this engine: rolling windows are
    reconstructed via prefix-sum-snapshot subtraction, not by directly
    summing a raw-value ring buffer, specifically so this holds exactly --
    not just approximately -- despite floating-point cancellation making
    the two mathematically-equal approaches numerically different."""
    rng = random.Random(2024)
    pair = StatsPairDef("A", "B")
    windows = (3, 5, 0)
    cfg = StatsConfig(pairs=(pair,), windows=windows)
    for _ in range(30):
        n = rng.randint(1, 40)
        a_vals = [rng.uniform(-1000, 1000) for _ in range(n)]
        b_vals = [rng.uniform(-1000, 1000) for _ in range(n)]
        xs = _monthly("A", 2000, a_vals)
        ys = _monthly("B", 2000, b_vals)
        expected = compute_series_correlation(xs + ys, cfg)

        triples = _aligned_triples(xs + ys, pair)
        k = rng.randint(0, len(triples))
        state = init_series_correlation_state()
        state, out1 = resume_series_correlation(
            "A", "B", "diff", "diff", triples[:k], state, windows
        )
        _state, out2 = resume_series_correlation(
            "A", "B", "diff", "diff", triples[k:], state, windows
        )
        key = lambda r: (r["window"], r["observation_date"])
        assert sorted(out1 + out2, key=key) == sorted(expected, key=key)


def test_series_lead_lag_finds_planted_lead():
    random.seed(42)
    base = [random.gauss(0, 1) for _ in range(120)]
    # B's diff equals A's diff two periods ago -> A leads B by 2.
    a_vals, b_vals = [100.0], [100.0]
    for t in range(1, 120):
        a_vals.append(a_vals[-1] + base[t])
        b_vals.append(b_vals[-1] + (base[t - 2] if t >= 2 else 0.0))
    rows = _monthly("A", 2010, a_vals) + _monthly("B", 2010, b_vals)
    cfg = StatsConfig(
        pairs=(StatsPairDef("A", "B"),), windows=(0,), max_lag=5, granger_lags=3
    )
    out = compute_series_lead_lag(rows, cfg)
    assert len(out) == 11  # lags -5..+5
    by_lag = {r["lag"]: r for r in out}
    assert by_lag[2]["cross_correlation"] == pytest.approx(1.0, abs=0.05)
    assert all(r["best_lag"] == 2 for r in out)
    # Granger: A causes B strongly; B does not cause A
    assert by_lag[0]["granger_p_ab"] < 0.01
    assert by_lag[0]["granger_p_ba"] > 0.05
    assert all(r["as_of_date"] == out[0]["as_of_date"] for r in out)


def test_series_lead_lag_too_short_emits_nothing():
    rows = _monthly("A", 2024, [1, 2, 3]) + _monthly("B", 2024, [3, 2, 1])
    cfg = StatsConfig(
        pairs=(StatsPairDef("A", "B"),), windows=(0,), max_lag=5, granger_lags=4
    )
    assert compute_series_lead_lag(rows, cfg) == []


def test_f_survival_known_values():
    assert _f_sf(4.10, 2, 10) == pytest.approx(0.05, abs=0.002)
    assert _f_sf(0.0, 3, 20) == 1.0
    assert _f_sf(100.0, 2, 50) < 1e-6


def test_granger_insufficient_data():
    assert _granger([1.0, 2.0], [2.0, 1.0], 4) == (None, None)
