"""spec003 Phase 3 (Incremental Gold): correctness property tests for the
checkpointed/resumable expanding accumulators in
:mod:`fred_pipeline.writer.features`.

The core contract these primitives must satisfy: for any split point ``k``,
resuming from a checkpoint built on ``xs[:k]`` and extending with ``xs[k:]``
must reproduce the non-resumable full-recompute function's output for ``xs``
bit-for-bit. Without this, an incremental Gold rebuild could silently drift
from what a full rebuild would have produced.
"""

import random

import pytest

from fred_pipeline.writer.features import (
    _expanding_mean_std,
    init_expanding_mean_std_state,
    init_expanding_percentile_state,
    resume_expanding_mean_std,
    resume_expanding_percentile,
)
from fred_pipeline.writer.terminal_views import _expanding_percentile

# ---- expanding mean/std -----------------------------------------------------


def test_resume_mean_std_matches_full_recompute_in_one_shot():
    values = [1.0, 2.0, 3.0, 4.5, -2.0, 7.25, 0.0]
    expected_means, expected_stds = _expanding_mean_std(values)

    state = init_expanding_mean_std_state()
    state, means, stds = resume_expanding_mean_std(state, values)

    assert means == expected_means
    assert stds == expected_stds


@pytest.mark.parametrize("k", [0, 1, 2, 3, 5, 6, 7])
def test_resume_mean_std_is_exact_across_a_split_point(k):
    values = [1.0, 2.0, 3.0, 4.5, -2.0, 7.25, 0.0]
    expected_means, expected_stds = _expanding_mean_std(values)

    state = init_expanding_mean_std_state()
    state, means1, stds1 = resume_expanding_mean_std(state, values[:k])
    state, means2, stds2 = resume_expanding_mean_std(state, values[k:])

    assert means1 + means2 == expected_means
    assert stds1 + stds2 == expected_stds


def test_resume_mean_std_random_split_points_property():
    rng = random.Random(1234)
    for _ in range(50):
        n = rng.randint(1, 40)
        values = [rng.uniform(-100, 100) for _ in range(n)]
        k = rng.randint(0, n)
        expected_means, expected_stds = _expanding_mean_std(values)

        state = init_expanding_mean_std_state()
        state, means1, stds1 = resume_expanding_mean_std(state, values[:k])
        state, means2, stds2 = resume_expanding_mean_std(state, values[k:])

        assert means1 + means2 == expected_means
        assert stds1 + stds2 == expected_stds


def test_resume_mean_std_multiple_chunks():
    """Not just a single split -- three checkpoint/resume cycles in a row,
    matching how a series would actually be extended across several routine
    Gold rebuilds."""
    values = list(range(1, 21))
    values = [float(v) * 1.3 for v in values]
    expected_means, expected_stds = _expanding_mean_std(values)

    state = init_expanding_mean_std_state()
    all_means: list[float] = []
    all_stds: list[float] = []
    for chunk in (values[:3], values[3:9], values[9:11], values[11:]):
        state, means, stds = resume_expanding_mean_std(state, chunk)
        all_means += means
        all_stds += stds

    assert all_means == expected_means
    assert all_stds == expected_stds


# ---- expanding percentile ----------------------------------------------------


def test_resume_percentile_matches_full_recompute_in_one_shot():
    values = [5.0, 3.0, 8.0, 3.0, 1.0, 9.0, 5.0]
    expected = _expanding_percentile(values)

    state = init_expanding_percentile_state()
    state, out = resume_expanding_percentile(state, values)

    assert out == expected


@pytest.mark.parametrize("k", [0, 1, 2, 3, 5, 6, 7])
def test_resume_percentile_is_exact_across_a_split_point(k):
    values = [5.0, 3.0, 8.0, 3.0, 1.0, 9.0, 5.0]
    expected = _expanding_percentile(values)

    state = init_expanding_percentile_state()
    state, out1 = resume_expanding_percentile(state, values[:k])
    state, out2 = resume_expanding_percentile(state, values[k:])

    assert out1 + out2 == expected


def test_resume_percentile_random_split_points_property_with_ties():
    rng = random.Random(5678)
    for _ in range(50):
        n = rng.randint(1, 40)
        # small integer-ish range so ties are common, exercising bisect_left's
        # tie-breaking against the linear-scan reference exactly.
        values = [float(rng.randint(0, 5)) for _ in range(n)]
        k = rng.randint(0, n)
        expected = _expanding_percentile(values)

        state = init_expanding_percentile_state()
        state, out1 = resume_expanding_percentile(state, values[:k])
        state, out2 = resume_expanding_percentile(state, values[k:])

        assert out1 + out2 == expected


def test_resume_percentile_multiple_chunks():
    values = [5.0, 3.0, 8.0, 3.0, 1.0, 9.0, 5.0, 2.0, 2.0, 6.5, 0.0]
    expected = _expanding_percentile(values)

    state = init_expanding_percentile_state()
    all_out: list = []
    for chunk in (values[:2], values[2:5], values[5:6], values[6:]):
        state, out = resume_expanding_percentile(state, chunk)
        all_out += out

    assert all_out == expected


def test_resume_percentile_first_value_ever_is_none_but_not_first_in_batch():
    """The None-for-the-first-observation rule is about the *entity's* whole
    history, not "first value passed to this call" -- resuming from a
    non-empty checkpoint must not re-emit None for the first new point."""
    state = init_expanding_percentile_state()
    state, out = resume_expanding_percentile(state, [10.0])
    assert out == [None]

    state, out = resume_expanding_percentile(state, [20.0])
    assert out == [1.0]  # 20.0 is above the one prior value -> rank 1.0
