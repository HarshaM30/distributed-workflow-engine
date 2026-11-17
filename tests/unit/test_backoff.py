from __future__ import annotations

import random
import statistics

import pytest
from hypothesis import given
from hypothesis import strategies as st

from workflow_engine.core.backoff import backoff_ceiling, backoff_delay

attempts = st.integers(min_value=1, max_value=10_000)
bases = st.floats(min_value=0, max_value=60, allow_nan=False)
caps = st.floats(min_value=0, max_value=3600, allow_nan=False)


@given(attempts, bases, caps, st.integers())
def test_full_jitter_bounds(attempt: int, base: float, cap: float, seed: int) -> None:
    delay = backoff_delay(attempt, base, cap, random.Random(seed))
    assert 0 <= delay <= backoff_ceiling(attempt, base, cap) <= cap


@given(st.integers(min_value=1, max_value=200), bases, caps)
def test_ceiling_never_shrinks(attempt: int, base: float, cap: float) -> None:
    assert backoff_ceiling(attempt + 1, base, cap) >= backoff_ceiling(attempt, base, cap)


def test_ceiling_doubles_until_cap() -> None:
    assert [backoff_ceiling(n, 1, 10) for n in range(1, 7)] == [1, 2, 4, 8, 10, 10]


def test_mean_delay_grows_with_attempts() -> None:
    rng = random.Random(42)
    means = [
        statistics.fmean(backoff_delay(n, 1, 300, rng) for _ in range(2000)) for n in range(1, 6)
    ]
    assert means == sorted(means)
    for n, mean in enumerate(means, start=1):
        assert mean == pytest.approx(backoff_ceiling(n, 1, 300) / 2, rel=0.1)


def test_delays_are_jittered() -> None:
    rng = random.Random(7)
    assert len({backoff_delay(3, 1, 300, rng) for _ in range(20)}) == 20


def test_huge_attempt_does_not_overflow() -> None:
    assert backoff_ceiling(10**9, 1, 300) == 300


def test_attempt_is_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        backoff_ceiling(0, 1, 10)
