"""Exponential backoff with full jitter: delay ~ U(0, min(cap, base * 2**(attempt - 1)))."""

from __future__ import annotations

import random

# 2**62 seconds is far past any sane cap; clamping keeps the float math from overflowing.
_MAX_EXPONENT = 62


def backoff_ceiling(attempt: int, base: float, cap: float) -> float:
    """Upper bound of the jitter window after failed attempt number `attempt` (1-based)."""
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    exponent = min(attempt - 1, _MAX_EXPONENT)
    return float(min(cap, base * 2.0**exponent))


def backoff_delay(attempt: int, base: float, cap: float, rng: random.Random | None = None) -> float:
    """Full jitter: uniform over [0, ceiling]. Spreads retries out so failures don't sync up."""
    ceiling = backoff_ceiling(attempt, base, cap)
    return (rng or random).uniform(0, ceiling)
