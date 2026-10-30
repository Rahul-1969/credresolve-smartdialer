"""Unit tests for statistical safety math: binomial tail probabilities, Wilson intervals, and risk bounds."""

import math
import pytest

from smartdialer.safety import (
    binom_tail_gt,
    max_total_calls_within_risk,
    wilson_upper,
)


def exact_binom_tail(n: int, p: float, c: int) -> float:
    """Exact computation using math.comb for validation."""
    if c < 0:
        return 1.0
    if c >= n or n <= 0:
        return 0.0
    return sum(math.comb(n, k) * (p**k) * ((1.0 - p) ** (n - k)) for k in range(c + 1, n + 1))


def test_binom_tail_gt_matches_exact_comb():
    """binom_tail_gt matches exact combinatoric calculations for small n."""
    for n in [5, 10, 15, 20]:
        for p in [0.1, 0.3, 0.5, 0.7, 0.9]:
            for c in range(-1, n + 2):
                approx = binom_tail_gt(n, p, c)
                exact = exact_binom_tail(n, p, c)
                assert math.isclose(approx, exact, abs_tol=1e-9), (
                    f"Mismatch on n={n}, p={p}, c={c}: approx={approx}, exact={exact}"
                )


def test_binom_tail_gt_is_monotone_in_n():
    """Tail probability P(Binomial(n, p) > c) is non-decreasing in n."""
    p = 0.4
    c = 8
    prev = 0.0
    for n in range(c, c + 30):
        val = binom_tail_gt(n, p, c)
        assert val >= prev - 1e-12, f"Monotonicity violation at n={n}: {val} < {prev}"
        prev = val


def test_binom_tail_gt_edge_cases():
    """Exact edge cases for p=0, p=1, c < 0, and c >= n."""
    # p <= 0
    assert binom_tail_gt(10, 0.0, 0) == 0.0
    assert binom_tail_gt(10, 0.0, 5) == 0.0
    assert binom_tail_gt(10, 0.0, -1) == 1.0

    # p >= 1
    assert binom_tail_gt(10, 1.0, 5) == 1.0
    assert binom_tail_gt(10, 1.0, 9) == 1.0
    assert binom_tail_gt(10, 1.0, 10) == 0.0

    # c >= n
    assert binom_tail_gt(10, 0.5, 10) == 0.0
    assert binom_tail_gt(10, 0.5, 15) == 0.0

    # c < 0
    assert binom_tail_gt(10, 0.5, -1) == 1.0


def test_wilson_upper_properties():
    """wilson_upper is 1.0 at n=0, always >= phat, and shrinks monotonically with sample size."""
    z = 1.645

    # 1. n=0 returns 1.0
    assert wilson_upper(0, 0, z) == 1.0

    # 2. Always >= phat
    for successes, n in [(0, 10), (3, 10), (5, 10), (10, 10), (25, 50)]:
        phat = successes / n
        upper = wilson_upper(successes, n, z)
        assert upper >= phat, f"Upper bound {upper} below phat {phat}"

    # 3. Shrinks with n for constant phat = 0.5
    u10 = wilson_upper(5, 10, z)
    u100 = wilson_upper(50, 100, z)
    u1000 = wilson_upper(500, 1000, z)
    assert u10 > u100 > u1000 > 0.5


def test_max_total_calls_within_risk_binary_search():
    """max_total_calls_within_risk finds the exact highest integer N within the risk limit."""
    cap = 15
    p = 0.5
    risk_limit = 0.02
    search_limit = 50

    best_n = max_total_calls_within_risk(cap, p, risk_limit, search_limit)
    assert binom_tail_gt(best_n, p, cap) <= risk_limit

    if best_n < search_limit:
        assert binom_tail_gt(best_n + 1, p, cap) > risk_limit

