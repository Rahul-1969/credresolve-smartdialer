"""Tests for sliding-window CircuitBreaker state transitions, probe limits, and window eviction."""

from smartdialer.providers.breaker import BreakerState, CircuitBreaker


def test_breaker_opens_on_failures_after_min_samples():
    """Breaker stays CLOSED until min_samples is reached, then opens if failure rate exceeds threshold."""
    cb = CircuitBreaker(min_samples=10, failure_rate_threshold=0.5)

    # 4 failures out of 4: min_samples (10) not reached yet -> stays CLOSED
    for i in range(4):
        cb.record_failure(now=float(i))
        assert cb.state(now=float(i)) == BreakerState.CLOSED
        assert cb.allow(now=float(i)) is True

    # 1 success: total 5 samples
    cb.record_success(now=5.0)
    assert cb.state(now=5.0) == BreakerState.CLOSED

    # 4 more failures: total 9 samples (8 failures, 1 success) -> still CLOSED
    for i in range(6, 10):
        cb.record_failure(now=float(i))
        assert cb.state(now=float(i)) == BreakerState.CLOSED

    # 10th sample is a failure -> 9 failures / 10 samples (90% >= 50%) -> OPENS!
    cb.record_failure(now=10.0)
    assert cb.state(now=10.0) == BreakerState.OPEN
    assert cb.allow(now=10.0) is False


def test_breaker_open_refuses_calls():
    """While OPEN, allow() returns False."""
    cb = CircuitBreaker(min_samples=2, failure_rate_threshold=0.5, open_s=15.0)
    cb.record_failure(now=1.0)
    cb.record_failure(now=2.0)
    assert cb.state(now=2.0) == BreakerState.OPEN

    # Stays OPEN during the cool-off period
    for t in [3.0, 5.0, 10.0, 16.9]:
        assert cb.state(now=t) == BreakerState.OPEN
        assert cb.allow(now=t) is False


def test_breaker_goes_half_open_after_open_s():
    """OPEN transitions to HALF_OPEN after open_s duration has elapsed."""
    cb = CircuitBreaker(min_samples=2, failure_rate_threshold=0.5, open_s=15.0)
    cb.record_failure(now=10.0)
    cb.record_failure(now=10.0)
    assert cb.state(now=10.0) == BreakerState.OPEN

    # Before open_s: still OPEN
    assert cb.state(now=24.9) == BreakerState.OPEN
    assert cb.allow(now=24.9) is False

    # At open_s (10 + 15 = 25.0): transitions to HALF_OPEN
    assert cb.state(now=25.0) == BreakerState.HALF_OPEN
    assert cb.allow(now=25.0) is True


def test_breaker_half_open_limits_probes():
    """In HALF_OPEN, allow admits at most half_open_max_probes in flight."""
    cb = CircuitBreaker(
        min_samples=2,
        failure_rate_threshold=0.5,
        open_s=10.0,
        half_open_max_probes=2,
    )
    cb.record_failure(now=0.0)
    cb.record_failure(now=0.0)

    # Advance to HALF_OPEN
    assert cb.state(now=10.0) == BreakerState.HALF_OPEN

    # First probe admitted
    assert cb.allow(now=10.0) is True
    # Second probe admitted
    assert cb.allow(now=10.0) is True
    # Third probe rejected (limit 2 reached)
    assert cb.allow(now=10.0) is False


def test_breaker_closes_after_enough_successes():
    """In HALF_OPEN, half_open_successes_to_close successes closes breaker and clears window."""
    cb = CircuitBreaker(
        min_samples=2,
        failure_rate_threshold=0.5,
        open_s=10.0,
        half_open_max_probes=2,
        half_open_successes_to_close=3,
    )
    cb.record_failure(now=0.0)
    cb.record_failure(now=0.0)
    assert cb.state(now=10.0) == BreakerState.HALF_OPEN

    # 1st probe
    assert cb.allow(now=10.0) is True
    cb.record_success(now=10.5)
    assert cb.state(now=10.5) == BreakerState.HALF_OPEN

    # 2nd probe
    assert cb.allow(now=11.0) is True
    cb.record_success(now=11.5)
    assert cb.state(now=11.5) == BreakerState.HALF_OPEN

    # 3rd probe
    assert cb.allow(now=12.0) is True
    cb.record_success(now=12.5)

    # 3 successes reached -> CLOSED!
    assert cb.state(now=12.5) == BreakerState.CLOSED
    assert cb.allow(now=12.5) is True


def test_breaker_probe_failure_reopens_immediately():
    """In HALF_OPEN, a single probe failure immediately reopens the breaker and resets the timer."""
    cb = CircuitBreaker(
        min_samples=2,
        failure_rate_threshold=0.5,
        open_s=10.0,
        half_open_max_probes=2,
        half_open_successes_to_close=3,
    )
    cb.record_failure(now=0.0)
    cb.record_failure(now=0.0)
    assert cb.state(now=10.0) == BreakerState.HALF_OPEN

    # 1st probe succeeds
    assert cb.allow(now=10.0) is True
    cb.record_success(now=10.5)

    # 2nd probe fails!
    assert cb.allow(now=11.0) is True
    cb.record_failure(now=11.5)

    # Immediately reopens
    assert cb.state(now=11.5) == BreakerState.OPEN
    assert cb.allow(now=11.5) is False

    # Timer was reset to opened_at=11.5 -> remains OPEN at 21.0
    assert cb.state(now=21.0) == BreakerState.OPEN
    # At 21.5 (11.5 + 10.0): goes HALF_OPEN again
    assert cb.state(now=21.5) == BreakerState.HALF_OPEN


def test_old_failures_slide_out_of_window():
    """Failures older than window_s are evicted, preventing spurious trips."""
    cb = CircuitBreaker(window_s=30.0, min_samples=10, failure_rate_threshold=0.5)

    # 5 failures at t=0.0
    for _ in range(5):
        cb.record_failure(now=0.0)

    # Advance time to t=35.0 (older than 30s window)
    # Record 10 successes at t=35.0
    for _ in range(10):
        cb.record_success(now=35.0)

    # Total samples in window is 10 (the 5 old failures were evicted)
    assert cb.state(now=35.0) == BreakerState.CLOSED
    assert cb.allow(now=35.0) is True

