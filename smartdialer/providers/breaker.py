"""In-memory sliding-window circuit breaker for telecom providers.

Single Responsibility:
    Track provider call success and failure rates over a sliding time window to
    temporarily fast-fail requests to unhealthy telecom providers.

Important Architectural Note:
    This circuit breaker is in-memory and per-worker, so each worker learns about an
    outage independently. That is acceptable because it protects performance only;
    the compliance-critical limits live in the DB-backed Safety Controller (a later task).
"""

from collections import deque
from enum import Enum


class BreakerState(str, Enum):
    """Operational states of the circuit breaker."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Pure, deterministic, in-memory circuit breaker parameterized by virtual time."""

    def __init__(
        self,
        window_s: float = 30.0,
        min_samples: int = 10,
        failure_rate_threshold: float = 0.5,
        open_s: float = 15.0,
        half_open_max_probes: int = 2,
        half_open_successes_to_close: int = 3,
    ) -> None:
        self.window_s = window_s
        self.min_samples = min_samples
        self.failure_rate_threshold = failure_rate_threshold
        self.open_s = open_s
        self.half_open_max_probes = half_open_max_probes
        self.half_open_successes_to_close = half_open_successes_to_close

        self._state: BreakerState = BreakerState.CLOSED
        self._opened_at: float | None = None
        self._samples: deque[tuple[float, bool]] = deque()  # (timestamp, is_success)
        self._probes_in_flight: int = 0
        self._half_open_successes: int = 0

    def state(self, now: float) -> BreakerState:
        """Evaluate and return the current breaker state at the given virtual time."""
        if self._state == BreakerState.OPEN:
            if self._opened_at is not None and now >= self._opened_at + self.open_s:
                self._state = BreakerState.HALF_OPEN
                self._probes_in_flight = 0
                self._half_open_successes = 0
                return BreakerState.HALF_OPEN
            return BreakerState.OPEN

        return self._state

    def allow(self, now: float) -> bool:
        """Determine whether a call attempt is permitted through this provider."""
        st = self.state(now)
        if st == BreakerState.CLOSED:
            return True
        if st == BreakerState.OPEN:
            return False
        if st == BreakerState.HALF_OPEN:
            if self._probes_in_flight < self.half_open_max_probes:
                self._probes_in_flight += 1
                return True
            return False
        return False

    def _prune_window(self, now: float) -> None:
        """Evict samples outside the sliding time window."""
        cutoff = now - self.window_s
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def record_success(self, now: float) -> None:
        """Record a successful provider interaction."""
        st = self.state(now)
        if st == BreakerState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)
            self._half_open_successes += 1
            if self._half_open_successes >= self.half_open_successes_to_close:
                # Enough consecutive successes in HALF_OPEN: close and clear window
                self._state = BreakerState.CLOSED
                self._opened_at = None
                self._probes_in_flight = 0
                self._half_open_successes = 0
                self._samples.clear()
        else:
            self._samples.append((now, True))
            self._prune_window(now)

    def record_failure(self, now: float) -> None:
        """Record a failed provider interaction."""
        st = self.state(now)
        if st == BreakerState.HALF_OPEN:
            # One failure in HALF_OPEN immediately reopens and resets the timer
            self._state = BreakerState.OPEN
            self._opened_at = now
            self._probes_in_flight = 0
            self._half_open_successes = 0
            self._samples.clear()
        elif st == BreakerState.CLOSED:
            self._samples.append((now, False))
            self._prune_window(now)

            if len(self._samples) >= self.min_samples:
                failures = sum(1 for _, ok in self._samples if not ok)
                rate = failures / len(self._samples)
                if rate >= self.failure_rate_threshold:
                    self._state = BreakerState.OPEN
                    self._opened_at = now
                    self._probes_in_flight = 0
                    self._half_open_successes = 0
        elif st == BreakerState.OPEN:
            # Already open; maintain state
            pass

