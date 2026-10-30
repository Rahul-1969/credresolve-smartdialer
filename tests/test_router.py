"""Tests for ProviderRouter failover policies, double-dial prevention, and circuit breaker skipping."""

import pytest

from smartdialer.providers.base import (
    PlaceResult,
    ProviderError,
    ProviderEvent,
    ProviderTimeout,
    TelecomProvider,
)
from smartdialer.providers.breaker import BreakerState, CircuitBreaker
from smartdialer.providers.router import PlaceOutcomeKind, ProviderRouter


class DummyProvider(TelecomProvider):
    """Controllable test provider for router testing."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.call_count = 0
        self.last_call_id: str | None = None
        self.mode = "success"  # "success", "error", "timeout", "timeout_then_success"

    def place_call(self, call_id: str, phone: str, now: float) -> PlaceResult:
        self.call_count += 1
        self.last_call_id = call_id

        if self.mode == "error":
            raise ProviderError(f"{self.name} definite error")
        elif self.mode == "timeout":
            raise ProviderTimeout(f"{self.name} ambiguous timeout")
        elif self.mode == "timeout_then_success":
            if self.call_count == 1:
                raise ProviderTimeout(f"{self.name} first timeout")
            return PlaceResult(provider_call_id=f"{self.name}_{call_id}", latency_s=0.1)

        return PlaceResult(provider_call_id=f"{self.name}_{call_id}", latency_s=0.1)

    def cancel_call(self, call_id: str, now: float) -> bool:
        return True

    def poll_events(self, now: float) -> list[ProviderEvent]:
        return []

    def get_call_status(self, call_id: str, now: float) -> str | None:
        return None


def test_fails_over_to_b_on_definite_error_from_a():
    """Definite ProviderError on Provider A triggers failover to Provider B."""
    prov_a = DummyProvider("prov_a")
    prov_a.mode = "error"
    prov_b = DummyProvider("prov_b")
    prov_b.mode = "success"

    router = ProviderRouter(
        providers={"prov_a": prov_a, "prov_b": prov_b},
        priority=["prov_a", "prov_b"],
        max_definite_failovers=1,
    )

    outcome = router.place("c_failover_1", "+1001", now=10.0)
    assert outcome.kind == PlaceOutcomeKind.PLACED
    assert outcome.provider == "prov_b"
    assert prov_a.call_count == 1
    assert prov_b.call_count == 1


def test_does_not_fail_over_on_timeout_assert_second_provider_never_called():
    """Double-dial prevention: ProviderTimeout on A MUST NOT fail over to B; B must never be called."""
    prov_a = DummyProvider("prov_a")
    prov_a.mode = "timeout"
    prov_b = DummyProvider("prov_b")
    prov_b.mode = "success"

    router = ProviderRouter(
        providers={"prov_a": prov_a, "prov_b": prov_b},
        priority=["prov_a", "prov_b"],
        timeout_retries=1,
    )

    outcome = router.place("c_timeout_1", "+1002", now=10.0)

    # Must NOT fail over to B
    assert outcome.kind == PlaceOutcomeKind.AMBIGUOUS
    assert outcome.provider == "prov_a"

    # Prov A was called initially + 1 timeout retry = 2 calls
    assert prov_a.call_count == 2
    # Prov B was NEVER called!
    assert prov_b.call_count == 0, "Provider B was called! Violates double-dial safety rule!"


def test_retries_same_call_id_after_timeout():
    """Router retries the same provider with the same call_id on timeout; succeeds if retry passes."""
    prov_a = DummyProvider("prov_a")
    prov_a.mode = "timeout_then_success"

    router = ProviderRouter(
        providers={"prov_a": prov_a},
        priority=["prov_a"],
        timeout_retries=1,
    )

    outcome = router.place("c_retry_ok", "+1003", now=10.0)
    assert outcome.kind == PlaceOutcomeKind.PLACED
    assert outcome.provider == "prov_a"
    assert prov_a.call_count == 2
    assert prov_a.last_call_id == "c_retry_ok"


def test_returns_ambiguous_when_unresolved():
    """When timeout retries are exhausted without resolution, returns AMBIGUOUS."""
    prov_a = DummyProvider("prov_a")
    prov_a.mode = "timeout"

    router = ProviderRouter(
        providers={"prov_a": prov_a},
        priority=["prov_a"],
        timeout_retries=2,
    )

    outcome = router.place("c_ambig", "+1004", now=10.0)
    assert outcome.kind == PlaceOutcomeKind.AMBIGUOUS
    assert outcome.provider == "prov_a"
    # Initial call + 2 retries = 3 calls
    assert prov_a.call_count == 3


def test_skips_provider_whose_breaker_is_open():
    """A provider whose circuit breaker is OPEN is skipped in priority order."""
    prov_a = DummyProvider("prov_a")
    prov_a.mode = "success"
    prov_b = DummyProvider("prov_b")
    prov_b.mode = "success"

    router = ProviderRouter(
        providers={"prov_a": prov_a, "prov_b": prov_b},
        priority=["prov_a", "prov_b"],
    )

    # Trip breaker for prov_a into OPEN state
    cb_a = router.breakers["prov_a"]
    for _ in range(10):
        cb_a.record_failure(now=0.0)
    assert cb_a.state(now=0.0) == BreakerState.OPEN

    # Place call: prov_a should be skipped; prov_b called directly
    outcome = router.place("c_skip_open", "+1005", now=0.0)
    assert outcome.kind == PlaceOutcomeKind.PLACED
    assert outcome.provider == "prov_b"
    assert prov_a.call_count == 0
    assert prov_b.call_count == 1


def test_returns_no_healthy_provider_when_all_are_open():
    """When every provider's breaker is OPEN, router returns NO_HEALTHY_PROVIDER without attempting."""
    prov_a = DummyProvider("prov_a")
    prov_b = DummyProvider("prov_b")

    router = ProviderRouter(
        providers={"prov_a": prov_a, "prov_b": prov_b},
        priority=["prov_a", "prov_b"],
    )

    # Trip both breakers into OPEN
    for name in ["prov_a", "prov_b"]:
        cb = router.breakers[name]
        for _ in range(10):
            cb.record_failure(now=0.0)
        assert cb.state(now=0.0) == BreakerState.OPEN

    outcome = router.place("c_all_open", "+1006", now=0.0)
    assert outcome.kind == PlaceOutcomeKind.NO_HEALTHY_PROVIDER
    assert prov_a.call_count == 0
    assert prov_b.call_count == 0


def test_health_reports_breaker_states():
    """health(now) returns current breaker state dictionary."""
    prov_a = DummyProvider("prov_a")
    prov_b = DummyProvider("prov_b")

    router = ProviderRouter(
        providers={"prov_a": prov_a, "prov_b": prov_b},
        priority=["prov_a", "prov_b"],
    )

    h_initial = router.health(now=0.0)
    assert h_initial == {"prov_a": BreakerState.CLOSED, "prov_b": BreakerState.CLOSED}

    for _ in range(10):
        router.breakers["prov_a"].record_failure(now=0.0)

    h_tripped = router.health(now=0.0)
    assert h_tripped["prov_a"] == BreakerState.OPEN
    assert h_tripped["prov_b"] == BreakerState.CLOSED

