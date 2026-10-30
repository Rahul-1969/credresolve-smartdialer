"""Intelligent telecom provider router with circuit breakers and safe failover logic.

Single Responsibility:
    Route outbound calls across prioritized telecom providers, managing per-provider
    circuit breakers and enforcing the double-dial prevention safety rule:
    failover is permitted ONLY on definite ProviderError, NEVER on ProviderTimeout.

Routing Rules:
    1. Priority Order: Providers are evaluated in descending order of priority,
       skipping any provider whose circuit breaker disallows calls (e.g. OPEN).
    2. No Healthy Providers: If every provider's breaker refuses, return NO_HEALTHY_PROVIDER
       without attempting any call.
    3. Definite Failure Failover: If a provider raises ProviderError, the call was definitely
       not placed. The router records the failure and safely fails over to the next provider
       up to max_definite_failovers times.
    4. Timeout Ambiguity & Double-Dial Prevention: If a provider raises ProviderTimeout,
       the call may already have been placed. The router MUST NOT fail over to another
       provider, as doing so risks calling the borrower twice. The router retries the SAME
       provider with the SAME call_id up to timeout_retries times. If unresolved, it returns
       AMBIGUOUS.
    5. Success: On successful placement, the router records success on the provider's breaker
       and returns PLACED.
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from smartdialer.providers.base import (
    ProviderError,
    ProviderTimeout,
    TelecomProvider,
)
from smartdialer.providers.breaker import BreakerState, CircuitBreaker


class PlaceOutcomeKind(str, Enum):
    """Categorical outcomes of a call placement attempt."""

    PLACED = "PLACED"
    DEFINITE_FAILURE = "DEFINITE_FAILURE"
    AMBIGUOUS = "AMBIGUOUS"
    NO_HEALTHY_PROVIDER = "NO_HEALTHY_PROVIDER"


@dataclass(frozen=True)
class PlaceOutcome:
    """Detailed result of a routed call placement attempt."""

    kind: PlaceOutcomeKind
    provider: str | None = None
    provider_call_id: str | None = None
    error: str | None = None

    @classmethod
    def placed(cls, provider: str, provider_call_id: str) -> "PlaceOutcome":
        return cls(kind=PlaceOutcomeKind.PLACED, provider=provider, provider_call_id=provider_call_id)

    @classmethod
    def definite_failure(cls, error: str | None = None) -> "PlaceOutcome":
        return cls(kind=PlaceOutcomeKind.DEFINITE_FAILURE, error=error)

    @classmethod
    def ambiguous(cls, provider: str, error: str | None = None) -> "PlaceOutcome":
        return cls(kind=PlaceOutcomeKind.AMBIGUOUS, provider=provider, error=error)

    @classmethod
    def no_healthy_provider(cls) -> "PlaceOutcome":
        return cls(kind=PlaceOutcomeKind.NO_HEALTHY_PROVIDER)


class ProviderRouter:
    """Telecom provider router with safety-governed failover and circuit breaker protection."""

    def __init__(
        self,
        providers: dict[str, TelecomProvider],
        priority: list[str],
        breaker_factory: Callable[[], CircuitBreaker] | None = None,
        max_definite_failovers: int = 1,
        timeout_retries: int = 1,
    ) -> None:
        self.providers = providers
        self.priority = priority
        self.max_definite_failovers = max_definite_failovers
        self.timeout_retries = timeout_retries

        bf = breaker_factory if breaker_factory is not None else (lambda: CircuitBreaker())
        self.breakers: dict[str, CircuitBreaker] = {name: bf() for name in providers}

    def place(self, call_id: str, phone: str, now: float) -> PlaceOutcome:
        """Route and place an outbound call adhering to safety and circuit breaker constraints."""
        attempted_count = 0
        definite_failovers = 0

        for provider_name in self.priority:
            provider = self.providers.get(provider_name)
            if provider is None:
                continue

            breaker = self.breakers[provider_name]
            if not breaker.allow(now):
                continue

            attempted_count += 1

            try:
                res = provider.place_call(call_id, phone, now)
                breaker.record_success(now)
                return PlaceOutcome.placed(provider_name, res.provider_call_id)
            except ProviderError as e:
                breaker.record_failure(now)
                definite_failovers += 1
                if definite_failovers > self.max_definite_failovers:
                    return PlaceOutcome.definite_failure(str(e))
                # Safe to failover: call definitely was not placed at this provider
                continue
            except ProviderTimeout as e:
                breaker.record_failure(now)
                # CRITICAL SAFETY: Never fail over to another provider on timeout.
                # Retry only the same provider with the same call_id.
                for _ in range(self.timeout_retries):
                    try:
                        retry_res = provider.place_call(call_id, phone, now)
                        breaker.record_success(now)
                        return PlaceOutcome.placed(provider_name, retry_res.provider_call_id)
                    except ProviderError:
                        breaker.record_failure(now)
                    except ProviderTimeout:
                        breaker.record_failure(now)

                return PlaceOutcome.ambiguous(provider_name, str(e))

        if attempted_count == 0:
            return PlaceOutcome.no_healthy_provider()

        return PlaceOutcome.definite_failure("All eligible providers failed with ProviderError")

    def place_call(self, call_id: str, phone: str, now: float) -> PlaceOutcome:
        """Alias for place()."""
        return self.place(call_id, phone, now)

    def health(self, now: float) -> dict[str, BreakerState]:
        """Return the current circuit breaker state for all registered providers."""
        return {name: breaker.state(now) for name, breaker in self.breakers.items()}

