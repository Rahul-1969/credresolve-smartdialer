"""Telecom provider adapters subpackage."""

from smartdialer.providers.base import (
    PlaceResult,
    ProviderError,
    ProviderEvent,
    ProviderTimeout,
    TelecomProvider,
)
from smartdialer.providers.behavior import BorrowerBehavior, Outcome
from smartdialer.providers.breaker import BreakerState, CircuitBreaker
from smartdialer.providers.mock import MockProvider, ProviderProfile
from smartdialer.providers.provider_a import make_provider_a
from smartdialer.providers.provider_b import make_provider_b
from smartdialer.providers.router import PlaceOutcome, PlaceOutcomeKind, ProviderRouter

__all__ = [
    "BorrowerBehavior",
    "BreakerState",
    "CircuitBreaker",
    "MockProvider",
    "Outcome",
    "PlaceOutcome",
    "PlaceOutcomeKind",
    "PlaceResult",
    "ProviderError",
    "ProviderEvent",
    "ProviderProfile",
    "ProviderRouter",
    "ProviderTimeout",
    "TelecomProvider",
    "make_provider_a",
    "make_provider_b",
]
