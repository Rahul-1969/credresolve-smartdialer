"""Base interface and data models for telecom providers.

Single Responsibility:
    Define the standard contract for outbound call placement, cancellation,
    event polling, and state reconciliation across telecom providers.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class PlaceResult:
    """Result of placing a call through a telecom provider."""

    provider_call_id: str
    latency_s: float


@dataclass(frozen=True)
class ProviderEvent:
    """Asynchronous telecom event delivered by a provider."""

    provider: str
    call_id: str
    provider_call_id: str
    provider_event_id: str
    event_type: str  # One of RINGING, ANSWERED, COMPLETED, FAILED, CANCELLED
    reason: str | None
    occurred_at: float
    deliver_at: float


class ProviderError(Exception):
    """Definite failure: the call was definitely NOT placed at the provider."""


class ProviderTimeout(Exception):
    """Ambiguous failure: the call may or may not exist at the provider."""


class TelecomProvider(ABC):
    """Abstract interface that all telecom providers (mock or real) must implement."""

    name: str

    @abstractmethod
    def place_call(self, call_id: str, phone: str, now: float) -> PlaceResult:
        """Initiate an outbound call.

        MUST be idempotent on call_id (call_id serves as the idempotency key).
        """

    @abstractmethod
    def cancel_call(self, call_id: str, now: float) -> bool:
        """Cancel an in-flight call.

        Returns True only if cancelled before the borrower answered.
        """

    @abstractmethod
    def poll_events(self, now: float) -> list[ProviderEvent]:
        """Poll delivered events with deliver_at <= now, ordered by (deliver_at, seq)."""

    @abstractmethod
    def get_call_status(self, call_id: str, now: float) -> str | None:
        """Retrieve ground truth call state from the provider, or None if unknown."""
