"""Base definitions and message contracts for pacing engines.

Single Responsibility:
    Define pacing modes, request contracts, and the PacingEngine protocol.

Architecture Contract:
    A PacingRequest is the ONLY thing a pacing engine may output.
    It is a request, never an order. Pacing engines have no authority to initiate
    calls directly; all requests must be evaluated and authorized by the Safety Controller.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from smartdialer.snapshot import SystemSnapshot


class PacingMode(str, Enum):
    """Operational mode requested by a pacing engine."""

    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


@dataclass(frozen=True)
class PacingRequest:
    """Outbound call batch proposal emitted by a pacing engine.

    This is the ONLY thing a pacing engine may output. It is a request, never an order.
    """

    mode: PacingMode
    requested_calls: int
    rationale: dict[str, Any]


@runtime_checkable
class PacingEngine(Protocol):
    """Protocol for pacing engines that propose outbound dial counts based on system snapshot."""

    def propose(self, snapshot: SystemSnapshot) -> PacingRequest:
        """Propose an outbound dial batch proposal from the current snapshot."""
        ...
