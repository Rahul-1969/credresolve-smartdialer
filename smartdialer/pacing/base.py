"""Base definitions and message contracts for pacing engines.

Single Responsibility:
    Define pacing modes and the immutable request contract emitted by pacing algorithms.

Architecture Contract:
    A PacingRequest is the ONLY thing a pacing engine may output.
    It is a request, never an order. Pacing engines have no authority to initiate
    calls directly; all requests must be evaluated and authorized by the Safety Controller.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


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
