"""Domain models and data definitions for SmartDialer."""

from enum import Enum


class AgentState(str, Enum):
    """Lifecycle states for a collections agent."""

    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class BorrowerState(str, Enum):
    """Lifecycle states for a borrower in a dialing queue."""

    QUEUED = "QUEUED"
    CLAIMED = "CLAIMED"
    IN_CALL = "IN_CALL"
    DONE = "DONE"
    FAILED_PERMANENT = "FAILED_PERMANENT"


class CallState(str, Enum):
    """Lifecycle states for an outbound collections call."""

    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
