"""Pure frozen data structures representing point-in-time system operational snapshots.

Single Responsibility:
    Define immutable representations of dialer telemetry, including agent staffing states,
    in-flight call tallies, response rates across observation windows, and abandonment rates.
    NO database dependencies or imports are allowed in this module.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RateStats:
    """Answer rate statistics over a defined observation window."""

    answered: int
    resolved: int


@dataclass(frozen=True)
class AbandonStats:
    """Call abandonment statistics over a defined compliance observation window."""

    answered: int
    abandoned: int


@dataclass(frozen=True)
class SystemSnapshot:
    """Point-in-time operational snapshot of the dialer ecosystem."""

    now: float
    agents_by_state: dict[str, int]
    available: int
    wrap_up: int
    connected: int
    online: int  # All agents except OFFLINE and PAUSED
    unbound_in_flight: int
    ringing_unbound: int
    unanswered_unbound: int
    bound_in_flight: int
    answered_waiting: int
    pending_progressive: int
    pending_predictive: int
    short: RateStats
    long: RateStats
    abandon: AbandonStats
    avg_talk_s: float | None

