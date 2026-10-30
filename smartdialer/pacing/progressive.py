"""Progressive pacing engine implementation.

Single Responsibility:
    Calculate outbound dial count strictly bound to available agent capacity
    (1 call per available agent).
"""

from smartdialer.pacing.base import PacingMode, PacingRequest
from smartdialer.snapshot import SystemSnapshot


class ProgressivePacer:
    """Progressive pacing engine proposing exactly one call per available agent."""

    def propose(self, snapshot: SystemSnapshot) -> PacingRequest:
        """Propose a batch of calls matching available agent capacity."""
        requested = snapshot.available
        rationale = {
            "available": snapshot.available,
            "rule": "one call per available agent",
        }
        return PacingRequest(
            mode=PacingMode.PROGRESSIVE,
            requested_calls=requested,
            rationale=rationale,
        )
