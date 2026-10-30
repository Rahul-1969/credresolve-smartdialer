"""Predictive pacing engine implementation.

Single Responsibility:
    Calculate outbound dial count using statistical models based on historical
    answer rates, average talk time, and agent departure rates.

Design Note:
    The pacer is optimistic (it counts agents about to finish talking); the
    Safety Controller only credits available and wrap-up agents, so safety will
    often REDUCE the request. That gap is by design.
"""

from dataclasses import dataclass
import math
from typing import Any

from smartdialer.pacing.base import PacingMode, PacingRequest
from smartdialer.snapshot import SystemSnapshot


@dataclass(frozen=True)
class PredictiveConfig:
    """Configuration parameters for the predictive pacing model."""

    horizon_s: float = 15.0
    wrap_up_mean_s: float = 30.0
    default_talk_s: float = 60.0
    prior_strength: float = 20.0
    min_p: float = 0.05
    max_p: float = 0.98
    max_request: int = 500


class PredictivePacer:
    """Predictive pacing engine that forecasts agent availability over a time horizon."""

    def __init__(self, config: PredictiveConfig = PredictiveConfig()) -> None:
        self.config = config

    def propose(self, snapshot: SystemSnapshot) -> PacingRequest:
        """Propose an outbound dial batch based on historical rates and expected agent capacity."""
        # 1. Historical answer rate calculation
        if snapshot.long.resolved > 0:
            p_long = snapshot.long.answered / snapshot.long.resolved
        else:
            p_long = None

        # Cold start fallback if long-term history is missing
        if p_long is None:
            requested = snapshot.available
            rationale = {
                "cold_start": True,
                "available": snapshot.available,
                "requested": requested,
            }
            return PacingRequest(
                mode=PacingMode.PREDICTIVE,
                requested_calls=requested,
                rationale=rationale,
            )

        # 2. Bayesian blended answer rate estimate
        prior_strength = self.config.prior_strength
        p_raw = (snapshot.short.answered + (prior_strength * p_long)) / (
            snapshot.short.resolved + prior_strength
        )
        p_hat = max(self.config.min_p, min(self.config.max_p, p_raw))

        # 3. Forecast agents becoming free within horizon
        talk = snapshot.avg_talk_s if snapshot.avg_talk_s is not None else self.config.default_talk_s
        talk = max(talk, 1.0)
        finish_talk = 1.0 - math.exp(-self.config.horizon_s / talk)
        finish_wrap = 1.0 - math.exp(-self.config.horizon_s / self.config.wrap_up_mean_s)

        expected_free = max(
            0.0,
            snapshot.available
            + (snapshot.wrap_up * finish_wrap)
            + (snapshot.connected * finish_talk)
            - snapshot.answered_waiting,
        )

        # 4. Target dial volume and net request
        n_target = math.ceil(expected_free / p_hat)
        in_flight = snapshot.unbound_in_flight + snapshot.pending_predictive
        requested = max(0, min(self.config.max_request, n_target - in_flight))

        rationale: dict[str, Any] = {
            "p_long": p_long,
            "p_hat": p_hat,
            "talk": talk,
            "finish_talk": finish_talk,
            "finish_wrap": finish_wrap,
            "expected_free": expected_free,
            "n_target": n_target,
            "in_flight": in_flight,
            "requested": requested,
            "available": snapshot.available,
            "wrap_up": snapshot.wrap_up,
            "connected": snapshot.connected,
            "answered_waiting": snapshot.answered_waiting,
            "unbound_in_flight": snapshot.unbound_in_flight,
            "pending_predictive": snapshot.pending_predictive,
        }

        return PacingRequest(
            mode=PacingMode.PREDICTIVE,
            requested_calls=requested,
            rationale=rationale,
        )
