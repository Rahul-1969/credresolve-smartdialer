"""Safety Controller and capacity authorization engine.

Single Responsibility:
    Act as the deterministic, non-negotiable gatekeeper between pacing engines and
    telecom providers. Enforce absolute regulatory and statistical safety bounds
    on outbound dial volume, calculate Wilson upper bounds on response rates,
    evaluate binomial tail probabilities for overdial risk, and manage allotment leases.

Architecture & Compliance Invariants:
    1. Single source of authority: Pacing engines output PacingRequests (proposals);
       only the Safety Controller issues approved allotments.
    2. Zero override policy: There are NO configuration flags, parameters, or switches
       to toggle or circumvent safety checks. All limits are enforced universally.
    3. Fail-Closed: Any unhandled exception during evaluation terminates the request
       with an immediate REJECTED verdict and zero approved calls.
"""

from collections.abc import Callable
from dataclasses import dataclass
import json
import math
import sqlite3
import time
from typing import Any

from smartdialer.metrics import read_snapshot
from smartdialer.pacing.base import PacingMode, PacingRequest
from smartdialer.repo import transaction
from smartdialer.snapshot import SystemSnapshot

# Hard regulatory and operational invariants (NOT configurable under any circumstances)
ABS_MAX_OVERDIAL_RATIO: float = 2.0
ABS_MAX_ABANDON_RATE: float = 0.03
ABS_MAX_RISK: float = 0.05
ABS_MAX_WRAP_UP_CREDIT: float = 0.75
ABS_MIN_AGENTS_FOR_PREDICTIVE: int = 3

DEFAULT_CAMPAIGN_ID: str = "default"


@dataclass(frozen=True)
class SafetyConfig:
    """Immutable configuration for safety bounds, observation windows, and thresholds."""

    max_overdial_ratio: float = 1.5
    abandon_rate_limit: float = 0.03
    abandon_window_s: float = 600.0
    abandon_min_answered: int = 30
    fallback_cooldown_s: float = 120.0
    risk_limit: float = 0.02
    z: float = 1.645
    short_window_s: float = 60.0
    long_window_s: float = 600.0
    min_samples: int = 30
    wrap_up_credit: float = 0.5
    min_agents_for_predictive: int = 10
    max_batch: int = 200
    allotment_ttl_s: float = 10.0
    half_open_cap: int = 2

    def __post_init__(self) -> None:
        """Validate that configured values do not loosen absolute limits."""
        if self.max_overdial_ratio > ABS_MAX_OVERDIAL_RATIO:
            raise ValueError(
                f"max_overdial_ratio {self.max_overdial_ratio} exceeds absolute limit {ABS_MAX_OVERDIAL_RATIO}"
            )
        if self.abandon_rate_limit > ABS_MAX_ABANDON_RATE:
            raise ValueError(
                f"abandon_rate_limit {self.abandon_rate_limit} exceeds absolute limit {ABS_MAX_ABANDON_RATE}"
            )
        if self.risk_limit > ABS_MAX_RISK:
            raise ValueError(
                f"risk_limit {self.risk_limit} exceeds absolute limit {ABS_MAX_RISK}"
            )
        if self.wrap_up_credit > ABS_MAX_WRAP_UP_CREDIT:
            raise ValueError(
                f"wrap_up_credit {self.wrap_up_credit} exceeds absolute limit {ABS_MAX_WRAP_UP_CREDIT}"
            )
        if self.min_agents_for_predictive < ABS_MIN_AGENTS_FOR_PREDICTIVE:
            raise ValueError(
                f"min_agents_for_predictive {self.min_agents_for_predictive} is below absolute minimum {ABS_MIN_AGENTS_FOR_PREDICTIVE}"
            )


def wilson_upper(successes: int, n: int, z: float) -> float:
    """Compute the Wilson score interval upper bound for a Bernoulli parameter.

    Returns 1.0 when sample size n is 0.
    """
    if n <= 0:
        return 1.0

    phat = successes / n
    z2 = z * z
    denom = 1.0 + (z2 / n)
    center = (phat + (z2 / (2.0 * n))) / denom
    spread = (z * math.sqrt((phat * (1.0 - phat) / n) + (z2 / (4.0 * (n**2))))) / denom

    upper = center + spread
    return max(0.0, min(1.0, upper))


def binom_tail_gt(n: int, p: float, c: int) -> float:
    """Compute P(Binomial(n, p) > c) using log-gamma summation."""
    if c < 0:
        return 1.0
    if c >= n or n <= 0:
        return 0.0
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0 if c < n else 0.0

    log_p = math.log(p)
    log_1_p = math.log1p(-p)
    lgamma_n_plus_1 = math.lgamma(n + 1)

    total_prob = 0.0
    for k in range(c + 1, n + 1):
        log_comb = lgamma_n_plus_1 - math.lgamma(k + 1) - math.lgamma(n - k + 1)
        log_term = log_comb + (k * log_p) + ((n - k) * log_1_p)
        total_prob += math.exp(log_term)

    return max(0.0, min(1.0, total_prob))


def max_total_calls_within_risk(
    cap: int,
    p: float,
    risk_limit: float,
    search_limit: int,
) -> int:
    """Find the largest N <= search_limit such that P(Binomial(N, p) > cap) <= risk_limit."""
    low = 0
    high = max(0, search_limit)
    best = 0

    while low <= high:
        mid = (low + high) // 2
        tail_prob = binom_tail_gt(mid, p, cap)
        if tail_prob <= risk_limit:
            best = mid
            low = mid + 1
        else:
            high = mid - 1

    return best


@dataclass(frozen=True)
class SafetyDecision:
    """Immutable verdict emitted by the Safety Controller."""

    verdict: str  # APPROVED | REDUCED | REJECTED | FALLBACK_PROGRESSIVE
    approved: int
    requested: int
    reasons: tuple[str, ...]
    allotment_id: int | None
    cancel_excess: int
    inputs: dict[str, Any]


def explain(decision: SafetyDecision) -> str:
    """Format a single-line audit summary of a SafetyDecision."""
    inp = decision.inputs
    mode = inp.get("mode", "UNKNOWN")
    req = decision.requested
    app = decision.approved
    verd = decision.verdict
    A = inp.get("A", 0)
    credit = inp.get("credit", 0)
    D = inp.get("D", 0)
    cap = inp.get("cap", 0)
    N0 = inp.get("N0", 0)
    p_hi = inp.get("p_hi", 0.0)
    k1 = inp.get("k1", 0)
    k2 = inp.get("k2", 0)
    batch = inp.get("batch", 0)
    binding = inp.get("binding", "NONE")

    return (
        f"{mode} requested={req} approved={app} {verd} | "
        f"A={A} credit={credit} D={D} cap={cap} N0={N0} p_hi={p_hi:.2f} "
        f"k1={k1} k2={k2} batch={batch} -> binding={binding}"
    )


class SafetyController:
    """Authorizes outbound dialing batches against real-time safety invariants."""

    def __init__(
        self,
        config: SafetyConfig,
        provider_health_fn: Callable[[float], dict[str, str]],
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config
        self.provider_health_fn = provider_health_fn
        self.clock = clock

    def evaluate(
        self,
        conn: sqlite3.Connection,
        request: PacingRequest,
        now: float | None = None,
    ) -> SafetyDecision:
        """Evaluate a pacing request inside an exclusive transaction."""
        curr_time = self.clock() if now is None else now

        try:
            with transaction(conn):
                # 1. Validation
                if (
                    not hasattr(request, "mode")
                    or not isinstance(request.mode, PacingMode)
                    or not isinstance(request.requested_calls, int)
                    or isinstance(request.requested_calls, bool)
                    or request.requested_calls < 0
                ):
                    reasons = ("INVALID_REQUEST",)
                    inputs = {
                        "mode": getattr(getattr(request, "mode", None), "value", "UNKNOWN"),
                        "requested": getattr(request, "requested_calls", None),
                        "binding": "INVALID_REQUEST",
                    }
                    self._record_decision(
                        conn,
                        curr_time,
                        inputs["mode"],
                        0,
                        0,
                        "REJECTED",
                        0,
                        reasons,
                        inputs,
                    )
                    return SafetyDecision(
                        verdict="REJECTED",
                        approved=0,
                        requested=0,
                        reasons=reasons,
                        allotment_id=None,
                        cancel_excess=0,
                        inputs=inputs,
                    )

                if request.requested_calls == 0:
                    inputs = {
                        "mode": request.mode.value,
                        "A": 0,
                        "wrap_up": 0,
                        "credit": 0,
                        "D": 0,
                        "cap": 0,
                        "N0": 0,
                        "p_hi": 1.0,
                        "k1": 0,
                        "k2": 0,
                        "batch": self.config.max_batch,
                        "binding": "REQUESTED",
                    }
                    self._record_decision(
                        conn,
                        curr_time,
                        request.mode.value,
                        0,
                        0,
                        "APPROVED",
                        0,
                        (),
                        inputs,
                    )
                    return SafetyDecision(
                        verdict="APPROVED",
                        approved=0,
                        requested=0,
                        reasons=(),
                        allotment_id=None,
                        cancel_excess=0,
                        inputs=inputs,
                    )

                # 2. Read snapshot independently
                snapshot = read_snapshot(
                    conn,
                    curr_time,
                    self.config.short_window_s,
                    self.config.long_window_s,
                    self.config.abandon_window_s,
                )

                # 3. Provider health checks
                health = self.provider_health_fn(curr_time)
                healthy_states = {"CLOSED", "HALF_OPEN"}
                has_any_healthy = any(st in healthy_states for st in health.values())
                if not has_any_healthy:
                    inputs = {
                        "mode": request.mode.value,
                        "A": snapshot.available,
                        "wrap_up": snapshot.wrap_up,
                        "credit": 0,
                        "D": snapshot.answered_waiting,
                        "cap": 0,
                        "N0": 0,
                        "p_hi": 1.0,
                        "k1": 0,
                        "k2": 0,
                        "batch": self.config.max_batch,
                        "binding": "ALL_PROVIDERS_UNHEALTHY",
                    }
                    reasons = ("ALL_PROVIDERS_UNHEALTHY",)
                    self._record_decision(
                        conn,
                        curr_time,
                        request.mode.value,
                        request.requested_calls,
                        0,
                        "REJECTED",
                        0,
                        reasons,
                        inputs,
                    )
                    return SafetyDecision(
                        verdict="REJECTED",
                        approved=0,
                        requested=request.requested_calls,
                        reasons=reasons,
                        allotment_id=None,
                        cancel_excess=0,
                        inputs=inputs,
                    )

                only_half_open = all(st != "CLOSED" for st in health.values())

                # 4. Abandon trip and state management
                cur_state = conn.execute(
                    "SELECT fallback_until, reason FROM safety_state WHERE campaign_id = ?;",
                    (DEFAULT_CAMPAIGN_ID,),
                )
                state_row = cur_state.fetchone()
                prior_fallback_until = state_row["fallback_until"] if state_row else 0.0

                reasons_list: list[str] = []
                abandon_data = snapshot.abandon
                tripped_now = False

                if (
                    abandon_data.answered >= self.config.abandon_min_answered
                    and (abandon_data.abandoned / abandon_data.answered)
                    >= self.config.abandon_rate_limit
                ):
                    new_fallback_until = curr_time + self.config.fallback_cooldown_s
                    conn.execute(
                        """
                        INSERT INTO safety_state (campaign_id, fallback_until, reason, updated_at)
                        VALUES (?, ?, 'ABANDON_RATE_BREACH', ?)
                        ON CONFLICT(campaign_id) DO UPDATE SET
                            fallback_until = excluded.fallback_until,
                            reason = excluded.reason,
                            updated_at = excluded.updated_at;
                        """,
                        (DEFAULT_CAMPAIGN_ID, new_fallback_until, curr_time),
                    )
                    fallback_active = True
                    tripped_now = True
                else:
                    fallback_active = curr_time < prior_fallback_until

                # 5. Effective Mode Determination
                forced_progressive = False
                if request.mode == PacingMode.PROGRESSIVE:
                    effective_mode = PacingMode.PROGRESSIVE
                elif fallback_active:
                    effective_mode = PacingMode.PROGRESSIVE
                    forced_progressive = True
                    if tripped_now:
                        reasons_list.append("ABANDON_RATE_BREACH")
                    else:
                        reasons_list.append("FALLBACK_COOLDOWN")
                elif snapshot.online < self.config.min_agents_for_predictive:
                    effective_mode = PacingMode.PROGRESSIVE
                    forced_progressive = True
                    reasons_list.append("SMALL_AGENT_POOL")
                else:
                    effective_mode = PacingMode.PREDICTIVE

                # 6. Capacity calculations
                A = snapshot.available
                D = snapshot.answered_waiting
                N0 = snapshot.unbound_in_flight + snapshot.pending_predictive

                if effective_mode == PacingMode.PROGRESSIVE:
                    credit = 0
                    cap = max(0, A - D)
                    p_hi = 1.0
                    N_risk = cap
                    k_raw = max(0, A - D - N0 - snapshot.pending_progressive)
                    k1 = k_raw
                    k2 = k_raw
                else:
                    credit = math.floor(self.config.wrap_up_credit * snapshot.wrap_up)
                    cap = max(0, A + credit - D)

                    short = snapshot.short
                    long = snapshot.long
                    if (
                        short.resolved < self.config.min_samples
                        or long.resolved < self.config.min_samples
                    ):
                        p_hi = 1.0
                        reasons_list.append("INSUFFICIENT_HISTORY")
                    else:
                        p_short = wilson_upper(short.answered, short.resolved, self.config.z)
                        p_long = wilson_upper(long.answered, long.resolved, self.config.z)
                        p_hi = max(p_short, p_long)

                    k1 = math.floor(cap * self.config.max_overdial_ratio) - N0
                    search_limit = math.floor(cap * ABS_MAX_OVERDIAL_RATIO) + 1
                    N_risk = max_total_calls_within_risk(
                        cap, p_hi, self.config.risk_limit, search_limit
                    )
                    k2 = N_risk - N0

                # Evaluate raw candidate bounds
                candidates = [
                    (request.requested_calls, "REQUESTED"),
                    (k1, "OVERDIAL_RATIO"),
                    (k2, "RISK_BOUND"),
                    (self.config.max_batch, "MAX_BATCH"),
                ]

                # Determine initial min bound
                min_val = min(c[0] for c in candidates)
                # Select the matching binding label (prefer RISK_BOUND if tied with OVERDIAL_RATIO)
                if min_val == request.requested_calls:
                    binding = "REQUESTED"
                elif min_val == k2:
                    binding = "RISK_BOUND"
                elif min_val == k1:
                    binding = "OVERDIAL_RATIO"
                else:
                    binding = "MAX_BATCH"

                k = max(0, min_val)

                if only_half_open:
                    if k > self.config.half_open_cap:
                        k = self.config.half_open_cap
                        reasons_list.append("PROVIDER_HALF_OPEN_CAP")
                        binding = "PROVIDER_HALF_OPEN_CAP"

                if request.requested_calls > self.config.max_batch and k == self.config.max_batch:
                    if "MAX_BATCH" not in reasons_list:
                        reasons_list.append("MAX_BATCH")

                # 7. Agent-drop guard
                allowed_total = min(
                    math.floor(cap * self.config.max_overdial_ratio),
                    N_risk,
                )
                excess = max(0, snapshot.unbound_in_flight - allowed_total)

                cancel_excess = 0
                if excess > 0:
                    approved = 0
                    verdict = "REJECTED"
                    reasons_list = ["AGENT_DROP"]
                    cancel_excess = min(excess, snapshot.ringing_unbound)
                    binding = "AGENT_DROP"
                else:
                    approved = k
                    # 8. Verdict
                    if forced_progressive:
                        verdict = "FALLBACK_PROGRESSIVE"
                    elif approved == request.requested_calls:
                        verdict = "APPROVED"
                    elif approved > 0:
                        verdict = "REDUCED"
                    else:
                        verdict = "REJECTED"

                inputs = {
                    "mode": effective_mode.value,
                    "A": A,
                    "wrap_up": snapshot.wrap_up,
                    "credit": credit,
                    "D": D,
                    "cap": cap,
                    "N0": N0,
                    "p_hi": p_hi,
                    "k1": k1,
                    "k2": k2,
                    "batch": self.config.max_batch,
                    "binding": binding,
                }

                # 9. Allotment insertion
                allotment_id: int | None = None
                if approved > 0:
                    expires_at = curr_time + self.config.allotment_ttl_s
                    cur_allot = conn.execute(
                        """
                        INSERT INTO safety_allotments (campaign_id, mode, approved, remaining, created_at, expires_at)
                        VALUES (?, ?, ?, ?, ?, ?);
                        """,
                        (
                            DEFAULT_CAMPAIGN_ID,
                            effective_mode.value,
                            approved,
                            approved,
                            curr_time,
                            expires_at,
                        ),
                    )
                    allotment_id = cur_allot.lastrowid

                # 10. Decision log insertion
                reasons_tuple = tuple(reasons_list)
                self._record_decision(
                    conn,
                    curr_time,
                    effective_mode.value,
                    request.requested_calls,
                    approved,
                    verdict,
                    cancel_excess,
                    reasons_tuple,
                    inputs,
                )

                return SafetyDecision(
                    verdict=verdict,
                    approved=approved,
                    requested=request.requested_calls,
                    reasons=reasons_tuple,
                    allotment_id=allotment_id,
                    cancel_excess=cancel_excess,
                    inputs=inputs,
                )

        except Exception as e:
            # Fail closed: never propagate exception; return safe REJECTED verdict
            err_reason = f"SAFETY_ERROR:{type(e).__name__}"
            err_inputs = {"error": str(e), "binding": "FAIL_CLOSED"}
            try:
                with transaction(conn):
                    self._record_decision(
                        conn,
                        curr_time,
                        getattr(
                            getattr(request, "mode", None),
                            "value",
                            str(getattr(request, "mode", "UNKNOWN")),
                        ),
                        getattr(request, "requested_calls", 0),
                        0,
                        "REJECTED",
                        0,
                        (err_reason,),
                        err_inputs,
                    )
            except Exception:
                pass

            return SafetyDecision(
                verdict="REJECTED",
                approved=0,
                requested=getattr(request, "requested_calls", 0),
                reasons=(err_reason,),
                allotment_id=None,
                cancel_excess=0,
                inputs=err_inputs,
            )

    def consume(
        self,
        conn: sqlite3.Connection,
        allotment_id: int,
        now: float | None = None,
    ) -> bool:
        """Atomically consume one approved unit from a valid, non-expired allotment."""
        curr_time = self.clock() if now is None else now
        cur = conn.execute(
            """
            UPDATE safety_allotments
            SET remaining = remaining - 1
            WHERE id = ? AND remaining > 0 AND expires_at > ?;
            """,
            (allotment_id, curr_time),
        )
        return cur.rowcount == 1

    def _record_decision(
        self,
        conn: sqlite3.Connection,
        at: float,
        mode: str,
        requested: int,
        approved: int,
        verdict: str,
        cancel_excess: int,
        reasons: tuple[str, ...],
        inputs: dict[str, Any],
    ) -> None:
        """Record evaluation outcome in safety_decisions audit table."""
        conn.execute(
            """
            INSERT INTO safety_decisions (
                campaign_id, at, mode, requested, approved, verdict, cancel_excess, reasons, inputs
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                DEFAULT_CAMPAIGN_ID,
                at,
                mode,
                requested,
                approved,
                verdict,
                cancel_excess,
                json.dumps(list(reasons)),
                json.dumps(inputs),
            ),
        )
