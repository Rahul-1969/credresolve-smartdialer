"""Call state machine and idempotent event evaluation.

Single Responsibility:
    Define and enforce valid lifecycle transitions and event progression for outbound calls.
    Provide monotonic rank-based ordering guaranteeing that duplicate or out-of-order provider
    events are handled safely without corrupting call state.

Call Lifecycle and Rank Progression Rules:
    Each state has an assigned monotonic rank:
        QUEUED (0) -> RESERVED (1) -> INITIATED (2) -> RINGING (3) -> ANSWERED (4) -> CONNECTED (5) -> TERMINAL (6)

    Terminal states ({COMPLETED, FAILED, CANCELLED}) all share rank 6.

    Evaluation Matrix:
    +------------------------------------+--------------------------+---------------------+-------------------+
    | Current State                      | Target Event Type        | Condition           | Result            |
    +------------------------------------+--------------------------+---------------------+-------------------+
    | Any Terminal (COMPLETED/FAILED/...) | Any event                | Current is terminal | TERMINAL_IGNORED  |
    | Any Non-Terminal (Rank R_curr)     | Event with Rank R_target | R_target > R_curr   | APPLIED           |
    | Any Non-Terminal (Rank R_curr)     | Event with Rank R_target | R_target == R_curr  | DUPLICATE         |
    | Any Non-Terminal (Rank R_curr)     | Event with Rank R_target | R_target < R_curr   | STALE_IGNORED     |
    +------------------------------------+--------------------------+---------------------+-------------------+

    Core Transition Rules:
        1. A transition is applied only if the call is not terminal AND RANK[new] > current rank.
        2. Forward jumps are allowed (e.g. ANSWERED with no prior RINGING is valid: later state implies earlier ones).
        3. FAILED, CANCELLED, and COMPLETED are allowed from any non-terminal state.
        4. Terminal states are final: first terminal wins; later events are ignored.
        5. Same or lower rank = duplicate or stale = ignored, never an error.
"""

from enum import Enum

from smartdialer.models import CallState

RANK: dict[CallState, int] = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
}

TERMINAL: set[CallState] = {
    CallState.COMPLETED,
    CallState.FAILED,
    CallState.CANCELLED,
}


class ApplyResult(str, Enum):
    """Result classification for applying a provider event to a call."""

    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    STALE_IGNORED = "STALE_IGNORED"
    TERMINAL_IGNORED = "TERMINAL_IGNORED"
    UNKNOWN_CALL = "UNKNOWN_CALL"


def evaluate_transition(
    current_state: CallState | str,
    event_type: CallState | str,
) -> ApplyResult:
    """Evaluate whether an incoming event advances the call state machine.

    Pure function returning ApplyResult according to monotonic rank rules.
    """
    curr = CallState(current_state) if not isinstance(current_state, CallState) else current_state
    event = CallState(event_type) if not isinstance(event_type, CallState) else event_type

    if curr in TERMINAL:
        return ApplyResult.TERMINAL_IGNORED

    curr_rank = RANK[curr]
    event_rank = RANK[event]

    if event_rank > curr_rank:
        return ApplyResult.APPLIED
    if event_rank == curr_rank:
        return ApplyResult.DUPLICATE
    return ApplyResult.STALE_IGNORED
