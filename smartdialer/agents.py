"""Agent state machine and lifecycle transitions.

Single Responsibility:
    Define and enforce valid lifecycle states and state transitions for collections agents.

States and Rationale:
    - OFFLINE:
        The agent is disconnected, not on shift, or logged off.
        Transitions:
            -> AVAILABLE: Agent logs on and is ready to accept call reservations.
            -> PAUSED: Agent logs on directly into a paused/training state before beginning active dialing.

    - AVAILABLE:
        The agent is idle, ready, and awaiting outbound call assignment.
        Transitions:
            -> RESERVED: System locks the agent for an upcoming outbound call attempt.
            -> PAUSED: Agent initiates a break or administrative pause.
            -> OFFLINE: Agent logs off or disconnects while idle.

    - RESERVED:
        The agent is reserved by a dialer worker for an outbound call attempt.
        Transitions:
            -> DIALING: Telecom provider initiates dialing to the borrower.
            -> AVAILABLE: Call setup failed or cancelled before dialing; agent is safely returned to pool.
            -> OFFLINE: Agent abruptly disconnects or crashes during reservation.

    - DIALING:
        The telecom provider has initiated the call and is currently ringing the borrower.
        Transitions:
            -> CONNECTED: Borrower answers the call; audio bridge established between borrower and agent.
            -> AVAILABLE: Call resulted in busy, no answer, voicemail, or reject; agent is freed immediately.
            -> OFFLINE: Agent disconnects during ringing phase.

    - CONNECTED:
        Active conversation between borrower and agent.
        Transitions:
            -> WRAP_UP: Call finishes normally (either party hung up); agent begins post-call wrap-up.
            -> OFFLINE: Agent abruptly disconnects mid-call.

    - WRAP_UP:
        Post-call disposition, note-taking, and payment schedule recording.
        Transitions:
            -> AVAILABLE: Wrap-up completed; agent re-enters pool for subsequent calls.
            -> PAUSED: Agent schedules a break immediately following the call.
            -> OFFLINE: Agent ends shift following the call.

    - PAUSED:
        Agent is temporarily on break, meeting, or training.
        Transitions:
            -> AVAILABLE: Agent ends pause and resumes active dialing pool.
            -> OFFLINE: Agent logs off directly from paused state.
"""

from smartdialer.models import AgentState

ALLOWED_TRANSITIONS: dict[AgentState, set[AgentState]] = {
    AgentState.OFFLINE: {AgentState.AVAILABLE, AgentState.PAUSED},
    AgentState.AVAILABLE: {AgentState.RESERVED, AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.RESERVED: {AgentState.DIALING, AgentState.AVAILABLE, AgentState.OFFLINE},
    AgentState.DIALING: {AgentState.CONNECTED, AgentState.AVAILABLE, AgentState.OFFLINE},
    AgentState.CONNECTED: {AgentState.WRAP_UP, AgentState.OFFLINE},
    AgentState.WRAP_UP: {AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.PAUSED: {AgentState.AVAILABLE, AgentState.OFFLINE},
}


def is_allowed(from_state: AgentState | str, to_state: AgentState | str) -> bool:
    """Return True if transitioning from `from_state` to `to_state` is valid."""
    try:
        from_enum = AgentState(from_state)
        to_enum = AgentState(to_state)
    except ValueError:
        return False

    return to_enum in ALLOWED_TRANSITIONS.get(from_enum, set())
