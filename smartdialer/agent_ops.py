"""Agent lifecycle operations and loss handling.

Single Responsibility:
    Handle abrupt agent departure (agent going offline) and reconcile bound calls atomically.
"""

import sqlite3
from typing import Any

from smartdialer.calls import TERMINAL, CallState
from smartdialer.config import Config
from smartdialer.models import AgentState
from smartdialer.repo import AgentRepo, CallRepo, transaction


def agent_went_offline(
    conn: sqlite3.Connection,
    config: Config,
    agent_id: str,
    now: float,
) -> dict[str, Any]:
    """Transition an agent to OFFLINE and reconcile any bound call in ONE transaction.

    Returns:
        dict describing what occurred:
        - {'result': 'noop'} if agent missing or already OFFLINE
        - {'result': 'raced'} if conditional transition failed
        - {'result': 'offline', 'action': ...} if transition succeeded
    """
    with transaction(conn):
        cur = conn.execute(
            "SELECT id, state, reserved_call_id FROM agents WHERE id = ?;",
            (agent_id,),
        )
        agent = cur.fetchone()
        if agent is None or agent["state"] == AgentState.OFFLINE.value:
            return {"result": "noop"}

        prev_state = AgentState(agent["state"])
        call_id = agent["reserved_call_id"]

        agent_repo = AgentRepo(conn, config)
        ok = agent_repo.transition(agent_id, prev_state, AgentState.OFFLINE)
        if not ok:
            return {"result": "raced"}

        action_taken = "none"

        if call_id is not None:
            call_repo = CallRepo(conn, config)
            call = call_repo.get_call(call_id)
            if call is not None and CallState(call["state"]) not in TERMINAL:
                call_rank = call["state_rank"]
                call_state = call["state"]
                provider_call_id = call["provider_call_id"]

                if call_rank >= 4:
                    conn.execute(
                        "UPDATE calls SET abandoned = 1, updated_at = ? WHERE id = ?;",
                        (now, call_id),
                    )
                    conn.execute(
                        """
                        INSERT INTO call_actions (call_id, action, reason, created_at)
                        VALUES (?, 'TERMINATE', 'AGENT_LOST', ?)
                        ON CONFLICT(call_id, action) DO NOTHING;
                        """,
                        (call_id, now),
                    )
                    action_taken = "abandoned_terminate"
                elif call_state == CallState.RESERVED.value and provider_call_id is None:
                    call_repo.apply_event(call_id, CallState.CANCELLED, reason="AGENT_LOST", now=now)
                    action_taken = "cancelled_directly"
                else:
                    conn.execute(
                        """
                        INSERT INTO call_actions (call_id, action, reason, created_at)
                        VALUES (?, 'CANCEL', 'AGENT_LOST', ?)
                        ON CONFLICT(call_id, action) DO NOTHING;
                        """,
                        (call_id, now),
                    )
                    action_taken = "cancel_enqueued"

        return {
            "result": "offline",
            "agent_id": agent_id,
            "action": action_taken,
            "call_id": call_id,
        }

