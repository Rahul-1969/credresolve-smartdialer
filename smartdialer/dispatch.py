"""Dispatch orchestrator for progressive calls.

Single Responsibility:
    Atomically pair an available agent with an eligible queued borrower and initialize
    a progressive call record in a single all-or-nothing transaction.
"""

import sqlite3
from typing import Any

from smartdialer.config import Config
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo, transaction


class _DispatchAbort(Exception):
    """Internal exception to trigger transaction rollback on partial allocation."""


def start_progressive_call(
    conn: sqlite3.Connection,
    config: Config,
    worker_id: str,
    call_id: str,
) -> dict[str, Any] | None:
    """Atomically allocate 1 borrower, 1 available agent, and create a progressive call.

    Guarantees:
        Executed in one atomic re-entrant transaction.
        If either borrower claiming or agent reservation fails, the entire transaction rolls back,
        leaving NO claimed borrower and NO reserved agent orphaned.
    """
    try:
        with transaction(conn):
            b_repo = BorrowerRepo(conn, config)
            a_repo = AgentRepo(conn, config)
            c_repo = CallRepo(conn, config)

            borrowers = b_repo.claim_borrowers(worker_id, 1)
            if not borrowers:
                raise _DispatchAbort()
            borrower_id = borrowers[0]

            agents = a_repo.reserve_agents(worker_id, 1, call_ids=[call_id])
            if not agents:
                raise _DispatchAbort()
            agent_id = agents[0]

            created = c_repo.create_call(
                call_id=call_id,
                borrower_id=borrower_id,
                agent_id=agent_id,
                provider=config.default_provider,
                worker_id=worker_id,
            )
            if not created:
                raise _DispatchAbort()

            return c_repo.get_call(call_id)
    except _DispatchAbort:
        return None

