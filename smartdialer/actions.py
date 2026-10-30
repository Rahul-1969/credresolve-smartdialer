"""Transactional outbox processor for asynchronous telecom call actions (CANCEL, TERMINATE).

Single Responsibility:
    Manage the outbox lifecycle: idempotent enqueuing, atomic batch claiming with leases,
    and reliable provider execution outside transactions with retry and give-up policies.
"""

import sqlite3
from typing import Any

from smartdialer.config import Config
from smartdialer.providers.base import ProviderError, ProviderTimeout, TelecomProvider
from smartdialer.repo import transaction


def enqueue_action(
    conn: sqlite3.Connection,
    call_id: str,
    action: str,
    reason: str | None,
    now: float,
) -> bool:
    """Idempotently enqueue a call action into the transactional outbox.

    Returns True if a new action row was inserted, False if already present.
    """
    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO call_actions (call_id, action, reason, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(call_id, action) DO NOTHING;
            """,
            (call_id, action, reason, now),
        )
        return cur.rowcount > 0


def process_actions(
    conn: sqlite3.Connection,
    config: Config,
    providers: dict[str, TelecomProvider],
    worker_id: str,
    now: float,
    limit: int = 50,
) -> dict[str, int]:
    """Process a batch of pending call actions from the transactional outbox.

    1. In ONE short transaction, atomically claims up to `limit` rows matching claim criteria.
    2. OUTSIDE any transaction, calls the appropriate provider method (cancel_call or terminate_call).
    3. On boolean result: marks done_at = now.
       On ProviderError/ProviderTimeout: releases claim (claimed_by = NULL). If attempts reach
       config.action_max_attempts, stops retrying and tallies as gave_up.
    """
    counts = {
        "claimed": 0,
        "succeeded": 0,
        "already_progressed": 0,
        "retried": 0,
        "gave_up": 0,
    }

    # 1. Atomic batch claim in a single short transaction
    with transaction(conn):
        cur = conn.execute(
            """
            UPDATE call_actions
            SET claimed_by = ?,
                claim_expires_at = ?,
                attempts = attempts + 1
            WHERE id IN (
                SELECT id FROM call_actions
                WHERE done_at IS NULL
                  AND attempts < ?
                  AND (claimed_by IS NULL OR claim_expires_at <= ?)
                ORDER BY id ASC
                LIMIT ?
            )
            RETURNING id, call_id, action, reason, attempts;
            """,
            (
                worker_id,
                now + config.action_claim_lease_s,
                config.action_max_attempts,
                now,
                limit,
            ),
        )
        claimed_rows = cur.fetchall()

    counts["claimed"] = len(claimed_rows)
    if not claimed_rows:
        return counts

    # 2. Process each claimed action outside the claim transaction
    for row in claimed_rows:
        action_id = row["id"]
        call_id = row["call_id"]
        action = row["action"]
        attempts = row["attempts"]

        # Lookup provider assigned to this call
        cur_call = conn.execute("SELECT provider FROM calls WHERE id = ?;", (call_id,))
        call_info = cur_call.fetchone()
        provider_name = call_info["provider"] if call_info else None
        provider = providers.get(provider_name) if provider_name else None

        if provider is None:
            # Cannot route to provider: release or give up
            if attempts >= config.action_max_attempts:
                counts["gave_up"] += 1
            else:
                counts["retried"] += 1
            with transaction(conn):
                conn.execute(
                    "UPDATE call_actions SET claimed_by = NULL WHERE id = ?;",
                    (action_id,),
                )
            continue

        try:
            if action == "CANCEL":
                res = provider.cancel_call(call_id, now)
            elif action == "TERMINATE":
                res = provider.terminate_call(call_id, now)
            else:
                res = False

            # Boolean result (True or False) indicates provider handled the request
            with transaction(conn):
                conn.execute(
                    "UPDATE call_actions SET done_at = ?, claimed_by = NULL WHERE id = ?;",
                    (now, action_id),
                )

            if res:
                counts["succeeded"] += 1
            else:
                counts["already_progressed"] += 1

        except (ProviderError, ProviderTimeout):
            # Transient or ambiguous provider error: release claim for retry
            if attempts >= config.action_max_attempts:
                counts["gave_up"] += 1
            else:
                counts["retried"] += 1

            with transaction(conn):
                conn.execute(
                    "UPDATE call_actions SET claimed_by = NULL WHERE id = ?;",
                    (action_id,),
                )

    return counts

