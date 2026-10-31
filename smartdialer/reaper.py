"""Periodic reaper for stuck calls, orphaned reservations, and expired leases.

Single Responsibility:
    Reconcile distributed state drifts against telecom ground truth, recover expired
    leases, and ensure the system maintains invariants even in the face of worker crashes.
"""

import sqlite3
from typing import Any

from smartdialer.actions import enqueue_action
from smartdialer.calls import CallState
from smartdialer.config import Config
from smartdialer.models import AgentState
from smartdialer.providers.base import ProviderError, ProviderTimeout, TelecomProvider
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo, transaction


class Reaper:
    """Idempotent background janitor that audits and heals dangling states."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        config: Config,
        providers: dict[str, TelecomProvider],
    ) -> None:
        self.conn = conn
        self.config = config
        self.providers = providers

    def sweep(self, now: float) -> dict[str, int]:
        """Execute a full sweep across stale reserved calls, stuck calls, and expired leases."""
        counts = {
            "reconciled_reserved": 0,
            "reconciled_stuck": 0,
            "released_borrowers": 0,
            "released_agents": 0,
            "deferred": 0,
        }

        call_repo = CallRepo(self.conn, self.config)
        borrower_repo = BorrowerRepo(self.conn, self.config)
        agent_repo = AgentRepo(self.conn, self.config)

        # ---------------------------------------------------------
        # a) STALE RESERVED CALLS (updated_at < now - reserve_timeout_s)
        # ---------------------------------------------------------
        cur_stale = self.conn.execute(
            """
            SELECT id, borrower_id, provider, updated_at
            FROM calls
            WHERE state = 'RESERVED' AND updated_at < ?;
            """,
            (now - self.config.reserve_timeout_s,),
        )
        stale_calls = cur_stale.fetchall()

        for call in stale_calls:
            call_id = call["id"]
            borrower_id = call["borrower_id"]

            found_provider: TelecomProvider | None = None
            found_status: str | None = None
            deferred_item = False

            # Query ground truth across providers OUTSIDE transaction
            for p_name, prov in self.providers.items():
                try:
                    st = prov.get_call_status(call_id, now)
                    if st is not None:
                        found_provider = prov
                        found_status = st
                        break
                except (ProviderError, ProviderTimeout):
                    deferred_item = True

            if deferred_item and found_provider is None:
                counts["deferred"] += 1
                continue

            if found_provider is not None:
                # Call was placed at found_provider
                cur_phone = self.conn.execute(
                    "SELECT phone FROM borrowers WHERE id = ?;",
                    (borrower_id,),
                )
                b_row = cur_phone.fetchone()
                phone = b_row["phone"] if b_row else "+1000"

                try:
                    # Idempotent placement recovery
                    place_res = found_provider.place_call(call_id, phone, now)
                except (ProviderError, ProviderTimeout):
                    counts["deferred"] += 1
                    continue

                with transaction(self.conn):
                    self.conn.execute(
                        "UPDATE calls SET provider = ?, updated_at = ? WHERE id = ?;",
                        (found_provider.name, now, call_id),
                    )
                    call_repo.mark_initiated(call_id, place_res.provider_call_id, now=now)
                    if found_status in ("COMPLETED", "FAILED", "CANCELLED"):
                        reason = "RECONCILED" if found_status in ("FAILED", "CANCELLED") else None
                        call_repo.apply_event(
                            call_id, CallState(found_status), reason=reason, now=now
                        )
                counts["reconciled_reserved"] += 1

            else:
                # Found nowhere across providers: fail as provider error
                with transaction(self.conn):
                    call_repo.apply_event(
                        call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=now
                    )
                counts["reconciled_reserved"] += 1

        # ---------------------------------------------------------
        # b) STUCK CALLS (stuck_call_timeout_s / abandon_stuck_timeout_s)
        # ---------------------------------------------------------
        cur_stuck = self.conn.execute(
            """
            SELECT id, provider, state, abandoned, updated_at
            FROM calls
            WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
              AND (
                  (state IN ('INITIATED', 'RINGING', 'ANSWERED', 'CONNECTED') AND updated_at < ?)
                  OR
                  (abandoned = 1 AND updated_at < ?)
              );
            """,
            (
                now - self.config.stuck_call_timeout_s,
                now - self.config.abandon_stuck_timeout_s,
            ),
        )
        stuck_calls = cur_stuck.fetchall()

        for call in stuck_calls:
            call_id = call["id"]
            prov_name = call["provider"]
            is_abandoned = call["abandoned"] == 1
            prov = self.providers.get(prov_name) if prov_name else None

            if prov is None:
                with transaction(self.conn):
                    call_repo.apply_event(
                        call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=now
                    )
                counts["reconciled_stuck"] += 1
                continue

            # Query provider status OUTSIDE transaction
            try:
                truth = prov.get_call_status(call_id, now)
            except (ProviderError, ProviderTimeout):
                counts["deferred"] += 1
                continue

            if truth in ("COMPLETED", "FAILED", "CANCELLED"):
                reason = "RECONCILED" if truth in ("FAILED", "CANCELLED") else None
                with transaction(self.conn):
                    call_repo.apply_event(
                        call_id, CallState(truth), reason=reason, now=now
                    )
                counts["reconciled_stuck"] += 1

            elif truth is None:
                with transaction(self.conn):
                    call_repo.apply_event(
                        call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=now
                    )
                counts["reconciled_stuck"] += 1

            else:
                # Call is still active at provider: touch updated_at without changing version
                with transaction(self.conn):
                    self.conn.execute(
                        "UPDATE calls SET updated_at = ? WHERE id = ?;",
                        (now, call_id),
                    )
                if is_abandoned:
                    # Re-enqueue terminate action (idempotent ON CONFLICT)
                    enqueue_action(self.conn, call_id, "TERMINATE", "ABANDONED", now)

        # ---------------------------------------------------------
        # c) EXPIRED CLAIMS & RESERVATIONS
        # ---------------------------------------------------------
        # 1. Borrowers CLAIMED with lease_expires_at < now and no active non-terminal call
        cur_b = self.conn.execute(
            """
            SELECT b.id
            FROM borrowers b
            WHERE b.state = 'CLAIMED'
              AND b.lease_expires_at < ?
              AND NOT EXISTS (
                  SELECT 1 FROM calls c
                  WHERE c.borrower_id = b.id
                    AND c.state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
              );
            """,
            (now,),
        )
        for b_row in cur_b.fetchall():
            if borrower_repo.release_to_queued(b_row["id"], next_attempt_at=now, now=now, expect_state="CLAIMED"):
                counts["released_borrowers"] += 1

        # 2. Agents RESERVED with lease_expires_at < now and orphaned reservation
        cur_a = self.conn.execute(
            """
            SELECT a.id, a.reserved_call_id
            FROM agents a
            WHERE a.state = 'RESERVED'
              AND a.lease_expires_at < ?;
            """,
            (now,),
        )
        for a_row in cur_a.fetchall():
            aid = a_row["id"]
            cid = a_row["reserved_call_id"]
            should_release = False
            if cid is None:
                should_release = True
            else:
                c = call_repo.get_call(cid)
                if c is None or c["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
                    should_release = True

            if should_release:
                ok = agent_repo.transition(
                    aid, AgentState.RESERVED, AgentState.AVAILABLE, expect_call_id=cid
                )
                if ok:
                    counts["released_agents"] += 1

        return counts
