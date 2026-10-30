"""Stateless dialer execution loop and tick processing.

Single Responsibility:
    Execute a single discrete dialer tick across safety evaluation, capacity reservation,
    outbound placement, provider event consumption, and transactional action processing.
"""

from collections import Counter
from dataclasses import dataclass, field
import sqlite3
from typing import Any

from smartdialer.actions import enqueue_action, process_actions
from smartdialer.calls import ApplyResult, CallState
from smartdialer.config import Config
from smartdialer.metrics import read_snapshot
from smartdialer.pacing.base import PacingEngine, PacingMode, PacingRequest
from smartdialer.providers.base import TelecomProvider
from smartdialer.providers.router import PlaceOutcomeKind, ProviderRouter
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo, transaction
from smartdialer.safety import SafetyController, explain


class _ShortageException(Exception):
    """Internal sentinel exception used to trigger atomic rollback on resource shortage."""


@dataclass
class TickReport:
    """Detailed operational audit report emitted at the conclusion of each dialer tick."""

    now: float
    requested: int
    approved: int
    verdict: str
    reasons: tuple[str, ...]
    binding: str
    explain: str
    created: int
    placed: int
    definite_failures: int
    no_provider: int
    ambiguous: int
    cancel_enqueued: int
    events: Counter[ApplyResult] = field(default_factory=Counter)
    actions: dict[str, int] = field(default_factory=dict)
    pacing_rationale: dict[str, Any] = field(default_factory=dict)


class Dialer:
    """Stateless dialer worker executing periodic dispatch and reconciliation ticks."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        config: Config,
        safety: SafetyController,
        router: ProviderRouter,
        providers: dict[str, TelecomProvider],
        pacer: PacingEngine,
        worker_id: str,
    ) -> None:
        self.conn = conn
        self.config = config
        self.safety = safety
        self.router = router
        self.providers = providers
        self.pacer = pacer
        self.worker_id = worker_id
        self._call_counter = 0

    def tick(self, now: float) -> TickReport:
        """Execute one complete dialer tick at virtual timestamp `now`."""
        call_repo = CallRepo(self.conn, self.config)
        borrower_repo = BorrowerRepo(self.conn, self.config)
        agent_repo = AgentRepo(self.conn, self.config)

        # 1. System snapshot & pacing proposal
        snapshot = read_snapshot(
            self.conn,
            now,
            self.safety.config.short_window_s,
            self.safety.config.long_window_s,
            self.safety.config.abandon_window_s,
        )
        request = self.pacer.propose(snapshot)

        # 2. Safety evaluation
        decision = self.safety.evaluate(self.conn, request, now)
        explain_str = explain(decision)
        binding = decision.inputs.get("binding", "NONE")

        # 3. Handle cancel excess (capacity drop or fallback reduction)
        cancel_enqueued = 0
        if decision.cancel_excess > 0:
            cur = self.conn.execute(
                """
                SELECT c.id, c.state, c.provider_call_id
                FROM calls c
                WHERE c.agent_id IS NULL
                  AND c.state IN ('RESERVED', 'INITIATED', 'RINGING')
                  AND NOT EXISTS (
                      SELECT 1 FROM call_actions ca
                      WHERE ca.call_id = c.id AND ca.action = 'CANCEL'
                  )
                ORDER BY c.created_at DESC, c.id DESC
                LIMIT ?;
                """,
                (decision.cancel_excess,),
            )
            calls_to_cancel = cur.fetchall()
            for row in calls_to_cancel:
                cid = row["id"]
                c_state = row["state"]
                prov_call_id = row["provider_call_id"]
                if c_state == CallState.RESERVED.value and prov_call_id is None:
                    call_repo.apply_event(cid, CallState.CANCELLED, reason="CAPACITY_EXCESS", now=now)
                else:
                    enqueue_action(self.conn, cid, "CANCEL", "CAPACITY_EXCESS", now)
                cancel_enqueued += 1

        # 4. Outbound call placement batch
        created = 0
        placed = 0
        definite_failures = 0
        no_provider = 0
        ambiguous = 0

        if decision.approved > 0 and decision.allotment_id is not None:
            allotment_id = decision.allotment_id
            effective_mode = decision.inputs.get("mode", PacingMode.PROGRESSIVE.value)

            for _ in range(decision.approved):
                self._call_counter += 1
                call_id = f"{self.worker_id}-{now}-{self._call_counter}"

                phone: str | None = None
                phase1_ok = False

                # PHASE 1: Atomic resource reservation
                try:
                    with transaction(self.conn):
                        # (i) Consume approved allotment unit
                        if not self.safety.consume(self.conn, allotment_id, now=now):
                            raise _ShortageException()

                        # (ii) Claim borrower
                        claimed = borrower_repo.claim_borrowers(self.worker_id, 1, now=now)
                        if not claimed:
                            raise _ShortageException()
                        borrower_id = claimed[0]
                        cur_b = self.conn.execute(
                            "SELECT phone, attempts FROM borrowers WHERE id = ?;",
                            (borrower_id,),
                        )
                        b_row = cur_b.fetchone()
                        phone = b_row["phone"]
                        attempt_num = b_row["attempts"] + 1

                        # (iii) Reserve agent if mode is PROGRESSIVE
                        bound_agent_id: str | None = None
                        if effective_mode == PacingMode.PROGRESSIVE.value:
                            reserved_agents = agent_repo.reserve_agents(
                                self.worker_id, 1, call_ids=[call_id], now=now
                            )
                            if not reserved_agents:
                                raise _ShortageException()
                            bound_agent_id = reserved_agents[0]

                        # (iv) Insert initial call record
                        call_repo.create_call(
                            call_id=call_id,
                            borrower_id=borrower_id,
                            agent_id=bound_agent_id,
                            provider=self.config.default_provider,
                            attempt=attempt_num,
                            worker_id=self.worker_id,
                        )
                        phase1_ok = True
                except _ShortageException:
                    break

                if not phase1_ok or phone is None:
                    break

                created += 1

                # PHASE 2: Telecom provider placement outside any transaction
                outcome = self.router.place(call_id, phone, now)

                # PHASE 3: Atomic placement outcome reconciliation
                with transaction(self.conn):
                    if outcome.kind == PlaceOutcomeKind.PLACED:
                        placed += 1
                        self.conn.execute(
                            "UPDATE calls SET provider = ?, updated_at = ? WHERE id = ?;",
                            (outcome.provider, now, call_id),
                        )
                        res_init = call_repo.mark_initiated(
                            call_id, outcome.provider_call_id, now=now
                        )
                        if res_init != ApplyResult.APPLIED:
                            # Call already terminal (e.g. cancelled in flight); cancel at provider
                            enqueue_action(self.conn, call_id, "CANCEL", "ORPHAN", now)

                    elif outcome.kind == PlaceOutcomeKind.DEFINITE_FAILURE:
                        definite_failures += 1
                        call_repo.apply_event(
                            call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=now
                        )

                    elif outcome.kind == PlaceOutcomeKind.NO_HEALTHY_PROVIDER:
                        no_provider += 1
                        call_repo.apply_event(
                            call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=now
                        )

                    elif outcome.kind == PlaceOutcomeKind.AMBIGUOUS:
                        ambiguous += 1
                        self.conn.execute(
                            "UPDATE calls SET provider = ?, updated_at = ? WHERE id = ?;",
                            (outcome.provider, now, call_id),
                        )
                        # Leave call in RESERVED; reaper reconciles it later

        # 5. Event pump: poll delivered telecom events across all providers
        event_tallies: Counter[ApplyResult] = Counter()
        for p_name, provider in self.providers.items():
            polled = provider.poll_events(now)
            for evt in polled:
                res = call_repo.apply_event(
                    call_id=evt.call_id,
                    event_type=evt.event_type,
                    provider_event_id=evt.provider_event_id,
                    now=now,
                    reason=evt.reason,
                )
                event_tallies[res] += 1

        # 6. Action outbox processing
        action_report = process_actions(
            self.conn,
            self.config,
            self.providers,
            self.worker_id,
            now,
        )

        return TickReport(
            now=now,
            requested=request.requested_calls,
            approved=decision.approved,
            verdict=decision.verdict,
            reasons=decision.reasons,
            binding=binding,
            explain=explain_str,
            created=created,
            placed=placed,
            definite_failures=definite_failures,
            no_provider=no_provider,
            ambiguous=ambiguous,
            cancel_enqueued=cancel_enqueued,
            events=event_tallies,
            actions=action_report,
            pacing_rationale=request.rationale,
        )
