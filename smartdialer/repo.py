"""Persistence repositories and transaction management for SmartDialer entities.

Single Responsibility:
    Encapsulate SQL queries and atomic state transitions for agents, borrowers, and calls.
    Guarantee atomic, race-free reservation, claim, and provider-event applications using
    SQLite transactions, SAVEPOINT re-entrancy, and conditional UPDATE ... RETURNING constructs.

Concurrency Model and Locking Documentation:
    Why explicit BEGIN IMMEDIATE:
        SQLite in WAL mode allows multiple concurrent readers but only one active writer.
        Using deferred transactions can cause deadlocks when concurrent connections attempt
        to upgrade read locks to write locks simultaneously. BEGIN IMMEDIATE acquires the
        RESERVED lock up-front, establishing a clear serial queue of writers.

    Why re-entrant SAVEPOINTs:
        Complex multi-entity operations (e.g. progressive call dispatch or provider-event application
        with agent and borrower side-effects) require nesting atomic operations inside a single
        outer transaction without losing atomicity or breaking modular repository calls.

    Why bounded retries on "database is locked":
        Under intense concurrent spikes (e.g., 16 worker threads claiming or reserving in tight loops),
        even with PRAGMA busy_timeout=5000, momentary contention on WAL index checkpointing or
        lock transitions can surface sqlite3.OperationalError: database is locked.
        A bounded retry (max 5 tries) with randomized exponential backoff absorbs transient contention
        without failing worker operations or corrupting state.
"""

from contextlib import contextmanager
import random
import sqlite3
import time
from typing import Any, Callable, TypeVar
import uuid

from smartdialer.agents import is_allowed
from smartdialer.calls import RANK, TERMINAL, ApplyResult
from smartdialer.config import Config
from smartdialer.models import AgentState, BorrowerState, CallState

T = TypeVar("T")


class IllegalTransition(Exception):
    """Raised when an illegal state machine transition is attempted."""


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Context manager for explicit immediate and re-entrant transactions.

    If conn.in_transaction is already True, uses a uniquely named SAVEPOINT.
    Otherwise executes BEGIN IMMEDIATE, committing on success or rolling back on exception.
    """
    if conn.in_transaction:
        sp_name = f"sp_{uuid.uuid4().hex}"
        conn.execute(f"SAVEPOINT {sp_name};")
        try:
            yield conn
            conn.execute(f"RELEASE {sp_name};")
        except Exception:
            try:
                conn.execute(f"ROLLBACK TO {sp_name};")
                conn.execute(f"RELEASE {sp_name};")
            except sqlite3.OperationalError:
                pass
            raise
    else:
        conn.execute("BEGIN IMMEDIATE;")
        try:
            yield conn
            conn.execute("COMMIT;")
        except Exception:
            try:
                conn.execute("ROLLBACK;")
            except sqlite3.OperationalError:
                pass
            raise


def with_retry(
    op: Callable[[], T],
    conn: sqlite3.Connection | None = None,
    max_retries: int = 5,
    base_delay: float = 0.02,
) -> T:
    """Execute operation with bounded retries for transient 'database is locked' errors.

    Only retries when NOT already inside a transaction (outermost call).
    Nested calls execute op() directly.
    """
    if conn is not None and conn.in_transaction:
        return op()

    for attempt in range(max_retries):
        try:
            return op()
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() and attempt < max_retries - 1:
                sleep_sec = base_delay * (2 ** attempt) + random.uniform(0.005, 0.02)
                time.sleep(sleep_sec)
            else:
                raise


class AgentRepo:
    """Repository managing agent persistence, atomic reservation, and lifecycle transitions."""

    def __init__(self, conn: sqlite3.Connection, config: Config | None = None) -> None:
        self.conn = conn
        self.config = config or Config()

    def upsert_agent(self, agent_id: str, state: AgentState | str = AgentState.OFFLINE) -> None:
        """Insert agent if missing; does not clobber an existing agent's state."""
        st_val = state.value if isinstance(state, AgentState) else str(state)
        now = self.config.clock()
        avail_since = now if st_val == AgentState.AVAILABLE.value else None

        def _op() -> None:
            with transaction(self.conn):
                cur = self.conn.execute(
                    """
                    INSERT INTO agents (id, state, version, available_since, updated_at)
                    VALUES (?, ?, 0, ?, ?)
                    ON CONFLICT(id) DO NOTHING
                    RETURNING id;
                    """,
                    (agent_id, st_val, avail_since, now),
                )
                if cur.fetchone() is not None:
                    self.conn.execute(
                        """
                        INSERT INTO agent_events (agent_id, from_state, to_state, at)
                        VALUES (?, NULL, ?, ?);
                        """,
                        (agent_id, st_val, now),
                    )

        with_retry(_op, self.conn)

    def reserve_agents(
        self,
        worker_id: str,
        n: int,
        call_ids: list[str] | None = None,
    ) -> list[str]:
        """Atomically reserve up to n AVAILABLE agents, longest-idle first.

        Uses a single atomic conditional UPDATE ... RETURNING statement guarded by
        AND state='AVAILABLE' to guarantee that two concurrent workers can never reserve
        the same agent.
        """
        if n <= 0:
            return []

        now = self.config.clock()
        lease_expires = now + self.config.agent_reservation_lease_seconds

        def _op() -> list[str]:
            with transaction(self.conn):
                cur = self.conn.execute(
                    """
                    UPDATE agents
                    SET state = 'RESERVED',
                        reserved_by = ?,
                        lease_expires_at = ?,
                        version = version + 1,
                        updated_at = ?
                    WHERE id IN (
                        SELECT id FROM agents
                        WHERE state = 'AVAILABLE'
                        ORDER BY available_since ASC
                        LIMIT ?
                    )
                    AND state = 'AVAILABLE'
                    RETURNING id;
                    """,
                    (worker_id, lease_expires, now, n),
                )
                reserved_ids = [row["id"] for row in cur.fetchall()]

                # Record audit trail and optional call mapping in the same transaction
                for idx, agent_id in enumerate(reserved_ids):
                    call_id = call_ids[idx] if call_ids and idx < len(call_ids) else None
                    if call_id is not None:
                        self.conn.execute(
                            "UPDATE agents SET reserved_call_id = ? WHERE id = ?;",
                            (call_id, agent_id),
                        )
                    self.conn.execute(
                        """
                        INSERT INTO agent_events (agent_id, from_state, to_state, worker_id, call_id, at)
                        VALUES (?, 'AVAILABLE', 'RESERVED', ?, ?, ?);
                        """,
                        (agent_id, worker_id, call_id, now),
                    )

                return reserved_ids

        return with_retry(_op, self.conn)

    def transition(
        self,
        agent_id: str,
        from_state: AgentState | str,
        to_state: AgentState | str,
        worker_id: str | None = None,
        call_id: str | None = None,
        expect_call_id: str | None = None,
    ) -> bool:
        """Transition an agent from from_state to to_state.

        Raises IllegalTransition if the transition violates ALLOWED_TRANSITIONS.
        Returns True if the transition was applied (rowcount == 1).
        Returns False if the agent was not in from_state or expect_call_id did not match.
        """
        from_enum = AgentState(from_state) if not isinstance(from_state, AgentState) else from_state
        to_enum = AgentState(to_state) if not isinstance(to_state, AgentState) else to_state

        if not is_allowed(from_enum, to_enum):
            raise IllegalTransition(
                f"Transition from {from_enum.value} to {to_enum.value} is not allowed"
            )

        now = self.config.clock()

        set_clauses = ["state = ?", "version = version + 1", "updated_at = ?"]
        params: list[Any] = [to_enum.value, now]

        if to_enum == AgentState.AVAILABLE:
            set_clauses.append("available_since = ?")
            params.append(now)
        else:
            set_clauses.append("available_since = NULL")

        if to_enum in (AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE):
            set_clauses.append("reserved_by = NULL")
            set_clauses.append("lease_expires_at = NULL")
            set_clauses.append("reserved_call_id = NULL")
        elif to_enum == AgentState.RESERVED:
            set_clauses.append("reserved_by = ?")
            params.append(worker_id)
            set_clauses.append("reserved_call_id = ?")
            params.append(call_id)
            set_clauses.append("lease_expires_at = ?")
            params.append(now + self.config.agent_reservation_lease_seconds)
        else:
            # DIALING, CONNECTED, WRAP_UP:
            # Keep reserved_call_id and reserved_by as is.
            # Clear lease_expires_at on DIALING/CONNECTED/WRAP_UP.
            set_clauses.append("lease_expires_at = NULL")

        where_clauses = ["id = ?", "state = ?"]
        where_params: list[Any] = [agent_id, from_enum.value]

        if expect_call_id is not None:
            where_clauses.append("reserved_call_id = ?")
            where_params.append(expect_call_id)

        params.extend(where_params)
        sql = f"UPDATE agents SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)};"

        def _op() -> bool:
            with transaction(self.conn):
                cur = self.conn.execute(sql, params)
                if cur.rowcount == 1:
                    effective_call_id = call_id or expect_call_id
                    self.conn.execute(
                        """
                        INSERT INTO agent_events (agent_id, from_state, to_state, worker_id, call_id, at)
                        VALUES (?, ?, ?, ?, ?, ?);
                        """,
                        (agent_id, from_enum.value, to_enum.value, worker_id, effective_call_id, now),
                    )
                    return True
                return False

        return with_retry(_op, self.conn)

    def get_agent(self, agent_id: str) -> dict[str, Any] | None:
        """Retrieve agent record as a dictionary or None if not found."""
        cur = self.conn.execute("SELECT * FROM agents WHERE id = ?;", (agent_id,))
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def count_by_state(self) -> dict[str, int]:
        """Return counts of agents grouped by lifecycle state."""
        counts = {state.value: 0 for state in AgentState}
        cur = self.conn.execute("SELECT state, COUNT(*) as cnt FROM agents GROUP BY state;")
        for row in cur.fetchall():
            counts[row["state"]] = row["cnt"]
        return counts


class BorrowerRepo:
    """Repository managing borrower persistence, atomic queue claiming, and release."""

    def __init__(self, conn: sqlite3.Connection, config: Config | None = None) -> None:
        self.conn = conn
        self.config = config or Config()

    def upsert_borrower(
        self,
        borrower_id: str,
        phone: str,
        state: BorrowerState | str = BorrowerState.QUEUED,
        next_attempt_at: float = 0.0,
    ) -> None:
        """Insert borrower if missing; does not clobber existing state."""
        st_val = state.value if isinstance(state, BorrowerState) else str(state)
        now = self.config.clock()

        def _op() -> None:
            with transaction(self.conn):
                self.conn.execute(
                    """
                    INSERT INTO borrowers (id, phone, state, attempts, next_attempt_at, version, updated_at)
                    VALUES (?, ?, ?, 0, ?, 0, ?)
                    ON CONFLICT(id) DO NOTHING;
                    """,
                    (borrower_id, phone, st_val, next_attempt_at, now),
                )

        with_retry(_op, self.conn)

    def enqueue_borrower(self, borrower_id: str, phone: str, next_attempt_at: float = 0.0) -> None:
        """Enqueue a borrower for dialing."""
        self.upsert_borrower(
            borrower_id=borrower_id,
            phone=phone,
            state=BorrowerState.QUEUED,
            next_attempt_at=next_attempt_at,
        )

    def claim_borrowers(self, worker_id: str, n: int) -> list[str]:
        """Atomically claim up to n QUEUED borrowers eligible for dialing (next_attempt_at <= now).

        Guaranteed race-free via conditional UPDATE ... RETURNING with AND state='QUEUED'.
        """
        if n <= 0:
            return []

        now = self.config.clock()
        lease_expires = now + self.config.borrower_claim_lease_seconds

        def _op() -> list[str]:
            with transaction(self.conn):
                cur = self.conn.execute(
                    """
                    UPDATE borrowers
                    SET state = 'CLAIMED',
                        claimed_by = ?,
                        lease_expires_at = ?,
                        version = version + 1,
                        updated_at = ?
                    WHERE id IN (
                        SELECT id FROM borrowers
                        WHERE state = 'QUEUED' AND next_attempt_at <= ?
                        ORDER BY next_attempt_at ASC
                        LIMIT ?
                    )
                    AND state = 'QUEUED'
                    RETURNING id;
                    """,
                    (worker_id, lease_expires, now, now, n),
                )
                return [row["id"] for row in cur.fetchall()]

        return with_retry(_op, self.conn)

    def release_borrower(
        self,
        borrower_id: str,
        worker_id: str,
        next_attempt_at: float,
    ) -> bool:
        """Release a claimed borrower back to QUEUED state.

        Succeeds only if the borrower is in CLAIMED state and claimed_by == worker_id.
        """
        now = self.config.clock()

        def _op() -> bool:
            with transaction(self.conn):
                cur = self.conn.execute(
                    """
                    UPDATE borrowers
                    SET state = 'QUEUED',
                        claimed_by = NULL,
                        lease_expires_at = NULL,
                        next_attempt_at = ?,
                        version = version + 1,
                        updated_at = ?
                    WHERE id = ? AND claimed_by = ? AND state = 'CLAIMED';
                    """,
                    (next_attempt_at, now, borrower_id, worker_id),
                )
                return cur.rowcount == 1

        return with_retry(_op, self.conn)

    def transition_to_in_call(self, borrower_id: str, claimed_by: str) -> bool:
        """Transition a claimed borrower to IN_CALL state."""
        now = self.config.clock()

        def _op() -> bool:
            with transaction(self.conn):
                cur = self.conn.execute(
                    """
                    UPDATE borrowers
                    SET state = 'IN_CALL',
                        version = version + 1,
                        updated_at = ?
                    WHERE id = ? AND claimed_by = ? AND state = 'CLAIMED';
                    """,
                    (now, borrower_id, claimed_by),
                )
                return cur.rowcount == 1

        return with_retry(_op, self.conn)

    def mark_done(self, borrower_id: str) -> bool:
        """Mark borrower as DONE upon successful call completion."""
        now = self.config.clock()

        def _op() -> bool:
            with transaction(self.conn):
                cur = self.conn.execute(
                    """
                    UPDATE borrowers
                    SET state = 'DONE',
                        claimed_by = NULL,
                        lease_expires_at = NULL,
                        version = version + 1,
                        updated_at = ?
                    WHERE id = ? AND state != 'DONE';
                    """,
                    (now, borrower_id),
                )
                return cur.rowcount == 1

        return with_retry(_op, self.conn)

    def record_failure(self, borrower_id: str, now: float | None = None) -> bool:
        """Record a call failure for a borrower, incrementing attempts and calculating backoff.

        If attempts reach config.max_attempts, transitions to FAILED_PERMANENT.
        Otherwise resets to QUEUED with next_attempt_at = now + config.retry_backoff_seconds * 2**(attempts-1).
        Conditional on state != 'FAILED_PERMANENT' and idempotent.
        """
        ts = self.config.clock() if now is None else now

        def _op() -> bool:
            with transaction(self.conn):
                cur = self.conn.execute("SELECT attempts, state FROM borrowers WHERE id = ?;", (borrower_id,))
                row = cur.fetchone()
                if row is None:
                    return False
                if row["state"] == BorrowerState.FAILED_PERMANENT.value:
                    return False

                new_attempts = row["attempts"] + 1
                if new_attempts >= self.config.max_attempts:
                    self.conn.execute(
                        """
                        UPDATE borrowers
                        SET state = 'FAILED_PERMANENT',
                            attempts = ?,
                            claimed_by = NULL,
                            lease_expires_at = NULL,
                            version = version + 1,
                            updated_at = ?
                        WHERE id = ?;
                        """,
                        (new_attempts, ts, borrower_id),
                    )
                else:
                    backoff = self.config.retry_backoff_seconds * (2 ** (new_attempts - 1))
                    next_at = ts + backoff
                    self.conn.execute(
                        """
                        UPDATE borrowers
                        SET state = 'QUEUED',
                            attempts = ?,
                            next_attempt_at = ?,
                            claimed_by = NULL,
                            lease_expires_at = NULL,
                            version = version + 1,
                            updated_at = ?
                        WHERE id = ?;
                        """,
                        (new_attempts, next_at, ts, borrower_id),
                    )
                return True

        return with_retry(_op, self.conn)

    def get_borrower(self, borrower_id: str) -> dict[str, Any] | None:
        """Retrieve borrower record as a dictionary or None if not found."""
        cur = self.conn.execute("SELECT * FROM borrowers WHERE id = ?;", (borrower_id,))
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def count_by_state(self) -> dict[str, int]:
        """Return counts of borrowers grouped by lifecycle state."""
        counts = {state.value: 0 for state in BorrowerState}
        cur = self.conn.execute("SELECT state, COUNT(*) as cnt FROM borrowers GROUP BY state;")
        for row in cur.fetchall():
            counts[row["state"]] = row["cnt"]
        return counts


class CallRepo:
    """Repository managing call persistence, idempotent provider-event application, and side effects."""

    def __init__(self, conn: sqlite3.Connection, config: Config | None = None) -> None:
        self.conn = conn
        self.config = config or Config()

    def create_call(
        self,
        call_id: str,
        borrower_id: str,
        agent_id: str | None,
        provider: str,
        attempt: int = 1,
        worker_id: str | None = None,
    ) -> bool:
        """Insert a call in state RESERVED rank 1. Idempotent on call_id."""
        now = self.config.clock()

        def _op() -> bool:
            with transaction(self.conn):
                cur = self.conn.execute(
                    """
                    INSERT INTO calls (id, borrower_id, agent_id, provider, state, state_rank, version, attempt, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'RESERVED', 1, 0, ?, ?, ?)
                    ON CONFLICT(id) DO NOTHING;
                    """,
                    (call_id, borrower_id, agent_id, provider, attempt, now, now),
                )
                if cur.rowcount == 0:
                    return False

                # Ensure agent records binding to this call
                if agent_id is not None:
                    self.conn.execute(
                        "UPDATE agents SET reserved_call_id = ? WHERE id = ?;",
                        (call_id, agent_id),
                    )

                # Record initial event
                self.conn.execute(
                    """
                    INSERT INTO call_events (call_id, provider_event_id, event_type, received_at, applied)
                    VALUES (?, NULL, 'RESERVED', ?, 1);
                    """,
                    (call_id, now),
                )

                # Transition borrower to IN_CALL
                if worker_id is not None:
                    BorrowerRepo(self.conn, self.config).transition_to_in_call(borrower_id, worker_id)
                else:
                    self.conn.execute(
                        """
                        UPDATE borrowers
                        SET state = 'IN_CALL', version = version + 1, updated_at = ?
                        WHERE id = ? AND state = 'CLAIMED';
                        """,
                        (now, borrower_id),
                    )

                return True

        return with_retry(_op, self.conn)

    def mark_initiated(
        self,
        call_id: str,
        provider_call_id: str,
        now: float | None = None,
    ) -> ApplyResult:
        """Set provider_call_id and transition call to INITIATED."""
        ts = self.config.clock() if now is None else now

        def _op() -> ApplyResult:
            with transaction(self.conn):
                self.conn.execute(
                    "UPDATE calls SET provider_call_id = ?, updated_at = ? WHERE id = ?;",
                    (provider_call_id, ts, call_id),
                )
                return self.apply_event(call_id, CallState.INITIATED, now=ts)

        return with_retry(_op, self.conn)

    def apply_event(
        self,
        call_id: str,
        event_type: CallState | str,
        provider_event_id: str | None = None,
        now: float | None = None,
    ) -> ApplyResult:
        """Apply a provider event to a call idempotently, executing state side effects in the same txn."""
        ts = self.config.clock() if now is None else now
        target_state = CallState(event_type) if not isinstance(event_type, CallState) else event_type
        target_rank = RANK[target_state]

        def _op() -> ApplyResult:
            with transaction(self.conn):
                # 1. Load the call first (unknown -> UNKNOWN_CALL, leaves no call_events row)
                call = self.get_call(call_id)
                if call is None:
                    return ApplyResult.UNKNOWN_CALL

                # 2. Event recording and deduplication
                event_row_id = None
                if provider_event_id is not None:
                    cur = self.conn.execute(
                        """
                        INSERT INTO call_events (call_id, provider_event_id, event_type, received_at, applied)
                        VALUES (?, ?, ?, ?, 0)
                        ON CONFLICT(call_id, provider_event_id) DO NOTHING;
                        """,
                        (call_id, provider_event_id, target_state.value, ts),
                    )
                    if cur.rowcount == 0:
                        # Row already existed with this provider_event_id -> DUPLICATE
                        return ApplyResult.DUPLICATE
                    event_row_id = cur.lastrowid
                else:
                    cur = self.conn.execute(
                        """
                        INSERT INTO call_events (call_id, provider_event_id, event_type, received_at, applied)
                        VALUES (?, NULL, ?, ?, 0);
                        """,
                        (call_id, target_state.value, ts),
                    )
                    event_row_id = cur.lastrowid

                curr_state = CallState(call["state"])
                curr_rank = call["state_rank"]

                # 3. Conditional state update
                cur_upd = self.conn.execute(
                    """
                    UPDATE calls
                    SET state = ?,
                        state_rank = ?,
                        version = version + 1,
                        updated_at = ?
                    WHERE id = ?
                      AND state_rank < ?
                      AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
                    """,
                    (target_state.value, target_rank, ts, call_id, target_rank),
                )

                if cur_upd.rowcount == 0:
                    if curr_state in TERMINAL:
                        return ApplyResult.TERMINAL_IGNORED
                    if target_rank == curr_rank:
                        return ApplyResult.DUPLICATE
                    return ApplyResult.STALE_IGNORED

                # Transition applied: mark event row as applied
                if event_row_id is not None:
                    self.conn.execute(
                        "UPDATE call_events SET applied = 1 WHERE id = ?;",
                        (event_row_id,),
                    )

                # 4. Atomic side effects on agent and borrower (guarded by expect_call_id=call_id)
                agent_id = call["agent_id"]
                borrower_id = call["borrower_id"]
                agent_repo = AgentRepo(self.conn, self.config)
                borrower_repo = BorrowerRepo(self.conn, self.config)

                if target_state == CallState.INITIATED:
                    if agent_id:
                        agent_repo.transition(
                            agent_id,
                            AgentState.RESERVED,
                            AgentState.DIALING,
                            call_id=call_id,
                            expect_call_id=call_id,
                        )

                elif target_state in (CallState.ANSWERED, CallState.CONNECTED):
                    if agent_id:
                        applied_agent = agent_repo.transition(
                            agent_id,
                            AgentState.DIALING,
                            AgentState.CONNECTED,
                            call_id=call_id,
                            expect_call_id=call_id,
                        )
                        if not applied_agent:
                            # Forward jump: if agent still in RESERVED, transition RESERVED->DIALING then DIALING->CONNECTED
                            if agent_repo.transition(
                                agent_id,
                                AgentState.RESERVED,
                                AgentState.DIALING,
                                call_id=call_id,
                                expect_call_id=call_id,
                            ):
                                agent_repo.transition(
                                    agent_id,
                                    AgentState.DIALING,
                                    AgentState.CONNECTED,
                                    call_id=call_id,
                                    expect_call_id=call_id,
                                )

                elif target_state == CallState.COMPLETED:
                    if agent_id:
                        applied_agent = agent_repo.transition(
                            agent_id,
                            AgentState.CONNECTED,
                            AgentState.WRAP_UP,
                            call_id=call_id,
                            expect_call_id=call_id,
                        )
                        if not applied_agent:
                            # Release idle agent if call completed before connecting
                            if not agent_repo.transition(
                                agent_id,
                                AgentState.DIALING,
                                AgentState.AVAILABLE,
                                call_id=call_id,
                                expect_call_id=call_id,
                            ):
                                agent_repo.transition(
                                    agent_id,
                                    AgentState.RESERVED,
                                    AgentState.AVAILABLE,
                                    call_id=call_id,
                                    expect_call_id=call_id,
                                )
                    borrower_repo.mark_done(borrower_id)

                elif target_state in (CallState.FAILED, CallState.CANCELLED):
                    if agent_id:
                        if not agent_repo.transition(
                            agent_id,
                            AgentState.RESERVED,
                            AgentState.AVAILABLE,
                            call_id=call_id,
                            expect_call_id=call_id,
                        ):
                            agent_repo.transition(
                                agent_id,
                                AgentState.DIALING,
                                AgentState.AVAILABLE,
                                call_id=call_id,
                                expect_call_id=call_id,
                            )
                    borrower_repo.record_failure(borrower_id, now=ts)

                return ApplyResult.APPLIED

        return with_retry(_op, self.conn)

    def get_call(self, call_id: str) -> dict[str, Any] | None:
        """Retrieve call record as a dictionary or None if not found."""
        cur = self.conn.execute("SELECT * FROM calls WHERE id = ?;", (call_id,))
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def count_by_state(self) -> dict[str, int]:
        """Return counts of calls grouped by lifecycle state."""
        counts = {state.value: 0 for state in CallState}
        cur = self.conn.execute("SELECT state, COUNT(*) as cnt FROM calls GROUP BY state;")
        for row in cur.fetchall():
            counts[row["state"]] = row["cnt"]
        return counts
