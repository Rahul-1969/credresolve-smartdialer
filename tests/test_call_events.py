"""Tests for provider event application: duplicates, out-of-order, late arrivals, atomicity, and concurrency."""

import random
import threading
import pytest

from smartdialer.calls import ApplyResult, RANK, TERMINAL
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, BorrowerState, CallState
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo


def _setup_call(db_file, call_id="c1", agent_id="a1", borrower_id="b1", initial_agent_state=AgentState.RESERVED):
    conn = connect(db_file)
    init_schema(conn)
    cfg = Config(db_path=str(db_file))
    b_repo = BorrowerRepo(conn, cfg)
    a_repo = AgentRepo(conn, cfg)
    c_repo = CallRepo(conn, cfg)

    b_repo.enqueue_borrower(borrower_id, "+1000", next_attempt_at=0.0)
    a_repo.upsert_agent(agent_id, state=initial_agent_state)
    c_repo.create_call(call_id, borrower_id, agent_id, provider="provider_a")
    return conn, cfg, c_repo, a_repo, b_repo


@pytest.mark.parametrize("use_provider_event_id", [True, False])
def test_duplicate_events(tmp_path, use_provider_event_id):
    """Test 2: DUPLICATES (ANSWERED, ANSWERED, ANSWERED, COMPLETED).

    Asserts exactly one applied ANSWERED and COMPLETED row, and exactly one
    RESERVED->DIALING, DIALING->CONNECTED, CONNECTED->WRAP_UP agent transition.
    """
    db_file = tmp_path / f"dup_{use_provider_event_id}.db"
    conn, cfg, c_repo, a_repo, b_repo = _setup_call(db_file)

    evt_ans = "evt_ans_1" if use_provider_event_id else None
    evt_comp = "evt_comp_1" if use_provider_event_id else None

    # First ANSWERED -> APPLIED (agent jumps RESERVED->DIALING->CONNECTED)
    res1 = c_repo.apply_event("c1", CallState.ANSWERED, provider_event_id=evt_ans)
    assert res1 == ApplyResult.APPLIED

    # Second & Third ANSWERED -> DUPLICATE
    res2 = c_repo.apply_event("c1", CallState.ANSWERED, provider_event_id=evt_ans)
    assert res2 == ApplyResult.DUPLICATE

    res3 = c_repo.apply_event("c1", CallState.ANSWERED, provider_event_id=evt_ans)
    assert res3 == ApplyResult.DUPLICATE

    # COMPLETED -> APPLIED (agent CONNECTED->WRAP_UP)
    res4 = c_repo.apply_event("c1", CallState.COMPLETED, provider_event_id=evt_comp)
    assert res4 == ApplyResult.APPLIED

    # Verify call_events: exactly 1 applied ANSWERED and 1 applied COMPLETED
    cur = conn.execute("SELECT event_type, applied FROM call_events WHERE applied = 1 ORDER BY id;")
    applied_events = [(row["event_type"], row["applied"]) for row in cur.fetchall()]
    # Including the initial RESERVED event on creation
    assert applied_events == [("RESERVED", 1), ("ANSWERED", 1), ("COMPLETED", 1)]

    # Verify agent_events audit trail: exactly RESERVED->DIALING, DIALING->CONNECTED, CONNECTED->WRAP_UP
    cur_ag = conn.execute(
        """
        SELECT from_state, to_state FROM agent_events
        WHERE from_state IS NOT NULL
        ORDER BY id;
        """
    )
    transitions = [(row["from_state"], row["to_state"]) for row in cur_ag.fetchall()]
    assert transitions == [
        ("RESERVED", "DIALING"),
        ("DIALING", "CONNECTED"),
        ("CONNECTED", "WRAP_UP"),
    ]

    conn.close()


def test_out_of_order_events(tmp_path):
    """Test 3: OUT OF ORDER (COMPLETED, ANSWERED, RINGING).

    Final state COMPLETED, agent is handled safely, subsequent lower-rank events ignored.
    """
    db_file = tmp_path / "ooo.db"
    conn, cfg, c_repo, a_repo, b_repo = _setup_call(db_file)

    # 1. COMPLETED arrives first
    res_comp = c_repo.apply_event("c1", CallState.COMPLETED)
    assert res_comp == ApplyResult.APPLIED

    call = c_repo.get_call("c1")
    assert call["state"] == CallState.COMPLETED.value

    # Agent was still RESERVED, so COMPLETED safely releases agent to AVAILABLE
    agent = a_repo.get_agent("a1")
    assert agent["state"] == AgentState.AVAILABLE.value

    # 2. ANSWERED arrives later -> ignored because call is already terminal
    res_ans = c_repo.apply_event("c1", CallState.ANSWERED)
    assert res_ans == ApplyResult.TERMINAL_IGNORED

    # 3. RINGING arrives later -> ignored because call is already terminal
    res_ring = c_repo.apply_event("c1", CallState.RINGING)
    assert res_ring == ApplyResult.TERMINAL_IGNORED

    # Agent remains AVAILABLE and no extra transitions occurred
    cur_ag = conn.execute("SELECT from_state, to_state FROM agent_events WHERE from_state IS NOT NULL;")
    ag_trans = [(r["from_state"], r["to_state"]) for r in cur_ag.fetchall()]
    assert ag_trans == [("RESERVED", "AVAILABLE")]

    conn.close()


def test_late_answer_after_cancel(tmp_path):
    """Test 4: LATE ANSWER AFTER CANCEL (CANCELLED then ANSWERED).

    Call stays CANCELLED, returns TERMINAL_IGNORED, agent is AVAILABLE,
    call_events records ANSWERED with applied=0 as compliance audit log.
    """
    db_file = tmp_path / "late_answer.db"
    conn, cfg, c_repo, a_repo, b_repo = _setup_call(db_file)

    res_cancel = c_repo.apply_event("c1", CallState.CANCELLED)
    assert res_cancel == ApplyResult.APPLIED

    agent = a_repo.get_agent("a1")
    assert agent["state"] == AgentState.AVAILABLE.value

    res_answer = c_repo.apply_event("c1", CallState.ANSWERED, provider_event_id="late_evt_99")
    assert res_answer == ApplyResult.TERMINAL_IGNORED

    call = c_repo.get_call("c1")
    assert call["state"] == CallState.CANCELLED.value

    # Inspect call_events for anomaly audit record (applied=0)
    cur = conn.execute(
        "SELECT event_type, provider_event_id, applied FROM call_events WHERE provider_event_id = 'late_evt_99';"
    )
    late_evt = cur.fetchone()
    assert late_evt is not None
    assert late_evt["applied"] == 0
    assert late_evt["event_type"] == CallState.ANSWERED.value

    conn.close()


def test_crash_after_answered_resumption(tmp_path):
    """Test 5: CRASH AFTER ANSWERED.

    Apply INITIATED and ANSWERED on conn_a, close conn_a abruptly, open conn_b, apply COMPLETED.
    Final state consistent: call COMPLETED, agent WRAP_UP, borrower DONE.
    """
    db_file = tmp_path / "crash_resume.db"
    conn_a, cfg, c_repo_a, a_repo_a, b_repo_a = _setup_call(db_file)

    c_repo_a.apply_event("c1", CallState.INITIATED)
    c_repo_a.apply_event("c1", CallState.ANSWERED)

    # Abruptly close connection A without cleanup
    conn_a.close()

    # Worker B comes up with fresh connection B
    conn_b = connect(db_file)
    c_repo_b = CallRepo(conn_b, cfg)
    a_repo_b = AgentRepo(conn_b, cfg)
    b_repo_b = BorrowerRepo(conn_b, cfg)

    res_comp = c_repo_b.apply_event("c1", CallState.COMPLETED)
    assert res_comp == ApplyResult.APPLIED

    call = c_repo_b.get_call("c1")
    assert call["state"] == CallState.COMPLETED.value

    agent = a_repo_b.get_agent("a1")
    assert agent["state"] == AgentState.WRAP_UP.value

    borrower = b_repo_b.get_borrower("b1")
    assert borrower["state"] == BorrowerState.DONE.value

    conn_b.close()


def test_atomicity_on_side_effect_failure(tmp_path, monkeypatch):
    """Test 6: ATOMICITY.

    When AgentRepo.transition raises during apply_event(ANSWERED), all changes roll back.
    Redelivery after removing error applies cleanly.
    """
    db_file = tmp_path / "atomicity.db"
    conn, cfg, c_repo, a_repo, b_repo = _setup_call(db_file)

    # Transition to INITIATED first
    c_repo.apply_event("c1", CallState.INITIATED)
    call_before = c_repo.get_call("c1")
    agent_before = a_repo.get_agent("a1")

    # Monkeypatch AgentRepo.transition to simulate unexpected crash/error
    orig_transition = AgentRepo.transition

    def buggy_transition(self, *args, **kwargs):
        raise RuntimeError("Simulated side-effect failure")

    monkeypatch.setattr(AgentRepo, "transition", buggy_transition)

    with pytest.raises(RuntimeError, match="Simulated side-effect failure"):
        c_repo.apply_event("c1", CallState.ANSWERED, provider_event_id="evt_fail_1")

    # Verify everything rolled back to state prior to failed ANSWERED
    call_after = c_repo.get_call("c1")
    agent_after = a_repo.get_agent("a1")
    assert call_after["state"] == call_before["state"]
    assert call_after["state_rank"] == call_before["state_rank"]
    assert agent_after["state"] == agent_before["state"]

    # Verify call_events has no committed record for evt_fail_1
    cur = conn.execute("SELECT * FROM call_events WHERE provider_event_id = 'evt_fail_1';")
    assert cur.fetchone() is None

    # Remove monkeypatch and redeliver event
    monkeypatch.setattr(AgentRepo, "transition", orig_transition)
    res_redelivery = c_repo.apply_event("c1", CallState.ANSWERED, provider_event_id="evt_fail_1")
    assert res_redelivery == ApplyResult.APPLIED

    call_final = c_repo.get_call("c1")
    assert call_final["state"] == CallState.ANSWERED.value
    agent_final = a_repo.get_agent("a1")
    assert agent_final["state"] == AgentState.CONNECTED.value

    conn.close()


def _assert_call_agent_invariant(call_state: CallState, agent_state: AgentState):
    """Invariant helper: agent state must always be compatible with the call state."""
    allowed_by_call = {
        CallState.QUEUED: {AgentState.AVAILABLE, AgentState.OFFLINE},
        CallState.RESERVED: {AgentState.RESERVED, AgentState.AVAILABLE, AgentState.OFFLINE},
        CallState.INITIATED: {AgentState.DIALING, AgentState.RESERVED, AgentState.OFFLINE},
        CallState.RINGING: {AgentState.DIALING, AgentState.RESERVED, AgentState.OFFLINE},
        CallState.ANSWERED: {AgentState.CONNECTED, AgentState.DIALING, AgentState.RESERVED, AgentState.OFFLINE},
        CallState.CONNECTED: {AgentState.CONNECTED, AgentState.DIALING, AgentState.OFFLINE},
        CallState.COMPLETED: {AgentState.WRAP_UP, AgentState.AVAILABLE, AgentState.OFFLINE},
        CallState.FAILED: {AgentState.AVAILABLE, AgentState.OFFLINE},
        CallState.CANCELLED: {AgentState.AVAILABLE, AgentState.OFFLINE},
    }
    assert agent_state in allowed_by_call[call_state], (
        f"Invariant broken: call={call_state.value}, agent={agent_state.value}"
    )


def test_seeded_property_stream_300_iterations(tmp_path):
    """Test 7: SEEDED PROPERTY TEST (random.Random(1234), 300 iterations).

    Valid stream [INITIATED, RINGING, ANSWERED, CONNECTED, COMPLETED] shuffled with
    duplicates and dropped middle events (never terminal).
    Asserts rank never decreases, once terminal never changes, invariant holds, final state is COMPLETED.
    """
    rng = random.Random(1234)
    db_file = tmp_path / "prop_stream.db"
    conn = connect(db_file)
    init_schema(conn)
    cfg = Config(db_path=str(db_file))
    c_repo = CallRepo(conn, cfg)
    a_repo = AgentRepo(conn, cfg)
    b_repo = BorrowerRepo(conn, cfg)

    base_stream = [
        CallState.INITIATED,
        CallState.RINGING,
        CallState.ANSWERED,
        CallState.CONNECTED,
        CallState.COMPLETED,
    ]

    for i in range(300):
        call_id = f"c_prop_{i}"
        agent_id = f"a_prop_{i}"
        borrower_id = f"b_prop_{i}"

        b_repo.enqueue_borrower(borrower_id, f"+19{i:04d}", next_attempt_at=0.0)
        a_repo.upsert_agent(agent_id, state=AgentState.RESERVED)
        c_repo.create_call(call_id, borrower_id, agent_id, provider="provider_a")

        # Build random event stream: drop some middle events, duplicate some events, shuffle
        middle_events = [e for e in base_stream[:-1] if rng.random() > 0.3]
        # Always include at least one event leading to or the terminal itself
        stream = middle_events + [CallState.COMPLETED]
        # Add random duplicate injections
        if stream:
            for _ in range(rng.randint(0, 3)):
                stream.append(rng.choice(stream))
        rng.shuffle(stream)

        # Apply stream and verify invariants after every step
        prev_rank = 1  # starts at RESERVED (rank 1)
        was_terminal = False

        for event in stream:
            c_repo.apply_event(call_id, event)
            call = c_repo.get_call(call_id)
            agent = a_repo.get_agent(agent_id)

            curr_rank = call["state_rank"]
            curr_state = CallState(call["state"])
            curr_agent_state = AgentState(agent["state"])

            # Invariant 1: Rank never decreases
            assert curr_rank >= prev_rank, f"Rank decreased from {prev_rank} to {curr_rank}"
            prev_rank = curr_rank

            # Invariant 2: Once terminal, never changes
            if was_terminal:
                assert curr_state in TERMINAL
            if curr_state in TERMINAL:
                was_terminal = True

            # Invariant 3: Call and agent state compatibility
            _assert_call_agent_invariant(curr_state, curr_agent_state)

        # Invariant 4: Final state is COMPLETED
        final_call = c_repo.get_call(call_id)
        assert final_call["state"] == CallState.COMPLETED.value

    conn.close()


def test_concurrent_same_event_application(tmp_path):
    """Test 8: CONCURRENT (8 threads with Barrier applying ANSWERED simultaneously).

    Exactly one returns APPLIED, the rest DUPLICATE or STALE_IGNORED;
    exactly one DIALING->CONNECTED agent transition.
    """
    db_file = tmp_path / "concurrent_event.db"
    conn, cfg, c_repo, a_repo, b_repo = _setup_call(db_file)

    # Put call into INITIATED, agent in DIALING
    c_repo.apply_event("c1", CallState.INITIATED)
    agent = a_repo.get_agent("a1")
    assert agent["state"] == AgentState.DIALING.value
    conn.close()

    num_threads = 8
    barrier = threading.Barrier(num_threads)
    results = []
    errors = []

    def worker_apply():
        thread_conn = connect(db_file)
        thread_repo = CallRepo(thread_conn, cfg)
        try:
            barrier.wait()
            res = thread_repo.apply_event("c1", CallState.ANSWERED, provider_event_id="unique_ans_event")
            results.append(res)
        except Exception as exc:
            errors.append(exc)
        finally:
            thread_conn.close()

    threads = [threading.Thread(target=worker_apply) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors, f"Thread errors: {errors}"
    assert len(results) == num_threads

    applied_count = sum(1 for r in results if r == ApplyResult.APPLIED)
    duplicate_count = sum(1 for r in results if r in (ApplyResult.DUPLICATE, ApplyResult.STALE_IGNORED))

    # Exactly one thread applied the event; all others were duplicate/stale
    assert applied_count == 1, f"Expected 1 APPLIED, got {applied_count} (results: {results})"
    assert duplicate_count == num_threads - 1

    # Verify agent transition in agent_events has exactly one DIALING->CONNECTED
    verify_conn = connect(db_file)
    cur = verify_conn.execute(
        """
        SELECT COUNT(*) as cnt FROM agent_events
        WHERE from_state = 'DIALING' AND to_state = 'CONNECTED';
        """
    )
    assert cur.fetchone()["cnt"] == 1
    verify_conn.close()
