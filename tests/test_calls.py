"""Tests for call state machine, rank matrix, failure backoff, and progressive dispatch."""

import itertools
import threading
import pytest

from smartdialer.calls import RANK, ApplyResult, evaluate_transition
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.dispatch import start_progressive_call
from smartdialer.models import AgentState, BorrowerState, CallState
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo

ALL_CALL_STATES = list(CallState)
CALL_PAIRS = list(itertools.product(ALL_CALL_STATES, ALL_CALL_STATES))


@pytest.fixture
def call_env(tmp_path):
    """Provide a thread-isolated database and repositories."""
    db_file = tmp_path / "calls_test.db"
    conn = connect(db_file)
    init_schema(conn)
    cfg = Config(db_path=str(db_file))
    yield conn, cfg
    conn.close()


@pytest.mark.parametrize("current_state,event_type", CALL_PAIRS)
def test_full_rank_transition_matrix(call_env, current_state, event_type):
    """Assert all 81 (current_state, event_type) combinations yield correct ApplyResult and final state."""
    conn, cfg = call_env
    call_repo = CallRepo(conn, cfg)
    borrower_repo = BorrowerRepo(conn, cfg)
    agent_repo = AgentRepo(conn, cfg)

    call_id = f"c_{current_state.value}_{event_type.value}"
    borrower_id = f"b_{current_state.value}_{event_type.value}"
    agent_id = f"a_{current_state.value}_{event_type.value}"

    borrower_repo.enqueue_borrower(borrower_id, "+1000", next_attempt_at=0.0)
    agent_repo.upsert_agent(agent_id, state=AgentState.AVAILABLE)

    # Insert call directly in current_state with corresponding rank
    curr_rank = RANK[current_state]
    now = cfg.clock()
    conn.execute(
        """
        INSERT INTO calls (id, borrower_id, agent_id, provider, state, state_rank, version, attempt, created_at, updated_at)
        VALUES (?, ?, ?, 'provider_a', ?, ?, 0, 1, ?, ?);
        """,
        (call_id, borrower_id, agent_id, current_state.value, curr_rank, now, now),
    )

    expected_result = evaluate_transition(current_state, event_type)
    expected_final_state = event_type.value if expected_result == ApplyResult.APPLIED else current_state.value

    result = call_repo.apply_event(call_id, event_type=event_type)
    assert result == expected_result

    call = call_repo.get_call(call_id)
    assert call["state"] == expected_final_state


def test_failure_retry_backoff_and_permanent_failure(tmp_path):
    """Assert FAILED calculates exponential backoff on retry and flags FAILED_PERMANENT on max attempts."""
    db_file = tmp_path / "retry_test.db"
    conn = connect(db_file)
    init_schema(conn)

    current_time = 1000.0

    def mock_clock():
        return current_time

    config = Config(
        db_path=str(db_file),
        clock=mock_clock,
        max_attempts=3,
        retry_backoff_seconds=60.0,
    )
    b_repo = BorrowerRepo(conn, config)
    a_repo = AgentRepo(conn, config)
    c_repo = CallRepo(conn, config)

    borrower_id = "borrower_fail_test"
    agent_id = "agent_fail_test"
    call_id_1 = "call_fail_1"

    b_repo.enqueue_borrower(borrower_id, "+1999", next_attempt_at=current_time)
    a_repo.upsert_agent(agent_id, state=AgentState.AVAILABLE)

    # Attempt 1: Call created and fails
    c_repo.create_call(call_id_1, borrower_id, agent_id, provider="provider_a", attempt=1)
    res1 = c_repo.apply_event(call_id_1, CallState.FAILED, now=current_time)
    assert res1 == ApplyResult.APPLIED

    # Check borrower: attempt 1 -> next_attempt_at = 1000.0 + 60 * 2**(1-1) = 1060.0
    b1 = b_repo.get_borrower(borrower_id)
    assert b1["state"] == BorrowerState.QUEUED.value
    assert b1["attempts"] == 1
    assert b1["next_attempt_at"] == 1060.0

    # Agent should have been released to AVAILABLE
    agent = a_repo.get_agent(agent_id)
    assert agent["state"] == AgentState.AVAILABLE.value

    # Attempt 2: Advance clock to 1060.0
    current_time = 1060.0
    call_id_2 = "call_fail_2"
    c_repo.create_call(call_id_2, borrower_id, agent_id, provider="provider_a", attempt=2)
    res2 = c_repo.apply_event(call_id_2, CallState.FAILED, now=current_time)
    assert res2 == ApplyResult.APPLIED

    # Check borrower: attempt 2 -> next_attempt_at = 1060.0 + 60 * 2**(2-1) = 1180.0
    b2 = b_repo.get_borrower(borrower_id)
    assert b2["state"] == BorrowerState.QUEUED.value
    assert b2["attempts"] == 2
    assert b2["next_attempt_at"] == 1180.0

    # Attempt 3: Advance clock to 1180.0 (max attempts = 3)
    current_time = 1180.0
    call_id_3 = "call_fail_3"
    c_repo.create_call(call_id_3, borrower_id, agent_id, provider="provider_a", attempt=3)
    res3 = c_repo.apply_event(call_id_3, CallState.FAILED, now=current_time)
    assert res3 == ApplyResult.APPLIED

    # Check borrower: attempt 3 >= max_attempts (3) -> FAILED_PERMANENT
    b3 = b_repo.get_borrower(borrower_id)
    assert b3["state"] == BorrowerState.FAILED_PERMANENT.value
    assert b3["attempts"] == 3

    conn.close()


def test_dispatch_progressive_concurrency_and_empty_agents(tmp_path):
    """Assert racing dispatch calls create exactly 1 call and leave no orphaned claims or reservations."""
    db_file = tmp_path / "dispatch_test.db"
    setup_conn = connect(db_file)
    init_schema(setup_conn)
    setup_b_repo = BorrowerRepo(setup_conn)
    setup_a_repo = AgentRepo(setup_conn)

    # 1 agent and 2 borrowers
    setup_a_repo.upsert_agent("agent_solo", state=AgentState.AVAILABLE)
    setup_b_repo.enqueue_borrower("borrower_1", "+1001", next_attempt_at=0.0)
    setup_b_repo.enqueue_borrower("borrower_2", "+1002", next_attempt_at=0.0)
    setup_conn.close()

    barrier = threading.Barrier(2)
    results = {}
    config = Config(db_path=str(db_file))

    def worker_dispatch(worker_id: str, call_id: str):
        conn = connect(db_file)
        try:
            barrier.wait()
            results[worker_id] = start_progressive_call(conn, config, worker_id, call_id)
        finally:
            conn.close()

    t1 = threading.Thread(target=worker_dispatch, args=("w1", "call_w1"))
    t2 = threading.Thread(target=worker_dispatch, args=("w2", "call_w2"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    res_w1 = results["w1"]
    res_w2 = results["w2"]

    # Exactly one succeeded and the other returned None
    assert (res_w1 is not None and res_w2 is None) or (res_w2 is not None and res_w1 is None)

    # Verify no orphan borrower claim or reservation
    verify_conn = connect(db_file)
    verify_b_repo = BorrowerRepo(verify_conn)
    verify_a_repo = AgentRepo(verify_conn)

    b_counts = verify_b_repo.count_by_state()
    assert b_counts[BorrowerState.IN_CALL.value] == 1
    assert b_counts[BorrowerState.QUEUED.value] == 1
    assert b_counts[BorrowerState.CLAIMED.value] == 0

    a_counts = verify_a_repo.count_by_state()
    assert a_counts[AgentState.RESERVED.value] == 1
    assert a_counts[AgentState.AVAILABLE.value] == 0

    # With 0 agents: dispatch returns None and borrower stays QUEUED
    res_zero = start_progressive_call(verify_conn, config, "w_zero", "call_zero")
    assert res_zero is None
    b_counts_after = verify_b_repo.count_by_state()
    assert b_counts_after[BorrowerState.QUEUED.value] == 1
    assert b_counts_after[BorrowerState.CLAIMED.value] == 0

    verify_conn.close()

