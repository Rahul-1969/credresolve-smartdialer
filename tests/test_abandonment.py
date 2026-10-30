"""Tests for bind-on-answer, abandonment, and non-penalized retry logic."""

from pathlib import Path
import sqlite3
import threading
import pytest

from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.metrics import read_snapshot
from smartdialer.models import AgentState, BorrowerState, CallState
from smartdialer.repo import CallRepo


def test_scenario_1_unbound_call_answered_with_available_agent():
    """1. Unbound call ANSWERED with an AVAILABLE agent -> call.agent_id set, agent CONNECTED, reserved_call_id == call, abandoned 0."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b1', '5550001', 'IN_CALL', 0, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, updated_at) VALUES ('a1', 'AVAILABLE', ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c1', 'b1', NULL, 'INITIATED', 2, ?, ?)",
        (now, now),
    )

    repo = CallRepo(conn, cfg)
    res = repo.apply_event("c1", CallState.ANSWERED, now=now + 5.0)
    assert res.value == "APPLIED"

    call = repo.get_call("c1")
    assert call["agent_id"] == "a1"
    assert call["abandoned"] == 0
    assert call["state"] == "ANSWERED"

    cur_a = conn.execute("SELECT * FROM agents WHERE id = 'a1';")
    agent = cur_a.fetchone()
    assert agent["state"] == AgentState.CONNECTED.value
    assert agent["reserved_call_id"] == "c1"

    cur_actions = conn.execute("SELECT COUNT(*) as cnt FROM call_actions WHERE call_id = 'c1';")
    assert cur_actions.fetchone()["cnt"] == 0


def test_scenario_2_unbound_call_answered_with_no_agent():
    """2. Unbound call ANSWERED with NO agent -> abandoned = 1, exactly one TERMINATE row (also with duplicate events)."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b2', '5550002', 'IN_CALL', 0, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c2', 'b2', NULL, 'INITIATED', 2, ?, ?)",
        (now, now),
    )

    repo = CallRepo(conn, cfg)
    # First ANSWERED event
    res1 = repo.apply_event(
        "c2",
        CallState.ANSWERED,
        provider_event_id="pevt_ans_1",
        now=now + 5.0,
    )
    assert res1.value == "APPLIED"

    # Duplicate ANSWERED event with same provider_event_id
    res_dup1 = repo.apply_event(
        "c2",
        CallState.ANSWERED,
        provider_event_id="pevt_ans_1",
        now=now + 6.0,
    )
    assert res_dup1.value == "DUPLICATE"

    # Duplicate ANSWERED event without provider_event_id
    res_dup2 = repo.apply_event(
        "c2",
        CallState.ANSWERED,
        provider_event_id=None,
        now=now + 7.0,
    )
    assert res_dup2.value == "DUPLICATE"

    call = repo.get_call("c2")
    assert call["agent_id"] is None
    assert call["abandoned"] == 1

    cur_actions = conn.execute("SELECT * FROM call_actions WHERE call_id = 'c2';")
    rows = cur_actions.fetchall()
    assert len(rows) == 1
    assert rows[0]["action"] == "TERMINATE"
    assert rows[0]["reason"] == "ABANDONED"


def test_scenario_3_completed_after_abandonment():
    """3. After that, COMPLETED -> borrower QUEUED (not DONE) with next_attempt_at = now + abandon_retry_delay_s and unchanged attempts; end_reason 'ABANDONED'; read_snapshot counts it."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b3', '5550003', 'IN_CALL', 0, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c3', 'b3', NULL, 'INITIATED', 2, ?, ?)",
        (now, now),
    )

    repo = CallRepo(conn, cfg)
    repo.apply_event("c3", CallState.ANSWERED, now=now + 2.0)
    # Now call is abandoned
    call_ans = repo.get_call("c3")
    assert call_ans["abandoned"] == 1

    completed_time = now + 10.0
    repo.apply_event("c3", CallState.COMPLETED, now=completed_time)

    call_comp = repo.get_call("c3")
    assert call_comp["state"] == "COMPLETED"
    assert call_comp["abandoned"] == 1
    assert call_comp["end_reason"] == "ABANDONED"

    cur_b = conn.execute("SELECT * FROM borrowers WHERE id = 'b3';")
    borrower = cur_b.fetchone()
    assert borrower["state"] == BorrowerState.QUEUED.value
    assert borrower["attempts"] == 0
    assert borrower["next_attempt_at"] == pytest.approx(completed_time + cfg.abandon_retry_delay_s)

    # Verify read_snapshot counts the abandonment
    snap = read_snapshot(conn, completed_time + 1.0, 120.0, 900.0, 600.0)
    assert snap.abandon.answered == 1
    assert snap.abandon.abandoned == 1


def test_scenario_4_completed_arriving_before_answered_for_unbound():
    """4. COMPLETED arriving before any ANSWERED for an unbound call -> abandoned = 1."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b4', '5550004', 'IN_CALL', 0, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c4', 'b4', NULL, 'INITIATED', 2, ?, ?)",
        (now, now),
    )

    repo = CallRepo(conn, cfg)
    completed_time = now + 8.0
    repo.apply_event("c4", CallState.COMPLETED, now=completed_time)

    call = repo.get_call("c4")
    assert call["state"] == "COMPLETED"
    assert call["abandoned"] == 1
    assert call["end_reason"] == "ABANDONED"

    cur_b = conn.execute("SELECT * FROM borrowers WHERE id = 'b4';")
    borrower = cur_b.fetchone()
    assert borrower["state"] == BorrowerState.QUEUED.value
    assert borrower["attempts"] == 0
    assert borrower["next_attempt_at"] == pytest.approx(completed_time + cfg.abandon_retry_delay_s)


def test_scenario_5_concurrent_bind_and_abandon(tmp_path: Path):
    """5. CONCURRENT: 20 unbound calls, 8 AVAILABLE agents, one thread per call applying ANSWERED at once.

    Assert exactly 8 bound to 8 DISTINCT agents, exactly 12 abandoned, 12 TERMINATE rows,
    no agent bound twice.
    """
    db_file = tmp_path / "concurrent_bind.db"
    main_conn = connect(db_file)
    init_schema(main_conn)
    cfg = Config()

    now = 100.0
    num_calls = 20
    num_agents = 8

    for i in range(num_calls):
        main_conn.execute(
            "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES (?, ?, 'IN_CALL', 0, ?);",
            (f"b_conc_{i:02d}", f"555{i:04d}", now),
        )
        main_conn.execute(
            "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES (?, ?, NULL, 'INITIATED', 2, ?, ?);",
            (f"c_conc_{i:02d}", f"b_conc_{i:02d}", now, now),
        )

    for j in range(num_agents):
        main_conn.execute(
            "INSERT INTO agents (id, state, updated_at) VALUES (?, 'AVAILABLE', ?);",
            (f"a_conc_{j:02d}", now),
        )

    barrier = threading.Barrier(num_calls)
    errors: list[Exception] = []

    def worker_thread(call_id: str):
        t_conn = connect(db_file)
        try:
            t_repo = CallRepo(t_conn, cfg)
            barrier.wait()
            t_repo.apply_event(call_id, CallState.ANSWERED, now=now + 5.0)
        except Exception as e:
            errors.append(e)
        finally:
            t_conn.close()

    threads = [
        threading.Thread(target=worker_thread, args=(f"c_conc_{i:02d}",))
        for i in range(num_calls)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Worker threads encountered errors: {errors}"

    # Verify results
    cur_bound = main_conn.execute(
        "SELECT id, agent_id, abandoned FROM calls WHERE agent_id IS NOT NULL;"
    )
    bound_calls = cur_bound.fetchall()
    assert len(bound_calls) == num_agents

    bound_agent_ids = [r["agent_id"] for r in bound_calls]
    assert len(set(bound_agent_ids)) == num_agents, f"Duplicate agent assignments: {bound_agent_ids}"

    for r in bound_calls:
        assert r["abandoned"] == 0

    cur_abandoned = main_conn.execute(
        "SELECT id, agent_id, abandoned FROM calls WHERE abandoned = 1;"
    )
    abandoned_calls = cur_abandoned.fetchall()
    assert len(abandoned_calls) == (num_calls - num_agents)

    for r in abandoned_calls:
        assert r["agent_id"] is None

    # Check call_actions: exactly 12 TERMINATE rows
    cur_actions = main_conn.execute(
        "SELECT COUNT(*) as cnt FROM call_actions WHERE action = 'TERMINATE' AND reason = 'ABANDONED';"
    )
    assert cur_actions.fetchone()["cnt"] == (num_calls - num_agents)

    # Check agent states and reserved_call_id
    cur_agents = main_conn.execute("SELECT id, state, reserved_call_id FROM agents;")
    agent_rows = cur_agents.fetchall()
    assert len(agent_rows) == num_agents
    for a in agent_rows:
        assert a["state"] == AgentState.CONNECTED.value
        assert a["reserved_call_id"] is not None

    reserved_call_ids = [a["reserved_call_id"] for a in agent_rows]
    assert len(set(reserved_call_ids)) == num_agents, "Multiple agents bound to same call!"

    main_conn.close()


def test_scenario_6_bound_progressive_call_unaffected():
    """6. A bound (progressive) call is unaffected by this bind-on-answer logic."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_prog', '5550009', 'IN_CALL', 0, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_prog', 'RESERVED', 'c_prog', ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c_prog', 'b_prog', 'a_prog', 'INITIATED', 2, ?, ?)",
        (now, now),
    )

    repo = CallRepo(conn, cfg)
    # Apply ANSWERED
    res_ans = repo.apply_event("c_prog", CallState.ANSWERED, now=now + 2.0)
    assert res_ans.value == "APPLIED"

    call = repo.get_call("c_prog")
    assert call["agent_id"] == "a_prog"
    assert call["abandoned"] == 0

    cur_actions = conn.execute("SELECT COUNT(*) as cnt FROM call_actions WHERE call_id = 'c_prog';")
    assert cur_actions.fetchone()["cnt"] == 0

    cur_a = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_prog';")
    a_row = cur_a.fetchone()
    assert a_row["state"] == AgentState.CONNECTED.value
    assert a_row["reserved_call_id"] == "c_prog"

    # Apply COMPLETED
    res_comp = repo.apply_event("c_prog", CallState.COMPLETED, now=now + 10.0)
    assert res_comp.value == "APPLIED"

    cur_a2 = conn.execute("SELECT state FROM agents WHERE id = 'a_prog';")
    assert cur_a2.fetchone()["state"] == AgentState.WRAP_UP.value

    cur_b = conn.execute("SELECT state FROM borrowers WHERE id = 'b_prog';")
    assert cur_b.fetchone()["state"] == BorrowerState.DONE.value


def test_scenario_7_non_penalized_vs_penalized_failures():
    """7. Non-penalized reasons: FAILED(PROVIDER_ERROR) and CANCELLED leave attempts unchanged; FAILED(NO_ANSWER) increments attempts."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(nonpenalized_retry_delay_s=5.0, retry_backoff_seconds=60.0)
    repo = CallRepo(conn, cfg)

    now = 100.0

    # 1. PROVIDER_ERROR
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_p1', '5550001', 'IN_CALL', 1, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c_p1', 'b_p1', NULL, 'INITIATED', 2, ?, ?)",
        (now, now),
    )
    repo.apply_event("c_p1", CallState.FAILED, reason="PROVIDER_ERROR", now=now)
    b_p1 = conn.execute("SELECT * FROM borrowers WHERE id = 'b_p1';").fetchone()
    assert b_p1["state"] == BorrowerState.QUEUED.value
    assert b_p1["attempts"] == 1  # Unchanged
    assert b_p1["next_attempt_at"] == pytest.approx(now + 5.0)

    # 2. CANCELLED
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_p2', '5550002', 'IN_CALL', 2, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c_p2', 'b_p2', NULL, 'INITIATED', 2, ?, ?)",
        (now, now),
    )
    repo.apply_event("c_p2", CallState.CANCELLED, now=now)
    b_p2 = conn.execute("SELECT * FROM borrowers WHERE id = 'b_p2';").fetchone()
    assert b_p2["state"] == BorrowerState.QUEUED.value
    assert b_p2["attempts"] == 2  # Unchanged
    assert b_p2["next_attempt_at"] == pytest.approx(now + 5.0)

    # 3. Penalized: FAILED(NO_ANSWER)
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_p3', '5550003', 'IN_CALL', 1, ?)",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) VALUES ('c_p3', 'b_p3', NULL, 'INITIATED', 2, ?, ?)",
        (now, now),
    )
    repo.apply_event("c_p3", CallState.FAILED, reason="NO_ANSWER", now=now)
    b_p3 = conn.execute("SELECT * FROM borrowers WHERE id = 'b_p3';").fetchone()
    assert b_p3["state"] == BorrowerState.QUEUED.value
    assert b_p3["attempts"] == 2  # Incremented from 1
    # Exponential backoff: retry_backoff_seconds * 2**(attempts - 1) = 60 * 2**(2-1) = 120
    assert b_p3["next_attempt_at"] == pytest.approx(now + 120.0)

