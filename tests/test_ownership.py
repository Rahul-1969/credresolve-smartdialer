"""Tests for agent-call ownership tracking, guarded transitions, and leak prevention."""

import itertools
import pytest

from smartdialer.calls import ApplyResult, RANK, evaluate_transition
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, BorrowerState, CallState
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo


@pytest.fixture
def env(tmp_path):
    """Provide thread-isolated database and repos."""
    db_file = tmp_path / "ownership_test.db"
    conn = connect(db_file)
    init_schema(conn)
    cfg = Config(db_path=str(db_file))
    yield conn, cfg, AgentRepo(conn, cfg), BorrowerRepo(conn, cfg), CallRepo(conn, cfg)
    conn.close()


def test_scenario_late_completed_does_not_release_rebound_agent(env):
    """Scenario a: Call A CONNECTED on X; X goes OFFLINE->AVAILABLE->reserved for B (DIALING).

    Late COMPLETED for Call A must NOT release agent X or interfere with Call B.
    """
    conn, cfg, a_repo, b_repo, c_repo = env

    b_repo.enqueue_borrower("b_A", "+1001", next_attempt_at=0.0)
    b_repo.enqueue_borrower("b_B", "+1002", next_attempt_at=0.0)

    # 1. Start Call A with Agent X (reserved for Call A), bring to CONNECTED
    a_repo.upsert_agent("X", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=["call_A"])

    c_repo.create_call("call_A", "b_A", "X", provider="provider_a")
    c_repo.apply_event("call_A", CallState.INITIATED)
    c_repo.apply_event("call_A", CallState.ANSWERED)

    agent = a_repo.get_agent("X")
    assert agent["state"] == AgentState.CONNECTED.value
    assert agent["reserved_call_id"] == "call_A"

    # 2. Agent X abruptly disconnects (OFFLINE), then logs back on (AVAILABLE)
    a_repo.transition("X", AgentState.CONNECTED, AgentState.OFFLINE)
    a_repo.transition("X", AgentState.OFFLINE, AgentState.AVAILABLE)

    agent = a_repo.get_agent("X")
    assert agent["state"] == AgentState.AVAILABLE.value
    assert agent["reserved_call_id"] is None

    # Record agent_event id baseline before Call B reservation
    cur_baseline = conn.execute("SELECT MAX(id) as max_id FROM agent_events;")
    baseline_event_id = cur_baseline.fetchone()["max_id"] or 0

    # 3. Reserve X for Call B and advance Call B to DIALING
    reserved = a_repo.reserve_agents("w2", 1, call_ids=["call_B"])
    assert reserved == ["X"]
    c_repo.create_call("call_B", "b_B", "X", provider="provider_a")
    c_repo.apply_event("call_B", CallState.INITIATED)

    agent_b = a_repo.get_agent("X")
    assert agent_b["state"] == AgentState.DIALING.value
    assert agent_b["reserved_call_id"] == "call_B"

    # 4. Late COMPLETED event arrives for Call A!
    res_late = c_repo.apply_event("call_A", CallState.COMPLETED)
    assert res_late == ApplyResult.APPLIED

    # Assert Call A is COMPLETED
    call_a = c_repo.get_call("call_A")
    assert call_a["state"] == CallState.COMPLETED.value

    # Assert Agent X is STILL DIALING with reserved_call_id == call_B
    agent_after_late = a_repo.get_agent("X")
    assert agent_after_late["state"] == AgentState.DIALING.value
    assert agent_after_late["reserved_call_id"] == "call_B"

    # Assert Call B is completely untouched
    call_b = c_repo.get_call("call_B")
    assert call_b["state"] == CallState.INITIATED.value

    # Assert no AVAILABLE agent_events row was written for X after the reservation for B
    cur_events = conn.execute(
        """
        SELECT to_state FROM agent_events
        WHERE agent_id = 'X' AND id > ? AND to_state = 'AVAILABLE';
        """,
        (baseline_event_id + 1,),  # baseline + 1 is the AVAILABLE->RESERVED for B
    )
    assert cur_events.fetchall() == []


@pytest.mark.parametrize("terminal_event", [CallState.FAILED, CallState.CANCELLED])
def test_scenario_late_failed_cancelled_does_not_release_rebound_agent(env, terminal_event):
    """Scenario b: Same scenario with FAILED and CANCELLED applied to Call A."""
    conn, cfg, a_repo, b_repo, c_repo = env

    b_repo.enqueue_borrower(f"b_A_{terminal_event.value}", "+1001", next_attempt_at=0.0)
    b_repo.enqueue_borrower(f"b_B_{terminal_event.value}", "+1002", next_attempt_at=0.0)
    agent_id = f"X_{terminal_event.value}"
    call_a_id = f"call_A_{terminal_event.value}"
    call_b_id = f"call_B_{terminal_event.value}"

    a_repo.upsert_agent(agent_id, state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=[call_a_id])

    c_repo.create_call(call_a_id, f"b_A_{terminal_event.value}", agent_id, provider="provider_a")
    c_repo.apply_event(call_a_id, CallState.INITIATED)
    c_repo.apply_event(call_a_id, CallState.ANSWERED)

    # Agent drops OFFLINE then AVAILABLE
    a_repo.transition(agent_id, AgentState.CONNECTED, AgentState.OFFLINE)
    a_repo.transition(agent_id, AgentState.OFFLINE, AgentState.AVAILABLE)

    # Reserve for Call B and initiate
    a_repo.reserve_agents("w2", 1, call_ids=[call_b_id])
    c_repo.create_call(call_b_id, f"b_B_{terminal_event.value}", agent_id, provider="provider_a")
    c_repo.apply_event(call_b_id, CallState.INITIATED)

    # Late terminal event arrives for Call A
    res = c_repo.apply_event(call_a_id, terminal_event)
    assert res == ApplyResult.APPLIED

    call_a = c_repo.get_call(call_a_id)
    assert call_a["state"] == terminal_event.value

    # Agent remains bound to Call B in DIALING
    agent = a_repo.get_agent(agent_id)
    assert agent["state"] == AgentState.DIALING.value
    assert agent["reserved_call_id"] == call_b_id

    # Call B untouched
    call_b = c_repo.get_call(call_b_id)
    assert call_b["state"] == CallState.INITIATED.value


def test_guarded_transition_expect_call_id(env):
    """Scenario c: transition with expect_call_id returns False on mismatch, True on match."""
    conn, cfg, a_repo, b_repo, c_repo = env

    a_repo.upsert_agent("agent_guard", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("worker_1", 1, call_ids=["call_correct"])

    # Wrong call_id -> returns False, no mutation, no audit entry
    cur_events_before = conn.execute("SELECT COUNT(*) as cnt FROM agent_events;").fetchone()["cnt"]
    res_wrong = a_repo.transition(
        "agent_guard",
        AgentState.RESERVED,
        AgentState.DIALING,
        expect_call_id="call_wrong",
    )
    assert res_wrong is False

    cur_events_after = conn.execute("SELECT COUNT(*) as cnt FROM agent_events;").fetchone()["cnt"]
    assert cur_events_after == cur_events_before

    agent = a_repo.get_agent("agent_guard")
    assert agent["state"] == AgentState.RESERVED.value

    # Right call_id -> returns True, mutates, records audit entry
    res_right = a_repo.transition(
        "agent_guard",
        AgentState.RESERVED,
        AgentState.DIALING,
        expect_call_id="call_correct",
    )
    assert res_right is True

    agent_after = a_repo.get_agent("agent_guard")
    assert agent_after["state"] == AgentState.DIALING.value
    assert agent_after["reserved_call_id"] == "call_correct"


def test_reserved_call_id_lifecycle(env):
    """Scenario d: reserved_call_id is retained during active call, cleared only on release."""
    conn, cfg, a_repo, b_repo, c_repo = env

    a_repo.upsert_agent("agent_lifecycle", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=["call_999"])

    agent = a_repo.get_agent("agent_lifecycle")
    assert agent["state"] == AgentState.RESERVED.value
    assert agent["reserved_call_id"] == "call_999"

    # RESERVED -> DIALING (keeps reserved_call_id, clears lease_expires_at)
    a_repo.transition("agent_lifecycle", AgentState.RESERVED, AgentState.DIALING)
    agent = a_repo.get_agent("agent_lifecycle")
    assert agent["state"] == AgentState.DIALING.value
    assert agent["reserved_call_id"] == "call_999"
    assert agent["lease_expires_at"] is None

    # DIALING -> CONNECTED (keeps reserved_call_id)
    a_repo.transition("agent_lifecycle", AgentState.DIALING, AgentState.CONNECTED)
    agent = a_repo.get_agent("agent_lifecycle")
    assert agent["state"] == AgentState.CONNECTED.value
    assert agent["reserved_call_id"] == "call_999"

    # CONNECTED -> WRAP_UP (keeps reserved_call_id)
    a_repo.transition("agent_lifecycle", AgentState.CONNECTED, AgentState.WRAP_UP)
    agent = a_repo.get_agent("agent_lifecycle")
    assert agent["state"] == AgentState.WRAP_UP.value
    assert agent["reserved_call_id"] == "call_999"

    # WRAP_UP -> AVAILABLE (clears reserved_call_id, reserved_by, lease_expires_at)
    a_repo.transition("agent_lifecycle", AgentState.WRAP_UP, AgentState.AVAILABLE)
    agent = a_repo.get_agent("agent_lifecycle")
    assert agent["state"] == AgentState.AVAILABLE.value
    assert agent["reserved_call_id"] is None
    assert agent["reserved_by"] is None
    assert agent["lease_expires_at"] is None


def test_unknown_call_leaves_no_events(env):
    """Scenario e: apply_event on a missing call returns UNKNOWN_CALL and writes 0 call_events rows."""
    conn, cfg, a_repo, b_repo, c_repo = env

    cur_before = conn.execute("SELECT COUNT(*) as cnt FROM call_events;").fetchone()["cnt"]

    res = c_repo.apply_event("non_existent_call_123", CallState.ANSWERED, provider_event_id="prov_evt_xyz")
    assert res == ApplyResult.UNKNOWN_CALL

    cur_after = conn.execute("SELECT COUNT(*) as cnt FROM call_events;").fetchone()["cnt"]
    assert cur_after == cur_before

    # Verify no row exists for this call_id
    cur_check = conn.execute("SELECT * FROM call_events WHERE call_id = 'non_existent_call_123';")
    assert cur_check.fetchall() == []


ALL_STATES = list(CallState)
ALL_PAIRS = list(itertools.product(ALL_STATES, ALL_STATES))


@pytest.mark.parametrize("current_state,event_type", ALL_PAIRS)
def test_evaluate_transition_agrees_with_sql_apply_event(env, current_state, event_type):
    """Scenario 5: Assert evaluate_transition pure function agrees with apply_event for every (current, event) pair."""
    conn, cfg, a_repo, b_repo, c_repo = env

    # Enqueue borrower to satisfy FOREIGN KEY constraint
    b_repo.enqueue_borrower("b_dummy", "+1000", next_attempt_at=0.0)

    call_id = f"c_match_{current_state.value}_{event_type.value}"
    now = cfg.clock()
    curr_rank = RANK[current_state]

    # Directly insert call in current_state
    conn.execute(
        """
        INSERT INTO calls (id, borrower_id, agent_id, provider, state, state_rank, version, attempt, created_at, updated_at)
        VALUES (?, 'b_dummy', NULL, 'provider_a', ?, ?, 0, 1, ?, ?);
        """,
        (call_id, current_state.value, curr_rank, now, now),
    )

    sql_result = c_repo.apply_event(call_id, event_type)
    pure_result = evaluate_transition(current_state, event_type)

    assert sql_result == pure_result, (
        f"Mismatch on ({current_state.value}, {event_type.value}): SQL={sql_result}, pure={pure_result}"
    )
