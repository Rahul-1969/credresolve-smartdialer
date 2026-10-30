"""Tests for calls table columns: answered_at, end_reason, and conditional agent binding."""

import pytest

from smartdialer.calls import ApplyResult
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, CallState
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo


@pytest.fixture
def env(tmp_path):
    """Provide thread-isolated database and repos."""
    db_file = tmp_path / "call_columns_test.db"
    conn = connect(db_file)
    init_schema(conn)
    cfg = Config(db_path=str(db_file))
    yield conn, cfg, AgentRepo(conn, cfg), BorrowerRepo(conn, cfg), CallRepo(conn, cfg)
    conn.close()


def test_answered_at_set_once_and_never_overwritten(env):
    """answered_at is recorded at first answered event and preserved through connected and completed."""
    conn, cfg, a_repo, b_repo, c_repo = env

    b_repo.enqueue_borrower("b_col_1", "+1001", next_attempt_at=0.0)
    a_repo.upsert_agent("a_col_1", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=["call_col_1"])

    c_repo.create_call("call_col_1", "b_col_1", "a_col_1", provider="provider_a")
    call = c_repo.get_call("call_col_1")
    assert call["answered_at"] is None
    assert call["end_reason"] is None

    # INITIATED
    c_repo.apply_event("call_col_1", CallState.INITIATED, now=10.0)
    call = c_repo.get_call("call_col_1")
    assert call["answered_at"] is None

    # ANSWERED at t=20.0
    res_ans = c_repo.apply_event("call_col_1", CallState.ANSWERED, now=20.0)
    assert res_ans == ApplyResult.APPLIED
    call = c_repo.get_call("call_col_1")
    assert call["answered_at"] == 20.0

    # CONNECTED at t=25.0 -> answered_at remains 20.0
    res_conn = c_repo.apply_event("call_col_1", CallState.CONNECTED, now=25.0)
    assert res_conn == ApplyResult.APPLIED
    call = c_repo.get_call("call_col_1")
    assert call["answered_at"] == 20.0

    # COMPLETED at t=40.0 -> answered_at remains 20.0, end_reason is set
    res_comp = c_repo.apply_event("call_col_1", CallState.COMPLETED, now=40.0)
    assert res_comp == ApplyResult.APPLIED
    call = c_repo.get_call("call_col_1")
    assert call["answered_at"] == 20.0
    assert call["end_reason"] == "COMPLETED"


def test_end_reason_stored_for_failed_with_custom_reasons(env):
    """end_reason stores custom reasons like NO_ANSWER or PROVIDER_ERROR on FAILED calls."""
    conn, cfg, a_repo, b_repo, c_repo = env

    # Call 1: NO_ANSWER
    b_repo.enqueue_borrower("b_fail_1", "+1002", next_attempt_at=0.0)
    a_repo.upsert_agent("a_fail_1", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=["call_fail_1"])
    c_repo.create_call("call_fail_1", "b_fail_1", "a_fail_1", provider="provider_a")
    c_repo.apply_event("call_fail_1", CallState.INITIATED, now=10.0)

    res_fail1 = c_repo.apply_event(
        "call_fail_1",
        CallState.FAILED,
        now=20.0,
        reason="NO_ANSWER",
    )
    assert res_fail1 == ApplyResult.APPLIED
    call1 = c_repo.get_call("call_fail_1")
    assert call1["state"] == CallState.FAILED.value
    assert call1["end_reason"] == "NO_ANSWER"
    assert call1["answered_at"] is None

    # Call 2: PROVIDER_ERROR
    b_repo.enqueue_borrower("b_fail_2", "+1003", next_attempt_at=0.0)
    a_repo.upsert_agent("a_fail_2", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=["call_fail_2"])
    c_repo.create_call("call_fail_2", "b_fail_2", "a_fail_2", provider="provider_a")
    c_repo.apply_event("call_fail_2", CallState.INITIATED, now=10.0)

    res_fail2 = c_repo.apply_event(
        "call_fail_2",
        CallState.FAILED,
        now=15.0,
        reason="PROVIDER_ERROR",
    )
    assert res_fail2 == ApplyResult.APPLIED
    call2 = c_repo.get_call("call_fail_2")
    assert call2["state"] == CallState.FAILED.value
    assert call2["end_reason"] == "PROVIDER_ERROR"
    assert call2["answered_at"] is None


def test_ignored_events_change_nothing(env):
    """DUPLICATE, STALE_IGNORED, and TERMINAL_IGNORED events never modify answered_at or end_reason."""
    conn, cfg, a_repo, b_repo, c_repo = env

    b_repo.enqueue_borrower("b_ign", "+1004", next_attempt_at=0.0)
    a_repo.upsert_agent("a_ign", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=["call_ign"])
    c_repo.create_call("call_ign", "b_ign", "a_ign", provider="provider_a")

    # ANSWERED at t=10.0 with event_id
    res_ans = c_repo.apply_event(
        "call_ign",
        CallState.ANSWERED,
        provider_event_id="evt_ans_1",
        now=10.0,
    )
    assert res_ans == ApplyResult.APPLIED
    call_before = c_repo.get_call("call_ign")
    assert call_before["answered_at"] == 10.0

    # 1. DUPLICATE event at t=12.0 with different reason/time
    res_dup = c_repo.apply_event(
        "call_ign",
        CallState.ANSWERED,
        provider_event_id="evt_ans_1",
        now=12.0,
        reason="SHOULD_NOT_APPLY",
    )
    assert res_dup == ApplyResult.DUPLICATE
    call_after_dup = c_repo.get_call("call_ign")
    assert call_after_dup["answered_at"] == 10.0
    assert call_after_dup["end_reason"] is None

    # 2. STALE event (RINGING after ANSWERED)
    res_stale = c_repo.apply_event(
        "call_ign",
        CallState.RINGING,
        provider_event_id="evt_ring_late",
        now=13.0,
        reason="SHOULD_NOT_APPLY",
    )
    assert res_stale == ApplyResult.STALE_IGNORED
    call_after_stale = c_repo.get_call("call_ign")
    assert call_after_stale["answered_at"] == 10.0
    assert call_after_stale["end_reason"] is None

    # Terminate call with COMPLETED
    res_comp = c_repo.apply_event(
        "call_ign",
        CallState.COMPLETED,
        provider_event_id="evt_comp_1",
        now=20.0,
        reason="NORMAL_COMPLETION",
    )
    assert res_comp == ApplyResult.APPLIED
    call_term = c_repo.get_call("call_ign")
    assert call_term["end_reason"] == "NORMAL_COMPLETION"

    # 3. TERMINAL_IGNORED (late FAILED event arriving after COMPLETED)
    res_term_ign = c_repo.apply_event(
        "call_ign",
        CallState.FAILED,
        provider_event_id="evt_fail_late",
        now=25.0,
        reason="LATE_FAILURE",
    )
    assert res_term_ign == ApplyResult.TERMINAL_IGNORED
    call_after_term_ign = c_repo.get_call("call_ign")
    assert call_after_term_ign["state"] == CallState.COMPLETED.value
    assert call_after_term_ign["end_reason"] == "NORMAL_COMPLETION"
    assert call_after_term_ign["answered_at"] == 10.0


def test_create_call_conditional_agent_binding(env):
    """create_call only binds agent if agent is RESERVED and reserved_call_id is None or matches."""
    conn, cfg, a_repo, b_repo, c_repo = env

    b_repo.enqueue_borrower("b_cond_1", "+1005", next_attempt_at=0.0)
    b_repo.enqueue_borrower("b_cond_2", "+1006", next_attempt_at=0.0)

    # Agent is RESERVED for call_1
    a_repo.upsert_agent("a_cond", state=AgentState.AVAILABLE)
    a_repo.reserve_agents("w1", 1, call_ids=["call_1"])

    # create_call with call_1 matches
    assert c_repo.create_call("call_1", "b_cond_1", "a_cond", provider="provider_a") is True
    agent = a_repo.get_agent("a_cond")
    assert agent["reserved_call_id"] == "call_1"

    # Now create_call for call_2 trying to bind the same agent (which is bound to call_1)
    # The call creation succeeds, but agent binding UPDATE affects 0 rows and leaves agent bound to call_1
    c_repo.create_call("call_2", "b_cond_2", "a_cond", provider="provider_a")
    agent_check = a_repo.get_agent("a_cond")
    assert agent_check["reserved_call_id"] == "call_1"

