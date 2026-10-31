"""Tests for agent_went_offline operations, loss recovery, and lifecycle edge cases."""

from pathlib import Path
import random
import pytest

from smartdialer.actions import process_actions
from smartdialer.agent_ops import agent_went_offline
from smartdialer.calls import ApplyResult, CallState
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, BorrowerState
from smartdialer.pacing.predictive import PredictiveConfig, PredictivePacer
from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.mock import MockProvider, ProviderProfile
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo
from smartdialer.snapshot import AbandonStats, RateStats, SystemSnapshot


def test_14_agent_ops_branches_and_edge_cases():
    """14. test_agent_ops: noop on missing/already-offline agent; raced transition returns 'raced'; each call-stage branch."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0

    # 1. Missing agent -> noop
    res_missing = agent_went_offline(conn, cfg, "non_existent_agent", now)
    assert res_missing == {"result": "noop"}

    # 2. Already OFFLINE agent -> noop
    conn.execute(
        "INSERT INTO agents (id, state, updated_at) VALUES ('a_off', 'OFFLINE', ?);",
        (now,),
    )
    res_off = agent_went_offline(conn, cfg, "a_off", now)
    assert res_off == {"result": "noop"}

    # 3. Raced transition: state changed right between read and transition
    conn.execute(
        "INSERT INTO agents (id, state, updated_at) VALUES ('a_race', 'AVAILABLE', ?);",
        (now,),
    )
    # Monkeypatch AgentRepo.transition to simulate race
    orig_transition = AgentRepo.transition

    def failing_transition(self, aid, from_st, to_st, *args, **kwargs):
        if aid == "a_race":
            return False
        return orig_transition(self, aid, from_st, to_st, *args, **kwargs)

    AgentRepo.transition = failing_transition  # type: ignore
    try:
        res_race = agent_went_offline(conn, cfg, "a_race", now)
        assert res_race == {"result": "raced"}
    finally:
        AgentRepo.transition = orig_transition  # type: ignore

    # 4. Branch: call state RESERVED with provider_call_id NULL -> cancelled_directly
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, updated_at) VALUES ('b_res', '+1001', 'IN_CALL', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_res', 'RESERVED', 'c_res', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider_call_id, created_at, updated_at) "
        "VALUES ('c_res', 'b_res', 'a_res', 'RESERVED', 1, NULL, ?, ?);",
        (now, now),
    )
    res_branch_res = agent_went_offline(conn, cfg, "a_res", now)
    assert res_branch_res["result"] == "offline"
    assert res_branch_res["action"] == "cancelled_directly"

    c_res = CallRepo(conn, cfg).get_call("c_res")
    assert c_res["state"] == "CANCELLED"
    assert c_res["end_reason"] == "AGENT_LOST"

    # 5. Branch: call rank >= 4 (ANSWERED / CONNECTED) -> abandoned_terminate
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, updated_at) VALUES ('b_ans', '+1002', 'IN_CALL', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_ans', 'CONNECTED', 'c_ans', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider_call_id, created_at, updated_at) "
        "VALUES ('c_ans', 'b_ans', 'a_ans', 'CONNECTED', 5, 'p_123', ?, ?);",
        (now, now),
    )
    res_branch_ans = agent_went_offline(conn, cfg, "a_ans", now)
    assert res_branch_ans["result"] == "offline"
    assert res_branch_ans["action"] == "abandoned_terminate"

    c_ans = CallRepo(conn, cfg).get_call("c_ans")
    assert c_ans["abandoned"] == 1
    actions = conn.execute("SELECT * FROM call_actions WHERE call_id = 'c_ans';").fetchall()
    assert len(actions) == 1
    assert actions[0]["action"] == "TERMINATE"
    assert actions[0]["reason"] == "AGENT_LOST"

    # 6. Branch: otherwise (e.g. INITIATED/RINGING) -> cancel_enqueued
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, updated_at) VALUES ('b_ring', '+1003', 'IN_CALL', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_ring', 'DIALING', 'c_ring', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider_call_id, created_at, updated_at) "
        "VALUES ('c_ring', 'b_ring', 'a_ring', 'RINGING', 3, 'p_456', ?, ?);",
        (now, now),
    )
    res_branch_ring = agent_went_offline(conn, cfg, "a_ring", now)
    assert res_branch_ring["result"] == "offline"
    assert res_branch_ring["action"] == "cancel_enqueued"

    actions_ring = conn.execute("SELECT * FROM call_actions WHERE call_id = 'c_ring';").fetchall()
    assert len(actions_ring) == 1
    assert actions_ring[0]["action"] == "CANCEL"
    assert actions_ring[0]["reason"] == "AGENT_LOST"


def test_4_agent_offline_in_setup_ringing():
    """4. Agent offline in setup: bound call RINGING, agent goes offline -> CANCEL action -> after process_actions and CANCELLED event: call is CANCELLED, borrower QUEUED unchanged, agent stays OFFLINE."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(nonpenalized_retry_delay_s=5.0)

    now = 100.0
    rng = random.Random(42)
    behavior = BorrowerBehavior(answer_rate=0.0, mean_talk_s=30.0)
    profile = ProviderProfile(setup_latency_s=0.1, latency_jitter_s=0.0, error_rate=0.0, timeout_rate=0.0)
    mock = MockProvider("mock_p", profile, behavior, rng)

    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_ring4', '+1004', 'IN_CALL', 1, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_ring4', 'DIALING', 'c_ring4', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, provider, state, state_rank, provider_call_id, created_at, updated_at) "
        "VALUES ('c_ring4', 'b_ring4', 'a_ring4', 'mock_p', 'RINGING', 3, 'mock_c4', ?, ?);",
        (now, now),
    )
    mock.place_call("c_ring4", "+1004", now=now)

    # Agent goes offline
    res = agent_went_offline(conn, cfg, "a_ring4", now=now)
    assert res["action"] == "cancel_enqueued"

    # Agent is OFFLINE
    ag = conn.execute("SELECT state FROM agents WHERE id = 'a_ring4';").fetchone()
    assert ag["state"] == AgentState.OFFLINE.value

    # Process outbox action
    act_res = process_actions(conn, cfg, {"mock_p": mock}, "worker_1", now=now)
    assert act_res["succeeded"] == 1

    # Provider delivers CANCELLED event
    events = mock.poll_events(now + 2.0)
    cancel_evts = [e for e in events if e.event_type == "CANCELLED"]
    assert len(cancel_evts) == 1

    call_repo = CallRepo(conn, cfg)
    for e in events:
        call_repo.apply_event(e.call_id, e.event_type, provider_event_id=e.provider_event_id, reason=e.reason, now=now + 2.0)

    # Call is CANCELLED
    c = call_repo.get_call("c_ring4")
    assert c["state"] == "CANCELLED"

    # Borrower is QUEUED with attempts unchanged
    b = conn.execute("SELECT state, attempts, next_attempt_at FROM borrowers WHERE id = 'b_ring4';").fetchone()
    assert b["state"] == BorrowerState.QUEUED.value
    assert b["attempts"] == 1  # Unchanged
    assert b["next_attempt_at"] == pytest.approx(now + 2.0 + 5.0)

    # Agent stays OFFLINE
    ag_after = conn.execute("SELECT state FROM agents WHERE id = 'a_ring4';").fetchone()
    assert ag_after["state"] == AgentState.OFFLINE.value


def test_5_cancel_loses_race_rebind_or_abandon():
    """5. Cancel loses the race: provider already answered before cancel processed -> ANSWERED applied: call rebound to AVAILABLE agent if exists, otherwise abandoned."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0

    # Setup bound call with agent A1
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_race5', '+1005', 'IN_CALL', 0, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_race5_1', 'DIALING', 'c_race5', ?);",
        (now,),
    )
    # Available replacement agent A2
    conn.execute(
        "INSERT INTO agents (id, state, updated_at) VALUES ('a_race5_2', 'AVAILABLE', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) "
        "VALUES ('c_race5', 'b_race5', 'a_race5_1', 'RINGING', 3, ?, ?);",
        (now, now),
    )

    # Agent A1 abruptly goes offline
    agent_went_offline(conn, cfg, "a_race5_1", now=now)

    # Provider answers before cancel processed: apply ANSWERED
    call_repo = CallRepo(conn, cfg)
    res = call_repo.apply_event("c_race5", CallState.ANSWERED, now=now + 1.0)
    assert res.value == "APPLIED"

    # Call was rebound to available agent A2
    c = call_repo.get_call("c_race5")
    assert c["agent_id"] == "a_race5_2"
    assert c["abandoned"] == 0

    ag2 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_race5_2';").fetchone()
    assert ag2["state"] == AgentState.CONNECTED.value
    assert ag2["reserved_call_id"] == "c_race5"

    # Variant: same scenario but NO available agent -> abandoned = 1 and TERMINATE enqueued
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_race5_noag', '+1006', 'IN_CALL', 0, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_race5_3', 'DIALING', 'c_race5_noag', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) "
        "VALUES ('c_race5_noag', 'b_race5_noag', 'a_race5_3', 'RINGING', 3, ?, ?);",
        (now, now),
    )
    agent_went_offline(conn, cfg, "a_race5_3", now=now)

    res_noag = call_repo.apply_event("c_race5_noag", CallState.ANSWERED, now=now + 1.0)
    assert res_noag.value == "APPLIED"

    c_noag = call_repo.get_call("c_race5_noag")
    assert c_noag["agent_id"] is None
    assert c_noag["abandoned"] == 1

    actions = conn.execute("SELECT action, reason FROM call_actions WHERE call_id = 'c_race5_noag';").fetchall()
    # Expect TERMINATE action enqueued
    assert any(a["action"] == "TERMINATE" and a["reason"] == "ABANDONED" for a in actions)


def test_6_agent_offline_while_connected():
    """6. Agent offline while CONNECTED: call abandoned = 1 + TERMINATE; after COMPLETED borrower QUEUED (not DONE), end_reason 'ABANDONED'."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(abandon_retry_delay_s=300.0)

    now = 100.0
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_conn6', '+1007', 'IN_CALL', 0, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_conn6', 'CONNECTED', 'c_conn6', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) "
        "VALUES ('c_conn6', 'b_conn6', 'a_conn6', 'CONNECTED', 5, ?, ?);",
        (now, now),
    )

    res = agent_went_offline(conn, cfg, "a_conn6", now=now)
    assert res["action"] == "abandoned_terminate"

    c = conn.execute("SELECT abandoned FROM calls WHERE id = 'c_conn6';").fetchone()
    assert c["abandoned"] == 1

    actions = conn.execute("SELECT action, reason FROM call_actions WHERE call_id = 'c_conn6';").fetchall()
    assert len(actions) == 1
    assert actions[0]["action"] == "TERMINATE"
    assert actions[0]["reason"] == "AGENT_LOST"

    # COMPLETED arrives
    call_repo = CallRepo(conn, cfg)
    call_repo.apply_event("c_conn6", CallState.COMPLETED, now=now + 10.0)

    c_done = call_repo.get_call("c_conn6")
    assert c_done["state"] == "COMPLETED"
    assert c_done["end_reason"] == "ABANDONED"

    b = conn.execute("SELECT state, attempts, next_attempt_at FROM borrowers WHERE id = 'b_conn6';").fetchone()
    assert b["state"] == BorrowerState.QUEUED.value
    assert b["attempts"] == 0
    assert b["next_attempt_at"] == pytest.approx(now + 10.0 + 300.0)


def test_15_part1_guards_and_predictive_zero_talk():
    """15. Part 1 guards: release_to_queued does not resurrect DONE or FAILED_PERMANENT; PredictivePacer zero talk time."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()
    b_repo = BorrowerRepo(conn, cfg)

    now = 100.0

    # 1. DONE borrower cannot be resurrected
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_done', '+1008', 'DONE', 1, ?);",
        (now,),
    )
    res_done = b_repo.release_to_queued("b_done", next_attempt_at=now + 10.0, now=now)
    assert res_done is False
    b_done = b_repo.get_borrower("b_done")
    assert b_done["state"] == BorrowerState.DONE.value

    # 2. FAILED_PERMANENT borrower cannot be resurrected
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_perm', '+1009', 'FAILED_PERMANENT', 3, ?);",
        (now,),
    )
    res_perm = b_repo.release_to_queued("b_perm", next_attempt_at=now + 10.0, now=now)
    assert res_perm is False
    b_perm = b_repo.get_borrower("b_perm")
    assert b_perm["state"] == BorrowerState.FAILED_PERMANENT.value

    # 3. PredictivePacer handles avg_talk_s = 0.0 without exception (clamped to at least 1.0)
    pacer = PredictivePacer(PredictiveConfig(default_talk_s=0.0))
    snap = SystemSnapshot(
        now=now,
        agents_by_state={"AVAILABLE": 10},
        available=10,
        wrap_up=2,
        connected=5,
        online=17,
        unbound_in_flight=0,
        ringing_unbound=0,
        unanswered_unbound=0,
        bound_in_flight=0,
        answered_waiting=0,
        pending_progressive=0,
        pending_predictive=0,
        short=RateStats(answered=20, resolved=40),
        long=RateStats(answered=50, resolved=100),
        abandon=AbandonStats(answered=50, abandoned=0),
        avg_talk_s=0.0,
    )
    req = pacer.propose(snap)
    assert req.requested_calls >= 0
    assert req.rationale["talk"] == 1.0

    # 4. Part 2.2: Bound call whose agent is gone rebinds or abandons on ANSWERED/CONNECTED
    # Seed an available replacement agent
    conn.execute(
        "INSERT INTO agents (id, state, updated_at) VALUES ('a_rep_15', 'AVAILABLE', ?);",
        (now,),
    )
    # Agent a_gone is already OFFLINE and unlinked
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_gone', 'OFFLINE', NULL, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_p22', '+1010', 'IN_CALL', 0, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, created_at, updated_at) "
        "VALUES ('c_p22', 'b_p22', 'a_gone', 'RINGING', 3, ?, ?);",
        (now, now),
    )
    call_repo = CallRepo(conn, cfg)
    res_p22 = call_repo.apply_event("c_p22", CallState.ANSWERED, now=now + 2.0)
    assert res_p22 == ApplyResult.APPLIED
    c_p22 = call_repo.get_call("c_p22")
    assert c_p22["agent_id"] == "a_rep_15"
    assert c_p22["abandoned"] == 0

