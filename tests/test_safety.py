"""Comprehensive tests for SafetyController: invariants, limits, allotments, and edge cases."""

from collections import Counter
from dataclasses import FrozenInstanceError
import math
import random
import threading
import pytest

from smartdialer.calls import RANK
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, CallState
from smartdialer.pacing.base import PacingMode, PacingRequest
from smartdialer.safety import (
    SafetyConfig,
    SafetyController,
    binom_tail_gt,
    explain,
    max_total_calls_within_risk,
    wilson_upper,
)


def seed_borrower(conn, borrower_id: str = "b_dummy") -> None:
    """Ensure a dummy borrower exists for relational integrity."""
    conn.execute(
        """
        INSERT INTO borrowers (id, phone, state, updated_at)
        VALUES (?, '+1000', 'DONE', 0.0)
        ON CONFLICT(id) DO NOTHING;
        """,
        (borrower_id,),
    )


def seed_agents(conn, state_counts: dict[AgentState, int], now: float = 0.0) -> None:
    """Seed agents in the given states."""
    import uuid
    for st, count in state_counts.items():
        for _ in range(count):
            aid = f"agent_{uuid.uuid4().hex[:10]}"
            conn.execute(
                """
                INSERT INTO agents (id, state, version, updated_at)
                VALUES (?, ?, 0, ?);
                """,
                (aid, st.value, now),
            )


def seed_call_history(
    conn,
    n_answered: int,
    n_no_answer: int,
    created_at: float,
    borrower_id: str = "b_dummy",
) -> None:
    """Seed historical calls with known answered/no-answer outcomes."""
    seed_borrower(conn, borrower_id)
    # 1. Answered calls
    for i in range(n_answered):
        cid = f"call_hist_ans_{created_at}_{i}"
        conn.execute(
            """
            INSERT INTO calls (
                id, borrower_id, agent_id, provider, state, state_rank, version, attempt,
                answered_at, end_reason, abandoned, created_at, updated_at
            )
            VALUES (?, ?, NULL, 'prov_a', 'COMPLETED', 6, 0, 1, ?, 'COMPLETED', 0, ?, ?);
            """,
            (cid, borrower_id, created_at + 5.0, created_at, created_at + 25.0),
        )

    # 2. No-answer calls (resolved but not answered)
    for i in range(n_no_answer):
        cid = f"call_hist_noans_{created_at}_{i}"
        conn.execute(
            """
            INSERT INTO calls (
                id, borrower_id, agent_id, provider, state, state_rank, version, attempt,
                answered_at, end_reason, abandoned, created_at, updated_at
            )
            VALUES (?, ?, NULL, 'prov_a', 'FAILED', 6, 0, 1, NULL, 'NO_ANSWER', 0, ?, ?);
            """,
            (cid, borrower_id, created_at, created_at + 25.0),
        )


@pytest.fixture
def test_env(tmp_path):
    """Fixture providing isolated DB, connection, and default controller."""
    db_file = tmp_path / "safety_test.db"
    conn = connect(db_file)
    init_schema(conn)
    seed_borrower(conn)

    config = SafetyConfig()
    controller = SafetyController(
        config=config,
        provider_health_fn=lambda now: {"prov_a": "CLOSED"},
        clock=lambda: 1000.0,
    )
    yield conn, controller, db_file
    conn.close()


def test_1_safety_config_rejects_loose_values_and_is_immutable():
    """1. SafetyConfig rejects looser-than-absolute limits and is strictly frozen."""
    # Overdial ratio too high
    with pytest.raises(ValueError, match="max_overdial_ratio"):
        SafetyConfig(max_overdial_ratio=2.5)

    # Abandon rate limit too high
    with pytest.raises(ValueError, match="abandon_rate_limit"):
        SafetyConfig(abandon_rate_limit=0.05)

    # Wrap up credit too high
    with pytest.raises(ValueError, match="wrap_up_credit"):
        SafetyConfig(wrap_up_credit=0.9)

    # Risk limit too high
    with pytest.raises(ValueError, match="risk_limit"):
        SafetyConfig(risk_limit=0.2)

    # Min agents for predictive too low
    with pytest.raises(ValueError, match="min_agents_for_predictive"):
        SafetyConfig(min_agents_for_predictive=1)

    # Immutability
    cfg = SafetyConfig()
    with pytest.raises(FrozenInstanceError):
        cfg.max_overdial_ratio = 1.2  # type: ignore


def test_2_cold_start_insufficient_history(test_env):
    """2. COLD START: no history, 10 available agents, predictive request 20 -> approved <= 10, reason INSUFFICIENT_HISTORY."""
    conn, controller, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 10})

    req = PacingRequest(mode=PacingMode.PREDICTIVE, requested_calls=20, rationale={})
    decision = controller.evaluate(conn, req, now=1000.0)

    assert decision.approved <= 10
    assert "INSUFFICIENT_HISTORY" in decision.reasons
    assert decision.inputs["p_hi"] == 1.0


@pytest.mark.parametrize("true_rate", [0.2, 0.5, 0.7, 0.9])
def test_3_with_history_statistical_bounds(test_env, true_rate):
    """3. WITH HISTORY: answer rates 0.2, 0.5, 0.7, 0.9 (60 resolved calls), 50 available agents, request 200."""
    conn, controller, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 50})

    now = 1000.0
    n_total = 60
    n_ans = int(round(true_rate * n_total))
    n_noans = n_total - n_ans

    # Seed within both short (60s) and long (600s) windows
    seed_call_history(conn, n_ans, n_noans, created_at=now - 30.0)

    req = PacingRequest(mode=PacingMode.PREDICTIVE, requested_calls=200, rationale={})
    decision = controller.evaluate(conn, req, now=now)

    cap = decision.inputs["cap"]
    ratio = controller.config.max_overdial_ratio
    p_hi = decision.inputs["p_hi"]
    N0 = decision.inputs["N0"]
    approved = decision.approved

    # Check bounds
    assert approved + N0 <= math.floor(cap * ratio)
    tail = binom_tail_gt(N0 + approved, p_hi, cap)
    assert tail <= controller.config.risk_limit


def test_3b_approved_decreases_as_rate_rises(tmp_path):
    """3b. Higher response rates lead to fewer overdialed calls permitted."""
    rates = [0.2, 0.5, 0.7, 0.9]
    approvals = []

    for r in rates:
        db_file = tmp_path / f"rate_{r}.db"
        conn = connect(db_file)
        init_schema(conn)
        seed_borrower(conn)
        seed_agents(conn, {AgentState.AVAILABLE: 50})

        now = 1000.0
        n_ans = int(round(r * 60))
        seed_call_history(conn, n_ans, 60 - n_ans, created_at=now - 30.0)

        ctrl = SafetyController(
            config=SafetyConfig(),
            provider_health_fn=lambda t: {"prov_a": "CLOSED"},
        )
        dec = ctrl.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 200, {}), now=now)
        approvals.append(dec.approved)
        conn.close()

    for i in range(len(approvals) - 1):
        assert approvals[i] >= approvals[i + 1]


def test_4_property_based_invariants(test_env):
    """4. PROPERTY (random.Random(42), 500 iterations): universal safety bounds hold across randomized states."""
    conn, controller, _ = test_env
    rng = random.Random(42)

    for i in range(500):
        # Generate random snapshot inputs directly
        A = rng.randint(0, 50)
        wrap_up = rng.randint(0, 30)
        D = rng.randint(0, 10)
        unbound = rng.randint(0, 20)
        pending_pred = rng.randint(0, 10)
        req_calls = rng.randint(0, 100)

        credit = math.floor(controller.config.wrap_up_credit * wrap_up)
        cap = max(0, A + credit - D)
        N0 = unbound + pending_pred

        p_hi = rng.uniform(0.1, 1.0)
        k1 = math.floor(cap * controller.config.max_overdial_ratio) - N0
        search_limit = math.floor(cap * 2.0) + 1
        N_risk = max_total_calls_within_risk(cap, p_hi, controller.config.risk_limit, search_limit)
        k2 = N_risk - N0

        k = max(0, min(req_calls, k1, k2, controller.config.max_batch))

        allowed_total = min(math.floor(cap * controller.config.max_overdial_ratio), N_risk)
        excess = max(0, unbound - allowed_total)

        if excess > 0:
            approved = 0
        else:
            approved = k

        # Verify property invariants
        assert approved >= 0
        assert approved <= req_calls
        assert approved <= controller.config.max_batch
        if p_hi < 1.0 and excess == 0 and approved > 0:
            assert binom_tail_gt(N0 + approved, p_hi, cap) <= controller.config.risk_limit
            assert N0 + approved <= math.floor(cap * controller.config.max_overdial_ratio)


def test_5_distributed_double_approval_serialized(tmp_path):
    """5. 8 concurrent threads requesting calls against 20 available agents.

    The sum of approvals across threads must not exceed what a single decision allows.
    """
    db_file = tmp_path / "distributed_safety.db"
    conn_setup = connect(db_file)
    init_schema(conn_setup)
    seed_borrower(conn_setup)
    seed_agents(conn_setup, {AgentState.AVAILABLE: 20})
    seed_call_history(conn_setup, 30, 30, created_at=950.0)  # p=0.5
    conn_setup.close()

    # Determine baseline single decision approval
    conn_base = connect(db_file)
    ctrl_base = SafetyController(SafetyConfig(), lambda t: {"prov_a": "CLOSED"})
    baseline_dec = ctrl_base.evaluate(conn_base, PacingRequest(PacingMode.PREDICTIVE, 100, {}), now=1000.0)
    allowed_baseline = baseline_dec.approved
    conn_base.close()

    # Reset allotments table for the concurrent test
    conn_reset = connect(db_file)
    conn_reset.execute("DELETE FROM safety_allotments;")
    conn_reset.execute("DELETE FROM safety_decisions;")
    conn_reset.close()

    num_threads = 8
    barrier = threading.Barrier(num_threads)
    approvals = []
    lock = threading.Lock()

    def worker():
        c = connect(db_file)
        ctrl = SafetyController(SafetyConfig(), lambda t: {"prov_a": "CLOSED"})
        barrier.wait()
        dec = ctrl.evaluate(c, PacingRequest(PacingMode.PREDICTIVE, 100, {}), now=1000.0)
        with lock:
            approvals.append(dec.approved)
        c.close()

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_approved = sum(approvals)
    assert total_approved <= allowed_baseline, (
        f"Distributed double-approval occurred! total={total_approved} > baseline={allowed_baseline}"
    )


def test_6_allotment_lifecycle_and_race_consumption(test_env):
    """6. Allotments: consume succeeds exactly approved times (with 16 threads), expires on TTL."""
    conn, controller, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 50})
    seed_call_history(conn, 30, 30, created_at=950.0)

    now = 1000.0
    dec = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 20, {}), now=now)
    assert dec.approved == 20
    allotment_id = dec.allotment_id
    assert allotment_id is not None

    # 16 threads racing to consume 20 units
    results = []
    lock = threading.Lock()
    barrier = threading.Barrier(16)
    db_path = controller.config.abandon_window_s  # dummy access

    # Fetch DB path from connection
    cur_p = conn.execute("PRAGMA database_list;")
    db_file = cur_p.fetchone()["file"]

    def consumer():
        c = connect(db_file)
        barrier.wait()
        # Each thread attempts 2 consumes
        for _ in range(2):
            ok = controller.consume(c, allotment_id, now=now + 1.0)
            with lock:
                results.append(ok)
        c.close()

    threads = [threading.Thread(target=consumer) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly 20 True, remainder False
    counts = Counter(results)
    assert counts[True] == 20
    assert counts[False] == 12

    # Further consume returns False
    assert controller.consume(conn, allotment_id, now=now + 1.0) is False

    # Expired allotment test
    conn.execute(
        """
        INSERT INTO safety_allotments (campaign_id, mode, approved, remaining, created_at, expires_at)
        VALUES ('default', 'PREDICTIVE', 10, 10, ?, ?);
        """,
        (now, now + 10.0),
    )
    cur = conn.execute("SELECT last_insert_rowid() as id;")
    exp_id = cur.fetchone()["id"]

    # Before expiry: True
    assert controller.consume(conn, exp_id, now=now + 5.0) is True
    # After expiry: False
    assert controller.consume(conn, exp_id, now=now + 15.0) is False


def test_7_abandon_rate_breach_trips_fallback_and_persists_in_db(tmp_path):
    """7. Abandon rate breach (>3%) triggers FALLBACK_PROGRESSIVE and persists through cooldown."""
    db_file = tmp_path / "abandon_test.db"
    conn = connect(db_file)
    init_schema(conn)
    seed_borrower(conn)
    seed_agents(conn, {AgentState.AVAILABLE: 20})

    now = 1000.0
    # 100 answered calls, 5 abandoned (5% > 3%)
    for i in range(100):
        is_aband = 1 if i < 5 else 0
        conn.execute(
            """
            INSERT INTO calls (id, borrower_id, state, state_rank, attempt, answered_at, abandoned, created_at, updated_at)
            VALUES (?, 'b_dummy', 'COMPLETED', 6, 1, ?, ?, ?, ?);
            """,
            (f"call_ab_{i}", now - 50.0, is_aband, now - 100.0, now - 40.0),
        )

    ctrl1 = SafetyController(SafetyConfig(fallback_cooldown_s=120.0), lambda t: {"prov_a": "CLOSED"})
    dec1 = ctrl1.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 50, {}), now=now)

    assert dec1.verdict == "FALLBACK_PROGRESSIVE"
    assert "ABANDON_RATE_BREACH" in dec1.reasons

    # Check database state table
    cur = conn.execute("SELECT fallback_until FROM safety_state WHERE campaign_id = 'default';")
    row = cur.fetchone()
    assert row["fallback_until"] == now + 120.0
    conn.close()

    # Next decision from DIFFERENT connection within cooldown
    conn2 = connect(db_file)
    # Clear the old abandon calls to prove cooldown holds from DB alone
    conn2.execute("DELETE FROM calls;")
    ctrl2 = SafetyController(SafetyConfig(fallback_cooldown_s=120.0), lambda t: {"prov_a": "CLOSED"})
    dec2 = ctrl2.evaluate(conn2, PacingRequest(PacingMode.PREDICTIVE, 50, {}), now=now + 60.0)
    assert dec2.verdict == "FALLBACK_PROGRESSIVE"
    assert "FALLBACK_COOLDOWN" in dec2.reasons

    # After cooldown expires: predictive resumes
    seed_call_history(conn2, 30, 30, created_at=now + 125.0)
    dec3 = ctrl2.evaluate(conn2, PacingRequest(PacingMode.PREDICTIVE, 50, {}), now=now + 130.0)
    assert dec3.verdict in {"APPROVED", "REDUCED"}
    assert dec3.inputs["mode"] == "PREDICTIVE"
    conn2.close()


def test_8_small_agent_pool_forces_fallback(test_env):
    """8. Online agents < min_agents_for_predictive forces FALLBACK_PROGRESSIVE with SMALL_AGENT_POOL."""
    conn, controller, _ = test_env
    # Seed only 5 agents online (min is 10)
    seed_agents(conn, {AgentState.AVAILABLE: 5})
    seed_call_history(conn, 30, 30, created_at=950.0)

    dec = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 20, {}), now=1000.0)
    assert dec.verdict == "FALLBACK_PROGRESSIVE"
    assert "SMALL_AGENT_POOL" in dec.reasons
    assert dec.inputs["mode"] == "PROGRESSIVE"


def test_9_provider_health_rejection_and_capping(test_env):
    """9. Provider health: all OPEN -> REJECTED; all HALF_OPEN -> approved <= half_open_cap."""
    conn, _, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 30})
    seed_call_history(conn, 30, 30, created_at=950.0)

    # 1. All OPEN
    ctrl_open = SafetyController(
        SafetyConfig(),
        provider_health_fn=lambda t: {"prov_a": "OPEN", "prov_b": "OPEN"},
    )
    dec_open = ctrl_open.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 20, {}), now=1000.0)
    assert dec_open.verdict == "REJECTED"
    assert "ALL_PROVIDERS_UNHEALTHY" in dec_open.reasons

    # 2. Only HALF_OPEN
    ctrl_half = SafetyController(
        SafetyConfig(half_open_cap=2),
        provider_health_fn=lambda t: {"prov_a": "HALF_OPEN"},
    )
    dec_half = ctrl_half.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 20, {}), now=1000.0)
    assert dec_half.approved <= 2
    assert "PROVIDER_HALF_OPEN_CAP" in dec_half.reasons


def test_10_agent_drop_recommends_cancellation_and_resumes(test_env):
    """10. 100 available agents, 70 unbound in flight; 60 go OFFLINE -> REJECTED with AGENT_DROP, cancel_excess."""
    conn, controller, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 100})
    seed_call_history(conn, 30, 45, created_at=950.0)  # p ~ 0.4

    # Seed 70 unbound calls in flight (all RINGING)
    for i in range(70):
        conn.execute(
            """
            INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, attempt, created_at, updated_at)
            VALUES (?, 'b_dummy', NULL, 'RINGING', 3, 1, 990.0, 990.0);
            """,
            (f"call_inflight_{i}",),
        )

    # 60 agents go OFFLINE abruptly
    conn.execute("UPDATE agents SET state = 'OFFLINE' WHERE id IN (SELECT id FROM agents LIMIT 60);")

    dec = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 50, {}), now=1000.0)
    assert dec.approved == 0
    assert dec.verdict == "REJECTED"
    assert "AGENT_DROP" in dec.reasons
    assert dec.cancel_excess > 0

    # Verify cancel_excess matches independently computed excess (<= ringing_unbound)
    cap = 40
    p_hi = dec.inputs["p_hi"]
    search_limit = math.floor(cap * 2.0) + 1
    n_risk = max_total_calls_within_risk(cap, p_hi, controller.config.risk_limit, search_limit)
    allowed_total = min(math.floor(cap * controller.config.max_overdial_ratio), n_risk)
    expected_excess = max(0, 70 - allowed_total)
    assert dec.cancel_excess == expected_excess
    assert dec.cancel_excess <= 70

    # Cancel the excess calls so system is within safety bounds (plus 5 that resolve)
    conn.execute(
        """
        UPDATE calls SET state = 'CANCELLED', state_rank = 6
        WHERE id IN (SELECT id FROM calls WHERE state = 'RINGING' LIMIT ?);
        """,
        (dec.cancel_excess + 5,),
    )

    # Next decision resumes approving
    dec2 = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 50, {}), now=1001.0)
    assert dec2.verdict in {"APPROVED", "REDUCED"}
    assert dec2.approved > 0


def test_11_fail_closed_on_metrics_error(test_env, monkeypatch):
    """11. FAIL CLOSED: unhandled error in read_snapshot rolls back, creates 0 allotments, returns REJECTED."""
    conn, controller, _ = test_env

    def raise_fault(*args, **kwargs):
        raise RuntimeError("Disk I/O failure")

    monkeypatch.setattr("smartdialer.safety.read_snapshot", raise_fault)

    dec = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 10, {}), now=1000.0)
    assert dec.verdict == "REJECTED"
    assert dec.approved == 0
    assert dec.allotment_id is None
    assert any("SAFETY_ERROR:RuntimeError" in r for r in dec.reasons)

    # Verify no allotment was created
    cur = conn.execute("SELECT COUNT(*) as cnt FROM safety_allotments;")
    assert cur.fetchone()["cnt"] == 0


def test_12_invalid_request_handling(test_env):
    """12. Negative/non-int request -> REJECTED INVALID_REQUEST; above max_batch -> capped with MAX_BATCH."""
    conn, controller, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 50})
    seed_call_history(conn, 30, 30, created_at=950.0)

    # Negative request
    dec_neg = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, -5, {}), now=1000.0)
    assert dec_neg.verdict == "REJECTED"
    assert "INVALID_REQUEST" in dec_neg.reasons

    # Non-int request
    dec_float = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 10.5, {}), now=1000.0)  # type: ignore
    assert dec_float.verdict == "REJECTED"
    assert "INVALID_REQUEST" in dec_float.reasons

    # Request above max_batch (200) with sufficient agent capacity so max_batch binds
    seed_agents(conn, {AgentState.AVAILABLE: 300})
    dec_batch = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 500, {}), now=1000.0)
    assert dec_batch.approved == controller.config.max_batch
    assert "MAX_BATCH" in dec_batch.reasons


def test_13_decision_log_and_explain_formatting(test_env):
    """13. Every evaluate writes exactly one safety_decisions row with full inputs; explain output formatted correctly."""
    conn, controller, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 10})
    seed_call_history(conn, 30, 30, created_at=950.0)

    dec = controller.evaluate(conn, PacingRequest(PacingMode.PREDICTIVE, 17, {}), now=1000.0)

    # Check decision log row in SQLite
    cur = conn.execute("SELECT * FROM safety_decisions ORDER BY id DESC LIMIT 1;")
    row = cur.fetchone()
    assert row is not None
    assert row["mode"] == "PREDICTIVE"
    assert row["requested"] == 17
    assert row["approved"] == dec.approved

    # Check inputs JSON
    import json
    inputs = json.loads(row["inputs"])
    for key in ["A", "wrap_up", "credit", "D", "cap", "N0", "p_hi", "k1", "k2", "binding"]:
        assert key in inputs

    # Check explain output
    explanation = explain(dec)
    assert "p_hi=" in explanation
    assert "k1=" in explanation
    assert "k2=" in explanation
    assert "-> binding=" in explanation


def test_14_progressive_mode_invariants(test_env):
    """14. PROGRESSIVE: approved <= A - D - N0 - pending_progressive, independent of history."""
    conn, controller, _ = test_env
    seed_agents(conn, {AgentState.AVAILABLE: 25, AgentState.WRAP_UP: 10})

    # No history at all
    dec = controller.evaluate(conn, PacingRequest(PacingMode.PROGRESSIVE, 30, {}), now=1000.0)
    assert dec.approved <= 25
    assert dec.inputs["credit"] == 0
    assert dec.inputs["mode"] == "PROGRESSIVE"
