"""Tests for Reaper background janitor, crash reconciliation, and lease expiry."""

from pathlib import Path
import random
import threading
import pytest

from smartdialer.actions import enqueue_action
from smartdialer.calls import CallState
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, BorrowerState
from smartdialer.providers.base import ProviderError
from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.mock import MockProvider, ProviderProfile
from smartdialer.reaper import Reaper
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo


def test_7_worker_crash_stale_reserved_call():
    """7. Worker crash (reaper a): Phase 1 done, provider.place_call placed directly, but no mark_initiated.

    Advance past reserve_timeout_s; sweep -> call INITIATED with provider_call_id, runs to terminal.
    Variant: provider never saw the call -> FAILED reason PROVIDER_ERROR, agent AVAILABLE, borrower QUEUED unchanged.
    """
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(reserve_timeout_s=10.0, nonpenalized_retry_delay_s=5.0)

    now = 100.0
    rng = random.Random(42)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=30.0)
    profile = ProviderProfile(setup_latency_s=0.1, latency_jitter_s=0.0, error_rate=0.0, timeout_rate=0.0)
    mock = MockProvider("prov_a", profile, behavior, rng)
    providers = {"prov_a": mock}

    # Case 1: Provider call WAS placed, but worker crashed before mark_initiated
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_crash1', '+18005550001', 'IN_CALL', 0, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_crash1', 'RESERVED', 'c_crash1', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider, created_at, updated_at) "
        "VALUES ('c_crash1', 'b_crash1', 'a_crash1', 'RESERVED', 1, 'prov_a', ?, ?);",
        (now, now),
    )
    # Physical provider placement happened
    mock.place_call("c_crash1", "+18005550001", now=now)

    reaper = Reaper(conn, cfg, providers)

    # Before reserve_timeout_s: sweep does not touch it
    sweep_early = reaper.sweep(now + 5.0)
    assert sweep_early["reconciled_reserved"] == 0
    c_early = CallRepo(conn, cfg).get_call("c_crash1")
    assert c_early["state"] == "RESERVED"

    # Advance past reserve_timeout_s: sweep reconciles it to INITIATED (or terminal)
    sweep_late = reaper.sweep(now + 15.0)
    assert sweep_late["reconciled_reserved"] == 1

    c_late = CallRepo(conn, cfg).get_call("c_crash1")
    assert c_late["state"] != "RESERVED"
    assert c_late["provider_call_id"] is not None

    # Events flow and call finishes normally
    evts = mock.poll_events(now + 50.0)
    call_repo = CallRepo(conn, cfg)
    for e in evts:
        call_repo.apply_event(e.call_id, e.event_type, provider_event_id=e.provider_event_id, reason=e.reason, now=now + 50.0)

    c_fin = call_repo.get_call("c_crash1")
    assert c_fin["state"] == "COMPLETED"

    # Case 2 (Variant): Provider never saw the call -> FAILED PROVIDER_ERROR, agent AVAILABLE, borrower QUEUED unchanged
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_crash2', '+18005550002', 'IN_CALL', 1, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_crash2', 'RESERVED', 'c_crash2', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider, created_at, updated_at) "
        "VALUES ('c_crash2', 'b_crash2', 'a_crash2', 'RESERVED', 1, 'prov_a', ?, ?);",
        (now, now),
    )

    sweep_var = reaper.sweep(now + 15.0)
    assert sweep_var["reconciled_reserved"] == 1

    c_var = call_repo.get_call("c_crash2")
    assert c_var["state"] == "FAILED"
    assert c_var["end_reason"] == "PROVIDER_ERROR"

    a_var = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_crash2';").fetchone()
    assert a_var["state"] == AgentState.AVAILABLE.value
    assert a_var["reserved_call_id"] is None

    b_var = conn.execute("SELECT state, attempts FROM borrowers WHERE id = 'b_crash2';").fetchone()
    assert b_var["state"] == BorrowerState.QUEUED.value
    assert b_var["attempts"] == 1  # Unchanged attempts


def test_10_expired_leases_and_concurrent_sweeping(tmp_path: Path):
    """10. Expired leases: expired borrower claim and expired agent reservation released; idempotent sweep; concurrent sweeps."""
    db_file = tmp_path / "reaper_leases.db"
    conn = connect(db_file)
    init_schema(conn)
    cfg = Config()

    now = 100.0

    # 1. Expired claimed borrower with no active call
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, claimed_by, lease_expires_at, updated_at) "
        "VALUES ('b_exp_claim', '+18005550010', 'CLAIMED', 'w1', ?, ?);",
        (now - 5.0, now - 10.0),
    )

    # 2. Expired reserved agent with no active call
    conn.execute(
        "INSERT INTO agents (id, state, reserved_by, lease_expires_at, updated_at) "
        "VALUES ('a_exp_res', 'RESERVED', 'w1', ?, ?);",
        (now - 5.0, now - 10.0),
    )

    providers = {}
    reaper = Reaper(conn, cfg, providers)

    # First sweep releases both
    r1 = reaper.sweep(now)
    assert r1["released_borrowers"] == 1
    assert r1["released_agents"] == 1

    b1 = conn.execute("SELECT state FROM borrowers WHERE id = 'b_exp_claim';").fetchone()
    assert b1["state"] == BorrowerState.QUEUED.value

    a1 = conn.execute("SELECT state FROM agents WHERE id = 'a_exp_res';").fetchone()
    assert a1["state"] == AgentState.AVAILABLE.value

    # Second sweep changes nothing (idempotency)
    r2 = reaper.sweep(now)
    assert r2["released_borrowers"] == 0
    assert r2["released_agents"] == 0

    # Concurrent sweeping test with 4 threads
    # Seed 20 expired borrowers and 20 expired agents
    for i in range(20):
        conn.execute(
            "INSERT INTO borrowers (id, phone, state, claimed_by, lease_expires_at, updated_at) VALUES (?, '+1000', 'CLAIMED', 'w1', ?, ?);",
            (f"b_conc_{i}", now - 5.0, now - 10.0),
        )
        conn.execute(
            "INSERT INTO agents (id, state, reserved_by, lease_expires_at, updated_at) VALUES (?, 'RESERVED', 'w1', ?, ?);",
            (f"a_conc_{i}", now - 5.0, now - 10.0),
        )

    barrier = threading.Barrier(4)
    sweep_counts = []
    errors = []

    def sweep_worker():
        t_conn = connect(db_file)
        try:
            t_reaper = Reaper(t_conn, cfg, providers)
            barrier.wait()
            res = t_reaper.sweep(now)
            sweep_counts.append(res)
        except Exception as e:
            errors.append(e)
        finally:
            t_conn.close()

    threads = [threading.Thread(target=sweep_worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Errors in concurrent sweep: {errors}"
    total_rel_b = sum(c["released_borrowers"] for c in sweep_counts)
    total_rel_a = sum(c["released_agents"] for c in sweep_counts)
    assert total_rel_b == 20
    assert total_rel_a == 20

    conn.close()


def test_13_terminate_dead_letter_recovered_by_reaper():
    """13. TERMINATE dead-letter: action_max_attempts exhausted against dead provider; reaper (b) still resolves abandoned call once provider recovers."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(abandon_stuck_timeout_s=10.0)

    now = 100.0
    rng = random.Random(111)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=60.0)
    profile = ProviderProfile(setup_latency_s=0.1, latency_jitter_s=0.0, error_rate=0.0, timeout_rate=0.0)
    mock = MockProvider("prov_recov", profile, behavior, rng)
    providers = {"prov_recov": mock}

    # Place call at provider (it is ANSWERED, then COMPLETED at provider)
    mock.place_call("c_deadletter", "+18005550005", now=now)

    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_dead', '+18005550005', 'IN_CALL', 0, ?);",
        (now,),
    )
    # Abandoned call stuck in DB
    conn.execute(
        "INSERT INTO calls (id, borrower_id, provider, state, state_rank, abandoned, created_at, updated_at) "
        "VALUES ('c_deadletter', 'b_dead', 'prov_recov', 'INITIATED', 2, 1, ?, ?);",
        (now, now),
    )

    # Exhaust action attempts in call_actions (simulating dead provider)
    conn.execute(
        "INSERT INTO call_actions (call_id, action, reason, created_at, attempts, done_at) "
        "VALUES ('c_deadletter', 'TERMINATE', 'ABANDONED', ?, 10, NULL);",
        (now,),
    )

    # Advance time past provider completion (at 100 + latency + talk ~ 170s) and past abandon_stuck_timeout_s
    later = now + 200.0
    reaper = Reaper(conn, cfg, providers)
    sweep_res = reaper.sweep(later)
    assert sweep_res["reconciled_stuck"] == 1

    # Call in DB was resolved to COMPLETED with end_reason 'ABANDONED'
    c_after = CallRepo(conn, cfg).get_call("c_deadletter")
    assert c_after["state"] == "COMPLETED"
    assert c_after["end_reason"] == "ABANDONED"


def test_reaper_live_call_throttled_query_frequency():
    """0.3 Live long call is queried exactly once across 10 sweeps within one timeout window."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(stuck_call_timeout_s=90.0)

    now = 100.0
    rng = random.Random(42)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=1000.0)
    profile = ProviderProfile(setup_latency_s=0.1, latency_jitter_s=0.0, error_rate=0.0, timeout_rate=0.0)
    mock = MockProvider("prov_live", profile, behavior, rng)

    class CountingProviderWrapper:
        def __init__(self, inner: MockProvider):
            self.inner = inner
            self.query_counts: dict[str, int] = {}

        def get_call_status(self, call_id: str, t: float) -> str | None:
            self.query_counts[call_id] = self.query_counts.get(call_id, 0) + 1
            return self.inner.get_call_status(call_id, t)

        def place_call(self, call_id: str, phone: str, t: float):
            return self.inner.place_call(call_id, phone, t)

        def cancel_call(self, call_id: str, t: float) -> bool:
            return self.inner.cancel_call(call_id, t)

        def terminate_call(self, call_id: str, t: float) -> bool:
            return self.inner.terminate_call(call_id, t)

        def poll_events(self, t: float):
            return self.inner.poll_events(t)

        @property
        def name(self) -> str:
            return self.inner.name

    wrapper = CountingProviderWrapper(mock)
    providers = {"prov_live": wrapper}

    # Place call at provider so provider reports live (e.g. ANSWERED)
    mock.place_call("c_long", "+18005550099", now=now)

    # Seed borrower and call in DB
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, updated_at) VALUES ('b_long', '+18005550099', 'IN_CALL', ?);",
        (now,),
    )
    # Call is in ANSWERED with updated_at = now - 95.0 (> 90.0s ago)
    conn.execute(
        "INSERT INTO calls (id, borrower_id, provider, state, state_rank, version, created_at, updated_at) "
        "VALUES ('c_long', 'b_long', 'prov_live', 'ANSWERED', 4, 1, ?, ?);",
        (now - 100.0, now - 95.0),
    )

    reaper = Reaper(conn, cfg, providers)  # type: ignore

    # Run 10 sweeps within a 45-second window (all within the 90s stuck_call_timeout_s window)
    for step in range(10):
        t = now + step * 5.0
        reaper.sweep(t)

    # The call should have been queried on the FIRST sweep, updating its updated_at,
    # and skipped on the remaining 9 sweeps
    assert wrapper.query_counts.get("c_long", 0) == 1

    # Check version was NOT touched
    c_row = conn.execute("SELECT version, updated_at, state FROM calls WHERE id = 'c_long';").fetchone()
    assert c_row["version"] == 1
    assert c_row["updated_at"] == now
    assert c_row["state"] == "ANSWERED"


def test_reaper_reconciles_dropped_completed_frees_connected_agent():
    """0.4 Reconcile a dropped COMPLETED for a CONNECTED call: sweep frees agent into WRAP_UP."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(stuck_call_timeout_s=60.0)

    now = 100.0
    rng = random.Random(42)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=30.0)
    profile = ProviderProfile(setup_latency_s=0.1, latency_jitter_s=0.0, error_rate=0.0, timeout_rate=0.0)
    mock = MockProvider("prov_drop", profile, behavior, rng)
    providers = {"prov_drop": mock}

    # Place call at provider
    mock.place_call("c_stuck_conn", "+18005550088", now=now)

    # Outage on prov_drop that drops events during completion (t=110 to t=160)
    mock.inject_outage(start=now + 5.0, end=now + 60.0, mode="error", events="drop")

    # In DB: call is CONNECTED, agent is CONNECTED, borrower is IN_CALL
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) "
        "VALUES ('a_conn_stuck', 'CONNECTED', 'c_stuck_conn', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, updated_at) "
        "VALUES ('b_conn_stuck', '+18005550088', 'IN_CALL', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, provider, state, state_rank, created_at, updated_at) "
        "VALUES ('c_stuck_conn', 'b_conn_stuck', 'a_conn_stuck', 'prov_drop', 'CONNECTED', 5, ?, ?);",
        (now, now),
    )

    # Advance time past completion and stuck_call_timeout_s: now + 120s
    later = now + 120.0
    # Provider ground truth has completed
    assert mock.get_call_status("c_stuck_conn", later) == "COMPLETED"

    reaper = Reaper(conn, cfg, providers)
    sweep_res = reaper.sweep(later)
    assert sweep_res["reconciled_stuck"] == 1

    # Verify agent was freed from CONNECTED to WRAP_UP
    ag = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_conn_stuck';").fetchone()
    assert ag["state"] == AgentState.WRAP_UP.value
    assert ag["reserved_call_id"] == "c_stuck_conn"

    # Verify call state is COMPLETED and borrower is DONE
    c = conn.execute("SELECT state, end_reason FROM calls WHERE id = 'c_stuck_conn';").fetchone()
    assert c["state"] == "COMPLETED"

    b = conn.execute("SELECT state FROM borrowers WHERE id = 'b_conn_stuck';").fetchone()
    assert b["state"] == BorrowerState.DONE.value

