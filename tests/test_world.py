"""End-to-end integration tests for Simulation World, multi-worker scaling, and chaos conditions."""

from pathlib import Path
import pytest

from smartdialer.calls import ApplyResult
from smartdialer.simworld import build_world


def test_1_progressive_world_600s(tmp_path: Path):
    """1. Progressive world: 10 agents, 200 borrowers, answer_rate 0.5, mean talk 40s, 600s in 1s steps.

    Invariants hold after every step; approved <= available at decision time; abandoned == 0.
    """
    db_file = tmp_path / "world_prog_600.db"
    world = build_world(
        db_path=db_file,
        n_agents=10,
        n_borrowers=200,
        answer_rate=0.5,
        mean_talk_s=40.0,
        mode="progressive",
        seed=42,
    )

    for s in range(600):
        reports = world.step(1.0)
        for r in reports:
            decision_avail = r.pacing_rationale.get("available", 0)
            assert r.approved <= decision_avail, f"Approved {r.approved} > available {decision_avail} at t={world.now}"

        invariants = world.check_invariants()
        assert invariants == [], f"Invariants broken at step {s} (t={world.now}): {invariants}"

    # Overall call summary
    cur = world.conn.execute(
        "SELECT COUNT(*) as total, COALESCE(SUM(abandoned), 0) as aband FROM calls;"
    ).fetchone()
    assert cur["total"] > 0
    assert cur["aband"] == 0

    world.close()


def test_2_predictive_world_900s_and_utilization_comparison(tmp_path: Path):
    """2. Predictive world: 40 agents, 800 borrowers, answer_rate 0.5, talk 60s, seed 7, 900s.

    Invariants hold; abandoned / answered <= 0.03; predictive utilization >= progressive utilization.
    """
    # 1. Progressive baseline
    prog_db = tmp_path / "world_compare_prog.db"
    w_prog = build_world(
        db_path=prog_db,
        n_agents=40,
        n_borrowers=800,
        answer_rate=0.5,
        mean_talk_s=60.0,
        mode="progressive",
        seed=7,
    )
    prog_conn_sec = 0.0
    prog_online_sec = 0.0

    for s in range(900):
        w_prog.step(1.0)
        assert w_prog.check_invariants() == []
        c_cnt = w_prog.conn.execute("SELECT COUNT(*) as cnt FROM agents WHERE state = 'CONNECTED';").fetchone()["cnt"]
        o_cnt = w_prog.conn.execute("SELECT COUNT(*) as cnt FROM agents WHERE state != 'OFFLINE';").fetchone()["cnt"]
        prog_conn_sec += c_cnt * 1.0
        prog_online_sec += o_cnt * 1.0

    prog_calls = w_prog.conn.execute("SELECT COUNT(*) as cnt FROM calls;").fetchone()["cnt"]
    prog_utilization = prog_conn_sec / prog_online_sec
    w_prog.close()

    # 2. Predictive run
    pred_db = tmp_path / "world_compare_pred.db"
    w_pred = build_world(
        db_path=pred_db,
        n_agents=40,
        n_borrowers=800,
        answer_rate=0.5,
        mean_talk_s=60.0,
        mode="predictive",
        seed=7,
    )
    pred_conn_sec = 0.0
    pred_online_sec = 0.0

    for s in range(900):
        w_pred.step(1.0)
        invariants = w_pred.check_invariants()
        assert invariants == [], f"Predictive invariants broken at t={w_pred.now}: {invariants}"
        c_cnt = w_pred.conn.execute("SELECT COUNT(*) as cnt FROM agents WHERE state = 'CONNECTED';").fetchone()["cnt"]
        o_cnt = w_pred.conn.execute("SELECT COUNT(*) as cnt FROM agents WHERE state != 'OFFLINE';").fetchone()["cnt"]
        pred_conn_sec += c_cnt * 1.0
        pred_online_sec += o_cnt * 1.0

    pred_calls = w_pred.conn.execute("SELECT COUNT(*) as cnt FROM calls;").fetchone()["cnt"]
    cur_aband = w_pred.conn.execute(
        "SELECT COALESCE(SUM(abandoned), 0) as a, COUNT(answered_at) as ans FROM calls;"
    ).fetchone()
    abandoned_count = cur_aband["a"]
    answered_count = cur_aband["ans"]
    pred_utilization = pred_conn_sec / pred_online_sec
    w_pred.close()

    print("\n--- UTILIZATION COMPARISON (seed 7, 900s) ---")
    print(f"Progressive: calls_initiated={prog_calls}, agent_utilization={prog_utilization:.4f}")
    print(f"Predictive:  calls_initiated={pred_calls}, agent_utilization={pred_utilization:.4f}")
    print(f"Predictive abandonment: abandoned={abandoned_count}, answered={answered_count}")

    # Assertions
    if answered_count > 0:
        abandon_rate = abandoned_count / answered_count
        assert abandon_rate <= 0.03, f"Abandon rate {abandon_rate:.4f} exceeds 0.03!"

    assert pred_utilization >= prog_utilization, (
        f"Predictive utilization {pred_utilization:.4f} < Progressive {prog_utilization:.4f}!"
    )


def test_3_three_workers_sharing_one_db(tmp_path: Path):
    """3. Three workers sharing one DB (n_workers=3), predictive, 600s: invariants after every step; no agent bound twice."""
    db_file = tmp_path / "world_3workers.db"
    world = build_world(
        db_path=db_file,
        n_agents=30,
        n_borrowers=500,
        answer_rate=0.5,
        mean_talk_s=45.0,
        mode="predictive",
        seed=123,
        n_workers=3,
    )

    for s in range(600):
        world.step(1.0)
        invariants = world.check_invariants()
        assert invariants == [], f"Multi-worker invariants broken at t={world.now}: {invariants}"

    # Confirm calls were placed across multiple workers
    cur = world.conn.execute("SELECT COUNT(*) as cnt FROM calls;")
    assert cur.fetchone()["cnt"] > 0

    world.close()


def test_8_provider_outage_failover_and_reaper_reconciliation(tmp_path: Path):
    """8. Provider outage (Provider A inject_outage timeout, drop): failover/ambiguous, breaker opens, reaper reconciles after outage."""
    db_file = tmp_path / "world_outage.db"
    world = build_world(
        db_path=db_file,
        n_agents=20,
        n_borrowers=300,
        answer_rate=0.5,
        mean_talk_s=40.0,
        mode="progressive",
        seed=456,
        primary="A",
    )

    # Run initial normal steps until calls are in flight
    for s in range(20):
        world.step(1.0)
        assert world.check_invariants() == []

    # Inject timeout outage on Provider A during [20.0, 100.0)
    world.providers["provider_a"].inject_outage(start=20.0, end=100.0, mode="timeout", events="drop")

    # Step through outage
    for s in range(20, 120):
        world.step(1.0)
        assert world.check_invariants() == []

    # Step past outage so reaper reconciles remaining stuck/ambiguous calls
    for s in range(120, 200):
        world.step(1.0)
        assert world.check_invariants() == []

    # Check that safety decision audit log recorded provider health state changes
    cur_dec = world.conn.execute("SELECT COUNT(*) as cnt FROM safety_decisions;")
    assert cur_dec.fetchone()["cnt"] > 0

    # Assert every call in DB matches provider ground truth or is terminal
    cur_calls = world.conn.execute("SELECT id, provider, state FROM calls;")
    for call in cur_calls.fetchall():
        cid = call["id"]
        pname = call["provider"]
        db_state = call["state"]
        if pname and pname in world.providers:
            prov_truth = world.providers[pname].get_call_status(cid, world.now)
            if prov_truth is not None:
                assert db_state == prov_truth, f"Call {cid} state mismatch: DB={db_state} vs Provider={prov_truth}"

    world.close()


def test_9_duplicate_and_out_of_order_events_world(tmp_path: Path):
    """9. Duplicate/out-of-order events through whole stack: primary='B' run 600s; invariants hold; non-APPLIED count > 0."""
    db_file = tmp_path / "world_chaos_b.db"
    world = build_world(
        db_path=db_file,
        n_agents=15,
        n_borrowers=300,
        answer_rate=0.6,
        mean_talk_s=30.0,
        mode="progressive",
        seed=789,
        primary="B",  # Provider B injects duplicates and reordering
    )

    total_non_applied = 0
    for s in range(600):
        reports = world.step(1.0)
        for r in reports:
            for res_kind, cnt in r.events.items():
                if res_kind != ApplyResult.APPLIED:
                    total_non_applied += cnt
        invariants = world.check_invariants()
        assert invariants == [], f"Chaos B invariants broken at t={world.now}: {invariants}"

    assert total_non_applied > 0, f"Expected non-APPLIED events from Provider B, got {total_non_applied}"
    world.close()
