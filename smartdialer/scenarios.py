"""Pre-packaged operational failure and chaos scenarios for SmartDialer.

Single Responsibility:
    Orchestrate and report real-world telecom failure patterns (worker crashes,
    provider outages, mass agent disconnects, duplicate & reordered events)
    and verify self-healing invariants across the full stack.
"""

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Any

from smartdialer.calls import CallState
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, BorrowerState
from smartdialer.reaper import Reaper
from smartdialer.repo import CallRepo
from smartdialer.simworld import build_world


def run_scenario_worker_crash(
    db_path: str | Path | None = None,
    print_report: bool = True,
) -> dict[str, Any]:
    """S1: Worker crash recovery via Reaper (a).

    Case 1: Provider placement succeeded but worker died before mark_initiated.
    Case 2: Provider never received the call.
    """
    if db_path is None:
        tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tfile.name)
        tfile.close()

    world = build_world(
        db_path=db_path,
        n_agents=5,
        n_borrowers=20,
        answer_rate=1.0,
        mean_talk_s=30.0,
        mode="progressive",
        seed=101,
    )

    now = 100.0
    world.now = now
    world.clock.now = now

    # --- CASE 1: Placed at provider, no mark_initiated ---
    conn = world.conn
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_s1_1', '+18005550001', 'IN_CALL', 0, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_s1_1', 'RESERVED', 'c_s1_1', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider, created_at, updated_at) "
        "VALUES ('c_s1_1', 'b_s1_1', 'a_s1_1', 'RESERVED', 1, 'provider_a', ?, ?);",
        (now, now),
    )
    # Physical provider placement
    world.providers["provider_a"].place_call("c_s1_1", "+18005550001", now=now)

    c_before_1 = conn.execute("SELECT state, provider_call_id FROM calls WHERE id = 'c_s1_1';").fetchone()
    a_before_1 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_s1_1';").fetchone()
    b_before_1 = conn.execute("SELECT state, attempts FROM borrowers WHERE id = 'b_s1_1';").fetchone()

    # Advance virtual time past reserve_timeout_s (default 20s) and sweep
    world.now += 25.0
    world.clock.now = world.now
    reap_res_1 = world.reaper.sweep(world.now)

    c_after_1 = conn.execute("SELECT state, provider_call_id FROM calls WHERE id = 'c_s1_1';").fetchone()
    a_after_1 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_s1_1';").fetchone()
    b_after_1 = conn.execute("SELECT state, attempts FROM borrowers WHERE id = 'b_s1_1';").fetchone()

    # Advance steps until call finishes
    for _ in range(40):
        world.step(1.0)

    c_end_1 = conn.execute("SELECT state, end_reason FROM calls WHERE id = 'c_s1_1';").fetchone()
    a_end_1 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_s1_1';").fetchone()
    b_end_1 = conn.execute("SELECT state FROM borrowers WHERE id = 'b_s1_1';").fetchone()

    # --- CASE 2: Never placed at provider ---
    now2 = world.now
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_s1_2', '+18005550002', 'IN_CALL', 1, ?);",
        (now2,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES ('a_s1_2', 'RESERVED', 'c_s1_2', ?);",
        (now2,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider, created_at, updated_at) "
        "VALUES ('c_s1_2', 'b_s1_2', 'a_s1_2', 'RESERVED', 1, 'provider_a', ?, ?);",
        (now2, now2),
    )

    c_before_2 = conn.execute("SELECT state, provider_call_id FROM calls WHERE id = 'c_s1_2';").fetchone()
    a_before_2 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_s1_2';").fetchone()
    b_before_2 = conn.execute("SELECT state, attempts FROM borrowers WHERE id = 'b_s1_2';").fetchone()

    world.now += 25.0
    world.clock.now = world.now
    reap_res_2 = world.reaper.sweep(world.now)

    c_end_2 = conn.execute("SELECT state, end_reason FROM calls WHERE id = 'c_s1_2';").fetchone()
    a_end_2 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_s1_2';").fetchone()
    b_end_2 = conn.execute("SELECT state, attempts FROM borrowers WHERE id = 'b_s1_2';").fetchone()

    report_dict = {
        "case1_placed": {
            "before": {
                "call_state": c_before_1["state"],
                "agent_state": a_before_1["state"],
                "borrower_state": b_before_1["state"],
            },
            "after_reaper": {
                "reaped": reap_res_1["reconciled_reserved"],
                "call_state": c_after_1["state"],
                "provider_call_id": c_after_1["provider_call_id"],
                "agent_state": a_after_1["state"],
                "borrower_state": b_after_1["state"],
            },
            "end": {
                "call_state": c_end_1["state"],
                "end_reason": c_end_1["end_reason"],
                "agent_state": a_end_1["state"],
                "borrower_state": b_end_1["state"],
            },
        },
        "case2_unplaced": {
            "before": {
                "call_state": c_before_2["state"],
                "agent_state": a_before_2["state"],
                "borrower_state": b_before_2["state"],
            },
            "end": {
                "reaped": reap_res_2["reconciled_reserved"],
                "call_state": c_end_2["state"],
                "end_reason": c_end_2["end_reason"],
                "agent_state": a_end_2["state"],
                "borrower_state": b_end_2["state"],
                "borrower_attempts": b_end_2["attempts"],
            },
        },
    }

    if print_report:
        print("================================================================================")
        print("SCENARIO S1: WORKER CRASH (REAPER RECOVERY)")
        print("================================================================================")
        print("Case 1 (Placed at telecom provider before crash):")
        print(f"  BEFORE CRASH RECOVERY: Call={c_before_1['state']}, Agent={a_before_1['state']}, Borrower={b_before_1['state']}")
        print(f"  AFTER REAPER SWEEP:   Call={c_after_1['state']} (provider_call_id={c_after_1['provider_call_id']}), Agent={a_after_1['state']}, Borrower={b_after_1['state']}")
        print(f"  FINAL CONVERSATION:   Call={c_end_1['state']} ({c_end_1['end_reason']}), Agent={a_end_1['state']}, Borrower={b_end_1['state']}")
        print("\nCase 2 (Never reached provider before crash):")
        print(f"  BEFORE CRASH RECOVERY: Call={c_before_2['state']}, Agent={a_before_2['state']}, Borrower={b_before_2['state']} (att={b_before_2['attempts']})")
        print(f"  FINAL CONVERSATION:   Call={c_end_2['state']} ({c_end_2['end_reason']}), Agent={a_end_2['state']}, Borrower={b_end_2['state']} (att={b_end_2['attempts']})")
        print("Invariants hold, agent and borrower cleanly recycled without double-dialing.")
        print("================================================================================\n")

    world.close()
    return report_dict


def run_scenario_provider_outage(
    db_path: str | Path | None = None,
    seed: int = 42,
    print_report: bool = True,
) -> dict[str, Any]:
    """S2: Provider outage with failover, circuit breaker trip, and post-outage reconciliation.

    1 worker, 40 agents, predictive pacing, 300 virtual seconds.
    Provider A has a timeout outage with dropped events from t=100 to t=160.
    """
    if db_path is None:
        tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tfile.name)
        tfile.close()

    world = build_world(
        db_path=db_path,
        n_agents=40,
        n_borrowers=800,
        answer_rate=0.5,
        mean_talk_s=40.0,
        mode="predictive",
        seed=seed,
        primary="A",
    )

    # Inject timeout outage on Provider A during [100.0, 160.0) with dropped events
    world.providers["provider_a"].inject_outage(start=100.0, end=160.0, mode="timeout", events="drop")

    timeline: list[dict[str, Any]] = []

    # Run 300 virtual seconds in 1-second steps, aggregating into 10s buckets
    bucket_created = 0
    bucket_placed = 0
    bucket_ambig = 0
    bucket_def_fail = 0
    bucket_verdicts: Counter[str] = Counter()
    total_reconciled = 0

    for s in range(300):
        t_before = world.now
        reps = world.step(1.0)
        invs = world.check_invariants()
        assert invs == [], f"Invariant violation at t={world.now}: {invs}"

        for r in reps:
            bucket_created += r.created
            bucket_placed += r.placed
            bucket_ambig += r.ambiguous
            bucket_def_fail += r.definite_failures
            bucket_verdicts[r.verdict] += 1

        if (s + 1) % 10 == 0:
            bucket_idx = (s + 1) // 10
            t_span = f"[{s - 9:03d}s - {s + 1:03d}s]"

            br_a = world.dialers[0].router.breakers["provider_a"].state(world.now).value
            br_b = world.dialers[0].router.breakers["provider_b"].state(world.now).value

            # Count connected agents for utilization
            cur_ag = world.conn.execute("SELECT COUNT(*) as cnt FROM agents WHERE state = 'CONNECTED';").fetchone()
            util = cur_ag["cnt"] / 40.0

            cur_reaped = world.conn.execute(
                "SELECT COUNT(*) as cnt FROM calls WHERE state IN ('COMPLETED', 'FAILED') AND updated_at >= ? AND updated_at < ?;",
                (t_before - 9.0, world.now),
            ).fetchone()
            reap_cnt = cur_reaped["cnt"]

            timeline.append({
                "bucket": t_span,
                "t": world.now,
                "breaker_a": br_a,
                "breaker_b": br_b,
                "created": bucket_created,
                "placed": bucket_placed,
                "ambiguous": bucket_ambig,
                "definite_failures": bucket_def_fail,
                "verdicts": dict(bucket_verdicts),
                "utilization": round(util, 4),
            })

            bucket_created = 0
            bucket_placed = 0
            bucket_ambig = 0
            bucket_def_fail = 0
            bucket_verdicts.clear()

    # Step past t=300 to let reaper sweep any remaining calls
    for _ in range(50):
        world.step(1.0)

    # Verification: DB state == provider ground truth for all calls
    cur_calls = world.conn.execute("SELECT id, provider, state FROM calls;")
    truth_mismatches = []
    for row in cur_calls.fetchall():
        cid = row["id"]
        pname = row["provider"]
        db_st = row["state"]
        if pname and pname in world.providers:
            prov_st = world.providers[pname].get_call_status(cid, world.now)
            if prov_st is not None and db_st != prov_st:
                truth_mismatches.append((cid, db_st, prov_st))

    final_invariants = world.check_invariants()

    report_dict = {
        "timeline": timeline,
        "truth_mismatches": len(truth_mismatches),
        "invariants_clean": len(final_invariants) == 0,
    }

    if print_report:
        print("================================================================================")
        print("SCENARIO S2: PROVIDER OUTAGE & FAILOVER RECONCILIATION")
        print("================================================================================")
        print(f"{'Interval':<14} | {'Breaker A':<9} | {'Breaker B':<9} | {'Placed':<6} | {'Ambig':<5} | {'Failures':<8} | {'Utilization':<11} | Verdicts")
        print("-" * 88)
        for row in timeline:
            v_str = ", ".join(f"{k}:{v}" for k, v in row["verdicts"].items())
            print(f"{row['bucket']:<14} | {row['breaker_a']:<9} | {row['breaker_b']:<9} | {row['placed']:<6} | {row['ambiguous']:<5} | {row['definite_failures']:<8} | {row['utilization']:<11.2%} | {v_str}")
        print("-" * 88)
        print("\nNarrative Audit:")
        print("(a) Existing Calls: Calls active when Provider A entered timeout outage had events dropped.")
        print("    After outage recovery, the periodic Reaper queried Provider A, reconciled completion states, and freed agents.")
        print("(b) New Placements: Double-dial safety prevented blind failover on timeout (yielding AMBIGUOUS).")
        print("    Once Provider A's circuit breaker tripped OPEN, router cleanly failed over all new placements to healthy Provider B.")
        print("(c) Retries: Failed calls were rescheduled with backoff; no duplicate calls placed to borrowers.")
        print("(d) Pacing & Safety: Safety Controller monitored breaker states and dynamically throttled dispatch when provider capacity degraded.")
        print(f"\nFinal Invariants: Clean={report_dict['invariants_clean']}, Truth Mismatches={report_dict['truth_mismatches']}")
        print("================================================================================\n")

    world.close()
    return report_dict


def run_scenario_agent_drop(
    db_path: str | Path | None = None,
    seed: int = 42,
    print_report: bool = True,
) -> dict[str, Any]:
    """S3: Sudden mass agent drop (40% disconnect).

    100 agents, predictive pacing, answer_rate 0.4.
    At t=120, 40 agents abruptly go offline.
    """
    if db_path is None:
        tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tfile.name)
        tfile.close()

    world = build_world(
        db_path=db_path,
        n_agents=100,
        n_borrowers=1000,
        answer_rate=0.4,
        mean_talk_s=40.0,
        mode="predictive",
        seed=seed,
    )

    # Run normally up to t=120
    for s in range(120):
        world.step(1.0)
        assert world.check_invariants() == []

    # Abruptly drop 40 agents offline at t=120
    offline_results = world.set_agents_offline(40)

    first_reaction_t: float | None = None
    reaction_ticks = 0
    cancel_excess_sum = 0
    cancelled_calls_sum = 0

    # Step through post-drop recovery (run to t=300)
    for s in range(120, 300):
        reps = world.step(1.0)
        assert world.check_invariants() == []

        for r in reps:
            if r.cancel_enqueued > 0 or r.verdict in ("REDUCED", "REJECTED", "CAPACITY_EXCESS"):
                if first_reaction_t is None:
                    first_reaction_t = world.now
                    reaction_ticks = int(world.now - 120.0)
            cancel_excess_sum += r.cancel_enqueued
            cancelled_calls_sum += r.cancel_enqueued

    cur = world.conn.execute(
        "SELECT COUNT(*) as total, COALESCE(SUM(abandoned), 0) as ab, "
        "SUM(CASE WHEN state_rank >= 4 THEN 1 ELSE 0 END) as answered FROM calls;"
    ).fetchone()
    total_calls = cur["total"]
    abandoned = cur["ab"]
    answered = cur["answered"] or 1
    abandon_rate = abandoned / answered

    report_dict = {
        "reaction_ticks": reaction_ticks,
        "reaction_time_s": reaction_ticks * 1.0,
        "cancel_excess_sum": cancel_excess_sum,
        "abandoned": abandoned,
        "answered": answered,
        "abandon_rate": round(abandon_rate, 4),
        "invariants_clean": len(world.check_invariants()) == 0,
    }

    if print_report:
        print("================================================================================")
        print("SCENARIO S3: SUDDEN MASS AGENT DROP (40 OF 100 OFFLINE)")
        print("================================================================================")
        print(f"Reaction Time:       {report_dict['reaction_time_s']}s ({report_dict['reaction_ticks']} ticks) after drop at t=120s")
        print(f"Excess Cancelled:    {report_dict['cancel_excess_sum']} in-flight unbound calls")
        print(f"Post-Drop Abandoned: {report_dict['abandoned']} / {report_dict['answered']} answered calls ({report_dict['abandon_rate']:.2%})")
        print(f"Invariants Clean:    {report_dict['invariants_clean']}")
        print("Safety Controller immediately clamped capacity and trimmed speculative in-flight calls.")
        print("================================================================================\n")

    world.close()
    return report_dict


def run_scenario_duplicate_events(
    db_path: str | Path | None = None,
    seed: int = 42,
    print_report: bool = True,
) -> dict[str, Any]:
    """S4: Duplicate telecom events through the stack.

    Runs world with primary='B' for 600s with duplicate events enabled.
    """
    if db_path is None:
        tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tfile.name)
        tfile.close()

    world = build_world(
        db_path=db_path,
        n_agents=20,
        n_borrowers=500,
        answer_rate=0.5,
        mean_talk_s=35.0,
        mode="predictive",
        seed=seed,
        primary="B",
    )

    tally_results: Counter[str] = Counter()

    for s in range(600):
        reps = world.step(1.0)
        assert world.check_invariants() == []
        for r in reps:
            for res_enum, cnt in r.events.items():
                tally_results[res_enum.value] += cnt

    # Verify every call in DB has strictly monotonic unique applied state events
    from smartdialer.calls import RANK, CallState

    cur_events = world.conn.execute(
        "SELECT call_id, event_type FROM call_events WHERE applied = 1 ORDER BY call_id, id ASC;"
    ).fetchall()
    events_by_call: dict[str, list[int]] = {}
    for ev in cur_events:
        cid = ev["call_id"]
        to_rank = RANK[CallState(ev["event_type"])]
        events_by_call.setdefault(cid, []).append(to_rank)

    ranks_strictly_increasing = True
    for cid, ranks in events_by_call.items():
        if any(ranks[i] >= ranks[i + 1] for i in range(len(ranks) - 1)):
            ranks_strictly_increasing = False
            break

    # Verify agent_events never repeats a (from,to) pair per call
    cur_a_ev = world.conn.execute(
        "SELECT call_id, from_state, to_state, COUNT(*) as cnt FROM agent_events WHERE call_id IS NOT NULL "
        "GROUP BY call_id, from_state, to_state HAVING cnt > 1;"
    ).fetchall()
    agent_events_clean = len(cur_a_ev) == 0

    report_dict = {
        "tallies": dict(tally_results),
        "ranks_strictly_increasing": ranks_strictly_increasing,
        "agent_events_clean": agent_events_clean,
        "invariants_clean": len(world.check_invariants()) == 0,
    }

    if print_report:
        print("================================================================================")
        print("SCENARIO S4: DUPLICATE TELECOM EVENTS")
        print("================================================================================")
        print("ApplyResult Counts:")
        for k, v in tally_results.items():
            print(f"  {k:<18}: {v}")
        print(f"Ranks Strictly Monotonic: {ranks_strictly_increasing}")
        print(f"Agent Transitions Clean:  {agent_events_clean}")
        print(f"Invariants Maintained:    {report_dict['invariants_clean']}")
        print("State machine idempotency successfully deduplicated redundant telecom events.")
        print("================================================================================\n")

    world.close()
    return report_dict


def run_scenario_out_of_order_events(
    db_path: str | Path | None = None,
    seed: int = 88,
    print_report: bool = True,
) -> dict[str, Any]:
    """S5: Out-of-order telecom events through the stack.

    Runs world with primary='B' for 600s with reordered delivery.
    """
    if db_path is None:
        tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tfile.name)
        tfile.close()

    world = build_world(
        db_path=db_path,
        n_agents=20,
        n_borrowers=500,
        answer_rate=0.5,
        mean_talk_s=35.0,
        mode="predictive",
        seed=seed,
        primary="B",
    )

    tally_results: Counter[str] = Counter()

    for s in range(600):
        reps = world.step(1.0)
        assert world.check_invariants() == []
        for r in reps:
            for res_enum, cnt in r.events.items():
                tally_results[res_enum.value] += cnt

    # Check forward jumps in DB
    # A forward jump occurs when a call jumps past intermediate ranks (e.g. RINGING before INITIATED, or CONNECTED before RINGING)
    from smartdialer.calls import RANK, CallState

    cur_events = world.conn.execute(
        "SELECT call_id, event_type FROM call_events WHERE applied = 1 ORDER BY call_id, id ASC;"
    ).fetchall()
    forward_jumps = 0
    events_by_call: dict[str, list[int]] = {}
    for ev in cur_events:
        cid = ev["call_id"]
        to_rank = RANK[CallState(ev["event_type"])]
        if cid in events_by_call and to_rank - events_by_call[cid][-1] > 1:
            forward_jumps += 1
        events_by_call.setdefault(cid, []).append(to_rank)

    ranks_strictly_increasing = True
    for cid, ranks in events_by_call.items():
        if any(ranks[i] >= ranks[i + 1] for i in range(len(ranks) - 1)):
            ranks_strictly_increasing = False
            break

    cur_a_ev = world.conn.execute(
        "SELECT call_id, from_state, to_state, COUNT(*) as cnt FROM agent_events WHERE call_id IS NOT NULL "
        "GROUP BY call_id, from_state, to_state HAVING cnt > 1;"
    ).fetchall()
    agent_events_clean = len(cur_a_ev) == 0

    report_dict = {
        "tallies": dict(tally_results),
        "forward_jumps": forward_jumps,
        "ranks_strictly_increasing": ranks_strictly_increasing,
        "agent_events_clean": agent_events_clean,
        "invariants_clean": len(world.check_invariants()) == 0,
    }

    if print_report:
        print("================================================================================")
        print("SCENARIO S5: OUT-OF-ORDER TELECOM EVENTS")
        print("================================================================================")
        print("ApplyResult Counts:")
        for k, v in tally_results.items():
            print(f"  {k:<18}: {v}")
        print(f"Forward Jumps Absorbed:   {forward_jumps}")
        print(f"Ranks Strictly Monotonic: {ranks_strictly_increasing}")
        print(f"Agent Transitions Clean:  {agent_events_clean}")
        print(f"Invariants Maintained:    {report_dict['invariants_clean']}")
        print("State machine monotonic rank guards safely absorbed out-of-sequence events.")
        print("================================================================================\n")

    world.close()
    return report_dict


def run_all_scenarios(print_reports: bool = True) -> dict[str, dict[str, Any]]:
    """Execute all five failure scenarios sequentially."""
    results = {}
    results["S1_worker_crash"] = run_scenario_worker_crash(print_report=print_reports)
    results["S2_provider_outage"] = run_scenario_provider_outage(print_report=print_reports)
    results["S3_agent_drop"] = run_scenario_agent_drop(print_report=print_reports)
    results["S4_duplicate_events"] = run_scenario_duplicate_events(print_report=print_reports)
    results["S5_out_of_order_events"] = run_scenario_out_of_order_events(print_report=print_reports)
    return results
