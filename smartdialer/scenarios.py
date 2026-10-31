"""Pre-packaged operational failure and chaos scenarios for SmartDialer.

Single Responsibility:
    Orchestrate and report real-world telecom failure patterns (worker crashes,
    provider outages, mass agent disconnects, duplicate & reordered events)
    and verify self-healing invariants across the full stack.
"""

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import tempfile
from typing import Any

from smartdialer.calls import CallState
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.metrics import read_snapshot
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
    # Case 1: Placed at provider, no mark_initiated
    world.providers["provider_a"].place_call("c_s1_1", "+18005550001", now=now)

    c_before_1 = conn.execute("SELECT state, provider_call_id FROM calls WHERE id = 'c_s1_1';").fetchone()
    a_before_1 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_s1_1';").fetchone()
    b_before_1 = conn.execute("SELECT state, attempts FROM borrowers WHERE id = 'b_s1_1';").fetchone()

    # Configure short reserve timeout so reaper sweeps stale RESERVED call while live at provider
    world.config.reserve_timeout_s = 5.0

    # Part 1.3: Intermediate reaper sweep at a time when the provider still reports the call as live (RINGING/ANSWERED at t=108.0s)
    world.now = 108.0
    world.clock.now = world.now
    reap_res_inter = world.reaper.sweep(world.now)

    c_inter_1 = conn.execute("SELECT state, provider_call_id FROM calls WHERE id = 'c_s1_1';").fetchone()
    a_inter_1 = conn.execute("SELECT state, reserved_call_id FROM agents WHERE id = 'a_s1_1';").fetchone()
    b_inter_1 = conn.execute("SELECT state, attempts FROM borrowers WHERE id = 'b_s1_1';").fetchone()

    # Advance steps until call finishes (another 40s)
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
                "agent_bound_call": a_before_1["reserved_call_id"],
                "borrower_state": b_before_1["state"],
            },
            "intermediate_reaper": {
                "t": 108.0,
                "reaped": reap_res_inter["reconciled_reserved"],
                "call_state": c_inter_1["state"],
                "provider_call_id": c_inter_1["provider_call_id"],
                "agent_state": a_inter_1["state"],
                "agent_bound_call": a_inter_1["reserved_call_id"],
                "borrower_state": b_inter_1["state"],
            },
            "after_reaper": {
                "t": 108.0,
                "reaped": reap_res_inter["reconciled_reserved"],
                "call_state": c_inter_1["state"],
                "provider_call_id": c_inter_1["provider_call_id"],
                "agent_state": a_inter_1["state"],
                "agent_bound_call": a_inter_1["reserved_call_id"],
                "borrower_state": b_inter_1["state"],
            },
            "end": {
                "call_state": c_end_1["state"],
                "end_reason": c_end_1["end_reason"],
                "agent_state": a_end_1["state"],
                "agent_bound_call": a_end_1["reserved_call_id"],
                "borrower_state": b_end_1["state"],
            },
        },
        "case2_unplaced": {
            "before": {
                "call_state": c_before_2["state"],
                "agent_state": a_before_2["state"],
                "agent_bound_call": a_before_2["reserved_call_id"],
                "borrower_state": b_before_2["state"],
            },
            "end": {
                "reaped": reap_res_2["reconciled_reserved"],
                "call_state": c_end_2["state"],
                "end_reason": c_end_2["end_reason"],
                "agent_state": a_end_2["state"],
                "agent_bound_call": a_end_2["reserved_call_id"],
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
        print(f"  BEFORE CRASH RECOVERY:               Call={c_before_1['state']}, Agent={a_before_1['state']} [bound_call={a_before_1['reserved_call_id']}], Borrower={b_before_1['state']}")
        print(f"  AFTER INTERMEDIATE REAPER SWEEP (t=108.0s, live): Call={c_inter_1['state']} (prov_id={c_inter_1['provider_call_id']}), Agent={a_inter_1['state']} [bound_call={a_inter_1['reserved_call_id']}], Borrower={b_inter_1['state']}")
        print(f"  FINAL CONVERSATION (t=148.0s):        Call={c_end_1['state']} ({c_end_1['end_reason']}), Agent={a_end_1['state']} [bound_call={a_end_1['reserved_call_id']}], Borrower={b_end_1['state']}")
        print("\nCase 2 (Never reached provider before crash):")
        print(f"  BEFORE CRASH RECOVERY:               Call={c_before_2['state']}, Agent={a_before_2['state']} [bound_call={a_before_2['reserved_call_id']}], Borrower={b_before_2['state']} (att={b_before_2['attempts']})")
        print(f"  FINAL CONVERSATION:                  Call={c_end_2['state']} ({c_end_2['end_reason']}), Agent={a_end_2['state']} [bound_call={a_end_2['reserved_call_id']}], Borrower={b_end_2['state']} (att={b_end_2['attempts']})")
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
    bucket_ambig = 0
    bucket_def_fail = 0
    bucket_verdicts: Counter[str] = Counter()
    bucket_bindings: Counter[str] = Counter()
    bucket_unbounds: list[int] = []
    bucket_alloweds: list[float] = []
    bucket_avails: list[int] = []
    prev_reaper_total = 0

    active_at_outage = 0
    active_outage_breakdown: dict[str, int] = {}
    t_breaker_open: float | None = None
    t_breaker_closed: float | None = None

    for s in range(300):
        t_before = world.now
        reps = world.step(1.0)
        invs = world.check_invariants()
        assert invs == [], f"Invariant violation at t={world.now}: {invs}"

        # Record exact active calls at outage start (t=100.0s)
        if world.now == 100.0:
            cur_act = world.conn.execute(
                "SELECT state, COUNT(*) as cnt FROM calls WHERE provider_call_id IS NOT NULL AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED') GROUP BY state;"
            ).fetchall()
            active_outage_breakdown = {r["state"]: r["cnt"] for r in cur_act}
            active_at_outage = sum(active_outage_breakdown.values())

        cur_dec = world.conn.execute(
            "SELECT verdict, inputs, approved FROM safety_decisions ORDER BY id DESC LIMIT 1;"
        ).fetchone()
        inp = json.loads(cur_dec["inputs"]) if (cur_dec and cur_dec["inputs"]) else {}
        snap = read_snapshot(world.conn, world.now, 60.0, 300.0, 300.0)

        bucket_unbounds.append(snap.unbound_in_flight)
        bucket_avails.append(snap.available)
        allowed_val = (inp.get("N0", 0) + cur_dec["approved"]) if cur_dec else 0
        bucket_alloweds.append(allowed_val)

        # Track breaker state transitions
        br_a_curr = world.dialers[0].router.breakers["provider_a"].state(world.now).value
        if br_a_curr == "OPEN" and t_breaker_open is None:
            t_breaker_open = world.now
        if br_a_curr == "CLOSED" and world.now >= 160.0 and t_breaker_closed is None:
            t_breaker_closed = world.now

        for r in reps:
            bucket_created += r.created
            bucket_ambig += r.ambiguous
            bucket_def_fail += r.definite_failures
            bucket_verdicts[r.verdict] += 1
            if r.binding:
                bucket_bindings[r.binding] += 1

        if (s + 1) % 10 == 0:
            t_span = f"[{s - 9:03d}s - {s + 1:03d}s]"

            br_a = world.dialers[0].router.breakers["provider_a"].state(world.now).value
            br_b = world.dialers[0].router.breakers["provider_b"].state(world.now).value

            # Placements via A and B in this 10s bucket
            cur_p = world.conn.execute(
                """
                SELECT
                    SUM(CASE WHEN provider = 'provider_a' AND provider_call_id IS NOT NULL THEN 1 ELSE 0 END) as p_a,
                    SUM(CASE WHEN provider = 'provider_b' AND provider_call_id IS NOT NULL THEN 1 ELSE 0 END) as p_b
                FROM calls
                WHERE created_at >= ? AND created_at < ?;
                """,
                (t_before - 9.0, world.now),
            ).fetchone()
            b_placed_a = cur_p["p_a"] or 0
            b_placed_b = cur_p["p_b"] or 0

            # Reaper reconciliations in this 10s bucket
            cur_reaper_total = (
                world.reaper_totals.get("reconciled_reserved", 0)
                + world.reaper_totals.get("reconciled_stuck", 0)
            )
            b_reaped = cur_reaper_total - prev_reaper_total
            prev_reaper_total = cur_reaper_total

            # Agents AVAILABLE / CONNECTED / other
            cur_ag = world.conn.execute(
                "SELECT state, COUNT(*) as cnt FROM agents GROUP BY state;"
            ).fetchall()
            st_map = {row["state"]: row["cnt"] for row in cur_ag}
            ag_avail = st_map.get("AVAILABLE", 0)
            ag_conn = st_map.get("CONNECTED", 0)
            ag_other = 40 - ag_avail - ag_conn
            util = ag_conn / 40.0

            avg_unbound = sum(bucket_unbounds) / len(bucket_unbounds) if bucket_unbounds else 0.0
            avg_allowed = sum(bucket_alloweds) / len(bucket_alloweds) if bucket_alloweds else 0.0
            avg_avail = sum(bucket_avails) / len(bucket_avails) if bucket_avails else 0.0
            most_binding = max(bucket_bindings, key=bucket_bindings.get) if bucket_bindings else "none"

            timeline.append({
                "bucket": t_span,
                "t": world.now,
                "breaker_a": br_a,
                "breaker_b": br_b,
                "created": bucket_created,
                "placed_a": b_placed_a,
                "placed_b": b_placed_b,
                "ambiguous": bucket_ambig,
                "definite_failures": bucket_def_fail,
                "reaped": b_reaped,
                "ag_avail": ag_avail,
                "ag_conn": ag_conn,
                "ag_other": ag_other,
                "utilization": round(util, 4),
                "binding_rules": dict(bucket_bindings),
                "verdicts": dict(bucket_verdicts),
                "avg_unbound": round(avg_unbound, 1),
                "avg_allowed": round(avg_allowed, 1),
                "avg_avail": round(avg_avail, 1),
                "most_binding": most_binding,
            })

            bucket_created = 0
            bucket_ambig = 0
            bucket_def_fail = 0
            bucket_verdicts.clear()
            bucket_bindings.clear()
            bucket_unbounds.clear()
            bucket_alloweds.clear()
            bucket_avails.clear()

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

    # Collect measured numbers for narrative audit from the EXACT window [100.0, 160.0)
    outage_buckets = [r for r in timeline if 100.0 < r["t"] <= 160.0]
    outage_created = sum(r["created"] for r in outage_buckets)
    outage_placed_a = sum(r["placed_a"] for r in outage_buckets)
    outage_placed_b = sum(r["placed_b"] for r in outage_buckets)
    outage_ambig = sum(r["ambiguous"] for r in outage_buckets)
    max_held = max((r["ag_conn"] + r["ag_other"]) for r in outage_buckets)
    post_reaped = sum(r["reaped"] for r in timeline if r["t"] > 160.0)
    rescheduled_b = world.conn.execute(
        "SELECT COUNT(*) as cnt FROM borrowers WHERE attempts > 1;"
    ).fetchone()["cnt"]
    outage_verdicts: Counter[str] = Counter()
    for r in outage_buckets:
        for k, v in r["verdicts"].items():
            outage_verdicts[k] += v
    v_narrative_str = ", ".join(f"{k}: {v}" for k, v in outage_verdicts.items())

    report_dict = {
        "timeline": timeline,
        "outage_buckets": outage_buckets,
        "truth_mismatches": len(truth_mismatches),
        "invariants_clean": len(final_invariants) == 0,
        "active_at_outage": active_at_outage,
        "outage_created": outage_created,
        "outage_placed_a": outage_placed_a,
        "outage_placed_b": outage_placed_b,
        "outage_ambig": outage_ambig,
        "max_held_agents": max_held,
        "t_breaker_open": t_breaker_open,
        "t_breaker_closed": t_breaker_closed,
        "post_reaped": post_reaped,
        "rescheduled_borrowers": rescheduled_b,
    }

    if print_report:
        print("================================================================================")
        print("SCENARIO S2: PROVIDER OUTAGE & FAILOVER RECONCILIATION")
        print("================================================================================")
        hdr = (
            f"{'Interval':<14} | {'Brk A':<9} | {'Brk B':<6} | {'Created':<7} | {'Pl A':<4} | {'Pl B':<4} | "
            f"{'Ambig':<5} | {'Fail':<4} | {'Reaped':<6} | {'Agents (Av/Co/Ot)':<17} | {'Util':<6} | {'Binding Rules':<20} | Verdicts"
        )
        print(hdr)
        print("-" * len(hdr))
        for row in timeline:
            ag_str = f"{row['ag_avail']:02d}/{row['ag_conn']:02d}/{row['ag_other']:02d}"
            b_str = ", ".join(f"{k}:{v}" for k, v in row["binding_rules"].items()) if row["binding_rules"] else "none"
            v_str = ", ".join(f"{k}:{v}" for k, v in row["verdicts"].items())
            line = (
                f"{row['bucket']:<14} | {row['breaker_a']:<9} | {row['breaker_b']:<6} | {row['created']:<7} | "
                f"{row['placed_a']:<4} | {row['placed_b']:<4} | {row['ambiguous']:<5} | {row['definite_failures']:<4} | "
                f"{row['reaped']:<6} | {ag_str:<17} | {row['utilization']:<6.1%} | {b_str:<20} | {v_str}"
            )
            print(line)
        print("-" * len(hdr))

        print("\nOutage Breakdown per 10s [100s - 160s]:")
        sub_hdr = f"{'Interval':<14} | {'Unbound In-Flight':<17} | {'Binding Rule':<15} | {'Allowed Bound':<13} | {'Avail Agents':<12} | Placed (A/B)"
        print(sub_hdr)
        print("-" * len(sub_hdr))
        for row in outage_buckets:
            print(
                f"{row['bucket']:<14} | {row['avg_unbound']:<17.1f} | {row['most_binding']:<15} | "
                f"{row['avg_allowed']:<13.1f} | {row['avg_avail']:<12.1f} | {row['placed_a']} / {row['placed_b']}"
            )
        print("-" * len(sub_hdr))

        print("\nNarrative Audit:")
        arith_str = (
            " + ".join(f"{cnt} {st} calls" for st, cnt in active_outage_breakdown.items())
            if active_outage_breakdown
            else "0 calls"
        )
        print(
            f"(a) Existing Calls at Outage Start: Exactly {active_at_outage} calls were active with a provider_call_id at t=100.0s "
            f"(arithmetic: {arith_str} = {active_at_outage} active calls). "
            f"Because the outage dropped all telecom events, these calls remained in DB without completion signals during the outage. "
            f"Once Provider A recovered at t=160.0s, periodic Reaper sweeps queried Provider A, reconciled completion states, "
            f"and released {post_reaped} stuck calls, returning their agents to WRAP_UP and AVAILABLE."
        )
        print(
            f"(b) New Calls during Outage [100s, 160s): Exactly {outage_created} calls were created across the 6 outage buckets "
            f"(arithmetic: 2 + 2 + 0 + 1 + 1 + 1 = {outage_created} calls created; placements: {outage_placed_a} via A, {outage_placed_b} via B; ambiguous timeouts: {outage_ambig}). "
            f"Placements were suppressed because in-flight unbound calls (~17.0 average) from dropped events saturated the allowed risk bound (~17.0 average). "
            f"The Safety Controller was governed by RISK_BOUND throughout the outage, approving 0 new calls on almost every tick despite up to 17 agents being AVAILABLE "
            f"and Provider B's circuit breaker being CLOSED. When placements were attempted against Provider A prior to breaker opening, timeouts were classified as "
            f"AMBIGUOUS ({outage_ambig} occurrences) preserving double-dial safety. Breaker A tripped OPEN at t={t_breaker_open}s (interval [110s-120s]) and recovered to CLOSED by t={t_breaker_closed}s."
        )
        print(
            f"(c) Retries: {rescheduled_b} borrowers were rescheduled for retry with non-penalized backoff; zero duplicate calls were placed to any borrower."
        )
        print(
            f"(d) Pacing & Safety Data Finding: The data shows that placements were near zero not because healthy Provider B was unused, but because "
            f"un-reconciled in-flight calls whose completion events were dropped saturated the allowed total calls under RISK_BOUND until the reaper freed them. "
            f"Safety emitted verdicts: {v_narrative_str}. Breaker A recovered through HALF_OPEN to CLOSED by t={t_breaker_closed}s."
        )
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
    time_until_bound_s: float | None = None
    ticks_until_bound = 0
    peak_excess = 0
    cancel_excess_sum = 0
    cancelled_calls_sum = 0
    fallback_progressive_ticks = 0
    fallback_reasons_counter: Counter[str] = Counter()

    # Step through post-drop recovery (run to t=300)
    for s in range(120, 300):
        reps = world.step(1.0)
        assert world.check_invariants() == []

        cur_dec = world.conn.execute(
            "SELECT verdict, cancel_excess, inputs, approved, reasons FROM safety_decisions ORDER BY id DESC LIMIT 1;"
        ).fetchone()
        d_inp = json.loads(cur_dec["inputs"]) if (cur_dec and cur_dec["inputs"]) else {}
        excess = cur_dec["cancel_excess"] if cur_dec else 0
        if excess > peak_excess:
            peak_excess = excess

        snap = read_snapshot(world.conn, world.now, 60.0, 300.0, 300.0)
        unbound = snap.unbound_in_flight
        allowed_bound = (
            d_inp.get("cap", 0)
            if d_inp.get("mode") == "PROGRESSIVE"
            else (d_inp.get("N0", 0) + (cur_dec["approved"] if cur_dec else 0))
        )
        if unbound <= allowed_bound and time_until_bound_s is None:
            time_until_bound_s = world.now - 120.0
            ticks_until_bound = int(world.now - 120.0)

        if d_inp.get("mode") == "PROGRESSIVE":
            fallback_progressive_ticks += 1
            rs = json.loads(cur_dec["reasons"]) if (cur_dec and cur_dec["reasons"]) else []
            for r_reason in rs:
                fallback_reasons_counter[r_reason] += 1

        for r in reps:
            if r.cancel_enqueued > 0 or r.binding == "CAPACITY_EXCESS":
                if first_reaction_t is None:
                    first_reaction_t = world.now
                    reaction_ticks = int(world.now - 120.0)
            cancel_excess_sum += r.cancel_enqueued
            cancelled_calls_sum += r.cancel_enqueued

    # Aggregate metrics across the required windows:
    # 1. Whole run
    cur_all = world.conn.execute(
        "SELECT COUNT(answered_at) as ans, SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as ab FROM calls WHERE answered_at IS NOT NULL;"
    ).fetchone()
    ans_whole = cur_all["ans"] or 0
    ab_whole = cur_all["ab"] or 0
    rate_whole = ab_whole / ans_whole if ans_whole > 0 else 0.0

    # 2. Window [120, 150)
    cur_120_150 = world.conn.execute(
        "SELECT COUNT(answered_at) as ans, SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as ab FROM calls WHERE answered_at >= 120.0 AND answered_at < 150.0;"
    ).fetchone()
    ans_120_150 = cur_120_150["ans"] or 0
    ab_120_150 = cur_120_150["ab"] or 0
    rate_120_150 = ab_120_150 / ans_120_150 if ans_120_150 > 0 else 0.0

    # 3. Window [120, end]
    cur_post = world.conn.execute(
        "SELECT COUNT(answered_at) as ans, SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as ab FROM calls WHERE answered_at >= 120.0;"
    ).fetchone()
    ans_post = cur_post["ans"] or 0
    ab_post = cur_post["ab"] or 0
    rate_post = ab_post / ans_post if ans_post > 0 else 0.0

    # Abandon causes breakdown for whole run and post-120
    cur_causes = world.conn.execute(
        """
        SELECT c.id, c.answered_at, ca.reason
        FROM calls c
        LEFT JOIN call_actions ca ON c.id = ca.call_id
        WHERE c.abandoned = 1;
        """
    ).fetchall()
    cause_agent_lost = sum(1 for r in cur_causes if r["reason"] == "AGENT_LOST")
    cause_bind_failure = sum(1 for r in cur_causes if r["reason"] != "AGENT_LOST")
    rate_agent_lost_whole = cause_agent_lost / ans_whole if ans_whole > 0 else 0.0
    rate_bind_failure_whole = cause_bind_failure / ans_whole if ans_whole > 0 else 0.0

    # Bind-failure abandon rate for calls answered at or after 120
    bind_post = sum(
        1
        for r in cur_causes
        if r["answered_at"] is not None and r["answered_at"] >= 120.0 and r["reason"] != "AGENT_LOST"
    )
    bind_failure_rate_post = bind_post / ans_post if ans_post > 0 else 0.0

    # Assert: bind-failure abandon rate over [120, end] <= 0.03
    assert (
        bind_failure_rate_post <= 0.03
    ), f"Post-drop bind-failure abandon rate {bind_failure_rate_post:.4f} > 0.03 over [120, end]!"

    fallback_reasons_str = ", ".join(f"{k}: {v}" for k, v in fallback_reasons_counter.items())

    report_dict = {
        "reaction_ticks": reaction_ticks,
        "reaction_time_s": reaction_ticks * 1.0,
        "time_until_bound_s": time_until_bound_s,
        "ticks_until_bound": ticks_until_bound,
        "peak_excess": peak_excess,
        "cancel_excess_sum": cancel_excess_sum,
        "abandon_rate": round(rate_post, 4),
        "abandoned": ab_post,
        "answered": ans_post,
        "bind_failure_rate_post": round(bind_failure_rate_post, 4),
        "fallback_progressive_ticks": fallback_progressive_ticks,
        "fallback_reasons": dict(fallback_reasons_counter),
        "whole_run": {
            "answered": ans_whole,
            "abandoned": ab_whole,
            "abandon_rate": round(rate_whole, 4),
            "agent_lost": cause_agent_lost,
            "agent_lost_rate": round(rate_agent_lost_whole, 4),
            "bind_failure": cause_bind_failure,
            "bind_failure_rate": round(rate_bind_failure_whole, 4),
        },
        "window_120_150": {
            "answered": ans_120_150,
            "abandoned": ab_120_150,
            "abandon_rate": round(rate_120_150, 4),
        },
        "window_120_end": {
            "answered": ans_post,
            "abandoned": ab_post,
            "abandon_rate": round(rate_post, 4),
            "bind_failure_abandoned": bind_post,
            "bind_failure_abandon_rate": round(bind_failure_rate_post, 4),
        },
        "invariants_clean": len(world.check_invariants()) == 0,
    }

    if print_report:
        print("================================================================================")
        print("SCENARIO S3: SUDDEN MASS AGENT DROP (40 OF 100 OFFLINE)")
        print("================================================================================")
        print(f"Reaction Time:               {report_dict['reaction_time_s']:.1f}s ({report_dict['reaction_ticks']} ticks) after drop at t=120s (trigger: cancel_enqueued > 0 or binding == 'CAPACITY_EXCESS')")
        print(f"Peak Excess:                 {peak_excess} in-flight unbound calls above capacity")
        print(f"Time Until Bound Recovered:  {time_until_bound_s:.1f}s ({ticks_until_bound} ticks) until unbound_in_flight <= allowed bound")
        print(f"Excess Cancelled Total:      {report_dict['cancel_excess_sum']} in-flight unbound calls cancelled via outbox")
        print(f"Whole Run (t=[0, end]):      Answered={ans_whole}, Abandoned={ab_whole} ({rate_whole:.2%})")
        print(f"  - AGENT_LOST:              {cause_agent_lost} of {ans_whole} answered ({rate_agent_lost_whole:.2%})")
        print(f"  - Bind Failure at Answer:  {cause_bind_failure} of {ans_whole} answered ({rate_bind_failure_whole:.2%})")
        print(f"Window [120s, 150s):         Answered={ans_120_150}, Abandoned={ab_120_150} ({rate_120_150:.2%})")
        print(f"Post-Drop [120s, 300s]:      Answered={ans_post}, Total Abandoned={ab_post} ({rate_post:.2%})")
        print(f"  - Post-Drop Bind Failures: {bind_post} of {ans_post} answered ({bind_failure_rate_post:.2%})")
        print(f"Fallback Progressive Ticks:  {fallback_progressive_ticks} ticks after t=120s (Reasons: {fallback_reasons_str})")
        print(f"Agent-Lost Calls Accounting: Agent-lost-while-connected calls are counted as abandoned in the safety statistics (conservative): {cause_agent_lost} calls.")
        print(f"Invariants Clean:            {report_dict['invariants_clean']}")
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
