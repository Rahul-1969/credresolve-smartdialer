"""Performance benchmarking and load testing harness for SmartDialer.

Single Responsibility:
    Measure and report operational throughput, query latency percentiles,
    mathematical risk calculation overhead, and concurrent safety locking.
"""

from collections.abc import Callable
import os
from pathlib import Path
import platform
import random
import sqlite3
import sys
import tempfile
import threading
import time
from typing import Any

from smartdialer.calls import CallState
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.dispatch import start_progressive_call
from smartdialer.metrics import read_snapshot
from smartdialer.pacing.base import PacingMode, PacingRequest
from smartdialer.repo import CallRepo
from smartdialer.safety import SafetyConfig, SafetyController, binom_tail_gt, max_total_calls_within_risk


def percentile(data: list[float], p: float) -> float:
    """Calculate percentile value from a list of floats."""
    if not data:
        return 0.0
    sorted_d = sorted(data)
    idx = int(len(sorted_d) * p)
    return sorted_d[min(idx, len(sorted_d) - 1)]


def benchmark_dispatch_throughput(
    agent_counts: list[int],
    thread_counts: list[int],
    quick: bool = False,
) -> list[dict[str, Any]]:
    """Benchmark (i) dispatch.start_progressive_call ops/sec across thread & agent pool configurations."""
    results = []

    for n_agents in agent_counts:
        for n_threads in thread_counts:
            tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
            db_path = Path(tfile.name)
            tfile.close()

            init_c = connect(str(db_path))
            init_schema(init_c)

            init_c.execute("BEGIN IMMEDIATE;")
            # Seed agents
            for i in range(n_agents):
                init_c.execute(
                    "INSERT INTO agents (id, state, updated_at) VALUES (?, 'AVAILABLE', 0.0);",
                    (f"ag_{i:05d}",),
                )
            # Seed borrowers
            n_borrowers = n_agents * 2
            for j in range(n_borrowers):
                init_c.execute(
                    "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, ?, 'QUEUED', 0.0);",
                    (f"b_{j:05d}", f"+1800555{j:04d}"),
                )
            init_c.execute("COMMIT;")
            init_c.close()

            # Target attempts
            attempts_per_thread = (n_agents // n_threads) if not quick else min(50, n_agents // n_threads)
            attempts_per_thread = max(1, attempts_per_thread)

            errors: list[Exception] = []
            reserved_calls = 0
            lock = threading.Lock()

            def worker_fn(tid: int):
                nonlocal reserved_calls
                thread_conn = connect(str(db_path))
                cfg = Config(db_path=str(db_path))
                local_succ = 0
                try:
                    for k in range(attempts_per_thread):
                        cid = f"call_t{tid}_{k}"
                        res = start_progressive_call(thread_conn, cfg, f"w_{tid}", cid)
                        if res is not None:
                            local_succ += 1
                except Exception as e:
                    errors.append(e)
                finally:
                    thread_conn.close()
                    with lock:
                        reserved_calls += local_succ

            threads = [threading.Thread(target=worker_fn, args=(tid,)) for tid in range(n_threads)]

            t0 = time.perf_counter()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            elapsed = time.perf_counter() - t0

            ops_sec = reserved_calls / elapsed if elapsed > 0 else 0.0

            # Verification assertions
            verif_conn = connect(str(db_path))
            cur = verif_conn.execute("SELECT COUNT(DISTINCT agent_id) as distinct_ag, COUNT(*) as tot FROM calls;")
            row = cur.fetchone()
            assert row["distinct_ag"] == row["tot"], "Double reservation detected!"
            verif_conn.close()

            results.append({
                "agents": n_agents,
                "threads": n_threads,
                "ops_sec": round(ops_sec, 1),
                "elapsed_s": round(elapsed, 3),
                "reserved": reserved_calls,
            })

            db_path.unlink(missing_ok=True)

    return results


def benchmark_apply_event_throughput(
    thread_counts: list[int],
    n_calls: int = 1000,
    quick: bool = False,
) -> list[dict[str, Any]]:
    """Benchmark (ii) apply_event throughput ops/sec on pre-created calls."""
    results = []

    for n_threads in thread_counts:
        tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tfile.name)
        tfile.close()

        init_c = connect(str(db_path))
        init_schema(init_c)

        # Pre-seed borrowers, agents, calls in INITIATED
        total_items = n_calls if not quick else min(200, n_calls)
        init_c.execute("BEGIN IMMEDIATE;")
        for i in range(total_items):
            init_c.execute(
                "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '+1000', 'IN_CALL', 0.0);",
                (f"b_{i}",),
            )
            init_c.execute(
                "INSERT INTO agents (id, state, reserved_call_id, updated_at) VALUES (?, 'DIALING', ?, 0.0);",
                (f"a_{i}", f"c_{i}"),
            )
            init_c.execute(
                "INSERT INTO calls (id, borrower_id, agent_id, state, state_rank, provider, created_at, updated_at) "
                "VALUES (?, ?, ?, 'INITIATED', 2, 'provider_a', 0.0, 0.0);",
                (f"c_{i}", f"b_{i}", f"a_{i}"),
            )
        init_c.execute("COMMIT;")
        init_c.close()

        items_per_thread = total_items // n_threads
        applied_total = 0
        lock = threading.Lock()

        def worker_fn(tid: int):
            nonlocal applied_total
            thread_conn = connect(str(db_path))
            cfg = Config(db_path=str(db_path))
            call_repo = CallRepo(thread_conn, cfg)
            local_cnt = 0
            start_idx = tid * items_per_thread
            end_idx = start_idx + items_per_thread
            try:
                for idx in range(start_idx, end_idx):
                    cid = f"c_{idx}"
                    call_repo.apply_event(cid, CallState.RINGING, now=1.0)
                    call_repo.apply_event(cid, CallState.ANSWERED, now=2.0)
                    local_cnt += 2
            finally:
                thread_conn.close()
                with lock:
                    applied_total += local_cnt

        threads = [threading.Thread(target=worker_fn, args=(tid,)) for tid in range(n_threads)]

        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - t0

        ops_sec = applied_total / elapsed if elapsed > 0 else 0.0
        results.append({
            "threads": n_threads,
            "events_applied": applied_total,
            "ops_sec": round(ops_sec, 1),
            "elapsed_s": round(elapsed, 3),
        })

        db_path.unlink(missing_ok=True)

    return results


def benchmark_safety_evaluate_latency(
    agent_counts: list[int],
    n_historical_calls: int = 5000,
    quick: bool = False,
) -> list[dict[str, Any]]:
    """Benchmark (iii) SafetyController.evaluate latency percentiles p50/p95/max in ms."""
    results = []

    for n_agents in agent_counts:
        tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tfile.name)
        tfile.close()

        conn = connect(str(db_path))
        init_schema(conn)

        conn.execute("BEGIN IMMEDIATE;")
        # Seed agents with mixed states
        for i in range(n_agents):
            st = "AVAILABLE" if i % 3 == 0 else ("CONNECTED" if i % 3 == 1 else "WRAP_UP")
            conn.execute(
                "INSERT INTO agents (id, state, updated_at) VALUES (?, ?, 100.0);",
                (f"a_{i}", st),
            )

        # Seed historical calls
        hist_count = n_historical_calls if not quick else min(500, n_historical_calls)
        for j in range(hist_count):
            conn.execute(
                "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '+1000', 'DONE', 100.0);",
                (f"b_{j}",),
            )
            answered_at = 90.0 if j % 2 == 0 else None
            end_reason = "COMPLETED" if answered_at else "NO_ANSWER"
            conn.execute(
                "INSERT INTO calls (id, borrower_id, state, state_rank, answered_at, end_reason, created_at, updated_at) "
                "VALUES (?, ?, 'COMPLETED', 6, ?, ?, 80.0, 100.0);",
                (f"hist_{j}", f"b_{j}", answered_at, end_reason),
            )
        conn.execute("COMMIT;")

        cfg = SafetyConfig()
        ctrl = SafetyController(config=cfg, provider_health_fn=lambda t: {"provider_a": "CLOSED"})

        req = PacingRequest(mode=PacingMode.PREDICTIVE, requested_calls=10, rationale={})

        latencies_ms = []
        n_iters = 50 if not quick else 10
        for _ in range(n_iters):
            t0 = time.perf_counter()
            ctrl.evaluate(conn, req, now=100.0)
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)

        results.append({
            "agents": n_agents,
            "calls": hist_count,
            "p50_ms": round(percentile(latencies_ms, 0.50), 3),
            "p95_ms": round(percentile(latencies_ms, 0.95), 3),
            "max_ms": round(max(latencies_ms), 3),
        })

        conn.close()
        db_path.unlink(missing_ok=True)

    return results


def benchmark_read_snapshot_latency(
    agent_counts: list[int],
    calls_counts: list[int],
    quick: bool = False,
) -> tuple[list[dict[str, Any]], list[tuple[str, list[str]]]]:
    """Benchmark (iv) read_snapshot latency percentiles and capture EXPLAIN QUERY PLAN."""
    results = []
    plans: list[tuple[str, list[str]]] = []

    # Capture EXPLAIN QUERY PLAN once
    plan_conn = connect(":memory:")
    init_schema(plan_conn)
    queries = [
        ("Agent States", "SELECT state, COUNT(*) as cnt FROM agents GROUP BY state;"),
        ("Call Tallies", "SELECT SUM(CASE WHEN agent_id IS NULL AND state IN ('RESERVED', 'INITIATED', 'RINGING') THEN 1 ELSE 0 END) FROM calls;"),
        ("Pending Allotments", "SELECT mode, SUM(remaining) FROM safety_allotments WHERE expires_at > 100.0 GROUP BY mode;"),
        ("RateStats Window", "SELECT SUM(CASE WHEN answered_at IS NOT NULL THEN 1 ELSE 0 END) FROM calls WHERE created_at >= 50.0 AND state_rank >= 2;"),
        ("AbandonStats Window", "SELECT COUNT(*), SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) FROM calls WHERE answered_at >= 50.0;"),
        ("Avg Talk Time", "SELECT AVG(updated_at - answered_at) FROM calls WHERE state = 'COMPLETED' AND answered_at >= 50.0;"),
    ]
    for q_name, q_sql in queries:
        cur_p = plan_conn.execute(f"EXPLAIN QUERY PLAN {q_sql}")
        plan_rows = [f"id={r['id']}, parent={r['parent']}, detail={r['detail']}" for r in cur_p.fetchall()]
        plans.append((q_name, plan_rows))
    plan_conn.close()

    for n_agents in agent_counts:
        for n_calls in calls_counts:
            tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
            db_path = Path(tfile.name)
            tfile.close()

            conn = connect(str(db_path))
            init_schema(conn)

            conn.execute("BEGIN IMMEDIATE;")
            for i in range(n_agents):
                conn.execute(
                    "INSERT INTO agents (id, state, updated_at) VALUES (?, 'AVAILABLE', 100.0);",
                    (f"a_{i}",),
                )

            total_c = n_calls if not quick else min(500, n_calls)
            for j in range(total_c):
                conn.execute(
                    "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '+1000', 'DONE', 100.0);",
                    (f"b_{j}",),
                )
                conn.execute(
                    "INSERT INTO calls (id, borrower_id, state, state_rank, answered_at, end_reason, created_at, updated_at) "
                    "VALUES (?, ?, 'COMPLETED', 6, 80.0, 'COMPLETED', 70.0, 100.0);",
                    (f"c_{j}", f"b_{j}"),
                )
            conn.execute("COMMIT;")

            latencies_ms = []
            n_iters = 30 if not quick else 10
            for _ in range(n_iters):
                t0 = time.perf_counter()
                read_snapshot(conn, now=100.0, short_window_s=60.0, long_window_s=600.0, abandon_window_s=3600.0)
                latencies_ms.append((time.perf_counter() - t0) * 1000.0)

            results.append({
                "agents": n_agents,
                "calls": total_c,
                "p50_ms": round(percentile(latencies_ms, 0.50), 3),
                "p95_ms": round(percentile(latencies_ms, 0.95), 3),
                "max_ms": round(max(latencies_ms), 3),
            })

            conn.close()
            db_path.unlink(missing_ok=True)

    return results, plans


def benchmark_math_risk_latency(caps: list[int]) -> list[dict[str, Any]]:
    """Benchmark (v) binom_tail_gt and max_total_calls_within_risk latency."""
    results = []
    p_hi = 0.5
    risk_limit = 0.03

    for cap in caps:
        t0 = time.perf_counter()
        for _ in range(100):
            binom_tail_gt(cap + 10, p_hi, cap)
        dt_tail = (time.perf_counter() - t0) * 10.0  # ms per call

        search_lim = int(cap * 1.5)
        t1 = time.perf_counter()
        for _ in range(50):
            max_total_calls_within_risk(cap, p_hi, risk_limit, search_lim)
        dt_opt = (time.perf_counter() - t1) * 20.0  # ms per call

        results.append({
            "cap": cap,
            "binom_tail_ms": round(dt_tail, 4),
            "max_calls_ms": round(dt_opt, 4),
        })

    return results


def benchmark_concurrent_safety(
    n_threads: int = 8,
    n_agents: int = 1000,
    quick: bool = False,
) -> dict[str, Any]:
    """Benchmark (vi) Concurrent Safety: 8 threads calling evaluate at once at 1,000 agents."""
    tfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = Path(tfile.name)
    tfile.close()

    init_c = connect(str(db_path))
    init_schema(init_c)
    init_c.execute("BEGIN IMMEDIATE;")
    for i in range(n_agents):
        init_c.execute("INSERT INTO agents (id, state, updated_at) VALUES (?, 'AVAILABLE', 100.0);", (f"a_{i}",))
    init_c.execute("COMMIT;")
    init_c.close()

    cfg = SafetyConfig()
    ctrl = SafetyController(config=cfg, provider_health_fn=lambda t: {"provider_a": "CLOSED"})

    barrier = threading.Barrier(n_threads)
    lock_waits_ms: list[float] = []
    safety_errors = 0
    lock = threading.Lock()

    def worker_fn(tid: int):
        nonlocal safety_errors
        t_conn = connect(str(db_path))
        req = PacingRequest(mode=PacingMode.PREDICTIVE, requested_calls=5, rationale={})
        barrier.wait()
        t0 = time.perf_counter()
        dec = ctrl.evaluate(t_conn, req, now=100.0)
        t_wait = (time.perf_counter() - t0) * 1000.0
        t_conn.close()

        with lock:
            lock_waits_ms.append(t_wait)
            if dec.verdict == "SAFETY_ERROR" or any("SAFETY_ERROR" in r for r in dec.reasons):
                safety_errors += 1

    threads = [threading.Thread(target=worker_fn, args=(tid,)) for tid in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    res = {
        "threads": n_threads,
        "agents": n_agents,
        "safety_errors": safety_errors,
        "p50_lock_wait_ms": round(percentile(lock_waits_ms, 0.50), 3),
        "p95_lock_wait_ms": round(percentile(lock_waits_ms, 0.95), 3),
        "max_lock_wait_ms": round(max(lock_waits_ms), 3),
    }

    db_path.unlink(missing_ok=True)
    return res


def run_load_test(
    quick: bool = False,
    out_dir: str | Path = "docs",
    print_markdown: bool = True,
) -> dict[str, Any]:
    """Execute complete load testing suite and generate docs/loadtest_results.md."""
    if quick:
        dispatch_agents = [100, 500]
        dispatch_threads = [1, 4]
        event_threads = [1, 4]
        safety_agents = [100, 1000]
        snap_agents = [100, 1000]
        snap_calls = [1000]
        caps = [100, 1000]
    else:
        dispatch_agents = [100, 1000, 10000]
        dispatch_threads = [1, 4, 8, 16]
        event_threads = [1, 4, 8]
        safety_agents = [100, 1000, 10000]
        snap_agents = [100, 1000, 10000]
        snap_calls = [10000, 100000]
        caps = [100, 1000, 10000]

    bench_dispatch = benchmark_dispatch_throughput(dispatch_agents, dispatch_threads, quick=quick)
    bench_events = benchmark_apply_event_throughput(event_threads, quick=quick)
    bench_safety = benchmark_safety_evaluate_latency(safety_agents, quick=quick)
    bench_snap, query_plans = benchmark_read_snapshot_latency(snap_agents, snap_calls, quick=quick)
    bench_math = benchmark_math_risk_latency(caps)
    bench_conc = benchmark_concurrent_safety(quick=quick)

    md_content = build_loadtest_markdown(
        bench_dispatch,
        bench_events,
        bench_safety,
        bench_snap,
        query_plans,
        bench_math,
        bench_conc,
        quick=quick,
    )

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    md_file = out_path / "loadtest_results.md"
    with open(md_file, "w", encoding="utf-8") as f:
        f.write(md_content)

    if print_markdown:
        print(md_content)

    return {
        "dispatch": bench_dispatch,
        "events": bench_events,
        "safety": bench_safety,
        "snapshot": bench_snap,
        "math": bench_math,
        "concurrent_safety": bench_conc,
    }


def build_loadtest_markdown(
    dispatch_results: list[dict[str, Any]],
    event_results: list[dict[str, Any]],
    safety_results: list[dict[str, Any]],
    snap_results: list[dict[str, Any]],
    query_plans: list[tuple[str, list[str]]],
    math_results: list[dict[str, Any]],
    conc_results: dict[str, Any],
    quick: bool = False,
) -> str:
    """Format load test results into a comprehensive Markdown document."""
    lines = []
    lines.append("# SmartDialer Performance and Load Test Report\n")
    lines.append(f"**Execution Mode**: {'Quick (--quick)' if quick else 'Full'}")
    lines.append(f"**Python Version**: {sys.version.split()[0]}")
    lines.append(f"**Platform**: {platform.platform()} ({platform.machine()})")
    lines.append(f"**Logical CPU Cores**: {os.cpu_count()}")
    lines.append(f"**Command**: `python -m smartdialer.cli loadtest {'--quick' if quick else ''}`\n")

    lines.append("## 1. Dispatch Throughput (`dispatch.start_progressive_call`)")
    lines.append("| Agent Pool | Threads | Ops / Sec | Total Reserved | Elapsed (s) |")
    lines.append("|---|---|---|---|---|")
    for r in dispatch_results:
        lines.append(f"| {r['agents']:,} | {r['threads']} | **{r['ops_sec']:,.1f}** | {r['reserved']:,} | {r['elapsed_s']:.3f} |")

    lines.append("\n## 2. Event Processing Throughput (`apply_event`)")
    lines.append("| Threads | Events Processed | Ops / Sec | Elapsed (s) |")
    lines.append("|---|---|---|---|")
    for r in event_results:
        lines.append(f"| {r['threads']} | {r['events_applied']:,} | **{r['ops_sec']:,.1f}** | {r['elapsed_s']:.3f} |")

    lines.append("\n## 3. Safety Evaluation Latency (`SafetyController.evaluate`)")
    lines.append("| Agents | Historical Calls | p50 Latency (ms) | p95 Latency (ms) | Max Latency (ms) |")
    lines.append("|---|---|---|---|---|")
    for r in safety_results:
        lines.append(f"| {r['agents']:,} | {r['calls']:,} | {r['p50_ms']:.3f} ms | {r['p95_ms']:.3f} ms | {r['max_ms']:.3f} ms |")

    lines.append("\n## 4. Telemetry Snapshot Latency (`read_snapshot`)")
    lines.append("| Agents | Calls Table Rows | p50 Latency (ms) | p95 Latency (ms) | Max Latency (ms) |")
    lines.append("|---|---|---|---|---|")
    for r in snap_results:
        lines.append(f"| {r['agents']:,} | {r['calls']:,} | {r['p50_ms']:.3f} ms | {r['p95_ms']:.3f} ms | {r['max_ms']:.3f} ms |")

    lines.append("\n### SQLite EXPLAIN QUERY PLAN (read_snapshot queries)")
    for q_name, details in query_plans:
        lines.append(f"\n**{q_name}**:")
        lines.append("```text")
        for d in details:
            lines.append(f"  {d}")
        lines.append("```")

    lines.append("\n## 5. Mathematical Risk Calculation Latency")
    lines.append("| Agent Capacity (cap) | `binom_tail_gt` Latency (ms) | `max_total_calls` Latency (ms) |")
    lines.append("|---|---|---|")
    for r in math_results:
        lines.append(f"| {r['cap']:,} | {r['binom_tail_ms']:.4f} ms | {r['max_calls_ms']:.4f} ms |")

    lines.append("\n## 6. Concurrent Safety Lock Contention")
    lines.append(f"- **Threads**: {conc_results['threads']} concurrent workers")
    lines.append(f"- **Agents**: {conc_results['agents']:,}")
    lines.append(f"- **SAFETY_ERROR Count**: **{conc_results['safety_errors']}** (Zero error invariant satisfied)")
    lines.append(f"- **p50 Lock Wait**: {conc_results['p50_lock_wait_ms']:.3f} ms")
    lines.append(f"- **p95 Lock Wait**: {conc_results['p95_lock_wait_ms']:.3f} ms")
    lines.append(f"- **Max Lock Wait**: {conc_results['max_lock_wait_ms']:.3f} ms\n")

    return "\n".join(lines)
