"""Concurrency and stress tests for atomic agent reservation and borrower claiming."""

import threading
import pytest

from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, BorrowerState
from smartdialer.repo import AgentRepo, BorrowerRepo


def test_agent_reservation_race_200_times(tmp_path):
    """The Race Test: 2 threads compete for 1 AVAILABLE agent across 200 iterations.

    Guarantees:
        In 100% of iterations, exactly one worker gets the agent and the other gets [].
        Never do both workers reserve the same agent.
        Connections are strictly isolated per thread.
    """
    db_file = tmp_path / "race_agents.db"
    setup_conn = connect(db_file)
    init_schema(setup_conn)
    setup_repo = AgentRepo(setup_conn)

    start_barrier = threading.Barrier(3)  # main + 2 workers
    race_barrier = threading.Barrier(2)   # 2 workers
    done_barrier = threading.Barrier(3)   # main + 2 workers

    results = {}
    stop_event = threading.Event()
    thread_errors = []

    def worker_loop(worker_id: str):
        conn = connect(db_file)
        repo = AgentRepo(conn)
        try:
            while True:
                try:
                    start_barrier.wait()
                except threading.BrokenBarrierError:
                    break
                if stop_event.is_set():
                    break
                try:
                    race_barrier.wait()
                except threading.BrokenBarrierError:
                    break
                results[worker_id] = repo.reserve_agents(worker_id, 1)
                try:
                    done_barrier.wait()
                except threading.BrokenBarrierError:
                    break
        except Exception as exc:
            thread_errors.append((worker_id, exc))
        finally:
            conn.close()

    t1 = threading.Thread(target=worker_loop, args=("worker_1",))
    t2 = threading.Thread(target=worker_loop, args=("worker_2",))
    t1.start()
    t2.start()

    try:
        for i in range(200):
            agent_id = f"race_agent_{i}"
            setup_repo.upsert_agent(agent_id, state=AgentState.AVAILABLE)

            # Signal workers to compete
            start_barrier.wait()
            # Wait for both workers to finish attempt
            done_barrier.wait()

            assert not thread_errors, f"Thread errors: {thread_errors}"

            w1_res = results.get("worker_1", [])
            w2_res = results.get("worker_2", [])

            # Exactly one worker wins; the other receives empty list
            assert (w1_res == [agent_id] and w2_res == []) or (
                w2_res == [agent_id] and w1_res == []
            ), f"Race failure on iteration {i}: w1={w1_res}, w2={w2_res}"

        stop_event.set()
        start_barrier.wait()
    finally:
        stop_event.set()
        try:
            start_barrier.reset()
        except threading.BrokenBarrierError:
            pass
        try:
            race_barrier.reset()
        except threading.BrokenBarrierError:
            pass
        try:
            done_barrier.reset()
        except threading.BrokenBarrierError:
            pass
        t1.join(timeout=2.0)
        t2.join(timeout=2.0)
        setup_conn.close()


def test_agent_reservation_stress_50_agents_16_threads(tmp_path):
    """Stress Test: 50 AVAILABLE agents contested by 16 concurrent threads.

    Guarantees:
        - Exactly 50 agents are reserved in total.
        - Zero duplicate reservations across workers.
        - Every agent row has state='RESERVED' and a valid reserved_by.
        - Exactly 50 AVAILABLE->RESERVED audit entries in agent_events.
    """
    db_file = tmp_path / "stress_agents.db"
    setup_conn = connect(db_file)
    init_schema(setup_conn)
    setup_repo = AgentRepo(setup_conn)

    # Populate 50 available agents
    for i in range(50):
        setup_repo.upsert_agent(f"agent_{i:03d}", state=AgentState.AVAILABLE)

    setup_conn.close()

    num_threads = 16
    barrier = threading.Barrier(num_threads)
    results = [[] for _ in range(num_threads)]
    errors = []

    def worker_loop(thread_idx: int):
        worker_id = f"worker_{thread_idx}"
        worker_conn = connect(db_file)
        repo = AgentRepo(worker_conn)
        try:
            barrier.wait()
            while True:
                reserved = repo.reserve_agents(worker_id, 5)
                if not reserved:
                    break
                results[thread_idx].extend(reserved)
        except Exception as exc:
            errors.append((worker_id, exc))
        finally:
            worker_conn.close()

    threads = [
        threading.Thread(target=worker_loop, args=(i,)) for i in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors, f"Encountered thread exceptions: {errors}"

    # Flatten all reserved IDs
    all_reserved = [agent_id for thread_res in results for agent_id in thread_res]
    assert len(all_reserved) == 50, f"Expected 50 total reserved, got {len(all_reserved)}"
    assert len(set(all_reserved)) == 50, "Detected duplicate agent reservations!"

    # Verify database state
    verify_conn = connect(db_file)
    cur = verify_conn.execute("SELECT id, state, reserved_by FROM agents;")
    rows = cur.fetchall()
    assert len(rows) == 50
    for row in rows:
        assert row["state"] == AgentState.RESERVED.value
        assert row["reserved_by"] is not None

    # Verify agent_events audit trail
    cur_events = verify_conn.execute(
        """
        SELECT COUNT(*) as cnt FROM agent_events
        WHERE from_state = 'AVAILABLE' AND to_state = 'RESERVED';
        """
    )
    events_count = cur_events.fetchone()["cnt"]
    assert events_count == 50, f"Expected 50 agent_events rows, got {events_count}"
    verify_conn.close()


def test_borrower_claim_race_200_times(tmp_path):
    """The Race Test for Borrowers: 2 threads compete for 1 QUEUED borrower across 200 iterations.

    Guarantees:
        In 100% of iterations, exactly one worker claims the borrower and the other gets [].
        No borrower is ever claimed twice.
        Connections are strictly isolated per thread.
    """
    db_file = tmp_path / "race_borrowers.db"
    main_conn = connect(db_file)
    init_schema(main_conn)
    setup_repo = BorrowerRepo(main_conn)

    start_barrier = threading.Barrier(3)  # main + 2 workers
    race_barrier = threading.Barrier(2)   # 2 workers
    done_barrier = threading.Barrier(3)   # main + 2 workers

    results = {}
    stop_event = threading.Event()
    thread_errors = []

    def worker_loop(worker_id: str):
        conn = connect(db_file)
        repo = BorrowerRepo(conn)
        try:
            while True:
                try:
                    start_barrier.wait()
                except threading.BrokenBarrierError:
                    break
                if stop_event.is_set():
                    break
                try:
                    race_barrier.wait()
                except threading.BrokenBarrierError:
                    break
                results[worker_id] = repo.claim_borrowers(worker_id, 1)
                try:
                    done_barrier.wait()
                except threading.BrokenBarrierError:
                    break
        except Exception as exc:
            thread_errors.append((worker_id, exc))
        finally:
            conn.close()

    t1 = threading.Thread(target=worker_loop, args=("worker_1",))
    t2 = threading.Thread(target=worker_loop, args=("worker_2",))
    t1.start()
    t2.start()

    try:
        for i in range(200):
            borrower_id = f"race_borrower_{i}"
            setup_repo.enqueue_borrower(borrower_id, f"+1000{i:04d}", next_attempt_at=0.0)

            start_barrier.wait()
            done_barrier.wait()

            assert not thread_errors, f"Thread errors: {thread_errors}"

            w1_res = results.get("worker_1", [])
            w2_res = results.get("worker_2", [])

            assert (w1_res == [borrower_id] and w2_res == []) or (
                w2_res == [borrower_id] and w1_res == []
            ), f"Borrower race failure on iteration {i}: w1={w1_res}, w2={w2_res}"

        stop_event.set()
        start_barrier.wait()
    finally:
        stop_event.set()
        try:
            start_barrier.reset()
        except threading.BrokenBarrierError:
            pass
        try:
            race_barrier.reset()
        except threading.BrokenBarrierError:
            pass
        try:
            done_barrier.reset()
        except threading.BrokenBarrierError:
            pass
        t1.join(timeout=2.0)
        t2.join(timeout=2.0)
        main_conn.close()


def test_borrower_claim_stress_50_borrowers_16_threads(tmp_path):
    """Stress Test: 50 QUEUED borrowers contested by 16 concurrent threads.

    Guarantees:
        - Exactly 50 borrowers are claimed in total.
        - Zero duplicate claims across workers.
        - Every borrower row has state='CLAIMED' and non-null claimed_by.
    """
    db_file = tmp_path / "stress_borrowers.db"
    setup_conn = connect(db_file)
    init_schema(setup_conn)
    setup_repo = BorrowerRepo(setup_conn)

    for i in range(50):
        setup_repo.enqueue_borrower(f"borrower_{i:03d}", f"+1000{i:03d}", next_attempt_at=0.0)

    setup_conn.close()

    num_threads = 16
    barrier = threading.Barrier(num_threads)
    results = [[] for _ in range(num_threads)]
    errors = []

    def worker_loop(thread_idx: int):
        worker_id = f"worker_{thread_idx}"
        worker_conn = connect(db_file)
        repo = BorrowerRepo(worker_conn)
        try:
            barrier.wait()
            while True:
                claimed = repo.claim_borrowers(worker_id, 5)
                if not claimed:
                    break
                results[thread_idx].extend(claimed)
        except Exception as exc:
            errors.append((worker_id, exc))
        finally:
            worker_conn.close()

    threads = [
        threading.Thread(target=worker_loop, args=(i,)) for i in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors, f"Encountered thread exceptions: {errors}"

    all_claimed = [b_id for thread_res in results for b_id in thread_res]
    assert len(all_claimed) == 50, f"Expected 50 total claimed, got {len(all_claimed)}"
    assert len(set(all_claimed)) == 50, "Detected duplicate borrower claims!"

    verify_conn = connect(db_file)
    cur = verify_conn.execute("SELECT id, state, claimed_by FROM borrowers;")
    rows = cur.fetchall()
    assert len(rows) == 50
    for row in rows:
        assert row["state"] == BorrowerState.CLAIMED.value
        assert row["claimed_by"] is not None
    verify_conn.close()
