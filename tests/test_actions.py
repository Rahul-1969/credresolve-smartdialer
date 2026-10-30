"""Tests for transactional outbox processor and call actions."""

from pathlib import Path
import random
import sqlite3
import threading
import pytest

from smartdialer.actions import enqueue_action, process_actions
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.providers.base import (
    PlaceResult,
    ProviderError,
    ProviderEvent,
    TelecomProvider,
)
from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.mock import MockProvider, ProviderProfile


class CountingStubProvider(TelecomProvider):
    """Stub provider tracking invocations of cancel and terminate."""

    def __init__(self, name: str = "stub") -> None:
        self.name = name
        self.cancel_count = 0
        self.terminate_count = 0

    def place_call(self, call_id: str, phone: str, now: float) -> PlaceResult:
        return PlaceResult(provider_call_id=f"{self.name}_{call_id}", latency_s=0.1)

    def cancel_call(self, call_id: str, now: float) -> bool:
        self.cancel_count += 1
        return True

    def terminate_call(self, call_id: str, now: float) -> bool:
        self.terminate_count += 1
        return True

    def poll_events(self, now: float) -> list[ProviderEvent]:
        return []

    def get_call_status(self, call_id: str, now: float) -> str | None:
        return "ANSWERED"


class AlwaysFailingProvider(TelecomProvider):
    """Provider that consistently raises ProviderError on all calls."""

    name: str = "fail_prov"

    def place_call(self, call_id: str, phone: str, now: float) -> PlaceResult:
        raise ProviderError("Placement error")

    def cancel_call(self, call_id: str, now: float) -> bool:
        raise ProviderError("Cancel error")

    def terminate_call(self, call_id: str, now: float) -> bool:
        raise ProviderError("Terminate error")

    def poll_events(self, now: float) -> list[ProviderEvent]:
        return []

    def get_call_status(self, call_id: str, now: float) -> str | None:
        return "ANSWERED"


def test_enqueue_action_idempotence():
    """enqueue_action is idempotent: duplicate actions for same (call_id, action) are ignored."""
    conn = connect(":memory:")
    init_schema(conn)

    now = 100.0
    # First enqueue succeeds
    res1 = enqueue_action(conn, "c_enq_1", "CANCEL", "STALE_CALL", now)
    assert res1 is True

    # Duplicate enqueue returns False
    res2 = enqueue_action(conn, "c_enq_1", "CANCEL", "DIFFERENT_REASON", now + 1.0)
    assert res2 is False

    cur = conn.execute("SELECT * FROM call_actions WHERE call_id = 'c_enq_1';")
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["action"] == "CANCEL"
    assert rows[0]["reason"] == "STALE_CALL"


def test_terminate_success_marks_done_and_emits_completed():
    """TERMINATE success marks action done and provider later emits COMPLETED."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 0.0
    rng = random.Random(1234)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=60.0)
    profile = ProviderProfile(
        setup_latency_s=0.2,
        latency_jitter_s=0.0,
        error_rate=0.0,
        timeout_rate=0.0,
    )
    mock = MockProvider("mock_a", profile, behavior, rng)

    # Place call at mock provider
    mock.place_call("c_outbox_1", "+18005550001", now=0.0)

    # Insert into DB
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_o1', '+18005550001', 'IN_CALL', 0, 0.0);"
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, provider, state, state_rank, created_at, updated_at) VALUES ('c_outbox_1', 'b_o1', 'mock_a', 'INITIATED', 2, 0.0, 0.0);"
    )

    # Advance virtual time until call is answered (at 10.0s)
    assert mock.get_call_status("c_outbox_1", now=10.0) == "ANSWERED"

    # Enqueue TERMINATE action
    enqueue_action(conn, "c_outbox_1", "TERMINATE", "ABANDONED", now=10.0)

    # Process outbox
    res = process_actions(conn, cfg, {"mock_a": mock}, worker_id="w_1", now=10.0)
    assert res["claimed"] == 1
    assert res["succeeded"] == 1

    # Verify action marked done
    cur = conn.execute("SELECT * FROM call_actions WHERE call_id = 'c_outbox_1';")
    row = cur.fetchone()
    assert row["done_at"] == 10.0
    assert row["claimed_by"] is None

    # Advance time: provider emits COMPLETED with reason TERMINATED
    events = mock.poll_events(now=12.0)
    comp_events = [e for e in events if e.call_id == "c_outbox_1" and e.event_type == "COMPLETED"]
    assert len(comp_events) == 1
    assert comp_events[0].reason == "TERMINATED"


def test_provider_outage_releases_claim_and_retries_after_outage():
    """Provider outage -> row released and retried successfully after the outage."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    rng = random.Random(5678)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=60.0)
    profile = ProviderProfile(
        setup_latency_s=0.2,
        latency_jitter_s=0.0,
        error_rate=0.0,
        timeout_rate=0.0,
    )
    mock = MockProvider("mock_b", profile, behavior, rng)
    mock.place_call("c_outbox_2", "+18005550002", now=0.0)

    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_o2', '+18005550002', 'IN_CALL', 0, 0.0);"
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, provider, state, state_rank, created_at, updated_at) VALUES ('c_outbox_2', 'b_o2', 'mock_b', 'INITIATED', 2, 0.0, 0.0);"
    )

    assert mock.get_call_status("c_outbox_2", now=10.0) == "ANSWERED"

    # Inject outage [12.0, 20.0)
    mock.inject_outage(start=12.0, end=20.0, mode="error", events="drop")

    enqueue_action(conn, "c_outbox_2", "TERMINATE", "ABANDONED", now=15.0)

    # Attempt process during outage (now=15.0s)
    res1 = process_actions(conn, cfg, {"mock_b": mock}, worker_id="w_outage", now=15.0)
    assert res1["claimed"] == 1
    assert res1["retried"] == 1
    assert res1["succeeded"] == 0

    # Claim must be released
    row1 = conn.execute("SELECT * FROM call_actions WHERE call_id = 'c_outbox_2';").fetchone()
    assert row1["done_at"] is None
    assert row1["claimed_by"] is None
    assert row1["attempts"] == 1

    # Attempt process after outage has ended (now=25.0s)
    res2 = process_actions(conn, cfg, {"mock_b": mock}, worker_id="w_outage", now=25.0)
    assert res2["claimed"] == 1
    assert res2["succeeded"] == 1

    row2 = conn.execute("SELECT * FROM call_actions WHERE call_id = 'c_outbox_2';").fetchone()
    assert row2["done_at"] == 25.0
    assert row2["claimed_by"] is None
    assert row2["attempts"] == 2


def test_two_workers_racing_on_same_row(tmp_path: Path):
    """Two workers racing on the same row: the provider is called exactly once."""
    db_file = tmp_path / "race_actions.db"
    main_conn = connect(db_file)
    init_schema(main_conn)
    cfg = Config()

    now = 50.0
    main_conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_race', '5550001', 'IN_CALL', 0, ?);",
        (now,),
    )
    main_conn.execute(
        "INSERT INTO calls (id, borrower_id, provider, state, state_rank, created_at, updated_at) VALUES ('c_race', 'b_race', 'counting_stub', 'INITIATED', 2, ?, ?);",
        (now, now),
    )
    enqueue_action(main_conn, "c_race", "TERMINATE", "ABANDONED", now)

    provider = CountingStubProvider("counting_stub")
    barrier = threading.Barrier(2)
    results: list[dict[str, int]] = []
    errors: list[Exception] = []

    def worker_run(wid: str):
        t_conn = connect(db_file)
        try:
            barrier.wait()
            res = process_actions(
                t_conn, cfg, {"counting_stub": provider}, worker_id=wid, now=now
            )
            results.append(res)
        except Exception as e:
            errors.append(e)
        finally:
            t_conn.close()

    t1 = threading.Thread(target=worker_run, args=("worker_1",))
    t2 = threading.Thread(target=worker_run, args=("worker_2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors, f"Errors encountered: {errors}"
    assert provider.terminate_count == 1, f"Provider called {provider.terminate_count} times, expected 1!"

    total_claimed = sum(r["claimed"] for r in results)
    assert total_claimed == 1
    total_succeeded = sum(r["succeeded"] for r in results)
    assert total_succeeded == 1

    main_conn.close()


def test_give_up_after_action_max_attempts():
    """Actions stop being retried and are counted as gave_up after action_max_attempts."""
    conn = connect(":memory:")
    init_schema(conn)

    cfg = Config(action_max_attempts=3, action_claim_lease_s=5.0)
    failing_prov = AlwaysFailingProvider()

    now = 100.0
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, attempts, updated_at) VALUES ('b_fail', '5550002', 'IN_CALL', 0, ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO calls (id, borrower_id, provider, state, state_rank, created_at, updated_at) VALUES ('c_fail', 'b_fail', 'fail_prov', 'INITIATED', 2, ?, ?);",
        (now, now),
    )
    enqueue_action(conn, "c_fail", "TERMINATE", "ABANDONED", now)

    # Attempt 1 (now = 100.0) -> attempt count becomes 1
    r1 = process_actions(conn, cfg, {"fail_prov": failing_prov}, "w1", now=100.0)
    assert r1["claimed"] == 1
    assert r1["retried"] == 1
    assert r1["gave_up"] == 0

    # Attempt 2 (now = 110.0, lease expired) -> attempt count becomes 2
    r2 = process_actions(conn, cfg, {"fail_prov": failing_prov}, "w1", now=110.0)
    assert r2["claimed"] == 1
    assert r2["retried"] == 1
    assert r2["gave_up"] == 0

    # Attempt 3 (now = 120.0, lease expired) -> attempt count becomes 3 (== max_attempts) -> gave_up
    r3 = process_actions(conn, cfg, {"fail_prov": failing_prov}, "w1", now=120.0)
    assert r3["claimed"] == 1
    assert r3["retried"] == 0
    assert r3["gave_up"] == 1

    # Attempt 4 (now = 130.0) -> attempts < max_attempts is no longer satisfied, row is ignored
    r4 = process_actions(conn, cfg, {"fail_prov": failing_prov}, "w1", now=130.0)
    assert r4["claimed"] == 0

