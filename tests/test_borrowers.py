"""Tests for BorrowerRepo queue claiming and release logic."""

import pytest

from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import BorrowerState
from smartdialer.repo import BorrowerRepo


@pytest.fixture
def borrower_repo(tmp_path):
    """Provide a thread-isolated BorrowerRepo connected to a temporary SQLite database."""
    db_file = tmp_path / "borrowers_test.db"
    conn = connect(db_file)
    init_schema(conn)
    repository = BorrowerRepo(conn=conn, config=Config(db_path=str(db_file)))
    yield repository
    conn.close()


def test_enqueue_and_claim_borrowers(borrower_repo):
    """Assert eligible queued borrowers are claimed atomically up to limit."""
    now = borrower_repo.config.clock()
    borrower_repo.enqueue_borrower("b1", "+1000000001", next_attempt_at=now - 20.0)
    borrower_repo.enqueue_borrower("b2", "+1000000002", next_attempt_at=now - 10.0)
    borrower_repo.enqueue_borrower("b3", "+1000000003", next_attempt_at=now + 500.0)  # Future attempt

    # Zero or negative request
    assert borrower_repo.claim_borrowers("w1", 0) == []
    assert borrower_repo.claim_borrowers("w1", -1) == []

    # Claim up to 5, but only b1 and b2 are <= current time
    claimed = borrower_repo.claim_borrowers("w1", 5)
    assert set(claimed) == {"b1", "b2"}

    # Verify state in DB
    b1 = borrower_repo.get_borrower("b1")
    assert b1["state"] == BorrowerState.CLAIMED.value
    assert b1["claimed_by"] == "w1"
    assert b1["lease_expires_at"] is not None

    # b3 is in future, so not claimed
    b3 = borrower_repo.get_borrower("b3")
    assert b3["state"] == BorrowerState.QUEUED.value


def test_claim_future_attempt_not_eligible(tmp_path):
    """Assert borrowers scheduled in the future are ignored by claim_borrowers."""
    db_file = tmp_path / "borrower_future.db"
    conn = connect(db_file)
    init_schema(conn)

    current_time = 100.0
    config = Config(db_path=str(db_file), clock=lambda: current_time)
    repo = BorrowerRepo(conn=conn, config=config)

    repo.enqueue_borrower("b_past", "+1000", next_attempt_at=90.0)
    repo.enqueue_borrower("b_future", "+1001", next_attempt_at=150.0)

    claimed = repo.claim_borrowers("w1", 10)
    assert claimed == ["b_past"]

    conn.close()


def test_release_borrower(borrower_repo):
    """Assert release_borrower resets state to QUEUED only if caller matches claimed_by."""
    now = borrower_repo.config.clock()
    borrower_repo.enqueue_borrower("b1", "+1000", next_attempt_at=now - 10.0)
    claimed = borrower_repo.claim_borrowers("worker_A", 1)
    assert claimed == ["b1"]

    # Wrong worker tries to release
    failed_release = borrower_repo.release_borrower("b1", worker_id="worker_B", next_attempt_at=now + 60.0)
    assert failed_release is False

    b1_after_failed = borrower_repo.get_borrower("b1")
    assert b1_after_failed["state"] == BorrowerState.CLAIMED.value
    assert b1_after_failed["claimed_by"] == "worker_A"

    # Correct worker releases
    success_release = borrower_repo.release_borrower("b1", worker_id="worker_A", next_attempt_at=now + 60.0)
    assert success_release is True

    b1_after_success = borrower_repo.get_borrower("b1")
    assert b1_after_success["state"] == BorrowerState.QUEUED.value
    assert b1_after_success["claimed_by"] is None
    assert b1_after_success["lease_expires_at"] is None
    assert b1_after_success["next_attempt_at"] == now + 60.0


def test_borrower_count_by_state(borrower_repo):
    """Assert count_by_state accurately tallies borrower states."""
    borrower_repo.enqueue_borrower("b1", "+1000")
    borrower_repo.enqueue_borrower("b2", "+1001")
    borrower_repo.claim_borrowers("w1", 1)

    counts = borrower_repo.count_by_state()
    assert counts[BorrowerState.QUEUED.value] == 1
    assert counts[BorrowerState.CLAIMED.value] == 1
    assert counts[BorrowerState.IN_CALL.value] == 0
