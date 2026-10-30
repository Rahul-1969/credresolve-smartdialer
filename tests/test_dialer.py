"""Tests for Dialer loop, orphan rules, and placement reconciliation."""

from pathlib import Path
import random
import pytest

from smartdialer.actions import process_actions
from smartdialer.calls import CallState
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.dialer import Dialer
from smartdialer.models import AgentState, BorrowerState
from smartdialer.pacing.progressive import ProgressivePacer
from smartdialer.providers.base import PlaceResult, ProviderTimeout
from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.mock import MockProvider, ProviderProfile
from smartdialer.providers.router import PlaceOutcome, PlaceOutcomeKind, ProviderRouter
from smartdialer.reaper import Reaper
from smartdialer.repo import CallRepo
from smartdialer.safety import SafetyConfig, SafetyController


def test_11_orphan_rule_cancels_at_provider_if_terminal_before_initiated():
    """11. Orphan rule: call cancelled locally between Phase 1 and Phase 3 -> CANCEL action enqueued, provider truth becomes CANCELLED."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0
    rng = random.Random(42)
    behavior = BorrowerBehavior(answer_rate=0.5, mean_talk_s=30.0)
    profile = ProviderProfile(setup_latency_s=0.2, latency_jitter_s=0.0, error_rate=0.0, timeout_rate=0.0)
    mock = MockProvider("prov_a", profile, behavior, rng)
    providers = {"prov_a": mock}

    router = ProviderRouter(providers=providers, priority=["prov_a"])
    safety = SafetyController(
        config=SafetyConfig(),
        provider_health_fn=lambda t: {"prov_a": "CLOSED"},
    )
    pacer = ProgressivePacer()
    dialer = Dialer(conn, cfg, safety, router, providers, pacer, "worker_orphan")

    # Seed 1 borrower and 1 agent
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, updated_at) VALUES ('b_orph', '+18005550001', 'QUEUED', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, updated_at) VALUES ('a_orph', 'AVAILABLE', ?);",
        (now,),
    )

    # Monkeypatch router.place to cancel the call locally in the DB before returning PLACED
    orig_place = router.place

    def cancel_between_phases(call_id: str, phone: str, call_now: float) -> PlaceOutcome:
        outcome = orig_place(call_id, phone, call_now)
        # Simulate local cancellation (e.g. timeout or concurrent cancel)
        call_repo = CallRepo(conn, cfg)
        call_repo.apply_event(call_id, CallState.CANCELLED, reason="TEST_CANCEL", now=call_now)
        return outcome

    router.place = cancel_between_phases  # type: ignore

    report = dialer.tick(now)
    assert report.created == 1
    assert report.placed == 1

    # Check that a CANCEL action was enqueued with reason 'ORPHAN'
    cur_actions = conn.execute("SELECT * FROM call_actions WHERE reason = 'ORPHAN';")
    actions = cur_actions.fetchall()
    assert len(actions) == 1
    assert actions[0]["action"] == "CANCEL"
    orphan_call_id = actions[0]["call_id"]

    # The action was either processed during dialer.tick or can be processed now
    assert (
        report.actions.get("succeeded", 0) == 1
        or process_actions(conn, cfg, providers, "worker_orphan", now)["succeeded"] == 1
    )

    # Verify provider's ground truth for this call is CANCELLED (or will be once latency elapses)
    assert mock.get_call_status(orphan_call_id, now + 1.0) == "CANCELLED"


def test_12_ambiguous_placement_resolved_by_reaper():
    """12. Ambiguous placement: a provider timeout leaves the call RESERVED; the reaper later resolves it."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config(reserve_timeout_s=5.0)

    now = 100.0
    rng = random.Random(99)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=30.0)
    profile = ProviderProfile(
        setup_latency_s=0.1,
        latency_jitter_s=0.0,
        error_rate=0.0,
        timeout_rate=0.0,
    )
    mock = MockProvider("prov_a", profile, behavior, rng)
    mock.inject_outage(start=50.0, end=150.0, mode="timeout", events="delay")
    providers = {"prov_a": mock}

    router = ProviderRouter(providers=providers, priority=["prov_a"])
    safety = SafetyController(
        config=SafetyConfig(),
        provider_health_fn=lambda t: {"prov_a": "CLOSED"},
    )
    pacer = ProgressivePacer()
    dialer = Dialer(conn, cfg, safety, router, providers, pacer, "worker_ambig")

    # Seed 1 borrower and 1 agent
    conn.execute(
        "INSERT INTO borrowers (id, phone, state, updated_at) VALUES ('b_ambig', '+18005550002', 'QUEUED', ?);",
        (now,),
    )
    conn.execute(
        "INSERT INTO agents (id, state, updated_at) VALUES ('a_ambig', 'AVAILABLE', ?);",
        (now,),
    )

    report = dialer.tick(now)
    assert report.created == 1
    assert report.ambiguous == 1

    # Call remains RESERVED in DB with provider assigned
    cur = conn.execute("SELECT id, state, provider FROM calls WHERE borrower_id = 'b_ambig';")
    call_row = cur.fetchone()
    assert call_row["state"] == "RESERVED"
    assert call_row["provider"] == "prov_a"
    call_id = call_row["id"]

    # Advance time past reserve_timeout_s and outage
    now_later = now + 60.0
    reaper = Reaper(conn, cfg, providers)
    sweep_res = reaper.sweep(now_later)
    assert sweep_res["reconciled_reserved"] == 1

    # Call was reconciled to INITIATED (or terminal)
    c_after = CallRepo(conn, cfg).get_call(call_id)
    assert c_after["state"] in ("INITIATED", "RINGING", "ANSWERED", "CONNECTED", "COMPLETED")


def test_dialer_shortage_rollback_leaves_allotment_unconsumed():
    """Shortage in Phase 1 (e.g. no borrowers) rolls back the whole unit, leaving allotment unconsumed."""
    conn = connect(":memory:")
    init_schema(conn)
    cfg = Config()

    now = 100.0
    # Seed 5 available agents but 0 borrowers
    for i in range(5):
        conn.execute(
            "INSERT INTO agents (id, state, updated_at) VALUES (?, 'AVAILABLE', ?);",
            (f"ag_{i}", now),
        )

    prof = ProviderProfile(setup_latency_s=0.1, latency_jitter_s=0.0, error_rate=0.0, timeout_rate=0.0)
    mock = MockProvider("prov_a", prof, BorrowerBehavior(0.5, 30.0), random.Random(1))
    providers = {"prov_a": mock}
    router = ProviderRouter(providers=providers, priority=["prov_a"])
    safety = SafetyController(SafetyConfig(), lambda t: {"prov_a": "CLOSED"})
    dialer = Dialer(conn, cfg, safety, router, providers, ProgressivePacer(), "w_shortage")

    report = dialer.tick(now)
    assert report.approved == 5
    assert report.created == 0  # Stopped early on borrower shortage

    # Allotment remaining should still be 5
    cur = conn.execute("SELECT approved, remaining FROM safety_allotments;")
    row = cur.fetchone()
    assert row["approved"] == 5
    assert row["remaining"] == 5
