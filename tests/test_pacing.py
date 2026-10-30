"""Unit tests for progressive and predictive pacing engines."""

import pytest
from smartdialer.pacing import (
    PacingEngine,
    PacingMode,
    PacingRequest,
    PredictiveConfig,
    PredictivePacer,
    ProgressivePacer,
)
from smartdialer.snapshot import AbandonStats, RateStats, SystemSnapshot


def make_snapshot(
    available: int = 10,
    wrap_up: int = 0,
    connected: int = 0,
    online: int = 10,
    unbound_in_flight: int = 0,
    ringing_unbound: int = 0,
    unanswered_unbound: int = 0,
    bound_in_flight: int = 0,
    answered_waiting: int = 0,
    pending_progressive: int = 0,
    pending_predictive: int = 0,
    short_ans: int = 20,
    short_res: int = 50,
    long_ans: int = 40,
    long_res: int = 100,
    avg_talk_s: float | None = 60.0,
) -> SystemSnapshot:
    """Helper factory for building test SystemSnapshots."""
    return SystemSnapshot(
        now=1000.0,
        agents_by_state={
            "AVAILABLE": available,
            "WRAP_UP": wrap_up,
            "CONNECTED": connected,
            "OFFLINE": 0,
            "PAUSED": 0,
            "RESERVED": 0,
            "DIALING": 0,
        },
        available=available,
        wrap_up=wrap_up,
        connected=connected,
        online=online,
        unbound_in_flight=unbound_in_flight,
        ringing_unbound=ringing_unbound,
        unanswered_unbound=unanswered_unbound,
        bound_in_flight=bound_in_flight,
        answered_waiting=answered_waiting,
        pending_progressive=pending_progressive,
        pending_predictive=pending_predictive,
        short=RateStats(answered=short_ans, resolved=short_res),
        long=RateStats(answered=long_ans, resolved=long_res),
        abandon=AbandonStats(answered=100, abandoned=1),
        avg_talk_s=avg_talk_s,
    )


def test_protocol_conformance():
    """Verify that both pacers satisfy the PacingEngine protocol."""
    assert isinstance(ProgressivePacer(), PacingEngine)
    assert isinstance(PredictivePacer(), PacingEngine)


def test_progressive_pacer_basic():
    """Progressive pacer proposes exactly the available agent count."""
    pacer = ProgressivePacer()
    snap = make_snapshot(available=12)
    req = pacer.propose(snap)

    assert isinstance(req, PacingRequest)
    assert req.mode == PacingMode.PROGRESSIVE
    assert req.requested_calls == 12
    assert req.rationale["available"] == 12
    assert "one call per available agent" in req.rationale["rule"]

    # Determinism check
    req2 = pacer.propose(snap)
    assert req == req2


def test_predictive_pacer_cold_start():
    """Predictive pacer falls back to available capacity during cold start (no long-term history)."""
    pacer = PredictivePacer()
    snap = make_snapshot(available=15, long_ans=0, long_res=0)
    req = pacer.propose(snap)

    assert isinstance(req, PacingRequest)
    assert req.mode == PacingMode.PREDICTIVE
    assert req.requested_calls == 15
    assert req.rationale.get("cold_start") is True
    assert req.rationale["available"] == 15


def test_predictive_pacer_determinism():
    """Predictive pacer is purely functional and deterministic."""
    pacer = PredictivePacer()
    snap = make_snapshot(available=10, connected=20, wrap_up=5)
    r1 = pacer.propose(snap)
    r2 = pacer.propose(snap)
    assert r1 == r2


def test_predictive_pacer_rationale_intermediate_keys():
    """Rationale must contain all required intermediate math components by name."""
    pacer = PredictivePacer()
    snap = make_snapshot(available=10, wrap_up=5, connected=15)
    req = pacer.propose(snap)

    expected_keys = {
        "p_long",
        "p_hat",
        "talk",
        "finish_talk",
        "finish_wrap",
        "expected_free",
        "n_target",
        "in_flight",
        "requested",
        "available",
        "wrap_up",
        "connected",
        "answered_waiting",
        "unbound_in_flight",
        "pending_predictive",
    }
    for k in expected_keys:
        assert k in req.rationale, f"Missing rationale key: {k}"


def test_predictive_higher_p_hat_fewer_calls():
    """Higher answer rate (p_hat) results in fewer outbound calls for the same expected capacity."""
    pacer = PredictivePacer()
    # Low answer rate (20%) -> requires more calls to fill seats
    snap_low = make_snapshot(available=20, short_ans=10, short_res=50, long_ans=20, long_res=100)
    req_low = pacer.propose(snap_low)

    # High answer rate (80%) -> requires fewer calls to fill seats
    snap_high = make_snapshot(available=20, short_ans=40, short_res=50, long_ans=80, long_res=100)
    req_high = pacer.propose(snap_high)

    assert req_low.requested_calls > req_high.requested_calls
    assert req_low.rationale["p_hat"] < req_high.rationale["p_hat"]


def test_predictive_connected_agents_boost_proposals():
    """More connected agents increase expected free capacity over the horizon, boosting proposals."""
    pacer = PredictivePacer()
    snap_idle = make_snapshot(available=5, connected=0)
    req_idle = pacer.propose(snap_idle)

    snap_active = make_snapshot(available=5, connected=40)
    req_active = pacer.propose(snap_active)

    assert req_active.requested_calls > req_idle.requested_calls
    assert req_active.rationale["expected_free"] > req_idle.rationale["expected_free"]


def test_predictive_never_negative_and_capped_by_max_request():
    """Proposals are clamped to [0, max_request]."""
    cfg = PredictiveConfig(max_request=30)
    pacer = PredictivePacer(config=cfg)

    # In flight exceeds target -> 0 calls requested
    snap_crowded = make_snapshot(available=2, unbound_in_flight=100)
    req_crowded = pacer.propose(snap_crowded)
    assert req_crowded.requested_calls == 0

    # Huge capacity -> capped at max_request (30)
    snap_huge = make_snapshot(available=500)
    req_huge = pacer.propose(snap_huge)
    assert req_huge.requested_calls == 30

