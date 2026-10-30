"""Tests for MockProvider, Provider A, Provider B, and telecom simulation behavior."""

import random
import pytest

from smartdialer.providers.base import ProviderError, ProviderTimeout
from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.mock import MockProvider, ProviderProfile
from smartdialer.providers.provider_a import make_provider_a
from smartdialer.providers.provider_b import make_provider_b


def test_determinism_same_seed_yields_identical_streams():
    """1. Determinism: same seed -> identical event streams for Provider B over 100 calls."""
    def run_simulation(seed: int):
        rng = random.Random(seed)
        behavior = BorrowerBehavior(answer_rate=0.7, mean_talk_s=30.0)
        provider = make_provider_b(rng, behavior)

        events = []
        for i in range(100):
            call_id = f"call_det_{i}"
            now = float(i * 10)
            try:
                provider.place_call(call_id, f"+100{i}", now)
            except (ProviderError, ProviderTimeout):
                pass

        # Poll all events up to 5000s
        for t in range(0, 5000, 5):
            events.extend(provider.poll_events(float(t)))
        return events

    run1 = run_simulation(seed=42)
    run2 = run_simulation(seed=42)

    assert len(run1) > 0
    assert len(run1) == len(run2)

    for e1, e2 in zip(run1, run2):
        assert e1.provider == e2.provider
        assert e1.call_id == e2.call_id
        assert e1.provider_event_id == e2.provider_event_id
        assert e1.event_type == e2.event_type
        assert e1.reason == e2.reason
        assert pytest.approx(e1.occurred_at, 1e-6) == e2.occurred_at
        assert pytest.approx(e1.deliver_at, 1e-6) == e2.deliver_at


def test_provider_a_reliability_and_causal_ordering():
    """2. Provider A with answer_rate 1.0: in-order events, no duplicates, error rate < 2% over 1000 calls."""
    rng = random.Random(101)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=20.0)
    provider = make_provider_a(rng, behavior)

    placed_count = 0
    error_count = 0
    total_calls = 1000

    for i in range(total_calls):
        call_id = f"call_a_{i}"
        now = float(i)
        try:
            provider.place_call(call_id, f"+100{i}", now)
            placed_count += 1
        except ProviderError:
            error_count += 1

    # Error rate under 2%
    error_rate = error_count / total_calls
    assert error_rate < 0.02, f"Error rate too high: {error_rate}"

    # Poll all events
    polled = provider.poll_events(now=10000.0)
    seen_event_ids = set()
    call_events_map: dict[str, list[str]] = {}

    for evt in polled:
        # Assert no duplicates in Provider A
        assert evt.provider_event_id not in seen_event_ids, f"Duplicate event ID in Provider A: {evt.provider_event_id}"
        seen_event_ids.add(evt.provider_event_id)
        call_events_map.setdefault(evt.call_id, []).append(evt.event_type)

    assert len(call_events_map) == placed_count
    for call_id, event_types in call_events_map.items():
        assert event_types == ["RINGING", "ANSWERED", "COMPLETED"], (
            f"Call {call_id} delivered out-of-order: {event_types}"
        )


def test_provider_b_anomalies_duplicates_reordering_and_timeouts():
    """3. Provider B over 500 calls: duplicates occur, out-of-order delivery occurs, timeouts ~5% with real ambiguity."""
    rng = random.Random(202)
    behavior = BorrowerBehavior(answer_rate=0.6, mean_talk_s=40.0)
    provider = make_provider_b(rng, behavior)

    timeouts = 0
    errors = 0
    placed = 0
    timed_out_call_ids = []
    total_calls = 500

    for i in range(total_calls):
        call_id = f"call_b_{i}"
        now = float(i * 2)
        try:
            provider.place_call(call_id, f"+200{i}", now)
            placed += 1
        except ProviderTimeout:
            timeouts += 1
            timed_out_call_ids.append((call_id, now))
        except ProviderError:
            errors += 1

    # Timeouts around 5% (wide tolerance [1.5%, 10%])
    timeout_rate = timeouts / total_calls
    assert 0.015 <= timeout_rate <= 0.10, f"Unexpected timeout rate: {timeout_rate}"

    # Ambiguity check: at least one timed-out call DID exist at the provider
    timed_out_and_existed = [
        cid for cid, t in timed_out_call_ids if provider.get_call_status(cid, t + 100.0) is not None
    ]
    assert len(timed_out_and_existed) > 0, "Expected at least one timed-out call to have been placed at provider"

    # Poll events gradually to observe delivery order and duplicates
    polled_events = []
    for step in range(0, 3000, 2):
        polled_events.extend(provider.poll_events(float(step)))

    # Check for duplicates (same provider_event_id seen more than once)
    event_id_counts: dict[str, int] = {}
    for evt in polled_events:
        event_id_counts[evt.provider_event_id] = event_id_counts.get(evt.provider_event_id, 0) + 1

    duplicate_event_ids = [eid for eid, cnt in event_id_counts.items() if cnt > 1]
    assert len(duplicate_event_ids) > 0, "Expected duplicate events in Provider B"

    # Check for out-of-order causal delivery (e.g. ANSWERED delivered before RINGING)
    delivery_order: dict[str, list[str]] = {}
    for evt in polled_events:
        delivery_order.setdefault(evt.call_id, []).append(evt.event_type)

    out_of_order_calls = 0
    for call_id, evts in delivery_order.items():
        if "RINGING" in evts and "ANSWERED" in evts:
            if evts.index("ANSWERED") < evts.index("RINGING"):
                out_of_order_calls += 1

    assert out_of_order_calls > 0, "Expected at least one out-of-order delivery in Provider B"


def test_idempotency_and_no_answer():
    """4. place_call is idempotent on call_id; answer_rate 0 yields RINGING then FAILED(NO_ANSWER)."""
    rng = random.Random(303)
    behavior = BorrowerBehavior(answer_rate=0.0, mean_talk_s=30.0)
    provider = make_provider_a(rng, behavior)

    # Call placement
    res1 = provider.place_call("call_idem_1", "+3001", now=10.0)
    # Idempotent second placement returns exact same PlaceResult
    res2 = provider.place_call("call_idem_1", "+3001", now=12.0)
    assert res1.provider_call_id == res2.provider_call_id
    assert res1.latency_s == res2.latency_s

    # Advance time and collect events
    events = provider.poll_events(now=100.0)
    assert [e.event_type for e in events] == ["RINGING", "FAILED"]
    failed_event = events[1]
    assert failed_event.reason == "NO_ANSWER"


def test_cancellation_race_before_and_after_answer():
    """5. cancel before answer -> True; cancel after true answer time -> False even before delivery."""
    # Scenario A: Cancel before borrower answers
    rng1 = random.Random(404)
    # deterministic behavior with answer_rate=1.0 and mean_talk=30
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=30.0)
    profile = ProviderProfile(
        setup_latency_s=1.0,
        latency_jitter_s=0.0,
        error_rate=0.0,
        timeout_rate=0.0,
        ring_timeout_s=25.0,
    )
    prov1 = MockProvider("prov1", profile, behavior, rng1)
    prov1.place_call("call_cancel_early", "+4001", now=0.0)

    # At now=1.5s: call is RINGING (ringing started at 1.0s, answers after ~5s)
    status_at_1_5 = prov1.get_call_status("call_cancel_early", 1.5)
    assert status_at_1_5 == "RINGING"

    # Cancel at now=1.5s
    cancelled = prov1.cancel_call("call_cancel_early", now=1.5)
    assert cancelled is True

    # Advance virtual time and verify CANCELLED is emitted, and ANSWERED is NEVER delivered
    evts = prov1.poll_events(now=100.0)
    event_types = [e.event_type for e in evts]
    assert "CANCELLED" in event_types
    assert "ANSWERED" not in event_types
    assert "COMPLETED" not in event_types

    # Scenario B: Cancel after the true answer time (cancel-vs-answer race)
    # Setup profile with large reorder jitter on delivery so ANSWERED event is delayed
    rng2 = random.Random(505)
    profile_delayed = ProviderProfile(
        setup_latency_s=0.5,
        latency_jitter_s=0.0,
        error_rate=0.0,
        timeout_rate=0.0,
        reorder_rate=1.0,
        reorder_jitter_s=50.0,  # Delays delivery by up to 50s
    )
    prov2 = MockProvider("prov2", profile_delayed, behavior, rng2)
    prov2.place_call("call_cancel_late", "+4002", now=0.0)

    # Let true answer happen at e.g. now=10.0s
    true_status = prov2.get_call_status("call_cancel_late", now=10.0)
    assert true_status in {"ANSWERED", "COMPLETED"}

    # Attempt to cancel at now=10.0s -> MUST return False!
    assert prov2.cancel_call("call_cancel_late", now=10.0) is False


def test_outage_modes_and_event_handling():
    """6. Outage modes ('error', 'timeout') and event policies ('delay', 'drop')."""
    rng = random.Random(606)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=30.0)
    profile = ProviderProfile(
        setup_latency_s=0.5,
        latency_jitter_s=0.0,
        error_rate=0.0,
        timeout_rate=0.0,
    )
    prov = MockProvider("prov_outage", profile, behavior, rng)

    # 1. Error mode outage
    prov.inject_outage(start=10.0, end=20.0, mode="error", events="delay")
    with pytest.raises(ProviderError):
        prov.place_call("call_outage_err", "+5001", now=15.0)

    # 2. Timeout mode outage
    prov.inject_outage(start=30.0, end=40.0, mode="timeout", events="delay")
    with pytest.raises(ProviderTimeout):
        prov.place_call("call_outage_to", "+5002", now=35.0)

    # 3. Events "delay": events falling in [50, 70) are delayed until 70
    prov.inject_outage(start=50.0, end=70.0, mode="error", events="delay")
    # Place a call at now=49.0 that schedules events during the outage window
    prov.place_call("call_delayed", "+5003", now=49.0)

    # Poll at 49.5 before outage starts: delivers RINGING
    evts_at_49_5 = prov.poll_events(now=49.5)
    assert len([e for e in evts_at_49_5 if e.call_id == "call_delayed"]) == 1

    # Polling during outage at now=60.0 must yield no delayed events (e.g. ANSWERED)
    evts_at_60 = prov.poll_events(now=60.0)
    assert [e for e in evts_at_60 if e.call_id == "call_delayed"] == []

    # Polling at now=70.0 releases the delayed events
    evts_at_70 = prov.poll_events(now=70.0)
    assert len([e for e in evts_at_70 if e.call_id == "call_delayed"]) > 0

    # 4. Events "drop": events falling in [80, 100) are dropped permanently, but ground truth is preserved
    prov.inject_outage(start=80.0, end=100.0, mode="error", events="drop")
    prov.place_call("call_dropped", "+5004", now=79.0)

    # Advance time past outage to 150.0
    evts_at_150 = prov.poll_events(now=150.0)
    dropped_events = [e for e in evts_at_150 if e.call_id == "call_dropped"]
    # Events scheduled during [80, 100) were dropped
    assert len(dropped_events) < 3

    # Ground truth still reports truth!
    truth = prov.get_call_status("call_dropped", now=150.0)
    assert truth in {"ANSWERED", "COMPLETED"}


def test_cancel_call_clears_scheduled_truth():
    """Cancel accepted clears scheduled truth: get_call_status returns RINGING/INITIATED until cancelled_at, then CANCELLED."""
    rng = random.Random(707)
    behavior = BorrowerBehavior(answer_rate=1.0, mean_talk_s=30.0)
    profile = ProviderProfile(
        setup_latency_s=1.0,
        latency_jitter_s=0.0,
        error_rate=0.0,
        timeout_rate=0.0,
    )
    prov = MockProvider("prov_cancel_fix", profile, behavior, rng)
    prov.place_call("c_cancel_truth", "+7001", now=0.0)

    # Initial state at 0.5 is INITIATED
    assert prov.get_call_status("c_cancel_truth", now=0.5) == "INITIATED"
    # At 1.5 it is RINGING
    assert prov.get_call_status("c_cancel_truth", now=1.5) == "RINGING"

    # Cancel at 1.5s (before borrower answers)
    cancelled = prov.cancel_call("c_cancel_truth", now=1.5)
    assert cancelled is True

    # With setup_latency_s=1.0, cancelled_at = 1.5 + 1.0 = 2.5s
    # Before cancelled_at (e.g. 2.0s): status is RINGING (never ANSWERED or COMPLETED)
    assert prov.get_call_status("c_cancel_truth", now=2.0) == "RINGING"

    # At and after cancelled_at (e.g. 2.5s, 50.0s): status is CANCELLED
    assert prov.get_call_status("c_cancel_truth", now=2.5) == "CANCELLED"
    assert prov.get_call_status("c_cancel_truth", now=50.0) == "CANCELLED"
