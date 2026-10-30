"""Integration test: 300 calls through Provider B with router, event application, and chaos."""

import random
import pytest

from smartdialer.calls import RANK, ApplyResult
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState, BorrowerState, CallState
from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.provider_b import make_provider_b
from smartdialer.providers.router import PlaceOutcomeKind, ProviderRouter
from smartdialer.repo import AgentRepo, BorrowerRepo, CallRepo


def test_300_calls_provider_b_chaos_integration(tmp_path):
    """300 calls through Provider B: router placement, time advance, event deduplication, and invariant checks."""
    db_file = tmp_path / "integration_test.db"
    conn = connect(db_file)
    init_schema(conn)
    cfg = Config(db_path=str(db_file))

    a_repo = AgentRepo(conn, cfg)
    b_repo = BorrowerRepo(conn, cfg)
    c_repo = CallRepo(conn, cfg)

    # 1. Provider B with fixed seed, answer_rate 0.5, mean talk 60s
    seed = 42
    rng = random.Random(seed)
    behavior = BorrowerBehavior(answer_rate=0.5, mean_talk_s=60.0)
    prov_b = make_provider_b(rng, behavior)
    router = ProviderRouter(
        providers={"provider_b": prov_b},
        priority=["provider_b"],
        timeout_retries=1,
    )

    num_calls = 300
    all_call_ids = [f"call_{i:03d}" for i in range(num_calls)]

    # 2. Setup 300 calls, each with claimed borrower and reserved agent
    for i in range(num_calls):
        call_id = all_call_ids[i]
        borrower_id = f"b_{i:03d}"
        agent_id = f"a_{i:03d}"
        phone = f"+1800555{i:04d}"

        b_repo.enqueue_borrower(borrower_id, phone, next_attempt_at=0.0)
        claimed = b_repo.claim_borrowers("w1", 1)
        assert claimed == [borrower_id]

        a_repo.upsert_agent(agent_id, state=AgentState.AVAILABLE)
        reserved = a_repo.reserve_agents("w1", 1, call_ids=[call_id])
        assert reserved == [agent_id]

        assert c_repo.create_call(call_id, borrower_id, agent_id, provider="provider_b") is True

    # 3. Place calls via router at now=0.0
    for i in range(num_calls):
        call_id = all_call_ids[i]
        phone = f"+1800555{i:04d}"

        outcome = router.place(call_id, phone, now=0.0)
        if outcome.kind == PlaceOutcomeKind.PLACED:
            c_repo.mark_initiated(call_id, outcome.provider_call_id, now=0.0)
        elif outcome.kind == PlaceOutcomeKind.DEFINITE_FAILURE:
            c_repo.apply_event(call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=0.0)
        elif outcome.kind == PlaceOutcomeKind.AMBIGUOUS:
            if prov_b.get_call_status(call_id, 0.0) is None:
                c_repo.apply_event(call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=0.0)
            else:
                c_repo.mark_initiated(call_id, f"provider_b_{call_id}", now=0.0)
        else:
            c_repo.apply_event(call_id, CallState.FAILED, reason="PROVIDER_ERROR", now=0.0)

    # 4. Advance virtual time 0 to 1500s in 0.5s steps and apply polled events
    results_count: dict[ApplyResult, int] = {res: 0 for res in ApplyResult}

    for step in range(3001):
        t = step * 0.5
        events = prov_b.poll_events(now=t)
        for evt in events:
            res = c_repo.apply_event(
                evt.call_id,
                evt.event_type,
                provider_event_id=evt.provider_event_id,
                now=t,
                reason=evt.reason,
            )
            results_count[res] += 1

    # 5. Assertions at virtual time 1500s

    # (i) Every call is terminal and matches provider ground truth
    for call_id in all_call_ids:
        call = c_repo.get_call(call_id)
        assert call is not None
        assert call["state"] in {"COMPLETED", "FAILED", "CANCELLED"}, f"Call {call_id} not terminal: {call['state']}"
        prov_status = prov_b.get_call_status(call_id, 1500.0)
        if prov_status is None:
            # Placement failed or timed out without creating call at provider
            assert call["state"] == "FAILED"
            assert call["end_reason"] == "PROVIDER_ERROR"
        else:
            assert call["state"] == prov_status, (
                f"Call {call_id} state mismatch: DB={call['state']} vs Prov={prov_status}"
            )

    # (ii) For every call, applied call_events ordered by id have strictly increasing rank
    for call_id in all_call_ids:
        cur = conn.execute(
            "SELECT event_type FROM call_events WHERE call_id = ? AND applied = 1 ORDER BY id ASC;",
            (call_id,),
        )
        applied_ranks = [RANK[CallState(row["event_type"])] for row in cur.fetchall()]
        assert len(applied_ranks) > 0
        assert applied_ranks == sorted(applied_ranks)
        assert len(applied_ranks) == len(set(applied_ranks)), (
            f"Call {call_id} applied events not strictly increasing: {applied_ranks}"
        )

    # (iii) For every agent, agent_events contains each (from_state, to_state) pair at most once
    for i in range(num_calls):
        agent_id = f"a_{i:03d}"
        cur = conn.execute(
            "SELECT from_state, to_state FROM agent_events WHERE agent_id = ? ORDER BY id ASC;",
            (agent_id,),
        )
        pairs = [(row["from_state"], row["to_state"]) for row in cur.fetchall()]
        assert len(pairs) == len(set(pairs)), f"Duplicate transition pair in agent {agent_id}: {pairs}"

    # (iv) No agent is left in RESERVED/DIALING/CONNECTED and no borrower left IN_CALL/CLAIMED
    cur_agents = conn.execute(
        "SELECT id, state FROM agents WHERE state IN ('RESERVED', 'DIALING', 'CONNECTED');"
    )
    unreleased_agents = cur_agents.fetchall()
    assert unreleased_agents == [], f"Agents left in non-terminal states: {unreleased_agents}"

    cur_borrowers = conn.execute(
        "SELECT id, state FROM borrowers WHERE state IN ('IN_CALL', 'CLAIMED');"
    )
    unreleased_borrowers = cur_borrowers.fetchall()
    assert unreleased_borrowers == [], f"Borrowers left in non-terminal states: {unreleased_borrowers}"

    # (v) Counts of DUPLICATE, STALE_IGNORED, TERMINAL_IGNORED each > 0, total > 0
    dup_count = results_count[ApplyResult.DUPLICATE]
    stale_count = results_count[ApplyResult.STALE_IGNORED]
    term_count = results_count[ApplyResult.TERMINAL_IGNORED]
    applied_count = results_count[ApplyResult.APPLIED]
    total_chaos = dup_count + stale_count + term_count

    assert dup_count > 0, f"Expected duplicates > 0, got {dup_count}"
    assert stale_count > 0, f"Expected stale > 0, got {stale_count}"
    assert term_count > 0, f"Expected terminal ignored > 0, got {term_count}"
    assert total_chaos > 0

    print(
        f"\n[CHAOS METRICS] APPLIED: {applied_count}, DUPLICATE: {dup_count}, "
        f"STALE_IGNORED: {stale_count}, TERMINAL_IGNORED: {term_count}, TOTAL_CHAOS: {total_chaos}"
    )

    conn.close()

