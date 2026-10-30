"""Generate real explain() examples using seeded SQLite databases and SafetyController."""

import time
from smartdialer.db import connect, init_schema
from smartdialer.pacing.base import PacingMode, PacingRequest
from smartdialer.safety import SafetyConfig, SafetyController, explain


def main():
    print("=== SEEDING SCENARIO A: REDUCED DECISION ===")
    conn_a = connect(":memory:")
    init_schema(conn_a)

    now = 1000.0

    # 1. Seed 10 AVAILABLE agents and 4 WRAP_UP agents (total 14 online)
    for i in range(10):
        conn_a.execute(
            "INSERT INTO agents (id, state, updated_at) VALUES (?, 'AVAILABLE', ?)",
            (f"agent_avail_{i}", now),
        )
    for i in range(4):
        conn_a.execute(
            "INSERT INTO agents (id, state, updated_at) VALUES (?, 'WRAP_UP', ?)",
            (f"agent_wrap_{i}", now),
        )

    # 2. Seed 3 in-flight unbound calls (state RESERVED)
    for i in range(3):
        conn_a.execute(
            "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '5550001', 'IN_CALL', ?)",
            (f"borrower_inf_{i}", now),
        )
        conn_a.execute(
            "INSERT INTO calls (id, borrower_id, state, state_rank, created_at, updated_at) VALUES (?, ?, 'RESERVED', 1, ?, ?)",
            (f"call_inflight_{i}", f"borrower_inf_{i}", now, now),
        )

    # 3. Seed 60 completed calls: 30 answered, 30 unanswered
    # 3. Seed 60 completed calls: 30 answered, 30 unanswered within short window (now - 30)
    for i in range(30):
        conn_a.execute(
            "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '5550002', 'DONE', ?)",
            (f"borrower_ans_{i}", now),
        )
        conn_a.execute(
            """
            INSERT INTO calls (id, borrower_id, state, state_rank, answered_at, created_at, updated_at)
            VALUES (?, ?, 'COMPLETED', 6, ?, ?, ?)
            """,
            (f"call_ans_{i}", f"borrower_ans_{i}", now - 15, now - 30, now - 10),
        )
    for i in range(30):
        conn_a.execute(
            "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '5550003', 'DONE', ?)",
            (f"borrower_noans_{i}", now),
        )
        conn_a.execute(
            """
            INSERT INTO calls (id, borrower_id, state, state_rank, answered_at, end_reason, created_at, updated_at)
            VALUES (?, ?, 'COMPLETED', 6, NULL, 'NO_ANSWER', ?, ?)
            """,
            (f"call_noans_{i}", f"borrower_noans_{i}", now - 30, now - 10),
        )

    controller = SafetyController(
        config=SafetyConfig(),
        provider_health_fn=lambda t: {"mock": "CLOSED"},
        clock=lambda: now,
    )

    req_a = PacingRequest(
        mode=PacingMode.PREDICTIVE,
        requested_calls=15,
        rationale={"source": "explain_examples"},
    )
    decision_a = controller.evaluate(conn_a, req_a, now=now)
    print("Decision A:")
    print(f"  verdict: {decision_a.verdict}")
    print(f"  approved: {decision_a.approved}")
    print(f"  reasons: {decision_a.reasons}")
    print(f"  inputs: {decision_a.inputs}")
    print("Explain A:")
    print(explain(decision_a))
    print()

    print("=== SEEDING SCENARIO B: CAPACITY_EXCESS DECISION ===")
    conn_b = connect(":memory:")
    init_schema(conn_b)

    # 1. Seed 10 AVAILABLE agents (cap = 10, max overdial ratio = 1.5 -> allowed = 15)
    for i in range(10):
        conn_b.execute(
            "INSERT INTO agents (id, state, updated_at) VALUES (?, 'AVAILABLE', ?)",
            (f"agent_b_avail_{i}", now),
        )

    # 2. Seed 60 completed calls: 30 answered, 30 unanswered
    for i in range(30):
        conn_b.execute(
            "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '5550004', 'DONE', ?)",
            (f"borrower_b_ans_{i}", now),
        )
        conn_b.execute(
            """
            INSERT INTO calls (id, borrower_id, state, state_rank, answered_at, created_at, updated_at)
            VALUES (?, ?, 'COMPLETED', 6, ?, ?, ?)
            """,
            (f"call_b_ans_{i}", f"borrower_b_ans_{i}", now - 15, now - 30, now - 10),
        )
    for i in range(30):
        conn_b.execute(
            "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '5550005', 'DONE', ?)",
            (f"borrower_b_noans_{i}", now),
        )
        conn_b.execute(
            """
            INSERT INTO calls (id, borrower_id, state, state_rank, answered_at, end_reason, created_at, updated_at)
            VALUES (?, ?, 'COMPLETED', 6, NULL, 'NO_ANSWER', ?, ?)
            """,
            (f"call_b_noans_{i}", f"borrower_b_noans_{i}", now - 30, now - 10),
        )

    # 3. Seed 20 unbound in-flight calls (state INITIATED, agent_id NULL) -> excess = 20 - 12 = 8
    for i in range(20):
        conn_b.execute(
            "INSERT INTO borrowers (id, phone, state, updated_at) VALUES (?, '5550006', 'IN_CALL', ?)",
            (f"borrower_b_inf_{i}", now),
        )
        conn_b.execute(
            "INSERT INTO calls (id, borrower_id, state, state_rank, created_at, updated_at) VALUES (?, ?, 'INITIATED', 2, ?, ?)",
            (f"call_b_inflight_{i}", f"borrower_b_inf_{i}", now, now),
        )

    req_b = PacingRequest(
        mode=PacingMode.PREDICTIVE,
        requested_calls=5,
        rationale={"source": "explain_examples"},
    )
    decision_b = controller.evaluate(conn_b, req_b, now=now)
    print("Decision B:")
    print(f"  verdict: {decision_b.verdict}")
    print(f"  approved: {decision_b.approved}")
    print(f"  cancel_excess: {decision_b.cancel_excess}")
    print(f"  reasons: {decision_b.reasons}")
    print(f"  inputs: {decision_b.inputs}")
    print("Explain B:")
    print(explain(decision_b))


if __name__ == "__main__":
    main()
