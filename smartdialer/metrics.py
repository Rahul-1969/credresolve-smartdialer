"""Telemetry collection and system snapshot reader for dialer operations.

Single Responsibility:
    Query the SQLite database to compute atomic point-in-time metrics across
    agent availability, active calls, response rate windows, and abandonment rates.

Metric Definitions:
    - unbound_in_flight: Calls with agent_id IS NULL and state in (RESERVED, INITIATED, RINGING).
    - ringing_unbound: The RINGING subset of unbound_in_flight calls.
    - unanswered_unbound: Calls with agent_id IS NULL and state in (RESERVED, INITIATED, RINGING).
    - bound_in_flight: Calls with agent_id NOT NULL and state in (RESERVED, INITIATED, RINGING).
    - answered_waiting: Calls with agent_id IS NULL, state in (ANSWERED, CONNECTED) and abandoned = 0.
    - pending_*: SUM(remaining) over safety_allotments with expires_at > now, grouped by mode.
    - RateStats (window w): Among calls with created_at >= now - w and state_rank >= 2:
      answered = count(answered_at IS NOT NULL);
      resolved = answered + count(answered_at IS NULL AND end_reason = 'NO_ANSWER').
      Provider errors and cancellations are excluded from both counts as they do not indicate
      borrower intent.
    - AbandonStats: Among calls with answered_at >= now - abandon_window_s:
      answered = total count, abandoned = count where abandoned = 1.
    - avg_talk_s: Average of (updated_at - answered_at) over COMPLETED calls with
      answered_at >= now - long_window_s, or None if no completed calls exist in the window.
"""

import sqlite3

from smartdialer.models import AgentState
from smartdialer.snapshot import AbandonStats, RateStats, SystemSnapshot


def read_snapshot(
    conn: sqlite3.Connection,
    now: float,
    short_window_s: float,
    long_window_s: float,
    abandon_window_s: float,
) -> SystemSnapshot:
    """Read a consistent point-in-time SystemSnapshot from the database."""
    # 1. Agent states
    agents_by_state = {st.value: 0 for st in AgentState}
    cur_agents = conn.execute("SELECT state, COUNT(*) as cnt FROM agents GROUP BY state;")
    for row in cur_agents.fetchall():
        agents_by_state[row["state"]] = row["cnt"]

    available = agents_by_state.get(AgentState.AVAILABLE.value, 0)
    wrap_up = agents_by_state.get(AgentState.WRAP_UP.value, 0)
    connected = agents_by_state.get(AgentState.CONNECTED.value, 0)
    online = sum(
        cnt
        for st, cnt in agents_by_state.items()
        if st not in (AgentState.OFFLINE.value, AgentState.PAUSED.value)
    )

    # 2. Call tallies
    cur_calls = conn.execute(
        """
        SELECT
            SUM(CASE WHEN agent_id IS NULL AND state IN ('RESERVED', 'INITIATED', 'RINGING') THEN 1 ELSE 0 END) AS unbound_in_flight,
            SUM(CASE WHEN agent_id IS NULL AND state = 'RINGING' THEN 1 ELSE 0 END) AS ringing_unbound,
            SUM(CASE WHEN agent_id IS NULL AND state IN ('RESERVED', 'INITIATED', 'RINGING') THEN 1 ELSE 0 END) AS unanswered_unbound,
            SUM(CASE WHEN agent_id IS NOT NULL AND state IN ('RESERVED', 'INITIATED', 'RINGING') THEN 1 ELSE 0 END) AS bound_in_flight,
            SUM(CASE WHEN agent_id IS NULL AND state IN ('ANSWERED', 'CONNECTED') AND abandoned = 0 THEN 1 ELSE 0 END) AS answered_waiting
        FROM calls;
        """
    )
    call_row = cur_calls.fetchone()
    unbound_in_flight = call_row["unbound_in_flight"] or 0
    ringing_unbound = call_row["ringing_unbound"] or 0
    unanswered_unbound = call_row["unanswered_unbound"] or 0
    bound_in_flight = call_row["bound_in_flight"] or 0
    answered_waiting = call_row["answered_waiting"] or 0

    # 3. Pending allotments
    pending_progressive = 0
    pending_predictive = 0
    cur_allotments = conn.execute(
        """
        SELECT mode, SUM(remaining) as total_remaining
        FROM safety_allotments
        WHERE expires_at > ?
        GROUP BY mode;
        """,
        (now,),
    )
    for row in cur_allotments.fetchall():
        if row["mode"] == "PROGRESSIVE":
            pending_progressive = row["total_remaining"] or 0
        elif row["mode"] == "PREDICTIVE":
            pending_predictive = row["total_remaining"] or 0

    # 4. RateStats helper
    def _read_rate_stats(window_s: float) -> RateStats:
        cutoff = now - window_s
        cur = conn.execute(
            """
            SELECT
                SUM(CASE WHEN answered_at IS NOT NULL THEN 1 ELSE 0 END) AS answered,
                SUM(CASE WHEN answered_at IS NULL AND end_reason = 'NO_ANSWER' THEN 1 ELSE 0 END) AS no_answer
            FROM calls
            WHERE created_at >= ? AND state_rank >= 2;
            """,
            (cutoff,),
        )
        r = cur.fetchone()
        ans = r["answered"] or 0
        no_ans = r["no_answer"] or 0
        return RateStats(answered=ans, resolved=ans + no_ans)

    short_stats = _read_rate_stats(short_window_s)
    long_stats = _read_rate_stats(long_window_s)

    # 5. AbandonStats
    abandon_cutoff = now - abandon_window_s
    cur_abandon = conn.execute(
        """
        SELECT
            COUNT(*) AS answered,
            SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) AS abandoned
        FROM calls
        WHERE answered_at >= ?;
        """,
        (abandon_cutoff,),
    )
    r_ab = cur_abandon.fetchone()
    ab_ans = r_ab["answered"] or 0
    ab_cnt = r_ab["abandoned"] or 0
    abandon_stats = AbandonStats(answered=ab_ans, abandoned=ab_cnt)

    # 6. avg_talk_s
    talk_cutoff = now - long_window_s
    cur_talk = conn.execute(
        """
        SELECT AVG(updated_at - answered_at) AS avg_talk
        FROM calls
        WHERE state = 'COMPLETED' AND answered_at >= ?;
        """,
        (talk_cutoff,),
    )
    r_talk = cur_talk.fetchone()
    avg_talk = float(r_talk["avg_talk"]) if (r_talk and r_talk["avg_talk"] is not None) else None

    return SystemSnapshot(
        now=now,
        agents_by_state=agents_by_state,
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
        short=short_stats,
        long=long_stats,
        abandon=abandon_stats,
        avg_talk_s=avg_talk,
    )

