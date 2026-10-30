"""Virtual-time discrete simulation world for end-to-end SmartDialer testing.

Single Responsibility:
    Construct and orchestrate multi-worker dialer fleets, mock telecom providers,
    reaper cycles, and agent state transitions in virtual time without physical sleeping or threads.
"""

from collections.abc import Callable
from pathlib import Path
import random
import sqlite3
from typing import Any

from smartdialer.agent_ops import agent_went_offline
from smartdialer.calls import CallState
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.dialer import Dialer, TickReport
from smartdialer.models import AgentState
from smartdialer.pacing import PredictivePacer, ProgressivePacer
from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.provider_a import make_provider_a
from smartdialer.providers.provider_b import make_provider_b
from smartdialer.providers.router import ProviderRouter
from smartdialer.reaper import Reaper
from smartdialer.repo import AgentRepo, transaction
from smartdialer.safety import SafetyConfig, SafetyController


class _VirtualClock:
    """Callable holder for synchronized virtual time."""

    def __init__(self, initial: float = 0.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now


class World:
    """Discrete virtual-time dialer test harness."""

    def __init__(
        self,
        db_path: str,
        config: Config,
        providers: dict[str, Any],
        dialers: list[Dialer],
        reaper: Reaper,
        clock: _VirtualClock,
        sim_wrap_up_s: float = 15.0,
        worker_conns: list[sqlite3.Connection] | None = None,
    ) -> None:
        self.db_path = db_path
        self.config = config
        self.providers = providers
        self.dialers = dialers
        self.reaper = reaper
        self.clock = clock
        self.sim_wrap_up_s = sim_wrap_up_s
        self.now = clock.now
        self._last_reaper_sweep = 0.0
        self._worker_conns = worker_conns or []
        self.conn = connect(db_path)

    def advance_agents(self, now: float) -> int:
        """Transition agents in WRAP_UP whose duration exceeds sim_wrap_up_s to AVAILABLE."""
        agent_repo = AgentRepo(self.conn, self.config)
        cur = self.conn.execute(
            """
            SELECT id FROM agents
            WHERE state = 'WRAP_UP' AND updated_at <= ?;
            """,
            (now - self.sim_wrap_up_s,),
        )
        freed = 0
        for row in cur.fetchall():
            if agent_repo.transition(
                row["id"], AgentState.WRAP_UP, AgentState.AVAILABLE, now=now
            ):
                freed += 1
        return freed

    def step(self, dt: float) -> list[TickReport]:
        """Advance virtual time by dt, step wrap-up agents, run dialer ticks, and sweep reaper."""
        self.now += dt
        self.clock.now = self.now
        self.advance_agents(self.now)

        reports: list[TickReport] = []
        for dialer in self.dialers:
            rep = dialer.tick(self.now)
            reports.append(rep)

        if self.now - self._last_reaper_sweep >= self.config.reaper_interval:
            self.reaper.sweep(self.now)
            self._last_reaper_sweep = self.now

        return reports

    def set_agents_offline(self, n: int) -> list[dict[str, Any]]:
        """Choose n online agents in deterministic order and transition them offline."""
        cur = self.conn.execute(
            """
            SELECT id FROM agents
            WHERE state != 'OFFLINE'
            ORDER BY id ASC
            LIMIT ?;
            """,
            (n,),
        )
        results = []
        for row in cur.fetchall():
            res = agent_went_offline(self.conn, self.config, row["id"], self.now)
            results.append(res)
        return results

    def set_agents_online(self, n: int) -> int:
        """Choose n offline agents in deterministic order and transition them to AVAILABLE."""
        agent_repo = AgentRepo(self.conn, self.config)
        cur = self.conn.execute(
            """
            SELECT id FROM agents
            WHERE state = 'OFFLINE'
            ORDER BY id ASC
            LIMIT ?;
            """,
            (n,),
        )
        brought_online = 0
        for row in cur.fetchall():
            if agent_repo.transition(
                row["id"], AgentState.OFFLINE, AgentState.AVAILABLE, now=self.now
            ):
                brought_online += 1
        return brought_online

    def check_invariants(self) -> list[str]:
        """Verify universal multi-entity integrity invariants across calls, agents, and borrowers."""
        violations: list[str] = []

        # (a) No agent is the agent_id of more than one non-terminal call
        cur_a = self.conn.execute(
            """
            SELECT agent_id, COUNT(*) as cnt
            FROM calls
            WHERE agent_id IS NOT NULL AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
            GROUP BY agent_id
            HAVING cnt > 1;
            """
        )
        for r in cur_a.fetchall():
            violations.append(
                f"Agent {r['agent_id']} is assigned to {r['cnt']} non-terminal calls"
            )

        # (b) Every agent in RESERVED/DIALING/CONNECTED has reserved_call_id naming a NON-terminal call
        # whose agent_id is that agent
        cur_b = self.conn.execute(
            """
            SELECT a.id, a.state, a.reserved_call_id
            FROM agents a
            WHERE a.state IN ('RESERVED', 'DIALING', 'CONNECTED');
            """
        )
        for a in cur_b.fetchall():
            cid = a["reserved_call_id"]
            if not cid:
                violations.append(f"Agent {a['id']} in state {a['state']} has NULL reserved_call_id")
                continue
            cur_c = self.conn.execute(
                "SELECT id, agent_id, state FROM calls WHERE id = ?;", (cid,)
            )
            c = cur_c.fetchone()
            if not c:
                violations.append(
                    f"Agent {a['id']} points to missing call {cid}"
                )
            elif c["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
                violations.append(
                    f"Agent {a['id']} points to terminal call {cid} (state={c['state']})"
                )
            elif c["agent_id"] != a["id"]:
                violations.append(
                    f"Agent {a['id']} points to call {cid} whose agent_id is {c['agent_id']}"
                )

        # (c) Every non-terminal call with agent_id NOT NULL points to an agent whose reserved_call_id equals it
        cur_c = self.conn.execute(
            """
            SELECT c.id, c.agent_id, c.state
            FROM calls c
            WHERE c.agent_id IS NOT NULL AND c.state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
            """
        )
        for c in cur_c.fetchall():
            aid = c["agent_id"]
            cur_ag = self.conn.execute(
                "SELECT id, state, reserved_call_id FROM agents WHERE id = ?;", (aid,)
            )
            ag = cur_ag.fetchone()
            if not ag:
                violations.append(f"Call {c['id']} points to non-existent agent {aid}")
            elif ag["reserved_call_id"] != c["id"]:
                violations.append(
                    f"Call {c['id']} points to agent {aid} whose reserved_call_id is {ag['reserved_call_id']}"
                )

        # (d) No borrower is IN_CALL without a non-terminal call
        cur_d = self.conn.execute(
            """
            SELECT b.id
            FROM borrowers b
            WHERE b.state = 'IN_CALL'
              AND NOT EXISTS (
                  SELECT 1 FROM calls c
                  WHERE c.borrower_id = b.id
                    AND c.state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
              );
            """
        )
        for b in cur_d.fetchall():
            violations.append(
                f"Borrower {b['id']} is IN_CALL without an active non-terminal call"
            )

        return violations

    def close(self) -> None:
        """Close all owned database connections."""
        self.conn.close()
        for c in self._worker_conns:
            try:
                c.close()
            except Exception:
                pass


def build_world(
    db_path: str | Path,
    n_agents: int,
    n_borrowers: int,
    answer_rate: float | Callable[[float], float],
    mean_talk_s: float | Callable[[float], float],
    mode: str,  # 'progressive' | 'predictive'
    seed: int,
    n_workers: int = 1,
    primary: str = "A",
    sim_wrap_up_s: float = 15.0,
    safety_config: SafetyConfig | None = None,
) -> World:
    """Build and initialize a complete Simulation World instance."""
    path_str = str(db_path)
    init_conn = connect(path_str)
    init_schema(init_conn)

    # 1. Seed agents in state AVAILABLE
    for i in range(n_agents):
        init_conn.execute(
            """
            INSERT INTO agents (id, state, available_since, updated_at)
            VALUES (?, 'AVAILABLE', 0.0, 0.0);
            """,
            (f"agent_{i:04d}",),
        )

    # 2. Seed borrowers in state QUEUED
    for j in range(n_borrowers):
        init_conn.execute(
            """
            INSERT INTO borrowers (id, phone, state, updated_at)
            VALUES (?, ?, 'QUEUED', 0.0);
            """,
            (f"borrower_{j:04d}", f"+1800555{j:04d}"),
        )

    init_conn.close()

    # 3. Telecom providers
    shared_behavior = BorrowerBehavior(answer_rate=answer_rate, mean_talk_s=mean_talk_s)
    rng_a = random.Random(seed * 1000 + 1)
    rng_b = random.Random(seed * 1000 + 2)

    prov_a = make_provider_a(rng_a, shared_behavior)
    prov_b = make_provider_b(rng_b, shared_behavior)
    providers = {"provider_a": prov_a, "provider_b": prov_b}

    priority = (
        ["provider_a", "provider_b"] if primary == "A" else ["provider_b", "provider_a"]
    )
    default_provider = priority[0]

    vclock = _VirtualClock(0.0)

    cfg = Config(
        db_path=path_str,
        default_provider=default_provider,
        reaper_interval=5.0,
        clock=vclock,
    )
    s_cfg = safety_config or SafetyConfig()

    # 4. Dialers and workers
    dialers: list[Dialer] = []
    worker_conns: list[sqlite3.Connection] = []

    for w in range(n_workers):
        w_conn = connect(path_str)
        worker_conns.append(w_conn)

        w_router = ProviderRouter(
            providers=providers,
            priority=priority,
            max_definite_failovers=1,
        )

        def make_health_fn(router: ProviderRouter) -> Callable[[float], dict[str, str]]:
            return lambda t: {p_name: st.value for p_name, st in router.health(t).items()}

        safety_ctrl = SafetyController(
            config=s_cfg,
            provider_health_fn=make_health_fn(w_router),
            clock=vclock,
        )

        if mode.lower() == "progressive":
            pacer = ProgressivePacer()
        else:
            pacer = PredictivePacer()

        dialer = Dialer(
            conn=w_conn,
            config=cfg,
            safety=safety_ctrl,
            router=w_router,
            providers=providers,
            pacer=pacer,
            worker_id=f"worker_{w:02d}",
        )
        dialers.append(dialer)

    # 5. Dedicated reaper
    reaper_conn = connect(path_str)
    worker_conns.append(reaper_conn)
    reaper = Reaper(conn=reaper_conn, config=cfg, providers=providers)

    return World(
        db_path=path_str,
        config=cfg,
        providers=providers,
        dialers=dialers,
        reaper=reaper,
        clock=vclock,
        sim_wrap_up_s=sim_wrap_up_s,
        worker_conns=worker_conns,
    )
