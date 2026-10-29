"""Tests for agent state machine and AgentRepo persistence."""

import itertools
import pytest

from smartdialer.agents import ALLOWED_TRANSITIONS, is_allowed
from smartdialer.config import Config
from smartdialer.db import connect, init_schema
from smartdialer.models import AgentState
from smartdialer.repo import AgentRepo, IllegalTransition


@pytest.fixture
def repo(tmp_path):
    """Provide a thread-isolated AgentRepo connected to a temporary SQLite database."""
    db_file = tmp_path / "agents_test.db"
    conn = connect(db_file)
    init_schema(conn)
    repository = AgentRepo(conn=conn, config=Config(db_path=str(db_file)))
    yield repository
    conn.close()


ALL_STATES = list(AgentState)
ALL_PAIRS = list(itertools.product(ALL_STATES, ALL_STATES))


@pytest.mark.parametrize("from_state,to_state", ALL_PAIRS)
def test_all_state_transitions(repo, from_state, to_state):
    """Assert every allowed transition succeeds and every disallowed pair raises IllegalTransition."""
    agent_id = f"agent_{from_state.value}_{to_state.value}"
    repo.upsert_agent(agent_id, state=from_state)

    if is_allowed(from_state, to_state):
        res = repo.transition(agent_id, from_state=from_state, to_state=to_state)
        assert res is True
        agent = repo.get_agent(agent_id)
        assert agent is not None
        assert agent["state"] == to_state.value
    else:
        with pytest.raises(IllegalTransition):
            repo.transition(agent_id, from_state=from_state, to_state=to_state)
        # Verify state was untouched
        agent = repo.get_agent(agent_id)
        assert agent is not None
        assert agent["state"] == from_state.value


def test_transition_with_wrong_from_state_returns_false(repo):
    """Assert transition with incorrect from_state returns False and mutates nothing."""
    agent_id = "agent_idle"
    repo.upsert_agent(agent_id, state=AgentState.AVAILABLE)

    # Valid transition pair (RESERVED -> DIALING), but agent is actually in AVAILABLE
    result = repo.transition(
        agent_id,
        from_state=AgentState.RESERVED,
        to_state=AgentState.DIALING,
    )
    assert result is False

    agent = repo.get_agent(agent_id)
    assert agent is not None
    assert agent["state"] == AgentState.AVAILABLE.value
    assert agent["version"] == 0


def test_reserve_agents_bounds_and_zero(repo):
    """Assert requesting from zero returns empty list, and requesting more than available returns only available."""
    # Zero available
    assert repo.reserve_agents("worker_1", 5) == []

    # Add 2 agents
    repo.upsert_agent("a1", state=AgentState.AVAILABLE)
    repo.upsert_agent("a2", state=AgentState.AVAILABLE)

    # Request 5, should only get 2
    reserved = repo.reserve_agents("worker_1", 5)
    assert len(reserved) == 2
    assert set(reserved) == {"a1", "a2"}

    # Subsequent request gets none
    assert repo.reserve_agents("worker_1", 1) == []


def test_longest_idle_first_ordering(tmp_path):
    """Assert reserve_agents selects agents in longest-idle-first order using injected clock."""
    db_file = tmp_path / "ordering_test.db"
    conn = connect(db_file)
    init_schema(conn)

    current_time = 100.0

    def mock_clock():
        return current_time

    config = Config(db_path=str(db_file), clock=mock_clock)
    repo = AgentRepo(conn=conn, config=config)

    # Insert agent 1 at t=100.0
    current_time = 100.0
    repo.upsert_agent("agent_1", state=AgentState.AVAILABLE)

    # Insert agent 2 at t=120.0
    current_time = 120.0
    repo.upsert_agent("agent_2", state=AgentState.AVAILABLE)

    # Insert agent 3 at t=110.0
    current_time = 110.0
    repo.upsert_agent("agent_3", state=AgentState.AVAILABLE)

    # Reserve 2 agents -> longest idle should be agent_1 (100.0) then agent_3 (110.0)
    current_time = 150.0
    reserved = repo.reserve_agents("worker_ordered", 2)
    assert reserved == ["agent_1", "agent_3"]

    conn.close()


def test_adversarial_stale_view(tmp_path):
    """Assert worker attempting transition on a stale view returns False and does not overwrite reservation."""
    db_file = tmp_path / "stale_test.db"
    conn_x = connect(db_file)
    conn_y = connect(db_file)
    init_schema(conn_x)

    repo_x = AgentRepo(conn=conn_x)
    repo_y = AgentRepo(conn=conn_y)

    repo_x.upsert_agent("shared_agent", state=AgentState.AVAILABLE)

    # Worker X inspects agent as AVAILABLE
    agent_snapshot = repo_x.get_agent("shared_agent")
    assert agent_snapshot["state"] == AgentState.AVAILABLE.value

    # Worker Y reserves the agent concurrently
    reserved_y = repo_y.reserve_agents("worker_Y", 1)
    assert reserved_y == ["shared_agent"]

    # Worker X tries to transition based on its earlier view
    success_x = repo_x.transition(
        "shared_agent",
        from_state=AgentState.AVAILABLE,
        to_state=AgentState.RESERVED,
        worker_id="worker_X",
    )
    assert success_x is False

    # Verify agent remains reserved by Y
    final_agent = repo_x.get_agent("shared_agent")
    assert final_agent["state"] == AgentState.RESERVED.value
    assert final_agent["reserved_by"] == "worker_Y"

    conn_x.close()
    conn_y.close()

