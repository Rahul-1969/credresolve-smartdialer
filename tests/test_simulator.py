"""Unit tests for SmartDialer simulation engine."""

from pathlib import Path
import pytest

from smartdialer.simulator import run_single_simulation


def test_simulator_determinism(tmp_path: Path):
    """Running same scenario, parameters, and seed produces identical metrics."""
    m1, _ = run_single_simulation(
        scenario="B",
        variant="clean",
        mode="predictive",
        agents=20,
        duration=180,
        seed=42,
    )

    m2, _ = run_single_simulation(
        scenario="B",
        variant="clean",
        mode="predictive",
        agents=20,
        duration=180,
        seed=42,
    )

    assert m1.calls_initiated == m2.calls_initiated
    assert m1.calls_connected == m2.calls_connected
    assert m1.answered == m2.answered
    assert m1.abandoned == m2.abandoned
    assert m1.utilization == m2.utilization
    assert m1.abandon_rate == m2.abandon_rate


def test_simulator_predictive_compliance_and_invariants(tmp_path: Path):
    """Short simulation run verifies predictive abandon_rate <= 0.03 and clean invariants."""
    m, tl = run_single_simulation(
        scenario="B",
        variant="clean",
        mode="predictive",
        agents=20,
        duration=180,
        seed=99,
    )

    assert m.abandon_rate <= 0.03
    assert m.utilization > 0.0
    assert len(tl) == 6  # 180s / 30s buckets

