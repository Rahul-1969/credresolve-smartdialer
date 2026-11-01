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


def test_check_simulation_metrics_long_vs_short(capsys):
    """check_simulation_metrics raises on long runs with violations and only warns on short runs."""
    from smartdialer.simulator import check_simulation_metrics

    # 1. Answer rate violation on clean scenario A (target 0.2, observed 0.5):
    # Long run with >= 600 resolved calls raises AssertionError:
    long_violating = {
        "scenario": "A",
        "variant": "clean",
        "mode": "predictive",
        "duration": 1200,
        "calls_initiated": 700,
        "answered": 350,
        "no_answer": 350,
        "observed_answer_rate": 0.50,
        "utilization": 0.50,
        "implied_utilization": 0.50,
    }
    with pytest.raises(AssertionError, match=r"Self-check violation: \|observed_answer_rate"):
        check_simulation_metrics(long_violating)

    # Short run with < 600 resolved calls prints warning to stderr and sets selfcheck_skipped:
    short_run = {
        "scenario": "A",
        "variant": "clean",
        "mode": "predictive",
        "duration": 120,
        "calls_initiated": 50,
        "answered": 25,
        "no_answer": 25,
        "observed_answer_rate": 0.50,
        "utilization": 0.50,
        "implied_utilization": 0.50,
    }
    check_simulation_metrics(short_run)
    assert short_run.get("selfcheck_skipped") is True
    captured = capsys.readouterr()
    assert "self-check skipped: only 50 resolved calls" in captured.err

    # 2. Implied-utilization violation (> 0.05 diff):
    # Long run (duration >= 600) raises AssertionError:
    util_violating_long = {
        "scenario": "D",
        "variant": "clean",
        "mode": "progressive",
        "duration": 1200,
        "calls_initiated": 100,
        "answered": 50,
        "no_answer": 50,
        "observed_answer_rate": 0.50,
        "utilization": 0.50,
        "implied_utilization": 0.65,  # diff 0.15 > 0.05
    }
    with pytest.raises(AssertionError, match=r"Self-check violation: \|implied_utilization"):
        check_simulation_metrics(util_violating_long)

    # Short run (duration < 600) only warns to stderr:
    util_violating_short = {
        "scenario": "D",
        "variant": "clean",
        "mode": "progressive",
        "duration": 300,
        "calls_initiated": 100,
        "answered": 50,
        "no_answer": 50,
        "observed_answer_rate": 0.50,
        "utilization": 0.50,
        "implied_utilization": 0.65,  # diff 0.15 > 0.05
    }
    check_simulation_metrics(util_violating_short)
    captured = capsys.readouterr()
    assert "Warning: Self-check violation: |implied_utilization" in captured.err


