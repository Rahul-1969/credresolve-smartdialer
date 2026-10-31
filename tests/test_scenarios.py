"""Unit tests for SmartDialer failure and chaos scenarios."""

from pathlib import Path
import pytest

from smartdialer.scenarios import (
    run_scenario_agent_drop,
    run_scenario_duplicate_events,
    run_scenario_out_of_order_events,
    run_scenario_provider_outage,
    run_scenario_worker_crash,
)


def test_scenario_1_worker_crash(tmp_path: Path):
    """S1: Worker crash recovery preserves invariants and reconciles placed and unplaced calls."""
    db_file = tmp_path / "s1_crash.db"
    res = run_scenario_worker_crash(db_path=db_file, print_report=False)
    assert res["case1_placed"]["after_reaper"]["reaped"] == 1
    assert res["case1_placed"]["end"]["call_state"] in ("COMPLETED", "FAILED")
    assert res["case2_unplaced"]["end"]["call_state"] == "FAILED"
    assert res["case2_unplaced"]["end"]["borrower_attempts"] == 1


def test_scenario_2_provider_outage(tmp_path: Path):
    """S2: Provider outage causes circuit trip, failover, and post-outage reaper reconciliation."""
    db_file = tmp_path / "s2_outage.db"
    res = run_scenario_provider_outage(db_path=db_file, seed=42, print_report=False)
    assert len(res["timeline"]) == 30
    assert res["invariants_clean"] is True
    assert res["truth_mismatches"] == 0


def test_scenario_3_agent_drop(tmp_path: Path):
    """S3: Sudden mass agent drop is detected within <= 2 ticks and keeps abandonment <= 0.03."""
    db_file = tmp_path / "s3_drop.db"
    res = run_scenario_agent_drop(db_path=db_file, seed=42, print_report=False)
    assert res["reaction_ticks"] <= 2
    assert res["abandon_rate"] <= 0.03
    assert res["invariants_clean"] is True


def test_scenario_4_duplicate_events(tmp_path: Path):
    """S4: Duplicate events are deduplicated with monotonic ranks."""
    db_file = tmp_path / "s4_dups.db"
    res = run_scenario_duplicate_events(db_path=db_file, seed=42, print_report=False)
    assert res["ranks_strictly_increasing"] is True
    assert res["agent_events_clean"] is True
    assert res["invariants_clean"] is True
    assert res["tallies"].get("DUPLICATE", 0) > 0 or res["tallies"].get("APPLIED", 0) > 0


def test_scenario_5_out_of_order_events(tmp_path: Path):
    """S5: Out-of-order events are absorbed via monotonic rank transitions."""
    db_file = tmp_path / "s5_ooo.db"
    res = run_scenario_out_of_order_events(db_path=db_file, seed=88, print_report=False)
    assert res["ranks_strictly_increasing"] is True
    assert res["agent_events_clean"] is True
    assert res["invariants_clean"] is True

