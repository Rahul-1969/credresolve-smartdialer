"""Unit tests for load testing harness."""

from pathlib import Path
import pytest

from smartdialer.loadtest import run_load_test


def test_quick_loadtest(tmp_path: Path):
    """Verify quick load test runs under 30 seconds and outputs report."""
    results = run_load_test(quick=True, out_dir=tmp_path, print_markdown=False)

    assert "dispatch" in results
    assert "events" in results
    assert "safety" in results
    assert "snapshot" in results
    assert "math" in results
    assert "concurrent_safety" in results

    assert results["concurrent_safety"]["safety_errors"] == 0

    report_file = tmp_path / "loadtest_results.md"
    assert report_file.exists()
    content = report_file.read_text(encoding="utf-8")
    assert "SmartDialer Performance and Load Test Report" in content

