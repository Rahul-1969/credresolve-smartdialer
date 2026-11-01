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


def test_quick_loadtest_default_out_path(monkeypatch, tmp_path: Path):
    """Verify loadtest --quick default output path is under results_quick/ and does not touch docs/."""
    from smartdialer.cli import main

    monkeypatch.chdir(tmp_path)

    exit_code = main(["loadtest", "--quick"])
    assert exit_code == 0

    quick_out = tmp_path / "results_quick" / "loadtest_results_quick.md"
    assert quick_out.exists(), f"Expected quick load test output at {quick_out}"
    content = quick_out.read_text(encoding="utf-8")
    assert "SmartDialer Performance and Load Test Report" in content
    assert "## 4. Single-Thread Dispatch Statement Breakdown at 1,000 Agents" in content
    assert "Attempt Distribution & 96 / 992 Truncation Root Cause" not in content

    docs_out = tmp_path / "docs" / "loadtest_results.md"
    assert not docs_out.exists(), "docs/loadtest_results.md should not have been created"


