"""Command-line interface for SmartDialer.

Single Responsibility:
    Provide CLI entrypoints and argument parsing for database initialization,
    operational failure scenarios, campaign simulation, and load testing.
"""

import argparse
from pathlib import Path
import sys

from smartdialer.db import connect, init_schema


def cmd_init_db(args: argparse.Namespace) -> int:
    """Initialize the SQLite database schema."""
    db_path = getattr(args, "db", "smartdialer.db")
    conn = connect(db_path)
    try:
        init_schema(conn)
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        )
        tables = [row["name"] for row in cur.fetchall()]
        print(f"Initialized database at '{db_path}'. Tables: {', '.join(tables)}")
        return 0
    finally:
        conn.close()


def cmd_scenarios(args: argparse.Namespace) -> int:
    """Run operational failure scenarios."""
    target = getattr(args, "name", "all") or "all"
    target = target.lower()

    from smartdialer.scenarios import (
        run_all_scenarios,
        run_scenario_agent_drop,
        run_scenario_duplicate_events,
        run_scenario_out_of_order_events,
        run_scenario_provider_outage,
        run_scenario_worker_crash,
    )

    if target in ("worker_crash", "s1"):
        run_scenario_worker_crash(print_report=True)
    elif target in ("provider_outage", "s2"):
        run_scenario_provider_outage(print_report=True)
    elif target in ("agent_drop", "s3"):
        run_scenario_agent_drop(print_report=True)
    elif target in ("duplicate_events", "s4"):
        run_scenario_duplicate_events(print_report=True)
    elif target in ("out_of_order_events", "s5"):
        run_scenario_out_of_order_events(print_report=True)
    elif target in ("all", ""):
        run_all_scenarios(print_reports=True)
    else:
        print(
            f"Unknown scenario: {target}. Available: worker_crash, provider_outage, "
            "agent_drop, duplicate_events, out_of_order_events, all"
        )
        return 1
    return 0


def cmd_simulate(args: argparse.Namespace) -> int:
    """Run dialer campaign simulation across scenarios."""
    from smartdialer.simulator import run_full_suite

    scenarios_arg = getattr(args, "scenarios", "A,B,C,D") or "A,B,C,D"
    sc_list = [s.strip().upper() for s in scenarios_arg.split(",") if s.strip()]
    agents = getattr(args, "agents", 50)
    duration = getattr(args, "duration", 1200)
    seed = getattr(args, "seed", 7)
    out_dir = getattr(args, "out", "docs")

    try:
        run_full_suite(
            scenarios=sc_list,
            agents=agents,
            duration=duration,
            seed=seed,
            out_dir=out_dir,
            print_markdown=True,
        )
    except AssertionError as exc:
        print(f"Self-check failed: {exc}", file=sys.stderr)
        return 2
    return 0


def cmd_loadtest(args: argparse.Namespace) -> int:
    """Run dialer performance and load tests."""
    from smartdialer.loadtest import run_load_test

    quick = getattr(args, "quick", False)
    default_out = "results_quick/loadtest_results_quick.md" if quick else "docs/loadtest_results.md"
    out_path = getattr(args, "out", None) or default_out
    run_load_test(quick=quick, out_path=out_path, print_markdown=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="smartdialer",
        description="SmartDialer collections-dialer prototype CLI",
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # init-db
    init_db_parser = subparsers.add_parser("init-db", help="Initialize the database")
    init_db_parser.add_argument(
        "--db",
        default="smartdialer.db",
        help="Path to SQLite database file (default: smartdialer.db)",
    )
    init_db_parser.set_defaults(func=cmd_init_db)

    # scenarios
    scenarios_parser = subparsers.add_parser("scenarios", help="Run operational failure scenarios")
    scenarios_parser.add_argument(
        "name",
        nargs="?",
        default="all",
        help="Scenario name: worker_crash, provider_outage, agent_drop, duplicate_events, out_of_order_events, all",
    )
    scenarios_parser.set_defaults(func=cmd_scenarios)

    # simulate
    simulate_parser = subparsers.add_parser("simulate", help="Run campaign simulation")
    simulate_parser.add_argument(
        "--scenarios",
        default="A,B,C,D",
        help="Comma-separated scenarios to run (default: A,B,C,D)",
    )
    simulate_parser.add_argument(
        "--agents",
        type=int,
        default=50,
        help="Number of simulated agents (default: 50)",
    )
    simulate_parser.add_argument(
        "--duration",
        type=int,
        default=1200,
        help="Duration in virtual seconds (default: 1200)",
    )
    simulate_parser.add_argument(
        "--seed",
        type=int,
        default=7,
        help="RNG seed (default: 7)",
    )
    simulate_parser.add_argument(
        "--out",
        default="docs",
        help="Output directory for generated results (default: docs)",
    )
    simulate_parser.set_defaults(func=cmd_simulate)

    # loadtest
    loadtest_parser = subparsers.add_parser("loadtest", help="Run dialer load tests")
    loadtest_parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick load test with smaller pools (under 30 seconds)",
    )
    loadtest_parser.add_argument(
        "--out",
        default=None,
        help="Output path for generated markdown report (default: docs/loadtest_results.md or results_quick/loadtest_results_quick.md if --quick)",
    )
    loadtest_parser.set_defaults(func=cmd_loadtest)

    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except AssertionError as exc:
        print(f"Self-check failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
