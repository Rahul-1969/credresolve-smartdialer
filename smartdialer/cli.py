"""Command-line interface for SmartDialer.

Single Responsibility:
    Provide CLI entrypoints and argument parsing for database initialization,
    campaign simulation, and load testing.
"""

import argparse
import sys


def cmd_init_db(args: argparse.Namespace) -> int:
    """Initialize the SQLite database schema."""
    print("not implemented yet")
    return 0


def cmd_simulate(args: argparse.Namespace) -> int:
    """Run dialer simulation across test scenarios."""
    print("not implemented yet")
    return 0


def cmd_loadtest(args: argparse.Namespace) -> int:
    """Run dialer load tests."""
    print("not implemented yet")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="smartdialer",
        description="SmartDialer collections-dialer prototype CLI",
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    init_db_parser = subparsers.add_parser("init-db", help="Initialize the database")
    init_db_parser.set_defaults(func=cmd_init_db)

    simulate_parser = subparsers.add_parser("simulate", help="Run campaign simulation")
    simulate_parser.set_defaults(func=cmd_simulate)

    loadtest_parser = subparsers.add_parser("loadtest", help="Run dialer load tests")
    loadtest_parser.set_defaults(func=cmd_loadtest)

    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

