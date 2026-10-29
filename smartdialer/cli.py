"""Command-line interface for SmartDialer.

Single Responsibility:
    Provide CLI entrypoints and argument parsing for database initialization,
    campaign simulation, and load testing.
"""

import argparse
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
    init_db_parser.add_argument(
        "--db",
        default="smartdialer.db",
        help="Path to SQLite database file (default: smartdialer.db)",
    )
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
