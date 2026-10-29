"""Database connection and schema management.

Single Responsibility:
    Own SQLite database connection lifecycle and schema initialization.
    Enforce WAL mode, busy timeout, foreign key constraints, and autocommit (isolation_level=None)
    for explicit transaction control across threads and processes.

Rule:
    One connection per thread/worker. Never share a connection across threads.
"""

from pathlib import Path
import sqlite3


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Create and configure a thread-isolated SQLite connection.

    Configures:
        - isolation_level=None: Disables implicit transaction management in Python's sqlite3,
          allowing explicit BEGIN IMMEDIATE, COMMIT, and ROLLBACK control.
        - timeout=5.0: Driver-level timeout.
        - PRAGMA journal_mode=WAL: Enables Write-Ahead Logging for high concurrency.
        - PRAGMA synchronous=NORMAL: Safe synchronization in WAL mode with fast writes.
        - PRAGMA busy_timeout=5000: SQLite-internal busy handler waiting up to 5000ms on locks.
        - PRAGMA foreign_keys=ON: Enforce relational referential integrity.
        - row_factory=sqlite3.Row: Enable name-based column access on returned rows.
    """
    path_str = str(db_path)
    conn = sqlite3.connect(path_str, timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Initialize the database schema idempotently (CREATE TABLE IF NOT EXISTS)."""
    statements = [
        """
        CREATE TABLE IF NOT EXISTS agents (
            id TEXT PRIMARY KEY,
            state TEXT NOT NULL CHECK(state IN ('OFFLINE','AVAILABLE','RESERVED','DIALING','CONNECTED','WRAP_UP','PAUSED')),
            version INTEGER NOT NULL DEFAULT 0,
            reserved_by TEXT,
            reserved_call_id TEXT,
            lease_expires_at REAL,
            available_since REAL,
            updated_at REAL NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS agent_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT NOT NULL,
            worker_id TEXT,
            call_id TEXT,
            at REAL NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS borrowers (
            id TEXT PRIMARY KEY,
            phone TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('QUEUED','CLAIMED','IN_CALL','DONE','FAILED_PERMANENT')),
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL NOT NULL DEFAULT 0,
            claimed_by TEXT,
            lease_expires_at REAL,
            version INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS calls (
            id TEXT PRIMARY KEY,
            borrower_id TEXT NOT NULL REFERENCES borrowers(id),
            agent_id TEXT REFERENCES agents(id),
            provider TEXT,
            provider_call_id TEXT,
            state TEXT NOT NULL,
            state_rank INTEGER NOT NULL DEFAULT 0,
            version INTEGER NOT NULL DEFAULT 0,
            attempt INTEGER NOT NULL DEFAULT 1,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS call_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_id TEXT NOT NULL,
            provider_event_id TEXT,
            event_type TEXT NOT NULL,
            received_at REAL NOT NULL,
            applied INTEGER NOT NULL,
            UNIQUE(call_id, provider_event_id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_agents_state_available
        ON agents(state, available_since);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_borrowers_state_next_attempt
        ON borrowers(state, next_attempt_at);
        """,
    ]

    # Run inside an explicit immediate transaction
    conn.execute("BEGIN IMMEDIATE;")
    try:
        for stmt in statements:
            conn.execute(stmt)
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
