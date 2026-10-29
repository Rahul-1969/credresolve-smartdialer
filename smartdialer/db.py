"""Database connection and transaction management.

Single Responsibility:
    Own SQLite database connection lifecycle, enforcing WAL journal mode,
    busy timeout configuration, and foreign key constraints for safe
    multi-process and multi-worker concurrent access.
"""

# TODO(task-N): Implement connection factory, WAL pragma setup, and transaction context managers.

