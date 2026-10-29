# SmartDialer

A collections-dialer prototype featuring progressive and predictive pacing with a deterministic Safety Controller positioned between pacing logic and telecom providers.

## Concurrency Model

The SQLite database (in WAL mode) serves as the single source of truth across all dialer workers.
Agent reservation and borrower claiming rely on a single atomic conditional `UPDATE ... RETURNING` statement guarded by state checks, preventing race conditions at the database engine level.
Each worker thread operates on its own dedicated database connection, never sharing connections across threads.
All write transactions use `BEGIN IMMEDIATE` up-front to acquire SQLite's reserved lock, eliminating deadlock from lock upgrades.
In-memory caches are explicitly avoided because local caches can become stale during concurrent worker execution, leading to double-allocation and compliance breaches.

## Setup

Run the following commands in Windows PowerShell:

```powershell
# 1. Create a Python 3.11 virtual environment
py -3.11 -m venv .venv

# 2. Activate the virtual environment
.\.venv\Scripts\Activate.ps1

# 3. Install the package in editable mode with development dependencies
pip install -e ".[dev]"

# 4. Run tests
pytest -q

# 5. Run the CLI
smartdialer --help
```
