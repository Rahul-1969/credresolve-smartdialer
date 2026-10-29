# SmartDialer

A collections-dialer prototype featuring progressive and predictive pacing with a deterministic Safety Controller positioned between pacing logic and telecom providers.

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

