# SmartDialer

## What it is

SmartDialer is a Python 3.11 collections dialer prototype with progressive and predictive pacing and a separate Safety Controller. It models campaign execution, pacing, safety decisions, call allocation, and provider routing. SQLite is the shared source of truth.

**Pipeline:** Campaign → Pacing Engine (Progressive | Predictive) → Safety Controller → Call Allocator (Dialer phases 1–3 in `dialer.py`) → Provider Router → Telecom Provider A/B

## Quick start

Requires Python 3.11 or later.

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest -q
```

### macOS / Linux

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

The test suite contains 346 tests and takes approximately 1–3 minutes.

## Run it

Initialize the database:

```bash
python -m smartdialer.cli init-db --db dialer.db
```

Run all failure scenarios:

```bash
python -m smartdialer.cli scenarios all
```

The failure scenarios cover S1 worker crash, S2 provider outage, S3 agent drop, S4 duplicate events, and S5 out-of-order events. To run S2:

```bash
python -m smartdialer.cli scenarios s2
```

Run the simulation:

A full run writes into docs/ and overwrites the committed results.

```bash
python -m smartdialer.cli simulate --scenarios A,B,C,D --agents 50 --duration 1200 --seed 7 --out docs
```

The full simulation takes several minutes. For a quick run:

```bash
python -m smartdialer.cli simulate --scenarios A,B,C,D --agents 20 --duration 300 --seed 7 --out results_quick
```

Run the quick load test:

```bash
python -m smartdialer.cli loadtest --quick
```

The full load test takes several minutes:

```bash
python -m smartdialer.cli loadtest
```

## Project layout

- `smartdialer/`: application package
  - Core modules: `agents.py`, `calls.py`, `repo.py`, `db.py`, `safety.py`, `metrics.py`, `snapshot.py`, `dialer.py`, `dispatch.py`, `reaper.py`, `actions.py`, `agent_ops.py`, `simworld.py`, `simulator.py`, `scenarios.py`, `loadtest.py`, `cli.py`, `config.py`, `models.py`
  - `pacing/`: `base.py`, `progressive.py`, `predictive.py`
  - `providers/`: `base.py`, `behavior.py`, `mock.py`, `provider_a.py`, `provider_b.py`, `breaker.py`, `router.py`
- `tests/`: test suites
- `docs/`: project documentation and results
- `scripts/explain_examples.py`: example explanation script

## Documentation

- `docs/architecture.md` — system architecture.
- `docs/architecture_decisions.md` — architecture decisions.
- `docs/agent_state_machine.md` — agent state machine.
- `docs/call_state_machine.md` — call state machine.
- `docs/simulation_results.md` — simulation results.
- `docs/simulation_results.csv` — simulation results in CSV format.
- `docs/simulation_timelines.csv` — simulation timelines.
- `docs/loadtest_results.md` — load-test results.
- `docs/scale_and_final_answer.md` — scaling considerations and final answer.

## Key guarantees

- Agent reservation, borrower claim, and allotment consumption use atomic conditional `UPDATE` operations.
- Call events are idempotent and rank-monotonic, with `UNIQUE(call_id, provider_event_id)`.
- Agent side effects are guarded by the owning call ID.
- The Safety Controller cannot be imported by the pacing package, as enforced by an AST test, and has no disable flag.
- Database allotments prevent double approval across workers.
- `CANCEL` and `TERMINATE` use a transactional outbox.
- The reaper reconciles stuck calls against provider ground truth.
- The system fails closed.

## Testing

The suites cover race and stress tests with threads, property tests, call-event chaos including duplicates, out-of-order events and crashes, safety math and decision tests, architecture enforcement, reaper and failure scenarios, the deterministic simulator, and the load-test harness.

## Known limitations

- single campaign
- SQLite single writer (see docs/scale_and_final_answer.md)
- the pacer asks for far more than safety approves
- predictive cancels some calls before they are answered
- agent loss while connected counts as abandoned and can trip the fallback
- calls table has no retention yet
- mock providers only (no real telecom integration)

## Note

AI-assisted development; the author can walk through any part.
