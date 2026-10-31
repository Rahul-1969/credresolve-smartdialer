# SmartDialer Architecture

## One dialer tick (any worker; workers are stateless, all shared state is in SQLite)

```mermaid
flowchart TD
    W["Dialer worker<br/>stateless between ticks"]
    DB[("Shared SQLite database<br/>WAL mode")]
    P["Pacing Engine<br/>Progressive or Predictive<br/>pure function, no DB or provider access"]
    S["Safety Controller"]
    A["Call Allocator<br/>(Dialer phases 1-3)"]
    R["Provider Router<br/>per-provider circuit breaker"]
    PA["Telecom Provider A<br/>fast, reliable"]
    PB["Telecom Provider B<br/>timeouts, duplicates, reordering"]
    EP["Event pump"]
    O["Outbox processor"]
    RE["Reaper"]
    H["Agent-went-offline handler"]

    W -->|1. read snapshot| DB
    W -->|2. snapshot| P
    P -->|PacingRequest: start N calls| S
    S <-->|3. re-read snapshot, write allotment and decision log<br/>one BEGIN IMMEDIATE| DB
    S -->|verdict and allotment| A
    A -->|phase 1: consume unit, claim borrower,<br/>reserve agent if progressive, create call| DB
    A -->|phase 2: place_call, no transaction open| R
    R -->|place_call| PA
    R -->|place_call| PB
    A -->|phase 3: mark initiated, fail, or leave RESERVED if ambiguous| DB
    PA -->|poll_events| EP
    PB -->|poll_events| EP
    EP -->|apply_event: dedupe, rank update,<br/>owner-guarded side effects, bind on answer or abandon| DB
    DB -->|pending call_actions| O
    O -->|cancel_call, terminate_call| PA
    O -->|cancel_call, terminate_call| PB
    RE -->|get_call_status, idempotent place_call| PA
    RE -->|get_call_status, idempotent place_call| PB
    RE -->|reconcile stuck calls, release expired leases| DB
    H -->|mark agent offline, enqueue CANCEL or TERMINATE| DB
```

The diagram deliberately has no database-to-provider edge. Providers are reached only by the router (placement), the outbox processor (cancel/terminate) and the reaper (ground-truth queries), and they reach the system only through the event pump.

## Tables

`agents`, `agent_events`, `borrowers`, `calls`, `call_events`, `call_actions` (the outbox), `safety_state`, `safety_allotments`, `safety_decisions`.

## Components

| Component | Responsibility |
|---|---|
| Pacing Engine | Pure function of a snapshot; returns a request for N calls, never an order. |
| Safety Controller | One write transaction: re-reads state, checks providers, abandon-rate breaker, statistical risk bound, caps; writes an allotment and a decision log row; fails closed. |
| Call Allocator | Phase 1 (transaction): consume allotment unit, claim borrower, reserve agent if progressive, create call. Phase 2 (no transaction): provider. Phase 3 (transaction): record outcome. |
| Provider Router | Per-provider circuit breaker; fails over only on a definite error; never on a timeout (retries the same provider with the same call_id, otherwise AMBIGUOUS). |
| Event pump | Applies provider events through `apply_event`: dedupe, rank-monotonic update, side effects guarded by the owning call, bind-on-answer or abandon. |
| Outbox processor | Executes CANCEL and TERMINATE against providers, outside transactions, with leases and retries. |
| Reaper | Reconciles RESERVED and stuck calls against provider ground truth; releases expired claims and reservations. |
| Agent-went-offline handler | Abandons and terminates a connected call; cancels a call that is still in setup. |

See `architecture_decisions.md` for why, and `scale_and_final_answer.md` for what breaks first.