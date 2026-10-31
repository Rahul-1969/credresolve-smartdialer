# Architecture decisions

Each entry: what I chose, why, and what it makes harder.

## 1. SQLite (WAL) as the single source of truth; no Kafka, Redis, or microservices
**Chosen:** one SQLite file in WAL mode, one connection per worker, `BEGIN IMMEDIATE` for every multi-statement write, no cache.
**Why:** every hard problem here (never reserve an agent twice, never approve the same capacity twice, apply provider events idempotently, recover from crashes) is about atomic conditional updates across a few tables. A single writer gives serializability for free, and with no cache the question "the DB says AVAILABLE but the cache says RESERVED" cannot arise: there is nothing to disagree with the database.
**Costs:** one writer caps write throughput; long write transactions (the safety evaluation) block everyone; no multi-node deployment. See docs/loadtest_results.md. At 10,000+ agents the same SQL shapes move to Postgres (`SELECT ... FOR UPDATE SKIP LOCKED`), with allotments and agent claims unchanged.

## 2. One conditional UPDATE for every claim
**Chosen:** agent reservation, borrower claim and allotment consumption are each a single `UPDATE ... WHERE state = <expected>` (with RETURNING), so the guard is part of the write.
**Why:** "check then write" is the classic race. The conditional update stays correct even on a multi-writer database.
**Costs:** the planner can pick a bad plan for `id IN (subquery) AND state = ...` (I found one in the load test). Plans must be checked with EXPLAIN.

## 3. Rank-based call state machine
**Chosen:** each call state has a rank; an event applies only if the call is not terminal and the rank is strictly higher; duplicates (same rank), stale events (lower rank) and events after a terminal state are recorded and ignored. Dedupe via UNIQUE(call_id, provider_event_id). Event row, call update and agent/borrower side effects are one transaction.
**Why:** providers repeat and reorder events. A rank rule makes ANSWERED,ANSWERED,ANSWERED,COMPLETED and COMPLETED,ANSWERED,RINGING both end in a sensible state without enumerating every legal pair.
**Costs:** a late event can be silently ignored, so ignored events are kept with applied=0 as an audit trail.

## 4. Ownership-guarded agent side effects
**Chosen:** an agent stays bound to its call (`reserved_call_id`) until it returns to AVAILABLE/PAUSED/OFFLINE, and every call side effect is conditional on that id.
**Why:** otherwise a late COMPLETED for an old call can release an agent already serving a new call (I found and tested exactly this).
**Costs:** more conditions on every agent update.

## 5. The Safety Controller is separate and cannot be bypassed
**Chosen:** the pacing engine is a pure function returning a PacingRequest. Only the Safety Controller can issue an allotment. The pacing package may not import it (enforced by an AST test); SafetyConfig is frozen, absolute limits are module constants, and there is no enable/disable flag. It fails closed.
**Why:** correctness before cleverness: a clever predictor must not be able to switch safety off.
**Costs:** the pacer cannot see what safety will allow, so it asks for far more than is approved (tens of calls against one or two).

## 6. Statistical bound plus an abandon-rate breaker
**Chosen:** the answer rate is bounded above with a Wilson upper bound over a short and a long window; the number of calls in flight is capped so that the probability that answers exceed free agents stays at or below 2% per decision; a hard overdial cap applies; and an abandon-rate breaker (3% over 10 minutes, 30-answer minimum) forces progressive mode for a cooldown stored in the DB.
**Why:** the upper bound errs on the side of fewer calls, and a sudden rise in answer rate (the dangerous direction) is caught by the short window.
**Costs:** the 2% is per decision, not a rate; repeated exposure produces a small sustained abandon rate (2.2% in scenario C), which is why the breaker exists. Agent loss mid-call is counted as abandoned (conservative).

## 7. Allotments as DB leases
**Chosen:** an approval is a short-lived row (remaining units, expiry). The next decision counts unconsumed units as already in flight; the allocator consumes one unit per call created.
**Why:** two workers evaluating at once would otherwise both approve the same capacity. The evaluation is one BEGIN IMMEDIATE transaction, so decisions are serialized.
**Costs:** the evaluation (snapshot reads plus the binomial search) runs inside the write lock.

## 8. Stateless ticks, three-phase dispatch, transactional outbox
**Chosen:** any worker can run any tick. Phase 1 (transaction): consume allotment, claim borrower, reserve agent if progressive, create call. Phase 2 (no transaction): call the provider. Phase 3 (transaction): record the outcome. Provider side effects that must happen later (CANCEL, TERMINATE) are rows in `call_actions`, written in the same transaction as the decision and executed by any worker.
**Why:** never hold the write lock across network I/O; a crash between phases leaves a RESERVED call the reaper can reconcile against the provider's ground truth.
**Costs:** more states and a reaper.

## 9. Provider idempotency and no failover on timeout
**Chosen:** `call_id` is the idempotency key. Failover happens only on a definite error. A timeout is ambiguous (the call may exist), so the router retries the same provider with the same `call_id` and otherwise returns AMBIGUOUS.
**Why:** failing over on a timeout can dial the borrower twice.
**Costs:** an ambiguous call occupies an agent until the reaper resolves it.

## 10. Bind on answer for predictive calls
**Chosen:** predictive calls start with no agent. When ANSWERED is applied, the same transaction reserves the longest-idle available agent; if there is none the call is marked abandoned and a TERMINATE action is queued; the borrower is requeued without an attempt penalty.
**Why:** it makes the abandoned-call event explicit, measurable and recoverable.

## 11. Virtual-time simulation with mock providers
**Chosen:** the whole stack runs in virtual time against two mock providers (reliable; slow with duplicates, reordering and timeouts) with fixed seeds.
**Why:** deterministic, fast failure and load scenarios without telecom costs.
**Costs:** behaviour of real providers will differ; the mocks encode my assumptions.

## 12. Per-worker circuit breaker
**Chosen:** an in-memory sliding-window breaker per provider per worker.
**Why:** it only protects performance. Compliance limits are computed from the DB, so a worker with stale breaker state cannot violate them.
**Costs:** each worker learns about an outage on its own.

## What I am least confident about
- The pacer is far more optimistic than what safety credits, and heavy cancellation churn results from that gap.
- The safety evaluation and the snapshot run inside the write lock, so they limit scale first.
- Dropped events pin in-flight capacity until the reaper's stuck-call timeout.
- Single campaign only; the calls table needs retention.

## What I would do with another week
- Credit expected finishers with a lower confidence bound (more utilization, same guarantee).
- Make the pacer use the capacity safety will credit.
- Move to Postgres with SKIP LOCKED and compute the statistical bound outside the lock.
- Separate abandonment caused by pacing from agent loss in the statistics.
- Retention for calls, and multi-campaign support.