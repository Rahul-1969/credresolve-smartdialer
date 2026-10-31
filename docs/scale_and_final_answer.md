# Scale analysis and the final question

## How would you get the utilization benefit of predictive dialing with the safety of progressive dialing?

1. **Keep the two jobs apart.** The pacing engine is a pure function that can only ask ("start N calls"). The Safety Controller alone issues allotments, and nothing in the pacing package can import or disable it (AST test, frozen config, absolute limits as constants, no flag).
2. **Make safety a bound, not a heuristic.** The answer rate is bounded above (Wilson upper bound, short and long window, so a sudden rise is caught). The number of unanswered calls in flight is capped so that the probability that answers exceed free agents stays at or below 2% per decision. Only available agents and half of wrap-up agents are credited. A hard overdial cap applies.
3. **Make the limit atomic across workers.** Approvals are short-lived allotments in the database that the next decision counts as already in flight, and the evaluation runs in one write transaction, so two workers cannot approve the same capacity.
4. **Make failure explicit and recoverable.** A predictive call has no agent until it is answered; at answer an agent is bound in the same transaction, or the call is marked abandoned and a TERMINATE action is queued. An abandon-rate breaker (3% over 10 minutes, 30-answer minimum) forces progressive mode for a cooldown stored in the DB. Small agent pools, unhealthy providers and any internal error fall back to progressive or reject (fail closed).
5. **Measure it** (same seed, 50 agents, 1,200 s, `docs/simulation_results.md`):

| Scenario | Progressive util | Predictive util | Gain | Predictive abandon | Cancelled / created |
|---|---|---|---|---|---|
| A clean (20% answer, 120 s talk) | 43.2% | 51.7% | +8.5 | 0.00% | 10.5% |
| B clean (50%, 90 s) | 62.7% | 66.7% | +4.1 | 0.00% | 13.2% |
| C clean (70%, 180 s) | 83.6% | 85.3% | +1.7 | 2.18% | 0.8% |
| D clean (changing) | 60.0% | 65.2% | +5.2 | 0.24% | 2.0% |
| A with provider chaos | 46.2% | 54.5% | +8.3 | 0.74% | 39.4% |
| B with provider chaos | 61.0% | 65.4% | +4.5 | 0.21% | 10.9% |
| C with provider chaos | 81.2% | 84.4% | +3.3 | 0.31% | 0.0% |
| D with provider chaos | 56.6% | 60.8% | +4.2 | 0.96% | 2.6% |

6. **Honest costs.** The gain is small when the answer rate is high (C: +1.7 points, with the breaker in fallback for about half the run, 579 ticks). Predictive cancels calls before they are answered (up to 39% of created calls in the worst run), which is wasted dialing. The pacer asks for tens of calls where safety approves one or two, so safety does most of the work.
7. **Next:** credit agents expected to finish with a lower confidence bound (more utilization, same guarantee), let the pacer use the capacity safety will credit, and separate agent-loss abandonment from pacing abandonment in the statistics (today a mass logoff trips the breaker).

## What breaks first at 100 → 1,000 → 10,000 agents

**The single writer, and the first thing holding it too long is the safety evaluation.** Figures are from `docs/loadtest_results.md` (16 logical cores, Windows) and are orders of magnitude; one run was noisy.

- A dispatch transaction (claim borrower, reserve agent, create call) is about 1–2 ms, and COMMIT is about 60% of that.
- One safety decision holds the write lock for about 26 ms at (1,000 agents, 10,000 calls), and 190–440 ms at (10,000 agents, 100,000 calls) in two runs. About 120–350 ms of that is the windowed snapshot queries, which cost in proportion to the calls in the window, and the binomial search at 10,000 capacity costs tens of ms up to about 125 ms.
- Back of envelope at 10,000 agents (50% answer, 90 s talk, 15 s wrap-up): about 74 conversations/s, about 150 placements/s, about 300–450 events/s. Writer time per second is about 0.25 s for dispatch, 0.14–0.8 s for events and 0.19–0.44 s for one evaluation per second, so roughly 0.6–1.5 s of work per wall-clock second. That saturates. At 1,000 agents the same arithmetic is roughly 10%.
- Adding workers does not help: they share the writer. Measured dispatch throughput fell as threads were added, because SQLite's busy handler polls instead of queueing.

**Fixes, in order of payoff**
1. **Shrink what runs under the lock.** Keep per-minute rollup counters (answered, no-answer, abandoned) updated inside `apply_event`, so the snapshot costs O(buckets) instead of O(calls in window). Compute the binomial search outside the lock (cache by capacity and a rounded-up p) and re-check only the final bound inside. Run one decider per campaign instead of every worker.
2. **Amortize commits.** Batch many dispatch units and many events per transaction. One writer thread per database is fastest.
3. **Go beyond one writer.** Shard by campaign or agent pool into separate databases, or move to Postgres. The conditional UPDATE shapes stay; claims use `SELECT … FOR UPDATE SKIP LOCKED`, and row-level locks stop writers serializing.
4. **Make recovery cheaper.** The reaper asks the provider about every live call; add a next-check time. Dropped events pin in-flight capacity until the stuck-call timeout (scenario S2: about 17 pinned calls at the bound while 17 agents sat idle), so use a short timeout for ringing calls.
5. **Data growth.** Archive terminal calls; the partial and composite indexes already keep active-call queries small.

**Already found and fixed by measurement:** the claim statements were O(pool size) because SQLite planned the outer UPDATE as a state-index scan. They are now primary-key lookups (about 5× in an isolated 10,000-agent benchmark), and `BEGIN IMMEDIATE` is retried to stop starvation errors.
