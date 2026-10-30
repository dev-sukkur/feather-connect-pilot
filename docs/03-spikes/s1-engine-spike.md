# S1 — Engine spike: DBOS vs Hatchet

**Question:** which durable engine should sit behind the `Engine` port (design §6.8, ADR-0003)?

**Method:**
- One engine-agnostic test suite drives a thin spike adapter for each engine.
- Every test records task executions in Postgres tables (`spike.exec_log`, `spike.effects`) and asserts on them, so both engines are judged the same way.
- Spike code: [`spikes/engine`](../../spikes/engine/README.md) (throwaway).

## Test specification

| # | Test | Procedure | Pass criteria |
|---|---|---|---|
| T1 | Priority and one-at-a-time per concurrency key | Queue 20 tasks over keys A and B with mixed priorities while the worker is paused, then start it | Never two tasks running at once on the same key. Within a key, higher priority starts first. |
| T2 | Crash mid-task | Task: three chunks, each writing an effect row keyed `(task, chunk)` and a checkpoint. Force-kill the worker during chunk 2, then restart it. | The task completes. Every effect row exists exactly once. Completed chunks are not redone, or are redone with no duplicate effects. |
| T3 | New code deployed mid-drain | Queue 50 one-second tasks. After about 10 complete, stop worker v1 and start worker v2 (a different code-version marker). | All 50 complete exactly once. Nothing is stranded. The code version that ran each task is recorded. |
| T4 | Dedup | Submit the same idempotency key 5 times concurrently, then 5 times sequentially | Exactly one execution |
| T5 | Durable sleep across a restart | Task sleeps 20 s. Force-kill the worker 5 s in, then restart it. | The task completes. Total elapsed is at least 20 s and well below 20 s plus the restart time plus another full sleep. |
| T6 | Retry timing | The task fails with a retryable error on attempts 1–3 and succeeds on attempt 4. A permanent error is not retried. | 4 attempts, with gaps following the policy within ±50%. The permanent error makes exactly 1 attempt. |
| T7 | Schema isolation and locks | While a long task runs, run DDL on our own `feather` schema (create table, alter table) | The DDL is not blocked (finishes in < 2 s). The engine's objects live in their own schema or database. The engine holds no locks on our tables. |
| T8 | Two workers share the work | Two worker processes; 200 short tasks | Each task runs exactly once. Both workers execute some of the tasks. |
| T9 | Shared vendor budget | Two task types (reader `high`, writer `low`) share a budget of 10 units per 2 s with a 60% reserve for `high`. Run 30 s of each. | The budget is never exceeded in any window. `high` gets at least 60% of the units when contended. Waits are durable sleeps, not failures. |
| T10 | Dispatch latency | Submit 50 tasks/s for 30 s through an HTTP ingress | p95 time from submit to start ≤ 1 s; nothing lost |
| T11 | Remote execution | A task labelled `site=hq` must run on a separate "agent" process | It runs only on the agent process. Record what the agent needs: DB access, engine access, or only HTTPS to our gateway. |

## Scoring (after the gate)

The gate is that all applicable tests pass, or fail only for reasons the design can absorb cheaply. Engines that pass the gate are then weighed:

| Criterion | Weight |
|---|---|
| Ops footprint (processes, containers, setup steps) | 20 |
| Fit with the port (concurrency keys, priorities, durable waits, labels, dedup) | 20 |
| Remote-execution model and security (D2) | 15 |
| Maturity and release stability | 15 |
| Measured latency and throughput | 10 |
| Debugging tools (secondary; the dashboard is ours) | 10 |
| Licence and governance | 5 |
| Workarounds the adapter needed (fewer is better) | 5 |

## Results

Filled in as the tests run: see [s1-engine-results.md](s1-engine-results.md).
