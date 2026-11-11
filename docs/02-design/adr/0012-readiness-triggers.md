# ADR-0012: Flows can start when the data they need is complete

- **Status:** Proposed. The sponsor answered its three questions on 2026-11-08: flow packages first, `on_late="rerun"` by default, and a skipped version counts as delivered
- **Date:** 2026-11-08
- **Deciders:** OSS Dev1 (sponsor)

## Context

In M1 a flow starts on a clock, by hand, or when webhook events arrive. A flow that depends on another flow's output, such as Postgres → SAP after Zakya → Postgres, runs on its own clock. It can read a half-written run, run on stale data when the upstream flow failed, and can't wait for several datasets of one store-day. Design §4.6 and requirements FR-B7 and FR-D3 planned readiness for M2. The sponsor brought it forward to M1.1, with flow packages.

## Decision

- **Readiness marks are written where the facts are:** the seal transaction records how far each dataset partition is complete, and applying a delivery records how far each route has applied, in order.
- **A dataset is complete through T** by rules per strategy: a complete version from a run started after the period ended; a `by_stamp` run's start minus a `settle` window (its lookback by default); for webhooks, every event sealed and `settle` passed, or the partition closed by the source or an operator.
- **`fc.ready(datasets, delivered_to=…, by=…)`** names what a flow waits for. **`fc.on_ready(ready, each="run" | "hour" | "day", at_most_every=…, on_late=…)`** is a trigger like `cron`.
- **A leader-side evaluator** fires each (flow, set, period, scope) exactly once, through a stored firing row submitted in one transaction with its run.
- **Late data** for a fired period reruns it by default (`on_late="rerun"`), or warns, or is ignored, per flow.
- **Readiness never blocks** routes, the upstream flow, or any flow that doesn't ask for it (H13).

Details: [the readiness design](../2026-11-08-readiness.md).

## Consequences

**Benefits:**

- Chains such as ingest → staging → SAP run in order, with no guessed gaps between schedules.
- Store-day work waits for all its inputs, store by store.
- Operators see what a flow is waiting for, and late data is visible instead of silently missed.

**Costs:**

- Three tables, a loop on the leader, and a row update in each seal and apply transaction.
- `settle` is a judgement per source. Set too short, a period fires early; `on_late` and the Readiness page catch and show it.
- Gating a clock run until data is ready, and readiness across environments, are left for later.

## Alternatives considered

- **Keep two clocks with a safety gap (M1).** Simple, but it guesses, and it breaks when upstream runs late or fails.
- **Make SAP and the warehouse routes of the ingest flow.** Correct ordering for free, but every destination then runs on the ingest's cadence and can't wait for several datasets.
- **A "run after flow X succeeds" trigger.** Easy, but a succeeded run can still have a failed delivery to staging, and it can't express "all three datasets of a store-day".
- **An external orchestrator (Airflow, Dagster) calling the API.** More to run and operate, and it can't see versions, partitions or delivery state.
