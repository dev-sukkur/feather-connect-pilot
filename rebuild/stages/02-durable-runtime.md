=== proposal.md ===
# Proposal

## Why

The model from stage 1 must survive crashes, restarts and several processes. This stage puts it on Postgres and a
durable engine, behind the two contracts stage 1 fixed, and proves that killing anything loses and duplicates
nothing.

## What Changes

- The Postgres store, passing the same contract tests as the in-memory one; one baseline migration.
- The spool, time partitions, retention.
- The durable engine adapter, workers, heartbeats, the sweeper, leader election.
- The scheduler: cron, intervals, runs for every tick, pollers, overlap, catch-up, pause.
- Shared rate budgets.
- The tenancy rules, enforced by tests.

Out of scope: the server's HTTP surface, packages, webhooks.

## Capabilities

### New Capabilities
- `tenancy`: the tenant on every record, and the rules that keep full multi-tenancy cheap to add
- `storage`: Postgres as the only database, the store contract, the spool and retention
- `durable-execution`: the engine port, tasks, retries, workers, the sweeper and engine conformance
- `scheduling`: triggers, runs, pollers, overlap, catch-up and pause
- `rate-budgets`: shared budgets with windows, reserves and deferral

### Modified Capabilities

None.

## Impact

Adds the engine adapter package, a worker process, a test Postgres for CI, and the chaos, engine conformance and
performance suites.

**Exit criteria**

- Every store contract test passes against Postgres and against memory.
- The engine conformance suite passes.
- The chaos suite passes: kill -9 mid-extract and mid-apply, a worker never restarted, a worker wrongly declared
  dead, a Postgres restart.
- Hard cases H6, H7 and H8 pass.
- Spool throughput is at least 20,000 rows per second per worker.

=== design.md ===
# Design

## Context

The pilot chose its engine by a spike (ADR-0003) and kept it behind a port (ADR-0002); both decisions carry over.
Its Stage 2 and Stage 3 reviews then found 38 defects, most of them in this layer. Each is now a scenario in these
specs, so the rebuild starts with the tests the pilot wrote after the fact.

## Goals / Non-Goals

**Goals:** durability proved by tests that kill real processes; one migration; tenancy rules that can't be broken
by accident.

**Non-Goals:** row-level security, quotas, per-tenant pools (backlog: full multi-tenancy).

## Decisions

- **One baseline migration**, with every table's `tenant_id` in its primary key and unique indexes, and a test
  that fails for a table that lacks it.
- **Contract tests use two tenants**, which the pilot's did not (ADR-0013 rule 6).
- **A tick and its run are written in one transaction.** The pilot stored the tick first and could lose it.
- **The seal and apply transactions write readiness marks** (how far each partition is complete, how far each route
  has applied it), though nothing reads them yet. The pilot designed readiness afterwards (ADR-0012) and would have
  had to reopen both transactions. See plan §5.2 R4; drop this if the sponsor declines.
- **Engine rules stay in the adapter:** a fixed application version, stable executor ids, flow code inside steps,
  0.5 s polling. From the pilot's design §6.12 and spike results.
- **Port first:** `feather-connect/tests/storage/`, `tests/runtime/test_scheduler.py`, `test_pollers.py`,
  `test_sweeper.py`, `test_budgets.py`, `test_retention.py`, `test_task_scans.py`, `test_slice_concurrency.py`,
  `feather-engine-dbos/tests/`, `tests/engine_conformance/`, `tests/chaos/test_kill_mid_apply.py`,
  `test_overlap_and_budgets.py`, `tests/perf/`.

## Risks / Trade-offs

- The chaos suite needs Docker and takes minutes. Run it nightly and on changes to this layer, and never merge
  while it is red or running.
- Lessons that are easy to lose, each a scenario here: pools need timeouts (F9); the sweeper must not mark a worker
  recovered when recovery failed (F5); a delivery whose task ended must not stay running (F7); sweeps must not
  scale with a backlog behind a failure (F11); budget waits must end (F12, S23).

=== tasks.md ===
# Tasks

## 1. Postgres store

- [ ] 1.1 Test Postgres in CI; run the stage 1 contract tests against it, all failing
- [ ] 1.2 The baseline migration; the tenant-in-keys test; the no-extensions test
- [ ] 1.3 The Postgres store, aggregate by aggregate, until the contract tests pass
- [ ] 1.4 Two-tenant contract tests

## 2. Spool and retention

- [ ] 2.1 Port the spool and retention tests; the throughput test
- [ ] 2.2 Chunks, time partitions made ahead, retention by dropping partitions

## 3. Engine

- [ ] 3.1 Port engine conformance; the adapter and its mapping tests
- [ ] 3.2 Workers: heartbeat, drain, stable ids
- [ ] 3.3 The sweeper and leader election

## 4. Scheduler and budgets

- [ ] 4.1 Port scheduler, poller and pause tests; add the crash-between-tick-and-run test
- [ ] 4.2 The scheduler, stream runs, overlap, catch-up
- [ ] 4.3 Port budget tests; budgets with windows, reserves, accounts and deferral

## 5. Exit

- [ ] 5.1 Chaos suite green
- [ ] 5.2 Engine conformance green
- [ ] 5.3 H6, H7, H8 green
- [ ] 5.4 Throughput at least 20,000 rows per second
- [ ] 5.5 `openspec validate 02-durable-runtime --strict` passes; archive the change
