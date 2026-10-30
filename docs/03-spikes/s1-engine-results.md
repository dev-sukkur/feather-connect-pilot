# S1 — Engine spike results: DBOS vs Hatchet (30 Oct 2026)

Test spec: [s1-engine-spike.md](s1-engine-spike.md). Code and raw results: [`spikes/engine`](../../spikes/engine/README.md); facts are in `results/*.jsonl`. The Hatchet adapter, its deployments and the T14 probes are on the branch `side/hatchet-engine`.

## Setup

| Item | Value |
|---|---|
| Machine | Windows 11 laptop, 8 cores. Docker Desktop (WSL2) with 7.7 GB. |
| Database | Postgres 17.11 in Docker |
| DBOS | DBOS Transact **3.4.0**. Library only; state in schema `dbos`. |
| Hatchet | hatchet-sdk **1.44.0** + `hatchet-lite:latest`, pulled on 30 Oct 2026. Its database migrated to `v1_0_160`; the binary reports only `v0.1.0-alpha.0`, so pin it by image digest. State in its own database `hatchet`. |
| Workers | Python 3.13 in **Linux containers** (`python:3.13-slim`), killed with `docker kill` (SIGKILL). DBOS was also run as native Windows processes, with the same results. Hatchet's SDK can't run workers on native Windows. |
| Noise | Other projects' containers that Docker restarted were running, one at about half a CPU core. Timings are laptop numbers, not the reference deployment. |

## Results

Numbers are from container workers unless noted. ✅ pass · ⚠️ passes with a caveat · ❌ fails as specified · ⛔ not available.

| # | Test | DBOS 3.4 | Hatchet (lite, SDK 1.44) |
|---|---|---|---|
| T1 | Priority, one at a time per key | ✅ No overlap. Priority order exact (2,2,2,1,1,1,0,0,0). | ⚠️ No overlap. The first run per key is admitted in arrival order, then priority applies (0,2,2,2,1,1,1,0,0). |
| T2a | Crash; the same worker restarts | ✅ Effects ×1. Resumed **7.0 s** after the kill (restart 5.9 s). The completed chunk wasn't redone: native step checkpoints. | ✅ Effects ×1. Resumed **30.6 s** after the kill (heartbeat timeout). Chunk checkpoints via our table. |
| T2b | Crash; another worker must take over | ⚠️ **No automatic takeover.** Our sweeper, calling DBOS's *internal* `_recover_pending_workflows`, finished it 7.2 s later. | ✅ **Automatic.** Done 36.7 s after the kill. |
| T3 | Deploy mid-drain, new app version | ❌ **2 of 50 in-flight tasks stranded** (version gating). Queued work moved to v2 correctly. | ✅ 50/50, no duplicates |
| T3 | Deploy mid-drain, same version + our sweeper | ✅ 50/50, no duplicates | ✅ 50/50 |
| T3 | DBOS `auto_upgrade` | ⛔ **Requires a DBOS enterprise key** | n/a |
| T4 | Dedup (10 submissions, 5 concurrent) | ✅ 1 run | ✅ 1 run (native idempotency) |
| T5 | 20 s durable sleep across a crash | ✅ Finished at **20.03 s** | ✅ Finished at **34.4 s**: resumed only after the heartbeat timeout |
| T6 | Retry timing (policy 1, 2, 4 s); permanent error | ✅ Gaps 1.05, 2.04, 4.04 s. Permanent error: 1 attempt. | ⚠️ Gaps 3.8, 4.9, 10.2 s: native backoff is factor^n plus about 1–2 s of dispatch, **fixed per task definition**. Permanent error: 1 attempt. |
| T7 | Schema isolation and locks | ✅ Own schema; DDL 0.05 s; no locks on ours; no extensions | ✅ Own database; DDL 0.03 s; no locks |
| T8 | Two workers, 200 tasks | ✅ All exactly once; split 96/104 | ✅ All exactly once; split 160/40 |
| T9 | Shared vendor budget (our service) | ✅ ≤ 10 per window; low ≤ 4; high ≥ 6 in every saturated window; 0 failures | ✅ Identical |
| T10 | 50 tasks/s, one submit per task | ✅ At 0.2 s polling: p50 0.15 s, **p95 0.27 s**. ❌ At the default 1.0 s polling: p95 66 s, because a worker dispatches at most slots ÷ poll interval. | ⚠️ Client submit took about 40 ms, so only 25/s was reached. p50 0.06 s, **p95 5.9 s**. |
| T10b | 50 tasks/s in batches of 10 | ⚠️ A start-up backlog that then drained (p95 6.0 s, falling to 1.5 s) | ⚠️ Bulk submit 46 ms per batch. Latency **flat at about 10 s**: one worker's capacity is about the arrival rate. |
| T11a | Native site routing | ✅ Exact. The **agent needs Postgres credentials.** | ✅ Exact. The **agent needs engine gRPC access and a tenant token.** |
| T11b | Relay through our gateway (durable wait + signal) | ✅ Resumed 0.059 s after the signal | ✅ Resumed 0.051 s after the signal (after an adapter fix) |
| T12 | Idle Postgres load | About **46 tx/s per extra worker** at 0.2 s polling (1 → 52, 3 → 144, measured with no other engine on the server); about 7 connections per worker | About **37 tx/s, flat in the number of workers** (1 → 37.4, 3 → 39.4); about 30 connections held by the engine |
| T13 | Drain 1,000 queued tasks, 1 worker × 16 slots | **55 tasks/s** | ❌ **The engine wedged on 2 of 2 attempts** (see the stability finding). After a restart it drained the leftover backlog at **37 tasks/s**. |

## Findings that matter for the design

**DBOS**

1. **Version gating.** Unpinned queued work is only dequeued by workers on the latest registered application version.
   - Rolling back to an older version stops all dequeuing until `set_latest_application_version` is called.
   - In-flight work on a dead old-version worker is stranded.
   - The built-in fix, `auto_upgrade`, is enterprise-only.
   - **We must own a version policy.** For example, one application version per runtime major, with code changes guarded by `DBOS.patch`, plus deploy tooling that sets the latest version.
2. **No liveness for executors without Conductor.** A worker's in-flight work is recovered only when a worker with the same executor id restarts.
   - Stable executor ids (one per container replica) cover restarts.
   - Workers that are scaled away need our sweeper, which uses an **internal** API. It needs a contract test in our adapter so an upgrade fails loudly.
3. **Throughput is slots ÷ polling interval per worker.** Each idle worker costs about 46 tx/s at 0.2 s polling. Size and tune both. Our runtime submits one task per (route, version), not per event, so task rates stay well below event rates.
4. **Strengths:**
   - exact priority and per-call retries;
   - native step checkpoints;
   - fast recovery on restart (7 s);
   - no extra server;
   - runs natively on Windows for development.

**Hatchet**

1. **Automatic takeover and version-agnostic deploys.** It needs nothing from us, at the cost of a roughly 30 s detection delay.
2. **Worker SDK is Linux/macOS only.** Its signal handling and listener subprocess fail on Windows, so Windows developers need WSL2 or Docker.
3. **Defaults to override.** `execution_timeout` is 60 s and `schedule_timeout` is 5 min, so a backlog older than 5 minutes would fail tasks by default.
4. **Durable operations need async durable tasks.** Retries are fixed per task definition, so our per-call retry policy would need our own loop inside durable tasks.
5. **Costs:** one extra container with its own database and about 30 Postgres connections. Engine load stays flat as workers are added.
6. **Monitoring UI.** Free and complete, but secondary because the dashboard is ours.

**Hatchet stability finding (T13, reproduced twice).** After 1,000 runs were bulk-submitted with no worker attached, `hatchet-lite` went into a degraded state once a worker connected.

- **Symptoms:**
  - "context deadline exceeded" errors even on trivial queries ("could not list tenants");
  - lease and slot-replenish errors, plus a concurrency-strategy lock timeout;
  - Postgres statement timeouts.
- **Postgres was not the bottleneck:** it showed 0 active Hatchet queries, with 30 of Hatchet's connections idle and tables of 14 MB or less. That points to pool starvation or a lock inside the engine, not database load.
- **Effect on work:**
  - the first attempt processed 408 tasks before the worker's stream was dropped, and it never reconnected in 5 minutes;
  - the second attempt processed 0, and its 1,000 submissions were apparently lost.
- **Recovery:** a restart cleared it, and the backlog drained at 37 tasks/s. About 7 minutes later the engine degraded again, with only one idle worker attached.

The root cause was not established, and a search turned up no matching issue. The sponsor asked for a re-test on a full deployment before deciding; see the next section.

## Re-test on full Hatchet (sponsor request, 30 Oct afternoon)

**Deployment.** Hatchet's reference Docker Compose stack, `spikes/engine/compose.hatchet-full.yaml` on the branch `side/hatchet-engine`:
- its own Postgres 15.6;
- RabbitMQ;
- migration and setup jobs;
- `hatchet-engine`;
- `hatchet-dashboard`.

All are `latest` images as of 30 Oct 2026. Workers are Linux containers that reach the engine through Docker's host gateway.

**Stall investigation.** T14 probes were added: (a) six abrupt worker kills with no load; (b) three 1,000-task bursts with a live worker and no kills.

| Stack | Run | Engine errors | Outcome |
|---|---|---|---|
| Full, fresh, features on | T13 attempts 1, 2, 3 | 0, 0, **761** | 26.6 and 29.9 tasks/s, then the **3rd attempt stalled**: 0 processed, same signature as lite |
| Same stack, engine restarted | T14a: six kills | **575** in about 4 min | Degraded for about 4 min, then self-recovered; the smoke test passed |
| Same stack, engine restarted | T14b: three bursts, live worker | **313** in the first 2 min | All 3 bursts completed, at 12.5–14 tasks/s including submission |
| **Fresh stack**, idempotency and per-key concurrency off | T14a + T13 ×2 | 0 | 37.2 and 39.6 tasks/s; kills harmless |
| **Fresh stack**, idempotency only | T14a + T13 ×2 | 0 | 38.9 and 40.2 tasks/s |
| **Fresh stack**, per-key concurrency only | T14a + T13 ×2 | 0 | 40.6 and 40.2 tasks/s |
| **Fresh stack**, both on (control) | T14a + T13 ×3 | 0 | 43.2, 46.6 and 48.6 tasks/s |

**Interpretation.**
- The stall is **not** specific to `hatchet-lite`, and our features are **not** a simple trigger.
- It appeared only on stacks that had already run a lot: `hatchet-lite` after the whole suite, and the first full stack after two bursts.
- Once it appeared, it **came back after engine restarts**. Only a fresh database cleared it.
- **4 fresh-stack runs (9 bursts, 24 kills) never failed.**

It's an intermittent failure mode with an unknown trigger and state that persists across restarts. Production can't fix that by rebuilding from an empty database. A fresh stack performs well: a 43–49 tasks/s drain, close to DBOS's 55.

**Timing re-test on a fresh full stack** (features on, 0 engine errors). Results match `hatchet-lite`:

| Test | Result |
|---|---|
| T1 | First-admitted-FIFO quirk remains |
| T2a | Resumed **30.0 s** after the kill |
| T2b | Automatic takeover in **35.9 s** |
| T5 | 20 s sleep finished at **34.7 s** |
| T8 | All 200 tasks ran on one worker: correct, but unbalanced |
| T10 | One-at-a-time submission: p50 0.06 s, **p95 10.7 s** (at about 21/s, client-limited) |
| T10b | Batched at 48/s: p50 5.5 s, **p95 11.1 s** |
| T11a, T11b | Pass (relay resumed in 0.041 s) |
| T12 | About **32 tx/s, flat** for 1 or 3 workers (Hatchet's own Postgres) |

**Before choosing Hatchet**, two things would be needed:
- a vendor issue with these logs;
- a multi-day soak test on a production-like stack.

**Both:** the gateway relay (T11b) works on either engine. It is the safer on-prem model, because sites get neither database nor engine credentials (design D2).

## Scoring (spec weights)

Re-scored after the full-deployment re-test.

| Criterion (weight) | DBOS | Hatchet lite | Hatchet full | Why |
|---|---|---|---|---|
| Ops footprint (20) | 17 | 13 | 10 | **DBOS:** adds no process.<br>**Hatchet lite:** one container, token setup, its own database.<br>**Hatchet full:** four long-running services and two setup jobs, including RabbitMQ (optional) and its own Postgres. |
| Fit with the port (20) | 19 | 14 | 14 | **DBOS:** matches priority, retries, step checkpoints and sync bodies.<br>**Hatchet:** static retries, async-only durable work, first-admitted-FIFO. |
| Remote model and security (15) | 10 | 12 | 12 | Native agents need DB credentials (DBOS) or engine credentials (Hatchet). Both work with our gateway relay. |
| Maturity and release stability (15) | 9 | 7 | 7 | **DBOS:** enterprise-gated features, internal recovery API, frequent majors.<br>**Hatchet:** an **intermittent stall that survives restarts** (not seen on fresh stacks), opaque engine version, no Windows workers. |
| Measured latency and throughput (10) | 8 | 5 | 5 | **DBOS:** p95 0.27 s at 50/s, 55 tasks/s per worker, 7 s recovery.<br>**Hatchet:** 37–49 tasks/s per worker on fresh stacks, but p95 tails of 10–11 s and 30–36 s takeover. |
| Debugging tools (10) | 4 | 9 | 9 | Hatchet's UI is free. DBOS's Conductor is paid. |
| Licence and governance (5) | 3 | 5 | 5 | Both MIT, but DBOS gates operational features behind paid tiers |
| Workarounds needed (5) | 2 | 3 | 3 | **DBOS:** sweeper, liveness, version policy.<br>**Hatchet:** Linux workers, timeouts, async bridge, retry loop. |
| **Total (100)** | **72** | **68** | **65** | |

## Recommendation

**DBOS passes the gate.**

**Hatchet passes the functional tests,** and a fresh full deployment drains at close to DBOS's rate. But it showed an intermittent stall that we couldn't explain, and that persisted across engine restarts. Its dispatch tails are about 10 s and its failover takes about 30 s.

The deciding questions are which set of gaps we would rather own, and how much an engine we can't debug ourselves should worry us.

**Recommended: DBOS Transact, with three conditions:**
1. **A version policy:** a fixed application version per runtime major, `DBOS.patch` for in-flight compatibility, and `feather deploy` setting the latest version.
2. **Liveness and recovery in our sweeper:** worker heartbeats in our run journal, plus dead-executor recovery behind an adapter contract test.
3. **Polling sized to load:** start at 0.2–0.5 s, add workers, and track the idle cost.

**Why:**
- It keeps ADR-0005 literally true: Postgres is the only moving part.
- It matches the port's semantics exactly.
- It recovers fastest when a worker restarts.
- It works on every developer machine.

Its gaps fall in the control plane we already planned to build, which has a leader-elected sweeper. Hatchet's gaps instead sit in the engine and SDK, which we don't control. The re-test showed one of them can stall all work and persist across restarts. The production-recommended full stack also adds four long-running services and two setup jobs against ADR-0005's "Postgres only".

**What would flip this to Hatchet:**
- many workers (where per-worker polling load starts to matter, around 20+);
- a need for engine-level automatic takeover without writing our own;
- a team that values Hatchet's free UI for engine debugging over the extra container.

**Decision owner:** the sponsor, at the end-of-day review (ADR-0003).
