# Phase 4 — Implementation plan for milestone M1

> Status: **draft, awaiting review.** Implements [design v0.2](../02-design/2026-10-30-feather-connect-design.md) §19, milestone M1, and the M1 rows of the [discovery requirements](../01-discovery/2026-10-30-feather-connect-discovery.md) §7.

M1 is too large for one plan, so it is split into six. Each one ends with software that runs and is tested on its own, and each is written for an engineer who hasn't seen the design.

| Plan | Delivers | What you can do when it's finished | Needs | Tasks |
|---|---|---|---|---|
| [1. Core loop](m1-01-core-loop.md) | SDK, data model, runtime and test harness, in-process | Write a flow and run the design's §5.7 test, with no database | — | 14 |
| [2. Postgres and DBOS](m1-02-postgres-dbos.md) | Postgres store and spool, `DbosEngine`, worker, sweeper | Run a flow durably and kill -9 a worker with nothing lost or duplicated | 1 | 10 |
| [3. Triggers and ingress](m1-03-triggers-ingress.md) | Scheduler, webhook ingress, sealer, pause and resume | A cron flow runs by itself, and a webhook reaches a destination in seconds | 2 | 7 |
| [4. Connectors](m1-04-connectors.md) | Conformance kit, Postgres, S3, REST, GraphQL, webhook, HTTP, DuckDB, five templates | Run real integrations from templates | 1 (and 3 for webhook) | 9 |
| [5. Control plane](m1-05-control-plane.md) | Auth, service layer, JSON API, HTML dashboard | Monitor and act on flows from the dashboard | 2, 3 | 11 |
| [6. CLI and M1 exit](m1-06-cli-exit.md) | `feather` CLI, `feather dev`, Compose, demo, hard-case suite | Run the whole demo in under 5 minutes, and pass the M1 exit criteria | 2–5 | 9 |

**Order.** See [Build order](#build-order-changed-31-oct-2026): the plans below are no longer built in sequence.

**Later plans depend on earlier code.** The interfaces between plans are fixed here, but details may shift during the build. Re-read a plan's **Consumes** lines against the real code before you start it.

**Code guide.** [code-guide.html](code-guide.html) explains each code folder and file, and the remaining work.

**Operating guide.** [operating-guide/index.html](operating-guide/index.html) shows how to run Feather Connect, then create, run, monitor and operate a flow.

## Build order (changed 31 Oct 2026)

The sponsor wants a dashboard running on Postgres as soon as possible, to run things and watch them live. So the remaining tasks run in the stages below instead of plan by plan. The plan files still hold each task's details; `2.3` means Plan 2, Task 3. The M1 scope and exit criteria don't change, only the order. Each stage gets one review, at its end.

| Stage | Result | Tasks |
|---|---|---|
| 1. Core loop | **Done.** Flows run in memory | Plan 1 |
| 2. Runnable dashboard | `feather dev` starts Postgres, the server, a worker and a demo. You sign in, watch runs arrive live, and retry, replay, pause and re-run from the browser | 2.1–2.6, 2.8, 2.9, 3.1, 3.2, 3.6, 5.1, 5.2, 5.3, 5.5–5.10, 6.1 (without `describe`), 6.5, 6.7, plus the demo task below |
| 3. Reliability and live events | Crash safety is proved, and webhooks reach destinations in seconds | 2.7, 2.10, 3.3, 3.4, 3.5, 3.7 |
| 4. Connectors | Real integrations from templates | 4.1–4.9 |
| 5. API, packaging and M1 exit | JSON API, the rest of the CLI, Compose, and the M1 exit report | 5.4, 5.11, 6.1 (`describe`), 6.2, 6.3, 6.4, 6.6, 6.8, 6.9 |

**Stage 2 demo task (new).** Create `examples/demo/`, used in place of `templates/fan-out-demo` until Stage 4. One flow reads a `simulation.Source` on `every("10s")` with a small fault rate, and delivers to two `simulation.Sink` routes, so the dashboard shows steady runs, occasional retries and failures with no external system. Test: load the project, run it on `LocalEngine`, and assert both routes receive rows.

**Changes this order needs:**
- Task 6.7 uses this demo, and doesn't need Plan 4.
- Task 5.3's and the pages' Postgres queries run against the store from Stage 2.
- Task 2.9's sweeper runs on the loops from Task 3.1, so build 3.1 right after 2.9.
- The `sealer` loop in Task 3.1 is added in Stage 3 with Task 3.4.
- Stage 2 also fixes a known gap: extraction must wait durably on `BudgetDeferred`, as delivery does.
- Until Stage 3 proves it, Stage 2 isn't claimed to be crash-safe.

**Closed in Stage 3:** F4, F11, F13, F14, and the slow database (DBOS's own connections now have the same timeouts). F12 is closed, apart from S23 below. The long test run never hung again in ten one-process runs. Leaked pools were fixed, but the cause of the hang is unproved.

**Open after the Stage 3 review:**

| Item | What's wrong | Close in |
|---|---|---|
| S5 | Unsigned callers can use up a webhook source's rate limit, because the limit runs before verification | Stage 5 |
| S6 | `by_stamp` pollers read every sealed watermark on each tick, so the cost grows with history | Stage 5 |
| S10–S23 | 14 minor findings, including webhook replays without an event id, stream-run leftovers, sinks honouring idempotency keys only in memory, and optimistic latency numbers (the test Postgres runs with `fsync=off`) | Stage 5 |
| Minor findings | 13 small items from the Stage 2 review, including trigger text such as `every 0:00:10`, navigation wrapping, and partial bulk refusals showing only in the audit row | Stage 5 |

## M1 exit criteria

| Exit criterion (design §19) | Proved by |
|---|---|
| The demo fans JSON simulation data out to Postgres, DuckDB and HTTP | Plan 6, Task 8 |
| Webhook to HTTP p95 ≤ 5 s | Plan 3, Task 7 with test connectors; Plan 6, Task 9 with the real ones |
| Re-run and replay work from the dashboard | Plan 5, Task 11 |
| kill -9 mid-apply loses and duplicates nothing | Plan 2, Task 10 |
| `feather dev` takes ≤ 5 minutes | Plan 6, Task 7 |
| H1–H4 and H6 pass | Plan 1 on `LocalEngine`; Plan 6, Task 9 on the full stack |
| H14 passes | Plan 3, Task 7 |

## Requirement coverage

| Requirements | Plan and task |
|---|---|
| FR-A1, A2 | 1.5 |
| FR-A3, A5, L2 | 6.2, 6.1 |
| FR-A4 | 6.3 |
| FR-A6 | 1.12 |
| FR-A7 | 4.9 |
| FR-B1, B5, E3, E7 | 1.10 |
| FR-B2, B3, B4 | 1.8, 1.3, 1.7 |
| FR-B6 | 1.10 (supersede), 3.4 (push order) |
| FR-B8 | 1.10 (ledger), 5.3 (action) |
| FR-B10, NFR-01 | 3.7 |
| FR-C1, C3 | 1.10, 1.8 |
| FR-D1, D2, E4 | 3.2, 3.5, 3.3 |
| FR-D4, G1, G2 | 5.3, 5.9 |
| FR-D5 | 3.6 |
| FR-E1, NFR-04 | 2.10 |
| FR-E2 | 1.6, 4.2 |
| FR-E6 | 1.14, 2.4 |
| FR-F1–F5 | 5.5–5.9 |
| FR-G5 | 5.10 |
| FR-H1 | 2.5 |
| FR-I1 | 2.1, 2.3 |
| FR-J1, J3, J4 | 1.4, 4.1 |
| FR-J2 | 1.4, 1.13, 4.3–4.8 |
| FR-K1, K3, K6 | 5.2, 2.5, 3.3 |
| FR-M1 | 5.1 |
| NFR-02 | 2.10 |
| NFR-06, 07, 15 | 6.6, 6.7 |
| NFR-08 | 1.1 (CI gates) |
| NFR-13 | 5.11 |
| NFR-14 | 4.9 |

## Where the code goes

```text
packages/
├── feather-connect/src/feather_connect/
│   ├── __init__.py          # the public API, used as `import feather_connect as fc`
│   ├── core/                # records, partitions, strategies, connector types, dataset, flow, triggers, transforms, context, errors
│   ├── engine/              # port.py, local.py
│   ├── storage/             # port.py, memory.py, spool.py, postgres/ (store, spool, migrations)
│   ├── runtime/             # runtime.py, plan.py, extract.py, seal.py, apply.py, budgets.py, http.py, sweeper.py, sealer.py, scheduler.py, leader.py
│   ├── connectors/          # memory.py, simulation.py (a namespace package, with no __init__.py)
│   ├── project.py           # feather.yaml, overlays, secrets
│   ├── worker.py            # the worker process
│   └── testing/             # harness.py, store_contract.py, conformance.py
├── feather-engine-dbos/src/feather_engine_dbos/
├── feather-server/src/feather_server/    # app, auth, services, api, web (templates, static), ingress
├── feather-cli/src/feather_cli/
└── connectors/              # feather-postgres, feather-s3, feather-http, feather-duckdb
templates/  examples/demo/  deploy/  tests/
```

Connector distributions install into the `feather_connect.connectors` namespace, so `from feather_connect.connectors import postgres` works once `feather-postgres` is installed. Third-party connectors register through the `feather_connect.connectors` entry point group.

## Decisions made while planning

The design leaves these open, or they became clear only when working through the tasks. The first five are already folded into design v0.2.

| Decision | Why |
|---|---|
| Partition snapshots hold each key and a 64-bit row hash | A delete record needs its key, so a key hash alone isn't enough |
| `by_stamp` without `keep_state` suppresses no-ops using row hashes of versions whose watermark falls within the lookback | Bronze flows such as §5.4 would otherwise append every lookback re-read |
| `by_stamp` reads once per run and splits by partition | It can't know which partitions changed until it reads |
| A complete read with no rows fails unless `allow_empty=True` | A transient empty API response must not wipe a destination |
| `snapshot_diff` with a `replace_partition` route also stores the whole slice | A version otherwise holds only the changes |
| Budget tokens aren't leased in blocks in M1 | One `UPDATE` per API call is cheap at these rates. Add leasing if the performance suite asks for it. |
| Libraries: `cronsim` (schedules), `typer` (CLI), `argon2-cffi` (passwords), `fsspec` and `s3fs` (S3 connector), `axe-playwright-python` (accessibility) | Small and well known. The `objects` spool backend is M2, so D6 stays open for it. |
| Templates ship as library functions first, such as `zakya_invoices_to_erp(map=..., extra_routes=...)`, and `feather new flow --template` copies one into the project only when a customer wants full control. Decided 7 Nov 2026 | A customer who calls the function gets our fixes on upgrade. A copied template is the customer's code and gets none. Plan 4 Task 9 and Plan 6's `feather new` follow this |
| Test Postgres runs on `127.0.0.1:25433` | Port 5432 and others are taken on the main development machine |

## Deferred past M1

These appear in the design, but not in its M1 scope:
- `fc.reconcile` and hard case H5, in M2;
- readiness, side-effect routes and approvals, overrides beyond pause and resume, RBAC, MCP, alerts, in M2;
- budget UI, in M2 (the reserve logic ships in M1, because the SQL is the same);
- row-level security policies, in M3 (every table carries `tenant_id` from M1);
- `sink.confirm`, in M6 with Magento;
- handling after a backup restore (design §7.5), such as marking versions with missing chunks `lost`, in M3 with the restore drill.

## Questions for the sponsor before or during M1

| Question | Blocks |
|---|---|
| Discovery Q9: GitHub organisation and PyPI name | Plan 6 image names and the first release |
| Discovery Q10: maintainers and governance | `CONTRIBUTING.md` and `CODEOWNERS` |
| Discovery Q11: is 14-day spool retention right? | Default value only; 14 days is used until told otherwise |
| Discovery Q2: GoFrugal docs and sandbox | M6 scope |
| ADR-0004 and the design review | The data model is built as designed. Changing it after the build costs more. |
