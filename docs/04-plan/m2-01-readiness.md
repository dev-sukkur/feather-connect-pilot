# M2 plan 1: readiness (stages R1–R3)

Builds the [readiness design](../02-design/2026-11-08-readiness.md) on flow packages. Scope and exit criteria are the
design's §11 and [milestones §4.2](milestones-and-scope.md#42-readiness-moved-to-early-m2). This plan records what
changed from the design now that every flow is packaged, and the order of the work.

## 1. What changes from the design

| Design | Built as | Why |
|---|---|---|
| §7.1: the evaluator runs on the scheduler's leader | It runs in the server, beside the catalogue scheduler, from the **catalogue specs** of active flows. A firing queues a run tagged with the flow's package and version, as a clock tick does, and that version's host takes it | The server holds no package code (flow packages §7.2). Everything readiness needs is in the spec or in the store |
| §7.2: `readiness_mark` per (dataset, partition), written by the seal | **Not a table.** The newest sealed version of a partition is already one indexed read (`latest_sealed`), and it carries its run, whose `started_at` and status are what §4 asks | A second copy of the same facts, kept in step in every seal, is a place for them to disagree |
| §7.2: `route_progress` per (route, dataset, partition), written by apply | **Not a table.** "Delivered through version v" is `Ledger.oldest_open(route, partition)` being none or later than v: the ledger already applies versions in order per partition (§4.2) | As above. The ledger is the truth for delivery |
| §7.2: `closed_at`, `closed_by` on the mark | `readiness_close` per (dataset, partition): who, when, why | Closing is the one fact that exists nowhere else |
| §7.2: `readiness_firing` | As designed, keyed by (tenant, flow, set, period, scope), with the run, reason, versions and late versions | Firing exactly once is the point |
| §7.1: `NOTIFY feather_ready` on seal and apply | As designed: the server's readiness loop listens and wakes at once; it also polls every 10 s for clock conditions (every 2 s on the memory store, which doesn't notify) | — |

So the cost of a pass is per waiting period, not per version: the evaluator looks at the periods from `start` (or the
last fired one) to now, for each scope it has seen, and reads one newest version and one oldest open delivery per
(dataset, partition) it needs.

### 1.1 What a spec carries

`Flow.spec_dict()` gains, so that the server can evaluate with no code:

- for each dataset: `lookback` (seconds, for `by_stamp`'s default `settle`) and `partition`, the scheme as data
  (`{"kind": "day", "field": "date", "tz": "Asia/Kolkata"}`, `{"kind": "field", "field": "store_id"}`,
  `{"kind": "fields", "parts": [...]}`, `{"kind": "none"}`; month and bucket likewise);
- for each `on_ready` trigger: the set (datasets by prefixed name, `delivered_to`, `by`, `settle`, `quarantine`) and
  `each`, `tz`, `at_most_every`, `on_late`, `start`.

A spec recorded before this has neither, and a flow with an `on_ready` trigger can only be installed by a version
that records them.

### 1.2 Names across packages

Datasets and routes are named by their prefixed names (`acme-invoices/invoices`), as §7.3 of the design says. A set may
name a dataset of another package by its prefixed name; activation refuses a flow whose set names a dataset that no
active flow reads, and a set whose upstream is deactivated later shows *upstream inactive* and doesn't fire.

## 2. Tasks

| # | Task | Proves |
|---|---|---|
| 1 | Migration 0028: `readiness_close`, `readiness_firing`; `NOTIFY feather_ready` from seal and from a delivery reaching applied or skipped; the store ports (memory and Postgres) | Store contract: a firing is inserted once per key; a close is kept with who and why |
| 2 | SDK: `fc.ready`, `fc.on_ready`, `ctx.ready`; spec fields of §1.1; validation of design §5 | Each refusal names its fix; a cycle is found; the spec round-trips |
| 3 | The evaluator in the server: completeness per design §4 from the store, firings exactly once, a run tagged with the version; the sweeper submits a firing with no run | Chaos: the server killed between the firing and its run; one run per firing |
| 4 | `each="run"` with `at_most_every` | Ingest → staging → SAP: SAP never starts before staging has the run's rows |
| 5 | `each="hour"` and `"day"`, `by` scopes, `start`, `settle` | Three datasets per store-day; one store late doesn't hold the others |
| 6 | `on_late` | A late version for a fired store-day reruns it once; `warn` and `ignore` |
| 7 | Close a partition (CLI, API), audited | A closed push partition is ready at once |
| 8 | Dashboard: Readiness page, flow and run page lines, *Fire now* | axe at 0 violations, light and dark; 360 px |
| 9 | CLI `feather ready status | close | fire` and the JSON API; Operating Guide section | Each command and endpoint with its tests |
| 10 | Hard cases H13, H16, H17 on the full stack | Pass |

Tasks 1–3 are stage R1, 4–7 R2, 8–10 R3. All three go in one PR.

## 2.1 Where it stands (12 Nov 2026)

| # | State | Where |
|---|---|---|
| 1 | Done | Migration 0028; `ReadinessStore` on memory and Postgres; NOTIFY `feather_ready` from seal and from applied or skipped |
| 2 | Done | `core/readiness.py`, `Flow.spec_dict()`, `project.prefixed`, `validate_project`; `tests/core/test_readiness.py` |
| 3 | Done | `runtime/ready_eval.py`, the server's `readiness` loop (every 2 s). A firing and its run are one transaction, so no sweeper step is needed: a server killed mid-fire leaves both or neither |
| 4–7 | Done | `tests/runtime/test_ready_eval.py` |
| 8 | Done | `/readiness`, lines on the run and flow pages; axe clean in light and dark |
| 9 | Done | `feather ready`, `/api/v1/readiness…`; Operating Guide §8c |
| 10 | Done on Postgres | `tests/hard_cases/test_readiness_cases.py`: H13, H16 (8 evaluators racing), H17. They stop at the queued run, which a host takes as it takes a clock tick's |

Also done (the items left open on 12 Nov):

- **Activation of a cross-package set** is refused, naming the dataset, until a flow that reads it is active
  (`services/packages.py`, `inactive_upstreams`). One deactivated later shows *upstream inactive* and doesn't fire.
- **NOTIFY wake**: `ReadinessStore.listen` on Postgres; `LoopRunner.wake`.
- **The Zakya chain**: `examples/zakya-demo/flows/chain.py`, `erp-check`, starts after each ingest run's invoices are
  applied to the ERP, at most every 5 minutes; `tests/demo/test_zakya_demo.py`.
- **A packaged poller's tick is a task of the day's stream run** (§3 below).

## 3. Next

- **A packaged poller's tick is a run** (carried from M1.1): done. Its host ticks its active flows' pollers into the
  day's stream run (`scheduler.tick(..., streams_of=...)`), and the server's catalogue ticks only the others, so a
  poller every 5 s writes a run a day, not 17,280. Readiness counts a stream run's ticks by their versions, not by runs, so it
  doesn't depend on this, but `each="run"` on a poller would fire 17,280 times a day without it.
