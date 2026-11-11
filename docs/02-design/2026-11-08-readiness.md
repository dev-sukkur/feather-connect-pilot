# Readiness: run a flow when the data it needs is complete

- **Status:** Proposed, for the sponsor's review
- **Sponsor's answers (8 Nov 2026):** flow packages come first in M1.1, then readiness; `on_late="rerun"` is the default; a skipped version counts as delivered
- **Date:** 2026-11-08
- **Decision record:** [ADR-0012](adr/0012-readiness-triggers.md)
- **Milestone:** M1.1, brought forward from M2 together with flow packages ([milestones](../04-plan/milestones-and-scope.md))
- **Builds on:** design §4.6 (readiness and consistent cuts), requirements FR-B7 and FR-D3, hard case H13

## 1. The problem

In M1 a flow starts on a clock (`cron`, `every`), by hand, or when webhook events arrive. It can't start because **other data is ready**. So a chain such as *Zakya → Postgres, then Postgres → SAP and the warehouse* runs on two clocks that know nothing of each other:

- SAP's flow may read Postgres while the Zakya flow is still writing to it, and pick up half of a run.
- If the Zakya flow fails or runs late, SAP's flow runs anyway, on old data, and nothing says so.
- A store-day posting can't wait until a store's invoices, returns and payments for that day are all in.
- The only fix today is to guess a safe gap between the two schedules.

## 2. What readiness gives

| Need | How |
|---|---|
| Run SAP and the warehouse only after a Zakya → Postgres run has finished and its rows are written | `fc.on_ready(fc.ready(zakya_invoices, delivered_to="staging"), each="run")` |
| …but not more than once every 4 hours | `fc.on_ready(..., each="run", at_most_every="4h")` |
| Post a store-day once its invoices, returns and payments are all complete | `fc.on_ready(fc.ready([invoices, returns, payments], by="store_id"), each="day", tz="Asia/Kolkata")` |
| See what a waiting flow is waiting for | The flow page and the Readiness page: "waiting for invoices of store 1509 to be delivered to staging through 18 Oct" |
| Know when data arrives after its period already ran | A *late* run, or a warning, as the flow chooses (§6) |
| Never hold up anything else | Readiness is read only by flows that ask for it. Routes, other flows and the upstream flow itself are never blocked (H13) |

## 3. Concepts

| Term | Meaning |
|---|---|
| **Read-complete through T** | Every record of the dataset stamped or dated up to T has been read and sealed in a version |
| **Delivered through T** | Read-complete through T, **and** a named route has applied every version up to that point |
| **Readiness set** | `fc.ready(...)`: one or more datasets, optionally a route each must be delivered to, and optionally a field to split by (per store) |
| **Period** | What one firing covers: one upstream run (`each="run"`), an hour or a day in a time zone |
| **Scope** | One value of the split field, such as store 1509. Each scope is ready, and fires, on its own |
| **Firing** | One run of the waiting flow for one (readiness set, period, scope). Stored, so it happens exactly once |

## 4. When a dataset is complete through T

This depends on how the dataset is read:

| Strategy | Read-complete through T when |
|---|---|
| `replace`, `snapshot_diff` with day (or hour) partitions | The partition for T's period has a complete version from a run that **started after the period ended**, so late edits within the period are in it |
| `replace`, `snapshot_diff` with no partitions | A complete version from a run that started after T |
| `by_stamp` | A run that started at S succeeded, with no failed partition, and its read covered stamps up to S − `settle`. Then it is complete through S − `settle`. `settle` defaults to the dataset's `lookback`, the window where late-stamped rows may still appear |
| `push` (webhooks) | Every event received up to T is sealed, and either T is older than `settle` (default 5 minutes), or the source said so (§4.1) |

### 4.1 A source that says a period is closed

Some systems know better than the clock, for example a POS that closes the store-day. A flow can mark a partition closed:

```bash
feather ready close invoices --partition "store_id=1509/date=2026-11-08" --env prod
```

The same is `POST /api/v1/datasets/{d}/partitions/{p}:close`, and a connector can do it from a source event. Closing is recorded with who did it, and audited. It never replaces the read: a closed partition is complete once its last version is sealed.

### 4.2 Delivered through T

`fc.ready(..., delivered_to="staging")` adds a condition: the route `staging` has applied, in order, every version of the dataset up to the read-complete point. The route's ledger already applies versions in order per partition, so this is "its newest applied version is the newest sealed one up to T". It follows the rules the ledger already has:

| The route's state | Counts as delivered? |
|---|---|
| Applied | Yes |
| Skipped by an operator | Yes. The firing records that a version was skipped, and the flow page shows it |
| Records turned down by validation (quarantine) | Yes, unless the set says `quarantine="blocks"` |
| Failed, pending or running | No. The waiting flow shows "waiting for staging: version 5 failed", with a link to it |

## 5. The SDK

```python
import feather_connect as fc

# Zakya → Postgres, every hour, as before.
ingest = fc.Flow("zakya-ingest", reads=zakya_invoices, deliver=[to_staging], triggers=[fc.every("1h")])

# Postgres → SAP after each ingest run is written to staging, at most every 4 hours.
staged = fc.ready(zakya_invoices, delivered_to="staging")
to_sap = fc.Flow(
    "to-sap",
    reads=staged_invoices,                 # the staging table, read by stamp as before
    deliver=[to_sap_route],
    triggers=[fc.on_ready(staged, each="run", at_most_every="4h")],
)

# A store-day posting once invoices, returns and payments of that store-day are complete.
store_day = fc.ready([invoices, returns, payments], by="store_id")
posting = fc.Flow(
    "store-day-posting",
    reads=store_day_totals,
    deliver=[to_erp],
    triggers=[fc.on_ready(store_day, each="day", tz="Asia/Kolkata", on_late="rerun")],
)
```

| Call | Arguments |
|---|---|
| `fc.ready(datasets, *, delivered_to=None, by=None, settle=None, quarantine="passes")` | `datasets`: one or a list. `delivered_to`: a route name, or a dict per dataset. `by`: a partition field, so each value is its own scope. `settle`: overrides §4's default |
| `fc.on_ready(ready, *, each="run" \| "hour" \| "day", tz=None, at_most_every=None, on_late="rerun" \| "warn" \| "ignore", start=None)` | `each="run"` needs one dataset in the set. `tz` is required for `hour` and `day`. `at_most_every` gathers ready periods into one run. `start` is the first period to fire, so a new flow doesn't fire for all of history |

The run a firing starts knows what it is for. `ctx.ready` holds the period (`start`, `end`), the scope (`{"store_id": "1509"}`) and, for `each="run"`, the upstream runs and versions. A dataset's planner can use it to read just that store-day:

```python
store_day_totals = fc.Dataset(
    "store-day-totals",
    source=postgres.Query("staging", sql=TOTALS_SQL),
    key="store_day",
    strategy=fc.strategy.replace(),
    partition=fc.partition.by_fields("store_id", "date"),
    planner=lambda ctx: [f"store_id={ctx.ready.scope['store_id']}/date={ctx.ready.start:%Y-%m-%d}"],
)
```

A flow may have both clock and readiness triggers. `feather validate` refuses: an `on_ready` set naming a dataset no flow reads; a `delivered_to` route that doesn't read that dataset; `by` a field the datasets don't partition on; `each="run"` with more than one dataset; `hour` or `day` without `tz`; and a readiness cycle (A waits for B, B waits for A).

## 6. When data arrives after its period fired

A version can be sealed for a period that already fired, for example an invoice edited after the store-day posting. `on_late` decides:

| `on_late` | What happens | For |
|---|---|---|
| `"rerun"` (default) | When that period is ready again, a new run starts for the same period and scope, with reason *late data* and the versions that caused it. A posting sink then reverses and posts again, or posts a delta (discovery Q8, M2 side-effect routes) | Totals and postings that must match the source |
| `"warn"` | No run. The Readiness page and the flow page show the period as *late*, with the versions | Flows where a person decides |
| `"ignore"` | Nothing | Flows that pick changes up by themselves anyway, such as a `by_stamp` read |

`each="run"` never has late data: each upstream run is its own period.

## 7. How it runs

### 7.1 Where readiness is worked out

- **Marks, written where the facts are.** The seal transaction already updates the dataset's watermark row (0012). It now also updates a **readiness mark** per (dataset, partition): read-complete through, the version and the run. Applying a delivery updates a **route progress** row: the newest version applied in order, per (route, dataset, partition). Both are single-row updates in transactions that already exist.
- **An evaluator on the leader.** The scheduler's leader runs a readiness loop. It wakes on `NOTIFY feather_ready` (sent by sealing and applying) and every 30 seconds for clock-based conditions, such as a period ending or `settle` passing. It looks only at sets whose marks changed, so the cost doesn't grow with history.
- **Firing exactly once.** A firing is a row keyed by (tenant, flow, set, period, scope). The evaluator inserts it and submits the run in one transaction, through the same path as a scheduled run. A leader that dies after the insert is caught by the sweeper, which submits runs for firings with no run, as it already does for queued runs (F6).
- **Overlap.** A firing follows the flow's `overlap` setting (`queue`, `skip`, `cancel_previous`), like a clock trigger.

### 7.2 Storage

| Table | One row per | Columns |
|---|---|---|
| `readiness_mark` | (dataset, partition) | complete_through, closed_at, closed_by, version_id, run_id, at |
| `route_progress` | (route, dataset, partition) | applied_through_seq, version_id, skipped (bool), at |
| `readiness_firing` | (flow, set, period, scope) | run_id, reason (`ready`, `late data`, `manual`), versions (jsonb), fired_at, late_versions (jsonb) |

All carry `tenant_id`, like every other table. The migration fills `readiness_mark` and `route_progress` from existing versions and the ledger, so a flow given `on_ready` today sees the history it already has.

### 7.3 Flow packages

Flow packages are built first (§12), so readiness names datasets and routes by their package-prefixed names (`acme-invoices/invoices`). A package may wait for a dataset of another package. Activation checks that the upstream package is installed and active; if it is later deactivated, the waiting flow shows *upstream inactive* and doesn't fire.

## 8. Dashboard, CLI and API

- **Readiness page:** each readiness set with its scopes as rows and periods as columns: *ready and fired* (with a link to the run), *waiting* (with what it waits for), *late*, *not started*. A filter by flow and scope.
- **Flow page:** for a flow with `on_ready`, a line such as "Next run when invoices of store 1509 are delivered to staging through 18 Nov 23:59 (now through 18 Nov 21:00)".
- **Run page:** a run started by readiness says which set, period and scope, and lists the upstream runs and versions with links.
- **Actions:** *Fire now* for a waiting period (needs a reason, audited); *Close partition* (§4.1).
- **CLI:** `feather ready status [flow]`, `feather ready close`, `feather ready fire <flow> --period … --scope …`.
- **API:** `GET /api/v1/readiness`, `GET /api/v1/flows/{flow}/readiness`, `POST /api/v1/flows/{flow}/readiness:fire`, `POST /api/v1/datasets/{d}/partitions/{p}:close`. Each action takes a reason and an `Idempotency-Key`, as in M1.

## 9. Limits of this design

- **Gating** a clock-triggered run until data is ready ("run at 06:00, but only if ready, otherwise wait") is left for later. `on_ready` with `start` covers most of it.
- Readiness works across flows and packages in **one environment**. Waiting for data in another environment is M3's fleet view.
- A `by_stamp` source whose stamps can arrive later than `settle` will make a period ready too early. The `on_late` setting catches what comes after, and the Readiness page shows how often it happens, so `settle` can be raised.

## 10. Plan

| # | Task | Proves |
|---|---|---|
| 1 | Migration for §7.2's tables and their backfill; marks written in the seal and apply transactions; `NOTIFY feather_ready` | Read-complete and delivered marks are right for each strategy in §4, on memory and Postgres stores |
| 2 | SDK: `fc.ready`, `fc.on_ready`, `ctx.ready`; validation rules (§5) | Each refusal names its fix; a cycle is found |
| 3 | The evaluator loop on the leader; firings exactly once; sweeper recovery | Chaos: kill the leader between the firing and the submit; one run per firing |
| 4 | `each="run"` with `at_most_every` | Ingest → staging → SAP: SAP never starts before staging has the run's rows |
| 5 | `each="hour"` and `"day"`, `by` scopes, `start`, `settle` | Three datasets per store-day; one store late doesn't hold the others |
| 6 | `on_late` | A late version for a fired store-day reruns it once; `warn` and `ignore` as described |
| 7 | Close a partition (CLI, API, connector hook), audited | A closed push partition is ready at once |
| 8 | Dashboard: Readiness page, flow and run page lines, *Fire now*; accessibility | axe at 0 violations, light and dark; 360 px |
| 9 | CLI and JSON API; Operating Guide section | Each command and endpoint with its tests |
| 10 | Hard cases: H13 on readiness, and two new ones (§11) | Pass on the full stack |

## 11. Exit criteria

- The Zakya chain runs on `feather dev`: ingest every hour to Postgres, then SAP and the warehouse each start only after an ingest run's rows are in staging. SAP never reads a half-written run.
- A store-day posting waits for invoices, returns and payments of that store-day and fires once per store; a store whose payments are late fires later on its own, without holding up the others.
- **H13** passes: header and line feeds arrive independently; lines are never blocked by headers; readiness gates only where configured.
- **New hard case H16:** the leader is killed while firing; exactly one run per firing.
- **New hard case H17:** a late version arrives for a fired period; with `on_late="rerun"` exactly one rerun starts, and it holds the late version.

## 12. Decisions

| # | Question | Decision (8 Nov 2026) |
|---|---|---|
| 1 | Which comes first in M1.1: readiness or flow packages? | **Flow packages first**, then readiness. Readiness is built on package-prefixed names from the start (§7.3) |
| 2 | The default for `on_late` | `"rerun"`, so totals and postings stay right without anyone watching |
| 3 | Does a route's skipped version count as delivered? | Yes, shown as skipped in the firing (§4.2) |
