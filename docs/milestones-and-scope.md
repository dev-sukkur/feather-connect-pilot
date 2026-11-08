# Milestones, scope and IDs

What each milestone includes, where its scope is defined, and what the short IDs in the docs, commits and reviews mean (S1, H4, F12, FR-B2 and so on).

- **Date:** 2026-11-08
- **Kept up to date:** when a milestone's scope or status changes, and at the end of each stage review.

## 1. Where scope is defined

| Question | Source of truth |
|---|---|
| What is in v1.0, later, or not planned at all | [Discovery §5](01-discovery/2026-10-30-feather-connect-discovery.md#5-scope) |
| Which milestone each requirement lands in | [Discovery §7–§8](01-discovery/2026-10-30-feather-connect-discovery.md#7-functional-requirements), the *Milestone* column |
| Each milestone's scope and exit criteria | [Design §19](02-design/2026-10-30-feather-connect-design.md#19-delivery-plan) |
| How M1 was split into plans, tasks and stages | [Plan README](04-plan/README.md) and its six plan files |
| What M1 proved, with measured numbers | [M1 exit report](06-release/m1-exit-report.md) |
| The flow packages work brought forward after M1 | [Flow packages design](02-design/2026-11-08-flow-packages.md) and [ADR-0011](adr/0011-flow-packages-hot-install.md) |

If this page and one of those disagree, the source wins. Fix this page.

## 2. Milestones at a glance

| Milestone | Status | Scope in one line | Exit criteria |
|---|---|---|---|
| **M1: MVP, the core loop** | **Done**, 8 Nov 2026 (PR 8) | SDK, durable runtime on Postgres, JSON API and HTML dashboard, CLI and `feather dev`, generic connectors and templates, Compose | All met (§3.3) |
| **Flow packages** | **Next.** Design accepted 8 Nov 2026 (PR 11) | Flows released as packages from the customer's own repository, installed from a registry built into `feather-server`, and activated or deactivated per flow with no restart | §4 |
| **M2: business connectors and operations** | Not started | SAP, Zakya and Magento connectors with retail templates; on-prem agent; side effects and approvals; readiness; RBAC; MCP; alerts | §5 |
| **M3: enterprise and v1.0** | Not started | Multi-tenancy, environment fleet view, SSO, vaults, Postgres CDC, hardening, the v1.0.0 release | §6 |
| **After v1.0** | Not scheduled | Helm, Kafka, more connectors, marketplace, lineage, in-dashboard AI | §7 |

## 3. M1: MVP, the core loop

### 3.1 Scope (design §19)

- **SDK:** datasets, flows, routes, strategies (`by_stamp`, `snapshot_diff`, `replace`, webhook `push`), partitions, versions, record state (`keep_state`), transforms, validation and quarantine.
- **Runtime:** ledger, replay, budget windows, scheduler, ingress, sealer and sweeper, on `LocalEngine` and `DbosEngine`, with the Postgres spool.
- **Server:** JSON API and HTML dashboard (overview, flows, flow detail, partition grid, runs and logs, deliveries and failures, connections), with run, re-run, backfill, cancel, retry, skip and replay (with preview).
- **CLI:** `dev`, `new`, `validate`, `run`, `test`, `backfill`, `replay`, `status`, `logs`, `server`, `worker`.
- **Connectors:** in-memory, JSON simulation, Postgres, S3, REST, GraphQL, webhook, HTTP, DuckDB and MotherDuck, plus the five generic templates.
- **Platform basics:** local users and tokens, env and file secrets, structured logs, Compose, `AGENTS.md`, environment overlays, the default tenant.

### 3.2 How it was built

| Stage | Result | Plan tasks | Merged |
|---|---|---|---|
| 1. Core loop | Flows run in memory | Plan 1 | On main, before the PR workflow |
| 2. Runnable dashboard | `feather dev`, Postgres, server, worker and demo; act on flows from the browser | 2.1–2.6, 2.8, 2.9, 3.1, 3.2, 3.6, 5.1–5.3, 5.5–5.10, 6.1, 6.5, 6.7 | PR 3 |
| 3. Reliability and live events | Crash safety proved; webhooks reach destinations in seconds | 2.7, 2.10, 3.3, 3.4, 3.5, 3.7 | PR 4 |
| 4. Connectors | Real integrations from templates; the Zakya demo | 4.1–4.9 | PR 6, PR 7 |
| 5. API, packaging and exit | JSON API, the rest of the CLI, Compose, the exit report | 5.4, 5.11, 6.1–6.4, 6.6, 6.8, 6.9 | PR 8 |

A task number such as `2.3` means Plan 2, Task 3.

### 3.3 Exit criteria and results

| Criterion | Result (M1 gate, 8 Nov 2026) |
|---|---|
| The demo fans JSON simulation data out to Postgres, DuckDB and HTTP | Passed |
| Webhook → HTTP p95 ≤ 5 s | p95 1.856 s; 2.004 s on a durable (fsync on) Postgres; 3,000 of 3,000 events, none lost or repeated |
| Re-run and replay work from the dashboard | Passed |
| kill -9 mid-apply loses and duplicates nothing | 12 chaos tests passed |
| `feather dev` ≤ 5 minutes | 67 s |
| Hard cases H1–H4, H6 and H14 pass | Passed; H14: 18,229 events, none lost |

### 3.4 Added after the gate, already on main

- **Zakya end-to-end demo** and the retail template `zakya_invoices_to_erp` (Stage 4). Retail templates are M2 scope; this one came early for the demo.
- **Dashboard** (PR 10): a page per version, the menu in a left sidebar, and **record trace** (`/trace`, `GET /api/v1/datasets/{d}/records/{key}/trace`, migration 0013). Record trace is M2 scope, brought forward. Version diff is still M2.

### 3.5 Left out of M1 on purpose

`fc.reconcile` and H5; readiness; side-effect routes and approvals; overrides beyond pause and resume; RBAC; MCP; alerts; the budgets page; row-level security (M3); `sink.confirm` (M2, with Magento); handling after a backup restore (M3). See the [plan README](04-plan/README.md#deferred-past-m1).

### 3.6 Known open items after M1

From the [exit report §5–§6](06-release/m1-exit-report.md#5-open): pyright can't follow the namespace packages; OAuth2 tokens are shared per process, not across processes (M2); `feather dev` helpers on macOS; a lookback made longer later doesn't reach older versions; an `Idempotency-Key` race in the API; `feather new --template` loses YAML comments; a scheduler tick lost on a crash; the server image carries the demo's `erp-token.txt` (m1); `latest_watermark` locks needlessly (m2); `feather server` binds 0.0.0.0 by default (m3).

Also found since: `feather validate` misses duplicate route names across flows, and `feather run --sim` fails with signed webhook sources.

## 4. Flow packages (next)

Brought forward ahead of the rest of M2 (sponsor, 8 Nov 2026). Details in the [design](02-design/2026-11-08-flow-packages.md); decision in [ADR-0011](adr/0011-flow-packages-hot-install.md).

**Scope:**
- A flow package is a Python wheel built with `feather package build` in the customer's own repository.
- Packages come from a registry built into `feather-server`, on by default, at `/registry/simple/`. Company Python package indexes can be added beside it.
- Install checks the package and stores it in Postgres. Installed flows start inactive.
- Each flow is activated and deactivated per environment. Running runs finish on deactivate. Activating another version switches it, and rollback works the same way.
- Workers become supervisors with one package host process per active package version, so nothing restarts.
- Every name carries the package prefix (`acme-invoices/invoices`).
- Roles `admin` and `operator`, pulled forward from M2's RBAC.
- Packages are the only way to deploy. The M1 project mode is retired, and M1 environments upgrade in place.

**Plan:** tasks 0–12 in [design §14](02-design/2026-11-08-flow-packages.md#14-plan).

**Exit criteria:** in a running Compose environment, with no container restarted, a customer installs a package built in another repository, activates a flow and sees it run; upgrades it, rolls it back and deactivates it; and another package keeps running throughout, with no run lost or doubled.

## 5. M2: business connectors and operations (~8 weeks)

**Scope (design §19):**
- **Connectors and templates:** SAP (OData, HANA read), Zoho Zakya, Magento, and the retail templates. GoFrugal too if its API docs arrive in time, otherwise M3.
- **On-prem:** the agent and its gateway.
- **Side effects:** side-effect routes with compensate, approvals and holds.
- **Data rules:** schema policy; readiness and `on_ready`; derived datasets and SQL transforms; bootstrapping newly added routes from history.
- **Operations:** budget priorities and a budgets page; overrides; RBAC (the two roles come earlier, with flow packages); audit; PII masking; version diff (record trace is done); SLAs and anomalies; alerts.
- **AI:** the MCP server and agent skills.
- **Delivery:** `feather diff`; the `objects` spool backend. Promotion between environments now comes with flow packages (`feather package promote`).

**Exit criteria:**
- The reference flows run in the reference organisation's qa/uat: Zakya invoices → MotherDuck, SAP prices → Zakya/GoFrugal/Magento, Magento orders → SAP with approvals.
- All hard cases, H1–H15, pass.
- Agent-driven recovery is shown in qa.

## 6. M3: enterprise and v1.0 (~8 weeks)

**Scope (design §19):**
- **Tenancy:** multi-tenancy (row-level security, quotas, per-tenant config and RBAC, worker pools).
- **Environments:** environment peers and the fleet view; protections for protected environments.
- **Integrations:** OIDC SSO; vault secret providers; Postgres CDC; Prometheus and OpenTelemetry export.
- **Hardening:** chaos and performance suites with published results; a security review.
- **Launch:** the docs site and the v1.0.0 release.

**Exit criteria:** NFR targets met; the release checklist complete; the reference organisation's in-scope flows cut over to prod.

## 7. After v1.0, and not planned

From [discovery §5](01-discovery/2026-10-30-feather-connect-discovery.md#5-scope):

| Later (after 1.0) | Not planned |
|---|---|
| Helm chart and Kubernetes; Kafka or Redpanda as an optional transport; a visual data-mapping assistant; an in-dashboard AI assistant; more connectors (Salesforce, Dynamics, NetSuite, Snowflake, BigQuery, Kafka, SFTP, Excel); a community connector marketplace; column-level lineage | A visual flow designer; a single-page JavaScript front end; hosted SaaS; an API gateway or developer portal; full BPM |

## 8. What the IDs mean

**Two prefixes mean two different things**, so read them with their context:
- **S** is a *spike* (S1–S4) in design and spike docs, and a *Stage 3 review finding* (S1–S23) in commits, the plan README and the exit report.
- **M** is a *milestone* (M1–M3), and also a *Minor review finding* within one review ("Stage 2 M13", "Stage 4 M6"). The exit report writes Minor findings in lower case (m1–m4).

| ID | Meaning | Defined in |
|---|---|---|
| M1–M3 | Milestones | Design §19 |
| FR-A1 … FR-M3 | Functional requirements, by area: A authoring, B data movement, C transformation, D triggers, E reliability, F–G dashboard and operations, H environments, I tenancy, J connectors, K security, L AI, M observability | Discovery §7 |
| NFR-01 … NFR-15 | Non-functional requirements (performance, durability, footprint, security, accessibility and others) | Discovery §8 |
| Q1–Q11 | Open questions for the sponsor | Discovery §15 |
| D1–D8 | Open design questions | Design §20 |
| H1–H15 | Hard cases: scenarios every milestone must pass (M1: H1–H4, H6, H14; M2: all) | Design §16; first run in spike S2 |
| S1–S4 (spikes) | S1 engine (DBOS chosen), S2 core model, S3 connector research, S4 Postgres spool | [Spikes](03-spikes/README.md) |
| 1–6 (plans), `2.3` (tasks) | The six M1 plans; `2.3` is Plan 2, Task 3 | [Plan README](04-plan/README.md) |
| Stage 1–5 | The order M1's tasks were built in | Plan README, *Build order* |
| F1–F15 | Stage 2 review findings | §8.1 below |
| S1–S23 (findings) | Stage 3 review findings | §8.2 below |
| B1–B6, rulings 1–12 | Stage 4 preflight: blockers, and binding rulings | §8.3 below |
| I1–I4, m1–m4 | End of M1 review (Stage 5): Important and Minor findings | [Exit report §6](06-release/m1-exit-report.md#6-end-of-stage-review) |
| ADR-0001 … 0011 | Architecture decision records | [ADR index](adr/README.md) |

The review notes behind §8.1–§8.3 are kept outside the repository, so their findings are summarised here.

### 8.1 Stage 2 review findings (F1–F15): all closed

| ID | Finding | Closed |
|---|---|---|
| F1 | Replay from the partition grid re-sent the whole history, and its preview went stale | Stage 2 fixes |
| F2 | Replaying one old version silently put old data in the destination | Stage 2 fixes |
| F3 | Replaying a version whose chunks were gone reported success and sent nothing | Stage 2 fixes |
| F4 | A task running twice could overwrite a sealed version's chunks | Stage 3 |
| F5 | The sweeper marked a worker dead before recovering it | Stage 2 fixes |
| F6 | A failed submit left a run queued for ever | Stage 2 fixes |
| F7 | A delivery whose task had ended could stay pending | Stage 2 fixes |
| F8 | Connector error text reached the database and pages unredacted | Stage 2 fixes |
| F9 | Pools had no connect timeout, keepalives or statement timeout | Stage 2 fixes |
| F10 | Each `feather dev` reload leaked Postgres connections | Stage 2 fixes |
| F11 | The sweeper's cost grew with every delivery waiting behind a failure | Stage 3 |
| F12 | A read needing more calls than the budget refills could never finish | Stage 3, with its rest as S23 |
| F13 | Staged versions of failed runs stayed for ever | Stage 3 |
| F14 | The retention report reached nobody | Stage 3 |
| F15 | The stage's main deliverable wasn't tested in CI | Stage 2 fixes |

The Stage 2 review's Minor findings (M1–M13) were fixed in Stages 3–5, except M13, which couldn't be reproduced.

### 8.2 Stage 3 review findings (S1–S23)

| ID | Finding | Closed |
|---|---|---|
| S1 | One bad event could block a push dataset's sealer for good | Stage 3 (`3fc08d4`) |
| S2 | A failed `receive` threw the event away | Stage 3 (`9d19cd8`) |
| S3 | A key moving partitions twice in one batch ended in the wrong partition | Stage 3 (`99999a4`) |
| S4 | Ingress hung under a database outage, then refused valid requests | Stage 3 (`0f21246`) |
| S5 | Unsigned callers could use up a webhook source's rate limit | Stage 5 (`61da57a`) |
| S6 | `by_stamp` pollers read every watermark on each tick | Stage 5 (`c1cda23`) |
| S7 | The push path's crash safety was unproved | Stage 3 (`412db4e`) |
| S8 | The Stage 3 proof didn't run in CI | Stage 3 (`ff14b0e`) |
| S9 | Guides and the plan README were out of date | `b456d57` |
| S10 | A resent webhook without an event id was stored twice | Stage 5 (`5898541`) |
| S11 | A crash after sealing left record state stale | Stage 5 (`f6345c2`) |
| S12 | Stream runs' leftovers were never cleaned up | Stage 5 (`fa4fbf2`) |
| S13 | Orphan claim rows | No risk; nothing to fix |
| S14 | Dropping an inbox day could deadlock | Stage 5 (`cefaad2`) |
| S15 | Run pages showed the newest 50 versions' deliveries on every run | Stage 5 (`bbaabbe`) |
| S16 | A huge `Content-Length` gave a 500 | Stage 3, with S4 |
| S17 | `on_record_error="fail"` was ignored on push datasets | Stage 5 (`54ed131`) |
| S18 | A multi-slice complete dataset on a poller failed at each tick, not at load | Stage 5 (`54ed131`) |
| S19 | Latency numbers were optimistic (the test Postgres had fsync off) | Stage 5: measured on a durable Postgres, p95 2.004 s (`7d7e424`) |
| S20 | The sealer could hold about 500 MB | Stage 5 (`1da6f2f`) |
| S21 | Built-in sinks honoured idempotency keys in memory only | No commit names it; to check |
| S22 | The DBOS URL lost blank and repeated query keys | Stage 5 (`6b94b87`) |
| S23 | F12's rest: budget waits could give up wrongly or stall | Stage 5 (`68892a0`) |

### 8.3 Stage 4 preflight (B1–B6 and rulings)

| ID | Blocker | Closed |
|---|---|---|
| B1 | Connector packages couldn't be imported in the dev setup | Stage 4 |
| B2 | Env overlays didn't switch flows to simulation connectors | Stage 4 (ruling 9) |
| B3 | S3's `_fc_object_mtime` column would be quarantined | Stage 4 (`_object_mtime`, ruling 8) |
| B4 | Saved read positions were never used | Stage 4 |
| B5 | Sources couldn't quarantine a bad row | Stage 4 (`fc.Unreadable`) |
| B6 | Sinks couldn't do `replace_partition` from what they received | Stage 4 |

The **rulings** are binding decisions for the build:
1. Sinks write `_fc_key`, `_fc_partition`, `_fc_version_id`, `_fc_op` and `_fc_applied_at`, plus a `_feather_applied` table in the destination.
2. One HTTP client per connector instance, with the budget passed per call.
3. Auth scheme settings go at the connection's top level; only secrets go under `auth`.
4. A `Retry-After` over 60 s raises `TransientError` at once.
5. Mapped response fields (`response_map`) go to a table.
6. Webhook settings are `signature_secret`, `unsigned` and `event_id_header`.
7. `http.Call` rejects deletes by default.
8. The S3 time column is `_object_mtime`.
9. dev and qa use simulation through the loader.
10. A connector is `maturity="beta"` once its conformance tests pass.
11. Data people may query, verify or retry goes in a table or object storage, never only in a log.
12. Templates ship first as library functions; `feather new --template` copies one for full control.

## 9. Numbering from now on

To stop the clashes in §8, new review findings carry their review's name: `PKG-R1-I3` is the third Important finding of the first flow packages review. Milestones keep M1–M3, spikes keep S1–S4, and the IDs above stay as they are.
