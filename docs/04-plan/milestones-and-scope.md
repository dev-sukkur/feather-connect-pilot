# Milestones, scope and IDs

What each milestone includes, where its scope is defined, and what the short IDs in the docs, commits and reviews mean (SP1, S5, H4, F12, FR-B2 and so on).

- **Date:** 2026-11-08
- **Kept up to date:** when a milestone's scope or status changes, and at the end of each stage review.

## 1. Where scope is defined

| Question | Source of truth |
|---|---|
| What is in v1.0, later, or not planned at all | [Discovery §5](../01-discovery/2026-10-30-feather-connect-discovery.md#5-scope) |
| Which milestone each requirement lands in | [Discovery §7–§8](../01-discovery/2026-10-30-feather-connect-discovery.md#7-functional-requirements), the *Milestone* column |
| Each milestone's scope and exit criteria | [Design §19](../02-design/2026-10-30-feather-connect-design.md#19-delivery-plan) |
| How M1 was split into plans, tasks and stages | [Plan README](README.md) and its six plan files |
| What M1 proved, with measured numbers | [M1 exit report](../06-release/m1-exit-report.md) |
| M1.1: what is brought forward from later milestones | §4 of this page; the [flow packages design](../02-design/2026-11-08-flow-packages.md) with [ADR-0011](../02-design/adr/0011-flow-packages-hot-install.md), and the [readiness design](../02-design/2026-11-08-readiness.md) with [ADR-0012](../02-design/adr/0012-readiness-triggers.md) |

If this page and one of those disagree, the source wins. Fix this page.

## 2. Milestones at a glance

| Milestone | Status | Scope in one line | Exit criteria |
|---|---|---|---|
| **M1: MVP, the core loop** | **Done**, 8 Nov 2026 (PR 8) | SDK, durable runtime on Postgres, JSON API and HTML dashboard, CLI and `feather dev`, generic connectors and templates, Compose | All met (§3.3) |
| **M1.1: brought forward** | **In progress.** Record trace done; flow packages being built (task 0, roles); then readiness and the Zakya connector | Everything brought forward from later milestones: record trace, flow packages, readiness, and a full Zakya connector | §4 |
| **M2: business connectors and operations** | Not started | SAP and Magento connectors with retail templates; on-prem agent; side effects and approvals; RBAC; alerts | §5 |
| **M3: enterprise and v1.0** | Not started | Environment fleet view, SSO, hardening, the v1.0.0 release. Multi-tenancy stays light (ADR-0013) | §6 |
| **M4: AI and integrations** | After v1.0 | MCP server and agent skills, Postgres CDC, metrics export, the `objects` spool, `feather diff`, SQL transforms, overrides and windows, SLA views, vaults | §7 |
| **M5: full multi-tenancy** | After M4 | Row-level security, per-tenant config and roles, quotas, worker pools; test environments as tenants | §8 |
| **Later** | Not scheduled | Sandbox refresh, Helm, Kafka, more connectors, marketplace, lineage, in-dashboard AI | §9 |

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
- **Dashboard** (PR 10): a page per version and the menu in a left sidebar. PR 10 also brought **record trace** forward; it now counts as M1.1 (§4).

### 3.5 Left out of M1 on purpose

`fc.reconcile` and H5; readiness (now M1.1); side-effect routes and approvals; overrides beyond pause and resume; RBAC; MCP; alerts; the budgets page; row-level security (M3); `sink.confirm` (M2, with Magento); handling after a backup restore (M3). See the [plan README](README.md#deferred-past-m1).

### 3.6 Known open items after M1

From the [exit report §5–§6](../06-release/m1-exit-report.md#5-open): pyright can't follow the namespace packages; OAuth2 tokens are shared per process, not across processes (M2); `feather dev` helpers on macOS; a lookback made longer later doesn't reach older versions; an `Idempotency-Key` race in the API; `feather new --template` loses YAML comments; a scheduler tick lost on a crash; the server image carries the demo's `erp-token.txt` (m1); `latest_watermark` locks needlessly (m2); `feather server` binds 0.0.0.0 by default (m3).

Also found since: `feather validate` misses duplicate route names across flows, and `feather run --sim` fails with signed webhook sources.

## 4. M1.1: brought forward from later milestones

M1.1 holds everything brought forward from M2 or later, after M1 closed (sponsor, 8 Nov 2026). Each piece keeps its own design and exit criteria.

| Piece | From | Status | Design |
|---|---|---|---|
| **Record trace** and the run page's **Records** panel | M2 | Done: PR 10 (`/trace`, migration 0013) and PR 15 (Records panel, migration 0014) | — |
| **Readiness** (`fc.ready`, `fc.on_ready`) | M2 (FR-B7, FR-D3) | Designed, awaiting review (ADR-0012 proposed) | [Readiness design](../02-design/2026-11-08-readiness.md) |
| **A full Zoho Zakya connector** (`feather-zakya`) | M2 (FR-J2) | Done: PR 21 (the live run is deferred, see below) | [Design §8.3–§8.4](../02-design/2026-10-30-feather-connect-design.md#83-v1-catalogue) |
| **Flow packages**, with the `admin` and `operator` roles | M2 (promotion, part of RBAC) | In progress: P1–P3 merged (PRs 20, 22, 23) | [Flow packages design](../02-design/2026-11-08-flow-packages.md) |

Order (sponsor, 8 Nov 2026): **flow packages first, then readiness.** The Zakya connector (added 9 Nov 2026) can go before, between or after them; it touches neither.

### Stages

M1.1 is built in stages. Each stage is one branch and one pull request: build, full test run, review, fixes, CI green, merge.

| Stage | Covers | Status |
|---|---|---|
| **P1** | Flow packages tasks 1–2: `feather package build`; the built-in registry at `/registry/simple/` with uploads, yanking and publish-only tokens; `feather package publish` and `search`; `feather registry add/list/test/remove` | Merged, PR 20 |
| **Z** | The Zakya connector (below) | Merged, PR 21 |
| **P2** | Tasks 3–4: installed-package tables; `feather package install/list/show/fetch` and the API; 11 named install checks; the load check in a child process | Merged, PR 22 |
| **P3** | Task 5a (environment config in the database, package prefixes, `feather config import`); connection settings visible and editable; encrypted secrets; webhooks declared in packages; task 5 (the scheduler reads active flows from the catalogue; `feather flow activate/deactivate`) | Merged, PR 23 |
| **U** | The dashboard redesign (§4.3) | Merged, with this document update |
| **P4** | Tasks 6–8: worker supervisor and package hosts; version switch and rollback; one Python environment per package, cached by lock hash | Not started |
| **P5** | Tasks 9–11: dashboard Packages pages and Activate/Deactivate; promotion qa → prod by sha256; uninstall; optional signing | Not started |
| **P6** | Task 12: retire the project mode; M1 environments upgrade in place | Not started |
| **R1–R3** | Readiness tasks 1–10, and hard cases H13, H16 and H17 (§4.2) | Not started |
| **D** | The design docs, the Operating Guide, the Feather Connect Guide, the code guide, and the M1.1 exit report | Not started |

Order (sponsor, 9 Nov 2026): P1, Z, P2, P3, U, P4, P5, P6, R1–R3, D. Stage U comes before any further dashboard work, so the Packages pages are built in the new layout.

### Decisions made while building (sponsor, 9–10 Nov 2026)

- **Connections in the database.** Settings that aren't secret (URL, accounts domain, organisation id, client id, budgets, timezone) are stored as plain values, visible and editable, and each change is audited with its old and new value. Secrets (client secret, refresh token, passwords, API keys) are encrypted with one master key per environment, kept outside the database. Once set, a secret can be replaced or removed but never shown again.
- **Webhooks in packages.** A package declares its webhooks in YAML: path, signature scheme (HMAC, Shopify, Stripe, Zoho, bearer), secret, key and stamp. The server's built-in receiver checks the signature, deduplicates and stores the raw event, and the package's own code parses it on workers. A package may not bring a webhook receiver written as code.
- **Dashboard layout.** The dashboard follows the layout of Docker Desktop's container list, not its branding. It is built in plain HTML and JavaScript, with a richer framework only if needed later.
- **The live Zakya test is deferred.** The connector is tested against a simulator and the conformance kit. The run against a real Zakya test organisation waits for the sponsor. It will also settle five open points: the eu host, which lists honour `last_modified_time`, the rate-limit codes, the `reference_number` filter on stock adjustments, and the sign-in lockout time.
- **The repository was renamed** to `dev-sukkur/feather-connect-pilot` on 9 Nov 2026.

### Zakya connector

**Scope:** a `feather-zakya` connector package in place of the demo's generic REST setup: Zoho OAuth with token refresh, the organisation id, the per-minute and per-day budgets, and pagination. Datasets: invoices, credit notes, payments, items and price lists, read by `last_modified_time` or in full. Writes: items, prices and stock. The retail template `zakya_invoices_to_erp` moves onto it.

**Exit criteria:** the conformance kit passes (met, PR 21); Zakya invoices → MotherDuck runs against a Zakya test organisation (discovery Q4: the organisation's daily API cap). *Deferred by the sponsor on 9 Nov 2026: run `uv run pytest packages/connectors/feather-zakya/tests/test_live.py -m live` with the variables in the connector's README.*

### 4.1 Flow packages

**Scope:**
- A flow package is a Python wheel built with `feather package build` in the customer's own repository.
- Packages come from a registry built into `feather-server`, on by default, at `/registry/simple/`. Company Python package indexes can be added beside it.
- Install checks the package and stores it in Postgres. Installed flows start inactive.
- Each flow is activated and deactivated per environment. Running runs finish on deactivate. Activating another version switches it, and rollback works the same way.
- Workers become supervisors with one package host process per active package version, so nothing restarts.
- Every name carries the package prefix (`acme-invoices/invoices`).
- Roles `admin` and `operator`, pulled forward from M2's RBAC.
- Packages are the only way to deploy. The M1 project mode is retired, and M1 environments upgrade in place.

**Plan:** tasks 0–12 in [design §14](../02-design/2026-11-08-flow-packages.md#14-plan).

**Exit criteria:** in a running Compose environment, with no container restarted, a customer installs a package built in another repository, activates a flow and sees it run; upgrades it, rolls it back and deactivates it; and another package keeps running throughout, with no run lost or doubled.

### 4.2 Readiness

**Scope:**
- Readiness marks written in the seal and apply transactions: how far each dataset partition is complete, and how far each route has applied it.
- `fc.ready(datasets, delivered_to=…, by=…)` and the trigger `fc.on_ready(ready, each="run" | "hour" | "day", at_most_every=…, on_late=…)`.
- Exactly one run per (flow, set, period, scope); late data reruns, warns or is ignored, per flow.
- A Readiness page, lines on the flow and run pages, *Fire now* and *Close partition*, the CLI and the JSON API.

**Plan:** tasks 1–10 in [design §10](../02-design/2026-11-08-readiness.md#10-plan).

**Exit criteria:** the Zakya chain runs with SAP and the warehouse starting only after an ingest run is in staging; a store-day posting waits for all three of its datasets, store by store; H13 passes, and new hard cases H16 (leader killed while firing) and H17 (late data reruns once) pass.

### 4.3 Dashboard redesign (stage U)

**Scope:**
- An app shell: dark theme first, with light too; a left sidebar of icons with labels; a top bar with a Ctrl+K command palette, help, theme and user menus; a bottom status bar showing server, database, workers, environment and version.
- Every list page follows one pattern: summary tiles with optional charts; a toolbar with search, a column chooser and an "only active" toggle; a dense table with select boxes, expandable rows, status dots, row actions and bulk actions; a "Showing N items" footer.
- Plain HTML and JavaScript on the server-rendered pages, with no framework, build step or CDN, and the strict CSP kept.

**Exit criteria:** accessibility checks report no violations in light or dark; every page works by keyboard; pages work at 360 px wide; screenshots are in `docs/04-plan/screenshots/`.

## 5. M2: business connectors and operations (~8 weeks)

**Scope (design §19):**
- **Connectors and templates:** SAP (OData, HANA read), Magento, and the retail templates. Zoho Zakya moved to M1.1. GoFrugal too if its API docs arrive in time, otherwise M3.
- **On-prem:** the agent and its gateway.
- **Side effects:** side-effect routes with compensate, approvals and holds.
- **Data rules:** schema policy; bootstrapping newly added routes from history. Derived datasets and SQL transforms moved to M4.
- **Operations:** budget priorities and a budgets page; RBAC (the two roles come earlier, in M1.1); audit; PII masking; version diff (record trace is in M1.1); alerts. Overrides, availability windows and SLA and anomaly views moved to M4.
- **Delivery:** promotion between environments comes with flow packages in M1.1 (`feather package promote`).

**Exit criteria:**
- The reference flows run in the reference organisation's qa/uat: Zakya invoices → MotherDuck, SAP prices → Zakya/GoFrugal/Magento, Magento orders → SAP with approvals.
- All hard cases, H1–H15, pass.

## 6. M3: enterprise and v1.0 (~8 weeks)

**Scope (design §19):**
- **Tenancy:** light, by [ADR-0013](../02-design/adr/0013-light-tenancy-and-non-prod-layout.md): the `tenant_id` foundation from M1 and the single `default` tenant. Full multi-tenancy is M5. Its eight rules apply to every change.
- **Environments:** environment peers and the fleet view; protections for protected environments. A Compose layout for the test environments on one machine, a database each (ADR-0013, layout B).
- **Integrations:** OIDC SSO. Vault secret providers, Postgres CDC and Prometheus/OpenTelemetry export moved to M4; v1 reads secrets from environment variables and files.
- **Hardening:** chaos and performance suites with published results; a security review.
- **Launch:** the docs site and the v1.0.0 release.

**Exit criteria:** NFR targets met; the release checklist complete; the reference organisation's in-scope flows cut over to prod.

## 7. M4: AI and integrations (after v1.0)

Moved out of v1 by the sponsor on 9 Nov 2026.

**Scope:**
- **AI:** the MCP server and agent skills (FR-L1); agent actions that change state in production create a hold for approval (FR-L3). `AGENTS.md` (FR-L2) shipped in M1.
- **Postgres CDC** as a source and an event trigger (FR-B2, FR-D2).
- **Prometheus and OpenTelemetry export** (FR-M2). The dashboard keeps working without them.
- **The `objects` spool backend** (S3-compatible, Azure Blob, GCS) for bulk extracts and long archives.
- **`feather diff --from qa --to prod`.**
- **Data rules:** derived datasets, SQL transforms on DuckDB and cached reference lookups (FR-C2, FR-C4, FR-C5).
- **Operations:** runtime overrides and connection availability windows (FR-G4, FR-D5); freshness/SLA and volume-anomaly views (FR-F7); vault and cloud secret providers (FR-K3).

**Exit criteria:** agent-driven recovery is shown in qa; a CDC dataset passes the hard cases that apply to push datasets; a 100-million-row extract runs on the `objects` spool.

## 8. M5: full multi-tenancy (after M4)

Moved here from "after v1" on 9 Nov 2026; [ADR-0013](../02-design/adr/0013-light-tenancy-and-non-prod-layout.md) keeps it cheap to add.

**Scope (FR-I2 to FR-I4):** the tenant chosen per request and per task; Postgres row-level security; per-tenant connections, secrets, budgets, quotas, alert channels, users and roles; dedicated worker pools; a tenant switcher; cross-tenant tests on every route; test environments as tenants of one non-prod deployment.

**Exit criteria:** cross-tenant tests pass on every API, HTML and MCP route; one tenant can't starve another of workers or budget.

## 9. Later, and not planned

From [discovery §5](../01-discovery/2026-10-30-feather-connect-discovery.md#5-scope), plus **sandbox refresh** from [ADR-0013](../02-design/adr/0013-light-tenancy-and-non-prod-layout.md) (`feather env refresh` with a masked sample or a full masked copy of production).

| Later (after 1.0) | Not planned |
|---|---|
| Helm chart and Kubernetes; Kafka or Redpanda as an optional transport; a visual data-mapping assistant; an in-dashboard AI assistant; more connectors (Salesforce, Dynamics, NetSuite, Snowflake, BigQuery, Kafka, SFTP, Excel); a community connector marketplace; column-level lineage | A visual flow designer; a single-page JavaScript front end; hosted SaaS; an API gateway or developer portal; full BPM |

## 10. What the IDs mean

**Read these with their context:**
- **SP** is a *spike* (SP1–SP4). Spikes were first numbered S1–S4 and were renamed on 8 Nov 2026, so an old commit, branch or note may still say "spike S2".
- **S** is a *Stage 3 review finding* (S1–S23), in commits, the plan README and the exit report.
- **M** is a *milestone* (M1–M3), and also a *Minor review finding* within one review ("Stage 2 M13", "Stage 4 M6"). The exit report writes Minor findings in lower case (m1–m4).

| ID | Meaning | Defined in |
|---|---|---|
| M1–M5, M1.1 | Milestones; M1.1 is what was brought forward from later ones; M4 and M5 come after v1.0 | Design §19; §2 of this page |
| FR-A1 … FR-M3 | Functional requirements, by area: A authoring, B data movement, C transformation, D triggers, E reliability, F–G dashboard and operations, H environments, I tenancy, J connectors, K security, L AI, M observability | Discovery §7 |
| NFR-01 … NFR-15 | Non-functional requirements (performance, durability, footprint, security, accessibility and others) | Discovery §8 |
| Q1–Q11 | Open questions for the sponsor | Discovery §15 |
| D1–D8 | Open design questions | Design §20 |
| H1–H17 | Hard cases: scenarios every milestone must pass (M1: H1–H4, H6, H14; M1.1: H13, H16, H17; M2: all) | Design §16 (H16 and H17: readiness design §11); first run in spike SP2 |
| SP1–SP4 | Spikes: SP1 engine (DBOS chosen), SP2 core model, SP3 connector research, SP4 Postgres spool | [Spikes](../03-spikes/README.md) |
| 1–6 (plans), `2.3` (tasks) | The six M1 plans; `2.3` is Plan 2, Task 3 | [Plan README](README.md) |
| Stage 1–5 | The order M1's tasks were built in | Plan README, *Build order* |
| F1–F15 | Stage 2 review findings | §10.1 below |
| S1–S23 (findings) | Stage 3 review findings | §10.2 below |
| B1–B6, rulings 1–12 | Stage 4 preflight: blockers, and binding rulings | §10.3 below |
| I1–I4, m1–m4 | End of M1 review (Stage 5): Important and Minor findings | [Exit report §6](../06-release/m1-exit-report.md#6-end-of-stage-review) |
| ADR-0001 … 0013 | Architecture decision records | [ADR index](../02-design/adr/README.md) |

The review notes behind §10.1–§10.3 are kept outside the repository, so their findings are summarised here.

### 10.1 Stage 2 review findings (F1–F15): all closed

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

### 10.2 Stage 3 review findings (S1–S23)

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

### 10.3 Stage 4 preflight (B1–B6 and rulings)

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

## 11. Numbering from now on

To stop the clashes in §10, new review findings carry their review's name: `PKG-R1-I3` is the third Important finding of the first flow packages review. Milestones keep M1–M3, spikes use SP1, SP2 and so on, and the other IDs above stay as they are.
