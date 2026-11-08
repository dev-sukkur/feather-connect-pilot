# Feather Connect — Discovery Document

> SDLC phase 1 of 7 · Version 0.3 (draft for review) · 30 October 2026
> Owner: OSS Dev1 · Prepared with Claude Code
> Next: [Design specification](../02-design/2026-10-30-feather-connect-design.md) · [Decision records](../adr/README.md)

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-10-30 | OSS Dev1, with Claude Code | First draft from the discovery Q&A and the reference evaluation |
| 0.2 | 2026-10-30 | OSS Dev1, with Claude Code | Sponsor direction: HTML-only front end; Postgres as the database (object storage optional) |
| 0.3 | 2026-10-30 | OSS Dev1, with Claude Code | Phase 3 results: engine decided (DBOS); vendor facts re-verified; GoFrugal is now "M2 if docs arrive, otherwise M3"; open questions updated |

Review status: **awaiting sponsor review.** Approving this document closes the discovery gate (§16).

---

## 1. Summary

Feather Connect is an open-source (MIT) integration hub written in Python. It moves data from one source to many destinations in near real time, runs ETL, and connects business applications, all with **one data model** and **one dashboard**.

Flows are plain Python code. Developers and AI assistants write, review, test and run them like any other code. Operators and AI agents monitor and act on every flow from one dashboard (or the CLI, or an MCP server): re-run a business slice such as "store 1509, 18 October", replay history to a new destination, approve a posting, pause a route. This works across dev, qa, uat and prod, for one or many tenants. Feather Connect runs with one command on a laptop and needs only Postgres in production. Object storage is optional, for very large volumes or long-term archives. The dashboard is plain server-rendered HTML, with no JavaScript framework and no Node build.

It is both a general-purpose open-source product and the integration platform of a **reference organisation**: a retailer running SAP S/4HANA, Zoho Zakya and GoFrugal point-of-sale, Magento e-commerce and a DuckDB/MotherDuck warehouse. That organisation is the first production deployment and the source of the hardest test cases.

## 2. Problem

### 2.1 In the market

- **Integration work is split across tool families that each solve one part.**
  - Commercial iPaaS tools (MuleSoft, Boomi, Workato, Zapier, Make) are strong on connectors and mapping screens. They are expensive, closed and weak at replaying history.
  - ELT tools (Airbyte) move tables in batches but don't handle application events or postings.
  - Streaming stacks (Kafka Connect, Debezium) are powerful but heavy to run.
  - Durable-workflow engines (Temporal, DBOS, Hatchet) make code reliable but have no data model or connectors.
  - Orchestrators (Dagster) are excellent for batch assets but not for low-latency events.
- **Teams stitch together two to four of these tools with scripts.** Monitoring ends up scattered across their UIs. Recovery means CLI commands and SQL written by the few people who built the pipelines.
- **Visual low-code flows are hard to diff, review and test,** and hard for AI to generate reliably. Code is what both developers and AI handle best.
- **Open-source options are partial.** Some are batch-only, some event-only, some heavy to run, and some are source-available rather than open-source.

### 2.2 In the reference organisation

The reference evaluation (Appendix B) inventoried the organisation's ERP, POS and HR pipelines. Recurring problems:

- **Workflow-engine features had been rebuilt by hand:** locks that disagree, a home-grown job queue, hand-built rate governors, polling loops and hand-run recovery scripts. A scheduled run could be skipped without any record.
- **Work can't be addressed by business slice.** Tools think in rows and cursors, so "backfill store 1509 for 18 October" is not possible.
- **Reading and writing are coupled.** If the warehouse is down during the ERP's night read window, that night's read is lost.
- **Some sources are awkward:**
  - history tables with no change stamp need whole-slice diffs;
  - some sources hard-delete rows;
  - some rewrite a record's key in place;
  - some rows become visible late.
- **Header and line feeds are coupled,** so a missing header blocks lines from moving on.
- **Vendor API call caps are uncoordinated.** One daily cap is shared by the readers and writers of the same vendor, and nothing coordinates them.
- **Operations are CLI-driven,** and every change or recovery depends on one reviewer.

## 3. Vision, goals and non-goals

**Vision.** Write a flow in Python, or ask an AI to. Run it anywhere with one command. Operate every flow, across environments and tenants, from one dashboard where humans and agents see and act on the same truth.

| ID | Goal | We'll know it's met when |
|---|---|---|
| G1 | One model for ETL and application integration | Batch extracts, webhook events and API postings are all built from the same SDK concepts. The reference organisation needs no second integration product. |
| G2 | Flows are code that developers and AI can write, review and test | On a benchmark task set, ≥ 80% of AI-generated flows (from a template) pass `feather validate` and their tests without manual fixes |
| G3 | One dashboard to monitor **and act** | Operators recover failed deliveries, re-run business slices and replay history without CLI or SQL |
| G4 | Easy to run and maintain | `feather dev` reaches a working demo in ≤ 5 minutes. Production needs only Postgres, plus the engine container if the chosen engine needs one. The dashboard needs no Node or JavaScript build. |
| G5 | Safe multi-environment and multi-tenant operation | dev/qa/uat/prod run from the same images with config overlays. Tenant data is isolated by database policy. |
| G6 | Batteries included | v1.0 ships the connector catalogue and flow templates in FR-J2 and FR-A7 |
| G7 | AI can operate the hub safely | An agent holding an MCP token can investigate and fix a failed delivery in qa. In prod it can only *request* state-changing actions, which a human approves. |

**Non-goals for v1.0:**
- a visual drag-and-drop flow designer;
- sub-second, high-volume streaming (Kafka-class);
- a hosted SaaS offering;
- replacing a data warehouse, dbt or BI tools;
- API management (developer portal, rate plans, gateway);
- general BPM or human-workflow beyond approvals;
- a Kubernetes operator.

## 4. Stakeholders and personas

| Persona | Who | Needs | Main surfaces |
|---|---|---|---|
| P1 Integration developer | Writes and reviews flows and connectors in Python, often with an AI assistant | Fast local loop, simulated sources, clear errors, tests, templates | SDK, CLI, IDE + AI, dashboard |
| P2 Operator | Support and operations staff, including junior staff | Clear health picture, safe one-click recovery, knobs without code | Dashboard |
| P3 Platform admin | Installs and upgrades environments | Simple deployment, tenants, users, secrets, budgets, upgrades | CLI, dashboard admin, compose |
| P4 AI agent | Claude Code, Copilot or a custom agent | Typed SDK, machine-readable CLI, MCP tools with clear permissions | SDK, CLI `--json`, MCP |
| P5 Business owner | Owns a process (sales posting, pricing) | Freshness, readiness, approval requests, SLA breaches | Dashboard, alerts |
| P6 OSS contributor | Builds connectors and templates | Connector SDK, conformance kit, contribution guide | SDK, repo |

Stakeholders: the sponsor and product owner (OSS Dev1), the reference organisation's integration, data and ERP teams, and the future open-source community.

## 5. Scope

| In v1.0 | Later (post-1.0) | Not planned |
|---|---|---|
| Python SDK, CLI, control plane, server-rendered HTML dashboard, MCP server | Helm chart / Kubernetes | Visual flow designer; single-page JavaScript front end |
| Durable engine behind a port (DBOS Transact, ADR-0003) | Kafka/Redpanda as optional transport | Hosted SaaS |
| One-to-many delivery, ETL transforms, webhook events, Postgres CDC | Visual data-mapping assistant | API gateway / developer portal |
| Partitions, versions, replay, backfill, readiness | In-dashboard AI assistant | Full BPM |
| dev/qa/uat/prod deployments; the tenant column, with one tenant (ADR-0013) | More connectors (Salesforce, Dynamics, NetSuite, Snowflake, BigQuery, Kafka, SFTP, Excel) | |
| On-prem agent (outbound-only) | Marketplace for community connectors | |
| Connectors and templates listed in FR-J2 and FR-A7 | Column-level lineage | |

## 6. Key use cases

| ID | Use case | Example from the reference organisation | Persona | Milestone |
|---|---|---|---|---|
| UC-01 | Replicate a database table incrementally into a warehouse | Postgres table → MotherDuck bronze | P1 | M1 |
| UC-02 | Pull a SaaS API into the warehouse within a shared daily call budget | Zakya invoices (list + detail) → MotherDuck bronze | P1, P2 | M2 |
| UC-03 | Fan out one source to many destinations | SAP prices → Zakya, GoFrugal and Magento | P1 | M2 |
| UC-04 | Receive webhooks and deliver to an API within seconds | Magento order → SAP sales order | P1 | M1 (generic), M2 (SAP) |
| UC-05 | Load files dropped in object storage | S3 CSV/Parquet → Postgres | P1 | M1 |
| UC-06 | Reverse ETL: diff a warehouse view and push only the changes | Today's effective price → POS | P1 | M2 |
| UC-07 | Post sales to the ERP with approval and compensation | Store-day POS sales → SAP postings | P1, P5 | M2 |
| UC-08 | Re-run or backfill a business slice from the dashboard | Re-run "store 1509, 18 Oct" | P2 | M1 |
| UC-09 | Replay history to a newly added destination | Add GoFrugal as a third price channel | P2 | M1 |
| UC-10 | Diagnose and fix a failed delivery; trace a record by business key | "Where is invoice INV-1001?" | P2, P4 | M1 (fix), M2 (trace) |
| UC-11 | Generate a flow with AI, validate it, run it on simulated data, promote it via CI | New supplier feed | P1, P4 | M1 |
| UC-12 | Agent operates via MCP within its permissions | Agent investigates overnight failures and requests a re-run | P4 | M2 |
| UC-13 | Onboard a tenant with its own connections, budgets and users | A franchise group | P3 | M3 |
| UC-14 | Promote a flow dev → qa → uat → prod | Release 1.4 of the price flow | P1, P3 | M2 |
| UC-15 | Simulate a source for demos and QA, including fault injection | JSON simulation with duplicates, late rows and 429s | P1 | M1 |

## 7. Functional requirements

Priorities use MoSCoW. Milestones: **M1** MVP, **M2** business connectors and operations, **M3** enterprise and v1.0 (design §19).

### A. Authoring

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-A1 | Flows are Python modules built with the `feather_connect` SDK. There is no proprietary DSL, XML or visual format. | Must | M1 |
| FR-A2 | A dataset is declared by its source, key, partition scheme, change strategy and optional schema. Behaviour beyond the defaults is added through Python hooks (custom planner, diff, mapping, sink), not configuration flags. | Must | M1 |
| FR-A3 | `feather new flow` and `feather new connector` scaffold code, config and tests, optionally from a template. | Must | M1 |
| FR-A4 | `feather run --local` runs a flow in-process against in-memory or simulated connectors. | Must | M1 |
| FR-A5 | `feather validate` checks a project statically (references, budgets, partition schemes, schemas, config) and returns machine-readable results. | Must | M1 |
| FR-A6 | A test harness (`feather_connect.testing`) lets flow tests load source records, run the flow, and assert sink outputs, ledger states and holds. | Must | M1 |
| FR-A7 | Templates for well-known flows can be instantiated and then edited freely (design §8.6). | Must | M1 generic, M2 retail |

### B. Data movement

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-B1 | One dataset can be delivered to any number of destinations ("routes"). Each route progresses, fails and recovers independently. | Must | M1 |
| FR-B2 | Change strategies, plus custom strategies via a hook:<br>• `by_stamp`: incremental, with lookback;<br>• `snapshot_diff`: hashes and diffs a slice, and detects hard deletes;<br>• `replace`: reloads the full slice;<br>• `push`: webhook or CDC. | Must | M1 (CDC M4) |
| FR-B3 | Partitions name business slices, such as store × day with an explicit timezone. They are the unit of planning, re-runs, verification and backfills. | Must | M1 |
| FR-B4 | Each extraction produces an immutable version: upserts plus deletes, marked complete or incomplete. Versions are retained according to policy. | Must | M1 |
| FR-B5 | Extraction never waits for destinations. Data is spooled first, so a destination outage cannot lose a read. | Must | M1 |
| FR-B6 | A route applies a partition's versions in order, and push datasets keep per-key order. As an opt-in, a whole-document event can supersede an older failed version. | Must | M1 |
| FR-B7 | Readiness ("this dataset or group is complete through T") is tracked and can gate or trigger downstream flows. It never blocks unrelated routes. | Should | M1.1 |
| FR-B8 | Any retained version can be replayed to a route, and a newly added route can be bootstrapped from history. | Must | M1 replay, M2 bootstrap |
| FR-B9 | Schema evolution policy: additive changes apply automatically. Drops, narrowing and type changes are held for approval. The policy can be overridden per dataset. | Should | M2 |
| FR-B10 | Event latency from webhook receipt to the destination being updated is ≤ 30 s at p95 (target 5 s) under normal load. | Must | M1 |

### C. Transformation (ETL)

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-C1 | Per-route record-level map and filter functions, and batch transforms over Arrow or Polars. | Must | M1 |
| FR-C2 | SQL transforms over versions, using embedded DuckDB. | Should | M2 |
| FR-C3 | Validation contracts (Pydantic or Arrow schema). Invalid records are quarantined with reasons while the rest of the version proceeds; this behaviour is configurable. | Must | M1 |
| FR-C4 | Derived datasets whose source is a query over other datasets or the warehouse (for example, today's effective price), recomputed on a schedule. | Should | M2 |
| FR-C5 | Cached reference lookups for enrichment. | Could | M2 |

### D. Triggers and scheduling

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-D1 | Triggers: cron with a timezone, fixed intervals (down to seconds, for pollers), and manual runs from the dashboard, CLI or MCP. | Must | M1 |
| FR-D2 | Event triggers: webhook arrivals (M1) and database CDC (M4). | Must | M1 / M4 |
| FR-D3 | Readiness triggers: run when a dataset group is complete through a period. | Should | M1.1 |
| FR-D4 | Backfill plans over a partition range, with concurrency and budget limits, previewed before execution. | Must | M1 |
| FR-D5 | Pause and resume per flow and per route. Connection availability windows (for example, ERP reads only 22:00–05:00). | Should | M1 pause/resume, M2 windows |

### E. Reliability

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-E1 | Durable execution: killing any process loses no acknowledged work. | Must | M1 |
| FR-E2 | Retries with backoff and jitter (honouring `Retry-After`), step timeouts and heartbeats. | Must | M1 |
| FR-E3 | Effectively-once delivery per (route, version), through a ledger check plus idempotent sinks (natural keys, idempotency keys or content guards). | Must | M1 |
| FR-E4 | No silent failures. Every scheduled tick and every delivery is recorded, including skips and their reasons. | Must | M1 |
| FR-E5 | Side-effect sinks implement `apply` and `compensate`. Postings can require approval by policy. | Should | M2 |
| FR-E6 | Named rate budgets shared across flows and routes, with multiple windows (per second up to per day) and priority reserves. | Must | M1 windows, M2 priorities and UI |
| FR-E7 | Concurrency control: one writer per (route, partition) by default, configurable parallelism, and fairness between tenants. | Must | M1 |

### F. Dashboard: monitor

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-F1 | Health overview: failures, stale datasets, budget use and worker status (M1); pending holds, alerts and agent status once those features arrive (M2). | Must | M1 |
| FR-F2 | Flow catalogue and detail: a graph from sources through routes to destinations, the effective config and where each value came from, and the code version. | Must | M1 |
| FR-F3 | Partition grid: the status of every dataset × partition × route over time, with drill-down. | Must | M1 |
| FR-F4 | Runs and tasks: timeline, inputs, logs and attempts. | Must | M1 |
| FR-F5 | Delivery ledger, and failures grouped by error. | Must | M1 |
| FR-F6 | Record trace by business key across versions and routes; version diffs; masked samples. | Should | M1.1 trace; M2 diffs and masked samples |
| FR-F7 | Freshness/SLA and volume-anomaly views. | Should | M2 |
| FR-F8 | A fleet view across registered environments. | Should | M3 |
| FR-F9 | Audit log view. | Must | M2 |

### G. Dashboard: act

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-G1 | Run now, re-run partitions, backfill, cancel. | Must | M1 |
| FR-G2 | Retry, skip, quarantine and replay deliveries, singly or in bulk, with an impact preview before execution. | Must | M1 |
| FR-G3 | Approve or reject holds, with a reason. | Should | M2 |
| FR-G4 | Runtime overrides of operational knobs (pause, budgets, concurrency, schedules, availability windows), each with a reason, an audit record and an optional expiry. Drift from git is shown. | Should | M2 |
| FR-G5 | Test a connection. | Must | M1 |

### H. Environments

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-H1 | dev, qa, uat and prod are separate deployments of the same images, configured by per-environment overlays. | Must | M1 |
| FR-H2 | Promotion by git tag and CI. Each environment shows the code version every flow runs. | Must | M1.1, as flow package promotion by checksum (ADR-0011) |
| FR-H3 | Production protections: a preview and a reason for mass actions, approval for destructive actions, and least-privilege defaults. | Should | M3 |
| FR-H4 | The dashboard can register several environments and switch between them. | Should | M3 |

### I. Multi-tenancy

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-I1 | Every operational record carries a tenant. Single-tenant installs use an implicit default tenant. | Must | M1 |
| FR-I2 | Per-tenant connections, secrets, budgets, quotas, alert channels, users and roles. | Should | M5 (ADR-0013) |
| FR-I3 | Tenant isolation is enforced in the database (row-level security) as well as in the API. | Must, once there is more than one tenant | M5 (ADR-0013) |
| FR-I4 | Optional dedicated worker pools or agents per tenant. | Could | M5 (ADR-0013) |

### J. Connectors

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-J1 | Connector SDK: source and sink protocols, declared capabilities and config schema, plus `check` and `discover`. | Must | M1 |
| FR-J2 | Connector catalogue:<br>• **M1:** in-memory, JSON simulation, Postgres, S3-compatible storage, REST, GraphQL, webhook, HTTP, DuckDB/MotherDuck;<br>• **M2:** SAP, Zoho Zakya, Magento, and GoFrugal if its API docs arrive by M1's end (otherwise M3);<br>• **M3:** Postgres CDC. | Must | M1–M3 |
| FR-J3 | A conformance kit that every connector must pass to be labelled beta or stable. | Must | M1 |
| FR-J4 | Connectors install as optional packages or extras. Third-party connectors work without forking. | Must | M1 |

### K. Security

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-K1 | Local users and API tokens (M1); OIDC single sign-on (M3). | Must | M1 / M3 |
| FR-K2 | Role-based access (viewer, operator, developer, admin, plus approver and PII-reader grants), scoped by tenant and environment. | Must | M1.1 admin and operator; M2 the rest |
| FR-K3 | Secrets are referenced, never stored. Environment variables and files are supported in M1; Vault and the AWS, Azure and GCP secret stores in M3. Secrets are never logged or displayed. | Must | M1 / M3 |
| FR-K4 | An immutable audit record for every action by a human, an agent or the system. | Must | M2 |
| FR-K5 | PII fields are tagged and masked in dashboard, CLI and MCP output. | Should | M2 |
| FR-K6 | Webhook signature verification and replay protection. | Must | M1 |

### L. AI operability

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-L1 | An MCP server exposes read, act and authoring tools under the same RBAC and audit as the API. | Must | M4 |
| FR-L2 | AI-ready projects: AGENTS.md conventions, JSON output from every CLI command, and connector descriptions that agents can query. | Must | M1 |
| FR-L3 | In production, an agent action that changes state creates a hold for human approval unless policy allows it. | Should | M4 |

### M. Observability

| ID | Requirement | Priority | Milestone |
|---|---|---|---|
| FR-M1 | Structured logs carrying correlation ids (tenant, flow, run, task, dataset, partition, version, route). | Must | M1 |
| FR-M2 | Optional export of Prometheus metrics and OpenTelemetry traces. The dashboard works without them. | Should | M4 |
| FR-M3 | Alert rules (failures, staleness, missing partitions, budgets, volume anomalies, waiting holds, agent down) sent to email, Slack, Teams and webhooks. | Must | M2 |

## 8. Non-functional requirements

Targets are validated by the suites named in the last column (design §16). The reference deployment sizes are defined in design §18.4.

| ID | Category | Requirement and target | Verified by |
|---|---|---|---|
| NFR-01 | Latency | Webhook to destination updated: p95 ≤ 30 s (target ≤ 5 s) at 50 events/s per environment on the *small* deployment | Performance suite |
| NFR-02 | Throughput | ≥ 20,000 rows/s per worker from source batch to spool, excluding source-side limits | Performance suite |
| NFR-03 | Capacity | ≥ 5 million records/day per environment on the *small* deployment | Performance suite |
| NFR-04 | Durability | Zero lost and zero duplicated deliveries under kill -9 of any process, a Postgres restart, an object-store outage or an agent disconnect | Chaos suite |
| NFR-05 | Recovery | A control-plane restart costs a running task at most one retry. RPO ≤ 5 min with Postgres point-in-time recovery; RTO ≤ 1 h on the *small* deployment. | Chaos suite, restore drill |
| NFR-06 | Runnability | From a clean install, `feather dev` shows a demo flow in the dashboard in ≤ 5 min. A Compose environment uses ≤ 4 containers. | Release checklist |
| NFR-07 | Footprint | The only required stateful dependency is Postgres ≥ 15, with no extensions, so any managed Postgres works. The engine container is needed only if the chosen engine requires one. Object storage (S3-compatible, Azure Blob, GCS) is optional. | Architecture review |
| NFR-08 | Maintainability | Python ≥ 3.12; fully typed SDK (pyright strict); ruff; coverage ≥ 85% for the SDK core and ≥ 70% overall; single-purpose modules | CI gates |
| NFR-09 | Compatibility | Semver for the SDK and API from 1.0. Deprecations survive ≥ 2 minor releases. DB migrations are forward-only. | Release process |
| NFR-10 | Security | Control plane targets OWASP ASVS level 2. No known critical or high vulnerabilities at release. SBOM and signed images. | Security review, CI scans |
| NFR-11 | Usability | An operator recovers a failed delivery (find, understand, retry or replay) in ≤ 5 clicks, without CLI or SQL | Usability test |
| NFR-12 | Portability | Images for linux/amd64 and arm64. Development on Windows (WSL2 / Docker Desktop), macOS and Linux. | CI matrix |
| NFR-13 | Accessibility | Core dashboard screens meet WCAG 2.1 AA | Automated axe checks |
| NFR-14 | Documentation | Every connector and template has reference docs and a runnable example that is tested in CI | Docs CI |
| NFR-15 | Cost | The smallest supported production deployment runs on one VM plus object storage, with no paid licence | Architecture review |

## 9. Market and feature comparison

### 9.1 Capability comparison by tool family

This is an indicative assessment for positioning. Verify each cell against current vendor documentation before publishing it externally.

● = strong · ◐ = partial or paid tier · ○ = weak or absent

| Capability | Commercial iPaaS (MuleSoft, Boomi, Workato) | Automation (Zapier, Make, n8n) | ELT (Airbyte) | Kafka Connect + Debezium | Apache Camel | Durable engines (Temporal, DBOS, Hatchet) | Dagster | Windmill | **Feather Connect (target)** |
|---|---|---|---|---|---|---|---|---|---|
| Code-first flows that AI can generate | ◐ | ○ | ◐ | ◐ | ● | ● | ● | ● | ● |
| Bulk ETL and app events in one model | ◐ | ○ | ○ | ◐ | ◐ | ○ | ○ | ○ | ● |
| Business-slice partitions (re-run, verify, backfill) | ○ | ○ | ○ | ○ | ○ | ○ | ● | ○ | ● |
| Replay history to a new destination | ◐ | ○ | ◐ | ● | ○ | ○ | ◐ | ○ | ● |
| Vendor rate budgets shared across flows | ◐ | ○ | ◐ | ○ | ◐ | ◐ | ◐ | ◐ | ● |
| Ops dashboard that can act, not just observe | ● | ◐ | ◐ | ○ | ○ | ◐ | ● | ● | ● |
| On-prem agent, outbound connections only | ● | ◐ | ◐ | ◐ | ● | ● | ◐ | ◐ | ● |
| Easy to self-host | ○ | ◐ | ◐ | ◐ | ● | ● | ◐ | ● | ● |
| Licence | Commercial | Commercial; n8n fair-code | ELv2 platform, MIT connectors | Apache-2.0 | Apache-2.0 | MIT (all three) | Apache-2.0 + paid cloud | AGPL + Enterprise | MIT |

### 9.2 Engine shortlist (from the reference evaluation)

These facts were observed on vendor pages and in documentation during the reference evaluation (October 2026). The ones that still affect the decision were re-verified on 30 Oct 2026 (ADR-0003).

| Product | Relevant facts | Verdict for Feather Connect |
|---|---|---|
| Hatchet | MIT; self-hosted engine and UI are free; needs only Postgres (RabbitMQ optional); native global rate limits shared across workflows (per second up to per year); concurrency keys; event fan-out; webhook ingest with signature checks; workers dial out over gRPC. Engine still pre-1.0 at the time. 4 MB payload cap. A worker that misses heartbeats for 30 s has its work re-queued. | Tested, and kept on a side branch (ADR-0003) |
| DBOS Transact | MIT library on our own Postgres, no extra server; queues with concurrency limits. Its rate limits count workflow starts, not API calls. No native fan-out or webhooks. The Conductor UI is paid (Pro $99/mo without alerting or RBAC; Teams $499/mo with them). Deploys pin workflow versions, so workflows must be kept short. | Chosen (ADR-0003). Its paid UI matters less because we build our own dashboard. |
| Windmill | Best-in-class UI and in-browser editing. Concurrency limits, agent workers, restart-from-step, critical alerts and audit logs are Enterprise features (from about $120/mo). AGPL. | Not chosen: AGPL core, and the key features are paid |
| Dagster | Best partition grid and lineage view. No low-latency webhook fan-out, per-key ordering or time-window rate limits. Dagster+ cost at store × day partitions was estimated at about $6.3k–10.5k/mo. | Not chosen, but its partition grid inspires ours |
| Temporal | Mature, MIT. Self-hosting means running several services. | Not shortlisted: too heavy to run for the "easy to run" goal |
| Pipedream | Its workflow product announced a shutdown for 31 March 2027. | Eliminated |
| Encore (Python) | Pre-release; no durable workflows; self-hosting loses the UI and cron; pub/sub has no Postgres option. | Eliminated |

### 9.3 Ideas we borrow

| Idea | Taken from |
|---|---|
| Connector catalogue, environments, managed connections | Commercial iPaaS |
| Connector spec, `check`/`discover`, catalogue-driven configuration | Airbyte |
| CDC event semantics (before/after, tombstones) | Debezium |
| Enterprise-integration-pattern vocabulary (router, splitter, aggregator, throttler) | Apache Camel |
| Durable steps, retries, idempotency, sagas | Temporal, DBOS, Hatchet |
| Partition status grid, asset lineage | Dagster |
| Operator-friendly runs page with bulk re-run | Windmill |
| Template gallery for common flows | n8n |

### 9.4 Where Feather Connect differs

1. **One model for ETL and events.** Dataset → record → partition → version → route covers a nightly ERP extract and a webhook posting alike.
2. **Partition-native operations.** Re-run, verify and backfill use business words ("store 1509, 18 October"), not row cursors.
3. **Retained, replayable versions.** Any destination, including one added later, can catch up from history.
4. **Shared vendor budgets with priority reserves,** so sales reads keep their share while master-data writes wait.
5. **Code-first, plus a dashboard that acts.** AI and humans operate through the same API, permissions and audit.
6. **Boring to run.** Postgres is the only required database; one command locally; HTML dashboard with no front-end build; MIT licence.

## 10. Lessons carried from the reference evaluation

| Observed | Requirement it creates |
|---|---|
| Engine features had been hand-built (locks, queues, rate governors, recovery scripts), and runs could vanish silently | Durable engine and no silent skips (FR-E1, FR-E4) |
| Backfills and re-runs could not target a slice like store × day | Partitions as the unit of work (FR-B3, FR-G1) |
| A warehouse outage during the ERP's night read window lost the read | A spool between extract and delivery (FR-B5) |
| Stampless history tables, hard deletes, keys rewritten in place, late-visible rows | `snapshot_diff`, explicit business keys, lookback and scheduled reconcile (FR-B2, FR-A2) |
| Header and line feeds were coupled, so a missing header blocked lines | Documents as records; readiness gates only where explicitly configured (FR-B7) |
| A record could land under two dates after its document date was edited | Records are the storage unit and partitions are an index, so a move is just an update (design §4.4) |
| One vendor API cap was shared by readers and writers without coordination | Shared budgets with priority reserves (FR-E6) |
| A single high-water mark hid gaps; duplicates were resolved by load time | A ledger of applied versions; dedupe by source version (FR-E3) |
| ERP postings needed a duplicate guard keyed on content | Side-effect sinks with an idempotency guard and `compensate` (FR-E5) |
| A future-dated price becomes valid on a date with no change at the source | Derived datasets recomputed on a schedule (FR-C4) |
| Operations were CLI-driven and depended on one reviewer | A dashboard that acts, knobs in config, and RBAC (FR-F, FR-G, FR-K2) |
| The engine UIs evaluated were paid, partial or engine-specific | Our own dashboard over our own data (ADR-0002) |

## 11. Decisions taken in discovery

| Topic | Decision | Record |
|---|---|---|
| Language | Python throughout: SDK, runtime, server and dashboard | [ADR-0001](../adr/0001-python-throughout-html-dashboard.md) |
| Front end | Server-rendered HTML (Jinja2 templates, CSS, HTMX for partial refresh); no SPA framework, TypeScript or Node build | [ADR-0001](../adr/0001-python-throughout-html-dashboard.md) |
| Architecture | Our own SDK, control plane and dashboard; engine behind a port | [ADR-0002](../adr/0002-own-control-plane-engine-behind-port.md) |
| Engine | DBOS Transact, chosen by a conformance spike | [ADR-0003](../adr/0003-durable-engine-selection-by-spike.md) |
| Data model | Dataset, record, partition, version, route ledger for both ETL and events | [ADR-0004](../adr/0004-unified-data-model.md) (proposed, confirm at design review) |
| Database | Postgres is the only required database, holding the spool too; object storage optional; no Kafka in v1 | [ADR-0005](../adr/0005-postgres-only-required-database.md) |
| Environments and tenancy | An environment is a deployment; a tenant is a namespace | [ADR-0006](../adr/0006-environment-is-deployment-tenant-is-namespace.md) |
| Authoring | Full-code flows; knobs via config and audited overrides; no visual designer in v1 | [ADR-0007](../adr/0007-full-code-flows-no-visual-designer.md) |
| AI | An MCP server with the same RBAC and audit as humans; no in-product LLM in v1 | [ADR-0008](../adr/0008-ai-operability-via-mcp.md) |
| Licence | MIT | [ADR-0009](../adr/0009-mit-licence.md) |
| Deployment | Docker Compose plus `feather dev`; Helm later | [ADR-0010](../adr/0010-compose-and-feather-dev-deployment.md) |
| Latency and volume | Seconds of latency, moderate volume | ADR-0005 |
| v1 connectors | In-memory, JSON simulation, Postgres, S3, REST/GraphQL, webhook, HTTP, SAP, Zoho Zakya, GoFrugal, Magento, DuckDB/MotherDuck | FR-J2 |

## 12. Assumptions and constraints

**Assumptions:**
- **A1.** Volume per environment starts at ≤ 5 million records/day. Most documents are < 1 MB.
- **A2.** "Near real time" means seconds (≤ 30 s p95), not sub-second.
- **A3.** Every environment can run Postgres ≥ 15, managed or in a container.
- **A4.** Postgres can hold the spool at the volumes in A1, with about 14 days of retention. Object storage is added only for bulk-heavy datasets or long archives.
- **A5.** Developers are comfortable with Python and git. Operators use only the dashboard.
- **A6.** Vendor API limits (such as Zoho's daily call caps) are hard constraints, and they are budgeted rather than worked around.
- **A7.** DBOS Transact is MIT-licensed and self-hostable without a licence fee (verified in ADR-0003).

**Constraints:**
- **C1. MIT licence.** Distributed packages must not depend on copyleft (GPL, AGPL, SSPL) code.
- **C2. Proprietary drivers are user-installed.** SAP's HANA client and NetWeaver RFC SDK are not redistributable. They are optional extras that the user installs.
- **C3. No Kubernetes requirement.** Everything must run without Kubernetes.
- **C4. Outbound-only from customer sites.** Sites behind corporate firewalls connect out only (on-prem agent).
- **C5. Python 3.12+.**
- **C6. Postgres is the database.** It is the only required one, and needs no extensions. No Redis, Kafka, SQLite or second database.
- **C7. HTML-only front end.** The dashboard is server-rendered HTML with no JavaScript framework, TypeScript or Node toolchain.

## 13. Risks

L = likelihood, I = impact (H/M/L).

| ID | Risk | L | I | Mitigation |
|---|---|---|---|---|
| R1 | Scope creep towards a full iPaaS (every connector, mapping UIs) | H | H | Strict milestones and non-goals; community connectors via the SDK and conformance kit |
| R2 | Our own dashboard takes longer than planned | M | H | Six core screens in M1; server-rendered HTML with no front-end build; API-first, so the CLI and MCP cover gaps |
| R3 | The chosen engine is immature or changes direction | M | M | Engine port and conformance suite; pinned versions; `LocalEngine` keeps tests engine-free |
| R4 | Vendor API caps fall below real volume | H | M | Shared budgets with reserves; negotiate higher caps; prefer bulk endpoints |
| R5 | SAP connectivity and driver licensing | M | H | OData first; drivers as user-installed extras; on-prem agent; early connector spike |
| R6 | GoFrugal API surface unknown | H | M | Confirmed in phase 3: no public API docs. Ask the vendor for docs and a sandbox. The connector is M2 if they arrive by M1's end, otherwise M3. File exchange is the fallback. |
| R7 | A tenant isolation bug leaks data | L | H | Row-level security in the DB plus API checks; automated cross-tenant tests |
| R8 | Postgres growth (spool, versions, ledger, logs) hurts performance | M | M | Daily and monthly table partitions dropped by retention jobs; compressed spool chunks; per-dataset switch to an object-storage spool for bulk-heavy datasets |
| R9 | Too many concepts for junior operators and new contributors | M | M | Defaults and templates; the dashboard speaks in business terms; tutorials |
| R10 | An AI agent takes a harmful action | L | H | Same RBAC; preview-then-confirm; prod holds; scoped tokens; audit |
| R11 | Maintaining two engine adapters is a burden | M | L | Ship one production adapter after the spike; the other becomes experimental |
| R12 | Open-source maintenance load exceeds team capacity | M | M | Connector maturity levels, CODEOWNERS, a contribution guide; the reference deployment funds the core |
| R13 | An HTML-only dashboard feels less interactive than a single-page app | M | L | HTMX partial refresh and live polling; server-rendered SVG charts; usability test against NFR-11 |

## 14. Success metrics

**Product (12 months after v1.0):**
- Time to first flow (install → flow running → visible in the dashboard) is ≤ 15 minutes in onboarding tests.
- AI-generated flows pass validation and tests ≥ 80% of the time on the benchmark task set (G2).
- Adoption: external production users, contributors and community connectors at beta or above. Baselines are set at v1.0.

**Reference organisation (by M3):**
- All in-scope integrations run on Feather Connect in prod: POS → warehouse, ERP masters → sales channels, orders and sales → ERP.
- Zero silent skips. Every failure is visible in the dashboard within 5 minutes.
- Median time to recover a failed delivery is ≤ 15 minutes, with no code change or SQL.
- The freshness SLA is met on ≥ 99% of days.
- Hand-built locks, queues and recovery scripts are retired.

## 15. Open questions

| ID | Question | Why it matters | Owner | Needed by |
|---|---|---|---|---|
| Q1 | DBOS or Hatchet? **Answered: DBOS Transact (ADR-0003).** | Runtime adapter, remote-worker model, ops footprint | Engine spike, then sponsor | Closed 30 Oct 2026 |
| Q2 | Which GoFrugal product and API access does the reference organisation have? Phase 3 research found no public API docs, so this needs the vendor. | Connector scope | Sponsor | Before M1 ends |
| Q3 | SAP: which S/4HANA release, which OData services are enabled, is HANA SQL read allowed, and is RFC/BAPI needed for postings? Phase 3 research supports OData first and `snapshot_diff` for prices. | SAP connector scope and licensing | Sponsor, SAP team | Before M2 starts |
| Q4 | What is the Zoho API daily cap for the reference organisation, now and after negotiation? Published plan caps (1,500–2,500 a day) are far below the 25,000 reported in the reference evaluation, so confirm it in writing. | Budget sizing; whether invoice-detail pulls fit | Sponsor | Before M2 starts |
| Q5 | Which identity provider for SSO: Google Workspace, Entra ID or Keycloak? | Auth design | Sponsor | M2 |
| Q6 | Where will the reference organisation run prod: VM, Railway, a cloud provider or on-prem? | Deployment docs, agent topology | Sponsor | M1 |
| Q7 | Must ERP price changes reach the POS within hours, or is nightly enough? | Derived-dataset schedule | Business owner | M2 |
| Q8 | For a late edit after a store-day was posted to the ERP: reverse and repost, or post a delta? | Side-effect sink design | Business owner, finance | M2 |
| Q9 | Where is the public home: GitHub organisation, PyPI name `feather-connect`, domain? | Release and docs | Sponsor | Before first public release |
| Q10 | Who are the maintainers, and what is the governance model (reviews, release cadence)? | OSS sustainability | Sponsor | M1 |
| Q11 | Is the default spool retention right (14 days after all routes applied), and what is the archive policy? | Storage cost, replay window | Sponsor | M1 |

## 16. SDLC roadmap and gates

| Phase | Output | Gate | Estimate |
|---|---|---|---|
| 1. Discovery | This document | Sponsor approves goals, scope, requirements and decisions | Done (draft) |
| 2. Design | [Design specification](../02-design/2026-10-30-feather-connect-design.md) + ADRs | Sponsor approves the architecture | Done (draft) |
| 3. Spikes | Engine choice (ADR-0003), a core-model prototype run against the hard cases, connector API findings (Zakya, GoFrugal, SAP, Magento), the `feather dev` approach | Spike report reviewed; ADR-0003 accepted | 2 weeks |
| 4. Plan | Implementation plan per milestone (tasks, tests, owners) | Plan approved | 3–4 days |
| 5. Build | M1 MVP, M2 business connectors and operations, M3 enterprise and hardening | Milestone exit criteria (design §19) | ~8 weeks each |
| 6. Test and release | v1.0.0 on PyPI and GHCR, docs site | Release checklist; go/no-go | 2 weeks hardening, overlapping M3 |
| 7. Operate | Runbooks, support, community | Ongoing | — |

Estimates assume two to three engineers working with AI assistants. They will be refined in phase 4.

---

## Appendix A — Glossary

| Term | Meaning |
|---|---|
| Connector | Code that reads from and/or writes to a system (`postgres`, `sap`, `zoho.zakya`) |
| Connection | A configured connector instance in one environment and tenant; secrets by reference |
| Dataset | A named stream of keyed records from one source entity (`zakya.invoice`) |
| Record | One keyed document, the unit of identity and storage |
| Partition | A named business slice (`store=1509/date=2026-10-18`) used to plan, re-run, verify and backfill |
| Version | An immutable batch of upserts and deletes for one dataset partition. *Complete* versions describe the whole slice. |
| Strategy | How a dataset detects change: `by_stamp`, `snapshot_diff`, `replace`, `push` |
| Spool | Where versions are written before delivery: Postgres by default, object storage optionally |
| Route | Dataset → optional transform → sink, with a mode and priority |
| Delivery / ledger | The application of one version to one route; the ledger is each route's progress and dead-letter state |
| Run / task | One execution of a flow, and the durable steps inside it |
| Readiness | "Complete through T" for a dataset or group |
| Budget | A shared rate limit with windows and priority reserves |
| Hold | Work waiting for a human decision |
| Override | An audited runtime change to an operational knob |
| Agent | An on-prem executor that connects out to the control plane |
| Engine port | The small interface behind which DBOS runs our tasks |
| Environment | One deployment: dev, qa, uat or prod |
| Tenant | A namespace for data, config, users and budgets inside an environment |

## Appendix B — Sources

1. Discovery Q&A with the sponsor, 30 October 2026. The decisions are in §11.
2. The reference evaluation: the shared Claude Code session "Explore: Feather Integration Hub — primitives & architecture" (October 2026), <https://claude.ai/code/cshk_01BcA2oRMDM5oNjNMEWbkxySTFc1k4cHCADTqrrZYPHu8P>. It contains:
   - a primitives survey across iPaaS tools, open-source integration tools, durable engines and Postgres-native building blocks;
   - inventories of problems in the reference organisation's existing pipelines;
   - hard-case tests of the data model;
   - engine pricing checks on vendor pages.
3. The vendor facts in §9.2 were observed by that session in October 2026. The ones that still affect the decision were re-verified when ADR-0003 was accepted.
