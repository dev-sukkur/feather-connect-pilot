# Spec extraction and rebuild plan

How to take what this pilot repository learned, write it down as an OpenSpec specification, and start again from
scratch in a new repository.

- **Status:** Proposed, for the sponsor's review
- **Date:** 2026-11-11
- **Delivers:** the specification in [`openspec/specs/`](../../openspec/specs), the staged build in
  [`rebuild/`](../../rebuild/README.md), and this plan
- **Tool:** [OpenSpec](https://github.com/Fission-AI/OpenSpec) 1.16, schema `spec-driven`

## 1. Summary

The request was to reverse-engineer an OpenSpec specification from this repository and rebuild from it elsewhere.
A survey of the repository says the plain version of that plan would lose most of what the pilot is worth, so this
document proposes a better one, and the specification it calls for is already written.

| | Plain plan | This proposal |
|---|---|---|
| Source | The code | The code, the design documents and decisions, and the tests, checked against each other |
| What a requirement says | What the code does today | What the product must do, with the pilot's known defects corrected and its scaffolding left out |
| How sure it is | Unstated | Each requirement carries a status: verified, built, partial or corrected |
| What the new repository starts with | A full spec that claims a system which doesn't exist yet | Eight staged changes; the spec grows as each stage is built and archived |
| Tests | Rewritten from the spec | The pilot's contract and acceptance suites are named per stage and ported first |
| Order of building | The pilot's order | A new order that builds in from the start what the pilot had to retrofit |

## 2. What the survey found

**The pilot is documented better than most finished products.** It holds a discovery document with 60 functional
and 15 non-functional requirements, a 1,483-line design, 13 decision records, two exit reports, and a page that
maps every requirement to a milestone. Reverse-engineering the code alone would throw the reasons away and keep
only the results.

**The code holds more than the product.** About 45,700 lines of source and 25,700 lines of tests in the packages.
Some of that exists only because the pilot changed direction part-way:

| In the code | Why it is there | Carry over? |
|---|---|---|
| `feather server upgrade --project`, the `renamed` table, 308 redirects for old names | The M1 project mode was retired in M1.1 and running environments had to move | No. A new repository has nothing to upgrade |
| 26 migrations | The schema grew stage by stage | No. One baseline migration |
| `pkgutil.extend_path` in `feather_connect/__init__.py`, with pyright ignores | Connector packages were added to a regular package | No. Lay the namespace out properly from the start |
| A 3,438-line Postgres store and a 1,724-line store contract in one file each | Grew with every stage | The contract carries over, split by aggregate |
| `spikes/`, the Hatchet side branch | The engine choice | The decision carries over (ADR-0003); the code does not |
| Finding IDs (F1–F15, S1–S23, B1–B6, I1–I4) | Five stage reviews | The lessons carry over as scenarios; the IDs stay in the pilot |

**The tests are the most valuable thing to keep.** 1,817 tests, and five suites that state the product's promises
directly:

| Suite | Where | What it proves |
|---|---|---|
| Store contract | `packages/feather-connect/src/feather_connect/testing/store_contract.py` | Every store operation, run against both the memory and the Postgres store |
| Connector conformance kit | `packages/feather-connect/src/feather_connect/testing/conformance.py` | What any connector must do to be called beta |
| Engine conformance | `tests/engine_conformance/` | The 12 checks any durable engine must pass |
| Hard cases | `tests/hard_cases/` | H1–H4 and H6; H14 is in `tests/e2e/test_webhook_latency.py` |
| Chaos | `tests/chaos/` | kill -9 of workers, hosts, the supervisor and Postgres loses and duplicates nothing |

**The pilot was not left green.** The last pull request of M1.1 merged while CI was still running. The last full
local run had 14 failures and 33 errors, the chaos suite still kills workers where it must now kill package hosts,
and the deploy test has not been looked at since the Compose file changed (M1.1 exit report §4). A specification
that says "as the code does" would inherit that uncertainty without saying so.

**A third of v1 is designed and not built.** Readiness, side-effect routes and approvals, the on-prem agent, schema
policy, alerts, the rest of the roles, single sign-on and the MCP server are specified in the design and have no
code. A code-only extraction can't see them.

## 3. The proposal

### 3.1 Distil from three sources

Each requirement in the specification was written from the three sources together, and says which it rests on:

1. **Intent:** the discovery document, the design, the flow packages design and the decision records say what was
   wanted and why.
2. **Contract:** the public SDK, the store contract, the engine port, the JSON API's routes, the CLI's commands,
   the manifest schema and the schema migrations say what was built.
3. **Evidence:** the tests, the hard cases and the exit reports say what was shown to work.

Where the three agree, the requirement is `verified`. Where the code does something no test shows, it is `built`.
Where the pilot is known to be wrong, the requirement states the right behaviour and is `corrected`.

### 3.2 Status on every requirement

Every requirement ends with a status line, `*Pilot: verified*`, and has a row in
[`openspec/traceability.md`](../../openspec/traceability.md) that says where it comes from:

```text
| A delivery is applied at most once | verified | I3, FR-E3, NFR-04, design §6.5, tests/chaos/test_kill_mid_apply.py::… |
```

| Status | Meaning | What the rebuild does |
|---|---|---|
| `verified` | Built, and a test in the pilot proves it | Port the test first, then build to it |
| `built` | Built; the pilot's tests don't prove it, or they are among those left failing | Write the test from the scenario, then build |
| `partial` | Built in part; the requirement text says which part | Build all of it |
| `corrected` | The pilot is known to do this wrong; the requirement states the right behaviour | Build it right; the trace names the defect |

The trace uses the pilot's own IDs (FR-, NFR-, I1–I10, H1–H17, design sections, ADRs) and test paths, so the pilot
repository stays the reference for anyone who wants the detail or the reason.

Features that are designed and not built are not in the specification, because nothing here proves them. They are
the backlog in §7, each with its design.

### 3.3 Leave the scaffolding behind, and correct what is known to be wrong

Left out of the specification on purpose: the project mode and its upgrade path, renamed-name redirects, the
migration history, and anything that names DBOS outside the engine adapter.

Written as `corrected` (the pilot's open items, from the two exit reports and the milestones page):

| Defect in the pilot | Where the right behaviour is specified |
|---|---|
| Two identical requests at the same instant both run (`Idempotency-Key` is check-then-set) | `operations` |
| A crash between a scheduler tick and its run loses the tick | `scheduling` |
| `feather validate` misses duplicate route names across flows | `flow-authoring` |
| `feather run --sim` fails with signed webhook sources | `local-dev` |
| A lookback made longer later doesn't reach older versions | `change-detection` |
| The server binds 0.0.0.0 by default | `deployment` |
| The server image carries a demo secret file | `deployment` |
| OAuth2 tokens are shared per process, not across processes | `connector-sdk` |
| `feather new --template` loses the comments in YAML files | `flow-templates` |
| Dashboard links to packaged names carry `%2F` | `flow-packages`, `dashboard` |
| Packaged datasets have no partition labels, check or backfill on the dashboard | `package-hosts` |

### 3.4 Stage the specification as changes

OpenSpec keeps two things apart: `specs/` is what is true of the system now, and `changes/` is what is proposed.
A new repository has no system yet, so filling its `specs/` with 240 requirements on day one would make every one
of them false.

So the specification is written once, here, where it is true: `openspec/specs/` in this repository describes the
pilot. For the new repository, [`rebuild/seed.py`](../../rebuild/seed.py) turns it into eight ordered changes, one
per build stage. Each change has a proposal, a design that carries the pilot's lessons for that stage, a task list,
and the stage's requirements as `## ADDED Requirements`. As a stage is built, `openspec archive` moves its
requirements into the new repository's `specs/`. At any moment `specs/` there says exactly what has been built,
and `openspec list` says what is left.

### 3.5 Build in a different order

The pilot built the core loop, then the dashboard, then connectors, and only then turned workers into supervisors,
added roles, moved configuration into the database and prefixed every name. Each of those touched most of what
came before. The rebuild takes them in the order that avoids the rework:

| Stage | Change | Capabilities | Why here |
|---|---|---|---|
| 1 | `01-core-model` | data-model, change-detection, flow-authoring, delivery, connector-sdk | The whole model runs in memory with no infrastructure; hard cases H1–H4 pass before any database exists |
| 2 | `02-durable-runtime` | tenancy, storage, durable-execution, scheduling, rate-budgets | Postgres and the engine behind their two contracts; chaos and engine conformance pass |
| 3 | `03-control-plane` | access-control, operations, observability | One service layer with roles and audit from its first action, not added later |
| 4 | `04-flow-packages` | flow-packages, package-hosts, environment-config | Packages are the only way to deploy from the start; the server never imports flow code; names are prefixed once |
| 5 | `05-push-ingestion` | push-ingestion | Built once, on declared webhooks and hosts, instead of twice |
| 6 | `06-dashboard` | dashboard | Built once, in its final layout, over a finished service layer |
| 7 | `07-connectors-and-templates` | connector-catalogue, flow-templates | Real connectors arrive as packages against a settled SDK and configuration model |
| 8 | `08-developer-experience-and-deployment` | local-dev, deployment | `feather dev`, images and Compose over the finished parts; the M1 and M1.1 exit criteria are the gate |

## 4. The specification

Twenty-two capabilities, 240 requirements and 310 scenarios, in [`openspec/specs/`](../../openspec/specs). All pass
`openspec validate --specs --strict`.

| Pilot status | Requirements |
|---|---|
| `verified` | 207 |
| `built` | 11 |
| `partial` | 10 |
| `corrected` | 12 |

| Capability | Covers | Main pilot sources |
|---|---|---|
| `data-model` | Records, keys, hashes, partitions, versions, the ten invariants | Design §4; `core/records.py`, `core/partitions.py` |
| `change-detection` | `by_stamp`, `snapshot_diff`, `replace`, custom; no-op suppression; moves; run-level deletes | Design §4.3; `runtime/extract.py`, `runtime/seal.py` |
| `flow-authoring` | `Dataset`, `Flow`, routes, triggers, transforms, static validation, the test harness | Design §5; `core/flow.py`, `testing/harness.py` |
| `delivery` | Routes, modes, the ledger, ordering, quarantine, replay, destination answers | Design §4.5, §6.4–6.5; `runtime/apply.py` |
| `connector-sdk` | Source, push source and sink contracts; specs; the shared HTTP client; the conformance kit | Design §8.1–8.2, §8.7; `core/connector.py`, `runtime/http.py` |
| `tenancy` | The tenant on every record; the eight rules | ADR-0006, ADR-0013 |
| `storage` | The store contract, the spool, chunk format, retention, growth | Design §7; `storage/port.py`, `storage/spool.py` |
| `durable-execution` | The engine port, tasks, retries, workers, the sweeper, engine conformance | Design §6.2, §6.8, §6.12; `engine/port.py` |
| `scheduling` | Cron and interval triggers, runs, stream runs, overlap, catch-up, pause | Design §6.7; `runtime/scheduler.py` |
| `rate-budgets` | Shared budgets, windows, reserves, vendor accounts, deferral | Design §6.6; `runtime/budgets.py` |
| `access-control` | Users, tokens, sessions, the two roles, audit, web security | Design §13; `feather_server/auth.py`, `services/access.py` |
| `operations` | Run, re-run, backfill, cancel, retry, skip, replay, pause; previews; API and CLI conventions | Design §9; `services/actions.py`, `api/routes.py` |
| `observability` | Structured logs, task logs, record trace, the health summary | Design §15; `logs.py`, `services/trace.py` |
| `flow-packages` | Build, the built-in registry, install checks, promotion, signing, uninstall | Flow packages design §4–§6, §16 |
| `package-hosts` | Supervisors, hosts, prepare, activate, switch, deactivate, isolation | Flow packages design §7, §11 |
| `environment-config` | Layered configuration, connections, encrypted secrets, key rotation | Flow packages design §8; `vault.py` |
| `push-ingestion` | Ingress, the inbox, signature schemes, the sealer, the latency budget | Design §6.3, §13.5; `ingress.py`, `runtime/sealer.py` |
| `dashboard` | The shell, the list pattern, the pages, safe actions, accessibility | Design §10; milestones §4.3 |
| `connector-catalogue` | Simulation, Postgres, S3, REST, GraphQL, HTTP, webhook, DuckDB, Zakya | Design §8.3–8.5; `packages/connectors/` |
| `flow-templates` | The five generic templates and the retail one | Design §8.6; `templates/` |
| `local-dev` | `feather dev`, `new`, `validate`, `run --local`, `test`, `describe` | Design §18.2; `feather_cli/commands/` |
| `deployment` | Images, Compose, settings, migrations, health, upgrades | Design §18; `deploy/` |

## 5. Decisions

### 5.1 Carried over as they are

Copy the 13 decision records into the new repository's `docs/adr/` unchanged, with their numbers: Python throughout
and an HTML dashboard (0001), our own control plane with the engine behind a port (0002), DBOS chosen by spike
(0003), one data model (0004), Postgres as the only required database (0005), an environment is a deployment and a
tenant is a namespace (0006), full-code flows (0007), AI through MCP (0008), MIT (0009), Compose and `feather dev`
(0010), flow packages (0011), readiness (0012, still proposed), light tenancy (0013).

### 5.2 Worth reopening before stage 1, because a rebuild is the one cheap moment

| # | Question | Recommendation |
|---|---|---|
| R1 | The name separator. Packaged names are `package/name`, which must be percent-encoded in every URL | Keep the slash in the CLI and on pages, and give every API and page route the two parts as two path segments (`/flows/{package}/{flow}`) |
| R2 | The store contract is one 1,724-line protocol file | Keep one contract, split by aggregate (runs, versions, ledger, inbox, budgets, catalogue, packages, access), each with its own contract tests |
| R3 | Connector packages share the `feather_connect.connectors` namespace through a path hack | Make `feather_connect.connectors` a true namespace package with no `__init__.py`, so type checkers follow it |
| R4 | Readiness was designed after the seal and apply transactions existed | Write the readiness marks in those transactions from stage 2, even though the trigger comes later (backlog item 1) |
| R5 | Stage reviews produced five ID schemes that clash | One scheme from the start: OpenSpec change names, and `PKG-R1-I3`-style finding IDs (milestones §11) |
| R6 | Package hosts have the worker's network and no sandbox | Unchanged for v1; say so in the security notes, as the pilot does |
| R7 | The public name: PyPI `feather-connect`, the GitHub organisation, the domain (discovery Q9) | Sponsor to decide before the new repository is created |

## 6. How to start the new repository

1. **Review the specification here.** Read `openspec/specs/` with `openspec view` or `openspec show <capability>`.
   Fix anything wrong in this repository, where the code is at hand to check against.
2. **Settle R1–R7** in §5.2. Each changes requirement text, so settle them before seeding.
3. **Create the empty repository** and seed it:

   ```bash
   python rebuild/seed.py ../feather-connect
   ```

   That writes `openspec/config.yaml`, `openspec/traceability.md`, the eight changes under `openspec/changes/`, and
   an empty `openspec/specs/`. It refuses a folder that already has an `openspec/` directory. A trial seed passed
   `openspec validate --all --strict`, and archiving its first change moved 61 requirements into `specs/`.
4. **Copy by hand** what the script doesn't own: `LICENSE`, the 13 decision records, and the discovery and design
   documents as `docs/reference/` for the reasons behind the requirements.
5. **Check the seed:** `openspec validate --all --strict` in the new repository.
6. **Build stage by stage.** For each change, in order: port the pilot's tests that the stage's `design.md` names,
   build until the scenarios pass, then `openspec archive <change>`. A stage is done when its exit criteria in
   `proposal.md` are met and CI is green. No stage merges with a failing suite, which is the rule the pilot's last
   pull request broke.
7. **Keep this repository read-only** once stage 1 starts, as the reference the traces point into.

## 7. Backlog: designed, not built

These are not in the specification. Each becomes its own OpenSpec change in the new repository, after stage 8,
written from the design named here.

| # | Feature | Requirements | Design in the pilot |
|---|---|---|---|
| 1 | Readiness: `fc.ready`, `fc.on_ready`, the Readiness page; hard cases H13, H16, H17 | FR-B7, FR-D3 | `docs/02-design/2026-11-08-readiness.md`, ADR-0012 |
| 2 | Side-effect routes with `compensate`, approvals and holds; H9 | FR-E5, FR-G3 | Design §4.5, §6.5 |
| 3 | `fc.reconcile`: a scheduled snapshot diff over recent partitions; H5 | FR-B2 | Design §4.3 |
| 4 | Schema evolution policy and `schema_change` holds; H12 | FR-B9 | Design §4.7 |
| 5 | Bootstrapping a newly added route from history; H11 | FR-B8 | Design §4.8 |
| 6 | Confirm-later sinks (`sink.confirm`) | — | Design §6.5, §8.4 |
| 7 | The on-prem agent and its gateway | Constraint C4 | Design §6.9 |
| 8 | Business connectors: SAP, Magento, GoFrugal; the retail templates | FR-J2, FR-A7 | Design §8.3–8.4, §8.6 |
| 9 | The rest of the roles (viewer, developer, approver, PII reader); PII masking | FR-K2, FR-K5 | Design §13.2, §13.4 |
| 10 | Alerts and their channels; budget priorities page; audit page | FR-M3, FR-E6, FR-F9 | Design §15.4–15.5 |
| 11 | Environment peers and the fleet view; production protections; OIDC single sign-on | FR-F8, FR-H3, FR-H4, FR-K1 | Design §10.4, §11, §13.1 |
| 12 | The MCP server and agent skills | FR-L1, FR-L3 | Design §14 |
| 13 | Postgres CDC; metrics export; the `objects` spool; SQL transforms; overrides and windows | FR-C2, FR-C4, FR-G4, FR-M2 | Design §5.5, §7.2, §15.2 |
| 14 | Full multi-tenancy | FR-I2–I4 | Design §12, ADR-0013 |

## 8. Risks

| Risk | Mitigation |
|---|---|
| The specification is wrong somewhere, and the rebuild copies the mistake | Review here first (§6 step 1); every requirement names the pilot test or code to check it against |
| A second-system rebuild grows beyond the pilot's scope | The eight stages hold only what the pilot built; everything else is the backlog, one change each |
| The rebuild takes as long as the pilot did | The model, the contracts and about 1,800 tests are already known, and the order in §3.5 avoids the rework that M1.1 was largely made of. No estimate is given here: size it after stage 1 |
| Requirements marked `built` or `corrected` have no proving test to port | Their scenarios are written to be turned into tests directly; each stage's task list starts with them |
| The pilot keeps changing while the rebuild runs | Freeze it (§6 step 7) |

## 9. For the sponsor to decide

1. Is the staged rebuild in §3.5 the order wanted, or should the pilot's order be kept?
2. R1–R7 in §5.2.
3. The new repository's name and where it lives.
4. Whether readiness (backlog item 1) should be stage 9 or be folded into stage 2, as R4 half-suggests.
