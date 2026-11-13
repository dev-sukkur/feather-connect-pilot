=== proposal.md ===
# Proposal

## Why

With the SDK, configuration and packages settled, real connectors can be written once against contracts that will
not move, and shipped the way customers' own flows are.

## What Changes

- Connectors: JSON simulation, Postgres, object storage, REST, GraphQL, HTTP call, webhook, DuckDB and MotherDuck,
  Zoho Zakya.
- Templates: the five generic ones as library functions and as copies, and the retail Zakya template.

Out of scope: SAP, Magento and GoFrugal connectors and the other retail templates (backlog).

## Capabilities

### New Capabilities
- `connector-catalogue`: the connectors that ship with the first release
- `flow-templates`: the generic templates and the retail Zakya template

### Modified Capabilities

None.

## Impact

Adds one distribution per connector family and two for templates. Proprietary drivers are never bundled.

**Exit criteria**

- Every connector passes the conformance kit and is labelled beta.
- Every template validates for every environment, and its own tests pass.
- The fan-out demo delivers every order to Postgres, DuckDB and an HTTP API.
- The Zakya demo delivers each invoice once, and a changed one again, against the simulator.

=== design.md ===
# Design

## Context

The pilot's Stage 4 began with six blockers (B1–B6) because connectors met an SDK that had not been used by a real
one, and it settled twelve rulings while building. The rulings are now requirements, so this stage starts where the
pilot's ended.

## Goals / Non-Goals

**Goals:** every connector at beta by the kit; templates that work as written.

**Non-Goals:** a live run against a real Zakya organisation, which waits for the sponsor's test organisation.

## Decisions

- **Rulings carried as requirements:** sinks write the `_fc_` columns and an applied-chunks table; one HTTP client
  per connector instance; scheme settings at the top level and only secrets under `auth`; a `Retry-After` over 60 s
  raises at once; mapped response fields go to a table; the HTTP call sink rejects deletes by default; the object
  time column is `_object_mtime`; dev and qa run on simulation through configuration; templates are library
  functions first.
- **Copying a template edits YAML in place**, keeping comments. The pilot rewrote the files.
- **Port first:** every `packages/connectors/*/tests/`, `feather-connect/tests/connectors/`,
  `packages/templates/*/tests/`, `tests/templates/`, `tests/demo/`, `tests/e2e/test_demo_fanout.py`.

## Risks / Trade-offs

- Five Zakya points are unconfirmed until a live run: the EU host, which lists honour `last_modified_time`, the
  rate-limit codes, the reference filter on stock adjustments, and the sign-in lockout time (pilot milestones §4).
- Zoho's published daily call caps are far below what the reference organisation reported; size budgets from a
  written confirmation (pilot discovery Q4).

=== tasks.md ===
# Tasks

## 1. Core connectors

- [ ] 1.1 Port each connector's tests and conformance subclass
- [ ] 1.2 JSON simulation with fault injection
- [ ] 1.3 Postgres; DuckDB and MotherDuck
- [ ] 1.4 Object storage
- [ ] 1.5 REST, GraphQL, HTTP call, webhook

## 2. Zakya

- [ ] 2.1 Port the Zakya tests and its simulator
- [ ] 2.2 Auth, budgets by account, sources, sinks

## 3. Templates

- [ ] 3.1 Port the template tests
- [ ] 3.2 The five generic templates as library functions; `feather new flow --template` with comment-keeping edits
- [ ] 3.3 The retail Zakya template and its demo

## 4. Exit

- [ ] 4.1 Every connector passes the kit
- [ ] 4.2 Every template validates and passes its tests
- [ ] 4.3 The fan-out demo and the Zakya demo pass
- [ ] 4.4 `openspec validate 07-connectors-and-templates --strict` passes; archive the change
