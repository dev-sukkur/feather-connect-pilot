=== proposal.md ===
# Proposal

## Why

The whole product rests on one data model. It can be built and proved in memory, with no database, server or
engine, so mistakes in it are found while they are still cheap to fix.

## What Changes

- The SDK: datasets, flows, routes, triggers, transforms, static validation.
- Records, keys, hashes, partitions, versions and the ten invariants.
- The change strategies and run-level delete reconciliation.
- Delivery: the ledger, ordering, modes, quarantine, retry, skip and replay, on an in-memory store and an in-process
  engine.
- The connector contracts, the shared HTTP client, the conformance kit, and the in-memory connectors.
- The flow test harness.

Out of scope: Postgres, the durable engine, the server, the CLI beyond what tests need, real connectors.

## Capabilities

### New Capabilities
- `data-model`: records, keys, hashes, partitions, versions and their invariants
- `change-detection`: the change strategies, no-op suppression, moves and run-level deletes
- `flow-authoring`: the SDK, static validation and the test harness
- `delivery`: routes, the ledger, ordering, modes, quarantine and replay
- `connector-sdk`: source, sink and push-source contracts, the HTTP client and the conformance kit

### Modified Capabilities

None.

## Impact

Creates the `feather-connect` package and the repository's tooling: uv workspace, ruff, strict pyright, pytest,
hypothesis, CI on Linux and Windows.

**Exit criteria**

- Hard cases H1, H2, H3 and H4 pass in memory.
- Property tests hold invariants I1–I4, I7 and I10 for generated sequences of reads.
- The conformance kit passes for the in-memory connectors.
- Coverage of the package is at least 85%, and strict type checking reports no error.

=== design.md ===
# Design

## Context

The pilot built this stage first too (its Plan 1), and its model survived every later stage unchanged. What changed
later, and cost rework, was around it: names gained a package prefix, the store contract grew to one 1,724-line
file, and connector packages were bolted onto a regular package.

## Goals / Non-Goals

**Goals:** the model, complete and proved, behind contracts that later stages implement without changing.

**Non-Goals:** durability, performance, any process other than a test runner.

## Decisions

- **The store contract is split by aggregate from the first commit** (runs, versions, ledger, record state,
  quarantine, answers). Each has its own contract tests, written to run against any implementation. Stage 2 adds
  Postgres by passing the same tests.
- **`feather_connect.connectors` is a true namespace package** with no `__init__.py`, so that connector
  distributions and strict type checking work without ignores.
- **Names are (package, short name) pairs in the model**, not strings with a slash, even though stage 4 is where
  packages arrive. Rendering a pair as `package/name` is a display matter.
- **The tenant is a field of every stored row and of the store's constructor now**, with `default` as the only
  value accepted.
- **Port the pilot's tests first.** From the pilot: `feather-connect/tests/core/`, `tests/runtime/test_extract.py`,
  `test_seal.py`, `test_apply.py`, `test_double_run.py`, `test_pause.py`, `tests/connectors/`,
  `tests/testing/test_harness.py`, `testing/store_contract.py`, `testing/conformance.py`, and the in-memory half of
  `tests/hard_cases/`.

## Risks / Trade-offs

- Run-level delete reconciliation is the subtlest code in the product (`runtime/extract.py` in the pilot, 1,050
  lines). Read the pilot's implementation and its tests together before writing it; the edge cases about rejected
  records and unchanged slices are in the tests, not the design.
- Requirements marked `corrected` here (a longer lookback, duplicate names across flows, OAuth2 tokens shared
  across processes) have no pilot test to port. Write theirs from the scenarios.

=== tasks.md ===
# Tasks

## 1. Repository and tooling

- [ ] 1.1 uv workspace, ruff, strict pyright, pytest, hypothesis; CI on Linux and Windows
- [ ] 1.2 The tenant rule test: every stored row type carries a tenant

## 2. Records, partitions, strategies

- [ ] 2.1 Port `core/test_records.py`, `test_partitions.py`, `test_strategies.py`; add property tests for hash stability
- [ ] 2.2 Records, keys, canonical JSON, row hash
- [ ] 2.3 Partition schemes with labels and bounds
- [ ] 2.4 Strategies and durations

## 3. Store contract and memory store

- [ ] 3.1 Contract tests per aggregate, ported and split from the pilot's `store_contract.py`
- [ ] 3.2 The in-memory store
- [ ] 3.3 The in-process engine, with the engine port

## 4. Extract, seal, apply

- [ ] 4.1 Port the extract, seal, apply and double-run tests
- [ ] 4.2 Extract with no-op suppression, quarantine, checkpoints and the empty-read guard
- [ ] 4.3 Staging, run-level delete reconciliation and sealing
- [ ] 4.4 Apply with modes, ordering, error classes, quarantine, answers
- [ ] 4.5 Retry, skip, replay and route pause

## 5. SDK and connectors

- [ ] 5.1 `Dataset`, `Flow`, `to`, triggers, transforms, `validate()` with every code
- [ ] 5.2 Connector contracts, spec, context, the shared HTTP client
- [ ] 5.3 The conformance kit and the in-memory connectors
- [ ] 5.4 `FlowHarness`

## 6. Exit

- [ ] 6.1 H1–H4 pass in memory
- [ ] 6.2 Property tests for I1–I4, I7, I10
- [ ] 6.3 Coverage at least 85%; pyright strict clean
- [ ] 6.4 `openspec validate 01-core-model --strict` passes; archive the change
