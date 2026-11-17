=== proposal.md ===
# Proposal

## Why

People and scripts need one safe way to see and act on what the runtime does. Building the service layer with
roles, previews and audit from its first action avoids adding them to every action later, as the pilot had to.

## What Changes

- The server process: local users, API tokens, sessions, the roles `operator` and `admin`, audit.
- One service layer with every operation: run, re-run, backfill, cancel, verify, retry, skip, replay, pause.
- Previews and confirmation tokens for mass actions.
- The JSON API under `/api/v1`, and the CLI's remote commands.
- Structured logs, task logs, record trace, health endpoints and the summary.

Out of scope: HTML pages (stage 6), packages (stage 4), webhooks (stage 5).

## Capabilities

### New Capabilities
- `access-control`: users, tokens, sessions, the two roles, audit and web security
- `operations`: the operator's actions, previews, and the API and CLI conventions
- `observability`: logs, task logs, record trace and health

### Modified Capabilities

None.

## Impact

Adds the `feather-server` and `feather-cli` packages and an OpenAPI document.

**Exit criteria**

- Every API route refuses a call with no credentials, and every admin action refuses an operator.
- Every state-changing action writes an audit record, checked by one test that walks the service layer.
- Two identical requests with one `Idempotency-Key`, sent at the same instant, act once.
- A flow can be run, re-run, backfilled, retried, skipped and replayed through the API and the CLI.

=== design.md ===
# Design

## Context

The pilot built the dashboard before the API and added roles a milestone later, so role checks and audit were
threaded through existing actions. It left the `Idempotency-Key` as check-then-set.

## Goals / Non-Goals

**Goals:** every operation in one layer; adapters with no logic of their own; nothing reachable without an actor.

**Non-Goals:** viewer and developer roles, approvals, single sign-on, the MCP server (all backlog).

## Decisions

- **API first, pages later.** The service layer is finished and tested through the API before any HTML exists.
- **Capabilities, not role checks.** Each action names its capability; one function decides. No route or command
  checks a role.
- **`Idempotency-Key` is claimed by a unique insert** in the same transaction as the action's first write.
- **Routes take a package and a short name as two path segments** (plan §5.2 R1), from the first route.
- **The server binds to loopback by default.**
- **Port first:** `feather-server/tests/test_auth.py`, `test_roles.py`, `test_actions.py`, `test_api.py`,
  `test_reads.py`, `test_trace.py`, `test_task_records.py`, `test_app.py`, `feather-cli/tests/test_remote.py`,
  `test_processes.py`, `feather-connect/tests/test_logs.py`.

## Risks / Trade-offs

- Replay has three traps the pilot fell into (F1–F3): replaying a partition must send only its newest version, an
  old version must be refused, and a version whose chunks are gone must fail loudly. All three are scenarios in
  `delivery`; test them through the API here.
- Error text from connectors must be redacted before it is stored (F8), not when it is shown.

=== tasks.md ===
# Tasks

## 1. Access

- [ ] 1.1 Port the auth and roles tests
- [ ] 1.2 Users, Argon2, sign-in throttle, sessions, CSRF, the content security policy
- [ ] 1.3 Tokens, scopes, the two roles, capabilities
- [ ] 1.4 Audit, with a test that every action in the service layer writes a record

## 2. Operations

- [ ] 2.1 Port the actions tests
- [ ] 2.2 Reads: flows, runs, tasks, partitions, versions, deliveries, failure groups, summary
- [ ] 2.3 Actions with previews and confirmation tokens
- [ ] 2.4 Verify a partition; test a connection; held events

## 3. API and CLI

- [ ] 3.1 Port the API tests; add the concurrent `Idempotency-Key` test
- [ ] 3.2 Routes, problem responses, cursors, the OpenAPI document
- [ ] 3.3 Remote commands with `--json`, `--dry-run`, `--yes`

## 4. Observability

- [ ] 4.1 JSON logs with correlation ids; redaction
- [ ] 4.2 Task logs, task records, record trace
- [ ] 4.3 `/healthz`, `/readyz`

## 5. Exit

- [ ] 5.1 No route answers without credentials; no admin action answers an operator
- [ ] 5.2 Every action audited
- [ ] 5.3 Concurrent idempotent requests act once
- [ ] 5.4 `openspec validate 03-control-plane --strict` passes; archive the change
