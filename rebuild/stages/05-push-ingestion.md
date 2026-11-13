=== proposal.md ===
# Proposal

## Why

Application events must reach destinations in seconds. The pilot built webhook ingestion against flow code loaded
in the server, then rebuilt it for packages. Here it is built once, on declared webhooks and package hosts.

## What Changes

- The webhook endpoint: verify, deduplicate, store, answer.
- Webhooks declared by packages, verified with built-in signature schemes.
- The inbox and the sealer, which runs on the host of the package's active version.
- Held events, with reprocess and release.

Out of scope: database change data capture (backlog).

## Capabilities

### New Capabilities
- `push-ingestion`: ingress, the inbox, signature schemes, the sealer and the latency budget

### Modified Capabilities

None.

## Impact

Adds a public, unauthenticated endpoint to the server, protected by signatures and rate limits.

**Exit criteria**

- 3,000 events at 50 a second, webhook to HTTP destination: all arrive once each, 95th percentile at most 5 s,
  measured on a Postgres with fsync on.
- Hard case H14: a tenfold burst loses nothing and recovers within 5 minutes.
- Killing the server mid-seal, or a worker mid-apply, loses and duplicates nothing.

=== design.md ===
# Design

## Context

The pilot met its latency target with room to spare (p95 about 2 s). Its Stage 3 review found the risks: one bad
event blocking a sealer for good (S1), a failed parse throwing an event away (S2), a key moving partitions twice in
a batch (S3), ingress hanging in a database outage (S4), and unsigned callers using up a source's rate (S5).

## Goals / Non-Goals

**Goals:** ingress that never waits on anything but the database, and not for long on that; per-key order; no
event lost to a bad neighbour.

**Non-Goals:** sub-second latency; exactly-once at the sender (a sender may resend, and is deduplicated).

## Decisions

- **The server verifies and stores; the host parses and seals.** The server never runs package code, so a package
  declares its webhooks and can't bring a receiver as code.
- **One apply task per (route, version), not per event**, so task rates stay well below event rates.
- **Measure on a durable database.** The pilot's first numbers came from a test Postgres with fsync off (S19).
- **Port first:** `feather-server/tests/test_ingress.py`, `feather-connect/tests/runtime/test_sealer.py`,
  `tests/core/test_webhooks.py`, `tests/packages/test_webhooks.py`, `tests/e2e/test_webhook_to_sink.py`,
  `test_webhook_latency.py`, `test_webhook_to_http_latency.py`, `tests/chaos/test_kill_mid_push.py`.

## Risks / Trade-offs

- The Zoho signature scheme is assumed, not confirmed against every Zoho product. Keep it marked unverified.
- Rate limits live in each server process, so several replicas allow several times the limit. They protect a
  replica; they are not a promise to a sender.
- Latency tests are sensitive to the test's own echo server (Nagle and delayed ACK cost the pilot a false failure).

=== tasks.md ===
# Tasks

## 1. Ingress

- [ ] 1.1 Port the ingress and webhook signature tests
- [ ] 1.2 Declared webhooks and the built-in schemes
- [ ] 1.3 The endpoint: ordered checks, two rate limits, bounded waits, 503 on a database outage
- [ ] 1.4 Deduplication by key; retention of event records

## 2. Sealer

- [ ] 2.1 Port the sealer tests
- [ ] 2.2 Batching by age, count and bytes; versions named after their first event
- [ ] 2.3 Per-key order across partitions
- [ ] 2.4 Quarantine, held events, retries of a failing parse
- [ ] 2.5 Sealing on the package's host

## 3. Exit

- [ ] 3.1 The latency test on a durable Postgres
- [ ] 3.2 H14
- [ ] 3.3 The push chaos tests
- [ ] 3.4 `openspec validate 05-push-ingestion --strict` passes; archive the change
