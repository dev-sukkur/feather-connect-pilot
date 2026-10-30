# ADR-0002: Our own control plane and dashboard; durable engine behind a port

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor)

## Context

Operators need one dashboard to monitor **and act** across flows, environments and tenants. The durable engine is still undecided (ADR-0003). The reference evaluation found the engine UIs to be paid, partial or engine-specific. Humans and AI agents need the same powers, under the same rules.

## Decision

- **Build three things of our own:**
  - the Python SDK;
  - the control plane, which keeps the catalogue, run journal, versions, spool, ledger, holds, overrides and audit in our Postgres schema;
  - the dashboard.
- **One service layer, three adapters.** The HTML dashboard, JSON API (used by the CLI) and MCP server are thin adapters over one service layer.
- **Durable execution goes through a small `Engine` port** (design §6.8), with a `LocalEngine` for tests.
- **Ours, not the engine's:** scheduling, budgets, the ledger and the run journal.

## Consequences

**Benefits:**
- The engine is swappable.
- The dashboard doesn't depend on any engine.
- There is one source of truth.
- Every client gets the same RBAC, previews and audit.

**Costs:**
- We build and maintain the dashboard and run journal.
- We must keep the port small and stop engine-specific features from leaking into the SDK.

## Alternatives considered

- **Engine-native flows and the engine's UI.** Faster to a demo, but it locks us in and contradicts "our own dashboard".
- **Broker-centric (Kafka + Connect-style).** Proven for streaming, but too heavy for seconds-level latency at moderate volume, and it breaks "easy to run".
