# ADR-0004: One data model for ETL and events

- **Status:** Proposed (adopted from the reference evaluation; to be confirmed at design review)
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor)

## Context

Feather Connect must handle both bulk ETL and application events, such as webhooks and API postings. Separate products for the two would duplicate connectors, operations and monitoring.

The reference evaluation tested a candidate model against 18 hard cases and had it reviewed adversarially. The revised core survived, with these elements:
- the record as the unit of storage;
- the partition as an index;
- versions marked complete or incomplete;
- run epochs;
- a sink ledger of applied versions.

## Decision

- **Concepts.** Dataset, Record, Partition, Version, Run, Route and Delivery ledger, with invariants I1–I9 (design §4).
- **Change strategies:** `by_stamp`, `snapshot_diff`, `replace` and `push`.
- **Route modes:** `upsert`, `append`, `replace_partition` and `side_effect`.
- **Readiness groups** give consistent cuts. They gate only the flows that ask for them.

## Consequences

**Benefits:**
- Backfills and re-runs by business slice.
- Replay to new routes.
- Reads decoupled from writes.
- One product for both kinds of work.

**Costs:**
- More concepts than a simple pipeline tool. Defaults, templates and business-language screens hide them.
- Versions and the spool consume storage, which retention keeps in check (design §7.3).

## Alternatives considered

- **A row/cursor model (Airbyte-like).** It can't express slices, deletes or replay well.
- **A pure event log.** It is weak for bulk tables and slice verification.
- **Two products,** one for ETL and one for integration. This duplicates connectors and operations.
