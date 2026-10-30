# ADR-0005: Postgres as the only required database; object storage optional; no Kafka in v1

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor)

## Context

The sponsor set three directions:
- Postgres is the database;
- the latency target is seconds, at moderate volume;
- the system must be easy to run and maintain.

Every extra stateful service adds deployment, backup, security and upgrade work.

## Decision

- **Postgres ≥ 15, with no extensions, holds:**
  - the catalogue;
  - runs and tasks;
  - the delivery ledger;
  - record state;
  - the audit trail;
  - the engine's state;
  - the **spool**, as zstd-compressed Parquet chunks in daily-partitioned tables (design §7.2).
- **Object storage** (S3-compatible, Azure Blob, GCS) is an **optional** spool backend, for bulk-heavy datasets and long archives.
- **No** Redis, Kafka, SQLite or second database.

## Consequences

**Benefits:**
- One stateful system to run, back up and secure.
- One point-in-time-recovery backup covers everything.
- Any managed Postgres works.

**Costs:**
- Postgres grows with spool volume. This is managed by dropping partitions for retention, compressing chunks, and switching bulk datasets to the object-storage backend.
- The throughput ceiling is lower than a log broker's.

## Revisit when

- sustained load exceeds about 1,000 events/s per environment; or
- the spool grows beyond about 200 GB; or
- retention needs can't be met economically in Postgres.

At that point, evaluate object storage by default or a Kafka/Redpanda transport.

## Alternatives considered

- **Postgres plus a required object store.** More moving parts for small installs.
- **Kafka or Redpanda.** Heavy to run, and more than seconds-level latency needs.
- **Redis queues.** Another stateful service, with weaker durability.
