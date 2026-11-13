# Feather Connect

An open-source, code-first integration hub: near-real-time one-to-many data movement, ETL and application
integration. Flows are plain Python; one dashboard runs, monitors and operates them.

- **One model for ETL and events**: datasets, partitions, versions and routes. A batch read and a webhook burst
  become versions alike, and each route delivers them on its own ledger.
- **Exactly what changed, once**: each route applies a version once, in order, per partition. A crash mid-apply
  resumes where it stopped; a destination that is down holds up only its own route.
- **Operate it**: re-run a business slice, replay history to one route, approve postings, compensate one, trace a
  record. Every change is audited with who and why.
- **Run it anywhere**: `feather dev` on a laptop; Docker Compose with Postgres as the only database.
- **MIT licensed.**

Start with [Getting started](getting-started.md), then [Concepts](concepts.md) and [Write a flow](write-a-flow.md).
Operators go to the [Operating Guide](operating.md) and the [runbooks](runbooks/failed-deliveries.md).
