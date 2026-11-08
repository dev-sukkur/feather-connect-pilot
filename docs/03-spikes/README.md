# Phase 3 — Spikes

The spikes settle the riskiest open questions in the design with evidence, before the implementation plan is written.

**Rules:**
- Spike code lives in [`spikes/`](../../spikes/README.md). It is **throwaway** and is never imported by product code.
- Each spike ends with a findings note in this folder and a recommendation.

| Spike | Question | Probe | Decides | Status |
|---|---|---|---|---|
| SP1 Engine | DBOS or Hatchet? | The same conformance suite ([spec](s1-engine-spike.md)) against an adapter for each engine | ADR-0003; design D1, D2 | **Done, DBOS chosen**: [results](s1-engine-results.md) |
| SP2 Core model | Do dataset, version and ledger hold up on the hard cases? | A minimal prototype on Postgres, run against hard cases H1–H4, H6, H10 and H13 | ADR-0004 | **Done**: [results](s2-core-model.md) |
| SP3 Connectors | What do the Zakya, GoFrugal, SAP and Magento APIs allow? | Desk research of public docs; live probes once sandbox access exists | Connector scope; discovery Q2–Q4; design D8 | **Desk research done**: [notes](s3-connectors.md). Live probes still need access. |
| SP4 Postgres spool and `feather dev` | Can Postgres hold the spool at the target throughput? Can `feather dev` run without Docker? | Benchmark of zstd Parquet chunks in a day-partitioned table | ADR-0005; NFR-02; design D3, D5 | **Done**: [results](s4-postgres-spool.md) |

## Day 1 outcome (30 Oct 2026)

**SP1 Engine:** DBOS passes all tests with three conditions that fit our control plane: a version policy, our own dead-worker sweeper, and polling sized to load.

Hatchet was **re-tested on its full reference deployment** at the sponsor's request:
- **Fresh stacks:** good, draining at 43–49 tasks/s with no errors in 9 bursts and 24 kills.
- **The stall:** an **intermittent engine stall** (first seen on `hatchet-lite`) also hit the full deployment. It persisted across engine restarts and its trigger is unknown.
- **Other gaps remain:** dispatch tails of about 10 s, 30–36 s failover, and workers that need Linux.

Score: DBOS 72, Hatchet lite 68, Hatchet full 65. **The sponsor chose DBOS** and asked to keep Hatchet on the branch `side/hatchet-engine`.

During the decision, a follow-up test (T2c, T2d) showed that a dead worker's tasks can be recovered through DBOS's public client API, so no internal API is needed.

**SP2 Core model:** all 8 hard cases pass. One design change: deletes implied by complete versions must be reconciled per run (new invariant I10).

**SP3 Connectors:**
- **SAP:** OData first and `snapshot_diff` for prices are confirmed. The official Python RFC binding is archived.
- **Zakya:** needs per-minute and per-day budget windows.
- **Magento:** bulk writes are confirmed later, which needs an "accepted, confirm later" sink result.
- **GoFrugal:** has no public API docs and is **blocked on the vendor**.

**SP4 Postgres spool:** validated with a wide margin:
- 154,000–332,000 rows/s written;
- 54 bytes per record;
- a 1-million-row diff in 0.34 s;
- a partition dropped in 0.02 s.

One format, Parquet, serves batch and push alike.

## Decisions and follow-ups

1. **Engine (ADR-0003):** accepted on 30 Oct 2026. DBOS, on three conditions, with Hatchet kept on a side branch.
2. **Design changes:** applied in [design v0.2](../02-design/2026-10-30-feather-connect-design.md):
   - **I10:** run-level delete reconciliation (§4.2, §4.3).
   - **Sink result "accepted, confirm later":** for Magento-style bulk APIs (§8.1, §8.4).
   - **Spool sizing:** about 0.3 GB a day at 5 million records (§7.2).
   - **DBOS operating rules:** a fixed app version per runtime major, worker heartbeats and recovery in the sweeper, and a polling setting (§6.12).
   - **Budget windows:** per minute and per day for Zoho (§6.6).
3. **GoFrugal (sponsor action):** request API documentation and a sandbox from the vendor. The connector is now "M2 if the docs arrive by M1's end, otherwise M3".
4. **Next:** phase 4, the [implementation plan](../04-plan/README.md).

## Environment

| Item | Value |
|---|---|
| Machine | Windows 11 Pro, 8 cores, Docker Desktop (WSL2) |
| Python and tools | Python 3.13 (managed by uv; 3.14 is also installed), uv 0.12 |
| Spike Postgres | Its own container on `127.0.0.1:25432`. Left running for review; stop it with `docker compose stop` in `spikes/engine`. |
| Spike Hatchet | Both stacks are stopped, with their volumes kept. Their code is on the branch `side/hatchet-engine`. |
| Untouched | The machine's PostgreSQL 18 service on port 5432, and other projects' containers |

## Limits of a single-machine spike

These are noted wherever they affect a result:
- "Behind NAT" and real network failures are simulated on one host.
- Throughput numbers come from a developer laptop, with other containers running, not the reference deployment.
- Crash tests use `docker kill` (SIGKILL), or a forced termination of the process tree for host workers.
