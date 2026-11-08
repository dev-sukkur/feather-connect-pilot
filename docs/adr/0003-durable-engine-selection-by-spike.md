# ADR-0003: Durable engine — DBOS Transact, chosen by a conformance spike

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** OSS Dev1 (sponsor), after the spike report

## Context

The reference evaluation shortlisted two MIT-licensed, Postgres-based engines (discovery §9.2):

| Engine | Strengths | Weaknesses |
|---|---|---|
| Hatchet | Free self-hosted UI; native shared rate limits, concurrency keys, fan-out and webhooks; workers dial out over gRPC | Pre-1.0 releases; a 4 MB payload cap; work is re-queued after 30 s without a heartbeat; needs an extra container |
| DBOS Transact | A library on our own Postgres with no extra server | The ops UI is paid; rate limits count workflow starts; no native fan-out or webhooks; deploys pin workflow versions |

Two of our earlier decisions change the weighting:
- We build our own dashboard (ADR-0002), so DBOS's paid UI matters less.
- Budgets and scheduling are ours (design §6.6, §6.7), so Hatchet's native rate limits matter less.

The open questions were footprint, remote execution, durability under failure, and maturity. We implemented the `Engine` port and an adapter for each engine at spike quality, then ran one conformance suite against both ([SP1 results](../03-spikes/s1-engine-results.md)).

## Decision

**DBOS Transact is the production engine, behind the `Engine` port (design §6.8).**

1. `DbosEngine` is the one production adapter. `LocalEngine` stays for tests and `feather run --local`.
2. We adopt DBOS on three conditions (design §6.12):
   - **A version policy:** a fixed application version per runtime major, `DBOS.patch` for in-flight compatibility, and `feather deploy` setting the latest version.
   - **Liveness and recovery in our sweeper:** worker heartbeats in our own tables, and recovery of a dead worker's tasks through DBOS's public client API.
   - **Polling sized to load:** start at 0.2–0.5 s, add workers, and track the idle cost.
3. On-prem agents use our gateway relay, so sites hold no database or engine credentials (answers design D2).
4. **Hatchet is kept on the git branch `side/hatchet-engine`,** ready if we need it later. The branch has the spike adapter, the lite and full stacks, the stall probes, and notes in `spikes/engine/HATCHET.md`. `main` doesn't carry it, and it isn't shipped.

## Why

| Criterion (weight) | DBOS | Hatchet lite | Hatchet full |
|---|---|---|---|
| Ops footprint (20) | 17 | 13 | 10 |
| Fit with the port (20) | 19 | 14 | 14 |
| Remote model and security (15) | 10 | 12 | 12 |
| Maturity and release stability (15) | 9 | 7 | 7 |
| Measured latency and throughput (10) | 8 | 5 | 5 |
| Debugging tools (10) | 4 | 9 | 9 |
| Licence and governance (5) | 3 | 5 | 5 |
| Workarounds needed (5) | 2 | 3 | 3 |
| **Total (100)** | **72** | **68** | **65** |

The score gap alone is small. These points decided it:
- **ADR-0005 stays literally true.** Postgres is the only moving part. Hatchet's full stack adds four long-running services and two setup jobs.
- **The port fits.** Priority, per-call retries, step checkpoints and synchronous task bodies work directly.
- **Recovery is fast and timing is tight.** 7 s to resume after a restart against about 30 s, and a p95 dispatch of 0.27 s against about 10 s.
- **Its gaps are in code we own.** The version policy and the sweeper belong in the control plane we already planned. Hatchet's gaps sit in the engine and SDK.
- **The stall.** Hatchet, lite and full, showed an intermittent engine stall with an unknown trigger. It persisted across engine restarts, and only a fresh database cleared it. Fresh stacks never failed and drained at 43–49 tasks/s, close to DBOS's 55. We saw it on one laptop, so it may not reproduce on a production server, but we can't rule that out.

**Found after scoring.** A follow-up test (T2c, T2d) showed that a dead worker's tasks can be recovered with public `DBOSClient` calls from outside any worker. The scores above still count an internal recovery API against DBOS, so they slightly understate it. The decision doesn't change.

## Consequences

**Benefits:**
- One backup covers everything, and there are no extra services to run.
- Workers ran natively on both Windows and Linux in the spike, so `feather dev` works on developer machines without WSL.
- Engine state lives next to our own tables.

**Costs:**
- We own the version policy and the sweeper. Until the sweeper resumes it, a dead worker's keyed task holds its key, so detection time matters (design §6.12).
- Idle load grows with workers: about 46 tx/s per worker at 0.2 s polling. Hatchet's stays flat.
- A worker dispatches at most slots ÷ polling interval. The DBOS default of 1.0 s is too slow for us (T10: p95 of 66 s at 50 tasks/s).
- There's no free engine-level UI. Our dashboard must show enough of what the engine is doing.
- DBOS gates some operational features behind paid tiers. Feather Connect is open source, so it can't depend on them.

**Revisit if:**
- we run many workers, around 20 or more, where per-worker polling load starts to matter;
- we want engine-level automatic takeover without our own sweeper;
- an adopting team values Hatchet's free UI for engine debugging over the extra services;
- DBOS's licence or its paid-tier boundary changes.

An organisation can still add DBOS's paid Conductor for engine debugging, recovery and automatic version upgrades. We just don't depend on it.

## Alternatives considered

- **Hatchet.** It passes the functional tests, takes over automatically from a dead worker, and has a free UI. We rejected it for now because of the stall, its footprint and its fit with the port. Before adopting it we would need a vendor issue on the stall and a multi-day soak test on a production-like stack (`HATCHET.md` on the branch).
- **Shipping both adapters.** This doubles the test matrix and the docs, for an option nobody needs yet. The port keeps a later switch possible.

## Vendor facts re-verified (30 Oct 2026)

This ADR required these to be re-checked on the vendors' pages before acceptance.

| Fact | Result | Source |
|---|---|---|
| DBOS Transact is MIT and needs no infrastructure beyond Postgres | Confirmed | [GitHub](https://github.com/dbos-inc/dbos-transact-py), and the 3.4.0 package metadata |
| Conductor: Pro $99/month, Teams $499/month | Confirmed. Pro lists distributed workflow recovery and automatic version upgrade. Alerting and RBAC start at Teams. | [DBOS pricing](https://www.dbos.dev/pricing) |
| Auto-upgrade is a paid feature | Confirmed. The docs call it a Transact Enterprise feature, and it needs a key. | [Upgrading workflows](https://docs.dbos.dev/python/tutorials/upgrading-workflows) |
| Hatchet is MIT | Confirmed | [GitHub](https://github.com/hatchet-dev/hatchet) |
| Hatchet's engine is pre-1.0 | Confirmed. The latest release is 0.114.2, of 26 Oct 2026. | [Releases](https://github.com/hatchet-dev/hatchet/releases) |
| 4 MB default payload limit | Confirmed | [Hatchet troubleshooting](https://docs.hatchet.run/v1/troubleshooting) |
| Work is re-queued after 30 s without a heartbeat | Confirmed, and measured (T2a) | [Hatchet FAQ](https://docs.hatchet.run/llms/v1/faq.md) |
| RabbitMQ is optional in the full stack | Confirmed. Postgres can be the broker. | [Hatchet Docker Compose](https://docs.hatchet.run/self-hosting/docker-compose) |

Not re-checked: DBOS's rate-limit semantics and its lack of native fan-out and webhooks. Budgets, fan-out and ingress are ours, so they no longer affect the decision.
