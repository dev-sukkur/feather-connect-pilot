# ADR-0003: Durable engine — DBOS or Hatchet, chosen by a conformance spike

- **Status:** Proposed (decision due at the end of SDLC phase 3)
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor), after the spike report

## Context

The reference evaluation shortlisted two MIT-licensed, Postgres-based engines (discovery §9.2):

| Engine | Strengths | Weaknesses |
|---|---|---|
| Hatchet | Free self-hosted UI; native shared rate limits, concurrency keys, fan-out and webhooks; workers dial out over gRPC | Pre-1.0 releases; a 4 MB payload cap; work is re-queued after 30 s without a heartbeat; needs an extra container |
| DBOS Transact | A library on our own Postgres with no extra server | The ops UI is paid; rate limits count workflow starts; no native fan-out or webhooks; deploys pin workflow versions |

Two of our earlier decisions change the weighting:
- We build our own dashboard (ADR-0002), so DBOS's paid UI no longer matters.
- Budgets and scheduling are ours (design §6.6, §6.7), so Hatchet's native rate limits matter less.

The open questions are footprint, remote execution, durability under failure, and maturity.

## Decision (proposed)

1. Implement the `Engine` port, the `LocalEngine`, and both adapters at spike quality.
2. Run the 11-test engine conformance suite (design §6.8) against both. Passing all 11 is the gate.
3. Weigh the engines on:
   - ops footprint;
   - fit with the port (concurrency keys, durable waits, labels);
   - latency and throughput at the NFR targets;
   - remote-execution model (open question D2);
   - maturity and release stability;
   - licence and governance;
   - engine-level debugging tools (secondary).
4. Ship **one** production adapter. The other stays experimental or community-maintained.

## Consequences

**Benefits:** the choice rests on evidence, not opinion.

**Costs:**
- Roughly a week of spike work.
- Both adapters must avoid leaking engine specifics into the SDK.

## Spike evidence (30 Oct 2026)

- Results: [S1 engine results](../03-spikes/s1-engine-results.md).
- Score after the full-deployment re-test: DBOS 72, Hatchet lite 68, Hatchet full 65.
- **Recommendation: DBOS Transact,** on three conditions:
  - a version policy;
  - executor liveness and recovery in our sweeper;
  - polling sized to load.
- Hatchet, both lite and full, showed an intermittent engine stall that persisted across restarts, though fresh stacks never failed. It also has dispatch tails of about 10 s and 30–36 s failover.
- **Awaiting the sponsor's decision.** This ADR becomes Accepted when the sponsor decides.

## Re-verify before accepting

Re-verify the pricing, licence and feature facts copied from the reference evaluation on the vendors' pages at spike time.
