# S2 — Core model prototype on the hard cases (30 Oct 2026)

**Question:** do dataset → record → partition → version → route ledger (ADR-0004) hold up on the hard cases, independent of the engine?

**Probe:** a minimal Postgres implementation, [`spikes/engine/coremodel.py`](../../spikes/engine/coremodel.py), with tests in [`tests_core/`](../../spikes/engine/tests_core/test_hard_cases.py). It has:
- versions with a complete or incomplete flag;
- record state;
- run-level delete reconciliation;
- a per-route ledger with blocking, quarantine and supersede;
- the `upsert` and `replace_partition` modes;
- readiness.

## Results: 8 of 8 pass

| Case | Scenario | Measured | Verdict |
|---|---|---|---|
| H1a | A record moves partitions (edited document date), arriving incrementally | The move is an upsert with `moved_from`. Both an `upsert` and a `replace_partition` route end with one copy, in the new partition. | Pass |
| H1b | A record moves between two slices of the **same complete run** | Sealing slices one at a time made **1 transient delete + re-insert**. Run-level reconciliation made **0**. | Pass, **with a design change** |
| H2 | `snapshot_diff` on a stampless table | Version = 2 upserts (1 changed, 1 new) + 1 delete (hard-deleted). The unchanged row wasn't re-sent. | Pass |
| H3 | Source rewrites the key in place | Keyed by business identity: 1 row. Keyed by source id: 2 rows (the bug). | Pass |
| H4 | Lookback re-read of 3 rows, 2 unchanged | Version holds 1 upsert; 1 record sent | Pass |
| H6 | Destination down while extraction continues | 6 versions sealed while down. The first version per partition failed and the rest waited. After recovery, all 6 were applied in seal order. | Pass |
| H10 | Invalid record later fixed at the source | Quarantined first (the rest of the version applied), then **superseded** automatically by the newer version | Pass |
| H13 | Lines arrive before headers | Lines landed without waiting. The readiness gate flipped only when both datasets were complete for the day. | Pass |

## Design change from H1b

**Deletes implied by complete versions are reconciled per run, not per slice.**
- When a run reads several slices in full, a key counts as deleted only if it is absent from *every* slice the run read.
- Otherwise a record that moved between two slices is deleted and then re-inserted.
- That is harmless for a warehouse, but wrong for side-effect routes: an ERP posting could be reversed and re-posted.

Update design §4.2 (a new invariant) and §4.3:

> **I10.** Deletes implied by complete versions are decided after every slice of the run has been read. A key absent from one slice but present in another slice of the same run is a move, not a delete.

## Not covered by the prototype

- Concurrency between runs (the engine spike covers concurrency keys).
- The side-effect `compensate` path, and approvals.
- Schema evolution holds.
- Large volumes (S4 measures those separately).
