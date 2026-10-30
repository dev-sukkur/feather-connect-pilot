# S4 — Postgres as the spool, and `feather dev` (30 Oct 2026)

**Question:** can Postgres alone hold the spool (ADR-0005) at the target throughput, and what does that cost in size, diff time and retention?

**Probe:** [`spikes/engine/spool_bench.py`](../../spikes/engine/spool_bench.py), run against Postgres 17 in Docker on the developer laptop (8 cores, 7.7 GB for the Docker VM).
- **Data:** 1,000,000 synthetic invoice-line rows with 14 columns plus `_fc_key`, `_fc_op` and `_fc_row_hash`.
- **Storage:** zstd-compressed Parquet chunks of 50,000 rows, in a `bytea` table partitioned by day.

## Results

| Measure | Result | Target or estimate | Verdict |
|---|---|---|---|
| Write, 1 writer | 154,000 rows/s (8.4 MB/s compressed) | NFR-02: ≥ 20,000 rows/s per worker | ~8× the target |
| Write, 4 writers | 288,000–332,000 rows/s | — | Scales with writers |
| Read back to Arrow | 316,000–385,000 rows/s | — | Fast |
| Size on disk | **54–56 bytes per record** (2.7 MB per 50k-row chunk) | Design estimated 100–300 | Smaller than assumed |
| `snapshot_diff`, 1M rows (1% changed, 0.5% deleted, 0.5% new) | **0.34 s** to find 14,962 upserts and 5,000 deletes. Snapshot is **16.5 bytes per row**. | Design: 16 bytes per row | As designed |
| Retention: drop a day partition (57 MB) | **0.019 s** | — | Cheap, with no vacuum debt |
| Push micro-batch of 10 events: Parquet chunk insert | p50 9.2 ms, p95 17.7 ms | Within the 5 s latency budget | — |
| Same batch as JSONB rows | p50 7.8 ms, p95 19.0 ms | — | Same cost |

## What this means

- **ADR-0005 holds with a wide margin.** At 5 million records a day, the spool grows by about **0.28 GB a day**, or roughly **4 GB at 14-day retention**. The design's sizing guide (§7.2) can drop from "7–21 GB" to about 4 GB.
- **Design D5 is answered.** Parquet chunks for push micro-batches cost the same as JSONB rows, so **one storage format** is enough and no inline-JSONB path is needed.
- **Snapshot diffs are cheap.** Diffing 1 million rows takes 0.3 s and 16 MB of state, so `snapshot_diff` is viable for large stampless tables partitioned into slices of a few million rows.

## `feather dev` without Docker (design D3): desk notes only

- **DBOS** needs only Postgres, so `feather dev` would need Docker, or an embedded Postgres, for nothing beyond the database.
- **Hatchet's** Python SDK has an *embedded sidecar* mode (`EmbeddedHatchetConfig`, observed in hatchet-sdk 1.44). It downloads a Hatchet binary and can manage its own Postgres data directory. Another downloaded binary is one more moving part on developer machines.
- **Not tested today.** Recommendation: keep Docker as the `feather dev` default, and revisit an embedded Postgres after M1.
