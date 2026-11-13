# Performance results

M3 plan 1 P1: NFR-01 (latency), NFR-02 (throughput) and NFR-03 (capacity), each with the machine it ran on, since a
shared CI runner and a laptop give different numbers. CI's `perf` job measures them on every pull request that touches
the product (NFR-03 nightly), and fails a number that misses its target or is more than 20% worse than its machine's
baseline in `perf-baseline.json`, taking the better of two runs.

## On CI: GitHub's `ubuntu-latest` runner

From the green run of PR 30's last commit (4f5263b, 12 Nov 2026), before the `perf` job had a runner of its own. These
are the runner's baseline.

| NFR | Number | Result | Target |
|---|---|---|---|
| 01 | Webhook to sink, p95 at 50 events/s for 60 s | **1.18 s** (p50 0.69 s, max 1.39 s), 3,000 of 3,000, no duplicates | ≤ 5 s |
| 01 | Webhook to an HTTP call through the real connectors, p95 | **1.41 s** (p50 0.85 s), 3,000 of 3,000, no repeats | ≤ 5 s |
| 01 | A 10× burst: lost, and time until p95 is back under target | **0 lost** of 18,260; **0 s** to recover | nothing lost; ≤ 300 s |
| 02 | Source batch to spool, one worker, 500,000 rows | **24,258 rows/s** | ≥ 20,000 |
| 03 | 5 million records a day | measured nightly from this release on | in ≤ 2 h of a day |

## On the development machine

Windows 11 laptop, 8 CPUs, Postgres 17 in Docker Desktop with other containers running, 13 Nov 2026.

| NFR | Number | Result | Target |
|---|---|---|---|
| 02 | Source batch to spool, after a warm-up | 14,693 rows/s: **misses** | ≥ 20,000 |
| 03 | 5 million records (10 batches of 500,000), read, spooled, sealed and applied | **449 s**, about 960 million records a day | 5 million a day, in ≤ 7,200 s |

The laptop's NFR-02 number is below target, while CI's is above it. The same laptop stalled other tests that day
(tests that take 5 s timed out at 120 s), and its Postgres runs in Docker Desktop's VM with other busy containers. The
target is for a worker on the *small* deployment (discovery §8), and CI's runner is the reference machine; the release
records CI's number on the release commit. NFR-03 has more than a hundredfold room on either machine.

## Repeat it

```bash
FEATHER_PERF_RESULTS=perf.jsonl FEATHER_PERF_MACHINE=my-machine FEATHER_PERF_CAPACITY=1 \
  uv run pytest tests/perf tests/e2e/test_webhook_latency.py tests/e2e/test_webhook_to_http_latency.py -s
uv run python tests/perf/compare.py perf.jsonl            # --update makes this run the machine's baseline
```
