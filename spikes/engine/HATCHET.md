# The Hatchet engine option

ADR-0003 chose DBOS Transact on 30 Oct 2026. This branch keeps Hatchet ready in case we need it later, so `main` doesn't have to carry it.

Everything here is spike quality and frozen as tested on that date. The full comparison is in [S1 engine results](../../docs/03-spikes/s1-engine-results.md).

## What this branch has that `main` doesn't

| Path | Contents |
|---|---|
| `spike/hatchet_engine.py` | The Hatchet adapter for the engine port |
| `compose.yaml`, profile `hatchet` | `hatchet-lite`, using its own database on the spike Postgres |
| `compose.hatchet-full.yaml` | The full reference stack: its own Postgres, RabbitMQ, migration and setup jobs, engine and dashboard |
| `tests/test_t14_stall_probe.py` | Probes for the engine stall: worker kill cycles and task bursts |
| `pyproject.toml`, `uv.lock` | `hatchet-sdk` 1.44.0 |

## Run it

Run these from `spikes/engine` in Git Bash. Hatchet's worker SDK needs Linux or macOS, so on Windows the workers run in containers (`SPIKE_WORKERS=docker`).

### Set up

Do this after switching from `main`, whose lock file and worker image have no `hatchet-sdk`.

```bash
docker compose up -d postgres
```

```bash
uv sync
```

```bash
docker build -f Dockerfile.worker -t feather-spike-worker .
```

### Hatchet lite

Start the container:

```bash
docker compose --profile hatchet up -d
```

Create a token. `.env` is git-ignored, so never commit it:

```bash
TOKEN=$(docker compose exec -T hatchet-lite /hatchet-admin token create --config /config --tenant-id 707d0855-80ab-4e1f-a156-f1c4546cbf52 2>/dev/null | tail -1 | tr -d '\r')
printf 'HATCHET_CLIENT_TOKEN=%s\nHATCHET_CLIENT_HOST_PORT=localhost:17077\n' "$TOKEN" > .env
```

Run the suite:

```bash
SPIKE_WORKERS=docker uv run pytest -v --engine hatchet
```

### Full deployment

Start the stack:

```bash
docker compose -f compose.hatchet-full.yaml up -d
```

Create a token. `.env.full` is git-ignored too:

```bash
TOKEN=$(MSYS_NO_PATHCONV=1 docker compose -f compose.hatchet-full.yaml run --rm --no-deps setup-config /hatchet/hatchet-admin token create --config /hatchet/config --tenant-id 707d0855-80ab-4e1f-a156-f1c4546cbf52 2>/dev/null | tail -1 | tr -d '\r')
printf 'HATCHET_CLIENT_TOKEN=%s\nHATCHET_CLIENT_HOST_PORT=localhost:27077\nHATCHET_CLIENT_SERVER_URL=http://localhost:28080\n' "$TOKEN" > .env.full
```

Run the suite. Workers reach the engine through Docker's host gateway:

```bash
SPIKE_WORKERS=docker SPIKE_ENV_FILE=.env.full SPIKE_DEPLOY=full SPIKE_HATCHET_WORKER_HOST_PORT=host.docker.internal:27077 SPIKE_HATCHET_WORKER_SERVER_URL=http://host.docker.internal:28080 SPIKE_T12_PG_URL=postgresql://hatchet:hatchet@127.0.0.1:5435/hatchet SPIKE_HATCHET_PG_URL=postgresql://hatchet:hatchet@127.0.0.1:5435/hatchet uv run pytest -v --engine hatchet
```

Reproducing the stall takes a stack that has already done a lot of work: run T13 three times, or the whole suite and then T13. Fresh stacks never failed. A stalled engine logs `context deadline exceeded` errors.

`SPIKE_HATCHET_IDEMPOTENCY=0` and `SPIKE_HATCHET_KEYED=0` turn off idempotency and per-key concurrency. We used them to check whether either feature triggers the stall, and neither did.

## What to expect

- **Good on a fresh stack:** 43–49 tasks/s drain with features on, automatic takeover after a crash, and no version gating on deploys.
- **Intermittent stall:** seen on both lite and full, only on stacks that had already run a lot. It persisted across engine restarts, and only a fresh database cleared it. The trigger is unknown.
- **Slower timing:** 30–36 s to recover from a crash (heartbeat timeout) and dispatch tails of about 10 s at p95.
- **Defaults to override:** `execution_timeout` is 60 s and `schedule_timeout` is 5 minutes.
- **Version pinning:** the binary reports only `v0.1.0-alpha.0`, so pin images by digest. The compose files here use `latest`, which is what we tested on 30 Oct 2026.

## To make Hatchet the production engine

Do the first two before committing to it:

1. **Raise the stall with the vendor.** Reproduce it, then file an issue with the engine and worker logs. Worker logs from our runs are in `results/logs/` on the machine that ran them and aren't in git, so expect to capture new ones.
2. **Soak test.** Run for several days on a production-like stack, with kills and bursts, and watch for the stall.

Then:

3. **Write the adapter against the product's engine port.** Use this one as a guide; don't copy it. It needs:
   - our own retry loop inside durable tasks, because native retries are fixed per task definition and our policy is per call;
   - a bridge from sync task bodies to Hatchet's async-only durable operations;
   - checkpoints for `ctx.chunk` in our own table, because Hatchet doesn't memoise plain code;
   - a decision on the first-admitted-FIFO behaviour under per-key concurrency (T1).
4. **Pass the conformance suite,** including T10's target of p95 ≤ 1 s, or accept a different target.
5. **Record the change:**
   - a new ADR that supersedes ADR-0003;
   - an exception to ADR-0005, because the full stack adds services beyond Postgres;
   - an update to ADR-0010, for the Compose file and the `feather dev` story on Windows.
