# Engine spike (throwaway)

This folder runs the conformance suite from [docs/03-spikes/s1-engine-spike.md](../../docs/03-spikes/s1-engine-spike.md) against the spike adapter for DBOS, the engine ADR-0003 chose.

The Hatchet adapter, its deployments and the stall probes are on the branch `side/hatchet-engine`. Start with `spikes/engine/HATCHET.md` on that branch.

## Run

1. Start the spike Postgres on `127.0.0.1:25432`:

   ```bash
   docker compose up -d postgres
   ```

2. Install the dependencies:

   ```bash
   uv sync
   ```

3. Run the suite:

   ```bash
   uv run pytest -v
   ```

4. Optionally, run the workers in Linux containers instead of as host processes. Build the image once, then set `SPIKE_WORKERS`:

   ```bash
   docker build -f Dockerfile.worker -t feather-spike-worker .
   ```

   ```bash
   SPIKE_WORKERS=docker uv run pytest -v
   ```

## Layout

| Path | Contents |
|---|---|
| `spike/port.py` | The `Engine` port from design §6.8, trimmed to what the tests need |
| `spike/dbos_engine.py` | The DBOS adapter |
| `spike/journal.py` | The `spike.exec_log` and `spike.effects` tables that every test asserts on |
| `spike/worker.py` | The worker process entry point; tests start and kill it |
| `tests/` | `test_t01_*.py` … `test_t13_*.py` |
| `coremodel.py`, `tests_core/` | S2: the core-model prototype and its hard cases |
| `spool_bench.py` | S4: the Postgres spool benchmark |
| `results/` | The raw facts behind the findings notes, for both engines. Worker logs stay out of git. |

The Postgres credentials in `compose.yaml` are local-only throwaway values for this container.
