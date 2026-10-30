# Engine spike (throwaway)

This folder runs the 11-test conformance suite from [docs/03-spikes/s1-engine-spike.md](../../docs/03-spikes/s1-engine-spike.md) against spike adapters for DBOS and Hatchet.

## Run

1. Start the spike Postgres on `127.0.0.1:25432`:

   ```bash
   docker compose up -d postgres
   ```

2. Install the dependencies:

   ```bash
   uv sync
   ```

3. Run the suite against DBOS:

   ```bash
   uv run pytest -v --engine dbos
   ```

4. Run the suite against Hatchet. This needs the `hatchet-lite` container first:

   ```bash
   docker compose --profile hatchet up -d
   ```

   ```bash
   uv run pytest -v --engine hatchet
   ```

## Layout

| Path | Contents |
|---|---|
| `spike/port.py` | The `Engine` port from design §6.8, trimmed to what the tests need |
| `spike/dbos_engine.py`, `spike/hatchet_engine.py` | One adapter per engine |
| `spike/journal.py` | The `spike.exec_log` and `spike.effects` tables that every test asserts on |
| `spike/worker.py` | The worker process entry point; tests start and kill it |
| `tests/` | `test_t01_*.py` … `test_t11_*.py` |

The Postgres credentials in `compose.yaml` are local-only throwaway values for this container.
