# The AI-flow benchmark (G2)

Discovery goal G2 says flows are code that developers *and AI* can write, review and test, and sets the measure:
on a benchmark task set, at least 80% of AI-generated flows, started from a template, pass `feather validate` and
their tests with no manual fixes. This folder is that task set and the runner that scores it.

## What is here

| Path | What it holds |
|---|---|
| `tasks/<task>/task.yaml` | The template the task starts from, a title, and the prompt: what to build, in a user's words |
| `tasks/<task>/fixtures/` | Sample rows the task brings, laid over the template's `fixtures/` |
| `tasks/<task>/test_acceptance.py` | The hidden test: does the flow do what the task asked? The AI never sees it |
| `accept.py` | What the acceptance tests share (the project, a flow by the dataset it reads, a run on fixture rows) |
| `reference/<task>/` | A reference answer, which shows the task can be done and the acceptance test is right |
| `run.py` | Writes the prompts, and scores a set of answers |

Ten tasks, two on each of the five templates:

| Task | Template | What it asks |
|---|---|---|
| `db-categories` | db-to-warehouse | Another table (`categories`), another bronze table, hourly in every environment |
| `db-items-clean` | db-to-warehouse | A map: SKU trimmed and upper case, a column left out, one added |
| `api-stock` | api-to-warehouse | Another API: its connection, auth, budget, paging, change stamp and filter |
| `api-prices-paise` | api-to-warehouse | A map with decimal rounding (half up) |
| `file-returns` | file-drop-to-db | Another path and format (JSON lines), dataset and table |
| `file-invoices-warehouse` | file-drop-to-db | A second destination with its own connection, simulated in dev and qa |
| `webhook-returns` | webhook-to-api | Another event type, endpoint and map |
| `webhook-orders-post` | webhook-to-api | Another method and endpoint; the template's map extended |
| `fan-out-channel` | fan-out-demo | A map every destination gets |
| `fan-out-schedule` | fan-out-demo | A rename and another schedule |

Several tasks change what the environment overlays name (a flow renamed, a connection replaced), so an answer
passes only if it carries the change into `env/*.yaml` as well: the kind of follow-through a reviewer would ask for.

## Running it

1. Write the prompts, one Markdown file per task:

   ```bash
   uv run python benchmarks/ai-flows/run.py prompts out/prompts
   ```

2. Give each prompt to the AI under test, in a fresh session with a copy of this repository **without the
   `benchmarks/ai-flows/reference` and `tasks/*/test_acceptance.py` files**, so it can read the template, the
   library and the docs, but not the answers. Save what it writes for each task under `answers/<task>/`, laid out as
   in the project (`flows/…`, `feather.yaml`, `env/…`, `tests/test_*.py`). Don't fix anything by hand.

3. Score the answers:

   ```bash
   uv run python benchmarks/ai-flows/run.py score answers --report out/g2.json
   ```

Each task is scored on a fresh copy of its template, without the template's own tests, with the task's fixtures
and the answer laid over it. It passes when every environment (dev, qa, uat, prod) validates with no error, the
answer brings tests of its own and they pass, and the hidden acceptance test passes. The run prints each task's
result and the stage it stopped at (`answer`, `validate`, `tests` or `acceptance`), writes the JSON report, and
exits 1 when the pass rate is below the target (`--target`, 0.8 by default). `--task <name>` scores one task;
`--jobs` sets how many run at once.

`score --reference` scores the reference answers: all ten pass. `tests/benchmarks` runs that on every CI build,
and checks that each acceptance test fails the template left as it was, so a task can't be passed by doing nothing.

## Adding a task

Add `tasks/<task>/` with its `task.yaml`, any fixtures and `test_acceptance.py` (import what it needs from
`benchmark_accept`), and `reference/<task>/` with an answer and its tests (`feather new flow` scaffolds a test that
fits). Then `score --reference --task <task>` should pass, and the template left as it was should fail it.
