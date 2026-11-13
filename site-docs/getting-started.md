# Getting started

You need [uv](https://docs.astral.sh/uv/) and, for `feather dev`, Docker. From a checkout of the repository:

<!-- test -->
```bash
uv sync
uv run feather validate --project examples/demo
uv run feather run orders --project examples/demo --env qa --local --sim
```

`feather validate` checks the demo's flows, connections and budgets. `feather run --local --sim` runs the `orders`
flow once on this machine, on simulation stand-ins for every connection, and prints what each route applied. The
tests run both commands on every pull request, so this page stays true.

## The dashboard

```bash
uv run feather dev
```

`feather dev` starts Postgres in Docker, the server, a worker and the demo's targets. It prints the dashboard's
address and the `dev` user's password, and opens the dashboard. The demo reads a shop's orders every minute and
delivers them to a Postgres table, a DuckDB file and a small HTTP API. Look at the flow `orders`, its runs, and each
route's deliveries. Stop the API (the demo's README says how) and watch the other routes carry on, then retry the
failed delivery and see it catch up.

## Your own project

```bash
uv run feather new project my-hub
cd my-hub
uv run feather new flow orders --template api-to-warehouse
uv run feather validate
uv run feather test
```

Then [write a flow](write-a-flow.md) of your own, and [deploy](deploy.md) it.
