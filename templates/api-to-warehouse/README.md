# api-to-warehouse

Paged REST API → warehouse, within a budget.

Every 15 minutes it pages through `/v1/prices?page=&per_page=` with `updated_since` set to the last watermark,
and upserts each price by `price_id` into `bronze_prices`. Every call takes a unit of the `pricing-api` budget
(60 a minute, 20,000 a day), so the flow waits rather than overrun the API's limits.

## Run it

`feather run --local --sim` runs the flow on this machine with the `dev` overlay: every connection the
overlay names as `simulation.source` or `simulation.sink` runs on a stand-in of the real connector, with
`fixtures/<dataset>.json` as its rows, so nothing outside is touched. `qa` does the same; `uat` and `prod` use
the real systems. `uv run pytest tests` runs the template's own tests.

## What to change for real systems

Set `base_url`, the auth scheme and `PRICING_API_KEY`, and the budget's limits to the API's. An API that pages
or filters otherwise is `api_to_warehouse(path=..., records_path=..., stamp_param=..., has_more_path=...)`.

The flow is a call to a library function in `feather_connect.templates.generic`, so a fix to the template
reaches the project when it upgrades. Pass the function what differs (connection names, a `map`,
`extra_routes`, a `schedule`), or copy it in with `feather new flow --template api-to-warehouse` for full control.
