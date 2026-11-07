# webhook-to-api

Webhook → map → HTTP.

A delivery partner posts each order as a webhook signed with HMAC-SHA256. The flow verifies it against the
secret, stores a repeated event (by `event_id`) once, maps the order with `partner_order` into the ERP's shape,
and upserts it with `PUT /orders/{order_id}` as events arrive. A 409 counts as applied; another 4xx rejects the
order and keeps the body it was sent with.

## Run it

`feather run --local --sim` runs the flow on this machine with the `dev` overlay: every connection the
overlay names as `simulation.source` or `simulation.sink` runs on a stand-in of the real connector, with
`fixtures/<dataset>.json` as its rows, so nothing outside is touched. `qa` does the same; `uat` and `prod` use
the real systems. `uv run pytest tests` runs the template's own tests.

## What to change for real systems

Set `PARTNER_WEBHOOK_SECRET` (what the partner signs with), the ERP's `base_url` and `ERP_TOKEN`. Other headers or
a body that carries several orders are `webhook_to_api(records_path=..., ...)`; another body shape is a `map`.

The flow is a call to a library function in `feather_connect.templates.generic`, so a fix to the template
reaches the project when it upgrades. Pass the function what differs (connection names, a `map`,
`extra_routes`, a `schedule`), or copy it in with `feather new flow --template webhook-to-api` for full control.
