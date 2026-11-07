# feather-http

HTTP connectors for Feather Connect.

- `http.Rest`: a source that pages through a JSON API (`Offset`, `Page`, `Cursor` or `Link` pagination), with
  `records_path`, `by_stamp` through a query parameter, `replace`, and list-then-detail for APIs that list
  summaries and serve each record on its own.
- `http.Call`: a sink that sends one request per record, with an `Idempotency-Key`, and keeps fields of each
  response.

Both share Feather's HTTP client (`feather_connect.runtime.http`): API-key, basic, bearer and OAuth2 auth (client
credentials and refresh token, the token cached and refreshed once on a 401), retries of 429, 5xx and timeouts
with `Retry-After`, budget metering of every attempt, and redaction of secrets and tokens.

## The connection

Scheme settings go at the top level and credentials under `auth`, as secret references:

```yaml
budgets:
  zoho-api: {limits: ["90/minute", "1400/day"]}
connections:
  zakya:
    connector: http.rest
    budget: zoho-api
    base_url: https://api.zakya.in
    auth_scheme: oauth2_refresh_token   # none, api_key, basic, bearer, oauth2_client_credentials
    token_url: https://accounts.zoho.in/oauth/v2/token
    token_prefix: Zoho-oauthtoken       # the word before the token in Authorization
    params: {organization_id: "60001"}  # sent with every request
    auth:
      client_id: ${secret:env:ZAKYA_CLIENT_ID}
      client_secret: ${secret:env:ZAKYA_CLIENT_SECRET}
      refresh_token: ${secret:env:ZAKYA_REFRESH_TOKEN}
  erp:
    connector: http.call
    base_url: https://erp.example.com
    auth_scheme: bearer
    auth: {token: "${secret:env:ERP_TOKEN}"}
```

## A flow

```python
import feather_connect as fc
from feather_connect.connectors import http

invoices = fc.Dataset(
    "invoices",
    source=http.Rest(
        "zakya",
        path="/inventory/v1/invoices",
        records_path="invoices",
        pagination=http.Page("page", "per_page", page_size=200, has_more_path="page_context.has_more_page"),
        stamp_param="last_modified_time",
        stamp_format="%Y-%m-%dT%H:%M:%S%z",
        detail_path="/inventory/v1/invoices/{invoice_id}",
        detail_records_path="invoice",
    ),
    key="invoice_id",
    strategy=fc.strategy.by_stamp("last_modified_time"),
)

flow = fc.Flow(
    "invoices-to-erp",
    reads=invoices,
    deliver=[
        fc.to(http.Call("erp", method="PUT", url="/invoices/{invoice_id}", response_map={"erp_id": "id"}))
    ],
    triggers=[fc.cron("*/5 * * * *", tz="UTC")],
)
```

A `by_stamp` read with `Page` or `Offset` pages by stamp: after a full page it asks again from the first page with
the stamp filter set to the last record's stamp, skipping what it has read, so that a record changed during the read
(and so moved to the end of a list sorted by stamp) is not skipped. A read stops with an error on an empty page that
says more follow, a response without `has_more_path`, a cursor seen before, or a `Link` to another origin.

`http.Call` sends one request per record, `url` and `body` filled in from the record (templates such as
`/orders/{order_id}`, values percent-encoded, or functions of the record), with an `Idempotency-Key` made from the
chunk and the record's key. A record that changes is best sent as an upsert by its own id (`PUT /invoices/{id}`):
each version gets a new key, so a POST would create it again. A 2xx or a 409 (applied already) applies the record;
a 3xx, or a status in `fail_statuses` (401, 403, 404, 405 and 407 by default), fails the chunk and the delivery, as
every record would get the same; any other 4xx rejects the record and keeps the body it was sent with; a 429, a 5xx
or a timeout is retried. `response_map` keeps fields of each JSON answer by dotted path, a 409's too, which the
delivery's page on the dashboard shows per record. Deletes are rejected unless `on_delete="skip"`.

Each list call and each detail call takes one unit from the connection's budget (`cost_key` and
`detail_cost_key` name them in the spec's `cost`), retries included, and so does each OAuth2 token request. A wait of under a minute is slept off;
a longer one defers the task durably.
