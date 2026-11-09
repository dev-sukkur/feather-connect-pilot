# feather-zakya

The Zoho Zakya connector. `zakya.source` reads one organisation's invoices, credit notes, payments, items and price
lists. `zakya.items`, `zakya.prices` and `zakya.stock` write its items, price list rates and stock adjustments.

## Connection

```yaml
budgets:
  zoho-api: {limits: ["90/minute", "1400/day"], account: "zakya:60001"}
connections:
  zakya:
    connector: zakya            # one connection serves zakya.source, .items, .prices and .stock
    budget: zoho-api
    domain: in                  # com, in or eu
    organization_id: "60001"
    auth:
      client_id: ${secret:env:ZAKYA_CLIENT_ID}
      client_secret: ${secret:env:ZAKYA_CLIENT_SECRET}
      refresh_token: ${secret:env:ZAKYA_REFRESH_TOKEN}
```

| Setting | Default | Meaning |
|---|---|---|
| `organization_id` | required | Sent as `organization_id` on every call |
| `domain` | `com` | The organisation's Zoho region: the API host (`api.zakya.com`, `.in`, `.eu`) and the accounts host (`accounts.zoho.com`, `.in`, `.eu`) |
| `base_url`, `accounts_url` | from `domain` | Override either host, for a simulator or a new region |
| `client_id`, `client_secret`, `refresh_token` | required | Secret references only. A Zoho API console *Self Client* of the region, and a refresh token made with the scopes the flows need |
| `timeout`, `retry_attempts` | as `http.rest` | Per call |

Auth uses the OAuth refresh-token flow. An access token is kept in memory per (tenant, connection) (ADR-0013 rule 7). It is
refreshed a minute before it expires, and once more on a 401. Tokens and secrets never reach logs or the audit table.

## Budgets

Zoho counts calls per minute and per day across every client of an organisation. Declare both windows a little below
Zoho's, and name the organisation as the budget's `account`. Every budget that names the same account takes from one
set of windows, whatever tenant or name it has (ADR-0013 rule 8). A 429 is waited out for its `Retry-After`. When the
daily cap is spent, Zoho answers until midnight in the organisation's time zone, and the task waits until then.

## Datasets

`zakya.dataset(name, connection="zakya", *, full=False, lookback="2m", page_size=200)` reads with page/per_page and
stops when `page_context.has_more_page` is false.

| Dataset | Path | Key | Stamp | Detail |
|---|---|---|---|---|
| `invoices` | `/invoices` | `invoice_id` | `last_modified_time` | `/invoices/{invoice_id}`, for line items |
| `credit_notes` | `/creditnotes` | `creditnote_id` | `last_modified_time` | `/creditnotes/{creditnote_id}` |
| `payments` | `/customerpayments` | `payment_id` | `last_modified_time` | none |
| `items` | `/items` | `item_id` | `last_modified_time` | none |
| `price_lists` | `/pricebooks` | `pricebook_id` | none: always in full | `/pricebooks/{pricebook_id}`, for rates |

A dataset with a stamp is read `by_stamp`, sorted by the stamp ascending, from `lookback` before the last watermark. A
record read again unchanged is dropped, so the overlap costs calls but never a duplicate. `full=True` (or no stamp)
reads with `snapshot_diff`, which sees deletes. `zakya.Source(...)` also takes `replace`.

## Sinks

| Connector | Mode | Idempotent by |
|---|---|---|
| `zakya.Items(connection, map=, match="sku")` | upsert | The SKU: GET `/items?sku=`, then POST if new, PUT if changed, nothing if unchanged |
| `zakya.Prices(connection, rate_field="rate", pricebook_field=, pricebook_id=)` | upsert | One GET and one PUT per price list and chunk, merging `pricebook_items` by item; an unchanged list is not written |
| `zakya.Stock(connection, quantity_field=, warehouse_field=, reason=)` | append | `reference_number` = `FC-` + a hash of the delivery's idempotency key and the record key; looked up before posting |

Deletes are rejected. A 401, 403, 405 or a missing organisation fails the chunk; other Zakya errors reject the record.
`Prices` rejects a price list that is not `per_item`, and a SKU it cannot find.

## Simulator

`feather_connect.connectors.zakya.testing.ZakyaSim` plays an organisation as an httpx transport, or over HTTP with
`running(sim)`. It covers the token endpoint, access token expiry (401, code 57), per-minute and per-day caps (429 with
`Retry-After`), paging and the `last_modified_time` filter. It also has `fail_next(status, times, retry_after)` and
random faults. `python -m feather_connect.connectors.zakya.testing --port 29100` serves it; the Zakya demo uses it.

## The live exit run

`tests/test_live.py::test_zakya_invoices_reach_motherduck` reads a real test organisation's invoices into MotherDuck
twice. It is marked `live`, and skipped unless these are set:

```
ZAKYA_LIVE_ORGANIZATION_ID  ZAKYA_LIVE_CLIENT_ID  ZAKYA_LIVE_CLIENT_SECRET  ZAKYA_LIVE_REFRESH_TOKEN
MOTHERDUCK_TOKEN  ZAKYA_LIVE_MOTHERDUCK_DB (such as md:feather_live)
optional: ZAKYA_LIVE_DOMAIN (com), ZAKYA_LIVE_DAILY_CAP (1000)

uv run pytest packages/connectors/feather-zakya/tests/test_live.py -m live -v -s
```

## Open questions

These need checking against a real organisation:

- The API host for the eu region.
- Which list endpoints honour the `last_modified_time` filter.
- Zoho's exact codes for the per-minute and daily caps.
- Whether `/inventoryadjustments` filters by `reference_number`.
- Whether price lists other than `per_item` need support.
