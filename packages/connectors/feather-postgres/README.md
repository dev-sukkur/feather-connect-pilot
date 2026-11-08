# feather-postgres

Postgres connectors for Feather Connect: `postgres.Query`, a source that reads a table or a query, and
`postgres.Table`, a sink that writes a table by key.

## A connection

The DSN is a secret under `auth`; `schema` is where tables are looked up (`public` by default).

```yaml
connections:
  shop:
    connector: postgres.query
    auth: {dsn: "${secret:env:SHOP_DSN}"}
  warehouse:
    connector: postgres.table
    schema: sales
    auth: {dsn: "${secret:env:WAREHOUSE_DSN}"}
```

## A flow

```python
import feather_connect as fc
from feather_connect.connectors import postgres

orders = fc.Dataset(
    "orders",
    source=postgres.Query("shop", table="orders"),
    key="order_id",
    strategy=fc.strategy.by_stamp("updated_at", lookback="5m"),
    partition=fc.partition.by_day("order_date", tz="Asia/Kolkata"),
)

flow = fc.Flow(
    "orders-to-warehouse",
    reads=orders,
    deliver=[fc.to(postgres.Table("warehouse", table="orders", key_columns=["order_id"]), mode="upsert")],
    triggers=[fc.every("5m")],
)
```

`Query` reads through a server-side cursor in batches of `batch_rows` (10,000), and pushes a partition's bounds and
a `by_stamp` read's `since` down to the server as parameters. `sql="select ..."` reads a query instead of a table.

`Table` supports `upsert` (insert or update by `key_columns`; a delete removes the key), `append` and
`replace_partition`, which stages the chunks of a version in a table of its own and swaps the slice in one
transaction on the last chunk. It writes five columns of its own, `_fc_key`, `_fc_partition`, `_fc_version_id`,
`_fc_op` and `_fc_applied_at`, adding them to the table if they are missing (on an `append` route also `_fc_version`,
`_fc_run` and `_fc_extracted_at`, which the runtime stamps each row with), and keeps each chunk's result in
`_feather_applied`, so that a chunk sent again changes nothing.

The table must exist. A record with a field the table has no column for fails the version with an error naming the
column (DuckDB would add the column; Postgres does not). A row that breaks a constraint is rejected on its own and
quarantined; the rest of the chunk is written. Names are always sent as quoted identifiers.
