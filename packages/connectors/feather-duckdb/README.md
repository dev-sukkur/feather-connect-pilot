# feather-duckdb

DuckDB and MotherDuck connectors for Feather Connect: `duckdb.Query`, a source that reads the rows of a query, and
`duckdb.Table`, a sink that writes a table from Arrow batches.

## A connection

`database` is a file path, or `md:<name>` for MotherDuck, whose token is a secret under `auth`. The token goes to
DuckDB in its config, never in the connection string, and is masked in every error.

```yaml
connections:
  lake:
    connector: duckdb.table
    database: md:analytics
    auth: {token: "${secret:env:MOTHERDUCK_TOKEN}"}
```

## A flow

```python
import feather_connect as fc
from feather_connect.connectors import duckdb, memory

orders = fc.Dataset(
    "orders",
    source=memory.Source("shop", rows=[{"order_id": "1", "total": 10}]),
    key="order_id",
    strategy=fc.strategy.replace(),
)

flow = fc.Flow(
    "orders-to-lake",
    reads=orders,
    deliver=[fc.to(duckdb.Table("lake", table="orders", key_columns=["order_id"]), mode="upsert")],
    triggers=[fc.every("15m")],
)
```

`Table` supports `upsert` (`insert or replace` when `key_columns` are the table's primary key, otherwise `merge`;
a delete removes the key), `append` and `replace_partition`, which stages a version's chunks in a table of their own
and swaps the slice in one transaction on the last chunk. Each write goes as Arrow batches of at most `write_rows`.
It writes the `_fc_key`, `_fc_partition`, `_fc_version_id`, `_fc_op` and `_fc_applied_at` columns and keeps each
chunk's result in `_feather_applied`, so that a chunk sent again changes nothing.

It creates a missing table from the first batch. An append adds a column that a record has and the table lacks; an
upsert or a replace fails naming the column instead (Postgres fails in every mode). Another process holding the
database file gives a `TransientError`, which is retried.

`Query(sql=...)` reads Arrow batches of `batch_rows`, with a partition's bounds and a `by_stamp` read's `since`
added as parameters.
