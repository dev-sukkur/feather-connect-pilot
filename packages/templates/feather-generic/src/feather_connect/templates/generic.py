"""The five generic flow templates, as library functions (ruling 12): a project calls one with what it changes (its
connection names, a map, extra routes, a schedule) and gets the template's fixes when it upgrades. `feather new flow
--template` copies one into a project, as editable code, for full control. The template projects under `templates/`
call these functions, with fixture packs in the retail vocabulary: items, prices, invoices and orders.

- `db_to_warehouse`: a Postgres table to a DuckDB or MotherDuck bronze table, by a change stamp.
- `api_to_warehouse`: a paged REST API to a warehouse table, within the connection's budget.
- `file_drop_to_db`: files dropped in S3-compatible storage to a Postgres table, by the objects' modified time.
- `webhook_to_api`: signed webhooks, mapped, to an HTTP API, one request each.
- `fan_out_demo`: a JSON simulation to Postgres, DuckDB and HTTP: the `feather dev` demo.
"""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import feather_connect as fc
from feather_connect.connectors import duckdb, http, postgres, s3, simulation  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.context import Context
from feather_connect.core.flow import RecordMap, RouteSpec
from feather_connect.core.records import Record
from feather_connect.core.triggers import Trigger

__all__ = [
    "EVERY_15_MINUTES",
    "EVERY_MINUTE",
    "api_to_warehouse",
    "db_to_warehouse",
    "fan_out_demo",
    "file_drop_to_db",
    "partner_order",
    "webhook_to_api",
]

# Default schedules: frozen triggers, so one value can be shared by every call.
EVERY_MINUTE = fc.cron("* * * * *", tz="UTC")
EVERY_15_MINUTES = fc.every("15m")
Schedule = Trigger | Sequence[Trigger]


def _triggers(schedule: Schedule) -> list[Trigger]:
    return list(schedule) if isinstance(schedule, Sequence) else [schedule]


def db_to_warehouse(
    *,
    source: str = "app_db",
    destination: str = "warehouse",
    table: str = "items",
    key: str = "item_id",
    stamp: str = "updated_at",
    bronze_table: str | None = None,
    lookback: str = "0s",
    map: RecordMap | None = None,
    extra_routes: Sequence[RouteSpec] = (),
    schedule: Schedule = EVERY_15_MINUTES,
    name: str = "items-to-warehouse",
) -> fc.Flow:
    """A flow that copies a Postgres `table`'s new and changed rows, by the `stamp` column, into the warehouse's
    `bronze_table` (`bronze_<table>` by default), upserted by `key`. `lookback` reads again a little before the last
    watermark, for rows stamped out of order."""
    dataset = fc.Dataset(
        table,
        source=postgres.Query(source, table=table),
        key=key,
        strategy=fc.strategy.by_stamp(stamp, lookback=lookback),
    )
    sink = duckdb.Table(destination, table=bronze_table or f"bronze_{table}", key_columns=[key])
    return fc.Flow(
        name, reads=dataset, deliver=[fc.to(sink, map=map), *extra_routes], triggers=_triggers(schedule)
    )


def api_to_warehouse(
    *,
    source: str = "pricing_api",
    destination: str = "warehouse",
    path: str = "/v1/prices",
    records_path: str | None = "prices",
    dataset: str = "prices",
    key: str = "price_id",
    stamp: str = "updated_at",
    stamp_param: str = "updated_since",
    page_size: int = 100,
    has_more_path: str | None = "has_more",
    table: str | None = None,
    map: RecordMap | None = None,
    extra_routes: Sequence[RouteSpec] = (),
    schedule: Schedule = EVERY_15_MINUTES,
    name: str = "prices-to-warehouse",
) -> fc.Flow:
    """A flow that pages through a REST API's `path` (`?page=&per_page=`) for records changed since the last read,
    passing the watermark as `stamp_param`, and upserts them by `key` into the warehouse's `table` (`bronze_<dataset>`
    by default). Every call is metered by the connection's budget, so the flow keeps within the API's limits."""
    rest = http.Rest(
        source,
        path=path,
        records_path=records_path,
        pagination=http.Page(page_size=page_size, has_more_path=has_more_path),
        stamp_param=stamp_param,
    )
    read = fc.Dataset(dataset, source=rest, key=key, strategy=fc.strategy.by_stamp(stamp))
    sink = duckdb.Table(destination, table=table or f"bronze_{dataset}", key_columns=[key])
    return fc.Flow(
        name, reads=read, deliver=[fc.to(sink, map=map), *extra_routes], triggers=_triggers(schedule)
    )


def file_drop_to_db(
    *,
    source: str = "landing",
    destination: str = "app_db",
    path: str = "s3://landing/invoices/*.csv",
    format: s3.Format = "csv",
    dataset: str = "invoices",
    key: str = "invoice_id",
    table: str | None = None,
    stable_after: str = "2m",
    map: RecordMap | None = None,
    extra_routes: Sequence[RouteSpec] = (),
    schedule: Schedule = EVERY_15_MINUTES,
    name: str = "invoice-files-to-db",
) -> fc.Flow:
    """A flow that reads the files dropped at `path` since the last read, by their modified time (`_object_mtime`),
    and upserts their rows by `key` into the Postgres `table` (`dataset` by default). A file modified less than
    `stable_after` ago is left for the next run, and a row that does not parse goes to quarantine with its text."""
    files = s3.Files(source, path=path, format=format, stable_after=stable_after)
    read = fc.Dataset(dataset, source=files, key=key, strategy=fc.strategy.by_stamp(s3.MTIME_FIELD))
    sink = postgres.Table(destination, table=table or dataset, key_columns=[key])
    return fc.Flow(
        name, reads=read, deliver=[fc.to(sink, map=map), *extra_routes], triggers=_triggers(schedule)
    )


def partner_order(record: Record, ctx: Context) -> dict[str, Any]:
    """The default map of `webhook_to_api`: a partner's order event as an order for the API, in plain names."""
    lines: Sequence[Mapping[str, Any]] = record.get("items") or []
    return {
        "external_id": record["order_id"],
        "store": record.get("store_id"),
        "customer": record.get("customer"),
        "placed_at": record.get("placed_at"),
        "lines": [
            {"sku": line.get("sku"), "quantity": line.get("qty"), "price": line.get("price")}
            for line in lines
        ],
        "total": record.get("total"),
    }


def webhook_to_api(
    *,
    source: str = "partner",
    destination: str = "erp",
    dataset: str = "orders",
    key: str = "order_id",
    signature_secret: str = "signature_secret",
    records_path: str | None = None,
    event_id_path: str | None = "event_id",
    url: str = "/orders/{order_id}",
    method: str = "PUT",
    map: RecordMap | None = partner_order,
    response_map: Mapping[str, str] | None = None,
    extra_routes: Sequence[RouteSpec] = (),
    name: str = "partner-orders-to-erp",
) -> fc.Flow:
    """A flow that takes a partner's signed order webhooks, maps each order with `map` (`partner_order` by default),
    and sends it to the API at `url` with `method`, an upsert by the order's id, as each event arrives. The webhook is
    verified with `signature_secret`, an entry under the source connection's `auth`, and a repeat of an event (by
    `event_id_path`) is stored once."""
    hook = http.Webhook(
        source, signature_secret=signature_secret, records_path=records_path, event_id_path=event_id_path
    )
    read = fc.Dataset(dataset, source=hook, key=key, strategy=fc.strategy.push())
    call = http.Call(destination, method=method, url=url, response_map=response_map)
    return fc.Flow(
        name, reads=read, deliver=[fc.to(call, map=map), *extra_routes], triggers=[fc.on_event(read)]
    )


def fan_out_demo(
    *,
    fixture: str | Path,
    source: str = "shop",
    warehouse: str = "warehouse",
    lake: str = "lake",
    erp: str = "erp",
    faults: simulation.Faults = simulation.Faults(),  # noqa: B008 - frozen, so it is safe to share
    map: RecordMap | None = None,
    extra_routes: Sequence[RouteSpec] = (),
    schedule: Schedule = EVERY_MINUTE,
    name: str = "orders-fan-out",
) -> fc.Flow:
    """A flow that reads a JSON simulation of a shop's orders (`fixture`, with `faults` if any) and delivers each to
    three destinations: the `orders` table in Postgres, `bronze_orders` in DuckDB, and `PUT /orders/{order_id}` on an
    HTTP API. Each run diffs the fixture against what was read before, so only the orders that changed go out. `map`
    shapes the record for each of them."""
    orders = fc.Dataset(
        "orders",
        source=simulation.Source(source, fixture=fixture, faults=faults),
        key="order_id",
        strategy=fc.strategy.snapshot_diff(),  # each run sends only the orders that changed
    )
    routes = [
        fc.to(postgres.Table(warehouse, table="orders", key_columns=["order_id"]), map=map),
        fc.to(duckdb.Table(lake, table="bronze_orders", key_columns=["order_id"]), map=map),
        fc.to(http.Call(erp, method="PUT", url="/orders/{order_id}"), map=map),
    ]
    return fc.Flow(name, reads=orders, deliver=[*routes, *extra_routes], triggers=_triggers(schedule))
