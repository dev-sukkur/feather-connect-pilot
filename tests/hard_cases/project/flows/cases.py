"""A flow per hard case. Each dataset reads `<case>.json` in FEATHER_HARD_FIXTURES, which the test rewrites between runs;
a simulation source reads a changed fixture again."""

import os
from pathlib import Path

import feather_connect as fc
from feather_connect.connectors import http, postgres, simulation  # pyright: ignore[reportAttributeAccessIssue]

FIXTURES = Path(os.environ["FEATHER_HARD_FIXTURES"])


def source(case: str) -> simulation.Source:
    return simulation.Source("shop", fixture=FIXTURES / f"{case}.json")


def table(name: str, key: str = "id") -> postgres.Table:
    return postgres.Table("warehouse", table=name, key_columns=[key])


h1 = fc.Flow(  # a record moves from one day to another
    "h1",
    reads=fc.Dataset(
        "h1.items",
        source=source("h1"),
        key="id",
        strategy=fc.strategy.replace(),
        partition=fc.partition.by_day("day", tz="UTC"),
    ),
    deliver=[
        fc.to(table("h1_upsert"), name="h1-upsert"),
        fc.to(table("h1_replace"), mode="replace_partition", name="h1-replace"),
    ],
    triggers=[fc.every("1d")],
)
h2 = fc.Flow(  # a table with no stamp: inserts, updates and hard deletes come from diffing snapshots
    "h2",
    reads=fc.Dataset("h2.items", source=source("h2"), key="id", strategy=fc.strategy.snapshot_diff()),
    deliver=[fc.to(table("h2"), name="h2-upsert")],
    triggers=[fc.every("1d")],
)
h3 = fc.Flow(  # the source rewrites its own row id, the business key stays
    "h3",
    reads=fc.Dataset("h3.items", source=source("h3"), key="sku", strategy=fc.strategy.snapshot_diff()),
    deliver=[fc.to(table("h3", key="sku"), name="h3-upsert")],
    triggers=[fc.every("1d")],
)
h4 = fc.Flow(  # rows read again inside the lookback must go out once
    "h4",
    reads=fc.Dataset(
        "h4.items", source=source("h4"), key="id", strategy=fc.strategy.by_stamp("u", lookback="1h")
    ),
    deliver=[fc.to(table("h4"), mode="append", name="h4-append")],
    triggers=[fc.every("1d")],
)
h6 = fc.Flow(  # one destination is down while the others take the read
    "h6",
    reads=fc.Dataset("h6.items", source=source("h6"), key="id", strategy=fc.strategy.snapshot_diff()),
    deliver=[
        fc.to(table("h6"), name="h6-warehouse"),
        fc.to(http.Call("erp", method="PUT", url="/orders/{id}"), name="h6-erp"),
    ],
    triggers=[fc.every("1d")],
)
