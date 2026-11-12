"""A flow per hard case. Each dataset reads `<case>.json` in FEATHER_HARD_FIXTURES, which the test rewrites between runs;
a simulation source reads a changed fixture again."""

import os
from pathlib import Path

import feather_connect as fc
from feather_connect.connectors import http, postgres, simulation  # pyright: ignore[reportAttributeAccessIssue]

# The install check loads the package with none of the host's variables: it reads an empty fixture then.
_EMPTY = Path(__file__).with_name("empty.json")
FIXTURES = Path(os.environ["FEATHER_HARD_FIXTURES"]) if "FEATHER_HARD_FIXTURES" in os.environ else None


def source(case: str) -> simulation.Source:
    return simulation.Source("shop", fixture=_EMPTY if FIXTURES is None else FIXTURES / f"{case}.json")


def table(name: str, key: str = "id") -> postgres.Table:
    return postgres.Table("warehouse", table=name, key_columns=[key])


def erp(connection: str) -> http.Call:
    return http.Call(connection, method="PUT", url="/orders/{id}")


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


# M2 plan 2's hard cases.

h5 = fc.Flow(  # a row stamped before the watermark is found by the scheduled reconcile
    "h5",
    reads=fc.Dataset(
        "h5.items", source=source("h5"), key="id", strategy=fc.strategy.by_stamp("u", lookback="1h")
    ),
    deliver=[fc.to(table("h5"), name="h5-upsert")],
    triggers=[fc.every("1d"), fc.reconcile(every="20s", lookback="400d")],
)
h7_reader = fc.Flow(  # H7: a high-priority flow and a low-priority one share the budget h7
    "h7-high",
    reads=fc.Dataset("h7.high", source=source("h7high"), key="id", strategy=fc.strategy.snapshot_diff()),
    deliver=[fc.to(erp("erp7high"), name="h7-high", priority="high")],
    triggers=[fc.every("1d")],
)
h7_writer = fc.Flow(
    "h7-low",
    reads=fc.Dataset("h7.low", source=source("h7low"), key="id", strategy=fc.strategy.snapshot_diff()),
    deliver=[fc.to(erp("erp7low"), name="h7-low", priority="low")],
    triggers=[fc.every("1d")],
)
h8 = fc.Flow(  # the budget runs out: the delivery waits for the next window, and does not fail
    "h8",
    reads=fc.Dataset("h8.items", source=source("h8"), key="id", strategy=fc.strategy.snapshot_diff()),
    deliver=[fc.to(erp("erp8"), name="h8-erp")],
    triggers=[fc.every("1d")],
)
h10 = fc.Flow(  # a record the ERP turned down is superseded by a newer version of its key
    "h10",
    reads=fc.Dataset(
        "h10.items", source=source("h10"), key="id", strategy=fc.strategy.snapshot_diff(), keep_state=True
    ),
    deliver=[fc.to(erp("erp10"), name="h10-erp")],
    triggers=[fc.every("1d")],
)
# H11: the test rewrites this line in its copy of the project and installs it again, deploying a route more.
H11_ROUTES = ["a"]
h11 = fc.Flow(
    "h11",
    reads=fc.Dataset("h11.items", source=source("h11"), key="id", strategy=fc.strategy.by_stamp("u")),
    deliver=[fc.to(table(f"h11_{r}"), name=f"h11-{r}") for r in H11_ROUTES],
    triggers=[fc.every("1d")],
)
h12 = fc.Flow(  # an added column applies, a dropped one holds the delivery
    "h12",
    reads=fc.Dataset("h12.items", source=source("h12"), key="id", strategy=fc.strategy.snapshot_diff()),
    deliver=[fc.to(table("h12"), name="h12-upsert")],
    triggers=[fc.every("1d")],
)
h15 = fc.Flow(  # new code is deployed while the deliveries drain, slowed by the budget h15
    "h15",
    reads=fc.Dataset(
        "h15.items",
        source=source("h15"),
        key="id",
        strategy=fc.strategy.snapshot_diff(),
        partition=fc.partition.by_bucket("id", 5),
    ),
    deliver=[fc.to(erp("erp15"), name="h15-erp")],
    triggers=[fc.every("1d")],
)
