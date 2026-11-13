# Write a flow

A flow is Python. This one reads prices and delivers them to two shops, each with its own map. The tests run this
page's example on every pull request.

```python
from typing import Any

import feather_connect as fc
from feather_connect.connectors import memory
from feather_connect.testing import FlowHarness

prices = fc.Dataset(
    "prices",
    source=memory.Source("erp"),
    key="sku",
    strategy=fc.strategy.snapshot_diff(),  # adds, changes and deletes against the last read
)


def to_shop(rec: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {"sku": rec["sku"], "price": rec["amount"]}


def to_pos(rec: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {"item_code": rec["sku"], "rate": rec["amount"]}


flow = fc.Flow(
    "prices-to-channels",
    reads=prices,
    deliver=[
        fc.to(memory.Sink("shop"), map=to_shop),
        fc.to(memory.Sink("pos"), map=to_pos),
    ],
    triggers=[fc.cron("*/15 * * * *", tz="Asia/Kolkata"), fc.manual()],
    owner="retail",
)

# Test it with every connection swapped for an in-memory one.
harness = FlowHarness(flow)
harness.source("prices").load([{"sku": "A1", "amount": 499.0}, {"sku": "B2", "amount": 999.0}])
harness.run()
assert harness.sink("shop").rows == [{"sku": "A1", "price": 499.0}, {"sku": "B2", "price": 999.0}]

harness.source("prices").load([{"sku": "A1", "amount": 549.0}])
harness.run()  # A1 changed, B2 deleted
assert harness.sink("pos").deleted_keys == ["B2"]
assert harness.ledger.all_applied()
```

## Join a reference, derive a dataset

A dataset with `keep_state=True` can be read by other flows as a reference. A SQL transform joins it to each chunk
it delivers, and a derived dataset is a query over references, read on its own triggers:

```python
materials = fc.Dataset(
    "sap.material", source=memory.Source("sap"), key="material",
    strategy=fc.strategy.snapshot_diff(), keep_state=True,
)
lines = fc.Dataset("zakya.line", source=memory.Source("zakya"), key="line", strategy=fc.strategy.replace())
enrich = fc.sql(
    "select b.*, m.description from batch b left join {{ dataset('sap.material') }} m using (material)"
)
master = fc.Flow("materials", reads=materials, deliver=[fc.to(memory.Sink("lake"), name="materials-lake")])
order_lines = fc.Flow("lines", reads=lines, deliver=[fc.to(memory.Sink("staging"), name="staging", map=enrich)])

h = FlowHarness(order_lines, also=[master])
h.source("sap.material").load([{"material": "M1", "description": "Steel bar"}])
h.run(flow="materials")
h.source("zakya.line").load([{"line": 1, "material": "M1"}, {"line": 2, "material": "M9"}])
h.run()
assert {r["line"]: r["description"] for r in h.sink("staging").rows} == {1: "Steel bar", 2: None}

effective = fc.Dataset(
    "pricing.material_count",
    source=fc.derived("select count(*) as n, 'all' as k from {{ dataset('sap.material') }}"),
    key="k",
    strategy=fc.strategy.snapshot_diff(),
)
counts = fc.Flow("counts", reads=effective, deliver=[fc.to(memory.Sink("report"), name="report")])
h = FlowHarness(counts, also=[master])
h.source("sap.material").load([{"material": "M1", "description": "Steel bar"}])
h.run(flow="materials")
h.run()
assert h.sink("report").rows == [{"n": 1, "k": "all"}]
```

In SQL, `{{ today }}` and `{{ now }}` are the time of the read or the delivery, in the derived dataset's `tz=`; from
Python, `ctx.lookup("sap.material", key)` and `ctx.reference("sap.material")` read the same state. A connection's
`windows:` in `feather.yaml` says when it may be read or written; work outside it waits. The
[Operating Guide](operating-guide/index.html#m31-flows) has the details.

Put the flow in `flows/` of a project, its connections in `feather.yaml` and `env/<env>.yaml`, and its secrets as
references (`${secret:env:NAME}`, or the environment's encrypted store). Then:

- `feather validate` checks it, `feather test` runs your tests, and `feather run <flow> --local --sim` runs it here.
- `feather package build` and `feather package publish` make it a versioned package for qa, uat and prod.

The [templates](reference/templates.md) are flows like this one, ready to use. The [connectors](reference/connectors.md)
are what a dataset reads and a route writes.
