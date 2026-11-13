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

Put the flow in `flows/` of a project, its connections in `feather.yaml` and `env/<env>.yaml`, and its secrets as
references (`${secret:env:NAME}`, or the environment's encrypted store). Then:

- `feather validate` checks it, `feather test` runs your tests, and `feather run <flow> --local --sim` runs it here.
- `feather package build` and `feather package publish` make it a versioned package for qa, uat and prod.

The [templates](reference/templates.md) are flows like this one, ready to use. The [connectors](reference/connectors.md)
are what a dataset reads and a route writes.
