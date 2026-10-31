from typing import Any, ClassVar

import feather_connect as fc
from feather_connect.connectors import memory
from feather_connect.core.connector import ApplyBatch, ApplyResult, ConnectorSpec, RouteMode
from feather_connect.core.context import Context
from feather_connect.testing import FlowHarness

_MODES: frozenset[RouteMode] = frozenset({"upsert"})


class SapODataSource(memory.Source):
    spec: ClassVar[ConnectorSpec] = ConnectorSpec("sap.odata", strategies=frozenset({"snapshot_diff"}))

    def __init__(self, connection: str) -> None:
        super().__init__(connection)
        self.real_calls = 0

    def read(self, ctx: Context, req: Any) -> Any:
        self.real_calls += 1
        raise AssertionError("real connector called")


class _FakeSink(memory.Sink):
    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        raise AssertionError("real connector called")


class ZakyaItemPrices(_FakeSink):
    spec: ClassVar[ConnectorSpec] = ConnectorSpec("zakya.item_prices", modes=_MODES)


class MagentoPrices(_FakeSink):
    spec: ClassVar[ConnectorSpec] = ConnectorSpec("magento.prices", modes=_MODES)


class GofrugalItemPrices(_FakeSink):
    spec: ClassVar[ConnectorSpec] = ConnectorSpec("gofrugal.item_prices", modes=_MODES)


price = fc.Dataset(
    "sap.price",
    source=SapODataSource("sap"),
    key=("material", "price_list"),
    strategy=fc.strategy.snapshot_diff(),
)


def to_zakya(rec: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {"sku": rec["material"], "rate": rec["amount"]}


def to_magento(rec: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {"sku": rec["material"], "price": rec["amount"], "store_id": 0}


def to_gofrugal(rec: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {"item_code": rec["material"], "price": rec["amount"]}


flow = fc.Flow(
    "sap-price-to-channels",
    reads=price,
    deliver=[
        fc.to(ZakyaItemPrices(connection="zakya"), map=to_zakya, priority="low"),
        fc.to(MagentoPrices(connection="magento"), map=to_magento),
        fc.to(GofrugalItemPrices(connection="gofrugal"), map=to_gofrugal),
    ],
    triggers=[fc.cron("*/15 * * * *", tz="Asia/Kolkata"), fc.manual()],
    owner="retail-integration",
)


def test_prices_fan_out_and_deletes_propagate():
    h = FlowHarness(flow)  # every connection is swapped for an in-memory connector
    h.source("sap.price").load(
        [
            {"material": "M1", "price_list": "RET", "amount": 499.0},
            {"material": "M2", "price_list": "RET", "amount": 999.0},
        ]
    )
    h.run()
    assert h.sink("zakya").rows == [{"sku": "M1", "rate": 499.0}, {"sku": "M2", "rate": 999.0}]

    h.source("sap.price").load([{"material": "M1", "price_list": "RET", "amount": 549.0}])
    h.run()  # snapshot_diff: M1 updated, M2 deleted
    assert h.sink("magento").deleted_keys == [("M2", "RET")]
    assert h.ledger.all_applied()


def test_harness_does_not_call_real_connectors():
    h = FlowHarness(flow)
    h.source("sap.price").load([{"material": "M1", "price_list": "RET", "amount": 1.0}])
    h.run()
    assert h.sink("gofrugal").rows == [{"item_code": "M1", "price": 1.0}]
    assert price.source.real_calls == 0  # type: ignore[attr-defined]
    assert isinstance(h.sink("zakya"), memory.Sink)
    assert not isinstance(h.sink("zakya"), ZakyaItemPrices)


def test_harness_stand_ins_keep_the_spec_of_the_real_connectors():
    h = FlowHarness(flow)
    assert h.source("sap.price").spec.strategies == frozenset({"snapshot_diff"})
    assert h.sink("zakya").spec.modes == _MODES


def test_harness_exposes_quarantine_and_versions():
    h = FlowHarness(flow)
    h.source("sap.price").load(
        [
            {"material": "M1", "price_list": "RET", "amount": 1.0},
            {"material": "M2", "amount": 2.0},  # no price_list, so no key
        ]
    )
    run = h.run()
    assert [q.key for q in h.quarantine.list_open()] != []
    versions = h.versions("sap.price")
    assert [v.run_id for v in versions] == [run]
    assert versions[0].upserts == 1
