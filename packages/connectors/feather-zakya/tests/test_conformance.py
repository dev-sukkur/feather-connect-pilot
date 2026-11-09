"""The Zakya source and sinks against the conformance kit, each in front of the simulator."""

from collections.abc import Callable, Mapping
from typing import Any, ClassVar

import pytest

from feather_connect.connectors import zakya  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.connectors.zakya.testing import ZakyaSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import RouteMode
from feather_connect.core.context import Context
from feather_connect.testing.conformance import SinkConformance, SourceConformance

KIT_ITEMS = [{"item_id": f"i-k{n}", "sku": f"k{n}", "name": f"item {n}"} for n in range(40)]


class _Faults:
    @pytest.fixture
    def ctx(self, make_ctx: Callable[..., Context]) -> Context:
        return make_ctx()

    @pytest.fixture
    def transient_fault(self, sim: ZakyaSim) -> Callable[[], None]:
        return lambda: sim.fail_next(503, times=100)

    @pytest.fixture
    def permanent_fault(self, sim: ZakyaSim) -> Callable[[], None]:
        return lambda: sim.fail_next(401, times=100)  # a new token doesn't help: the chunk fails


class TestSource(_Faults, SourceConformance):
    @pytest.fixture
    def source(self, wire: Callable[[Any], Any]) -> Any:
        return wire(zakya.Source("zakya", "items"))

    @pytest.fixture
    def seed(self, sim: ZakyaSim) -> Callable[[list[dict[str, Any]]], None]:
        return lambda rows: sim.seed("items", rows)

    @pytest.fixture
    def large_read_rows(self) -> int:
        return 20_000  # 100 pages; the kit's 200,000 would be 1,000 calls of the simulator

    @pytest.fixture
    def permanent_fault(self, sim: ZakyaSim) -> Callable[[], None]:
        return lambda: sim.fail_next(400, times=100)


class _KitItems:
    @pytest.fixture(autouse=True)
    def _items(self, sim: ZakyaSim) -> None:
        sim.seed("items", KIT_ITEMS)
        sim.seed("pricebooks", [{"pricebook_id": "pb1", "name": "Retail", "pricebook_type": "per_item"}])

    @staticmethod
    def sku_of(sim: ZakyaSim, item_id: str) -> str:
        return str(sim.items[item_id]["sku"])


class TestItems(_Faults, SinkConformance):
    deletes_supported = False  # an item is ended in Zakya, not by a flow

    @pytest.fixture
    def fresh_sink(self, wire: Callable[[Any], Any]) -> Callable[[], Any]:
        return lambda: wire(zakya.Items("zakya", map=lambda r: {**r, "sku": r["id"]}))

    @pytest.fixture
    def sink(self, fresh_sink: Callable[[], Any]) -> Any:
        return fresh_sink()

    @pytest.fixture
    def read_back(self, sim: ZakyaSim) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        return lambda: {i["sku"]: dict(i) for i in sim.items.values()}


class TestPrices(_Faults, _KitItems, SinkConformance):
    deletes_supported = False

    @pytest.fixture
    def fresh_sink(self, wire: Callable[[Any], Any]) -> Callable[[], Any]:
        return lambda: wire(
            zakya.Prices("zakya", map=lambda r: {"sku": r["id"], "rate": r["n"]}, pricebook_id="pb1")
        )

    @pytest.fixture
    def sink(self, fresh_sink: Callable[[], Any]) -> Any:
        return fresh_sink()

    @pytest.fixture
    def read_back(self, sim: ZakyaSim) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        def held() -> dict[str, dict[str, Any]]:
            book = sim.data["pricebooks"]["pb1"]
            return {self.sku_of(sim, i["item_id"]): dict(i) for i in book.get("pricebook_items", [])}

        return held


class TestStock(_Faults, _KitItems, SinkConformance):
    deletes_supported = False
    default_mode: ClassVar[RouteMode] = "append"

    @pytest.fixture
    def fresh_sink(self, wire: Callable[[Any], Any]) -> Callable[[], Any]:
        return lambda: wire(
            zakya.Stock("zakya", map=lambda r: {"sku": r["id"], "quantity_adjusted": r["n"] + 1})
        )

    @pytest.fixture
    def sink(self, fresh_sink: Callable[[], Any]) -> Any:
        return fresh_sink()

    @pytest.fixture
    def read_back(self, sim: ZakyaSim) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        def held() -> dict[str, dict[str, Any]]:
            found: dict[str, dict[str, Any]] = {}
            for adjustment in sim.data["inventoryadjustments"].values():
                for line in adjustment["line_items"]:
                    found[self.sku_of(sim, line["item_id"])] = dict(adjustment)
            return found

        return held
