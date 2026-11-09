"""The sinks when an answer is lost after Zakya carried a call out, when an item id goes stale, and across tenants:
what was done is found again, never done twice."""

from collections.abc import Callable
from typing import Any

import pytest

from feather_connect.connectors import zakya  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.connectors.zakya.sinks import _ItemIds, same_value  # pyright: ignore[reportMissingImports]
from feather_connect.connectors.zakya.testing import ZakyaSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import ApplyBatch, ApplyRecord, RouteMode
from feather_connect.core.context import Context
from feather_connect.core.errors import TransientError

TEA = {"item_id": "i1", "sku": "TEA-250", "name": "Assam tea 250 g", "rate": 180.0, "stock_on_hand": 10}


def _batch(
    rows: list[dict[str, Any]], mode: RouteMode = "upsert", *, chunk: int = 0, version: str = "v1"
) -> ApplyBatch:
    records = [ApplyRecord(str(r["sku"]), "upsert", r) for r in rows]
    return ApplyBatch("r", version, chunk, "*", mode, records, f"r:{version}:{chunk}", True, True)


def _until_applied(sink: Any, ctx: Context, batch: ApplyBatch) -> Any:
    """Apply as the runtime does: a transient failure fails the chunk, and the chunk is sent again."""
    for _ in range(3):
        try:
            return sink.apply(ctx, batch)
        except TransientError:
            continue
    raise AssertionError("the chunk never went through")


@pytest.mark.parametrize("lost", [None, 504], ids=["connection-dropped", "gateway-timeout"])
def test_a_stock_adjustment_whose_answer_is_lost_moves_stock_once(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any], lost: int | None
):  # Stage Z review 1
    sim.seed("items", [dict(TEA)])
    sim.fail_after_applying(status=lost)
    sink = wire(zakya.Stock("zakya"))
    batch = _batch([{"sku": "TEA-250", "quantity_adjusted": -3}], "append")
    with pytest.raises(TransientError, match="may have been carried out"):
        sink.apply(ctx, batch)  # Zakya made the adjustment; the client did not post it again
    result = sink.apply(ctx, batch)  # the chunk's next try finds it by its reference
    assert result.applied == 1 and result.responses[0].status == 200
    assert sim.items["i1"]["stock_on_hand"] == 7 and len(sim.data["inventoryadjustments"]) == 1
    assert sim.calls.count("POST /inventory/v1/inventoryadjustments") == 1


def test_an_item_whose_create_answer_is_lost_is_found_by_sku_not_made_twice(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):  # Stage Z review 1
    sim.fail_after_applying()
    sink = wire(zakya.Items("zakya"))
    batch = _batch([{"sku": "OIL-1", "name": "Mustard oil 1 l", "rate": 210}])
    result = _until_applied(sink, ctx, batch)
    assert result.applied == 1 and result.rejected == []
    assert [i["sku"] for i in sim.items.values()] == ["OIL-1"]
    assert sim.calls.count("POST /inventory/v1/items") == 1


def test_an_item_made_by_someone_else_after_the_lookup_is_updated(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any], monkeypatch: pytest.MonkeyPatch
):
    sink = wire(zakya.Items("zakya"))
    real = sink.find_item
    looks: list[str] = []

    def racing(ctx: Context, value: str) -> Any:
        looks.append(value)
        if len(looks) == 1:  # nothing yet; then someone makes it before our POST
            sim.seed("items", [dict(TEA, sku=value, rate=100.0)])
            return None
        return real(ctx, value)

    monkeypatch.setattr(sink, "find_item", racing)
    result = sink.apply(ctx, _batch([{"sku": "TEA-250", "rate": 180}]))
    assert result.applied == 1 and result.rejected == [] and sim.items["i1"]["rate"] == 180


def test_numbers_are_compared_as_numbers_so_an_unchanged_record_sends_nothing(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):  # Stage Z review 12
    sim.seed("items", [dict(TEA)])
    sim.seed(
        "pricebooks",
        [
            {
                "pricebook_id": "pb1",
                "pricebook_type": "per_item",
                "pricebook_items": [{"item_id": "i1", "pricebook_rate": 640.0}],
            }
        ],
    )
    wire(zakya.Items("zakya")).apply(ctx, _batch([{"sku": "TEA-250", "rate": "180", "stock_on_hand": "10"}]))
    wire(zakya.Prices("zakya")).apply(
        ctx, _batch([{"sku": "TEA-250", "pricebook_id": "pb1", "rate": "640.00"}])
    )
    assert not [c for c in sim.calls if c.startswith("PUT")]
    assert same_value(180.0, "180") and same_value("1e2", 100) and not same_value("180", "180.5")
    assert not same_value(True, 1) and not same_value("TEA", "tea")


def test_a_stale_item_id_is_forgotten_and_looked_up_once_more(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):  # Stage Z review 3
    sim.seed("items", [dict(TEA)])
    sink = wire(zakya.Stock("zakya"))
    sink.apply(ctx, _batch([{"sku": "TEA-250", "quantity_adjusted": 1}], "append"))
    sim.remove("items", "i1")  # deleted and made again in Zakya, under a new id
    sim.data["items"]["i2"] = dict(TEA, item_id="i2", stock_on_hand=0)
    result = sink.apply(ctx, _batch([{"sku": "TEA-250", "quantity_adjusted": 2}], "append", version="v2"))
    assert result.applied == 1 and sim.items["i2"]["stock_on_hand"] == 2


def test_item_ids_are_trusted_for_a_while_only():  # Stage Z review 3
    at = [0.0]
    ids = _ItemIds(ttl=300, clock=lambda: at[0])
    scope = ("default", "zakya", "60001")
    ids.put(scope, "TEA-250", "i1")
    assert ids.get(scope, "TEA-250") == "i1" and ids.get(("t2", "zakya", "60001"), "TEA-250") is None
    assert ids.get(("default", "zakya", "70002"), "TEA-250") is None
    at[0] = 300
    assert ids.get(scope, "TEA-250") is None


@pytest.mark.parametrize(
    ("sink", "row", "mode"),
    [
        (lambda: zakya.Items("zakya"), {"sku": "TEA-250", "rate": 200}, "upsert"),
        (lambda: zakya.Prices("zakya", pricebook_id="pb1"), {"sku": "TEA-250", "rate": 650}, "upsert"),
        (lambda: zakya.Stock("zakya"), {"sku": "TEA-250", "quantity_adjusted": -1}, "append"),
    ],
    ids=["items", "prices", "stock"],
)
def test_a_second_tenant_keeps_its_own_token_item_ids_and_references(
    sim: ZakyaSim,
    make_ctx: Callable[..., Context],
    wire: Callable[[Any], Any],
    sink: Callable[[], Any],
    row: dict[str, Any],
    mode: RouteMode,
):  # Stage Z review 3; ADR-0013 rule 7
    sim.seed("items", [dict(TEA)])
    sim.seed("pricebooks", [{"pricebook_id": "pb1", "pricebook_type": "per_item", "pricebook_items": []}])
    one = wire(sink())
    first, second = make_ctx(), make_ctx(tenant="t2")
    one.apply(first, _batch([row], mode))
    lookups = sim.calls.count("GET /inventory/v1/items")
    one.apply(second, _batch([row], mode))
    assert sim.token_requests == 2  # a token each
    assert sim.calls.count("GET /inventory/v1/items") == 2 * lookups  # t2 found the item itself
    assert one.item_ids.get(("t2", "zakya", "60001"), "TEA-250") == "i1"
    if mode == "append":  # each tenant's delivery is its own adjustment, under its own reference
        assert len(sim.data["inventoryadjustments"]) == 2 and sim.items["i1"]["stock_on_hand"] == 8
