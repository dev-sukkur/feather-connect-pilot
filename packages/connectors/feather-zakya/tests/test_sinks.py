"""The Zakya sinks: items upserted by SKU, price list rates set once per price list, and stock adjustments posted once
by their reference number."""

import dataclasses
from collections.abc import Callable
from typing import Any

import pytest

from feather_connect.connectors import zakya  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.connectors.zakya.testing import ZakyaSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import ApplyBatch, ApplyRecord, RouteMode
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError


def _batch(rows: list[dict[str, Any]], mode: RouteMode = "upsert", key: str = "r:v1:0") -> ApplyBatch:
    records = [ApplyRecord(str(r.get("sku", r.get("id"))), "upsert", r) for r in rows]
    return ApplyBatch("r", "v1", 0, "*", mode, records, key, True, True)


def _writes(sim: ZakyaSim) -> list[str]:
    return [c for c in sim.calls if not c.startswith("GET")]


def test_items_are_created_then_updated_by_sku_and_left_alone_when_unchanged(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):
    sink = wire(zakya.Items("zakya"))
    first = sink.apply(
        ctx, _batch([{"sku": "TEA-250", "name": "Tea", "rate": 180}, {"sku": "OIL-1", "name": "Oil"}])
    )
    assert first.applied == 2 and [r.status for r in first.responses] == [201, 201]
    tea = next(i for i in sim.items.values() if i["sku"] == "TEA-250")
    again = sink.apply(ctx, _batch([{"sku": "TEA-250", "name": "Tea", "rate": 180}]))
    assert again.applied == 1 and _writes(sim) == ["POST /inventory/v1/items"] * 2  # unchanged: nothing sent
    sink.apply(ctx, _batch([{"sku": "TEA-250", "name": "Tea", "rate": 190}]))
    assert _writes(sim)[-1] == f"PUT /inventory/v1/items/{tea['item_id']}"
    assert sim.items[tea["item_id"]]["rate"] == 190 and len(sim.items) == 2


def test_an_item_record_that_cant_be_matched_is_rejected(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):
    sim.seed("items", [{"item_id": "a", "sku": "DUP"}, {"item_id": "b", "sku": "DUP"}])
    sink = wire(zakya.Items("zakya"))
    batch = _batch([{"id": "x", "name": "no sku"}, {"sku": "DUP", "name": "twice"}])
    result = sink.apply(
        ctx, dataclasses.replace(batch, records=[*batch.records, ApplyRecord("gone", "delete", None)])
    )
    assert result.applied == 0
    assert [(r.key, r.error_class) for r in result.rejected] == [
        ("x", "MissingField"),
        ("DUP", "ZakyaError"),
        ("gone", "DeleteNotSupported"),
    ]


def test_a_wrong_credential_fails_the_chunk(sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]):
    sim.fail_next(403, times=5)
    with pytest.raises(PermanentError, match="the chunk fails"):
        wire(zakya.Items("zakya")).apply(ctx, _batch([{"sku": "TEA-250", "name": "Tea"}]))


def test_prices_are_set_once_per_price_list(sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]):
    sim.seed("items", [{"item_id": "i1", "sku": "TEA-250"}, {"item_id": "i2", "sku": "OIL-1"}])
    sim.seed(
        "pricebooks",
        [
            {
                "pricebook_id": "pb1",
                "pricebook_type": "per_item",
                "pricebook_items": [{"item_id": "i2", "pricebook_rate": 99.0}],
            },
            {"pricebook_id": "pb2", "pricebook_type": "fixed_percentage"},
        ],
    )
    sink = wire(zakya.Prices("zakya"))
    rows = [
        {"pricebook_id": "pb1", "sku": "TEA-250", "rate": 175},
        {"pricebook_id": "pb1", "sku": "OIL-1", "rate": 205.5},
        {"pricebook_id": "pb1", "sku": "NONE", "rate": 1},
        {"pricebook_id": "pb2", "sku": "TEA-250", "rate": 1},
    ]
    result = sink.apply(ctx, _batch(rows))
    assert result.applied == 2
    assert sorted((r.key, r.error_class) for r in result.rejected) == [
        ("NONE", "UnknownItem"),
        ("TEA-250", "NotPerItem"),
    ]
    assert _writes(sim) == ["PUT /inventory/v1/pricebooks/pb1"]
    assert sim.data["pricebooks"]["pb1"]["pricebook_items"] == [
        {"item_id": "i1", "pricebook_rate": 175.0},
        {"item_id": "i2", "pricebook_rate": 205.5},
    ]
    sink.apply(ctx, _batch(rows[:2]))
    assert _writes(sim) == ["PUT /inventory/v1/pricebooks/pb1"]  # the same rates: nothing sent


def test_stock_is_adjusted_once_per_record_whatever_the_repeats(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):
    sim.seed("items", [{"item_id": "i1", "sku": "TEA-250", "stock_on_hand": 10}])
    rows = [{"sku": "TEA-250", "quantity_adjusted": -3, "warehouse_id": "w1"}]
    batch = _batch(rows, "append")
    first = wire(zakya.Stock("zakya")).apply(ctx, batch)
    assert first.applied == 1 and first.responses[0].status == 201
    assert (
        wire(zakya.Stock("zakya")).apply(ctx, batch).responses[0].status == 200
    )  # a new instance: found, not posted
    (adjustment,) = sim.data["inventoryadjustments"].values()
    assert adjustment["reference_number"] == zakya.Stock.reference(batch, batch.records[0])
    assert adjustment["line_items"] == [{"item_id": "i1", "quantity_adjusted": -3, "warehouse_id": "w1"}]
    assert adjustment["date"] == "2026-11-07" and sim.items["i1"]["stock_on_hand"] == 7
    other = wire(zakya.Stock("zakya")).apply(
        ctx, _batch(rows, "append", key="r:v2:0")
    )  # another version: posted
    assert other.applied == 1 and sim.items["i1"]["stock_on_hand"] == 4
    unknown = wire(zakya.Stock("zakya")).apply(
        ctx, _batch([{"sku": "NONE", "quantity_adjusted": 1}], "append")
    )
    assert [r.error_class for r in unknown.rejected] == ["UnknownItem"]
