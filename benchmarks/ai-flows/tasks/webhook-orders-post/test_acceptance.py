# pyright: reportMissingImports=false
from benchmark_accept import fixture, flow_reading, mapped


def test_orders_are_posted_to_the_new_endpoint():
    (route,) = flow_reading("orders", "prod").routes
    assert (route.sink.connection, route.sink.method, route.sink.url) == ("erp", "POST", "/v2/orders")


def test_each_order_keeps_its_shape_with_its_channel_and_a_numeric_total():
    (route,) = flow_reading("orders", "prod").routes
    rows = fixture("orders")
    sent = mapped(route, rows, ["order_id"])
    assert [s["external_id"] for s in sent] == [r["order_id"] for r in rows]
    assert all(s["channel"] == "partner" for s in sent)
    assert [s["total"] for s in sent] == [float(r["total"]) for r in rows]
    assert all(isinstance(s["total"], float) for s in sent)
    first = sent[0]
    assert first["lines"] == [{"sku": "TEA-250", "quantity": 2, "price": "145.00"}]
    assert (first["store"], first["customer"], first["placed_at"]) == (
        "BLR-01",
        "Asha Traders",
        "2026-11-07T09:11:00Z",
    )
