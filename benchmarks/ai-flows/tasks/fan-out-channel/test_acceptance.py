# pyright: reportMissingImports=false
from benchmark_accept import delivered, fixture, flow_reading


def test_every_destination_gets_each_order_with_its_channel():
    rows = fixture("orders")
    sent = delivered(flow_reading("orders"), "orders", rows)
    assert len(sent) == 3
    for route, got in sent.items():
        assert sorted(r["order_id"] for r in got) == sorted(r["order_id"] for r in rows), route
        assert all(r["channel"] == "web" for r in got), route
        first = next(r for r in got if r["order_id"] == rows[0]["order_id"])
        assert {k: first[k] for k in rows[0]} == rows[0], route
