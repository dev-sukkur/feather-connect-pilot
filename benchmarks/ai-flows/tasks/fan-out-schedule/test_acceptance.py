# pyright: reportMissingImports=false
from benchmark_accept import delivered, fixture, flow_reading, trigger_kinds


def test_the_flow_is_shop_orders_every_five_minutes():
    flow = flow_reading("orders", "prod")
    assert flow.name == "shop-orders"
    assert trigger_kinds(flow) in (
        [{"kind": "every", "seconds": 300.0}],
        [{"kind": "cron", "expr": "*/5 * * * *", "tz": "UTC"}],
    )


def test_it_still_reaches_all_three_destinations():
    rows = fixture("orders")
    sent = delivered(flow_reading("orders"), "orders", rows)
    assert len(sent) == 3 and all(len(got) == len(rows) for got in sent.values())
