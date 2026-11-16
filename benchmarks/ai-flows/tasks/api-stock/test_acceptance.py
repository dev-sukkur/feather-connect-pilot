# pyright: reportMissingImports=false
from benchmark_accept import dataset_of, delivered, fixture, flow_reading, key_of, project


def test_the_inventory_api_is_read_within_its_budget():
    prod = project("prod")
    connection = prod.config["connections"]["inventory_api"]
    assert connection["connector"] == "http.rest"
    assert connection["base_url"] == "https://inventory.example.com"
    assert connection["budget"] == "inventory-api"
    assert "INVENTORY_API_KEY" in str(connection["auth"])
    assert sorted(prod.config["budgets"]["inventory-api"]["limits"]) == ["120/minute", "50000/day"]
    assert "pricing_api" not in prod.config["connections"]


def test_stock_levels_are_paged_by_their_change_stamp_into_bronze_stock():
    flow = flow_reading("stock", "prod")
    stock = dataset_of(flow, "stock")
    assert key_of(stock) == ["stock_id"]
    assert stock.strategy.name == "by_stamp"
    source = stock.source
    assert (source.connection, source.path, source.records_path, source.stamp_param) == (
        "inventory_api",
        "/v2/stock",
        "levels",
        "changed_since",
    )
    assert [r.sink.table for r in flow.routes] == ["bronze_stock"]


def test_every_level_is_delivered_in_dev():
    rows = fixture("stock")
    (sent,) = delivered(flow_reading("stock"), "stock", rows).values()
    assert sorted(r["stock_id"] for r in sent) == sorted(r["stock_id"] for r in rows)
