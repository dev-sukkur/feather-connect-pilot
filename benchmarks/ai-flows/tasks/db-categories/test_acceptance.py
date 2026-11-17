# pyright: reportMissingImports=false
from benchmark_accept import dataset_of, delivered, fixture, flow_reading, key_of, trigger_kinds


def test_categories_reach_bronze_categories_hourly():
    prod = flow_reading("categories", "prod")
    assert key_of(dataset_of(prod, "categories")) == ["category_id"]
    assert [r.sink.spec.name for r in prod.routes] == ["duckdb.table"]
    assert prod.routes[0].sink.table == "bronze_categories"
    assert trigger_kinds(prod) == [{"kind": "every", "seconds": 3600.0}]


def test_every_category_is_delivered_in_dev():
    rows = fixture("categories")
    (sent,) = delivered(flow_reading("categories"), "categories", rows).values()
    assert sorted(r["category_id"] for r in sent) == [r["category_id"] for r in rows]
