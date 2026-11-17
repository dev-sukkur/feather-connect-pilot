# pyright: reportMissingImports=false
from benchmark_accept import delivered, fixture, flow_reading, project


def test_invoices_go_to_postgres_and_the_warehouse():
    flow = flow_reading("invoices", "prod")
    assert sorted((r.sink.spec.name, r.sink.table, r.sink.connection) for r in flow.routes) == [
        ("duckdb.table", "bronze_invoices", "warehouse"),
        ("postgres.table", "invoices", "app_db"),
    ]
    warehouse = project("prod").config["connections"]["warehouse"]
    assert (warehouse["connector"], warehouse["database"]) == ("duckdb.table", "md:bronze")
    assert "MOTHERDUCK_TOKEN" in str(warehouse["auth"])
    for env in ("dev", "qa"):
        assert project(env).config["connections"]["warehouse"]["connector"] == "simulation.sink", env


def test_both_destinations_get_every_invoice_in_dev():
    rows = fixture("invoices")
    sent = delivered(flow_reading("invoices"), "invoices", rows)
    assert len(sent) == 2
    for route, got in sent.items():
        assert sorted(r["invoice_id"] for r in got) == sorted(r["invoice_id"] for r in rows), route
