# pyright: reportMissingImports=false
from benchmark_accept import delivered, fixture, flow_reading


def test_items_reach_the_warehouse_cleaned():
    rows = fixture("items")
    (sent,) = delivered(flow_reading("items"), "items", rows).values()
    by_id = {r["item_id"]: r for r in sent}
    assert sorted(by_id) == ["ITM-0001", "ITM-0002", "ITM-0003"]
    assert [by_id[i]["sku"] for i in sorted(by_id)] == ["TEA-250", "RICE-5K", "SOAP-100"]
    assert all("uom" not in r for r in sent)
    assert all(r["source_system"] == "app_db" for r in sent)
    assert by_id["ITM-0002"]["name"] == "Basmati rice 5 kg" and by_id["ITM-0002"]["category"] == "staples"
    assert by_id["ITM-0001"]["updated_at"] == "2026-11-07T08:01:00Z"
