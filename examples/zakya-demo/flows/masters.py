"""SAP's master data to the sales channel: items and retail prices, from the ERP to Zakya (UC-03).

- Items: `sap-materials` (flows/chain.py) reads the material master every 15 minutes and, besides staging, upserts
  each material into Zakya as an item, matched by its POS code (Zakya's SKU). A material that hasn't changed sends
  nothing (the snapshot is diffed), and an item that already holds the values is left alone (`zakya.items`).
- Prices: `sap-prices-to-zakya` reads SAP's retail price list and sets each item's rate in Zakya's *Retail* price
  list. A price needs its item in Zakya, so it runs once each `sap-materials` run has delivered to `zakya-items`
  (`fc.on_ready`), never before; a price change on its own is read on the next one, every 15 minutes at most.

A price is set, never deleted: a material SAP stops selling is ended in Zakya by hand (`zakya.prices` rejects a
delete, which the Quarantine page shows).
"""

from typing import Any

import feather_connect as fc
from feather_connect.connectors import sap_cpi, zakya  # pyright: ignore[reportAttributeAccessIssue]

UNIT = {"EA": "pcs"}  # SAP's unit of measure as Zakya names it


def to_zakya_item(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    """A material as a Zakya item: the POS code is its SKU, and SAP's list price its selling rate."""
    return {
        "sku": record["pos_code"],
        "name": record["description"],
        "ean": record["ean"],
        "unit": UNIT.get(str(record.get("uom")), str(record.get("uom") or "pcs")),
        "rate": float(record["price"]),
        "product_type": "goods",
    }


# The route `sap-materials` adds, to Zakya's items.
zakya_items_route = fc.to(zakya.Items("zakya"), name="zakya-items", map=to_zakya_item)


def to_zakya_price(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {"pricebook_id": record["pricebook_id"], "sku": record["pos_code"], "rate": float(record["price"])}


retail_prices = fc.Dataset(
    "retail_prices",
    source=sap_cpi.Source("sap_master", path="/http/prices", records_path="d.results"),
    key=["pricebook_id", "material"],
    strategy=fc.strategy.snapshot_diff(),
)
sap_prices_to_zakya = fc.Flow(
    "sap-prices-to-zakya",
    reads=retail_prices,
    deliver=[fc.to(zakya.Prices("zakya"), name="zakya-prices", map=to_zakya_price)],
    triggers=[fc.on_ready(fc.ready("materials", delivered_to="zakya-items"), each="run")],
)
