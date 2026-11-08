"""Acme's invoices: read a day at a time from the billing system and delivered to the ERP."""

from pathlib import Path

import feather_connect as fc
from feather_connect.connectors import simulation

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "invoices.json"

invoices = fc.Dataset(
    "invoices",
    source=simulation.Source("billing", fixture=FIXTURE),
    key="invoice_id",
    strategy=fc.strategy.replace(),
)
flow = fc.Flow(
    "invoices",
    reads=invoices,
    deliver=[fc.to(simulation.Sink("erp"), name="erp")],
    triggers=[fc.every("1h")],
    owner="acme-finance",
)
