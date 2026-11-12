"""The second link of the chain (readiness design §11): after each ingest run's invoices are in the ERP, a check of
what the ERP took starts, at most every 5 minutes. It never starts on a run whose deliveries are still going, or one
that failed: the Readiness page says what it waits for.

The check reads the ERP's receipts, which the demo's receiver keeps; here it is a small simulated stand-in, so that
the chain runs on `feather dev` with nothing more to set up.
"""

import feather_connect as fc
from feather_connect.connectors import simulation


receipts = fc.Dataset(
    "erp-receipts",
    source=simulation.Source("receipts", generator={"id": simulation.seq()}, rows_per_partition=3),
    key="id",
    strategy=fc.strategy.replace(),
)
erp_check = fc.Flow(
    "erp-check",
    reads=receipts,
    deliver=[fc.to(simulation.Sink("checked"), name="checked")],
    triggers=[
        fc.on_ready(fc.ready("invoices", delivered_to="erp"), each="run", at_most_every="5m")
    ],
)
