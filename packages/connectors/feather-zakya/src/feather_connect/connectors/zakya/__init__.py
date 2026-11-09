"""The Zoho Zakya connector: a source of an organisation's invoices, credit notes, payments, items and price lists,
and sinks for its items, prices and stock (design §8.3–§8.4).

    connections:
      zakya:
        connector: zakya          # serves zakya.source, zakya.items, zakya.prices and zakya.stock
        budget: zoho-api
        domain: in                # com, in or eu: the organisation's region
        organization_id: "60001"
        auth:
          client_id: ${secret:env:ZAKYA_CLIENT_ID}
          client_secret: ${secret:env:ZAKYA_CLIENT_SECRET}
          refresh_token: ${secret:env:ZAKYA_REFRESH_TOKEN}
    budgets:
      zoho-api: {limits: ["90/minute", "1400/day"], account: "zakya:60001"}

    from feather_connect.connectors import zakya

    invoices = zakya.dataset("invoices")                  # by last_modified_time, two minutes of overlap
    prices = zakya.dataset("price_lists")                 # in full: price lists carry no stamp
    deliver = fc.to(zakya.Stock("zakya"), mode="append")  # a stock adjustment per record, posted once
"""

from .config import API_PREFIX, DOMAINS, STAMP_FORMAT, ZakyaConfig, ZakyaError
from .sinks import Items, Prices, Stock
from .source import DATASETS, DEFAULT_LOOKBACK, Source, ZakyaDataset, dataset

__all__ = [
    "API_PREFIX",
    "DATASETS",
    "DEFAULT_LOOKBACK",
    "DOMAINS",
    "STAMP_FORMAT",
    "Items",
    "Prices",
    "Source",
    "Stock",
    "ZakyaConfig",
    "ZakyaDataset",
    "ZakyaError",
    "dataset",
]
