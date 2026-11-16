"""Prices to the warehouse, each with its price in paise: the `api_to_warehouse` template with a map."""

from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import feather_connect as fc
from feather_connect.templates.generic import api_to_warehouse  # pyright: ignore[reportMissingImports]


def with_paise(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    paise = (Decimal(str(record["price"])) * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP)
    return {**record, "price_paise": int(paise)}


prices = api_to_warehouse(
    source="pricing_api", destination="warehouse", path="/v1/prices", records_path="prices", map=with_paise
)
