"""The shop's orders, fanned out every 5 minutes: the `fan_out_demo` template, renamed."""

from pathlib import Path

import feather_connect as fc
from feather_connect.templates.generic import fan_out_demo  # pyright: ignore[reportMissingImports]

orders = fan_out_demo(
    fixture=Path(__file__).resolve().parents[1] / "fixtures" / "orders.json",
    schedule=fc.every("5m"),
    name="shop-orders",
)
