"""The shop's orders, fanned out, each tagged with its channel: the `fan_out_demo` template with a map."""

from pathlib import Path
from typing import Any

import feather_connect as fc
from feather_connect.templates.generic import fan_out_demo  # pyright: ignore[reportMissingImports]


def web_order(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {**record, "channel": "web"}


orders = fan_out_demo(fixture=Path(__file__).resolve().parents[1] / "fixtures" / "orders.json", map=web_order)
