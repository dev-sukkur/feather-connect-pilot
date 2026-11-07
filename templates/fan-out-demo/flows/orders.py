"""The shop's orders, fanned out: the `fan_out_demo` template over this project's fixture."""

from pathlib import Path

from feather_connect.templates.generic import fan_out_demo  # pyright: ignore[reportMissingImports]

orders = fan_out_demo(fixture=Path(__file__).resolve().parents[1] / "fixtures" / "orders.json")
