"""The package installs the subpackage `zakya` into the namespace `feather_connect.connectors`, and registers its four
connectors."""

from pathlib import Path

from feather_connect.core.registry import list_connectors


def test_the_connectors_are_registered_from_this_package():
    from feather_connect.connectors import zakya  # pyright: ignore[reportAttributeAccessIssue]

    package_src = Path(__file__).resolve().parents[1] / "src"
    assert Path(zakya.__file__).resolve().is_relative_to(package_src)
    found = {c.name: c for c in list_connectors() if c.name.startswith("zakya.")}
    assert {n: (c.kind, c.package, c.maturity) for n, c in found.items()} == {
        "zakya.items": ("sink", "feather-zakya", "beta"),
        "zakya.prices": ("sink", "feather-zakya", "beta"),
        "zakya.source": ("source", "feather-zakya", "beta"),
        "zakya.stock": ("sink", "feather-zakya", "beta"),
    }
    assert "organization_id" in found["zakya.source"].config_schema["required"]
