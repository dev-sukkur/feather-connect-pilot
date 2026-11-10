"""Old M1 names redirect to their package's names after the upgrade (flow packages §13)."""

from typing import Any

import pytest

from feather_server.renames import _Renames, target


class _Store:
    class packages:  # noqa: N801 - stands in for the store's port
        @staticmethod
        def renames() -> dict[tuple[str, str], str]:
            return {
                ("flow", "orders"): "acme/orders",
                ("dataset", "shop.items"): "acme/shop.items",
                ("route", "lake"): "acme/lake",
            }


class _State:
    store: Any = _Store()


@pytest.fixture
def renames() -> _Renames:
    return _Renames(_State())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("path", "method", "expected"),
    [
        ("/api/v1/flows/orders", "GET", ("/api/v1/flows/acme/orders", 308)),
        ("/api/v1/flows/orders:run", "POST", ("/api/v1/flows/acme/orders:run", 308)),
        ("/api/v1/datasets/shop.items/partitions", "GET", ("/api/v1/datasets/acme/shop.items/partitions", 308)),
        ("/api/v1/routes/lake/deliveries", "GET", ("/api/v1/routes/acme/lake/deliveries", 308)),
        ("/flows/orders", "GET", ("/packages/acme/flows/orders", 302)),
        ("/flows/orders/run", "POST", ("/flows/acme/orders/run", 307)),
        ("/routes/lake/deliveries", "GET", ("/routes/acme/lake/deliveries", 302)),
    ],
)
def test_an_old_name_redirects_to_its_new_one(
    renames: _Renames, path: str, method: str, expected: tuple[str, int]
) -> None:
    assert target(path, method, renames) == expected


@pytest.mark.parametrize("path", ["/api/v1/flows/other", "/flows/acme/orders", "/flows", "/runs/orders", "/flows/bulk"])
def test_anything_else_is_left_alone(renames: _Renames, path: str) -> None:
    assert target(path, "GET", renames) is None
