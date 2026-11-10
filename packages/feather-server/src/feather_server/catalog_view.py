"""What the server knows of a packaged flow's datasets and routes, from the catalogue (flow packages §11, §13).

The server imports no package code, so its runtime holds none of an installed package's flows: what a page or an API
answer says of a dataset or a route comes from the active flow's catalogue spec (`Flow.spec_dict`). That spec names
things but holds no code, so a partition is shown by its key (no label) and a route's write cost is unknown.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from feather_server.app import AppState

__all__ = ["CatalogDataset", "CatalogRoute", "dataset_of", "route_of"]


@dataclass(frozen=True)
class CatalogDataset:
    name: str
    flow: str
    connection: str
    connector: str
    key: tuple[str, ...]
    strategy: str
    priority: int
    routes: tuple[str, ...]


@dataclass(frozen=True)
class CatalogRoute:
    name: str
    flow: str
    connection: str
    connector: str
    mode: str
    priority: int
    datasets: tuple[str, ...]


def _specs(state: "AppState") -> list[Mapping[str, Any]]:
    """The catalogue specs of the active packaged flows."""
    catalogue = state.catalogue
    if catalogue is None:
        return []
    found: list[Mapping[str, Any]] = []
    for flow in catalogue.flows():
        try:
            found.append(state.store.catalog.flow(flow.name).spec)
        except KeyError:
            continue
    return found


def dataset_of(state: "AppState", name: str) -> CatalogDataset:
    """The active packaged dataset of that name. Raises `KeyError` if there is none."""
    for spec in _specs(state):
        for d in spec.get("datasets") or []:
            if d.get("name") == name:
                return CatalogDataset(
                    name,
                    str(spec["name"]),
                    str(d.get("connection", "")),
                    str(d.get("connector", "")),
                    tuple(str(k) for k in d.get("key") or ()),
                    str(d.get("strategy", "")),
                    int(d.get("priority", 0)),
                    tuple(str(r["name"]) for r in spec.get("routes") or []),
                )
    raise KeyError(f"no dataset is named {name!r}")


def route_of(state: "AppState", name: str) -> CatalogRoute:
    """The active packaged route of that name. Raises `KeyError` if there is none."""
    for spec in _specs(state):
        for r in spec.get("routes") or []:
            if r.get("name") == name:
                return CatalogRoute(
                    name,
                    str(spec["name"]),
                    str(r.get("connection", "")),
                    str(r.get("connector", "")),
                    str(r.get("mode", "")),
                    int(r.get("priority", 0)),
                    tuple(str(d["name"]) for d in spec.get("datasets") or []),
                )
    raise KeyError(f"no route is named {name!r}")
