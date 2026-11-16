"""What the server knows of a packaged flow's datasets and routes, from the catalogue (flow packages §11, §13).

The server imports no package code, so its runtime holds none of an installed package's flows: what a page or an API
answer says of a dataset or a route comes from the active flow's catalogue spec (`Flow.spec_dict`). That spec names
things but holds no code. Its partition scheme is there as data (`scheme_spec`), from which the server labels and
checks partition keys, unless the package wrote its own scheme; and its read and write costs per record, if the
package was checked by a Feather that recorded them (16 Nov 2026), from which previews estimate budget units.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from feather_connect.core.partitions import PartitionScheme, scheme_of_spec

if TYPE_CHECKING:
    from feather_server.app import AppState

__all__ = ["CatalogDataset", "CatalogRoute", "dataset_of", "partition_label", "route_of", "scheme_of"]


@dataclass(frozen=True)
class CatalogDataset:
    name: str
    flow: str
    connection: str
    connector: str
    key: tuple[str, ...]
    strategy: str
    priority: str
    routes: tuple[str, ...]
    pii: tuple[str, ...] = ()
    partition: PartitionScheme | None = None  # None: the package's own scheme, which only its code knows
    read_cost: int | None = None  # budget units per record read; None: not recorded


@dataclass(frozen=True)
class CatalogRoute:
    name: str
    flow: str
    connection: str
    connector: str
    mode: str
    priority: str
    datasets: tuple[str, ...]
    write_cost: int | None = None  # budget units per record written; None: not recorded


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
                    str(d.get("priority", "normal")),
                    tuple(str(r["name"]) for r in spec.get("routes") or []),
                    tuple(str(f) for f in d.get("pii") or ()),
                    scheme_of_spec(d.get("partition")),
                    _cost(d.get("read_cost")),
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
                    str(r.get("priority", "normal")),
                    tuple(str(d["name"]) for d in spec.get("datasets") or []),
                    _cost(r.get("write_cost")),
                )
    raise KeyError(f"no route is named {name!r}")


def _cost(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def scheme_of(state: "AppState", dataset: str) -> PartitionScheme | None:
    """The partition scheme of a dataset: the runtime's own, else the active package's from its catalogue spec. None
    when neither knows the dataset or its package wrote its own scheme."""
    try:
        return state.runtime.dataset(dataset).partition
    except KeyError:
        pass
    try:
        return dataset_of(state, dataset).partition
    except KeyError:
        return None


def partition_label(scheme: PartitionScheme | None, key: str) -> str:
    """`key` in business words by `scheme` (`Store 1509 · 18 Oct 2026`), or the key itself when there's no scheme or
    the key isn't one of its."""
    if scheme is None:
        return key
    try:
        return scheme.label_of(key)
    except (ValueError, KeyError):
        return key
