"""The connector registry: what connectors are installed, found by their entry points.

Each connector distribution registers its classes under the entry point group `feather_connect.connectors`, named
by `spec.name`. The registry is a catalogue for listing and describing connectors; flows build theirs in code, so it
loads nothing for a run.
"""

import importlib.metadata
from dataclasses import dataclass
from typing import Any, Literal

from feather_connect.core.connector import ConnectorSpec, Maturity

__all__ = ["GROUP", "ConnectorInfo", "RegistryError", "describe_connector", "list_connectors"]

GROUP = "feather_connect.connectors"
ConnectorKind = Literal["source", "push", "sink"]


class RegistryError(Exception):
    """An entry point does not name a connector the way the registry needs."""


@dataclass(frozen=True)
class ConnectorInfo:
    """One installed connector. `kind` is `sink`, `push` for a source that can also be pushed to, or `source`.
    `config_schema` is the JSON schema of its connection settings, empty when it has none. `package` is the
    distribution that registered it, when it came from one."""

    name: str
    kind: ConnectorKind
    spec: ConnectorSpec
    config_schema: dict[str, Any]
    maturity: Maturity
    package: str | None


def _info(entry: importlib.metadata.EntryPoint) -> ConnectorInfo:
    found: Any = entry.load()
    spec = getattr(found, "spec", None)
    if not isinstance(spec, ConnectorSpec):
        raise RegistryError(f"entry point {entry.name!r} ({entry.value}) has no ConnectorSpec as `spec`")
    if spec.name != entry.name:
        raise RegistryError(
            f"entry point {entry.name!r} ({entry.value}) names a connector whose spec says {spec.name!r}; "
            "the two must match"
        )
    if hasattr(found, "apply"):
        kind: ConnectorKind = "sink"
    elif "push" in spec.strategies:
        kind = "push"
    else:
        kind = "source"
    schema = spec.config.model_json_schema() if spec.config is not None else {}
    package = entry.dist.name if entry.dist is not None else None
    return ConnectorInfo(spec.name, kind, spec, schema, spec.maturity, package)


def list_connectors() -> list[ConnectorInfo]:
    """Every installed connector, by name."""
    found = [_info(entry) for entry in importlib.metadata.entry_points(group=GROUP)]
    return sorted(found, key=lambda info: info.name)


def describe_connector(name: str) -> ConnectorInfo:
    """The installed connector called `name`. Raises `LookupError` naming the ones there are."""
    known = list_connectors()
    for info in known:
        if info.name == name:
            return info
    raise LookupError(
        f"no connector named {name!r} is installed; there are {', '.join(i.name for i in known)}"
    )
