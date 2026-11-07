"""A connector from another distribution, found by its entry point alone (FR-J4)."""

from pydantic import BaseModel

from feather_connect.connectors import memory
from feather_connect.core.connector import ConnectorSpec, Secret


class AcmeConfig(BaseModel):
    host: str
    api_key: Secret


class AcmeSource(memory.Source):
    spec = ConnectorSpec(name="acme.source", config=AcmeConfig, strategies=frozenset({"replace"}))
