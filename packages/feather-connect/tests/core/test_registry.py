"""The connector registry: what is installed, found by entry point, with each config's JSON schema."""

import importlib.metadata
import sys
from pathlib import Path

import pytest

from feather_connect.core import registry

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "thirdparty_connector"


def test_builtin_connectors_are_listed():
    found = {info.name: info for info in registry.list_connectors()}
    assert {"memory.source", "memory.sink", "simulation.source", "simulation.sink"} <= found.keys()
    assert found["memory.sink"].kind == "sink" and found["memory.source"].kind == "push"
    assert found["memory.sink"].package == "feather-connect"
    assert found["memory.sink"].maturity == found["memory.sink"].spec.maturity


def test_describe_returns_the_config_json_schema():
    info = registry.describe_connector("http.rest")
    assert info.kind == "source" and info.package == "feather-http"
    assert info.config_schema["properties"]["base_url"]["type"] == "string"
    assert registry.describe_connector("memory.sink").config_schema == {}
    with pytest.raises(LookupError, match="no connector named 'nope'.*memory.sink"):
        registry.describe_connector("nope")


def test_third_party_entry_point_is_found(monkeypatch):
    monkeypatch.syspath_prepend(str(FIXTURE))
    real = importlib.metadata.entry_points
    acme = importlib.metadata.EntryPoint("acme.source", "acme_connector:AcmeSource", registry.GROUP)

    def entry_points(**params):
        found = real(**params)
        return (
            importlib.metadata.EntryPoints([*found, acme]) if params.get("group") == registry.GROUP else found
        )

    monkeypatch.setattr(importlib.metadata, "entry_points", entry_points)
    info = registry.describe_connector("acme.source")
    assert info.kind == "source" and info.maturity == "experimental" and info.package is None
    assert info.config_schema["properties"]["api_key"]["format"] == "password"
    sys.modules.pop("acme_connector", None)


def test_an_entry_point_named_unlike_its_spec_is_refused(monkeypatch):
    monkeypatch.syspath_prepend(str(FIXTURE))
    wrong = importlib.metadata.EntryPoint("acme.other", "acme_connector:AcmeSource", registry.GROUP)
    monkeypatch.setattr(
        importlib.metadata, "entry_points", lambda **params: importlib.metadata.EntryPoints([wrong])
    )
    with pytest.raises(registry.RegistryError, match="acme.other.*acme.source"):
        registry.list_connectors()
    sys.modules.pop("acme_connector", None)
