"""Activation refuses a flow whose `on_ready` set waits for another package's dataset that no active flow reads
(M2 plan 1 §1.2)."""

from dataclasses import dataclass, field
from typing import Any

from feather_server.services.packages import UpstreamInactive, inactive_upstreams


@dataclass
class _Activation:
    flow: str
    package: str
    version: str = "1.0.0"


@dataclass
class _Version:
    load_check: dict[str, Any]


@dataclass
class _Packages:
    specs: dict[str, dict[str, Any]] = field(default_factory=dict)

    def active(self) -> list[_Activation]:
        return [_Activation(f, f.split("/")[0]) for f in self.specs]

    def version(self, package: str, version: str) -> _Version:
        return _Version({"specs": {f: s for f, s in self.specs.items() if f.startswith(package + "/")}})


@dataclass
class _Store:
    packages: _Packages


def _waiting(*datasets: str) -> dict[str, Any]:
    return {"triggers": [{"kind": "on_ready", "ready": {"datasets": list(datasets)}}]}


def test_an_upstream_of_another_package_must_be_read_by_an_active_flow():
    store = _Store(_Packages())
    spec = _waiting("sap/postings", "ingest/lines")
    assert inactive_upstreams(store, "sap/post", spec) == ["ingest/lines"]  # type: ignore[arg-type]
    store.packages.specs["ingest/load"] = {"datasets": [{"name": "ingest/lines"}]}
    assert inactive_upstreams(store, "sap/post", spec) == []  # type: ignore[arg-type]


def test_own_package_datasets_and_other_triggers_need_nothing():
    store = _Store(_Packages())
    assert inactive_upstreams(store, "sap/post", _waiting("sap/postings")) == []  # type: ignore[arg-type]
    assert inactive_upstreams(store, "sap/post", {"triggers": [{"kind": "cron"}]}) == []  # type: ignore[arg-type]


def test_the_refusal_names_what_to_activate_first():
    error = UpstreamInactive("sap/post", ["ingest/lines"])
    assert error.state == "upstream_inactive" and "activate the flow that writes it first" in str(error)
