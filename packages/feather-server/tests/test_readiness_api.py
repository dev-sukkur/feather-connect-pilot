"""Readiness in the API (M2 plan 1, tasks 7 and 9): the status of each set, *Fire now* and *Close partition*, each
audited and with a reason."""

from datetime import timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import feather_connect as fc
from feather_connect.connectors import memory
from feather_connect.project import prefixed
from feather_connect.runtime.catalogue import ActiveFlow
from feather_server.app import AppState, create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.actions import Actions

ANN = Actor("human", "ann", "operator", "ann")
STORE_DAY = fc.partition.by_fields("store_id", fc.partition.by_day("date", tz="UTC"))


def _dataset(name: str, partition: Any = STORE_DAY) -> fc.Dataset:
    return fc.Dataset(
        name,
        source=memory.Source(connection=f"src_{name}"),
        key="id",
        strategy=fc.strategy.replace(),
        partition=partition,
    )


INVOICES = _dataset("invoices")
INGEST = fc.Flow("ingest", reads=INVOICES, deliver=[fc.to(memory.Sink(connection="staging"))])
POSTING = fc.Flow(
    "posting",
    reads=_dataset("totals", fc.partition.none()),
    deliver=[fc.to(memory.Sink(connection="erp"))],
    triggers=[fc.on_ready(fc.ready(INVOICES, by="store_id"), each="day", tz="UTC")],
)


class _Catalogue:
    def __init__(self, flows: list[ActiveFlow]) -> None:
        self._flows = flows

    def flows(self) -> list[ActiveFlow]:
        return self._flows


@pytest.fixture
def app(state: AppState) -> FastAPI:
    app = create_app(state=state, loops=lambda s: [])
    state.catalogue = _Catalogue(  # type: ignore[assignment]
        [
            ActiveFlow(f.name, "acme", "1.0.0", (), f.overlap, f.catch_up, True, frozenset(), f.spec_dict())
            for f in prefixed([INGEST, POSTING], "acme")
        ]
    )
    return app


def test_fire_now_queues_a_manual_firing_for_the_flows_host_and_is_audited(app: FastAPI, state: AppState):
    do = Actions(state)
    with pytest.raises(ValueError, match="reason"):
        do.fire_ready(ANN, "acme/posting", period="2026-11-08", scope="store_id=1509", reason=None)
    run_id = do.fire_ready(
        ANN, "acme/posting", period="2026-11-08", scope="store_id=1509", reason="till closed late"
    )
    run = state.store.runs.get_run(run_id)
    assert (run.trigger, run.status, run.params["version"]) == ("ready", "queued", "1.0.0")
    assert run.params["ready"]["scope"] == {"store_id": "1509"} and run.params["ready"]["reason"] == "manual"
    again = do.fire_ready(ANN, "acme/posting", period="2026-11-08", scope="store_id=1509", reason="once more")
    assert [f.seq for f in state.store.readiness.firings("acme/posting")] == [1, 0] and again != run_id
    with pytest.raises(ValueError, match="by field"):
        do.fire_ready(ANN, "acme/posting", period="2026-11-08", scope="region=south", reason="x")
    with pytest.raises(KeyError):
        do.fire_ready(ANN, "acme/nothing", period="2026-11-08", reason="x")
    results = [a.result for a in state.store.audit.list_audit(20) if a.action == "fire_ready"]
    assert results[0].startswith("failed") and any(r.startswith("ok: run") for r in results)


def test_close_status_and_fire_through_the_api(app: FastAPI, state: AppState):
    token = issue_token(state.store, "ci", timedelta(days=1), role="operator", clock=state.clock)
    client = TestClient(app, base_url="http://localhost:8000", headers={"Authorization": f"Bearer {token}"})
    closed = client.post(
        "/api/v1/datasets/acme/invoices/partitions/store_id=1509/date=2026-11-08:close",
        json={"reason": "POS closed the day"},
    )
    assert closed.status_code == 200, closed.text
    assert closed.json()["partition"] == "store_id=1509/date=2026-11-08"
    assert state.store.readiness.closes("acme/invoices", ["store_id=1509/date=2026-11-08"])
    refused = client.post("/api/v1/datasets/acme/invoices/partitions/p:close", json={})
    assert refused.status_code in (400, 422)
    fired = client.post(
        "/api/v1/flows/acme/posting/readiness:fire",
        json={"period": "2026-11-08", "scope": "store_id=1509", "reason": "by hand"},
        headers={"Idempotency-Key": "k1"},
    )
    assert fired.status_code == 202, fired.text
    status = client.get("/api/v1/flows/acme/posting/readiness")
    assert status.status_code == 200, status.text
    [found] = status.json()["sets"]
    assert found["flow"] == "acme/posting" and found["by"] == "store_id" and found["each"] == "day"
    assert client.get("/api/v1/readiness").json()["sets"][0]["set"] == found["set"]
    assert client.get("/api/v1/flows/acme/nothing/readiness").status_code == 404


def test_the_status_has_no_periods_by_store_until_a_store_has_data(app: FastAPI, state: AppState):
    [found] = Actions(state).readiness("acme/posting")
    assert found["periods"] == [] and found["datasets"] == ["acme/invoices"]
