"""The webhook-to-api template: every environment loads, and in dev signed events reach the ERP's stand-in,
mapped, through the ingress's checks, the sealer and the runtime."""

import logging
from datetime import UTC, datetime, timedelta
from itertools import islice
from pathlib import Path

from feather_connect.clock import FrozenClock
from feather_connect.core.connector import event_id_of
from feather_connect.core.context import Context
from feather_connect.engine.local import LocalEngine
from feather_connect.project import load_project, validate_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.sealer import seal_once
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

ROOT = Path(__file__).resolve().parents[1]
SECRET = "dev-webhook-secret-0123"


def test_every_environment_loads_and_validates():
    for env in ("dev", "qa", "uat", "prod"):
        project = load_project(ROOT, env)
        assert [p for p in validate_project(project) if p.severity == "error"] == [], env


def test_signed_events_reach_the_erp_mapped():
    (flow,) = load_project(ROOT, "dev").flows
    source, sink = flow.datasets[0].source, flow.routes[0].sink
    assert getattr(source, "stands_in", False) and getattr(sink, "stands_in", False)
    clock = FrozenClock(datetime(2026, 11, 7, 9, tzinfo=UTC))
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)

    def make_context(connection: str) -> Context:
        return Context({"signature_secret": SECRET}, logging.getLogger("t"), clock.now(), "UTC")

    runtime = Runtime([flow], store, spool, engine, clock=clock, make_context=make_context)
    engine.register(runtime.tasks())
    for request in islice(source.events(1000, secret=SECRET, start=clock.now()), 3):
        source.verify(make_context("partner"), request)
        store.inbox.put("orders", request, event_id_of(source, request))
    clock.advance(timedelta(seconds=2))
    assert seal_once(runtime, now=clock.now())
    engine.run_until_idle()
    sent = sorted(sink.rows, key=lambda r: r["external_id"])
    assert [r["external_id"] for r in sent] == ["ORD-00001", "ORD-00002", "ORD-00003"]
    assert sent[0]["lines"] == [{"sku": "TEA-250", "quantity": 2, "price": "145.00"}]
