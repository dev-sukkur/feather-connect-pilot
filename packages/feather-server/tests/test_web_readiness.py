"""The Readiness page (M2 plan 1 task 8): periods of the flows that wait for data, Fire now and Close partition."""

import re
from datetime import timedelta

from fastapi import FastAPI

import feather_connect as fc
from feather_connect.connectors import memory
from feather_connect.project import prefixed
from feather_connect.runtime.catalogue import ActiveFlow
from feather_connect.storage.port import VersionDraft
from feather_server.app import AppState, create_app
from feather_server.testing import sign_in

BY_DAY = fc.partition.by_fields("store_id", fc.partition.by_day("date", tz="UTC"))
INVOICES = fc.Dataset(
    "invoices", source=memory.Source(), key="id", strategy=fc.strategy.replace(), partition=BY_DAY
)
INGEST = fc.Flow("ingest", reads=INVOICES, deliver=[fc.to(memory.Sink(connection="staging"))])
POSTING = fc.Flow(
    "posting",
    reads=fc.Dataset(
        "totals", source=memory.Source(connection="t"), key="id", strategy=fc.strategy.replace()
    ),
    deliver=[fc.to(memory.Sink(connection="erp"))],
    triggers=[fc.on_ready(fc.ready(INVOICES, by="store_id"), each="day", tz="UTC")],
)


class _Catalogue:
    def __init__(self, flows: list[ActiveFlow]) -> None:
        self._flows = flows

    def flows(self) -> list[ActiveFlow]:
        return self._flows


def _app(state: AppState) -> FastAPI:
    app = create_app(state=state, loops=lambda s: [])
    state.catalogue = _Catalogue(  # type: ignore[assignment]
        [
            ActiveFlow(f.name, "acme", "1.0.0", (), f.overlap, f.catch_up, True, frozenset(), f.spec_dict())
            for f in prefixed([INGEST, POSTING], "acme")
        ]
    )
    return app


def _csrf(page: str) -> str:
    found = re.search(r'<meta name="csrf-token" content="([^"]+)"', page)
    assert found
    return found.group(1)


def test_the_page_shows_what_a_store_day_waits_for_and_fires_it_by_hand(state: AppState):
    browser = sign_in(_app(state))
    run = state.store.runs.create_run("acme/ingest", "every", "t", {})
    yesterday = (state.clock.now() - timedelta(days=1)).date().isoformat()
    state.store.versions.stage(
        VersionDraft(
            "v1", "acme/invoices", f"store_id=1509/date={yesterday}", run, True, 1, 0, "h", None, None, "s"
        )
    )
    state.store.versions.seal(
        ["v1"], ["acme/staging"]
    )  # read by a run made after the day ended: ready, and the evaluator would fire it within seconds
    page = browser.get("/readiness")
    assert page.status_code == 200
    assert "acme/posting" in page.text and "store_id=1509" in page.text and "Ready" in page.text
    refused = browser.post(
        "/readiness/fire",
        data={
            "csrf_token": _csrf(page.text),
            "flow": "acme/posting",
            "period": yesterday,
            "scope": "store_id=1509",
        },
    )
    assert refused.status_code == 422 and "reason" in refused.text
    fired = browser.post(
        "/readiness/fire",
        data={
            "csrf_token": _csrf(page.text),
            "flow": "acme/posting",
            "period": yesterday,
            "scope": "store_id=1509",
            "reason": "the till closed late",
        },
    )
    assert fired.status_code == 303 and fired.headers["location"].startswith("/readiness?fired=")
    after = browser.get(fired.headers["location"])
    assert "is queued" in after.text and "Fired" in after.text


def test_close_partition_from_the_page(state: AppState):
    browser = sign_in(_app(state))
    page = browser.get("/readiness")
    done = browser.post(
        "/readiness/close",
        data={
            "csrf_token": _csrf(page.text),
            "dataset": "acme/invoices",
            "partition": "store_id=1509/date=2026-11-08",
            "reason": "POS closed the day",
        },
    )
    assert done.status_code == 303 and done.headers["location"] == "/readiness?done=closed"
    assert state.store.readiness.closes("acme/invoices", ["store_id=1509/date=2026-11-08"])
    assert "partition is closed" in browser.get("/readiness?done=closed").text
