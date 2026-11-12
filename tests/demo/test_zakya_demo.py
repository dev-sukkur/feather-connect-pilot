"""The Zakya demo under examples/zakya-demo, end to end: both simulators on free ports in this process, the flow on the
local engine with the memory store, and the receiver's SQLite file read back."""

import importlib.util
import json
import shutil
import socket
import sqlite3
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

import uvicorn

from feather_connect.clock import FrozenClock
from feather_connect.engine.local import LocalEngine
from feather_connect.project import load_project, validate_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool
from feather_connect.templates.retail import zakya_invoices_to_erp  # pyright: ignore[reportMissingImports]

REPO = Path(__file__).resolve().parents[2]
DEMO = REPO / "examples" / "zakya-demo"


def _module(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"zakya_demo_{name}", DEMO / "sims" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _serve(app: Any) -> tuple[uvicorn.Server, threading.Thread, str]:
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert thread.is_alive() and time.monotonic() < deadline, "a simulator did not start"
        time.sleep(0.05)
    return server, thread, f"http://127.0.0.1:{port}"


@pytest.fixture
def sims(tmp_path: Path) -> Iterator[tuple[Any, Path, dict[str, str]]]:
    """The Zakya simulator (changing only when told), the receiver's SQLite file, and the URLs of both."""
    zakya_sim, receiver_sim = _module("zakya"), _module("receiver")
    zakya = zakya_sim.ZakyaSim(start=5)
    db = tmp_path / "receiver.sqlite"
    receiver = receiver_sim.Receiver(db, fault_rate=0)
    servers = [_serve(receiver_sim.create_app(receiver))]
    with zakya_sim.running(zakya) as zakya_url:
        yield zakya, db, {"zakya": zakya_url, "erp": servers[0][2]}
    for server, thread, _ in servers:
        server.should_exit = True
        thread.join(10)
    receiver.close()


def _received(db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "select body, idempotency_key, headers, erp_id, method, path from requests order by id"
        ).fetchall()
    return [
        {
            **json.loads(body),
            "_key": key,
            "_headers": json.loads(headers),
            "_erp_id": erp_id,
            "_call": f"{m} {p}",
        }
        for body, key, headers, erp_id, m, p in rows
    ]


def test_the_zakya_demo_project_is_sound():
    project = load_project(DEMO, "dev")
    assert [f.name for f in project.flows] == ["zakya-invoices", "invoices-to-sap"]
    assert [r.name for r in project.flows[0].routes] == ["erp", "staging"]
    assert [r.name for r in project.flows[1].routes] == ["sap"] and project.flows[1].routes[0].approval
    assert validate_project(project) == []
    (budget,) = (
        project.budgets.values()
    )  # M13: a dev day of the simulator takes far more than Zoho's 1400 calls
    assert [(w.units, w.seconds) for w in budget.windows] == [(90, 60), (20000, 86400)]


def test_the_sap_flow_starts_once_after_each_ingest_runs_invoices_are_in_staging():
    """The chain on the evaluator `feather dev`'s server runs (readiness design §11): `invoices-to-sap` waits for the
    ingest run's deliveries to staging, never for a run still delivering, and fires once for it."""
    from feather_connect.project import prefixed
    from feather_connect.runtime.catalogue import ActiveFlow
    from feather_connect.runtime.ready_eval import evaluate
    from feather_connect.storage.port import VersionDraft

    flows = [
        ActiveFlow(f.name, "zakya-demo", "1.0.0", (), f.overlap, f.catch_up, True, frozenset(), f.spec_dict())
        for f in prefixed(load_project(DEMO, "dev").flows, "zakya-demo")
    ]
    clock = FrozenClock(datetime(2026, 11, 12, 9, tzinfo=UTC))
    store = MemoryStore(clock)
    run = store.runs.create_run("zakya-demo/zakya-invoices", "every", "scheduler", {})
    store.versions.stage(
        VersionDraft("v1", "zakya-demo/invoices", "", run, True, 3, 0, "h", None, None, "sha")
    )
    store.versions.seal(["v1"], ["zakya-demo/erp", "zakya-demo/staging"])
    store.runs.set_run_status(run, "succeeded")
    assert evaluate(store, flows, clock.now()) == []  # its invoices are still going to staging
    store.ledger.transition("zakya-demo/staging", "v1", "running", actor="t")
    store.ledger.transition("zakya-demo/staging", "v1", "applied", actor="t")
    [check] = evaluate(store, flows, clock.now())
    assert store.runs.get_run(check).flow == "zakya-demo/invoices-to-sap"
    assert evaluate(store, flows, clock.now()) == []  # once


def test_a_budget_naming_another_organisation_is_a_problem(tmp_path: Path):  # Stage Z review 8
    copy = tmp_path / "demo"
    shutil.copytree(DEMO, copy, ignore=shutil.ignore_patterns(".data", "__pycache__"))
    for name in ("feather.yaml", "env/dev.yaml"):
        path = copy / name
        path.write_text(
            path.read_text(encoding="utf-8").replace("zakya:60001", "zakya:70002"), encoding="utf-8"
        )
    problems = validate_project(load_project(copy, "dev"))
    assert {p.code for p in problems} == {"budget_account_mismatch"}
    assert "'zakya:60001'" in problems[0].hint and "zakya:70002" in problems[0].message


def test_each_invoice_reaches_the_receiver_once_and_a_changed_one_again(sims, monkeypatch):
    zakya, db, urls = sims
    monkeypatch.chdir(DEMO.parent)  # the fake secret files are read against the project, wherever this runs
    project = load_project(DEMO, "dev")
    clock = FrozenClock(datetime(2026, 11, 7, 10, tzinfo=UTC))
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    for budget in project.budgets.values():
        store.budgets.define(budget)

    def config_of(connection: str) -> dict[str, Any]:
        config = project.connections[connection].resolve()
        base = urls[connection]
        return {**config, "base_url": base, **({"accounts_url": base} if "accounts_url" in config else {})}

    # The ERP leg alone: the template as the demo calls it, without the staging route the chain test covers.
    flow = zakya_invoices_to_erp(source="zakya", destination="erp", name="zakya-invoices", page_size=25)
    runtime = Runtime(
        [flow],
        store,
        spool,
        engine,
        code_sha="demo",
        clock=clock,
        budgets={c.name: c.budget for c in project.connections.values() if c.budget is not None},
        config_of=config_of,
    )
    engine.register(runtime.tasks())

    first = runtime.start_run("zakya-invoices")
    engine.run_until_idle()
    assert store.runs.get_run(first).status == "succeeded"
    received = _received(db)
    assert sorted(r["external_id"] for r in received) == sorted(zakya.invoices)  # each invoice once
    assert all(r["lines"] and r["status"] == "sent" for r in received)
    assert all(r["_call"] == f"PUT /invoices/{r['external_id']}" for r in received)  # an upsert by invoice id
    assert all("authorization" not in r["_headers"] for r in received)  # the receiver keeps no credentials
    (delivery,) = store.ledger.list_deliveries(route="erp")
    answers = store.responses.of_delivery("erp", delivery.version_id)
    assert len(answers) == 5 and all(
        a.status == 201 and str(a.fields["erp_id"]).startswith("ERP-") for a in answers
    )

    changed = zakya.change()
    (added,) = zakya.add(1)
    second = runtime.start_run("zakya-invoices")
    engine.run_until_idle()
    assert store.runs.get_run(second).status == "succeeded"
    received = _received(db)
    assert len(received) == 7
    again = [r for r in received if r["external_id"] == changed]
    assert [r["status"] for r in again] == ["sent", "paid"] and again[0]["_key"] != again[1]["_key"]
    assert (
        again[0]["_erp_id"] == again[1]["_erp_id"]
    )  # the paid invoice updates the ERP's record, not a new one
    assert len({r["_erp_id"] for r in received}) == 6
    assert [r["external_id"] for r in received].count(added) == 1

    third = runtime.start_run("zakya-invoices")  # nothing new: nothing is sent
    engine.run_until_idle()
    assert store.runs.get_run(third).status == "succeeded"
    assert len(_received(db)) == 7


def test_the_zakya_simulator_keeps_its_invoices_across_a_restart(tmp_path):
    zakya_sim = _module("zakya")
    state = tmp_path / "zakya.json"
    first = zakya_sim.ZakyaSim(start=3, state=state)
    first.change()
    again = zakya_sim.ZakyaSim(start=3, state=state)
    assert again.invoices == first.invoices
    assert again.add() == ["4600000000004"]


@pytest.mark.postgres
def test_the_chain_stages_each_invoice_then_posts_it_to_sap_once_held_over_the_limit(sims, monkeypatch):
    """The M2 reference chain: Zakya → Postgres staging → SAP through CPI. An invoice over 500,000 holds the SAP
    delivery until it is approved; once applied, a compensation reverses each posting."""
    from real_stack import scratch_database, server_url

    from feather_connect.connectors.sap_cpi.testing import CpiSim, serve  # pyright: ignore[reportMissingImports]
    from feather_connect.runtime.http import forget_tokens

    zakya, _db, urls = sims
    monkeypatch.chdir(DEMO.parent)
    forget_tokens()
    cpi = CpiSim(key="invoice_id", csrf=True)
    port = _free_port()
    cpi_server = serve(cpi, port)
    big = next(iter(zakya.invoices))
    zakya.invoices[big]["total"] = 750_000.0
    with scratch_database(server_url()) as staging_url:
        monkeypatch.setenv("FEATHER_DATABASE_URL", staging_url)
        import psycopg

        with psycopg.connect(staging_url, autocommit=True) as conn:
            conn.execute(_module("staging").DDL)
        project = load_project(DEMO, "dev")
        clock = FrozenClock(datetime(2026, 11, 12, 10, tzinfo=UTC))
        store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)

        def config_of(connection: str) -> dict[str, Any]:
            config = project.connections[connection].resolve()
            if connection == "sap":
                base = f"http://127.0.0.1:{port}"
                return {**config, "base_url": base, "token_url": f"{base}/oauth/token"}
            if connection in ("staging", "staging_read"):
                return config
            base = urls[connection]
            return {
                **config,
                "base_url": base,
                **({"accounts_url": base} if "accounts_url" in config else {}),
            }

        runtime = Runtime(
            project.flows, store, spool, engine, code_sha="demo", clock=clock, config_of=config_of
        )
        engine.register(runtime.tasks())
        try:
            ingest = runtime.start_run("zakya-invoices")
            engine.run_until_idle()
            assert store.runs.get_run(ingest).status == "succeeded"
            with psycopg.connect(staging_url) as conn:
                staged = conn.execute("select invoice_id from zakya_demo.invoices").fetchall()
            assert sorted(r[0] for r in staged) == sorted(zakya.invoices)

            clock.advance(timedelta(minutes=5))  # on_ready's at_most_every
            runtime.start_run("invoices-to-sap")
            engine.run_until_idle()
            print(
                "DEBUG",
                [(v.version_id, v) for v in store.versions.list_versions("staged-invoices")]
                if hasattr(store.versions, "list_versions")
                else dir(store.versions),
            )
            (delivery,) = store.ledger.list_deliveries(route="sap")
            assert delivery.status == "held" and cpi.posts == 0  # nothing of it reaches SAP before approval
            runtime.approve("sap", delivery.version_id, actor="ann", reason="checked with finance")
            engine.run_until_idle()
            assert store.ledger.get("sap", delivery.version_id).status == "applied"
            assert sorted(cpi.documents["invoices"]) == sorted(zakya.invoices) and cpi.posts == 5
            answers = store.responses.of_delivery("sap", delivery.version_id)
            assert len(answers) == 5 and all(str(a.fields["sap_document"]).startswith("9") for a in answers)

            clock.advance(timedelta(minutes=5))
            again = runtime.start_run("invoices-to-sap")  # nothing changed in staging: nothing posted
            engine.run_until_idle()
            assert store.runs.get_run(again).status == "succeeded" and cpi.posts == 5

            runtime.compensate("sap", delivery.version_id, actor="ann", reason="posted to the wrong company")
            engine.run_until_idle()
            assert store.ledger.get("sap", delivery.version_id).status == "compensated"
            assert (
                sorted(cpi.reversed["invoices"]) == sorted(zakya.invoices) and not cpi.documents["invoices"]
            )
        finally:
            cpi_server.shutdown()
            forget_tokens()
