"""The Zakya demo under examples/zakya-demo, end to end: the simulators on free ports in this process, the flows on
the local engine with the memory store, and what the simulators kept read back.

The reference chain covers B2B and B2C sales and a return of each: B2B invoices and credit notes reach SAP one
document each, B2C sales and returns summed per day and material."""

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

import httpx
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
FLOWS = ["zakya-invoices", "zakya-credit-notes", "sap-materials", "invoices-to-sap", "b2b-returns-to-sap", "b2c-to-sap"]
TEA, RICE, SUGAR = "8901234500101", "8901234500200", "8901234500507"  # EANs of master.json


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
    """The Zakya simulator in manual mode (changing only when told), the receiver's SQLite file, and both URLs."""
    zakya_sim, receiver_sim = _module("zakya"), _module("receiver")
    zakya = zakya_sim.ZakyaSim()
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
    assert [f.name for f in project.flows] == FLOWS
    routes = {f.name: [r.name for r in f.routes] for f in project.flows}
    assert routes["zakya-invoices"] == ["erp", "staging"]
    assert routes["zakya-credit-notes"] == ["staging-returns"]
    held = {f.name: bool(f.routes[0].approval) for f in project.flows[3:]}
    assert held == {"invoices-to-sap": True, "b2b-returns-to-sap": True, "b2c-to-sap": False}
    assert validate_project(project) == []
    (budget,) = (
        project.budgets.values()
    )  # M13: a dev day of the simulator takes far more than Zoho's 1400 calls
    assert [(w.units, w.seconds) for w in budget.windows] == [(90, 60), (20000, 86400)]


def test_the_sap_flows_start_once_after_each_ingest_runs_invoices_are_in_staging():
    """The chain on the evaluator `feather dev`'s server runs (readiness design §11): B2B `invoices-to-sap` and
    `b2c-to-sap` wait for the ingest run's deliveries to staging, never for a run still delivering, and fire once."""
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
    started = evaluate(store, flows, clock.now())
    assert sorted(store.runs.get_run(r).flow for r in started) == [
        "zakya-demo/b2c-to-sap",
        "zakya-demo/invoices-to-sap",
    ]
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
    zakya.add(3, channel="B2B")
    zakya.add(2, channel="B2C")
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
    assert sorted(r["status"] for r in received) == ["paid", "paid", "sent", "sent", "sent"]  # B2C is paid at once
    assert all(r["lines"] and all(line["sku"] for line in r["lines"]) for r in received)
    assert all(r["_call"] == f"PUT /invoices/{r['external_id']}" for r in received)  # an upsert by invoice id
    assert all("authorization" not in r["_headers"] for r in received)  # the receiver keeps no credentials
    (delivery,) = store.ledger.list_deliveries(route="erp")
    answers = store.responses.of_delivery("erp", delivery.version_id)
    assert len(answers) == 5 and all(
        a.status == 201 and str(a.fields["erp_id"]).startswith("ERP-") for a in answers
    )

    changed = zakya.change()  # only a B2B invoice is open to be paid
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


def test_the_zakya_simulator_keeps_its_sales_and_returns_across_a_restart(tmp_path):
    zakya_sim = _module("zakya")
    state = tmp_path / "zakya.json"
    first = zakya_sim.ZakyaSim(start=3, state=state)
    first.change()
    note = first.take_back_some()
    again = zakya_sim.ZakyaSim(start=3, state=state)
    assert again.invoices == first.invoices and again.credit_notes == first.credit_notes and note in again.credit_notes
    assert again.add() == ["4600000000004"]
    assert again.take_back_some() == "4700000000002"


def test_an_older_state_file_is_set_aside(tmp_path):
    state = tmp_path / "zakya.json"
    state.write_text(json.dumps({"next": 9, "invoices": {"4600000000001": {"invoice_id": "4600000000001"}}}))
    sim = _module("zakya").ZakyaSim(state=state)
    assert sim.invoices == {} and (tmp_path / "zakya.old.json").is_file()


def test_the_zakya_counter_raises_invoices_and_takes_goods_back_by_either_barcode():
    zakya_sim = _module("zakya")
    sim = zakya_sim.ZakyaSim()
    assert sim.mode == "manual"

    def call(method: str, path: str, body: Any = None) -> httpx.Response:
        return sim.handle(httpx.Request(method, f"http://sim{path}", json=body))

    assert "text/html" in call("GET", "/").headers["content-type"]
    made = call("POST", "/sim/invoices", {"customer_id": "c-900", "lines": [{"code": "TEA-250", "quantity": 2}]})
    assert made.status_code == 201
    invoice = sim.invoices[made.json()["invoice_id"]]
    assert invoice["status"] == "paid" and zakya_sim.channel_of(invoice) == "B2C"  # a counter sale
    (line,) = invoice["line_items"]
    assert (line["ean"], line["sku"], line["rate"], line["item_total"]) == (TEA, "TEA-250", 180.0, 360.0)

    b2b = call("POST", "/sim/invoices", {"customer_id": "c-101", "lines": [{"code": RICE, "quantity": 5, "rate": 600}]})
    assert sim.invoices[b2b.json()["invoice_id"]]["status"] == "sent"
    assert sim.invoices[b2b.json()["invoice_id"]]["gst_no"]  # a B2B customer has a GSTIN

    too_many = call("POST", "/sim/creditnotes", {"invoice_id": invoice["invoice_id"], "lines": [{"code": TEA, "quantity": 3}]})
    assert too_many.status_code == 400 and "only 2" in too_many.json()["error"]
    back = call("POST", "/sim/creditnotes", {"invoice_id": invoice["invoice_id"], "lines": [{"code": TEA, "quantity": 2}]})
    note = sim.credit_notes[back.json()["creditnote_id"]]
    assert note["invoice_id"] == invoice["invoice_id"] and note["total"] == invoice["total"]
    assert call("POST", "/sim/creditnotes", {"invoice_id": invoice["invoice_id"]}).status_code == 400  # all back
    assert call("POST", "/sim/invoices", {"customer_id": "c-900", "lines": [{"code": "NOPE", "quantity": 1}]}).status_code == 400

    assert call("POST", "/sim/mode", {"mode": "auto", "every": 5}).json() == {"mode": "auto", "every": 5.0}
    state = call("GET", "/sim/state").json()
    assert state["mode"] == "auto" and {c["channel"] for c in state["customers"]} == {"B2B", "B2C"}
    assert [v["invoice_number"] for v in state["invoices"]] == ["INV-00002", "INV-00001"]


class _Erp:
    """The ERP simulator, called as the sap_cpi connector calls it: a token, a CSRF token, then posts."""

    def __init__(self, sim: Any) -> None:
        self.sim = sim
        _, _, token = sim.answer("POST", "/oauth/token", {}, b"")
        self.auth = {"Authorization": f"Bearer {token['access_token']}"}
        _, headers, _ = sim.answer("GET", "/http/invoices", {**self.auth, "X-CSRF-Token": "Fetch"}, b"")
        self.csrf = headers["X-CSRF-Token"]
        self.sent = 0

    def post(self, iflow: str, body: dict[str, Any], key: str | None = None) -> tuple[int, Any]:
        self.sent += 1
        headers = {**self.auth, "X-CSRF-Token": self.csrf, "Idempotency-Key": key or f"k{self.sent}"}
        status, _, answer = self.sim.answer("POST", f"/http/{iflow}", headers, json.dumps(body).encode())
        return status, answer


def _invoice(invoice_id: str, *items: tuple[str, float, float], date: str = "2026-11-13") -> dict[str, Any]:
    return {
        "invoice_id": invoice_id,
        "BillingDocumentDate": date,
        "CustomerName": "Asha Traders",
        "Items": [{"Material": m, "Quantity": q, "NetAmount": a} for m, q, a in items],
    }


def test_the_erp_posts_once_checks_returns_against_their_invoice_and_reverses(tmp_path):
    erp = _Erp(_module("sap").ErpSim(tmp_path / "sap.sqlite"))
    status, _, answer = erp.sim.answer("GET", "/http/materials", erp.auth, b"")
    assert status == 200 and {m["ean"] for m in answer["d"]["results"]} >= {TEA, RICE, SUGAR}

    memo = {"creditnote_id": "47-1", "ReferenceInvoice": "46-1", "BillingDocumentDate": "2026-11-13",
            "Items": [{"Material": "MAT-1001", "Quantity": 4, "NetAmount": 720}]}
    status, answer = erp.post("creditmemos", memo)
    assert status == 422 and "not in the ERP" in answer["error"]  # its invoice isn't posted yet

    status, answer = erp.post("invoices", _invoice("46-1", ("MAT-1001", 10, 1800), (TEA, 2, 360)), key="a")
    assert status == 201 and answer["d"]["Status"] == "posted"
    assert erp.post("invoices", _invoice("46-1", ("MAT-1001", 10, 1800)), key="a") == (status, answer)  # a resend
    duplicate = erp.post("invoices", _invoice("46-1", ("MAT-1001", 10, 1800)), key="b")
    assert duplicate[0] == 409 and duplicate[1]["d"]["BillingDocument"] == answer["d"]["BillingDocument"]
    assert erp.post("invoices", _invoice("46-2", ("MAT-9999", 1, 1)))[0] == 422  # a material SAP doesn't know

    assert erp.post("creditmemos", memo)[0] == 201
    status, answer = erp.post("creditmemos", {**memo, "creditnote_id": "47-2", "Items": [{"Material": TEA, "Quantity": 9, "NetAmount": 1}]})
    assert status == 422 and "only 8 left" in answer["error"]  # 12 sold, 4 back already

    status, _, answer = erp.sim.answer(
        "POST", "/http/creditmemos/reverse", {**erp.auth, "X-CSRF-Token": erp.csrf}, json.dumps(memo).encode()
    )
    assert status == 200 and [d["status"] for d in erp.sim.documents(iflow="creditmemos")] == ["reversed"]
    assert erp.post("creditmemos", {**memo, "creditnote_id": "47-3", "Items": [{"Material": TEA, "Quantity": 12, "NetAmount": 1}]})[0] == 201


def test_the_erp_adds_up_a_day_by_material_and_parks_postings_in_manual_mode(tmp_path):
    erp = _Erp(_module("sap").ErpSim(tmp_path / "sap.sqlite"))
    erp.post("invoices", _invoice("46-1", (TEA, 10, 1800), (RICE, 5, 3200)))
    summary = {"PostingDate": "2026-11-13", "Material": "MAT-1001", "EAN": TEA}
    erp.post("salessummary", {**summary, "document_id": "B2C-1", "Quantity": 3, "NetAmount": 540})
    erp.post("salessummary", {**summary, "document_id": "B2C-2", "Quantity": 1, "NetAmount": 180})  # later that day
    erp.post("salessummary", {**summary, "document_id": "B2C-3", "Quantity": -1, "NetAmount": -180})  # a return
    erp.post("salessummary", {**summary, "document_id": "B2C-4", "PostingDate": "2026-11-14", "Quantity": 2, "NetAmount": 360})
    tea = {(r["posting_date"], r["material"]): r for r in erp.sim.by_material()}
    first = tea[("2026-11-13", "MAT-1001")]
    assert (first["sold_qty"], first["returned_qty"], first["net_qty"], first["net_amount"]) == (14, 1, 13, 2340)
    assert (first["b2b_qty"], first["b2c_qty"], first["documents"]) == (10, 3, 4)
    second = tea[("2026-11-14", "MAT-1001")]
    assert (second["net_qty"], second["qty_to_date"], second["amount_to_date"]) == (2, 15, 2700)
    kinds = {d["business_key"]: d["kind"] for d in erp.sim.documents(iflow="salessummary")}
    assert kinds["B2C-3"] == "B2C returns summary" and kinds["B2C-1"] == "B2C summary"

    erp.sim.set_mode("manual")
    status, answer = erp.post("salessummary", {**summary, "document_id": "B2C-5", "Quantity": 5, "NetAmount": 900})
    assert status == 201 and answer["d"]["Status"] == "parked"
    assert tea == {(r["posting_date"], r["material"]): r for r in erp.sim.by_material()}  # a parked one doesn't count
    erp.sim.decide(answer["d"]["BillingDocument"], "post")
    first = {(r["posting_date"], r["material"]): r for r in erp.sim.by_material()}[("2026-11-13", "MAT-1001")]
    assert first["net_qty"] == 18

    by_hand = erp.sim.enter({"kind": "return", "channel": "B2C", "posting_date": "2026-11-13", "lines": [{"code": "TEA-250", "quantity": 2}]})
    (entered,) = [d for d in erp.sim.documents() if d["doc_no"] == by_hand]
    assert entered["origin"] == "manual" and entered["total"] == -360.0


@pytest.mark.postgres
def test_the_chain_posts_b2b_per_invoice_and_b2c_per_material_with_a_return_of_each(sims, monkeypatch, tmp_path):
    """The reference chain, Zakya → Postgres staging → SAP through CPI. A B2B invoice is one billing document, held
    for approval over 500,000, and its credit note one credit memo against it. B2C sales and returns are summed per day
    and material, each run's new lines one summary each, and the ERP adds a day's summaries up."""
    from real_stack import scratch_database, server_url

    from feather_connect.runtime.http import forget_tokens

    zakya, _db, urls = sims
    small = zakya.raise_invoice("c-101", [{"code": TEA, "quantity": 10}, {"code": "RICE-5", "quantity": 5}])
    big = zakya.raise_invoice("c-102", [{"code": RICE, "quantity": 800}])  # 537,600 with tax: held
    counter = zakya.raise_invoice("c-900", [{"code": "TEA-250", "quantity": 2}, {"code": SUGAR, "quantity": 3}])
    zakya.raise_invoice("c-901", [{"code": TEA, "quantity": 1}])
    b2b_return = zakya.return_goods(small, [{"code": TEA, "quantity": 4}])
    zakya.return_goods(counter, [{"code": "SUGAR-1", "quantity": 1}])
    today = zakya.invoices[small]["date"]

    monkeypatch.chdir(DEMO.parent)
    forget_tokens()
    sap_sim = _module("sap")
    erp = sap_sim.ErpSim(tmp_path / "sap.sqlite")
    port = _free_port()
    erp_server = sap_sim.serve(erp, port)
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
            if connection in ("sap", "sap_master"):
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

        def run(flow: str, *, held: bool = False) -> None:
            started = runtime.start_run(flow)
            engine.run_until_idle()
            got = store.runs.get_run(started)
            assert got.status == "succeeded" or (held and "held" in str(got.reason)), (flow, got.reason)

        def summaries() -> list[tuple[str, str, float, float]]:
            return sorted(
                (d["body"]["SummaryType"], d["items"][0]["material"], d["items"][0]["quantity"], d["total"])
                for d in erp.documents(iflow="salessummary")
                if d["status"] == "posted"
            )

        try:
            for flow in ("zakya-invoices", "zakya-credit-notes", "sap-materials"):
                run(flow)
            with psycopg.connect(staging_url) as conn:
                staged = dict(conn.execute("select invoice_id, channel from zakya_demo.invoices").fetchall())
                returns = dict(conn.execute("select creditnote_id, channel from zakya_demo.credit_notes").fetchall())
            assert sorted(staged.values()) == ["B2B", "B2B", "B2C", "B2C"] and set(staged) == set(zakya.invoices)
            assert sorted(returns.values()) == ["B2B", "B2C"]

            clock.advance(timedelta(minutes=5))  # on_ready's at_most_every
            run("invoices-to-sap", held=True)
            (delivery,) = store.ledger.list_deliveries(route="sap")
            assert delivery.status == "held" and erp.posts == 0  # nothing of it reaches SAP before approval
            runtime.approve("sap", delivery.version_id, actor="ann", reason="checked with finance")
            engine.run_until_idle()
            assert store.ledger.get("sap", delivery.version_id).status == "applied"
            invoices = {d["business_key"]: d for d in erp.documents(iflow="invoices")}
            assert sorted(invoices) == sorted([small, big])  # B2B only: one billing document per invoice
            assert [(i["material"], i["quantity"]) for i in invoices[small]["items"]] == [("MAT-1001", 10), ("MAT-1002", 5)]
            answers = store.responses.of_delivery("sap", delivery.version_id)
            assert len(answers) == 2 and all(str(a.fields["sap_document"]).startswith("9") for a in answers)

            run("b2b-returns-to-sap")
            (memo,) = erp.documents(iflow="creditmemos")
            assert (memo["business_key"], memo["reference"], memo["status"]) == (b2b_return, small, "posted")
            assert [(i["material"], i["quantity"]) for i in memo["items"]] == [("MAT-1001", -4)]

            run("b2c-to-sap")
            assert summaries() == [
                ("B2C_RETURNS", "MAT-1005", -1, -48.0),  # the sugar back, on the day it came back
                ("B2C_SALES", "MAT-1001", 3, 540.0),  # tea of both counter sales, in one summary
                ("B2C_SALES", "MAT-1005", 3, 144.0),
            ]
            day = {r["material"]: r for r in erp.by_material() if r["posting_date"] == today}
            assert (day["MAT-1001"]["sold_qty"], day["MAT-1001"]["returned_qty"], day["MAT-1001"]["net_qty"]) == (13, 4, 9)
            assert (day["MAT-1001"]["b2b_qty"], day["MAT-1001"]["b2c_qty"]) == (6, 3)
            assert (day["MAT-1005"]["net_qty"], day["MAT-1005"]["net_amount"]) == (2, 96.0)
            assert day["MAT-1002"]["net_qty"] == 805

            posts = erp.posts
            clock.advance(timedelta(minutes=5))
            for flow in ("invoices-to-sap", "b2b-returns-to-sap", "b2c-to-sap"):
                run(flow)  # nothing changed in staging: nothing posted
            assert erp.posts == posts

            zakya.raise_invoice("c-900", [{"code": TEA, "quantity": 2}])  # later that day, another counter sale
            run("zakya-invoices")
            clock.advance(timedelta(minutes=5))
            run("b2c-to-sap")
            assert erp.posts == posts + 1  # only the new line, summed on its own
            tea = next(r for r in erp.by_material() if r["posting_date"] == today and r["material"] == "MAT-1001")
            assert (tea["b2c_qty"], tea["net_qty"]) == (5, 11)  # the day's summaries add up

            answered = {
                len(store.responses.of_delivery("sap-b2c-summary", d.version_id)): d
                for d in store.ledger.list_deliveries(route="sap-b2c-summary")
            }
            first, second = answered[3], answered[1]
            runtime.compensate("sap-b2c-summary", second.version_id, actor="ann", reason="posted twice by hand")
            engine.run_until_idle()
            assert store.ledger.get("sap-b2c-summary", second.version_id).status == "compensated"
            tea = next(r for r in erp.by_material() if r["posting_date"] == today and r["material"] == "MAT-1001")
            assert tea["b2c_qty"] == 3 and store.ledger.get("sap-b2c-summary", first.version_id).status == "applied"
        finally:
            erp_server.shutdown()
            erp.close()
            forget_tokens()
