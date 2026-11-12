"""The remote commands against a server on the in-memory store and the local engine, served on a free port."""

import json
import socket
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import uvicorn
from typer.testing import Result

from feather_connect.clock import FrozenClock
from feather_connect.core.errors import PermanentError
from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.testing import SECRET, World, build_world, day


@dataclass
class Served:
    url: str
    token: str
    world: World


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Served]:
    monkeypatch.setenv("WH_TOKEN", SECRET)
    world = build_world(tmp_path / "project", FrozenClock(datetime(2026, 10, 18, 10, tzinfo=UTC)))
    token = issue_token(world.store, "cli", timedelta(days=1), role="operator", clock=world.clock)
    port = _free_port()
    config = uvicorn.Config(
        create_app(state=world.state, loops=lambda s: []), host="127.0.0.1", port=port, log_level="warning"
    )
    running = uvicorn.Server(config)
    thread = threading.Thread(target=running.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not running.started and time.monotonic() < deadline:
        time.sleep(0.05)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FEATHER_SERVER", raising=False)
    monkeypatch.delenv("FEATHER_TOKEN", raising=False)
    yield Served(f"http://127.0.0.1:{port}", token, world)
    running.should_exit = True
    thread.join(10)


def _at(server: Served) -> list[str]:
    return ["--server", server.url, "--token", server.token]


def test_status_shows_failures_and_workers(cli: Callable[..., Result], server: Served) -> None:
    server.world.sink("warehouse").fail_next(PermanentError("no such table"))
    server.world.run([day(17)])
    doc = json.loads(cli("status", *_at(server), "--json").stdout)
    assert doc["failed_deliveries"] == 1 and doc["workers"] == []
    text = cli("status", *_at(server)).stdout
    assert "failed deliveries: 1" in text


def test_runs_filter_and_json(cli: Callable[..., Result], server: Served) -> None:
    server.world.run([day(16)])
    server.world.run([day(17)])
    doc = json.loads(cli("runs", "--flow", "orders", "--status", "succeeded", *_at(server), "--json").stdout)
    assert len(doc["items"]) == 2 and {r["status"] for r in doc["items"]} == {"succeeded"}
    assert json.loads(cli("runs", "--status", "failed", *_at(server), "--json").stdout)["items"] == []


def test_logs_print_a_run_task_logs(cli: Callable[..., Result], server: Served) -> None:
    run_id = server.world.run([day(18)])
    r = cli("logs", run_id, *_at(server), "--json")
    assert r.exit_code == 0, r.output
    doc = json.loads(r.stdout)
    assert doc["run_id"] == run_id and doc["tasks"] and all("lines" in t for t in doc["tasks"])


def test_backfill_dry_run_prints_the_impact_and_changes_nothing(
    cli: Callable[..., Result], server: Served
) -> None:
    r = cli("backfill", "orders", "--from", "2026-10-10", "--to", "2026-10-12", "--dry-run", *_at(server))
    assert r.exit_code == 0, r.output
    assert "3 partition(s)" in r.stdout and "nothing done" in r.stdout
    assert server.world.store.runs.list_runs() == []


def test_backfill_yes_starts_the_run(cli: Callable[..., Result], server: Served) -> None:
    r = cli(
        "backfill", "orders", "--from", "2026-10-10", "--to", "2026-10-11", "--yes", *_at(server), "--json"
    )
    doc = json.loads(r.stdout)
    assert r.exit_code == 0 and doc["done"] and doc["run_id"]
    assert [run.trigger for run in server.world.store.runs.list_runs()] == ["backfill"]


def test_backfill_asks_and_a_no_does_nothing(cli: Callable[..., Result], server: Served) -> None:
    r = cli("backfill", "orders", "--from", "2026-10-10", "--to", "2026-10-11", *_at(server), input="no\n")
    assert r.exit_code == 0 and "nothing done" in r.stdout
    assert server.world.store.runs.list_runs() == []


def test_replay_by_version_and_by_partition(cli: Callable[..., Result], server: Served) -> None:
    server.world.run([day(18)])
    version = server.world.store.versions.latest_sealed("orders", day(18))
    assert version is not None
    r = cli("replay", "warehouse", "--version", version.version_id, "--yes", *_at(server), "--json")
    assert r.exit_code == 0, r.output
    assert json.loads(r.stdout)["moved"] == [f"warehouse:{version.version_id}"]
    server.world.engine.run_until_idle()
    r = cli("replay", "gofrugal", "--partition", day(18), "--yes", *_at(server), "--json")
    assert r.exit_code == 0, r.output and len(json.loads(r.stdout)["moved"]) == 1
    assert cli("replay", "warehouse", *_at(server), "--json").exit_code == 1  # neither given


def test_run_without_local_starts_it_on_the_server(cli: Callable[..., Result], server: Served) -> None:
    r = cli("run", "orders", "--partition", day(18), *_at(server), "--json")
    assert r.exit_code == 0, r.output
    run_id = json.loads(r.stdout)["run_id"]
    assert server.world.store.runs.get_run(run_id).flow == "orders"


def test_server_and_token_come_from_the_env_file(
    cli: Callable[..., Result], server: Served, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = Path("project")
    monkeypatch.setenv("CLI_TOKEN", server.token)
    (project / "env" / "qa.yaml").write_text(
        f'server: {{url: "{server.url}", token: "${{secret:env:CLI_TOKEN}}"}}\n', encoding="utf-8"
    )
    r = cli("status", "--project", str(project), "--env", "qa", "--json")
    assert r.exit_code == 0, r.output


def test_unreachable_server_names_the_url(cli: Callable[..., Result]) -> None:
    r = cli("status", "--server", "http://127.0.0.1:9", "--token", "x", "--json")
    assert r.exit_code != 0 and "127.0.0.1:9" in json.loads(r.stdout)["error"]
    r = cli("status", "--server", "http://127.0.0.1:9", "--token", "x")
    assert r.exit_code != 0 and "127.0.0.1:9" in r.stderr


def test_rejected_token_says_so(cli: Callable[..., Result], server: Served) -> None:
    r = cli("status", "--server", server.url, "--token", "fc_wrong", "--json")
    doc = json.loads(r.stdout)
    assert r.exit_code != 0 and "refused the token" in doc["error"] and server.url in doc["error"]
    assert "--token" in doc["hint"]


def test_no_server_configured_says_where_to_set_one(
    cli: Callable[..., Result], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FEATHER_SERVER", raising=False)
    r = cli("runs", "--json")
    assert r.exit_code != 0 and "FEATHER_SERVER" in json.loads(r.stdout)["hint"]


def test_the_env_files_token_goes_only_to_the_env_files_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:  # stage 5 review
    from feather_cli.commands.remote import client

    (tmp_path / "env").mkdir()
    (tmp_path / "env" / "qa.yaml").write_text(
        'server: {url: "https://feather.qa.example", token: "fc_qa_token"}\n', encoding="utf-8"
    )
    with client(tmp_path, "qa", "https://somewhere.else.example", None, json_mode=True) as other:
        assert "authorization" not in other._http.headers  # pyright: ignore[reportPrivateUsage]
    with client(tmp_path, "qa", None, None, json_mode=True) as own:
        assert own._http.headers["authorization"] == "Bearer fc_qa_token"  # pyright: ignore[reportPrivateUsage]
    with client(tmp_path, "qa", "https://feather.qa.example/", None, json_mode=True) as same:
        assert same._http.headers["authorization"] == "Bearer fc_qa_token"  # pyright: ignore[reportPrivateUsage]


def test_ready_close_fire_and_status(cli: Callable[..., Result], server: Served) -> None:
    import feather_connect as fc
    from feather_connect.connectors import memory
    from feather_connect.project import prefixed
    from feather_connect.runtime.catalogue import ActiveFlow

    by_day = fc.partition.by_day("date", tz="UTC")
    invoices = fc.Dataset(
        "invoices", source=memory.Source(), key="id", strategy=fc.strategy.replace(), partition=by_day
    )
    ingest = fc.Flow("ingest", reads=invoices, deliver=[fc.to(memory.Sink(connection="staging"))])
    posting = fc.Flow(
        "posting",
        reads=fc.Dataset(
            "totals", source=memory.Source(connection="t"), key="id", strategy=fc.strategy.replace()
        ),
        deliver=[fc.to(memory.Sink(connection="erp"))],
        triggers=[fc.on_ready(fc.ready(invoices), each="day", tz="UTC")],
    )
    flows = [
        ActiveFlow(f.name, "acme", "1.0.0", (), f.overlap, f.catch_up, True, frozenset(), f.spec_dict())
        for f in prefixed([ingest, posting], "acme")
    ]

    class Catalogue:
        def flows(self) -> list[ActiveFlow]:
            return flows

    server.world.state.catalogue = Catalogue()  # type: ignore[assignment]
    closed = cli(
        "ready",
        "close",
        "acme/invoices",
        "--partition",
        "date=2026-10-17",
        "--reason",
        "day done",
        *_at(server),
    )
    assert closed.exit_code == 0, closed.output
    assert "closed by cli" in closed.stdout
    fired = cli(
        "ready",
        "fire",
        "acme/posting",
        "--period",
        "2026-10-17",
        "--reason",
        "by hand",
        *_at(server),
        "--json",
    )
    assert fired.exit_code == 0, fired.output
    run_id = json.loads(fired.stdout)["run_id"]
    assert server.world.store.runs.get_run(run_id).trigger == "ready"
    shown = cli("ready", "status", "acme/posting", *_at(server))
    assert shown.exit_code == 0, shown.output
    assert "acme/posting: acme/invoices, each day" in shown.stdout and "2026-10-17" in shown.stdout
    refused = cli("ready", "fire", "acme/posting", "--period", "2026-10-17", *_at(server))
    assert refused.exit_code != 0  # no reason


def test_holds_list_and_reject(cli: Callable[..., Result], server: Served) -> None:
    from feather_connect.storage.port import VersionDraft

    store = server.world.store
    run = store.runs.create_run("acme/orders", "every", "t", {})
    store.versions.stage(VersionDraft("v9", "acme/orders", "_", run, True, 1, 0, "h", None, None, "s"))
    store.versions.seal(["v9"], ["acme/sap"])
    store.ledger.transition("acme/sap", "v9", "running", actor="system")
    store.ledger.transition("acme/sap", "v9", "held", actor="system", reason="approval: 1 record to approve")
    listed = cli("holds", "list", *_at(server))
    assert listed.exit_code == 0 and "acme/sap v9 [_]: approval: 1 record to approve" in listed.stdout
    refused = cli("holds", "reject", "acme/sap", "v9", "--reason", "wrong", *_at(server))
    assert refused.exit_code != 0  # the token's user isn't an approver (M2 plan 2 O2)
    store.users.create_user("ann", "h", "operator")
    store.users.set_grants("ann", ["approver"])
    token = issue_token(
        store, "ann-cli", timedelta(days=1), role="operator", created_by="ann", clock=server.world.clock
    )
    at = ["--server", server.url, "--token", token]
    assert cli("holds", "reject", "acme/sap", "v9", *at).exit_code != 0  # no reason
    done = cli("holds", "reject", "acme/sap", "v9", "--reason", "wrong", *at)
    assert done.exit_code == 0, done.output
    assert "rejected" in done.stdout and store.ledger.get("acme/sap", "v9").status == "skipped"
