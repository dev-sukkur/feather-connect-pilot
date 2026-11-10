"""`feather server`, `feather worker` and the server's admin commands, against the test Postgres."""

import hashlib
import json
import logging
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.rows import dict_row
from typer.testing import Result

from feather_cli.commands import processes
from feather_connect.logs import TaskLogHandler
from feather_connect.storage.port import Store
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import at_head
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.storage.postgres.support import Pool
from feather_server.app import create_app
from feather_server.auth import verify_password
from feather_server.settings import Settings

pytestmark = pytest.mark.postgres


@pytest.fixture
def database(pg_url: str) -> Iterator[str]:
    """A new, empty database on the test server (not migrated)."""
    admin = psycopg.connect(pg_url, autocommit=True)
    name = f"feather_cli_{uuid.uuid4().hex[:10]}"
    admin.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
    try:
        yield re.sub(r"/[^/]*$", f"/{name}", pg_url)
    finally:
        admin.execute(sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name)))
        admin.close()


@pytest.fixture
def migrated(pg_url: str, cli: Callable[..., Result], monkeypatch: pytest.MonkeyPatch) -> str:
    """The test database of the session, migrated. It is never dropped while a store has a pool on it."""
    monkeypatch.setenv("FEATHER_DATABASE_URL", pg_url)
    r = cli("server", "migrate")
    assert r.exit_code == 0, r.output
    return pg_url


@pytest.fixture
def open_store() -> Iterator[Callable[[str], Store]]:
    """Stores on their own pools, closed at the end: a pool left open on a database that is then dropped logs
    errors, and those pass through every log handler of the process."""
    pools: list[Pool] = []

    def opener(url: str) -> Store:
        pool = Pool(url, min_size=1, max_size=3, kwargs={"row_factory": dict_row}, open=True)
        pools.append(pool)
        return PostgresStore(pool)

    yield opener
    for pool in pools:
        pool.close()


@pytest.fixture(autouse=True)
def no_task_log_handlers() -> Iterator[None]:
    """`create_app` adds a handler for task logs to the root logger and nothing removes it."""
    yield
    root = logging.getLogger()
    for handler in [h for h in root.handlers if isinstance(h, TaskLogHandler)]:
        root.removeHandler(handler)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def spawn(tmp_path: Path) -> Iterator[Callable[..., subprocess.Popen[bytes]]]:
    procs: list[subprocess.Popen[bytes]] = []

    def start(*args: str, env: dict[str, str]) -> subprocess.Popen[bytes]:
        log = (tmp_path / f"proc{len(procs)}.log").open("wb")
        proc = subprocess.Popen(
            [sys.executable, "-m", "feather_cli.main", *args],
            env={**os.environ, **env},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        procs.append(proc)
        return proc

    yield start
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait()


def wait_until(check: Callable[[], object], timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, urllib.error.URLError, KeyError):
            pass
        time.sleep(0.3)
    raise AssertionError("timed out waiting")


def process_env(database: str, project: Path) -> dict[str, str]:
    return {
        "FEATHER_DATABASE_URL": database,
        "FEATHER_PROJECT": str(project),
        "FEATHER_ENV": "dev",
        "FEATHER_SECRET_KEY": "a-test-secret-key",
    }


def test_server_starts_and_readyz_turns_ok(spawn, migrated, project):
    port = free_port()
    proc = spawn("server", "--port", str(port), env=process_env(migrated, project))

    def ready() -> bool:
        assert proc.poll() is None, "the server ended"
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=2) as response:
            return response.status == 200 and json.load(response) == {"database": "ok", "schema": "current"}

    wait_until(ready)


def test_migrate_is_idempotent(cli, database, monkeypatch):
    monkeypatch.setenv("FEATHER_DATABASE_URL", database)
    assert at_head(database) is False
    first = cli("server", "migrate", "--json")
    second = cli("server", "migrate", "--json")
    assert first.exit_code == second.exit_code == 0
    assert json.loads(second.stdout) == {"migrated": True}
    assert at_head(database) is True


def test_create_user_reads_the_password_from_stdin_and_the_user_can_log_in(
    cli, migrated, project, open_store
):
    password = "correct horse battery staple"
    name = "ann-" + uuid.uuid4().hex[:6]
    r = cli("server", "create-user", name, input=password + "\n")
    assert r.exit_code == 0 and name in r.stdout and password not in r.output
    row = open_store(migrated).users.get_user(name)
    assert row is not None and row.password_hash != password and verify_password(row.password_hash, password)

    app = create_app(Settings(database_url=migrated, engine="local", secret_key="k"))
    with TestClient(app, base_url="http://localhost:8000", follow_redirects=False) as browser:
        page = browser.get("/login").text
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', page)
        assert csrf
        form = {"username": name, "password": password, "csrf_token": csrf.group(1)}
        assert browser.post("/login", data=form).status_code == 303
        assert browser.get("/api/v1/me").json()["name"] == name


def test_create_user_refuses_a_name_that_is_taken_and_an_empty_password(cli, migrated):
    name = "dup-" + uuid.uuid4().hex[:6]
    assert cli("server", "create-user", name, input="pw-one-two\n").exit_code == 0
    again = cli("server", "create-user", name, input="pw-one-two\n")
    assert again.exit_code == 1 and name in again.stderr
    empty = cli("server", "create-user", "bob", input="\n")
    assert empty.exit_code == 1 and "password" in empty.stderr


def test_create_token_prints_once_and_stores_only_the_hash(cli, migrated, open_store):
    r = cli("server", "create-token", "ci", "--ttl", "90d", "--json")
    assert r.exit_code == 0
    document = json.loads(r.stdout)
    token = document["token"]
    assert token.startswith("fc_") and document["name"] == "ci"

    row = open_store(migrated).users.find_token(hashlib.sha256(token.encode()).hexdigest())
    assert row is not None and row.name == "ci" and row.expires_at is not None
    assert timedelta(days=89) < row.expires_at - row.created_at <= timedelta(days=90)
    with psycopg.connect(migrated) as conn:
        dump = json.dumps(conn.execute("select * from feather.api_token").fetchall(), default=str)
    assert token not in dump and token[3:] not in dump

    again = cli("server", "create-token", "ci", "--ttl", "1h", "--json")
    assert json.loads(again.stdout)["token"] != token


def test_create_token_with_a_ttl_that_is_not_a_duration_explains(cli, migrated):
    r = cli("server", "create-token", "ci", "--ttl", "soon")
    assert r.exit_code == 1 and "soon" in r.stderr and "90d" in r.stderr


def test_the_first_user_is_an_admin_the_next_an_operator_and_a_role_can_change(
    cli, database, open_store, monkeypatch
):
    monkeypatch.setenv("FEATHER_DATABASE_URL", database)
    assert cli("server", "migrate").exit_code == 0
    first = cli("server", "create-user", "root", "--json", input="pw-one\n")
    assert first.exit_code == 0 and json.loads(first.stdout) == {"user": "root", "role": "admin"}
    second = cli("server", "create-user", "ann", input="pw-two\n")
    assert second.exit_code == 0 and "ann, operator" in second.stdout
    chosen = cli("server", "create-user", "bob", "--role", "admin", "--json", input="pw-3\n")
    assert json.loads(chosen.stdout)["role"] == "admin"
    bad = cli("server", "create-user", "cy", "--role", "viewer", input="pw-4\n")
    assert bad.exit_code == 1 and "not a role" in bad.stderr

    assert cli("server", "set-role", "ann", "admin", "--reason", "on call").exit_code == 0
    assert cli("server", "set-role", "root", "operator").exit_code == 0
    assert cli("server", "set-role", "bob", "operator").exit_code == 0
    last = cli("server", "set-role", "ann", "operator")
    assert last.exit_code == 1 and "only admin" in last.stderr
    unknown = cli("server", "set-role", "nobody", "admin")
    assert unknown.exit_code == 1 and "nobody" in unknown.stderr
    store = open_store(database)
    assert [(u.username, u.role) for u in store.users.list_users()] == [
        ("ann", "admin"),
        ("bob", "operator"),
        ("root", "operator"),
    ]
    changes = [a for a in store.audit.list_audit() if a.action == "set_role" and a.result == "ok"]
    assert len(changes) == 3 and changes[-1].actor == "cli" and changes[-1].request["reason"] == "on call"


def test_a_token_is_an_operator_unless_told_and_never_above_its_user(cli, database, open_store, monkeypatch):
    monkeypatch.setenv("FEATHER_DATABASE_URL", database)
    assert cli("server", "migrate").exit_code == 0
    cli("server", "create-user", "root", input="pw-one\n")
    cli("server", "create-user", "ann", input="pw-two\n")
    robot = json.loads(cli("server", "create-token", "robot", "--json").stdout)
    assert robot["role"] == "operator"
    ops = json.loads(cli("server", "create-token", "ops", "--role", "admin", "--json").stdout)
    assert ops["role"] == "admin"
    mine = json.loads(cli("server", "create-token", "root-ci", "--user", "root", "--json").stdout)
    assert mine["role"] == "admin"
    above = cli("server", "create-token", "ann-ci", "--user", "ann", "--role", "admin")
    assert above.exit_code == 1 and "higher role than its user ann" in above.stderr
    nobody = cli("server", "create-token", "x", "--user", "nobody")
    assert nobody.exit_code == 1 and "nobody" in nobody.stderr
    row = open_store(database).users.find_token(hashlib.sha256(mine["token"].encode()).hexdigest())
    assert row is not None and (row.role, row.created_by) == ("admin", "root")
    ci = json.loads(cli("server", "create-token", "ci", "--scope", "publish", "--json").stdout)
    found = open_store(database).users.find_token(hashlib.sha256(ci["token"].encode()).hexdigest())
    assert ci["scope"] == "publish" and found is not None and found.scope == "publish"
    wrong = cli("server", "create-token", "x", "--scope", "everything")
    assert wrong.exit_code == 1 and "not a token scope" in wrong.stderr


def test_worker_starts_as_a_supervisor_and_keeps_running(spawn, migrated, project):
    # The project mode is retired (§13): a worker is a supervisor. It registers no worker of its own; its package
    # hosts do, once a package is installed and active. With none, it just keeps running.
    proc = spawn(
        "worker", "--slots", "3", env={**process_env(migrated, project), "FEATHER_WORKER_ID": "w-cli"}
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert proc.poll() is None, "the worker ended"
        time.sleep(0.5)


@pytest.mark.parametrize(
    "args", [("server",), ("server", "migrate"), ("server", "create-user", "x"), ("worker",)]
)
def test_missing_database_url_explains_what_to_set(cli, project, monkeypatch, args):
    monkeypatch.delenv("FEATHER_DATABASE_URL", raising=False)
    r = cli(*args, input="pw\n")
    assert r.exit_code != 0
    assert "FEATHER_DATABASE_URL" in r.stderr and "--database-url" in r.stderr


def test_a_database_that_is_not_migrated_says_to_run_migrate(cli, database, monkeypatch):
    monkeypatch.setenv("FEATHER_DATABASE_URL", database)
    r = cli("server", "create-user", "ann", input="pw-one-two\n")
    assert r.exit_code == 1 and "feather server migrate" in r.stderr


def test_a_database_that_is_away_names_the_problem_not_a_traceback(cli, monkeypatch):
    monkeypatch.setenv("FEATHER_DATABASE_URL", f"postgresql://x:y@127.0.0.1:{free_port()}/none")
    r = cli("server", "migrate")
    assert r.exit_code == 1 and "Traceback" not in r.stderr and "database" in r.stderr


@pytest.mark.parametrize("args", [("create-user", "pat"), ("create-token", "ci")])
def test_a_command_closes_the_store_it_opened(cli, migrated, monkeypatch, args):
    opened = []

    def recording(url):
        store, spool = connect(url)
        opened.append(store)
        return store, spool

    monkeypatch.setattr(processes, "connect", recording)
    name = f"{args[1]}-{uuid.uuid4().hex[:6]}"
    assert cli("server", args[0], name, input="pw-one-two\n").exit_code == 0
    assert [store.runs._db.pool.closed for store in opened] == [True]


def test_a_command_that_fails_closes_its_store_too(cli, migrated, monkeypatch):
    opened = []

    def recording(url):
        store, spool = connect(url)
        opened.append(store)
        return store, spool

    monkeypatch.setattr(processes, "connect", recording)
    assert cli("server", "create-user", "nobody", input="\n").exit_code == 1  # an empty password
    assert [store.runs._db.pool.closed for store in opened] == [True]


def test_the_server_warns_when_it_listens_beyond_this_machine():  # Stage 2 M12
    from feather_cli.commands.processes import bind_warnings

    assert bind_warnings("127.0.0.1", {"FEATHER_SECRET_KEY": "k"}) == []
    warned = bind_warnings("0.0.0.0", {"FEATHER_BASE_URL": "http://localhost:8080"})
    assert any("Secure" in w for w in warned) and any("FEATHER_SECRET_KEY" in w for w in warned)
    assert (
        bind_warnings("0.0.0.0", {"FEATHER_BASE_URL": "https://feather.example", "FEATHER_SECRET_KEY": "k"})
        == []
    )
