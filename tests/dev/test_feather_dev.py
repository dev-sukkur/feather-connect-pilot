"""`feather dev` as a person runs it: a real process, a real Postgres, the demo project, and the dashboard over HTTP.

The tests that start the container need Docker and are skipped without it. They touch only the container
`feather-dev-postgres` and its volume `feather-dev-postgres-data`, never any other container.
"""

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest
import yaml
from psycopg import sql
from typer.testing import CliRunner

from feather_cli.commands.dev import _in_use
from feather_cli.main import app

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "examples" / "demo"
CONTAINER = "feather-dev-postgres"
VOLUME = "feather-dev-postgres-data"

# These start a whole stack, some of them twice; the suite's default of 120 s would kill the session.
pytestmark = pytest.mark.timeout(420)
FIVE_MINUTES = 300
# The demo runs on the minute and a stack's first tick only notes the time. So its first run comes within a minute of
# the stack being ready, and its second a minute after that, with some room for a busy machine.
FIRST_RUN_S = 120
TWO_RUNS_S = 180


def docker_works() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], check=False, capture_output=True, timeout=30).returncode == 0
        )
    except (OSError, subprocess.SubprocessError):
        return False


needs_docker = pytest.mark.skipif(not docker_works(), reason="Docker is not running")
needs_postgres = pytest.mark.postgres


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def listening(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=False, capture_output=True, text=True, timeout=120)


def remove_dev_container_and_volume() -> None:
    """Only the dev stack's own container and volume are ever removed."""
    docker("rm", "-f", CONTAINER)
    docker("volume", "rm", "-f", VOLUME)


def wait_until(check: Callable[[], object], timeout: float = 90, what: str = "the condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, httpx.HTTPError, KeyError, psycopg.Error):
            pass
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


@dataclass
class Dev:
    """A running `feather dev` process and a way to look at its dashboard."""

    proc: subprocess.Popen[bytes]
    port: int
    log: Path
    env: dict[str, str]
    cookies: httpx.Cookies = field(default_factory=httpx.Cookies)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def output(self) -> str:
        return self.log.read_text(encoding="utf-8", errors="replace")

    def wait_ready(self, timeout: float = 120) -> None:
        def ready() -> bool:
            assert self.proc.poll() is None, f"feather dev ended early:\n{self.output()}"
            return httpx.get(f"{self.base}/readyz", timeout=2).status_code == 200

        wait_until(ready, timeout, "/readyz")
        # the banner comes once the files are being watched
        wait_until(lambda: "Ctrl+C" in self.output(), timeout, "the banner")

    def password(self) -> str | None:
        found = re.search(r"password:\s*(\S+)", self.output())
        return found.group(1) if found else None

    def sign_in(self) -> httpx.Client:
        password = self.password()
        assert password, f"no password in the output:\n{self.output()}"
        client = httpx.Client(base_url=self.base, follow_redirects=False, timeout=10)
        page = client.get("/login").text
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', page)
        assert csrf
        done = client.post(
            "/login", data={"username": "dev", "password": password, "csrf_token": csrf.group(1)}
        )
        assert done.status_code == 303, done.text
        return client

    def run_ids(self, client: httpx.Client) -> list[str]:
        return re.findall(r'href="/runs/([^"]+)"', client.get("/runs").text)

    def interrupt(self) -> int:
        """Press Ctrl+C: CTRL_BREAK_EVENT to the process group on Windows, SIGINT elsewhere."""
        if sys.platform == "win32":
            self.proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        else:
            self.proc.send_signal(signal.SIGINT)
        return self.proc.wait(timeout=90)


@pytest.fixture
def start_dev(tmp_path: Path) -> Iterator[Callable[..., Dev]]:
    started: list[Dev] = []
    roots: list[Path] = []  # the project of each

    def start(*args: str, env: dict[str, str] | None = None, port: int | None = None) -> Dev:
        port = port or free_port()
        merged = {k: v for k, v in os.environ.items() if k != "FEATHER_DATABASE_URL"}
        merged.update(env or {})
        log = tmp_path / f"dev{len(started)}.log"
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0  # type: ignore[attr-defined]
        proc = subprocess.Popen(
            [sys.executable, "-m", "feather_cli.main", "dev", "--no-browser", "--port", str(port), *args],
            env=merged,
            stdout=log.open("wb"),
            stderr=subprocess.STDOUT,
            creationflags=flags,
        )
        dev = Dev(proc, port, log, merged)
        started.append(dev)
        roots.append(Path(args[args.index("--project") + 1]) if "--project" in args else DEMO)
        return dev

    yield start
    for dev, root in zip(started, roots, strict=True):
        if dev.proc.poll() is None:
            try:
                dev.interrupt()  # Ctrl+C stops the helpers before feather ends
            except subprocess.TimeoutExpired:
                dev.proc.kill()
        dev.proc.wait()
        # A helper may still be ending (after a kill, Linux tells it so only once feather is gone): the next test that
        # starts feather dev on this project would find its port taken.
        config: dict[str, Any] = yaml.safe_load((root / "feather.yaml").read_text(encoding="utf-8")) or {}
        declared: dict[str, Any] = config.get("dev") or {}
        ports = [p["port"] for p in declared.get("processes") or [] if p.get("port")]
        wait_until(lambda: not any(_in_use(p) for p in ports), 30, f"helper ports {ports} to be free")  # noqa: B023


@pytest.fixture
def database(pg_url: str) -> Iterator[str]:
    """A new, empty database on the test server, for `FEATHER_DATABASE_URL`."""
    admin = psycopg.connect(pg_url, autocommit=True)
    name = f"feather_dev_{uuid.uuid4().hex[:10]}"
    admin.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
    try:
        yield re.sub(r"/[^/]*$", f"/{name}", pg_url)
    finally:
        admin.execute(sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name)))
        admin.close()


@pytest.fixture
def dev(
    database: str, start_dev: Callable[..., Dev]
) -> Dev:  # database first, so feather dev stops before it is dropped
    """`feather dev` on the test Postgres through FEATHER_DATABASE_URL, so it needs no Docker."""
    running = start_dev(env={"FEATHER_DATABASE_URL": database})
    running.wait_ready()
    return running


@pytest.fixture
def dev_clean() -> Iterator[None]:
    """A machine that has never run `feather dev`: no container and no volume."""
    remove_dev_container_and_volume()
    yield


@needs_postgres
def test_works_with_an_existing_database_url(dev, database):
    out = dev.output()
    assert "FEATHER_DATABASE_URL" in out and "http://localhost" in out
    assert dev.password()
    assert dev.sign_in().get("/api/v1/me").json() == {
        "kind": "human",
        "name": "dev",
        "role": "admin",
        "user": "dev",
    }  # the dev user is an admin


@needs_postgres
def test_demo_runs_and_reaches_destinations(dev, database, pg_connect):
    client = dev.sign_in()
    wait_until(lambda: len(dev.run_ids(client)) >= 2, TWO_RUNS_S, "two runs on the dashboard")
    store, _ = pg_connect(database)

    def applied(route: str) -> bool:
        done = store.ledger.list_deliveries(route=f"demo/{route}", status="applied")
        return bool(done) and sum(d.rows_applied for d in done) > 0

    # dev delivers to real targets: the demo schema of this database, a DuckDB file and sims/targets.py.
    wait_until(
        lambda: all(applied(route) for route in ("warehouse", "lake", "erp")),
        90,
        "rows applied on all three routes",
    )
    with psycopg.connect(database) as conn:
        assert conn.execute("select count(*) from demo.orders").fetchone() == (5,)
    page = client.get("/flows").text
    assert "orders" in page


@needs_postgres
def test_editing_a_flow_reloads_it(database, start_dev, tmp_path, pg_connect):
    project = tmp_path / "demo"
    shutil.copytree(DEMO, project, ignore=shutil.ignore_patterns("__pycache__", ".data"))
    running = start_dev("--project", str(project), env={"FEATHER_DATABASE_URL": database})
    running.wait_ready()
    store, _ = pg_connect(database)
    flow_file = project / "flows" / "orders.py"
    wait_until(lambda: store.catalog.flow("demo/orders").spec["owner"] == "demo", 120, "the installed flow")

    flow_file.write_text(flow_file.read_text().replace('owner = "demo"', 'owner = "reloaded"'))
    wait_until(lambda: store.catalog.flow("demo/orders").spec["owner"] == "reloaded", 60, "the reloaded flow")
    # the worker registers the new code before the restarted server is back
    wait_until(
        lambda: httpx.get(f"{running.base}/readyz", timeout=5).status_code == 200, 60, "the restarted server"
    )

    # a file that does not load leaves the running stack alone and says why
    flow_file.write_text("this is not python (\n")
    wait_until(lambda: "reload failed" in running.output(), 60, "the reload failure message")
    assert httpx.get(f"{running.base}/readyz", timeout=5).status_code == 200
    assert store.catalog.flow("demo/orders").spec["owner"] == "reloaded"

    flow_file.write_text(
        DEMO.joinpath("flows", "orders.py").read_text().replace('owner = "demo"', 'owner = "fixed"')
    )
    wait_until(lambda: store.catalog.flow("demo/orders").spec["owner"] == "fixed", 60, "the repaired flow")


def test_docker_not_running_explains_what_to_do(fake_no_docker):
    r = CliRunner().invoke(app, ["dev", "--no-browser", "--port", str(free_port())])
    assert r.exit_code != 0
    assert "Docker" in r.stderr and "FEATHER_DATABASE_URL" in r.stderr
    assert "Traceback" not in r.stderr


def test_docker_installed_but_not_answering_explains_what_to_do(fake_docker_daemon_down):
    r = CliRunner().invoke(app, ["dev", "--no-browser", "--port", str(free_port())])
    assert r.exit_code != 0
    assert "Docker" in r.stderr and "FEATHER_DATABASE_URL" in r.stderr


def test_no_docker_is_reported_before_a_busy_helper_port(fake_no_docker, tmp_path):
    """Without Docker nothing can run, so that is the first thing to say; a busy helper port can wait."""
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        project = tmp_path / "helpers"
        (project / "env").mkdir(parents=True)
        (project / "env" / "dev.yaml").write_text("{}\n")
        helper = f"    - {{name: sim, command: [python, x.py], port: {port}}}\n"
        (project / "feather.yaml").write_text("name: helpers\nmodules: []\ndev:\n  processes:\n" + helper)
        r = CliRunner().invoke(
            app, ["dev", "--no-browser", "--port", str(free_port()), "--project", str(project)]
        )
    assert r.exit_code != 0
    assert "Docker" in r.stderr and "already in use" not in r.stderr


def test_an_unreachable_database_url_is_explained_without_a_traceback(monkeypatch, tmp_path):  # M4
    monkeypatch.setenv("FEATHER_DATABASE_URL", f"postgresql://nobody:x@127.0.0.1:{free_port()}/none")
    project = tmp_path / "plain"
    (project / "flows").mkdir(parents=True)
    (project / "flows" / "__init__.py").write_text("")
    (project / "feather.yaml").write_text("name: plain\nmodules: []\n")
    (project / "env").mkdir()
    (project / "env" / "dev.yaml").write_text("{}\n")
    r = CliRunner().invoke(
        app, ["dev", "--no-browser", "--port", str(free_port()), "--project", str(project)]
    )
    assert r.exit_code != 0
    assert "FEATHER_DATABASE_URL" in r.stderr and "Traceback" not in r.stderr + r.stdout


def test_port_taken_names_the_port_and_the_flag(monkeypatch):
    monkeypatch.delenv("FEATHER_DATABASE_URL", raising=False)
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        r = CliRunner().invoke(app, ["dev", "--no-browser", "--port", str(port)])
    assert r.exit_code != 0
    assert str(port) in r.stderr and "--port" in r.stderr
    assert r.stdout == ""


def test_port_taken_with_json_is_still_one_json_document(monkeypatch):
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        r = CliRunner().invoke(app, ["dev", "--no-browser", "--port", str(port), "--json"])
    assert r.exit_code != 0
    assert str(port) in json.loads(r.stdout)["error"]


def test_a_project_that_does_not_load_stops_before_anything_starts(tmp_path, monkeypatch):
    monkeypatch.delenv("FEATHER_DATABASE_URL", raising=False)
    r = CliRunner().invoke(
        app, ["dev", "--no-browser", "--port", str(free_port()), "--project", str(tmp_path)]
    )
    assert r.exit_code != 0 and "feather.yaml" in r.stderr


@pytest.fixture
def fake_no_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FEATHER_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))


@pytest.fixture
def fake_docker_daemon_down(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A `docker` on the PATH that fails as the client does when the daemon is not running."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    if sys.platform == "win32":
        (bin_dir / "docker.bat").write_text(
            "@echo Cannot connect to the Docker daemon 1>&2\r\n@exit /b 1\r\n"
        )
    else:
        script = bin_dir / "docker"
        script.write_text("#!/bin/sh\necho Cannot connect to the Docker daemon >&2\nexit 1\n")
        script.chmod(0o755)
    monkeypatch.delenv("FEATHER_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))


@needs_docker
def test_dev_shows_the_demo_flow_within_five_minutes(dev_clean, start_dev):  # M1 exit criterion, NFR-06
    started = time.monotonic()
    running = start_dev()
    running.wait_ready(FIVE_MINUTES)
    client: Any = None

    def shown() -> bool:
        nonlocal client
        client = client or running.sign_in()
        return bool(running.run_ids(client)) and "orders" in client.get("/flows").text

    wait_until(shown, FIVE_MINUTES, "the demo flow on the dashboard")
    seconds_until_demo_flow_visible = time.monotonic() - started
    print(f"demo visible after {seconds_until_demo_flow_visible:.0f} s")
    assert seconds_until_demo_flow_visible <= FIVE_MINUTES
    assert running.interrupt() == 0


@needs_docker
def test_ctrl_c_then_restart_keeps_data_and_leaves_no_processes(dev_clean, start_dev):
    first = start_dev()
    first.wait_ready(FIVE_MINUTES)
    client = first.sign_in()
    wait_until(lambda: len(first.run_ids(client)) >= 2, TWO_RUNS_S, "two runs")
    seen = set(first.run_ids(client))
    password = first.password()
    assert password and "the user dev already exists" not in first.output()

    assert first.interrupt() == 0
    assert not listening(first.port)  # nothing is serving any more
    assert docker("inspect", "-f", "{{.State.Running}}", CONTAINER).stdout.strip() == "false"
    assert docker("volume", "inspect", VOLUME).returncode == 0  # the data stays

    second = start_dev()
    second.wait_ready(FIVE_MINUTES)
    assert (
        "the user dev already exists" in second.output() and second.password() is None
    )  # the password is shown once
    again = httpx.Client(base_url=second.base, follow_redirects=False, timeout=10)
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', again.get("/login").text)
    assert csrf
    done = again.post("/login", data={"username": "dev", "password": password, "csrf_token": csrf.group(1)})
    assert done.status_code == 303
    assert seen <= set(second.run_ids(again))  # the earlier runs are still there
    assert second.interrupt() == 0


@needs_postgres
def test_reset_password_gives_a_new_password_and_keeps_the_data(database, start_dev):
    env = {"FEATHER_DATABASE_URL": database}
    first = start_dev(env=env)
    first.wait_ready()
    old = first.password()
    assert old
    client = first.sign_in()
    wait_until(lambda: first.run_ids(client), FIRST_RUN_S, "a run")
    seen = set(first.run_ids(client))
    assert first.interrupt() == 0

    second = start_dev("--reset-password", env=env)
    second.wait_ready()
    new = second.password()
    assert new and new != old

    def sign_in_with(password: str) -> int:
        again = httpx.Client(base_url=second.base, follow_redirects=False, timeout=10)
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', again.get("/login").text)
        assert csrf
        data = {"username": "dev", "password": password, "csrf_token": csrf.group(1)}
        return again.post("/login", data=data).status_code

    assert sign_in_with(new) == 303
    assert sign_in_with(old) != 303
    assert seen <= set(second.run_ids(second.sign_in()))  # the data is kept
    # The password goes to the console only, never into a log line.
    json_lines = [line for line in second.output().splitlines() if line.startswith("{")]
    assert json_lines and not any(new in line for line in json_lines)
    assert second.interrupt() == 0


RELOADS = """
import faulthandler, json, sys, time
from pathlib import Path

import psycopg

from feather_cli.commands.dev import Stack
from feather_connect.storage.postgres.migrate import migrate
from feather_server.settings import Settings

url, project, port = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])
faulthandler.dump_traceback_later(100, exit=True, file=sys.stdout)  # within the test's limit, and it says where


def backends():
    with psycopg.connect(url, autocommit=True) as conn:
        return conn.execute(
            "select application_name, state, left(query, 80) from pg_stat_activity"
            " where datname = current_database() and pid <> pg_backend_pid()"
        ).fetchall()


migrate(url)
stack = Stack(Settings(env="dev", database_url=url, secret_key="k" * 32, engine_poll_s=0.5), project)
counts = []
for _ in range(3):
    stack.start(port)
    stack.stop()
    time.sleep(1)
    found = backends()
    counts.append(len(found))
    print("BACKENDS", found, flush=True)
print("COUNTS " + json.dumps(counts), flush=True)
"""


@needs_postgres
def test_a_reload_leaves_no_connections_behind(tmp_path: Path, pg_url: str) -> None:  # F10
    """Start and stop the stack as a reload does, in a process of its own, since DBOS is one per process."""
    project = tmp_path / "demo"
    shutil.copytree(DEMO, project, ignore=shutil.ignore_patterns("__pycache__", "out"))
    name = f"feather_reload_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(pg_url, autocommit=True) as admin:
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
    url = pg_url.rsplit("/", 1)[0] + f"/{name}"
    try:
        done = subprocess.run(
            [sys.executable, "-c", RELOADS, url, str(project), str(free_port())],
            check=False,
            capture_output=True,
            text=True,
            timeout=110,
            cwd=tmp_path,
        )
        found = re.search(r"COUNTS (\[.*\])", done.stdout)
        assert found, f"{done.stdout[-2000:]}\n{done.stderr[-2000:]}"
        first, _, last = json.loads(found.group(1))
        assert last <= first, f"backends after each stop: {found.group(1)}\n{done.stdout[-2000:]}"
    finally:
        with psycopg.connect(pg_url, autocommit=True) as admin:
            admin.execute(sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name)))
