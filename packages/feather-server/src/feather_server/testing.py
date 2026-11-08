"""A small world for tests of the server: a project on disk with one flow, `orders`, run on the in-memory store and
the local engine. `build_world` makes it, and the services of the server are ready on it.

The project is loaded from `root` for real, so its configuration has layers, and the warehouse connection holds a secret
reference that resolves to `SECRET` when the environment variable `WH_TOKEN` is set to it.
"""

import re
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

from fastapi import FastAPI
from fastapi.testclient import TestClient

from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink
from feather_connect.engine.local import LocalEngine
from feather_connect.project import load_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import Role
from feather_connect.storage.spool import MemorySpool
from feather_server.app import AppState
from feather_server.auth import hash_password
from feather_server.services.actions import Actions
from feather_server.services.reads import Reads
from feather_server.settings import Settings

SECRET = "s3cret-warehouse-token"
PASSWORD = "correct horse battery staple"

FLOW_MODULE = """
    import feather_connect as fc
    from feather_connect.connectors import memory
    from feather_connect.core.connector import ConnectorSpec

    ROWS = [
        {"id": day * 10 + n, "date": f"2026-10-{day:02d}"} for day in range(8, 19) for n in (1, 2)
    ]


    class Shop(memory.Source):
        spec = ConnectorSpec(
            name="shop.api", strategies=frozenset({"replace"}), cost={"read": 2}
        )


    orders = fc.Dataset(
        "orders",
        source=Shop("shop", ROWS),
        key="id",
        strategy=fc.strategy.replace(),
        partition=fc.partition.by_day("date", tz="UTC"),
    )
    flow = fc.Flow(
        "orders",
        reads=orders,
        deliver=[fc.to(memory.Sink("warehouse")), fc.to(memory.Sink("gofrugal"))],
        triggers=[fc.every("15m")],
        owner="ops",
        tags=["retail"],
    )
"""

FEATHER_YAML = """
    name: acme
    modules: [flows.orders]
    settings: {timezone: UTC}
    connections:
      shop: {connector: shop.api, site: hq}
      warehouse:
        connector: memory.sink
        database: dev_bronze
        auth: {token: "${secret:env:WH_TOKEN}"}
        budget: api
      gofrugal: {connector: memory.sink}
    budgets:
      api: {limits: ["90/minute"], reserve: {high: 0.6}}
"""

ENV_DEV = """
    flows:
      orders:
        triggers: {every: 15m}
"""

SECRET = "s3cret-warehouse-token"


def day(n: int) -> str:
    """The key of the partition of October 2026's day `n`."""
    return f"date=2026-10-{n:02d}"


@dataclass
class World:
    state: AppState
    engine: LocalEngine
    clock: FrozenClock
    actions: Actions
    reads: Reads

    @property
    def store(self) -> MemoryStore:
        assert isinstance(self.state.store, MemoryStore)
        return self.state.store

    def sink(self, route: str) -> Sink:
        found = next(r.sink for r in self.state.runtime.flow("orders").routes if r.name == route)
        assert isinstance(found, Sink)
        return found

    def run(self, partitions: list[str] | None = None) -> str:
        """Run the flow to the end and return the run."""
        run_id = self.state.runtime.start_run("orders", partitions=partitions)
        self.engine.run_until_idle()
        return run_id


def build_world(root: Path, clock: FrozenClock) -> World:
    """Write the project into the empty directory `root` and load it. The caller sets `WH_TOKEN` if it wants the
    warehouse secret to resolve."""
    for rel, text in (
        ("feather.yaml", FEATHER_YAML),
        ("flows/__init__.py", ""),
        ("flows/orders.py", FLOW_MODULE),
        ("env/dev.yaml", ENV_DEV),
    ):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(dedent(text), encoding="utf-8")
    project = load_project(root, "dev")
    sys.modules.pop("flows", None)  # the next world builds its own flows
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    for budget in project.budgets.values():
        store.budgets.define(budget)
    runtime = Runtime(project.flows, store, spool, engine, code_sha="sha", clock=clock)
    engine.register(runtime.tasks())
    state = AppState(
        Settings(secret_key="test-secret-key", engine="local"),
        project,
        store,
        spool,
        engine,
        runtime,
        clock=clock,
    )
    return World(state, engine, clock, Actions(state), Reads(state))


def sign_in(app: FastAPI, username: str = "op", role: Role = "operator") -> TestClient:
    """A browser signed in to `app` as a new user with `role`. Redirects are not followed, as a test wants to see
    them."""
    state: AppState = app.state.feather
    state.store.users.create_user(username, hash_password(PASSWORD), role)
    browser = TestClient(app, base_url="http://localhost:8000", follow_redirects=False)
    found = re.search(r'name="csrf_token" value="([^"]+)"', browser.get("/login").text)
    assert found
    browser.post("/login", data={"username": username, "password": PASSWORD, "csrf_token": found.group(1)})
    return browser


# Package registries, for the tests of the built-in registry, `feather package` and `feather registry`.

SAMPLE_PACKAGE = Path(__file__).resolve().parents[4] / "examples" / "packages" / "acme-invoices"


def sample_wheel(out_dir: Path, version: str | None = None) -> Path:
    """The sample flow package acme-invoices, built into `out_dir`, at `version` if given (from a copy of it)."""
    from feather_connect.packages.build import build_package

    root = SAMPLE_PACKAGE
    if version is not None:
        import shutil

        root = out_dir / f"src-{version}"
        shutil.copytree(SAMPLE_PACKAGE, root, ignore=shutil.ignore_patterns("dist", "__pycache__"))
        for name in ("feather.yaml", "pyproject.toml"):
            path = root / name
            text = path.read_text(encoding="utf-8")
            path.write_text(text.replace("1.0.0", version), encoding="utf-8")
    return build_package(root, out_dir).path


@contextmanager
def serve(app: FastAPI) -> Iterator[str]:
    """Run `app` under uvicorn on a free port of 127.0.0.1 in a thread, for tools that need a real server, such as
    `uv publish`. Yields its base URL."""
    import socket
    import threading
    import time

    import uvicorn

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_config=None, lifespan="off"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("the test server did not start")
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(10)


@contextmanager
def static_index(
    files: Mapping[str, Mapping[str, bytes]], token: str, *, tamper: bool = False
) -> Iterator[str]:
    """A tiny PEP 503 index, as a company registry serves one, on a free port: `files` maps each project to its files'
    names and bytes. Every request needs Basic credentials `__token__:<token>`. With `tamper`, each file's bytes differ
    from the sha256 its page gives. Yields the index's URL, ending in /simple/."""
    import base64
    import hashlib
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    expected = "Basic " + base64.b64encode(f"__token__:{token}".encode()).decode()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def _send(self, status: int, body: bytes, kind: str = "text/html") -> None:
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            if status == 401:
                self.send_header("WWW-Authenticate", 'Basic realm="company"')
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            if self.headers.get("Authorization") != expected:
                self._send(401, b"no")
                return
            parts = [p for p in self.path.split("/") if p]
            if parts == ["simple"]:
                links = "".join(f'<a href="{p}/">{p}</a>' for p in files)
                self._send(200, f"<html><body>{links}</body></html>".encode())
            elif len(parts) == 2 and parts[0] == "simple" and parts[1] in files:
                links = "".join(
                    f'<a href="/files/{name}#sha256={hashlib.sha256(data).hexdigest()}">{name}</a>'
                    for name, data in files[parts[1]].items()
                )
                self._send(200, f"<html><body>{links}</body></html>".encode())
            elif len(parts) == 2 and parts[0] == "files":
                found = [d for f in files.values() for n, d in f.items() if n == parts[1]]
                if not found:
                    self._send(404, b"none")
                else:
                    self._send(200, found[0] + (b"x" if tamper else b""), "application/octet-stream")
            else:
                self._send(404, b"none")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/simple/"
    finally:
        server.shutdown()
        server.server_close()
