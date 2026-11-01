"""`feather dev`: the whole stack on this machine (design 18.2).

Postgres comes from `FEATHER_DATABASE_URL`, or else from a container named `feather-dev-postgres` with a named volume.
The server and one worker then run in this process, on the demo project unless `--project` says another, and are
started again whenever a file under `flows/` or `env/`, or `feather.yaml`, changes. Ctrl+C stops them and the
container; the data stays in the volume. The generated password of the `dev` user is printed to the console when the
user is made, or again with `--reset-password`, and never goes to a log.
"""

import hashlib
import json
import logging
import os
import secrets
import shutil
import signal
import socket
import subprocess
import threading
import time
import uuid
import webbrowser
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Annotated, Any

import httpx
import psycopg
import typer
import uvicorn
from watchfiles import PythonFilter, watch

from feather_cli.options import Env, Json
from feather_cli.output import emit, fail
from feather_connect.logs import TaskLogHandler
from feather_connect.project import ProjectError, load_project
from feather_connect.storage.port import Store
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate
from feather_connect.worker import run_worker
from feather_server.app import create_app
from feather_server.auth import hash_password
from feather_server.settings import Settings

log = logging.getLogger(__name__)

CONTAINER = "feather-dev-postgres"
VOLUME = "feather-dev-postgres-data"
IMAGE = "postgres:17"
PORT_LABEL = "feather.dev.port"
DEV_USER = "dev"
HOST = "127.0.0.1"
NO_DOCKER_HINT = (
    "start Docker, or set FEATHER_DATABASE_URL to the Postgres URL of a database you already have"
)


def _demo_project() -> Path | None:
    """The demo project of a source checkout: examples/demo at the repository root."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "examples" / "demo" / "feather.yaml").is_file():
            return parent / "examples" / "demo"
    return None


def _in_use(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(1)
        if probe.connect_ex((HOST, port)) == 0:
            return True
    with socket.socket() as probe:
        try:
            probe.bind((HOST, port))
        except OSError:
            return True
    return False


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind((HOST, 0))
        return probe.getsockname()[1]


# Postgres in Docker.


def _docker(*args: str, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    # The docker that `shutil.which` finds is the one to run: on Windows a bare name is looked up differently.
    docker = shutil.which("docker") or "docker"
    return subprocess.run([docker, *args], check=False, capture_output=True, text=True, timeout=timeout)


def _check_docker(json_mode: bool) -> None:
    if shutil.which("docker") is None:
        fail("Docker is not installed, or is not on the PATH", hint=NO_DOCKER_HINT, json_mode=json_mode)
    try:
        answering = _docker("info", timeout=30).returncode == 0
    except subprocess.TimeoutExpired:
        answering = False
    if not answering:
        fail("Docker is not running", hint=NO_DOCKER_HINT, json_mode=json_mode)


def _first_line(text: str) -> str:
    lines = text.strip().splitlines()
    return lines[-1] if lines else "no output"


def _database_url(port: int) -> str:
    return f"postgresql://feather:feather@{HOST}:{port}/feather"


def _start_postgres_container(json_mode: bool) -> str:
    """Start the dev container (making it the first time) and return its URL once it accepts connections."""
    found = _docker("inspect", CONTAINER)
    if found.returncode == 0:
        info: dict[str, Any] = json.loads(found.stdout)[0]
        port = int(info["Config"]["Labels"][PORT_LABEL])
        if not info["State"]["Running"]:
            started = _docker("start", CONTAINER)
            if started.returncode != 0:
                fail(
                    f"could not start the container {CONTAINER}: {_first_line(started.stderr)}",
                    hint=f"free port {port} on this machine, or remove the container with: docker rm {CONTAINER}",
                    json_mode=json_mode,
                )
    else:
        port = _free_port()  # chosen once, then kept in the container's labels
        typer.echo(f"creating the Postgres container {CONTAINER} (the first time pulls {IMAGE})...", err=True)
        made = _docker(
            *("run", "-d", "--name", CONTAINER, "-v", f"{VOLUME}:/var/lib/postgresql/data"),
            *("-p", f"{HOST}:{port}:5432", "--label", f"{PORT_LABEL}={port}"),
            *("-e", "POSTGRES_USER=feather", "-e", "POSTGRES_PASSWORD=feather", "-e", "POSTGRES_DB=feather"),
            IMAGE,
            timeout=900,
        )
        if made.returncode != 0:
            fail(
                f"could not create the container {CONTAINER}: {_first_line(made.stderr)}",
                hint="check that Docker can pull postgres:17, or set FEATHER_DATABASE_URL",
                json_mode=json_mode,
            )
    url = _database_url(port)
    deadline = time.monotonic() + 120
    while True:
        try:
            psycopg.connect(url, connect_timeout=2).close()
            return url
        except psycopg.Error as error:
            if time.monotonic() > deadline:
                fail(
                    f"Postgres in {CONTAINER} does not answer: {_first_line(str(error))}",
                    hint=f"see: docker logs {CONTAINER}",
                    json_mode=json_mode,
                )
            time.sleep(0.5)


def _stop_postgres_container() -> None:
    _docker("stop", "-t", "10", CONTAINER, timeout=60)


# The server and the worker.


def _code_sha(project: Path) -> str:
    """A name for the code as it is on disk now, so that an edit to a flow is a new version of it."""
    digest = hashlib.sha1()
    files = [
        project / "feather.yaml",
        *sorted((project / "flows").rglob("*")),
        *sorted((project / "env").rglob("*")),
    ]
    for path in files:
        if path.is_file() and "__pycache__" not in path.parts:
            digest.update(path.relative_to(project).as_posix().encode())
            digest.update(path.read_bytes())
    return "dev-" + digest.hexdigest()[:12]


class Stack:
    """One worker and one server, in threads of this process. `start` loads the project afresh each time."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._worker_stop = threading.Event()
        self._worker: threading.Thread | None = None
        self._worker_error: BaseException | None = None
        self._store: Store | None = None  # one pool for the life of the command, not one per reload
        self._server: uvicorn.Server | None = None
        self._server_thread: threading.Thread | None = None

    def start(self, port: int) -> None:
        os.environ["FEATHER_CODE_SHA"] = _code_sha(self._settings.project)
        self._start_worker()
        try:
            self._start_server(port)
        except BaseException:
            self.stop()
            raise

    def _start_worker(self) -> None:
        self._worker_stop = threading.Event()
        self._worker_error = None
        # A worker that was stopped stays `draining` under its id, so each start takes a new one.
        worker_id = f"dev-{uuid.uuid4().hex[:6]}"

        def work() -> None:
            try:
                run_worker(
                    self._settings.project,
                    self._settings.env,
                    database_url=self._settings.database_url,
                    worker_id=worker_id,
                    poll_s=self._settings.engine_poll_s,
                    stop=self._worker_stop,
                )
            except Exception as error:  # noqa: BLE001  (kept, and raised by start() in the main thread)
                self._worker_error = error

        self._worker = threading.Thread(target=work, name="dev-worker", daemon=True)
        self._worker.start()
        self._store = self._store or connect(self._settings.database_url)[0]
        store = self._store
        deadline = time.monotonic() + 60
        while not any(w.worker_id == worker_id and w.state == "alive" for w in store.workers.list_workers()):
            if self._worker_error is not None or not self._worker.is_alive():
                raise RuntimeError(f"the worker did not start: {self._worker_error}") from self._worker_error
            if time.monotonic() > deadline:
                raise RuntimeError("the worker did not register within 60 s")
            time.sleep(0.2)

    def _start_server(self, port: int) -> None:
        config = uvicorn.Config(create_app(self._settings), host=HOST, port=port, log_config=None)
        self._server = uvicorn.Server(config)
        self._server_thread = threading.Thread(target=self._server.run, name="dev-server", daemon=True)
        self._server_thread.start()
        deadline = time.monotonic() + 60
        while not self._server.started:
            if not self._server_thread.is_alive():
                raise RuntimeError(f"the server could not listen on port {port}")
            if time.monotonic() > deadline:
                raise RuntimeError("the server did not start within 60 s")
            time.sleep(0.1)

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._server_thread is not None:
            self._server_thread.join(60)
        self._worker_stop.set()
        if self._worker is not None:
            self._worker.join(90)
        self._server = self._server_thread = self._worker = None
        root = logging.getLogger()
        for handler in [h for h in root.handlers if isinstance(h, TaskLogHandler)]:
            root.removeHandler(handler)  # each start adds its own


def _ensure_dev_user(store: Store, reset: bool = False) -> str | None:
    """Make the user `dev` the first time, and return its generated password; None when it exists.

    With `reset`, an existing user gets a new generated password instead, which is returned. The data is kept.
    """
    exists = store.users.get_user(DEV_USER) is not None
    if exists and not reset:
        return None
    password = secrets.token_urlsafe(12)
    if exists:
        store.users.set_password_hash(DEV_USER, hash_password(password))
    else:
        store.users.create_user(DEV_USER, hash_password(password))
    return password


def _wait_ready(port: int) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"http://{HOST}:{port}/readyz", timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.3)
    raise RuntimeError("the server did not become ready within 60 s")


def _watched(project: Path) -> list[Path]:
    return [p for p in (project / "flows", project / "env", project / "feather.yaml") if p.exists()]


def dev(
    project: Annotated[
        Path | None, typer.Option("--project", envvar="FEATHER_PROJECT", help="Default: the demo project.")
    ] = None,
    port: Annotated[int, typer.Option(help="The port of the dashboard.")] = 8080,
    no_browser: Annotated[bool, typer.Option("--no-browser", help="Do not open the dashboard.")] = False,
    reset_password: Annotated[
        bool,
        typer.Option(
            "--reset-password", help="Give the dev user a new password, shown once. The data is kept."
        ),
    ] = False,
    env: Env = "dev",
    json_mode: Json = False,
) -> None:
    """Run Postgres, the server and a worker here, on the demo project, and open the dashboard."""
    root = project or _demo_project()
    if root is None:
        fail("the demo project is not here", hint="name a project with --project", json_mode=json_mode)
    if not (root / "feather.yaml").is_file():
        fail(
            f"no feather.yaml in {root.resolve()}",
            hint="name the project directory with --project",
            json_mode=json_mode,
        )
    # Everything that can be wrong is checked before anything is started.
    if _in_use(port):
        fail(
            f"port {port} is already in use",
            hint=f"stop what is using it, or pick another port with --port (for example --port {port + 1})",
            json_mode=json_mode,
        )
    try:
        load_project(root, env)
    except ProjectError as error:
        fail(str(error), hint="run: feather validate", json_mode=json_mode)
    own_database = os.environ.get("FEATHER_DATABASE_URL")
    if not own_database:
        _check_docker(json_mode)

    stop = threading.Event()

    def on_signal(signum: int, frame: FrameType | None) -> None:
        stop.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):  # SIGBREAK is CTRL_BREAK_EVENT, on Windows only
        if (number := getattr(signal, name, None)) is not None:
            signal.signal(number, on_signal)

    container = not own_database
    try:
        _run(root, env, port, own_database, no_browser, reset_password, json_mode, stop)
    except (RuntimeError, psycopg.Error) as error:
        fail(str(error), hint="the log above says more", json_mode=json_mode)
    finally:
        if container:
            _stop_postgres_container()


def _run(
    root: Path,
    env: str,
    port: int,
    own_database: str | None,
    no_browser: bool,
    reset_password: bool,
    json_mode: bool,
    stop: threading.Event,
) -> None:
    database_url = own_database or _start_postgres_container(json_mode)
    migrate(database_url)
    store, _ = connect(database_url)
    password = _ensure_dev_user(store, reset_password)
    settings = Settings(
        env=env,
        database_url=database_url,
        engine_poll_s=0.5,
        secret_key=os.environ.get("FEATHER_SECRET_KEY") or secrets.token_urlsafe(32),
        base_url=f"http://localhost:{port}",
        project=root,
    )
    stack = Stack(settings)
    stack.start(port)
    try:
        _wait_ready(port)
        url = f"http://localhost:{port}"
        document = {
            "url": url,
            "user": DEV_USER,
            "password": password,
            "database": "FEATHER_DATABASE_URL" if own_database else CONTAINER,
            "project": str(root),
        }

        def announce() -> None:
            emit(document, json_mode=json_mode, human=lambda d: _announce(d, own_database is not None))
            if not no_browser:
                webbrowser.open(url)

        _watch(root, env, port, stack, stop, announce)
    finally:
        typer.echo("stopping...", err=True)
        stack.stop()


def _announce(document: dict[str, Any], own_database: bool) -> None:
    typer.echo(f"Feather Connect is running: {document['url']}")
    if document["password"]:
        typer.echo(f"  sign in as {document['user']}, password: {document['password']}")
        typer.echo("  (shown this once; it is stored hashed)")
    else:
        typer.echo(
            f"  the user {document['user']} already exists; its password was shown when it was created. "
            "To get a new one and keep the data, start again with: feather dev --reset-password. "
            f"To start over with no data: docker rm -f {CONTAINER} && docker volume rm {VOLUME}"
        )
    where = "your FEATHER_DATABASE_URL" if own_database else f"Docker container {CONTAINER}"
    typer.echo(f"  project {document['project']}, database in {where}")
    typer.echo("  edit a file under flows/ or env/ to reload; press Ctrl+C to stop")


def _watch(
    root: Path, env: str, port: int, stack: Stack, stop: threading.Event, announce: Callable[[], None]
) -> None:
    """Reload on changes until `stop` is set. `announce` is called once the files are being watched, so that an
    edit made after the announcement is never missed."""
    announced = False
    for changes in watch(
        *_watched(root),
        watch_filter=PythonFilter(extra_extensions=(".yaml", ".yml")),
        stop_event=stop,
        debounce=500,
        rust_timeout=500,
        yield_on_timeout=True,
        raise_interrupt=False,
    ):
        if not announced:
            announce()
            announced = True
        if stop.is_set():
            return
        if not changes:  # `watch` also yields when its timeout passes, so that a stop is noticed
            continue
        try:
            load_project(root, env)  # a file that does not load must not take the running stack down
        except ProjectError as error:
            typer.echo(f"reload failed, the running version stays: {error}", err=True)
            continue
        typer.echo("files changed: reloading...", err=True)
        stack.stop()
        try:
            stack.start(port)
            _wait_ready(port)
        except RuntimeError as error:
            typer.echo(f"reload failed, nothing is running now; fix it and save again: {error}", err=True)
        else:
            typer.echo("reloaded", err=True)
