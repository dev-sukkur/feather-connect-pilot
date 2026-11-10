"""What the tests that run the real stack share: scratch databases, and worker processes running Feather's `Runtime`.

The fixtures built on these are in the root `conftest.py`. This folder is on the path through `pythonpath` in
`pyproject.toml`, as test files can't import each other under `--import-mode=importlib`.
"""

import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any
from urllib.parse import urlsplit

import psycopg
from psycopg import sql

DEFAULT_URL = "postgresql://feather:feather@127.0.0.1:25433/feather_test"


def server_url() -> str:
    """Return the URL of the test server's admin database."""
    return os.environ.get("FEATHER_TEST_DATABASE_URL", DEFAULT_URL)


@contextmanager
def scratch_database(admin_url: str) -> Iterator[str]:
    """Create an empty database on the server of `admin_url`, give its URL, and drop it at the end."""
    name = f"feather_{uuid.uuid4().hex[:12]}"
    create = sql.SQL("create database {}").format(sql.Identifier(name))
    drop = sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name))
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=5) as admin:
        admin.execute(create)
    try:
        yield urlsplit(admin_url)._replace(path=f"/{name}").geturl()
    finally:  # on a session of its own, as a test may have restarted the server
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=5) as admin:
            admin.execute(drop)


def wait_until(check: Callable[[], Any], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except KeyError:
            pass
        time.sleep(0.2)
    raise AssertionError("timed out waiting")


_INSTALLED: dict[tuple[str, Path], str] = {}


def install_project(
    project: Path, pg_url: str, env: str = "dev", environ: Mapping[str, str] | None = None
) -> str:
    """Install `project` as a package on `pg_url`'s database and make its flows active, as `feather dev` does; once per
    project folder and database, so a worker started again runs the same version, and a test of its own on a shared
    database gets its own. Returns the package's name: flows are `<package>/<name>`."""
    from feather_cli.upgrade import install_dev_version
    from feather_connect.storage.postgres.connect import connect

    key = (pg_url, project.resolve())
    if key not in _INSTALLED:
        store, _ = connect(pg_url)
        saved = dict(os.environ)
        os.environ.update(environ or {})  # what the project reads as it loads, for its load check
        try:
            _INSTALLED[key] = install_dev_version(project, env=env, database_url=pg_url, store=store).package
        finally:
            os.environ.clear()
            os.environ.update(saved)
            store.close()
    return _INSTALLED[key]


def start_worker(
    project: Path,
    pg_url: str,
    worker_id: str,
    sha: str = "testsha",
    *,
    env: Mapping[str, str] | None = None,
    log: IO[bytes] | None = None,
    poll_s: float = 0.1,
) -> subprocess.Popen[bytes]:
    """Install `project` as a package (`install_project`) and start a supervisor for it,
    `python -m feather_connect.supervisor`, under `worker_id`. `env` adds to the environment, and `log` takes its
    output. The project mode is retired (flow packages §13): a worker runs installed packages."""
    install_project(project, pg_url, environ=env)
    extra = [name for name in (env or {}) if name.startswith("FEATHER_")]
    passed = ",".join(n for n in [os.environ.get("FEATHER_HOST_ENV_PASS", ""), *extra] if n)
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0  # type: ignore[attr-defined]
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "feather_connect.supervisor",
            "dev",
            "--database-url",
            pg_url,
            "--worker-id",
            worker_id,
            "--poll-s",
            str(poll_s),
            "--workdir",
            str(project),  # as `feather dev` runs it: the project's relative paths mean its folder
        ],
        env={**os.environ, "FEATHER_CODE_SHA": sha, **(env or {}), "FEATHER_HOST_ENV_PASS": passed},
        creationflags=flags,
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def kill_abruptly(proc: subprocess.Popen[bytes]) -> None:
    """Kill the worker at once, as `kill -9` does, and wait for it.

    On Windows `sys.executable` in a virtual environment is a launcher, whose child is the real Python: `proc.pid` is
    the launcher's, so it takes the tree.
    """
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], check=False, capture_output=True)
    else:
        proc.kill()
    proc.wait(timeout=30)


def stop_gracefully(proc: subprocess.Popen[bytes]) -> None:
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
    else:
        proc.terminate()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(
    project: Path,
    pg_url: str,
    port: int,
    *,
    env: Mapping[str, str] | None = None,
    log: IO[bytes] | None = None,
) -> subprocess.Popen[bytes]:
    """Start `feather server` on `port`. `env` adds to the environment, and `log` takes its output."""
    return subprocess.Popen(
        [sys.executable, "-m", "feather_cli.main", "server", "--port", str(port)],
        env={
            **os.environ,
            "FEATHER_DATABASE_URL": pg_url,
            "FEATHER_PROJECT": str(project),
            "FEATHER_ENV": "dev",
            "FEATHER_SECRET_KEY": "a-test-secret-key",
            **(env or {}),
        },
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def server_is_ready(proc: subprocess.Popen[bytes], url: str, log_file: Path) -> bool:
    """Whether the server answers `/readyz`. One that has died fails the check, with what it logged."""
    assert proc.poll() is None, log_file.read_text(errors="replace")
    try:
        with urllib.request.urlopen(f"{url}/readyz", timeout=2) as response:
            return response.status == 200
    except OSError:
        return False
