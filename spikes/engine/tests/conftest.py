from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable

import pytest

from spike import journal

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "results" / "logs"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--engine", default="dbos", choices=["dbos"])


@pytest.fixture(scope="session")
def engine_name(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--engine")


@pytest.fixture(scope="session", autouse=True)
def _schema() -> None:
    journal.init()


@pytest.fixture
def run(request: pytest.FixtureRequest, engine_name: str) -> str:
    return f"{engine_name}-{request.node.name}-{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="session")
def client():
    from spike.dbos_engine import DbosClient

    c = DbosClient()
    yield c
    c.close()


IMAGE = "feather-spike-worker"
NETWORK = "feather-spike_default"


class Workers:
    """Starts and kills workers: host processes, or Linux containers with SPIKE_WORKERS=docker.

    kill() is as abrupt as kill -9: TerminateProcess on the process tree, or docker kill (SIGKILL).
    """

    def __init__(self, engine: str, run: str) -> None:
        self.engine, self.run = engine, run
        self.procs: dict[str, subprocess.Popen] = {}
        self.containers: dict[str, str] = {}
        self.docker = os.environ.get("SPIKE_WORKERS") == "docker"
        LOGS.mkdir(parents=True, exist_ok=True)

    def _docker_run(self, name: str, args: list[str], env: dict | None) -> None:
        cname = f"fs-{uuid.uuid4().hex[:8]}-{name}"
        envs = {"SPIKE_DB_URL": "postgresql://feather:feather_spike@postgres:5432/feather_spike",
                **(env or {})}
        cmd = ["docker", "run", "-d", "--name", cname, "--network", NETWORK,
               "-v", f"{(ROOT / 'spike').as_posix()}:/app/spike:ro"]
        for k, v in envs.items():
            cmd += ["-e", f"{k}={v}"]
        cmd += [IMAGE, "python", "-m", "spike.worker", *args]
        subprocess.run(cmd, check=True, capture_output=True)
        self.containers[name] = cname

    def _docker_stop(self, name: str) -> None:
        cname = self.containers.pop(name)
        subprocess.run(["docker", "kill", cname], capture_output=True)
        with open(LOGS / f"{self.run}-{name}.log", "a", encoding="utf-8") as log:
            subprocess.run(["docker", "logs", cname], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(["docker", "rm", "-f", cname], capture_output=True)

    def start(self, name: str, *, labels: dict | None = None, slots: int = 4,
              version: str = "v1", auto_upgrade: bool = False, wait: bool = True,
              env: dict | None = None) -> float:
        """Start a worker; returns seconds until it reported ready (0 if not waited)."""
        # Unique per test: DBOS only dequeues unpinned work on the latest registered version.
        version = f"{version}-{self.run}"
        args = [sys.executable, "-m", "spike.worker", "--engine", self.engine, "--name", name,
                "--labels", json.dumps(labels or {}), "--slots", str(slots), "--version", version]
        if auto_upgrade:
            args.append("--auto-upgrade")
        journal.clear_ready(name)
        started = journal.scalar("select clock_timestamp()")
        t0 = time.monotonic()
        if self.docker:
            self._docker_run(name, args[3:], env)
        else:
            log = open(LOGS / f"{self.run}-{name}.log", "a", encoding="utf-8")
            self.procs[name] = subprocess.Popen(
                args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONUNBUFFERED": "1", **(env or {})})
        if not wait:
            return 0.0
        wait_until(lambda: journal.scalar(
            "select 1 from spike.worker_ready where worker = %s and at >= %s", (name, started)),
            timeout=90, what=f"worker {name} ready")
        return time.monotonic() - t0

    def kill(self, name: str) -> None:
        if name in self.containers:
            self._docker_stop(name)
            return
        # The venv python.exe on Windows is a launcher with the real interpreter as a child,
        # so terminate the whole tree (as abrupt as kill -9).
        p = self.procs.pop(name)
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True)
        p.wait(timeout=30)

    def stop_all(self) -> None:
        for name in list(self.procs) + list(self.containers):
            self.kill(name)


@pytest.fixture
def workers(engine_name: str, run: str):
    if journal.scalar("select to_regclass('dbos.workflow_status')"):
        # Drop work left queued by earlier tests so it can't flood this test's worker.
        journal.execute("delete from dbos.workflow_status where status in ('ENQUEUED', 'PENDING', 'DELAYED')")
    w = Workers(engine_name, run)
    yield w
    w.stop_all()


def wait_until(fn: Callable[[], Any], timeout: float, what: str = "condition",
               interval: float = 0.25) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what}")


def count(run: str, event: str, task_id: str | None = None) -> int:
    if task_id:
        return journal.scalar(
            "select count(*) from spike.exec_log where run = %s and event = %s and task_id = %s",
            (run, event, task_id))
    return journal.scalar(
        "select count(*) from spike.exec_log where run = %s and event = %s", (run, event))


def seconds(a: Any, b: Any) -> float:
    return (b - a).total_seconds()


def events(run: str, event: str | None = None) -> list[dict]:
    sql = "select * from spike.exec_log where run = %s"
    params: tuple = (run,)
    if event:
        sql += " and event = %s"
        params += (event,)
    return journal.query(sql + " order by at, id", params)


RESULTS = ROOT / "results"


def record(engine: str, test: str, **facts: Any) -> None:
    """Append measured facts for the findings report."""
    RESULTS.mkdir(exist_ok=True)
    mode = os.environ.get("SPIKE_WORKERS", "host")
    deploy = os.environ.get("SPIKE_DEPLOY", "library")
    with open(RESULTS / f"{engine}.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"test": test, "workers": mode, "deploy": deploy, "at": time.time(),
                            **facts}, default=str) + "\n")
