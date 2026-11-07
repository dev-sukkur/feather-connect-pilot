"""deploy/compose.yaml and the images: they start healthy, stay within four containers with one stateful volume, keep
their data over a restart, scale workers, and hold no secret. They need Docker and the images; they build them.

    uv run pytest tests/deploy -q

They run nightly and on changes to deploy/. The stack is its own Compose project on port 29000.
"""

import json
import os
import secrets
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "compose.yaml"
PROJECT = "feather-compose-test"
PORT = int(os.environ.get("FEATHER_COMPOSE_TEST_PORT", "29000"))
ORG = os.environ.get("FEATHER_IMAGE_ORG", "feather-connect")
IMAGES = [f"ghcr.io/{ORG}/feather-server:dev", f"ghcr.io/{ORG}/feather-worker:dev"]

pytestmark = [
    pytest.mark.docker,
    pytest.mark.timeout(900),
    pytest.mark.skipif(shutil.which("docker") is None, reason="needs Docker"),
]


def _docker_up() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, timeout=20, check=False).returncode == 0
        )
    except (OSError, subprocess.TimeoutExpired):
        return False


@dataclass
class Stack:
    env: dict[str, str]
    secret_key: str
    password: str

    def compose(self, *args: str, check: bool = True, timeout: int = 600) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), "-p", PROJECT, *args],
            env=self.env,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
        )

    def containers(self) -> list[dict[str, str]]:
        out = self.compose("ps", "--all", "--format", "json").stdout
        lines = [json.loads(line) for line in out.splitlines() if line.strip().startswith("{")]
        return lines

    def get(self, path: str) -> tuple[int, str]:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}{path}", timeout=5) as answer:
                return answer.status, answer.read().decode()
        except urllib.error.HTTPError as error:
            return error.code, ""
        except OSError:
            return 0, ""

    def wait_ready(self, seconds: float = 180) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.get("/readyz")[0] == 200:
                return
            time.sleep(2)
        logs = self.compose("logs", "--no-color", "--tail", "60", check=False).stdout
        raise AssertionError(f"the server was not ready within {seconds}s:\n{logs[-4000:]}")

    def sql(self, statement: str) -> str:
        return self.compose(
            "exec", "-T", "postgres", "psql", "-U", "feather", "-d", "feather", "-tAc", statement
        ).stdout.strip()


@pytest.fixture(scope="module")
def compose() -> Iterator[Stack]:
    if not _docker_up():
        pytest.skip("Docker is not running")
    env = {
        **os.environ,
        "FEATHER_SECRET_KEY": secrets.token_urlsafe(48),
        "POSTGRES_PASSWORD": secrets.token_urlsafe(16),
        "FEATHER_PORT": str(PORT),
        "FEATHER_BASE_URL": f"http://127.0.0.1:{PORT}",
        "FEATHER_ENV": "qa",  # dev delivers to targets on the host; qa is all simulation
    }
    stack = Stack(env, env["FEATHER_SECRET_KEY"], env["POSTGRES_PASSWORD"])
    stack.compose("down", "--volumes", "--remove-orphans", check=False)
    stack.compose("build")
    try:  # a failed start must not leave the volume, with this run's password, to the next run
        stack.compose("up", "-d", "--wait", "postgres", check=False)
        started = stack.compose("up", "-d", check=False)
        assert started.returncode == 0, started.stdout + started.stderr + stack.compose("logs", check=False).stdout
        stack.wait_ready()
        yield stack
    finally:
        stack.compose("down", "--volumes", "--remove-orphans", check=False)


def test_compose_refuses_to_start_without_the_secret_key() -> None:
    env = {k: v for k, v in os.environ.items() if k not in ("FEATHER_SECRET_KEY", "POSTGRES_PASSWORD")}
    env["POSTGRES_PASSWORD"] = "x"
    done = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "-p", PROJECT, "config", "-q"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode != 0 and "FEATHER_SECRET_KEY" in done.stderr


def test_compose_starts_healthy_and_serves_the_dashboard(compose: Stack) -> None:
    assert compose.get("/healthz")[0] == 200
    status, page = compose.get("/login")
    assert status == 200 and "<form" in page
    by_service = {c["Service"]: c for c in compose.containers()}
    assert by_service["migrate"]["State"] == "exited" and "(0)" in by_service["migrate"]["Status"]
    assert by_service["feather-worker"]["State"] == "running"


def test_compose_has_at_most_four_long_running_containers(compose: Stack) -> None:  # NFR-06
    containers = compose.containers()
    assert len(containers) <= 4
    running = [c for c in containers if c["State"] == "running"]
    assert sorted(c["Service"] for c in running) == ["feather-server", "feather-worker", "postgres"]


def test_only_postgres_is_stateful(compose: Stack) -> None:  # NFR-07: one named volume
    volumes = compose.compose("volumes", "--format", "json").stdout
    names = [json.loads(line)["Name"] for line in volumes.splitlines() if line.strip().startswith("{")]
    assert names == [f"{PROJECT}_pgdata"]


def test_no_secret_in_the_images(compose: Stack) -> None:
    for image in IMAGES:
        history = subprocess.run(
            ["docker", "history", "--no-trunc", "--format", "{{.CreatedBy}}", image],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        inspect = subprocess.run(
            ["docker", "image", "inspect", image], capture_output=True, text=True, check=True
        ).stdout
        for text in (history, inspect):
            assert compose.secret_key not in text and compose.password not in text
            assert "FEATHER_SECRET_KEY=" not in text
        user = json.loads(inspect)[0]["Config"]["User"]
        assert user == "feather"


def test_restart_keeps_data(compose: Stack) -> None:
    compose.sql("create table if not exists restart_marker (n int); insert into restart_marker values (7)")
    compose.compose("restart")
    compose.wait_ready()
    assert compose.sql("select n from restart_marker") == "7"
    assert compose.sql("select count(*) from feather.worker") != "0"


def test_two_workers_scale_with_replicas(compose: Stack) -> None:
    compose.compose("up", "-d", "--no-deps", "--scale", "feather-worker=2", "feather-worker")
    deadline = time.monotonic() + 120
    alive = "0"
    while time.monotonic() < deadline:
        alive = compose.sql("select count(*) from feather.worker where state = 'alive'")
        if int(alive or 0) >= 2:
            break
        time.sleep(2)
    workers = compose.sql("select worker_id, state, last_seen from feather.worker")
    logs = compose.compose("logs", "--no-color", "--tail", "30", "feather-worker", check=False).stdout
    states = [c["Name"] + " " + c["State"] for c in compose.containers()]
    assert int(alive) >= 2, f"{workers}\n{states}\n{logs[-3000:]}"
    compose.compose("up", "-d", "--no-deps", "--scale", "feather-worker=1", "feather-worker")
