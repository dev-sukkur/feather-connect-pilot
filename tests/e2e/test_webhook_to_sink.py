"""The promise of Stage 3, with nothing mocked: a signed webhook goes through a real server and a real worker on the test
Postgres, and reaches a sink. A repeat of the event's id reaches an append route once."""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest
from real_stack import kill_abruptly, scratch_database, server_url, start_worker, wait_until

from feather_connect.connectors.simulation import SIGNATURE_HEADER, TIMESTAMP_HEADER
from feather_connect.core.webhooks import sign

pytestmark = pytest.mark.postgres

SECRET = "e2e-signature-secret"
GENEROUS = 90  # a server and a worker to start: no statement about latency, which group L measures
ENV = {"HOOK_SECRET": SECRET, "FEATHER_CODE_SHA": "testsha"}


@pytest.fixture
def pg_url() -> Iterator[str]:
    """A database of its own, since a project's routes stay in the catalogue, where the other e2e tests would see them."""
    with scratch_database(server_url()) as url:
        yield url


@pytest.fixture
def push_project(tmp_path: Path) -> Path:
    (tmp_path / "flows").mkdir()
    (tmp_path / "env").mkdir()
    (tmp_path / "feather.yaml").write_text(
        dedent(
            """
            name: push-project
            modules: [flows.hooks]
            connections:
              e2e_hooks:
                connector: simulation.source
                auth: {signature_secret: "${secret:env:HOOK_SECRET}"}
              e2e_upsert: {connector: simulation.sink}
              e2e_append: {connector: simulation.sink}
            """
        )
    )
    (tmp_path / "env" / "dev.yaml").write_text("")
    (tmp_path / "flows" / "hooks.py").write_text(
        dedent(
            """
            from pathlib import Path

            import feather_connect as fc
            from feather_connect.connectors import simulation

            root = Path(__file__).parent.parent
            hooks = fc.Dataset(
                "e2e_hooks",
                source=simulation.Source("e2e_hooks", generator={"id": simulation.seq()}, rows_per_partition=0),
                key="id",
                strategy=fc.strategy.push(),
            )
            flow = fc.Flow(
                "e2e_push",
                reads=hooks,
                deliver=[
                    fc.to(simulation.Sink("e2e_upsert", capture_to=root / "out" / "upsert.jsonl")),
                    fc.to(simulation.Sink("e2e_append", capture_to=root / "out" / "append.jsonl"), mode="append"),
                ],
                triggers=[fc.on_event(hooks)],
            )
            """
        )
    )
    return tmp_path


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(push_project: Path, pg_url: str, db: Any) -> Iterator[str]:
    """Start `feather server` as a process, give its address, and stop it at the end. It runs no task, so killing it
    can't leave one hanging."""
    port = free_port()
    with (push_project / "server.log").open("wb") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "feather_cli.main", "server", "--port", str(port)],
            env={
                **os.environ,
                **ENV,
                "FEATHER_DATABASE_URL": pg_url,
                "FEATHER_PROJECT": str(push_project),
                "FEATHER_ENV": "dev",
                "FEATHER_SECRET_KEY": "a-test-secret-key",
            },
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    url = f"http://127.0.0.1:{port}"

    def ready() -> bool:
        assert proc.poll() is None, (push_project / "server.log").read_text(errors="replace")
        try:
            with urllib.request.urlopen(f"{url}/readyz", timeout=2) as response:
                return response.status == 200
        except OSError:
            return False

    try:
        wait_until(ready, GENEROUS)
        yield url
    finally:
        kill_abruptly(proc)


def post(url: str, payload: dict[str, Any], *, secret: str = SECRET) -> int:
    body = json.dumps(payload).encode()
    stamp = str(int(time.time()))
    request = urllib.request.Request(
        f"{url}/ingress/default/e2e_hooks",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(secret, body, stamp),
            TIMESTAMP_HEADER: stamp,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def captured(project: Path, name: str) -> list[dict[str, Any]]:
    out = project / "out" / f"{name}.jsonl"
    return [json.loads(line) for line in out.read_text().splitlines()] if out.exists() else []


def test_a_signed_webhook_reaches_the_sinks_once(push_project, server, db, pg_url, workers):
    store, _ = db
    workers.append(start_worker(push_project, pg_url, "w-push", env=ENV))

    assert post(server, {"event_id": "refused", "id": 99}, secret="not-the-secret") == 401
    assert store.inbox.oldest_age("e2e_hooks", datetime.now(UTC)) is None

    first = {"event_id": "e2e-1", "id": 1, "v": "first"}
    assert post(server, first) == 202
    assert post(server, first) == 202  # the sender heard nothing, and tries again

    wait_until(lambda: captured(push_project, "upsert"), GENEROUS)
    wait_until(lambda: captured(push_project, "append"), GENEROUS)
    assert captured(push_project, "upsert")[0]["payload"]["v"] == "first"

    # The repeat is seen to have gone nowhere by what comes after it, as the routes apply in order.
    assert post(server, first) == 202
    assert post(server, {"event_id": "e2e-2", "id": 1, "v": "second"}) == 202
    wait_until(lambda: len(captured(push_project, "append")) >= 2, GENEROUS)
    wait_until(lambda: len(captured(push_project, "upsert")) >= 2, GENEROUS)
    assert [line["payload"]["event_id"] for line in captured(push_project, "append")] == ["e2e-1", "e2e-2"]
    assert captured(push_project, "upsert")[-1]["payload"]["v"] == "second"

    versions = store.versions.list_versions("e2e_hooks")
    assert versions and all(not v.complete for v in versions)
    run_ids = {v.run_id for v in versions}
    assert len(run_ids) == 1  # no run per event
    assert store.runs.get_run(run_ids.pop()).trigger == "stream"
