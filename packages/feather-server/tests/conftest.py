import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from feather_connect.clock import FrozenClock
from feather_connect.engine.local import LocalEngine
from feather_connect.project import Project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool
from feather_server.app import AppState, create_app
from feather_server.auth import Actor
from feather_server.settings import Settings
from feather_server.testing import SECRET, World, build_world, sign_in


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(datetime(2026, 10, 18, 10, tzinfo=UTC))


@pytest.fixture
def state(clock: FrozenClock) -> AppState:
    """An app state on the in-memory store and the local engine, with no flows."""
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    runtime = Runtime([], store, spool, engine, code_sha="sha", clock=clock)
    project = Project("demo", Path("."), "dev", (), {}, {}, {"timezone": "UTC"}, "sha")
    return AppState(
        Settings(secret_key="test-secret-key"), project, store, spool, engine, runtime, clock=clock
    )


@pytest.fixture
def world(tmp_path: Path, clock: FrozenClock, monkeypatch: pytest.MonkeyPatch) -> World:
    monkeypatch.setenv("WH_TOKEN", SECRET)
    return build_world(tmp_path, clock)


@pytest.fixture
def ann() -> Actor:
    return Actor("human", "ann")


@pytest.fixture
def bob() -> Actor:
    return Actor("token", "bob-ci")


@pytest.fixture
def store(world: World) -> MemoryStore:
    return world.store


@pytest.fixture
def client(world: World) -> TestClient:
    """A browser signed in as `op` to the dashboard of the world."""
    return sign_in(create_app(state=world.state, loops=lambda s: []))


@pytest.fixture
def csrf(client: TestClient) -> Callable[[], str]:
    """The CSRF token of the signed-in session, read from a page as a browser would."""

    def token() -> str:
        found = re.search(r'<meta name="csrf-token" content="([^"]+)"', client.get("/").text)
        assert found
        return found.group(1)

    return token
