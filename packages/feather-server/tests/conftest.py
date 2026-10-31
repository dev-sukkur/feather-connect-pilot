from datetime import UTC, datetime
from pathlib import Path

import pytest

from feather_connect.clock import FrozenClock
from feather_connect.engine.local import LocalEngine
from feather_connect.project import Project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool
from feather_server.app import AppState
from feather_server.settings import Settings


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(datetime(2026, 10, 18, 10, tzinfo=UTC))


@pytest.fixture
def state(clock: FrozenClock) -> AppState:
    """An app state on the in-memory store and the local engine, with no flows."""
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    runtime = Runtime([], store, spool, engine, code_sha="sha", clock=clock)
    project = Project("demo", Path("."), "dev", (), {}, {}, {"timezone": "UTC"}, "sha")
    return AppState(Settings(), project, store, spool, engine, runtime, clock=clock)
