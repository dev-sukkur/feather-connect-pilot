import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlsplit

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql

from feather_connect.runtime.leader import AlwaysLeader
from feather_connect.storage.postgres.migrate import migrate
from feather_server.app import AppState, create_app
from feather_server.loops import LoopRunner
from feather_server.settings import Settings, SettingsError


def no_loops(state: AppState) -> list[LoopRunner]:
    return []


@pytest.fixture
def client(state: AppState) -> Iterator[TestClient]:
    with TestClient(create_app(state=state, loops=no_loops)) as client:
        yield client


def test_healthz_and_readyz(client: TestClient) -> None:
    assert client.get("/healthz").status_code == 200
    ready = client.get("/readyz")
    assert ready.status_code == 200 and ready.json()["database"] == "ok"


def test_the_state_is_on_the_app(state: AppState) -> None:
    assert create_app(state=state, loops=no_loops).state.feather is state


def test_the_lifespan_starts_and_stops_the_loops(state: AppState) -> None:
    ran: list[str] = []
    runner = LoopRunner("x", lambda: ran.append("x"), 0.01, AlwaysLeader())
    with TestClient(create_app(state=state, loops=lambda s: [runner])):
        deadline = time.monotonic() + 5
        while not ran and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ran
    seen = len(ran)
    time.sleep(0.1)
    assert len(ran) == seen


@pytest.fixture
def fresh_url(pg_url: str) -> Iterator[str]:
    """The URL of an empty database on the test server, with nothing migrated."""
    name = f"feather_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_url, autocommit=True) as admin:
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
        try:
            yield urlsplit(pg_url)._replace(path=f"/{name}").geturl()
        finally:
            admin.execute(sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name)))


def served(state: AppState, database_url: str) -> TestClient:
    state.settings = Settings(database_url=database_url, secret_key="test-secret-key")
    return TestClient(create_app(state=state, loops=no_loops))


def test_readyz_is_503_when_the_database_is_down(state: AppState) -> None:
    with served(state, "postgresql://feather:feather@127.0.0.1:1/none?connect_timeout=1") as client:
        ready = client.get("/readyz")
        assert ready.status_code == 503 and ready.json()["database"] == "down"
        assert client.get("/healthz").status_code == 200  # alive, though not ready


@pytest.mark.postgres
def test_readyz_is_503_until_the_schema_is_at_head(state: AppState, fresh_url: str) -> None:
    with served(state, fresh_url) as client:
        behind = client.get("/readyz")
        assert behind.status_code == 503 and behind.json() == {"database": "ok", "schema": "behind"}
        migrate(fresh_url)
        ready = client.get("/readyz")
        assert ready.status_code == 200 and ready.json() == {"database": "ok", "schema": "current"}


def test_settings_come_from_the_environment() -> None:
    settings = Settings.from_env(
        {
            "FEATHER_ENV": "prod",
            "FEATHER_DATABASE_URL": "postgresql://h/db",
            "FEATHER_PROJECT": "/srv/flows",
            "FEATHER_ENGINE": "local",
            "FEATHER_ENGINE_POLL_S": "2",
            "FEATHER_SECRET_KEY": "k",
            "FEATHER_BASE_URL": "https://feather.example",
        }
    )
    assert (settings.env, settings.database_url, settings.engine, settings.engine_poll_s) == (
        "prod",
        "postgresql://h/db",
        "local",
        2.0,
    )
    assert (settings.spool, settings.secret_key, settings.base_url) == (
        "postgres",
        "k",
        "https://feather.example",
    )
    assert str(settings.project).replace("\\", "/") == "/srv/flows"


def test_settings_have_defaults_and_say_what_is_missing_or_wrong() -> None:
    settings = Settings.from_env({"FEATHER_DATABASE_URL": "postgresql://h/db", "FEATHER_PROJECT": "p"})
    assert (settings.env, settings.engine, settings.engine_poll_s) == ("dev", "dbos", 0.5)
    assert len(settings.secret_key) >= 32  # made up for this process when none is set
    with pytest.raises(SettingsError, match="FEATHER_DATABASE_URL"):
        Settings.from_env({"FEATHER_PROJECT": "p"})
    with pytest.raises(SettingsError, match="FEATHER_PROJECT"):
        Settings.from_env({"FEATHER_DATABASE_URL": "x"})
    with pytest.raises(SettingsError, match="FEATHER_ENGINE"):
        Settings.from_env({"FEATHER_DATABASE_URL": "x", "FEATHER_PROJECT": "p", "FEATHER_ENGINE": "celery"})
    with pytest.raises(SettingsError, match="FEATHER_SPOOL"):
        Settings.from_env({"FEATHER_DATABASE_URL": "x", "FEATHER_PROJECT": "p", "FEATHER_SPOOL": "objects"})


@pytest.mark.postgres
@pytest.mark.parametrize("engine", ["local", "dbos"])
def test_create_app_builds_its_state_from_settings(
    engine: str, pg_url: str, pg: object, tmp_path: Path
) -> None:
    (tmp_path / "feather.yaml").write_text("name: demo\n")
    (tmp_path / "env").mkdir()
    (tmp_path / "env" / "dev.yaml").write_text("")
    settings = Settings(database_url=pg_url, project=tmp_path, engine=engine, secret_key="test-secret-key")  # type: ignore[arg-type]
    app = create_app(settings, loops=no_loops)
    with TestClient(app) as client:
        assert client.get("/readyz").json() == {"database": "ok", "schema": "current"}
        state: AppState = app.state.feather
        assert state.project.name == "demo" and state.store.runs.list_runs() == []
        assert type(state.engine).__name__ == {"local": "LocalEngine", "dbos": "DbosEngine"}[engine]
        assert state.closers
    assert not state.closers  # the lifespan's end let go of the pool and the engine (F10)
