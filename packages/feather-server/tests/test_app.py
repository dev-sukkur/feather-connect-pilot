import time
import uuid
from collections.abc import Iterator
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
            "FEATHER_ENGINE": "dbos",
            "FEATHER_ENGINE_POLL_S": "2",
            "FEATHER_SECRET_KEY": "k",
            "FEATHER_BASE_URL": "https://feather.example",
        }
    )
    assert (settings.env, settings.database_url, settings.engine, settings.engine_poll_s) == (
        "prod",
        "postgresql://h/db",
        "dbos",
        2.0,
    )
    assert (settings.spool, settings.secret_key, settings.base_url) == (
        "postgres",
        "k",
        "https://feather.example",
    )
    assert settings.legacy_project == "/srv/flows"  # only warned about: the project mode is retired (§13)


def test_the_server_refuses_the_local_engine(  # Stage 2 M10: nothing would run its tasks
) -> None:
    with pytest.raises(SettingsError, match="FEATHER_ENGINE=local"):
        Settings.from_env({"FEATHER_DATABASE_URL": "x", "FEATHER_PROJECT": "p", "FEATHER_ENGINE": "local"})


def test_settings_have_defaults_and_say_what_is_missing_or_wrong() -> None:
    settings = Settings.from_env({"FEATHER_DATABASE_URL": "postgresql://h/db", "FEATHER_PROJECT": "p"})
    assert (settings.env, settings.engine, settings.engine_poll_s) == ("dev", "dbos", 0.5)
    assert len(settings.secret_key) >= 32  # made up for this process when none is set
    with pytest.raises(SettingsError, match="FEATHER_DATABASE_URL"):
        Settings.from_env({"FEATHER_PROJECT": "p"})
    assert (
        Settings.from_env({"FEATHER_DATABASE_URL": "x"}).legacy_project is None
    )  # no project is needed any more
    with pytest.raises(SettingsError, match="FEATHER_ENGINE"):
        Settings.from_env({"FEATHER_DATABASE_URL": "x", "FEATHER_PROJECT": "p", "FEATHER_ENGINE": "celery"})
    with pytest.raises(SettingsError, match="FEATHER_SPOOL"):
        Settings.from_env({"FEATHER_DATABASE_URL": "x", "FEATHER_PROJECT": "p", "FEATHER_SPOOL": "objects"})


def test_the_ingress_queue_and_wait_are_settings() -> None:
    need = {"FEATHER_DATABASE_URL": "x", "FEATHER_PROJECT": "p"}
    settings = Settings.from_env(need)
    assert (settings.ingress_queue_max, settings.ingress_wait_s) == (5000, 5.0)
    settings = Settings.from_env({**need, "FEATHER_INGRESS_QUEUE_MAX": "10", "FEATHER_INGRESS_WAIT_S": "1.5"})
    assert (settings.ingress_queue_max, settings.ingress_wait_s) == (10, 1.5)
    for name, value in [("FEATHER_INGRESS_QUEUE_MAX", "0"), ("FEATHER_INGRESS_WAIT_S", "soon")]:
        with pytest.raises(SettingsError, match=name):
            Settings.from_env({**need, name: value})


@pytest.mark.postgres
@pytest.mark.parametrize("engine", ["local", "dbos"])
def test_create_app_builds_its_state_from_settings(engine: str, pg_url: str, pg: object) -> None:
    settings = Settings(database_url=pg_url, engine=engine, secret_key="test-secret-key")  # type: ignore[arg-type]
    app = create_app(settings, loops=no_loops)
    with TestClient(app) as client:
        assert client.get("/readyz").json() == {"database": "ok", "schema": "current"}
        state: AppState = app.state.feather
        assert state.project.name == "environment" and not state.project.flows  # no flow loads in the server
        assert state.store.runs.list_runs() == []
        assert type(state.engine).__name__ == {"local": "LocalEngine", "dbos": "DbosEngine"}[engine]
        assert state.closers
    assert not state.closers  # the lifespan's end let go of the pool and the engine (F10)
