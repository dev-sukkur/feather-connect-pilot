"""The server application: health endpoints now, and the loops that run for as long as it does."""

import logging
import os
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import psycopg
from fastapi import FastAPI, Response

from feather_connect.clock import Clock, SystemClock
from feather_connect.engine.local import LocalEngine
from feather_connect.engine.port import Engine
from feather_connect.logs import TaskLogHandler, configure_logging
from feather_connect.project import Project, load_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import at_head, migrate
from feather_connect.storage.spool import Spool
from feather_connect.worker import config_of
from feather_server import auth, ingress, web
from feather_server.loops import LoopRunner, default_loops
from feather_server.services.actions import Actions
from feather_server.services.reads import Reads
from feather_server.settings import Settings

log = logging.getLogger(__name__)


@dataclass
class AppState:
    """What the server's routes and loops share. It is on the app as `app.state.feather`."""

    settings: Settings
    project: Project
    store: Store
    spool: Spool
    engine: Engine
    runtime: Runtime
    clock: Clock = field(default_factory=SystemClock)
    closers: list[Callable[[], None]] = field(default_factory=list[Callable[[], None]])

    def close(self) -> None:
        """Let go of what `build_state` opened. A state made by hand has nothing here."""
        while self.closers:
            try:
                self.closers.pop()()
            except Exception:
                log.exception("could not close part of the server's state")


def build_state(settings: Settings) -> AppState:
    """Open the stores and the engine that `settings` names, and load the project. Migrates the database.

    The server submits work and workers run it, so with the DBOS engine nothing is registered here. The local engine
    runs in this process: its tasks are registered, and something has to ask it to run them.
    """
    project = load_project(settings.project, settings.env)
    migrate(settings.database_url)
    clock = SystemClock()
    store, spool = connect(settings.database_url, clock=clock)
    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"), secrets=[settings.secret_key])
    task_logs = TaskLogHandler(store)
    logging.getLogger().addHandler(task_logs)
    closers: list[Callable[[], None]] = [store.close, lambda: logging.getLogger().removeHandler(task_logs)]
    for budget in project.budgets.values():  # the overview reads their use
        store.budgets.define(budget)
    local = LocalEngine() if settings.engine == "local" else None
    if local is None:
        from feather_engine_dbos import DbosEngine

        dbos_engine = DbosEngine(settings.database_url)
        closers.append(dbos_engine.close)
        engine: Engine = dbos_engine
    else:
        engine = local

    def config(connection: str) -> Mapping[str, Any]:  # webhook sources verify and parse here, in the server
        return config_of(project, connection) if connection in project.connections else {}

    runtime = Runtime(
        project.flows,
        store,
        spool,
        engine,
        code_sha=project.code_sha,
        clock=clock,
        config_of=config,
        timezone=str(project.settings["timezone"]),
    )
    if local is not None:
        local.register(runtime.tasks())
    return AppState(settings, project, store, spool, engine, runtime, clock, closers)


def _readiness(state: AppState) -> dict[str, str]:
    """Report whether the database answers and its schema is at head. A state with no database URL has neither to
    check, and counts as ready."""
    url = state.settings.database_url
    if not url:
        return {"database": "ok"}
    try:
        current = at_head(url)
    except psycopg.Error:
        return {"database": "down"}
    return {"database": "ok", "schema": "current" if current else "behind"}


def create_app(
    settings: Settings | None = None,
    *,
    state: AppState | None = None,
    loops: Callable[[AppState], list[LoopRunner]] = default_loops,
) -> FastAPI:
    """Build the application. Pass a prepared `state` to run on other stores, as tests do, and `loops` to choose
    the loops the lifespan starts."""
    if state is None:
        state = build_state(settings or Settings.from_env())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        running = loops(state)
        for runner in running:
            runner.start()
        try:
            yield
        finally:
            for runner in running:
                runner.stop()
            state.close()

    app = FastAPI(title="Feather Connect", lifespan=lifespan)
    app.state.feather = state
    if not state.settings.secret_key:
        raise ValueError(
            "the server needs a secret key to sign sessions and confirmations: set FEATHER_SECRET_KEY"
        )
    app.state.reads = Reads(state)
    app.state.actions = Actions(state)
    auth.install(app, state)
    web.install(app, state)
    ingress.install(app, state)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz(response: Response) -> dict[str, Any]:
        report = _readiness(state)
        if report["database"] != "ok" or report.get("schema", "current") != "current":
            response.status_code = 503
        return report

    return app
