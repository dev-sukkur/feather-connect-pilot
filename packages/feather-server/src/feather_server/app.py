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
from feather_connect.logs import configure_logging, install_task_logs
from feather_connect.project import (
    Project,
    connection_windows,
    environment_layer,
    environment_project,
    use_db_secrets,
)
from feather_connect.runtime.catalogue import Catalogue
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import at_head, migrate
from feather_connect.storage.spool import Spool
from feather_connect.vault import Vault, keyring_from_env
from feather_connect.worker import config_of
from feather_server import agent, api, auth, ingress, registry, renames, web
from feather_server.loops import LoopRunner, default_loops
from feather_server.package_push import PackagePushes
from feather_server.services.accounts import Accounts
from feather_server.services.actions import Actions
from feather_server.services.environment import Environment
from feather_server.services.packages import Packages
from feather_server.services.reads import Reads
from feather_server.services.registries import Registries
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
    vault: Vault | None = None  # the process's one vault of its environment; made by `create_app` if None
    catalogue: Catalogue | None = None  # installed packages' active flows and webhooks; made by `create_app`
    pushes: PackagePushes | None = None  # their push datasets, for ingress; made by `create_app`

    def close(self) -> None:
        """Let go of what `build_state` opened. A state made by hand has nothing here."""
        while self.closers:
            try:
                self.closers.pop()()
            except Exception:
                log.exception("could not close part of the server's state")


def build_state(settings: Settings) -> AppState:
    """Open the stores and the engine that `settings` names, on the environment's own configuration. Migrates the
    database. No flow loads here: every flow is an installed package's (flow packages §13), which the server knows by
    its spec from the catalogue (`Catalogue`), workers load-check (`feather_connect.packages.prepare`) and package hosts
    run (`feather_connect.host`).

    The server submits work and workers run it, so with the DBOS engine nothing is registered here. The local engine
    runs in this process: its tasks are registered, and something has to ask it to run them.
    """
    keyring = keyring_from_env()  # None is fine: then db secrets fail, saying how to set one
    migrate(settings.database_url)
    clock = SystemClock()
    store, spool = connect(settings.database_url, clock=clock)
    vault = Vault(store.secrets, settings.env, keyring)
    use_db_secrets(vault.open)
    environment = environment_layer(
        store.env_config.entries(settings.env)
    )  # a bad entry is left out, with a warning
    project = environment_project(settings.env, environment)
    if settings.legacy_project:
        log.warning(
            "FEATHER_PROJECT is set (%s), but the project mode is retired: the server runs installed packages only. "
            "Make the project its first package with: feather server upgrade --project %s (Operating Guide, "
            "'Upgrading from the project mode'), then unset FEATHER_PROJECT",
            settings.legacy_project,
            settings.legacy_project,
        )
    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"), secrets=[settings.secret_key])
    closers: list[Callable[[], None]] = [store.close, install_task_logs(store)]
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

    # What the server submits (an operator's retry, replay or skip of a delivery) goes to the pool of the version its
    # run is tagged with, as a host's own submits do, so that host runs it: no other process has the code (§6).
    from feather_connect.host import PoolEngine

    runtime = Runtime(
        project.flows,
        store,
        spool,
        engine if local is not None else PoolEngine(engine, "server", store),
        code_sha=project.code_sha,
        clock=clock,
        config_of=config,
        timezone=str(project.settings["timezone"]),
        windows=connection_windows(project),
    )
    if local is not None:
        local.register(runtime.tasks())
    return AppState(settings, project, store, spool, engine, runtime, clock, closers, vault)


def readiness(state: AppState) -> dict[str, str]:
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
            async with mcp_running():
                yield
        finally:
            for runner in running:
                runner.stop()
            state.close()

    app = FastAPI(title="Feather Connect", version="1", lifespan=lifespan)
    app.state.feather = state
    if not state.settings.secret_key:
        raise ValueError(
            "the server needs a secret key to sign sessions and confirmations: set FEATHER_SECRET_KEY"
        )
    app.state.reads = Reads(state)
    app.state.actions = Actions(state)
    app.state.accounts = Accounts(state.store, state.clock)
    app.state.registries = Registries(
        state.store,
        state.clock,
        enabled=state.settings.registry,
        index_url=state.settings.base_url.rstrip("/") + "/registry/simple/",
    )
    app.state.packages = Packages(state.store, state.clock, app.state.registries, env=state.settings.env)
    if state.vault is None:
        state.vault = Vault(state.store.secrets, state.settings.env, keyring_from_env())
    app.state.environment = Environment(state.store, state.clock, state.settings.env, state.vault)
    if state.catalogue is None:
        state.catalogue = Catalogue(state.store, env=state.settings.env)  # refreshed on NOTIFY, and every REFRESH_S seconds
        state.catalogue.listen()
        state.closers.append(state.catalogue.close)
    if state.pushes is None:
        state.pushes = PackagePushes(state.store, state.catalogue, state.settings.env)
    renames.install(app, state)
    ingress.install(app, state)  # first, as routes are tried in order, and this one takes bursts
    auth.install(app, state)
    api.install(app)
    registry.install(app)
    mcp_running = agent.install(app)  # before the dashboard's catch-all routes
    web.install(app, state)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz(response: Response) -> dict[str, Any]:
        report = readiness(state)
        if report["database"] != "ok" or report.get("schema", "current") != "current":
            response.status_code = 503
        return report

    return app
