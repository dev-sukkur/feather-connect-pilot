"""A small world for tests of the server: a project on disk with one flow, `orders`, run on the in-memory store and
the local engine. `build_world` makes it, and the services of the server are ready on it.

The project is loaded from `root` for real, so its configuration has layers, and the warehouse connection holds a secret
reference that resolves to `SECRET` when the environment variable `WH_TOKEN` is set to it.
"""

import sys
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink
from feather_connect.engine.local import LocalEngine
from feather_connect.project import load_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool
from feather_server.app import AppState
from feather_server.services.actions import Actions
from feather_server.services.reads import Reads
from feather_server.settings import Settings

SECRET = "s3cret-warehouse-token"

FLOW_MODULE = """
    import feather_connect as fc
    from feather_connect.connectors import memory
    from feather_connect.core.connector import ConnectorSpec

    ROWS = [
        {"id": day * 10 + n, "date": f"2026-10-{day:02d}"} for day in range(8, 19) for n in (1, 2)
    ]


    class Shop(memory.Source):
        spec = ConnectorSpec(
            name="shop.api", strategies=frozenset({"replace"}), cost={"read": 2}
        )


    orders = fc.Dataset(
        "orders",
        source=Shop("shop", ROWS),
        key="id",
        strategy=fc.strategy.replace(),
        partition=fc.partition.by_day("date", tz="UTC"),
    )
    flow = fc.Flow(
        "orders",
        reads=orders,
        deliver=[fc.to(memory.Sink("warehouse")), fc.to(memory.Sink("gofrugal"))],
        triggers=[fc.every("15m")],
        owner="ops",
    )
"""

FEATHER_YAML = """
    name: acme
    modules: [flows.orders]
    settings: {timezone: UTC}
    connections:
      shop: {connector: shop.api, site: hq}
      warehouse:
        connector: memory.sink
        database: dev_bronze
        auth: {token: "${secret:env:WH_TOKEN}"}
        budget: api
      gofrugal: {connector: memory.sink}
    budgets:
      api: {limits: ["90/minute"], reserve: {high: 0.6}}
"""

ENV_DEV = """
    flows:
      orders:
        triggers: {every: 15m}
"""

SECRET = "s3cret-warehouse-token"


def day(n: int) -> str:
    """The key of the partition of October 2026's day `n`."""
    return f"date=2026-10-{n:02d}"


@dataclass
class World:
    state: AppState
    engine: LocalEngine
    clock: FrozenClock
    actions: Actions
    reads: Reads

    @property
    def store(self) -> MemoryStore:
        assert isinstance(self.state.store, MemoryStore)
        return self.state.store

    def sink(self, route: str) -> Sink:
        found = next(r.sink for r in self.state.runtime.flow("orders").routes if r.name == route)
        assert isinstance(found, Sink)
        return found

    def run(self, partitions: list[str] | None = None) -> str:
        """Run the flow to the end and return the run."""
        run_id = self.state.runtime.start_run("orders", partitions=partitions)
        self.engine.run_until_idle()
        return run_id


def build_world(root: Path, clock: FrozenClock) -> World:
    """Write the project into the empty directory `root` and load it. The caller sets `WH_TOKEN` if it wants the
    warehouse secret to resolve."""
    for rel, text in (
        ("feather.yaml", FEATHER_YAML),
        ("flows/__init__.py", ""),
        ("flows/orders.py", FLOW_MODULE),
        ("env/dev.yaml", ENV_DEV),
    ):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(dedent(text), encoding="utf-8")
    project = load_project(root, "dev")
    sys.modules.pop("flows", None)  # the next world builds its own flows
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    for budget in project.budgets.values():
        store.budgets.define(budget)
    runtime = Runtime(project.flows, store, spool, engine, code_sha="sha", clock=clock)
    engine.register(runtime.tasks())
    state = AppState(
        Settings(secret_key="test-secret-key", engine="local"),
        project,
        store,
        spool,
        engine,
        runtime,
        clock=clock,
    )
    return World(state, engine, clock, Actions(state), Reads(state))
