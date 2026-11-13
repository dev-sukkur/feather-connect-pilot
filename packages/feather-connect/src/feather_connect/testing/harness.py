"""FlowHarness: runs a flow against in-memory connectors on the local engine, for flow authors' tests.

The harness never touches the flow's real connectors. It builds a copy of the flow in which each dataset's source
and each route's sink is replaced by an in-memory one that declares the same spec, so what the real connector could
not do, the stand-in can't do either.
"""

import copy
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime

from feather_connect.clock import Clock, FrozenClock
from feather_connect.connectors import memory
from feather_connect.core.connector import PushSource, Sink, Source
from feather_connect.core.dataset import Dataset
from feather_connect.core.flow import Flow, RouteSpec
from feather_connect.core.windows import AvailabilityWindow
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import Ledger, QuarantineStore, Version
from feather_connect.storage.spool import MemorySpool

_START = datetime(2026, 1, 1, tzinfo=UTC)


def _source_like(real: Source | PushSource) -> memory.Source:
    class Stand(memory.Source):
        spec = real.spec

    return Stand(real.connection)


def _sink_like(real: Sink) -> memory.Sink:
    class Stand(memory.Sink):
        spec = real.spec

    return Stand(real.connection)


class FlowHarness:
    """Runs `flow` with every source and sink swapped for an in-memory one.

    Load rows into a source with `source(dataset).load(rows)`, which makes them the source's whole contents. `run`
    runs the flow to the end. `clock` defaults to a frozen clock shared by the engine, the store and the runtime.
    `also` adds other flows of the project, swapped the same way, such as the one that keeps a reference dataset a
    transform or a derived dataset reads; `run(flow=name)` runs one of them. `windows` maps a connection to its
    availability window, as `connections.<name>.windows` does in feather.yaml.
    """

    def __init__(
        self,
        flow: Flow,
        *,
        clock: Clock | None = None,
        also: Sequence[Flow] = (),
        windows: Mapping[str, AvailabilityWindow] | None = None,
    ) -> None:
        self._flow = flow
        self.clock: Clock = FrozenClock(_START) if clock is None else clock
        self._sources: dict[str, memory.Source] = {}
        self._sinks: dict[str, memory.Sink] = {}
        self.store = MemoryStore(self.clock)
        self.engine = LocalEngine(self.clock if isinstance(self.clock, FrozenClock) else None)
        self.runtime = Runtime(
            [self._swapped(f) for f in (flow, *also)],
            self.store,
            MemorySpool(),
            self.engine,
            code_sha="test",
            clock=self.clock,
            windows=windows,
        )
        self.engine.register(self.runtime.tasks())

    def _swapped(self, flow: Flow) -> Flow:
        datasets: list[Dataset] = []
        for dataset in flow.datasets:
            if getattr(dataset.source, "connectionless", False):  # a derived dataset reads the store: keep it
                datasets.append(dataset)
                continue
            stand = self._sources.setdefault(dataset.name, _source_like(dataset.source))
            datasets.append(replace(dataset, source=stand))
        routes: list[RouteSpec] = []
        for route in flow.routes:
            routes.append(replace(route, sink=self._sinks.setdefault(route.name, _sink_like(route.sink))))
        swapped = copy.copy(flow)
        swapped.datasets = datasets
        swapped.routes = routes
        return swapped

    def source(self, dataset_name: str) -> memory.Source:
        """Return the in-memory source standing in for the dataset's."""
        return self._sources[dataset_name]

    def sink(self, route_name: str) -> memory.Sink:
        """Return the in-memory sink standing in for the route's."""
        return self._sinks[route_name]

    def run(self, *, partitions: Sequence[str] | None = None, flow: str | None = None) -> str:
        """Start a manual run of the flow, or of the flow named `flow`, run it until nothing is left to do and return
        its id."""
        run_id = self.runtime.start_run(flow or self._flow.name, partitions=partitions)
        self.engine.run_until_idle()
        return run_id

    @property
    def ledger(self) -> Ledger:
        return self.store.ledger

    @property
    def quarantine(self) -> QuarantineStore:
        return self.store.quarantine

    def versions(self, dataset_name: str) -> list[Version]:
        """Return the dataset's versions, newest first."""
        return self.store.versions.list_versions(dataset_name)
