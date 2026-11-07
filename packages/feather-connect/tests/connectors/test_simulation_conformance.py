"""The simulation connector against the conformance kit, faults included."""

from collections.abc import Callable, Mapping
from typing import Any

import pytest

from feather_connect.connectors import simulation
from feather_connect.core.errors import PermanentError
from feather_connect.testing.conformance import SinkConformance, SourceConformance


class TestSimulationSource(SourceConformance):
    @pytest.fixture
    def source(self) -> simulation.Source:
        return simulation.Source(generator={"id": simulation.seq()}, rows_per_partition=0)

    @pytest.fixture
    def seed(self, source: simulation.Source) -> Callable[[list[dict[str, Any]]], None]:
        return source.load

    @pytest.fixture
    def transient_fault(self, source: simulation.Source) -> Callable[[], None]:
        def arm() -> None:
            source.faults = simulation.Faults(http_5xx=1.0)

        return arm


class TestSimulationSink(SinkConformance):
    restart_safe = False

    @pytest.fixture
    def sink(self) -> simulation.Sink:
        return simulation.Sink()

    @pytest.fixture
    def read_back(self, sink: simulation.Sink) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        return lambda: {row["id"]: row for row in sink.rows}

    @pytest.fixture
    def permanent_fault(self, sink: simulation.Sink) -> Callable[[], None]:
        return lambda: sink.fail_next(PermanentError("simulated refusal"))
