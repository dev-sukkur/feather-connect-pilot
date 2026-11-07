"""The memory connector against the conformance kit."""

from collections.abc import Callable, Mapping
from typing import Any

import pytest

from feather_connect.connectors import memory
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.testing.conformance import SinkConformance, SourceConformance


class TestMemorySource(SourceConformance):
    @pytest.fixture
    def source(self) -> memory.Source:
        return memory.Source()

    @pytest.fixture
    def seed(self, source: memory.Source) -> Callable[[list[dict[str, Any]]], None]:
        return source.load


class TestMemorySink(SinkConformance):
    restart_safe = False  # what it applied lives in the instance

    @pytest.fixture
    def sink(self) -> memory.Sink:
        return memory.Sink()

    @pytest.fixture
    def read_back(self, sink: memory.Sink) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        return lambda: {row["id"]: row for row in sink.rows}

    @pytest.fixture
    def transient_fault(self, sink: memory.Sink) -> Callable[[], None]:
        return lambda: sink.fail_next(TransientError("down for a moment"))

    @pytest.fixture
    def permanent_fault(self, sink: memory.Sink) -> Callable[[], None]:
        return lambda: sink.fail_next(PermanentError("no such table"))
