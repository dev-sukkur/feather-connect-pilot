"""The simulation connector against the conformance kit, faults included."""

import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from feather_connect.connectors import simulation
from feather_connect.core.connector import InboundRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.webhooks import sign
from feather_connect.testing.conformance import PushSourceConformance, SinkConformance, SourceConformance

T0 = datetime(2026, 11, 7, 9, tzinfo=UTC)


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


class TestSimulationPushSource(PushSourceConformance):
    secret = "push-kit-secret-0123"

    @pytest.fixture
    def source(self) -> simulation.Source:
        return simulation.Source("hooks", generator={"id": simulation.seq()}, rows_per_partition=0)

    @pytest.fixture
    def ctx(self) -> Context:
        return Context({"signature_secret": self.secret}, logging.getLogger("push-kit"), T0, "UTC")

    @pytest.fixture
    def sign_with(self) -> Callable[[str, bytes], InboundRequest]:
        def make(secret: str, body: bytes) -> InboundRequest:
            stamp = str(int(T0.timestamp()))
            headers = {
                simulation.SIGNATURE_HEADER: sign(secret, body, stamp),
                simulation.TIMESTAMP_HEADER: stamp,
            }
            return InboundRequest("POST", "/ingress/default/hooks", headers, body, T0)

        return make
