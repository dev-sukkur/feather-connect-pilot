"""`sap_cpi.Post` against the conformance kit, in front of the simulated CPI tenant, and a resend after a lost answer
posting nothing twice (H9)."""

import logging
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from feather_connect.connectors import sap_cpi  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.connectors.sap_cpi.testing import CpiSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import ApplyBatch, ApplyRecord
from feather_connect.core.context import Context
from feather_connect.runtime.http import forget_tokens
from feather_connect.testing.conformance import SinkConformance

BASE = "https://cpi.test"
CONFIG = {
    "base_url": BASE,
    "token_url": f"{BASE}/oauth/token",
    "client_id": "cid",
    "client_secret": "csecret",
    "retry_attempts": 2,
    "csrf": True,
}


@pytest.fixture(autouse=True)
def _no_tokens_kept() -> Iterator[None]:
    forget_tokens()
    yield
    forget_tokens()


@pytest.fixture
def sim() -> CpiSim:
    return CpiSim(key="id", csrf=True)


def _ctx() -> Context:
    return Context(dict(CONFIG), logging.getLogger("cpi"), datetime(2026, 11, 12, tzinfo=UTC), "UTC")


@pytest.fixture
def ctx() -> Context:
    return _ctx()


def _wired(sim: CpiSim) -> Any:
    sink = sap_cpi.Post("sap", path="/http/docs", reversal_path="/http/docs/reverse")
    sink.transport = sim.transport()
    sink.sleep = lambda s: None
    return sink


class TestPost(SinkConformance):
    deletes_supported = False  # a posting is reversed, not deleted

    @pytest.fixture
    def ctx(self) -> Context:
        return _ctx()

    @pytest.fixture
    def fresh_sink(self, sim: CpiSim) -> Callable[[], Any]:
        return lambda: _wired(sim)

    @pytest.fixture
    def sink(self, fresh_sink: Callable[[], Any]) -> Any:
        return fresh_sink()

    @pytest.fixture
    def read_back(self, sim: CpiSim) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        return lambda: dict(sim.documents.get("docs", {}))

    @pytest.fixture
    def transient_fault(self, sim: CpiSim) -> Callable[[], None]:
        return lambda: sim.fail_next(503, times=100)

    @pytest.fixture
    def permanent_fault(self, sim: CpiSim) -> Callable[[], None]:
        return lambda: sim.fail_next(401, times=100)


def test_a_chunk_sent_again_after_a_lost_answer_posts_nothing_twice(sim: CpiSim, ctx: Context):
    sink = _wired(sim)
    batch = ApplyBatch(
        "sap", "v1", 0, "_", "upsert", [ApplyRecord("1", "upsert", {"id": "1"})], "k", True, True
    )
    sink.apply(ctx, batch)
    sink.apply(ctx, batch)  # the runtime's retry of a chunk whose answer was lost
    assert sim.posts == 1
    assert sink.compensate(ctx, batch).applied == 1 and sim.reversed == {"docs": ["1"]}
