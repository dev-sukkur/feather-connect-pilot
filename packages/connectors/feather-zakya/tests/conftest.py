"""What the Zakya tests share: a simulated organisation on a clock the tests move, and contexts for its connection."""

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from feather_connect.connectors.zakya.testing import ZakyaSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.context import Context
from feather_connect.runtime.http import forget_tokens

NOW = datetime(2026, 11, 7, 9, tzinfo=UTC)
CONFIG: dict[str, Any] = {
    "organization_id": "60001",
    "base_url": "https://zakya.test",
    "accounts_url": "https://accounts.zoho.test",
    "retry_attempts": 3,
    # resolved from `auth`, as the runtime gives them
    "client_id": "1000.TESTCLIENT",
    "client_secret": "test-client-secret-0123456789",
    "refresh_token": "1000.test-refresh-token-0123456789",
}


class Clock:
    """The time the simulator sees, moved on by the connectors' sleeps."""

    def __init__(self) -> None:
        self.at = NOW
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.at

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.at += timedelta(seconds=seconds)


@pytest.fixture(autouse=True)
def _no_tokens_kept() -> Iterator[None]:
    forget_tokens()
    yield
    forget_tokens()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def sim(clock: Clock) -> ZakyaSim:
    return ZakyaSim(now=clock.now)


@pytest.fixture
def make_ctx() -> Callable[..., Context]:
    def make(tenant: str = "default", **config: Any) -> Context:
        return Context({**CONFIG, **config}, logging.getLogger("zakya-test"), NOW, "UTC", tenant=tenant)

    return make


@pytest.fixture
def ctx(make_ctx: Callable[..., Context]) -> Context:
    return make_ctx()


@pytest.fixture
def wire(sim: ZakyaSim, clock: Clock) -> Callable[[Any], Any]:
    """Point a Zakya connector at the simulator, its waits spent on the simulator's clock."""

    def wired(connector: Any) -> Any:
        connector.transport = sim.transport()
        connector.sleep = clock.sleep
        return connector

    return wired
