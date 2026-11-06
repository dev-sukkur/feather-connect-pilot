"""What the HTTP connector tests share: a context for a connection, and a budget to meter it."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from feather_connect.clock import FrozenClock
from feather_connect.core.context import Context
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import BudgetSpec, parse_limits

BASE = "https://api.example.test"
NOW = datetime(2026, 11, 6, 9, tzinfo=UTC)


@pytest.fixture
def make_ctx() -> Callable[..., Context]:
    def make(budget: BudgetHandle | None = None, **config: Any) -> Context:
        return Context(
            {"base_url": BASE, **config}, logging.getLogger("http-test"), NOW, "UTC", budget=budget
        )

    return make


class Budget:
    """A budget in a memory store whose clock moves on when a handle sleeps."""

    def __init__(self, limits: list[str]) -> None:
        self.clock = FrozenClock(NOW)
        self.store = MemoryStore(self.clock)
        self.store.budgets.define(BudgetSpec("api", parse_limits(limits)))
        self.sleeps: list[float] = []

    def handle(self) -> BudgetHandle:
        def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            self.clock.advance(timedelta(seconds=seconds))

        return BudgetHandle(self.store.budgets, "api", sleep=sleep)

    def used(self) -> list[float]:
        return [usage.used for usage in self.store.budgets.usage("api")]


@pytest.fixture
def make_budget() -> Callable[[list[str]], Budget]:
    return Budget
