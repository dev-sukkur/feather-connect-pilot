"""A scratch database on the test server for each test, as the destination or the source of the connectors. Named
`target` so that it stays clear of the root conftest's `pg`, which empties the store's schema."""

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
from real_stack import scratch_database, server_url

from feather_connect.core.context import Context


@pytest.fixture
def target_url() -> Iterator[str]:
    with scratch_database(server_url()) as url:
        yield url


@pytest.fixture
def target(target_url: str) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(target_url, autocommit=True) as conn:
        yield conn


@pytest.fixture
def make_ctx(target_url: str) -> Callable[..., Context]:
    def make(**config: Any) -> Context:
        return Context(
            {"dsn": target_url, **config},
            logging.getLogger("postgres-test"),
            datetime(2026, 11, 7, tzinfo=UTC),
            "UTC",
        )

    return make
