"""Fixtures shared by the tests that need Postgres. Start it with
`docker compose -f deploy/compose.test.yaml up -d`."""

import os
import uuid
from collections.abc import Iterator
from urllib.parse import urlsplit

import psycopg
import pytest
from psycopg import sql

DEFAULT_URL = "postgresql://feather:feather@127.0.0.1:25433/feather_test"


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    """A fresh, empty database on the test server, dropped when the session ends."""
    admin_url = os.environ.get("FEATHER_TEST_DATABASE_URL", DEFAULT_URL)
    name = f"feather_{uuid.uuid4().hex[:12]}"
    try:
        admin = psycopg.connect(admin_url, autocommit=True, connect_timeout=5)
    except psycopg.OperationalError as error:
        pytest.fail(
            f"the test Postgres is not reachable ({error}). "
            "Start it with: docker compose -f deploy/compose.test.yaml up -d",
            pytrace=False,
        )
    with admin:
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
        try:
            yield urlsplit(admin_url)._replace(path=f"/{name}").geturl()
        finally:
            admin.execute(sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name)))
