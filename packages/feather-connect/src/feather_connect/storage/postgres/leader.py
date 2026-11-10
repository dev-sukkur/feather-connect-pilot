"""The Postgres leader lock: see `feather_connect.runtime.leader`."""

import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import sql

from feather_connect.runtime.leader import LeaderLock
from feather_connect.storage.postgres.support import session_options


class PostgresLeaderLock(LeaderLock):
    """`pg_try_advisory_lock(key)` on one session, which this keeps open while it holds the lock."""

    def __init__(self, database_url: str, key: int) -> None:
        self._url = database_url
        self._key = key
        self._mutex = threading.Lock()
        self._conn: psycopg.Connection[tuple[Any, ...]] | None = None
        self._held = False

    def try_acquire(self) -> bool:
        with self._mutex:
            try:
                if self._conn is None or self._conn.closed:
                    self._conn, self._held = (
                        psycopg.connect(self._url, autocommit=True, **session_options(self._url)),
                        False,
                    )
                if self._held:
                    self._conn.execute("select 1")  # fails if the session is gone, and the lock with it
                    return True
                row = self._conn.execute("select pg_try_advisory_lock(%s)", (self._key,)).fetchone()
                self._held = row is not None and bool(row[0])
            except psycopg.Error:
                self._drop()
                return False
            if not self._held:
                self._drop()  # keep no idle session for a lock somebody else holds
            return self._held

    def release(self) -> None:
        with self._mutex:
            self._drop()

    def _drop(self) -> None:
        """Close the session, which frees the lock. The server does that too if the session is already gone."""
        conn, self._conn, self._held = self._conn, None, False
        if conn is not None:
            try:
                conn.close()
            except psycopg.Error:
                pass


def advisory_key(tenant: str, name: str) -> int:
    """A Postgres advisory lock key for `name` in `tenant`: the first 8 bytes of a sha256, signed."""
    import hashlib

    digest = hashlib.sha256(f"feather:{tenant}:{name}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


class KeyLocks:
    """Advisory locks on names, each held on a session of its own for as long as the `hold` block runs, and given up
    when the block ends or the session dies. A process that waits longer than `wait_s` gets a `psycopg.Error`."""

    def __init__(self, database_url: str, tenant: str, wait_s: float = 600.0) -> None:
        self._url = database_url
        self._tenant = tenant
        self._wait_ms = int(wait_s * 1000)

    @contextmanager
    def hold(self, name: str) -> Generator[None]:
        options: dict[str, Any] = {**session_options(self._url), "options": "-c statement_timeout=0"}
        with psycopg.connect(self._url, autocommit=True, **options) as conn:
            conn.execute(sql.SQL("set lock_timeout = {}").format(sql.Literal(self._wait_ms)))
            conn.execute("select pg_advisory_lock(%s)", (advisory_key(self._tenant, name),))
            yield
