"""The Postgres leader lock: see `feather_connect.runtime.leader`."""

import threading
from typing import Any

import psycopg

from feather_connect.runtime.leader import LeaderLock


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
                        psycopg.connect(self._url, autocommit=True, connect_timeout=5),
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
