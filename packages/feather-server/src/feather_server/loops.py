"""The loops the server runs, each on its own thread and behind its own leader lock.

Several server replicas can run the same loops: a replica runs one only while it holds that loop's lock, so each loop
has at most one runner at a time, and another replica takes over within one interval when the leader goes away.
"""

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from feather_connect.runtime.leader import AlwaysLeader, LeaderLock, leader_key
from feather_connect.runtime.retention import retention_once
from feather_connect.runtime.sweeper import sweep_once
from feather_connect.storage.postgres.leader import PostgresLeaderLock

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

_JOIN_S = 30.0


class LoopRunner:
    """Calls `fn` every `interval_s` seconds on a daemon thread, while it leads.

    `fn` raising does not end the loop: the error is logged, kept in `last_error` and the loop goes on. `count` is how
    many times `fn` has been called, failed or not, and `leading` says whether the last pass held the lock.
    """

    def __init__(self, name: str, fn: Callable[[], object], interval_s: float, lock: LeaderLock) -> None:
        self.name = name
        self.interval_s = interval_s
        self.count = 0
        self.last_error: Exception | None = None
        self.leading = False
        self._fn = fn
        self._lock = lock
        self._stopping = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"loop-{name}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """Stop after the pass that is running, give up the lock and wait for the thread."""
        self._stopping.set()
        if self._thread.is_alive():
            self._thread.join(_JOIN_S)
        self._lock.release()  # the thread does so too; if it never started, this is the only release
        self.leading = False

    def _run(self) -> None:
        try:
            while not self._stopping.is_set():
                self.leading = self._lock.try_acquire()
                if self.leading:
                    self._once()
                self._stopping.wait(self.interval_s)
        finally:
            self._lock.release()

    def _once(self) -> None:
        try:
            self._fn()
        except Exception as error:
            self.last_error = error
            log.exception("loop %s failed", self.name)
        self.count += 1


def default_loops(state: "AppState") -> list[LoopRunner]:
    """Return the loops of a server: the sweeper and retention.

    Each takes the Postgres lock of its name when the server has a database, and leads alone otherwise.
    """

    def lock(name: str) -> LeaderLock:
        url = state.settings.database_url
        return PostgresLeaderLock(url, leader_key(name)) if url else AlwaysLeader()

    def sweep() -> None:
        sweep_once(state.store, state.runtime, state.engine, now=state.clock.now())

    def retain() -> None:
        state.spool.ensure_partitions()  # so that the next days' writes find a partition
        retention_once(state.store, state.spool, now=state.clock.now())

    return [
        LoopRunner("sweeper", sweep, 5.0, lock("sweeper")),
        LoopRunner("retention", retain, 3600.0, lock("retention")),
    ]
