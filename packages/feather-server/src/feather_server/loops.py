"""The loops the server runs, each on its own thread and behind its own leader lock.

Several server replicas can run the same loops: a replica runs one only while it holds that loop's lock, so each loop
has at most one runner at a time, and another replica takes over within one interval when the leader goes away.
"""

import logging
import threading
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from feather_connect.runtime.catalogue import Catalogue
from feather_connect.runtime.leader import AlwaysLeader, LeaderLock, leader_key
from feather_connect.runtime.retention import RetentionReport, retention_once
from feather_connect.runtime.scheduler import tick
from feather_connect.runtime.sealer import seal_once
from feather_connect.runtime.sweeper import SweepReport, sweep_once
from feather_connect.storage.postgres.leader import PostgresLeaderLock

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

_JOIN_S = 30.0
_NAMED = 5  # items of a list that a log line spells out


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


def _some(items: Sequence[object]) -> str:
    """The size of a list and its first few items: a sweep can report a thousand."""
    shown = ", ".join(str(item) for item in items[:_NAMED])
    return f"{len(items)} ({shown}{', ...' if len(items) > _NAMED else ''})"


def log_retention(report: RetentionReport) -> None:
    """Say which old days a pass kept and why: they hold disk until whatever keeps them is put right."""
    for day, why in sorted(report.kept_days.items()):
        log.warning("retention kept %s: %s", day, why)
    for day, why in sorted(report.kept_inbox_days.items()):
        log.warning("retention kept inbox day %s: %s", day, why)


class SweepLog:
    """Logs what each sweep did. A resubmit repeats every sweep until it is taken up, so it is said once, when the
    list changes, and at INFO. The rest happens once per thing, and means something went wrong, so it is a warning."""

    def __init__(self) -> None:
        self._resubmits: tuple[frozenset[object], ...] = (frozenset(), frozenset(), frozenset())

    def __call__(self, report: SweepReport) -> None:
        for what, items in (
            ("marked workers dead", report.dead_workers),
            ("recovered tasks", report.recovered_tasks),
            ("failed stranded deliveries", report.stranded_deliveries),
            ("discarded versions", report.discarded_versions),
        ):
            if items:
                log.warning("sweeper %s: %s", what, _some(items))
        deliveries, seals, runs = (
            report.resubmitted_deliveries,
            report.resubmitted_seals,
            report.resubmitted_runs,
        )
        resubmits = (frozenset[object](deliveries), frozenset[object](seals), frozenset[object](runs))
        if resubmits != self._resubmits:
            self._resubmits = resubmits
            log.info(
                "sweeper resubmitting deliveries: %s; seals: %s; runs: %s",
                _some(deliveries),
                _some(seals),
                _some(runs),
            )


def default_loops(state: "AppState") -> list[LoopRunner]:
    """Return the loops of a server: the scheduler, the sweeper, retention and the sealer.

    Each takes the Postgres lock of its name when the server has a database, and leads alone otherwise.
    """

    def lock(name: str) -> LeaderLock:
        url = state.settings.database_url
        return PostgresLeaderLock(url, leader_key(name)) if url else AlwaysLeader()

    catalogue = Catalogue(state.store)  # installed packages' active flows (flow packages §7.2)
    catalogue.listen()
    state.closers.append(catalogue.close)

    def schedule() -> None:
        now = state.clock.now()
        tick(state.runtime, now=now)
        catalogue.tick(now)

    sweep_log = SweepLog()

    def sweep() -> None:
        sweep_log(sweep_once(state.store, state.runtime, state.engine, now=state.clock.now()))

    def retain() -> None:
        state.spool.ensure_partitions()  # so that the next days' writes find a partition
        state.store.inbox.ensure_partitions()
        log_retention(retention_once(state.store, state.spool, now=state.clock.now()))

    def seal() -> None:
        seal_once(state.runtime, now=state.clock.now())

    return [
        LoopRunner("scheduler", schedule, 1.0, lock("scheduler")),
        LoopRunner("sweeper", sweep, 5.0, lock("sweeper")),
        LoopRunner("retention", retain, 3600.0, lock("retention")),
        # Often enough that a batch waits about `max_age` (1 s), and no more than a quarter of that again.
        LoopRunner("sealer", seal, 0.25, lock("sealer")),
    ]
