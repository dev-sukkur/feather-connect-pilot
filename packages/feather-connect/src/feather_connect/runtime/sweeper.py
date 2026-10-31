"""The sweeper: finds work that fell through a crash and puts it back on its way (design 6.12 and 7.5).

A worker that stops sending heartbeats is marked dead and its tasks are re-queued. A pending delivery that has sat for
a while may have lost the submit that follows sealing, and a run whose slices are all read may have lost its seal task.
Both are submitted again under the same idempotency key, so one that exists is not run twice.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from feather_connect.engine.port import Engine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SweepReport:
    """What a sweep did. Deliveries are `(route, version_id)` and seals `(run_id, dataset)`."""

    dead_workers: list[str] = field(default_factory=list[str])
    recovered_tasks: list[str] = field(default_factory=list[str])
    resubmitted_deliveries: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])
    resubmitted_seals: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])


def sweep_once(
    store: Store,
    runtime: Runtime,
    engine: Engine,
    *,
    now: datetime,
    dead_after: timedelta = timedelta(seconds=30),
    pending_after: timedelta = timedelta(minutes=1),
) -> SweepReport:
    """Do one sweep as of `now`.

    A worker is dead when it hasn't sent a heartbeat for `dead_after`, and a delivery is lost when it has been pending for
    `pending_after`. What can't be put right, such as one delivery the engine refuses, is logged and left for the next
    sweep, so it doesn't hold up the rest.
    """
    dead = [
        w.worker_id
        for w in store.workers.list_workers()
        if w.state != "dead" and w.last_seen < now - dead_after
    ]
    recovered: list[str] = []
    for worker_id in dead:
        store.workers.mark(worker_id, "dead")
        recovered += engine.recover_worker(worker_id)
    deliveries: list[tuple[str, str]] = []
    for delivery in store.ledger.pending_older_than(pending_after, now=now):
        try:
            runtime.submit_apply(delivery.route, delivery.version_id)
        except Exception:
            log.exception("could not submit delivery %s of version %s", delivery.route, delivery.version_id)
        else:
            deliveries.append((delivery.route, delivery.version_id))
    seals: list[tuple[str, str]] = []
    for run_id, dataset in store.versions.runs_ready_to_seal():
        try:
            run = store.runs.get_run(run_id)
            if run.status == "cancelled":  # somebody stopped the run on purpose
                continue
            runtime.submit_seal(run.flow, dataset, run_id)
        except Exception:
            log.exception("could not submit the seal of dataset %s for run %s", dataset, run_id)
        else:
            seals.append((run_id, dataset))
    return SweepReport(dead, recovered, deliveries, seals)
