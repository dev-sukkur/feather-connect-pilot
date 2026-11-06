"""The sweeper: finds work that fell through a crash and puts it back on its way (design 6.12 and 7.5).

A worker that stops sending heartbeats has its tasks re-queued and is then marked dead, so a recovery that fails is
tried again. A pending delivery that has sat for a while may have lost the submit that follows sealing, a run still
queued may have lost the submit of its plan, and a run whose slices are all read may have lost its seal task. These are
submitted again under the same idempotency key, so one that exists is not run twice. A delivery still open when its task
has ended can't be helped by that, so it is failed, which lets an operator retry or skip it. A staged version of a run
that failed or was cancelled will never be sealed, so it goes, or it would keep its day's chunks for good.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from feather_connect.engine.port import Engine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store

log = logging.getLogger(__name__)
_QUEUED = 1000  # runs one sweep looks at
_RUNNING = 1000  # deliveries one sweep looks at


@dataclass(frozen=True)
class SweepReport:
    """What a sweep did. Deliveries are `(route, version_id)` and seals `(run_id, dataset)`."""

    dead_workers: list[str] = field(default_factory=list[str])
    recovered_tasks: list[str] = field(default_factory=list[str])
    resubmitted_deliveries: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])
    resubmitted_seals: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])
    resubmitted_runs: list[str] = field(default_factory=list[str])
    stranded_deliveries: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])
    discarded_versions: list[str] = field(default_factory=list[str])


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
    `pending_after` and is the next of its stream: one behind it waits its turn and has no task to lose, so its cost is
    nothing however long the queue behind a failure grows. What can't be put right, such as one delivery the engine refuses, is logged and left for the next
    sweep, so it doesn't hold up the rest.
    """
    dead: list[str] = []
    recovered: list[str] = []
    for worker in store.workers.list_workers():
        if worker.state == "dead" or worker.last_seen >= now - dead_after:
            continue
        try:  # in this order: a worker marked dead is never looked at again, so it must be recovered by then
            recovered += engine.recover_worker(worker.worker_id)
            store.workers.mark(worker.worker_id, "dead")
        except Exception:
            log.exception("could not recover worker %s", worker.worker_id)
        else:
            dead.append(worker.worker_id)
    deliveries: list[tuple[str, str]] = []
    stranded: list[tuple[str, str]] = []
    for delivery in store.ledger.pending_older_than(pending_after, now=now):
        if runtime.is_route_paused(delivery.route):  # held on purpose, and picked up when the pause ends
            continue
        name = (delivery.route, delivery.version_id)
        try:
            runtime.submit_apply(*name)
            ended = runtime.fail_stranded(*name)
        except Exception:
            log.exception("could not submit delivery %s of version %s", *name)
        else:
            (stranded if ended else deliveries).append(name)
    for delivery in store.ledger.list_deliveries(status="running", limit=_RUNNING):
        name = (delivery.route, delivery.version_id)
        try:
            if runtime.fail_stranded(*name):
                stranded.append(name)
        except Exception:
            log.exception("could not check delivery %s of version %s", *name)
    runs: list[str] = []
    for run in store.runs.list_runs(status="queued", limit=_QUEUED):
        if run.created_at >= now - pending_after or runtime.is_flow_paused(run.flow):
            continue
        try:
            runtime.submit_plan(run.flow, run.run_id)
        except Exception:
            log.exception("could not submit the plan of run %s", run.run_id)
        else:
            runs.append(run.run_id)
    seals: list[tuple[str, str]] = []
    for run_id, dataset in store.versions.runs_ready_to_seal():
        try:
            runtime.submit_seal(store.runs.get_run(run_id).flow, dataset, run_id)
        except Exception:
            log.exception("could not submit the seal of dataset %s for run %s", dataset, run_id)
        else:
            seals.append((run_id, dataset))
    discarded: list[str] = []
    for version in store.versions.abandoned():
        try:
            runtime.discard_abandoned(version.version_id)
        except Exception:
            log.exception("could not discard version %s of run %s", version.version_id, version.run_id)
        else:
            discarded.append(version.version_id)
    return SweepReport(dead, recovered, deliveries, seals, runs, stranded, discarded)
