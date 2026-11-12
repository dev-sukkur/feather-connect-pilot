"""The runtime: flows, store and engine wired into tasks, and the ways to start and steer a run.

A task's arguments are ids (I8), and all it reads or does outside itself happens in `ctx.step`. Each task has a row in
the run's journal from before it is submitted, so a run is over once none of its rows is running. The row is found by
`ctx.task_ref`, which is the task's idempotency key.

An `apply` task is submitted only for the oldest open delivery of its stream, and each one that ends submits the next.
A task submitted any earlier could find itself blocked and end, and its key would then be used up for good. Until
then a waiting delivery has a row but no task, which keeps its run going while an older version blocks it.
"""

import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

from feather_connect.clock import Clock, SystemClock
from feather_connect.core.approval import APPROVED
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import BudgetDeferred, PermanentError
from feather_connect.core.flow import Flow, RouteSpec
from feather_connect.engine.port import Engine, RetryPolicy, TaskCall, TaskContext, TaskFn
from feather_connect.ids import new_id
from feather_connect.logs import bind, redact
from feather_connect.runtime.apply import fail_open, oldest_open, replay_gap, run_apply
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import ExtractFailed, VerifyResult, discard_abandoned, run_extract
from feather_connect.runtime.plan import run_plan
from feather_connect.runtime.seal import run_seal
from feather_connect.storage.port import (
    CLOSED,
    STREAM_TRIGGER,
    Delivery,
    DeliveryStatus,
    InvalidTransition,
    Priority,
    Run,
    Store,
    TaskRow,
    Window,
    delivery_cursor,
    is_paused,
)
from feather_connect.storage.spool import Spool

log = logging.getLogger(__name__)
_TENANT = "default"
_UTC = "UTC"
_ACTIVE = ("queued", "running")
_PAUSED = "route paused"  # why a task ended without delivering: the key of its successor ends in `#n`
_MANY = 10_000
VERIFY = "verify"  # a run's param: the partition its host checks, for a packaged dataset (flow packages §12)
_READING = (
    "plan",
    "extract",
    "seal_run",
)  # the tasks that read: what a tick can overlap, and what a cancel stops
_SKIP = "skip"  # the kind of the row of a tick that didn't go ahead
_RUNS_LOOKED_AT = (
    20  # rows of tasks still reading that one tick looks at: there are few, as ticks don't overlap
)
_STREAM_RUNS_LOOKED_AT = 200  # running runs a sweep looks through for earlier days' stream runs
_LOST_AFTER = timedelta(minutes=1)  # how long a tick's plan may stay unstarted before it is submitted again
_READS_IN_VAIN = (
    5  # reads that spend budget and still come up short, each after the longest wait, before giving up
)


def _no_config(connection: str) -> Mapping[str, Any]:
    return {}


def _tick_ref(run_id: str, tick: str, what: str) -> str:
    """Return the key of a task of a tick. They share a prefix, so a tick's tasks can be found among the day's."""
    return f"tick:{run_id}:{tick}:{what}"


def _apply_key(delivery: Delivery) -> str:
    """Return the key of the delivery's task: the same state gets the same task, and a retry or a replay a new one."""
    return f"apply:{delivery.route}:{delivery.version_id}:{delivery.replays}:{delivery.retries}"


class FlowPaused(Exception):
    """A run was asked for on a flow that is paused. The message says why, as far as the pause says."""


class RunNotStarted(Exception):
    """The run was written, and its plan could not be submitted. The run has been marked failed with the reason."""

    def __init__(self, run_id: str, reason: str) -> None:
        super().__init__(reason)
        self.run_id = run_id
        self.reason = reason


@dataclass(frozen=True)
class _Extracted:
    """What an extract step hands to the steps after it: what it sealed, and the claim it read its slice under."""

    version_ids: list[str]
    writer: str


class Runtime:
    """Runs `flows` on `engine`. Register `tasks()` with whatever executes the tasks.

    `code_sha` identifies the running code. `make_context` gives the context to call a connector with, given the name
    of its connection, and by default holds no settings. `clock` defaults to the system clock.

    `budgets` maps a connection to the budget its calls are metered by, and the default context gives such a connection
    a `BudgetHandle` at the priority of its datasets and routes. `sleep` is how a handle spends a short wait.

    `config_of` gives the settings of a connection, secrets resolved, for the default context to hand to its
    connector; it is called each time a context is made, so a secret is never kept. `timezone` is the IANA name of
    the time zone that contexts carry. `prefix` is the package prefix of an installed package's flows: a task an M1
    worker submitted before the upgrade to packages (§13) names them without it, and is found by the prefixed name.
    """

    def __init__(
        self,
        flows: Iterable[Flow],
        store: Store,
        spool: Spool,
        engine: Engine,
        *,
        code_sha: str = "dev",
        clock: Clock | None = None,
        make_context: Callable[[str], Context] | None = None,
        budgets: Mapping[str, str] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        config_of: Callable[[str], Mapping[str, Any]] | None = None,
        timezone: str = _UTC,
        prefix: str | None = None,
    ) -> None:
        self._engine = engine
        self._prefix = prefix
        self._clock: Clock = SystemClock() if clock is None else clock
        self._deps = RuntimeDeps(store, spool, self._clock, code_sha, make_context or self._default_context)
        self._flows: dict[str, Flow] = {}
        self._datasets: dict[str, Dataset] = {}
        self._routes: dict[str, RouteSpec] = {}
        for flow in flows:
            self._add(self._flows, "flow", flow.name, flow)
            for dataset in flow.datasets:
                self._add(self._datasets, "dataset", dataset.name, dataset)
            for route in flow.routes:
                self._add(self._routes, "route", route.name, route)
        self._budgets = dict(budgets or {})
        self._sleep = sleep
        self._config_of = config_of or _no_config
        self._timezone = timezone
        self._priorities = self._connection_priorities()
        self._stream_runs: dict[str, tuple[str, str]] = {}  # by flow: the day, and the stream run of that day

    @staticmethod
    def _add[T](found: dict[str, T], kind: str, name: str, value: T) -> None:
        """Index `value` by name. The store knows these by name alone, so a name can mean only one thing."""
        if found.setdefault(name, value) is not value:
            raise ValueError(f"two different things are the {kind} named {name!r}: name them apart")

    def _connection_priorities(self) -> dict[str, Priority]:
        """Return the priority of each budgeted connection's work. One connection can't do work of two priorities."""
        found: dict[str, Priority] = {}
        uses: list[tuple[str, Priority]] = []
        uses.extend((d.source.connection, d.priority) for d in self._datasets.values())
        uses.extend((r.sink.connection, r.priority) for r in self._routes.values())
        for connection, priority in uses:
            if connection in self._budgets and found.setdefault(connection, priority) != priority:
                raise ValueError(
                    f"connection {connection!r} is used at two priorities: give each its own connection"
                )
        return found

    def _default_context(self, connection: str) -> Context:
        handle = None
        if connection in self._budgets:
            priority = self._priorities.get(connection, "normal")
            handle = BudgetHandle(self._deps.store.budgets, self._budgets[connection], priority, self._sleep)
        config = self._config_of(connection)
        return Context(
            config,
            logging.getLogger(connection),
            self._clock.now(),
            self._timezone,
            budget=handle,
            tenant=_TENANT,
        )

    @property
    def store(self) -> Store:
        return self._deps.store

    @property
    def deps(self) -> RuntimeDeps:
        return self._deps

    def make_context(self, connection: str) -> Context:
        """Return the context to call the connection's connector with. It holds the secrets of the connection, and
        raises `PermanentError` if one can't be resolved."""
        return self._deps.make_context(connection)

    @property
    def timezone(self) -> str:
        """The IANA name of the time zone that contexts carry and that a stream run's day is told in."""
        return self._timezone

    def flows(self) -> list[Flow]:
        """Return the flows, in the order they were given."""
        return list(self._flows.values())

    def flow(self, name: str) -> Flow:
        """Return the flow called `name`. Raises `KeyError` if there is none, as `dataset` and `route` do."""
        return self._find(self._flows, "flow", name)

    def dataset(self, name: str) -> Dataset:
        return self._find(self._datasets, "dataset", name)

    def route(self, name: str) -> RouteSpec:
        return self._find(self._routes, "route", name)

    def _find[T](self, found: Mapping[str, T], kind: str, name: str) -> T:
        if name in found:
            return found[name]
        if self._prefix is not None and (prefixed := f"{self._prefix}/{name}") in found:
            return found[prefixed]  # an M1 task's name, from before the upgrade
        raise KeyError(f"no {kind} is named {name!r}")

    def tasks(self) -> Mapping[str, TaskFn]:
        return {
            "feather.plan": self._logged(self._plan_task),
            "feather.extract": self._logged(self._extract_task),
            "feather.seal_run": self._logged(self._seal_task),
            "feather.apply": self._logged(self._apply_task),
        }

    def _logged(self, task: TaskFn) -> TaskFn:
        """Wrap a task so that everything it logs carries the ids of its run and its journal row."""

        def run(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any] | None:
            run_id = args["run_id"]
            row = self._deps.store.runs.task_by_ref(run_id, ctx.task_ref)
            with bind(
                tenant=_TENANT,
                flow=args.get("flow"),
                run=run_id,
                task=row.task_id if row else None,
                dataset=args.get("dataset"),
                partition=args.get("partition"),
                version=args.get("version_id"),
                route=args.get("route"),
            ):
                return task(ctx, args)

        return run

    # What starts and steers runs.

    def is_flow_paused(self, flow: str) -> bool:
        """Whether the flow is paused now: its ticks are skipped and a run is refused."""
        return is_paused(self._deps.store.overrides, f"flow:{flow}")

    def is_route_paused(self, route: str) -> bool:
        """Whether the route is paused now: its deliveries wait, and extraction goes on."""
        return is_paused(self._deps.store.overrides, f"route:{route}")

    def pause_flow(
        self, flow: str, *, actor: str, reason: str | None = None, expires_at: datetime | None = None
    ) -> None:
        """Pause the flow until `resume_flow`, or until `expires_at`. Raises `KeyError` if there is no such flow."""
        self.flow(flow)
        self._deps.store.overrides.set(
            f"flow:{flow}", "paused", True, actor=actor, reason=reason, expires_at=expires_at
        )

    def resume_flow(self, flow: str, *, actor: str, reason: str | None = None) -> None:
        """Lift the flow's pause. Ticks skipped while it lasted are not made up."""
        self.flow(flow)
        self._deps.store.overrides.clear(f"flow:{flow}", "paused", actor=actor, reason=reason)

    def pause_route(
        self, route: str, *, actor: str, reason: str | None = None, expires_at: datetime | None = None
    ) -> None:
        """Hold the route's deliveries until `resume_route`, or until `expires_at`, when the sweeper picks them up.

        Extraction and the other routes go on, and sealed versions pile up for the route in order (I2, I5).
        """
        self.route(route)
        self._deps.store.overrides.set(
            f"route:{route}", "paused", True, actor=actor, reason=reason, expires_at=expires_at
        )

    def resume_route(self, route: str, *, actor: str, reason: str | None = None) -> None:
        """Lift the route's pause and submit the oldest open delivery of each of its streams, which apply the backlog
        in seal order."""
        self.route(route)
        self._deps.store.overrides.clear(f"route:{route}", "paused", actor=actor, reason=reason)
        ledger = self._deps.store.ledger
        streams: set[tuple[str, str]] = set()
        page = ledger.list_deliveries(route=route, status="pending")
        while page:
            for delivery in page:
                if (delivery.dataset, delivery.partition) in streams:
                    continue
                streams.add((delivery.dataset, delivery.partition))
                next_up = self._next_of(delivery)
                if next_up is not None and next_up.status == "pending":
                    self.submit_apply(next_up.route, next_up.version_id)
            page = ledger.list_deliveries(route=route, status="pending", cursor=delivery_cursor(page[-1]))

    def _pause_note(self, target: str) -> str:
        found = next(
            (o for o in self._deps.store.overrides.list_all() if (o.target, o.key) == (target, "paused")),
            None,
        )
        if found is None:
            return ""
        return f" by {found.created_by}" + (f": {found.reason}" if found.reason else "")

    def start_run(
        self,
        flow: str,
        *,
        trigger: str = "manual",
        actor: str = "system",
        partitions: Sequence[str] | None = None,
        reason: str | None = None,
        max_concurrent: int | None = None,
    ) -> str:
        """Create a run and submit its plan. `partitions` overrides what would be planned, for a re-run of a slice.

        `max_concurrent` limits how many of the run's extract tasks are submitted at once, as a backfill needs: the
        plan submits that many, and each extract that ends submits the next.

        Raises `RunNotStarted` when the plan can't be submitted, having failed the run, so it isn't left `queued`.
        """
        self.flow(flow)
        if max_concurrent is not None and max_concurrent < 1:
            raise ValueError(f"max_concurrent is {max_concurrent}: it must be at least 1")
        if self.is_flow_paused(flow):
            raise FlowPaused(f"flow {flow!r} is paused{self._pause_note(f'flow:{flow}')}")
        params: dict[str, Any] = {} if partitions is None else {"partitions": list(partitions)}
        if max_concurrent is not None:
            params["max_concurrent"] = max_concurrent
        run_id = self._deps.store.runs.create_run(flow, trigger, actor, params, reason=reason)
        try:
            self.submit_plan(flow, run_id)
        except Exception as error:
            reason = redact(f"could not start: {error}")
            self._fail_unstarted(run_id, reason)
            raise RunNotStarted(run_id, reason) from error
        return run_id

    def submit_plan(self, flow: str, run_id: str) -> None:
        """Submit the task that plans the run. Doing it again changes nothing, whatever became of the task."""
        self.flow(flow)
        self._submit(
            TaskCall(
                "feather.plan",
                {"run_id": run_id, "flow": flow},
                f"plan:{run_id}",
                f"plan:{_TENANT}:{flow}",
                retry=self._deps.retry,
            ),
            run_id,
            "plan",
        )

    def _fail_unstarted(self, run_id: str, reason: str) -> None:
        """End a run whose plan never went out. If the database is what is away, the sweeper submits it later."""
        runs = self._deps.store.runs
        try:
            for row in runs.list_tasks(run_id, status="running"):
                runs.finish_task(row.task_id, "failed", reason)
            runs.set_run_status(run_id, "failed", reason)
        except Exception:
            log.exception("could not mark run %s failed", run_id)

    def verify_partition(self, dataset: str, partition: str) -> VerifyResult:
        """Read the partition from its source and say what differs from the last sealed version, changing nothing.

        Raises `KeyError` for an unknown dataset and `PermanentError` when the source can't be read as a run would.
        """
        found = self.dataset(dataset)
        if found.strategy.name == "push":
            raise PermanentError(
                f"dataset {dataset!r} is a push dataset: nothing can read it back to compare"
            )
        try:
            return run_extract(
                found, "verify", partition, routes=(), stage_only=False, deps=self._deps, dry_run=True
            )
        except ExtractFailed as error:
            raise PermanentError(str(error)) from error

    def active_runs(self, flow: str) -> list[Run]:
        """Return the flow's runs that are queued or running, oldest first. A stream run is not one that a tick can
        overlap, since it only holds the deliveries of versions that are already sealed."""
        runs = self._deps.store.runs
        found = [r for status in _ACTIVE for r in runs.list_runs(flow=flow, status=status, limit=_MANY)]
        return sorted((r for r in found if r.trigger != STREAM_TRIGGER), key=lambda r: r.run_id)

    def stream_run(self, flow: str, now: datetime) -> str:
        """Return the id of the flow's stream run of today, making it if need be. A poller's ticks and an event-driven
        flow's versions attach to it, so that a flow taking an event a second doesn't write a run a second.

        Its status is how it stands now. It is running while any of its tasks is, and between ticks it has settled:
        failed if a delivery is failed, or if the newest tick failed, and succeeded otherwise. It has no moment of
        ending. The day's turn is what closes it, as ticks and versions then go to the next day's run, and it keeps the
        status it last settled on once what it still holds has ended. "Today" is in the project's time zone.
        """
        day = now.astimezone(ZoneInfo(self._timezone)).date().isoformat()
        cached = self._stream_runs.get(flow)
        if cached is not None and cached[0] == day:
            return cached[1]
        run_id = self._deps.store.runs.stream_run(flow, day)
        self._stream_runs[flow] = (day, run_id)
        return run_id

    def start_stream_tick(self, flow: str, *, trigger_id: str, at: datetime) -> str:
        """Add a tick to the flow's stream run of the day of `at`, submit its plan, and return the run.

        The tick answers for itself: its failure to start, like its failure later on, ends this tick and not the run,
        and the next tick reads from the same watermark. Raises `FlowPaused` if the flow is paused, and `RunNotStarted`
        if the plan can't be submitted, having recorded that on the tick's row.
        """
        self.flow(flow)
        if self.is_flow_paused(flow):
            raise FlowPaused(f"flow {flow!r} is paused{self._pause_note(f'flow:{flow}')}")
        runs = self._deps.store.runs
        run_id = self.stream_run(flow, at)
        tick = f"{trigger_id}@{at.astimezone(UTC).isoformat()}"
        self._open_row(run_id, "plan", _tick_ref(run_id, tick, "plan"), None, None)
        runs.set_run_status(run_id, "running")
        try:
            self._engine.submit(self._tick_plan_call(flow, run_id, tick))
        except Exception as error:
            reason = redact(f"could not start: {error}")
            self._finish_task(_tick_ref(run_id, tick, "plan"), run_id, "failed", reason)
            raise RunNotStarted(run_id, reason) from error
        return run_id

    def skip_stream_tick(self, flow: str, *, trigger_id: str, at: datetime, reason: str) -> str:
        """Record a tick that doesn't go ahead as a skipped task of the day's stream run, and return the run (I6)."""
        self.flow(flow)
        runs = self._deps.store.runs
        run_id = self.stream_run(flow, at)
        tick = f"{trigger_id}@{at.astimezone(UTC).isoformat()}"
        task_id = runs.start_task(run_id, _SKIP, _tick_ref(run_id, tick, _SKIP), code_sha=self._deps.code_sha)
        runs.finish_task(task_id, "skipped", reason)
        return run_id

    def tick_in_flight(self, run_id: str) -> bool:
        """Say whether a tick of the stream run is still reading, which makes the next one overlap.

        Deliveries don't count: they wait in order, whatever the next tick reads. Since a tick that never ends would
        make every later one skip, this also looks after what it finds. A row whose task has ended fails, and the plan
        of a tick that may have been lost before its submit, as a crash could do, is submitted again under its key.
        """
        runs = self._deps.store.runs
        going = False
        for row in runs.list_tasks(run_id, status="running", kinds=_READING, limit=_RUNS_LOOKED_AT):
            if self._engine.has_ended(row.ref):
                self._finish_task(row.ref, run_id, "failed", f"its task {row.ref} ended without finishing it")
                continue
            going = True
            if row.kind == "plan" and self._clock.now() - row.started_at >= _LOST_AFTER:
                tick = row.ref.removeprefix(f"tick:{run_id}:").removesuffix(":plan")
                self._engine.submit(self._tick_plan_call(runs.get_run(run_id).flow, run_id, tick))
        return going

    def sweep_stream_runs(self) -> list[str]:
        """Look after what stream ticks leave behind, and return the ids of the staged versions discarded (S12).

        An earlier day's stream run that still says running gets no more ticks, so nothing else looks at it: its rows
        are checked as a tick would, and it settles once none runs. A staged version of a tick, while no task of its
        run reads, is one that a tick left when it ended without discarding it, and nothing would seal it.
        """
        runs = self._deps.store.runs
        today = self._clock.now().astimezone(ZoneInfo(self._timezone)).date().isoformat()
        for run in runs.list_runs(status="running", limit=_STREAM_RUNS_LOOKED_AT):
            if run.trigger != STREAM_TRIGGER or str(run.params.get("day", today)) >= today:
                continue
            self.tick_in_flight(run.run_id)
            self._settle_run(run.run_id)
        discarded: list[str] = []
        for version in self._deps.store.versions.stream_leftovers():
            if runs.list_tasks(version.run_id, status="running", kinds=_READING, limit=1):
                continue
            discard_abandoned(version.version_id, self._deps)
            discarded.append(version.version_id)
        return discarded

    def _tick_plan_call(self, flow: str, run_id: str, tick: str) -> TaskCall:
        return TaskCall(
            "feather.plan",
            {"run_id": run_id, "flow": flow, "tick": tick},
            _tick_ref(run_id, tick, "plan"),
            f"plan:{_TENANT}:{flow}",
            retry=self._deps.retry,
        )

    def cancel_run(self, run_id: str, *, actor: str, reason: str | None = None) -> None:
        """Cancel the run: its plan, extract and seal tasks stop, and the run ends `cancelled`, whatever follows.

        The deliveries of versions that were already sealed carry on, because sealed versions are the ledger's and must
        still reach their routes in order (I2). Cancelling a run that is over changes nothing.

        Raises `InvalidTransition` for a stream run, which holds a day of ticks and has no end to bring forward.
        """
        runs = self._deps.store.runs
        run = runs.get_run(run_id)
        if run.trigger == STREAM_TRIGGER:
            raise InvalidTransition(
                f"run {run_id} is the daily stream run of flow {run.flow!r} and can't be cancelled: pause the flow to "
                "stop its ticks. Its deliveries can each be retried or skipped"
            )
        if run.status not in _ACTIVE:
            return
        for row in runs.list_tasks(run_id, status="running", kinds=_READING):
            try:
                self._engine.cancel(row.ref)
            except KeyError:  # the row was opened and its task never submitted
                pass
            runs.finish_task(row.task_id, "cancelled", reason)
        runs.set_run_status(run_id, "cancelled", f"{actor}: {reason}" if reason else f"cancelled by {actor}")

    def retry(self, route: str, version_id: str, *, actor: str, reason: str | None = None) -> None:
        """Send a failed delivery back to be tried again, as a new task."""
        self._require(route, version_id, "failed", "retry")
        self._reopen(route, version_id, "pending", actor, reason)

    def replay(self, route: str, version_id: str, *, actor: str, reason: str | None = None) -> None:
        """Send an applied delivery back to be applied again, as a new task.

        Raises `InvalidTransition`, changing nothing, for each reason `replay_refusal` gives.
        """
        self._require(route, version_id, "applied", "replay")
        refusal = self.replay_refusal(route, version_id)
        if refusal is not None:
            raise InvalidTransition(refusal)
        self._reopen(route, version_id, "pending", actor, reason)

    def replay_refusal(self, route: str, version_id: str) -> str | None:
        """Say why the delivery may not replay now, or None if it may.

        Only the latest version of a stream that wasn't skipped replays. An older one would leave older data in the
        destination, which a later version that carries only changes would not correct (I2). A newer delivery of the
        route's partition that is still open also refuses it: its task could find the replay failed in front of it
        and end, using up its key. So does a version with nothing left to send (`replay_gap`).
        """
        delivery = self._deps.store.ledger.get(route, version_id)
        if (newer := self._newer_than(delivery)) is not None:
            return newer
        try:
            spec = self.route(route)
        except KeyError:  # a packaged route, whose code is on its host: the host checks what is left to send
            return None
        return replay_gap(spec, self._deps.store.versions.get(version_id), self._deps)

    def _newer_than(self, delivery: Delivery) -> str | None:
        """Say what, sealed after `delivery` in its route's partition, stops it from replaying."""
        route, version_id, partition = delivery.route, delivery.version_id, delivery.partition
        ledger = self._deps.store.ledger
        latest: Delivery | None = None
        page = ledger.list_deliveries(route=route, partition=partition, cursor=delivery_cursor(delivery))
        while page:
            for other in page:
                if other.status not in CLOSED:
                    return (
                        f"version {version_id} can't replay on route {route!r} while newer version "
                        f"{other.version_id} is {other.status} in partition {partition!r}: replay once that has "
                        "drained"
                    )
                if other.dataset == delivery.dataset and other.status != "skipped":
                    latest = other
            page = ledger.list_deliveries(route=route, partition=partition, cursor=delivery_cursor(page[-1]))
        if latest is None:
            return None
        return (
            f"version {version_id} is older than version {latest.version_id}, the latest of partition {partition!r}, "
            f"and replaying it would put older data in route {route!r}: replay {latest.version_id} instead"
        )

    def approve(self, route: str, version_id: str, *, actor: str, reason: str | None = None) -> None:
        """Approve a delivery held for approval: it applies as it is, as a new task. `InvalidTransition` unless held."""
        self._require(route, version_id, "held", "be approved")
        self._reopen(route, version_id, "pending", actor, f"{APPROVED}: {reason}" if reason else APPROVED)

    def reject(self, route: str, version_id: str, *, actor: str, reason: str | None = None) -> None:
        """Reject a delivery held for approval: it is skipped, and the versions behind it go ahead."""
        self._require(route, version_id, "held", "be rejected")
        self._reopen(route, version_id, "skipped", actor, f"rejected: {reason}" if reason else "rejected")

    def skip(self, route: str, version_id: str, *, actor: str, reason: str | None = None) -> None:
        """Give up on a failed delivery, so that the versions behind it can go ahead."""
        self._reopen(route, version_id, "skipped", actor, reason)

    def submit_apply(self, route: str, version_id: str) -> None:
        """Give the delivery a row, and submit its task if it is next in its stream.

        One that isn't next waits for the delivery ahead of it to submit it, since its task would end blocked and use
        up its key. Doing this again in the same state of the delivery changes nothing.
        """
        delivery = self._deps.store.ledger.get(route, version_id)
        run_id = self._deps.store.versions.get(version_id).run_id
        self._open_apply_row(delivery, run_id)
        next_up = self._next_of(delivery)
        if next_up is not None and next_up.version_id == version_id and not self.is_route_paused(route):
            self._engine.submit(self._apply_call(delivery, run_id))

    def fail_stranded(self, route: str, version_id: str) -> bool:
        """Fail the delivery if it is pending or running and its task has ended, and return whether it did.

        No task will finish such a delivery and its key can't run again, while `retry` and `skip` take only a failed
        one. A delivery whose task is queued, running or unknown to the engine is left alone.
        """
        deps = self._deps
        delivery = deps.store.ledger.get(route, version_id)
        if delivery.status not in ("pending", "running"):
            return False
        run_id = deps.store.versions.get(version_id).run_id
        ref = self._apply_ref(delivery, run_id)
        if not self._engine.has_ended(ref):
            return False
        why = f"its task {ref} ended without finishing it: retry or skip it"
        fail_open(delivery, why, deps)
        row = deps.store.runs.task_by_ref(run_id, ref)
        if row is not None and row.status == "running":
            deps.store.runs.finish_task(row.task_id, "failed", why)
        self._settle_run(run_id)
        return True

    def discard_abandoned(self, version_id: str) -> None:
        """Remove a staged version of a run that failed or was cancelled, with its chunks, which frees its day."""
        discard_abandoned(version_id, self._deps)

    def submit_seal(self, flow: str, dataset: str, run_id: str) -> None:
        """Submit the task that seals the run's slices of `dataset`."""
        self._submit(
            TaskCall(
                "feather.seal_run",
                {"run_id": run_id, "flow": flow, "dataset": dataset},
                f"seal:{run_id}:{dataset}",
                f"seal:{_TENANT}:{dataset}",
                retry=self._deps.retry,
            ),
            run_id,
            "seal_run",
            dataset,
        )

    def _require(self, route: str, version_id: str, status: DeliveryStatus, action: str) -> None:
        """Keep a retry from passing for a replay, or the other way round: the ledger allows both moves to pending."""
        current = self._deps.store.ledger.get(route, version_id).status
        if current != status:
            raise InvalidTransition(f"only a {status} delivery can {action}, and this one is {current}")

    def _reopen(
        self, route: str, version_id: str, to: DeliveryStatus, actor: str, reason: str | None
    ) -> None:
        """Move a delivery as an operator asked, and have the run answer for what follows instead of what failed."""
        deps = self._deps
        delivery = deps.store.ledger.transition(
            route, version_id, to, actor=actor, reason=reason, code_sha=deps.code_sha
        )
        run_id = deps.store.versions.get(version_id).run_id
        for row in deps.store.runs.tasks_by_ref(run_id, f"apply:{route}:{version_id}:"):
            if row.status == "failed":
                deps.store.runs.finish_task(row.task_id, "cancelled", row.error)
        if to == "pending":
            if deps.store.runs.get_run(run_id).status != "cancelled":  # a cancel stands (Stage 2 M11)
                deps.store.runs.set_run_status(run_id, "running")
            self.submit_apply(route, version_id)
        else:
            self._submit_next(delivery)
            self._settle_run(run_id)

    # The tasks.

    def _plan_task(self, ctx: TaskContext, args: Mapping[str, Any]) -> None:
        run_id = args["run_id"]
        tick = args.get("tick")

        def body() -> None:
            flow = self.flow(args["flow"])
            ctx.step("begin", lambda: self._deps.store.runs.set_run_status(run_id, "running"))
            check = self._deps.store.runs.get_run(run_id).params.get(VERIFY)
            if isinstance(check, Mapping):  # a question, not a read: it delivers nothing
                question = cast("Mapping[str, Any]", check)
                ctx.step("verify", lambda: self._answer_verify(run_id, question))
                return
            planned = ctx.step("plan", lambda: run_plan(flow, run_id, self._deps, tick))
            ctx.step("submit_extracts", lambda: self._submit_extracts(flow.name, run_id, planned, tick))
            self._finish_step(ctx, run_id)

        self._tracked(ctx, run_id, body)

    def _answer_verify(self, run_id: str, check: Mapping[str, Any]) -> None:
        """Check a partition for a verify run the server queued, and leave the counts, or why it couldn't, on the run.
        An error the source may recover from is raised, so the task is tried again as any read is."""
        runs = self._deps.store.runs
        dataset, partition = str(check.get("dataset")), str(check.get("partition"))
        try:
            self.dataset(dataset).partition.label_of(partition)
            found = self.verify_partition(dataset, partition)
        except (KeyError, ValueError, PermanentError) as error:
            message = (
                f"{partition!r} is not a partition of dataset {dataset!r}"
                if not isinstance(error, PermanentError)
                else str(error)
            )
            runs.set_run_result(run_id, {"error": message})
            runs.set_run_status(run_id, "failed", message)
            return
        runs.set_run_result(run_id, {"upserts": found.upserts, "deletes": found.deletes})
        runs.set_run_status(run_id, "succeeded")

    def _extract_task(self, ctx: TaskContext, args: Mapping[str, Any]) -> None:
        run_id, partition = args["run_id"], args["partition"]
        tick = args.get("tick")
        versions = self._deps.store.versions

        def hand_on() -> None:
            ctx.step("next_slice", lambda: self._submit_next_slice(ctx.task_ref, run_id))

        def body() -> None:
            flow, dataset = self.flow(args["flow"]), self.dataset(args["dataset"])
            sliced = ctx.step("sliced", lambda: bool(versions.slices_for_run(run_id, dataset.name)))
            extract = lambda: self._extract(flow, dataset, run_id, partition, sliced, tick)
            found = self._read_through_deferrals(ctx, dataset, extract)
            if not sliced:
                ctx.step("submit_applies", lambda: self._submit_applies(flow, run_id, found.version_ids))
            elif ctx.step(
                "finish_slice",
                lambda: versions.finish_slice(run_id, dataset.name, partition, found.writer),
            ):
                ctx.step("submit_seal", lambda: self.submit_seal(flow.name, dataset.name, run_id))
            hand_on()
            self._finish_step(ctx, run_id)

        def give_up(_: Exception) -> None:
            hand_on()
            if tick is not None:  # a stream run's status doesn't tell the sweeper that this tick is over
                ctx.step("discard", lambda: self._discard_tick(run_id, args["dataset"], args["tick"]))

        self._tracked(ctx, run_id, body, on_failure=give_up)

    def _seal_task(self, ctx: TaskContext, args: Mapping[str, Any]) -> None:
        run_id = args["run_id"]

        def body() -> None:
            flow, dataset = self.flow(args["flow"]), self.dataset(args["dataset"])
            ids = ctx.step("seal", lambda: self._seal(flow, dataset, run_id))
            ctx.step("submit_applies", lambda: self._submit_applies(flow, run_id, ids))
            self._finish_step(ctx, run_id)

        self._tracked(ctx, run_id, body)

    def _apply_task(self, ctx: TaskContext, args: Mapping[str, Any]) -> None:
        run_id, version_id = args["run_id"], args["version_id"]

        def body() -> None:
            route, dataset = self.route(args["route"]), self.dataset(args["dataset"])
            run_apply(ctx, dataset, route, version_id, self._deps)
            ctx.step("finish", lambda: self._finish_apply(ctx.task_ref, run_id, route.name, version_id))

        def close(error: Exception) -> None:
            message = str(error)
            ctx.step("close", lambda: self._close_delivery(args["route"], version_id, message))

        self._tracked(ctx, run_id, body, on_failure=close)

    def _tracked(
        self,
        ctx: TaskContext,
        run_id: str,
        body: Callable[[], None],
        on_failure: Callable[[Exception], None] | None = None,
    ) -> None:
        """Run a task's body, and answer for it in the journal if it is about to end in failure.

        `on_failure` runs once the failure is in the journal, for what the task owes whoever comes after it.
        """
        try:
            body()
        except PermanentError as error:
            self._fail_step(ctx, run_id, error)
            if on_failure is not None:
                on_failure(error)
            raise
        except Exception as error:
            if ctx.attempt >= self._deps.retry.max_attempts:
                self._fail_step(ctx, run_id, error)
                if on_failure is not None:
                    on_failure(error)
            else:
                self._note_retry(ctx, run_id, error)
            raise

    def _finish_step(self, ctx: TaskContext, run_id: str) -> None:
        ctx.step("finish", lambda: self._finish_task(ctx.task_ref, run_id, "succeeded"))

    def _note_retry(self, ctx: TaskContext, run_id: str, error: Exception) -> None:
        """Put in the journal that the attempt failed and the engine will run another, so that a retry is not silent.

        It is not a step: it sets the row to absolute values, so running it again on a replay changes nothing, and
        a step would be a new path in the journal of a body that failed before its first step.
        """
        attempt, message = ctx.attempt, redact(str(error)) or type(error).__name__
        log.warning(
            "attempt %d of %d failed and will be retried: %s", attempt, self._deps.retry.max_attempts, message
        )
        self._record_attempt(ctx.task_ref, run_id, attempt + 1, message)

    def _fail_step(self, ctx: TaskContext, run_id: str, error: Exception) -> None:
        message = redact(str(error))
        ctx.step("failed", lambda: self._finish_task(ctx.task_ref, run_id, "failed", message))

    def _read_through_deferrals[T](self, ctx: TaskContext, dataset: Dataset, read: Callable[[], T]) -> T:
        """Run a read as a step, sleeping off each budget deferral, or end it when waiting can't help (design 6.6).

        A deferral is a wait, not a failure: the sleep is durable, and uses up no attempt. But a read has no checkpoint,
        so it starts afresh after one and has to get every call it needs in one go. So each wait is twice the last, for
        the tokens to build up. A read that took all a window holds and wanted more
        can never finish, and neither can one that keeps failing after waiting that long (a read of low priority sees
        less than the window holds). Those end as a permanent failure that says why, instead of spending the budget for
        good. A read that was deferred having taken nothing has spent nothing: it just keeps waiting its turn. Waits stop
        doubling at the shortest window that holds what the read took (S23), so a read that fits it is never held for
        the longest window, nor given up on.
        """
        last = timedelta(0)
        deferrals = in_vain = 0
        while True:
            try:
                return ctx.step("extract", read)
            except BudgetDeferred as deferred:
                deferrals += 1
                windows = self._windows_step(ctx, deferrals, deferred.budget)
                holds = min((w.units for w in windows if w.seconds / w.units >= 60), default=None)
                longest = timedelta(seconds=max((w.seconds for w in windows), default=0))
                # S23: the waits double up to the shortest window that holds what the read took, not the longest: a
                # read that fits a minute's window has nothing to gain from waiting a day. Only a read that fits no
                # shorter window waits that long, and only waits at the longest count towards giving up.
                fits = [w.seconds for w in windows if w.units > deferred.taken]
                cap = timedelta(seconds=min(fits)) if fits else longest
                too_big = holds is not None and deferred.taken >= holds
                in_vain += deferred.taken > 0 and bool(windows) and cap >= longest and last >= longest
                if too_big or in_vain >= _READS_IN_VAIN:
                    size = (
                        f"holds {holds}" if too_big else f"gave too little after waiting {longest} each time"
                    )
                    raise PermanentError(
                        f"dataset {dataset.name!r}: the read took {deferred.taken} calls and needs more, but budget "
                        f"{deferred.budget!r} {size}. A read starts again after a deferral, so it needs all its calls "
                        "in one go: raise the budget's limit, or read less at a time"
                    ) from deferred
                wait = max(deferred.wait, min(last * 2, cap))
                log.warning(
                    "dataset %s: budget %s is used up after %d calls, and the read starts again in %s",
                    dataset.name,
                    deferred.budget,
                    deferred.taken,
                    wait,
                )
                ctx.sleep(wait)
                last = wait

    def _windows_step(self, ctx: TaskContext, deferral: int, budget: str) -> list[Window]:
        return ctx.step(f"budget_windows:{deferral}", lambda: self._windows_of(budget))

    def _windows_of(self, budget: str) -> list[Window]:
        """The windows of the budget. One the store doesn't know has none, and its wait is taken as it comes."""
        try:
            return [usage.window for usage in self._deps.store.budgets.usage(budget)]
        except KeyError:
            return []

    # What the steps do.

    def _discard_tick(self, run_id: str, dataset: str, tick: str) -> None:
        for version in self._deps.store.versions.staged_for_run(run_id, dataset, tick):
            discard_abandoned(version.version_id, self._deps)

    def _extract(
        self, flow: Flow, dataset: Dataset, run_id: str, partition: str, sliced: bool, tick: str | None
    ) -> _Extracted:
        writer = new_id()
        try:
            versions = run_extract(
                dataset,
                run_id,
                partition,
                routes=flow.routes,
                stage_only=sliced,
                deps=self._deps,
                writer=writer,
                tick=tick,
            )
        except ExtractFailed as error:  # the source would say the same again, so a retry can't help
            raise PermanentError(str(error)) from error
        return _Extracted([] if sliced else [version.version_id for version in versions], writer)

    def _seal(self, flow: Flow, dataset: Dataset, run_id: str) -> list[str]:
        try:
            versions = run_seal(dataset, run_id, routes=flow.routes, deps=self._deps)
        except ExtractFailed as error:
            raise PermanentError(str(error)) from error
        return [version.version_id for version in versions]

    @staticmethod
    def _extract_call(
        flow: str, run_id: str, dataset: str, partition: str, retry: RetryPolicy, tick: str | None = None
    ) -> TaskCall:
        args = {"run_id": run_id, "flow": flow, "dataset": dataset, "partition": partition}
        key = f"extract:{run_id}:{dataset}:{partition}"
        if tick is not None:
            args["tick"] = tick
            key = _tick_ref(run_id, tick, f"extract:{dataset}:{partition}")
        return TaskCall(
            "feather.extract",
            args,
            key,
            f"extract:{_TENANT}:{dataset}:{partition}",
            retry=retry,
        )

    def _submit_extracts(
        self, flow: str, run_id: str, planned: Mapping[str, Sequence[str]], tick: str | None = None
    ) -> None:
        """Open a row for every extract and submit them, or only the first `max_concurrent` of them.

        The rows of the rest are open without a task, as those of waiting deliveries are, which keeps the run going.
        Extract number `i` submits number `i + max_concurrent` when it ends, so a failed or retried task can neither
        lose a place nor take two.
        """
        limit = self._deps.store.runs.get_run(run_id).params.get("max_concurrent")
        every = [(dataset, partition) for dataset, partitions in planned.items() for partition in partitions]
        for place, (dataset, partition) in enumerate(every):
            call = self._extract_call(flow, run_id, dataset, partition, self._deps.retry, tick)
            if limit is None or place < limit:
                self._submit(call, run_id, "extract", dataset, partition)
            else:
                self._open_row(run_id, "extract", call.idempotency_key, dataset, partition)

    def _submit_next_slice(self, ref: str, run_id: str) -> None:
        """Submit the extract that waits for the place of the task `ref`, if the run is limited and still going."""
        runs = self._deps.store.runs
        run = runs.get_run(run_id)
        limit = run.params.get("max_concurrent")
        if limit is None or run.status not in _ACTIVE:
            return
        rows: list[TaskRow] = sorted(
            (r for r in runs.list_tasks(run_id) if r.kind == "extract"), key=lambda r: r.task_id
        )
        place = next(i for i, row in enumerate(rows) if row.ref == ref)
        if place + int(limit) < len(rows):
            waiting = rows[place + int(limit)]
            assert waiting.dataset is not None and waiting.partition is not None
            self._engine.submit(
                self._extract_call(run.flow, run_id, waiting.dataset, waiting.partition, self._deps.retry)
            )

    def _submit_applies(self, flow: Flow, run_id: str, version_ids: Sequence[str]) -> None:
        """Give the run a row for each new delivery, before any of them is submitted and can end the run."""
        ledger = self._deps.store.ledger
        deliveries = [ledger.get(route.name, v) for v in version_ids for route in flow.routes]
        for delivery in deliveries:
            self._open_apply_row(delivery, run_id)
        for delivery in deliveries:
            self.submit_apply(delivery.route, delivery.version_id)

    def _apply_ref(self, delivery: Delivery, run_id: str) -> str:
        """Return the key of the delivery's next apply task: `_apply_key`, and `#n` after it once `n` tasks of it ended
        because the route was paused, since an engine runs a key only once."""
        base = _apply_key(delivery)
        rows = self._deps.store.runs.tasks_by_ref(run_id, base)
        held = sum(
            1
            for r in rows
            if r.status == "cancelled" and r.error == _PAUSED and r.ref.partition("#")[0] == base
        )
        return f"{base}#{held}" if held else base

    def _submit_next(self, delivery: Delivery) -> None:
        """Submit what comes next in the delivery's stream, unless that is blocked."""
        next_up = self._next_of(delivery)
        if next_up is not None and next_up.status == "pending":
            self.submit_apply(next_up.route, next_up.version_id)

    def _next_of(self, delivery: Delivery) -> Delivery | None:
        return oldest_open(delivery.route, delivery.dataset, delivery.partition, self._deps)

    def _finish_apply(self, ref: str, run_id: str, route: str, version_id: str) -> None:
        """End an apply task by how its delivery stands: `run_apply` ends well even when the delivery failed."""
        delivery = self._deps.store.ledger.get(route, version_id)
        self._submit_next(delivery)
        if delivery.status in CLOSED:
            self._finish_task(ref, run_id, "succeeded")
            return
        if delivery.status == "pending" and self.is_route_paused(route):
            self._hold(ref, run_id, delivery)
            return
        next_up = self._next_of(delivery)
        if delivery.status == "pending" and next_up is not None and next_up.version_id == version_id:
            # The route was resumed after this task saw the pause. Its key is used up, so the delivery takes the next.
            self._hold(ref, run_id, delivery)
            self.submit_apply(route, version_id)
            return
        why = delivery.last_error or f"it is still {delivery.status}, behind an older version"
        self._finish_task(ref, run_id, "failed", f"route {route!r}, version {version_id}: {why}")

    def _close_delivery(self, route: str, version_id: str, error: str) -> None:
        """Fail what the ending task leaves open: the first open delivery of its stream, up to its own."""
        delivery = self._deps.store.ledger.get(route, version_id)
        next_up = self._next_of(delivery)
        if next_up is None or next_up.seal_seq > delivery.seal_seq:
            return
        if next_up.status in ("pending", "running"):
            fail_open(next_up, error, self._deps)

    def _hold(self, ref: str, run_id: str, delivery: Delivery) -> None:
        """End a task that found its route paused, and give its delivery a row to wait under, so that the run stays going."""
        self._deps.store.runs.finish_task(self._task_id(run_id, ref), "cancelled", _PAUSED)
        self._open_apply_row(delivery, run_id)

    def _finish_task(self, ref: str, run_id: str, status: str, error: str | None = None) -> None:
        self._deps.store.runs.finish_task(self._task_id(run_id, ref), status, error)
        self._settle_run(run_id)

    def _record_attempt(self, ref: str, run_id: str, attempt: int, error: str) -> None:
        self._deps.store.runs.record_attempt(self._task_id(run_id, ref), attempt, error)

    def _task_id(self, run_id: str, ref: str) -> str:
        row = self._deps.store.runs.task_by_ref(run_id, ref)
        if row is None:
            raise KeyError(f"run {run_id} has no task {ref}")
        return row.task_id

    def _settle_run(self, run_id: str) -> None:
        """Finish the run if none of its tasks is still running. It failed for the first reason any of them did."""
        runs = self._deps.store.runs
        run = runs.get_run(run_id)
        if run.status == "cancelled":
            return  # what the sealed versions still do is the ledger's business now
        if runs.list_tasks(run_id, status="running", limit=1):
            return
        if run.trigger == STREAM_TRIGGER:
            failed = self._stream_failure(run_id)
            if failed is None:  # a stream run gets over a failure, so what it said then must not stay
                runs.set_run_status(run_id, "succeeded", "")
            else:
                runs.set_run_status(run_id, "failed", failed.error)
            return
        found = runs.list_tasks(run_id, status="failed", limit=1)
        if found:
            runs.set_run_status(run_id, "failed", found[0].error)
        else:
            runs.set_run_status(run_id, "succeeded")

    def _stream_failure(self, run_id: str) -> TaskRow | None:
        """Return what makes a settled stream run failed: a failed delivery, which stays until someone acts, or else a
        failure of its newest tick. An older tick's failure is over, since the newer one read from the same watermark."""
        runs = self._deps.store.runs
        failed = runs.list_tasks(run_id, status="failed", kinds=("apply",), limit=1)
        if failed:
            return failed[0]
        newest = runs.list_tasks(run_id, kinds=("plan",), limit=1, newest_first=True)
        if not newest:
            return None
        tick = newest[0].ref.removesuffix("plan")
        return next((row for row in runs.tasks_by_ref(run_id, tick) if row.status == "failed"), None)

    def _submit(
        self,
        call: TaskCall,
        run_id: str,
        kind: str,
        dataset: str | None = None,
        partition: str | None = None,
    ) -> None:
        self._open_row(run_id, kind, call.idempotency_key, dataset, partition)
        self._engine.submit(call)

    def _open_apply_row(self, delivery: Delivery, run_id: str) -> None:
        self._open_row(
            run_id,
            "apply",
            self._apply_ref(delivery, run_id),
            delivery.dataset,
            delivery.partition,
            delivery.route,
        )

    def _open_row(
        self,
        run_id: str,
        kind: str,
        ref: str,
        dataset: str | None,
        partition: str | None,
        route: str | None = None,
    ) -> None:
        """Add the task's row to the run. `start_task` keeps a repeat from adding a second one."""
        self._deps.store.runs.start_task(
            run_id, kind, ref, dataset, partition, route, code_sha=self._deps.code_sha
        )

    def _apply_call(self, delivery: Delivery, run_id: str) -> TaskCall:
        return TaskCall(
            "feather.apply",
            {
                "run_id": run_id,
                "dataset": delivery.dataset,
                "partition": delivery.partition,
                "route": delivery.route,
                "version_id": delivery.version_id,
            },
            self._apply_ref(delivery, run_id),
            f"apply:{_TENANT}:{delivery.route}:{delivery.partition}",
            retry=self._deps.retry,
        )
