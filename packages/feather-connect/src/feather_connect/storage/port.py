"""The store ports: the run journal, versions, delivery ledger, record state and quarantine.

Every backend must pass `feather_connect.testing.store_contract.StoreContract`. The values here are frozen
dataclasses so that they pickle, which the engine needs for step results.
"""

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal, Protocol

Priority = Literal["low", "normal", "high"]
VersionStatus = Literal["staged", "sealed"]
DeliveryStatus = Literal["pending", "running", "applied", "failed", "held", "skipped", "compensated"]
QuarantineStatus = Literal["open", "superseded"]
WorkerState = Literal["alive", "draining", "dead"]


class VersionSealed(Exception):
    """A sealed version was staged again. Sealed versions never change (I1)."""


class StaleWriter(Exception):
    """An execution wrote to a version that a later execution has claimed. Only the last claim may write (I1)."""


class SliceFinished(Exception):
    """An execution claimed a slice whose run has marked it done: the read that finished it stands, for the seal task."""


class InvalidTransition(Exception):
    """A delivery was moved to a status that design section 6.5 doesn't allow from its current one."""


# The legal delivery transitions of design section 6.5. A status with no targets is final.
TRANSITIONS: Mapping[DeliveryStatus, frozenset[DeliveryStatus]] = {
    "pending": frozenset({"running"}),
    "running": frozenset({"applied", "pending", "failed", "held"}),
    "held": frozenset({"pending", "skipped"}),
    "failed": frozenset({"pending", "skipped"}),
    "applied": frozenset({"pending", "compensated"}),
    "skipped": frozenset(),
    "compensated": frozenset(),
}

# Statuses that no longer hold up the next version of a partition (I2).
CLOSED: frozenset[DeliveryStatus] = frozenset({"applied", "skipped", "compensated"})


def check_transition(frm: DeliveryStatus, to: DeliveryStatus) -> None:
    """Raise `InvalidTransition` unless a delivery may move from `frm` to `to`."""
    if to not in TRANSITIONS[frm]:
        raise InvalidTransition(f"a delivery cannot go from {frm} to {to}")


@dataclass(frozen=True)
class Run:
    """One run of a flow. `status` is one of queued, running, succeeded, failed, cancelled and skipped."""

    run_id: str
    flow: str
    trigger: str
    actor: str
    params: Mapping[str, Any]
    status: str
    reason: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class TaskRow:
    """One engine task of a run. `status` is running until `finish_task` sets succeeded, failed or cancelled."""

    task_id: str
    run_id: str
    kind: str
    ref: str
    dataset: str | None
    partition: str | None
    route: str | None
    worker: str | None
    code_sha: str | None
    status: str
    error: str | None
    started_at: datetime
    finished_at: datetime | None
    attempt: int = 1  # the attempt the task is on, or ended on: more than 1 means it was retried; `error` is the last failure


@dataclass(frozen=True)
class VersionDraft:
    """A version as the extract task describes it, before it is sealed."""

    version_id: str
    dataset: str
    partition: str
    run_id: str
    complete: bool
    upserts: int
    deletes: int
    content_hash: str
    schema_hash: str | None
    watermark: Any
    code_sha: str


@dataclass(frozen=True)
class Version(VersionDraft):
    """A draft plus where it stands. `seal_seq` and `sealed_at` are set when it is sealed."""

    status: VersionStatus = "staged"
    seal_seq: int | None = None
    sealed_at: datetime | None = None


@dataclass(frozen=True)
class Delivery:
    """The ledger row of one (route, version). `attempts` counts the times it started running."""

    route: str
    version_id: str
    dataset: str
    partition: str
    seal_seq: int
    status: DeliveryStatus
    attempts: int = 0
    last_error: str | None = None
    rows_applied: int = 0
    rows_rejected: int = 0
    replays: int = 0
    retries: int = 0


def delivery_cursor(delivery: Delivery) -> str:
    """The cursor that continues `Ledger.list_deliveries` after `delivery`."""
    return f"{delivery.seal_seq}:{delivery.route}"


@dataclass(frozen=True)
class Attempt:
    """One transition of a delivery, with who made it, why, and when."""

    route: str
    version_id: str
    from_status: DeliveryStatus
    to_status: DeliveryStatus
    actor: str
    reason: str | None
    error: str | None
    rows_applied: int
    rows_rejected: int
    code_sha: str | None
    at: datetime


@dataclass(frozen=True)
class StateRow:
    """What is known about one record: its partition, content hash and last payload."""

    key: str
    partition: str
    row_hash: int
    payload: Mapping[str, Any]
    last_version: str


@dataclass(frozen=True)
class QuarantineRow:
    """A record that was turned down.

    A sink rejected it, or for an entry with no `route`, extraction did. An extraction entry names its `dataset` and
    its `partition`. These and its `version_id` are None when the record had no partition.
    """

    route: str | None
    version_id: str | None
    key: str
    error: str
    status: QuarantineStatus
    dataset: str | None = None
    partition: str | None = None


class RunJournal(Protocol):
    """Runs and their tasks. Every trigger creates a run, including a skipped tick (I6)."""

    def create_run(
        self,
        flow: str,
        trigger: str,
        actor: str,
        params: Mapping[str, Any],
        status: str = "queued",
        reason: str | None = None,
    ) -> str: ...
    def set_run_status(self, run_id: str, status: str, reason: str | None = None) -> None:
        """Set the status, and the reason when one is given."""
        ...

    def get_run(self, run_id: str) -> Run: ...
    def list_runs(
        self, flow: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> list[Run]:
        """Newest first. `cursor` is the `run_id` of the last run of the previous page."""
        ...

    def start_task(
        self,
        run_id: str,
        kind: str,
        ref: str,
        dataset: str | None = None,
        partition: str | None = None,
        route: str | None = None,
        worker: str | None = None,
        code_sha: str | None = None,
    ) -> str:
        """Add a running task and return its id. A `(run_id, ref)` that exists keeps its row, whose id is returned."""
        ...

    def record_attempt(self, task_id: str, attempt: int, error: str) -> None:
        """Note that the task failed and goes on to attempt number `attempt`, with the error that ended the last one.
        `KeyError` for an unknown task."""
        ...

    def finish_task(self, task_id: str, status: str, error: str | None = None) -> None:
        """End the task. Without an `error`, the one `record_attempt` left stays."""
        ...

    def list_tasks(self, run_id: str) -> list[TaskRow]: ...
    def log(self, task_id: str, level: str, message: str) -> None: ...
    def task_logs(self, task_id: str) -> list[str]:
        """The task's log lines in order, each as `level: message`."""
        ...

    def delete_runs_before(self, cutoff: datetime) -> int:
        """Delete the runs last changed before `cutoff`, with their tasks, logs and slices; return how many.

        A run that is still going, or whose versions have a delivery that isn't closed, is kept.
        """
        ...

    def delete_logs_before(self, cutoff: datetime) -> int:
        """Delete the task log lines written before `cutoff`; return how many."""
        ...


class VersionStore(Protocol):
    """Versions, from staging to sealing, and the slices of a run."""

    def claim(self, version_id: str, writer: str, run_id: str, dataset: str, partition: str) -> None:
        """Make `writer` the only execution that may write this version, and forget the version staged before it.

        A task can run twice at once, and both copies use the same version id. The last claim wins: from here on the
        spool, `stage`, `seal`, `discard_staged` and `finish_slice` refuse any other writer with `StaleWriter`, so that a
        version is only ever sealed with the chunks of one execution.

        Raises `VersionSealed` for a sealed version, and `SliceFinished` when (`run_id`, `dataset`, `partition`) is a slice
        marked done, whose seal task may be working from it. Neither changes anything.
        """
        ...

    def take_over(self, version_ids: Sequence[str], writer: str) -> None:
        """Make `writer` the writer of staged versions, as they are, for a seal task to decide and seal them.

        Raises `VersionSealed`, changing nothing, if any of them is sealed.
        """
        ...

    def stage(self, draft: VersionDraft, writer: str | None = None) -> Version:
        """Store a draft. Staging a version again replaces it, until it is sealed; then `VersionSealed`.

        With a `writer`, raises `StaleWriter` unless it holds the claim.
        """
        ...

    def seal(
        self, version_ids: Sequence[str], routes: Sequence[str], writer: str | None = None
    ) -> list[Version]:
        """Seal each version, give it the next `seal_seq` and a pending delivery per route.

        Sealing a version that is already sealed changes nothing. With a `writer`, raises `StaleWriter`, sealing none,
        unless it holds the claim of each version that isn't sealed yet.
        """
        ...

    def discard_staged(self, version_id: str, writer: str | None = None) -> None:
        """Remove a staged version, which an extract found it had no need of. An unknown id changes nothing.

        Raises `VersionSealed` for a sealed version. With a `writer`, raises `StaleWriter` unless it holds the claim.
        """
        ...

    def get(self, version_id: str) -> Version:
        """Raises `KeyError` when there is no such version."""
        ...

    def latest_sealed(
        self, dataset: str, partition: str, before_seal_seq: int | None = None
    ) -> Version | None:
        """The partition's newest sealed version, or its newest sealed before `before_seal_seq` when given."""
        ...

    def since_watermark(self, dataset: str, partition: str, since: Any) -> list[Version]:
        """Sealed versions whose stored watermark is `>= since`, newest first. A None watermark never matches."""
        ...

    def latest_watermark(self, dataset: str) -> Any | None:
        """The greatest watermark of the dataset's sealed versions."""
        ...

    def register_slices(self, run_id: str, dataset: str, partitions: Iterable[str]) -> None: ...
    def finish_slice(self, run_id: str, dataset: str, partition: str, writer: str | None = None) -> bool:
        """Mark a slice done. True when every registered slice of the run's dataset is done. Safe to repeat.

        With a `writer`, raises `StaleWriter` unless the slice is done already or it holds the claim of the slice's version.
        """
        ...

    def slices_for_run(self, run_id: str, dataset: str) -> list[str]:
        """The partitions given to `register_slices` for the run's dataset, finished or not, each once."""
        ...

    def staged_for_run(self, run_id: str, dataset: str) -> list[Version]: ...
    def sealed_for_run(self, run_id: str, dataset: str) -> list[Version]:
        """All of the run's sealed versions of the dataset, in seal order."""
        ...

    def list_versions(self, dataset: str, partition: str | None = None, limit: int = 100) -> list[Version]:
        """Newest first."""
        ...

    def get_many(self, version_ids: Iterable[str]) -> list[Version]:
        """The versions that exist, in no particular order; an unknown id is left out."""
        ...

    def runs_ready_to_seal(self) -> list[tuple[str, str]]:
        """The `(run_id, dataset)` pairs whose slices are all finished and that still have a staged version.

        Their seal task has not run to the end: it was never submitted, or it was lost.
        """
        ...

    def sealed_before(self, cutoff: datetime, limit: int = 1000) -> list[Version]:
        """Sealed versions sealed before `cutoff` that retention may forget, oldest first.

        Not the newest sealed version of its (dataset, partition), and none that still has a delivery.
        """
        ...

    def delete_many(self, version_ids: Iterable[str]) -> int:
        """Delete the sealed versions' manifests, which `sealed_before` listed; return how many existed."""
        ...


class Ledger(Protocol):
    """One delivery per (route, version), moved only along the transitions of design section 6.5."""

    def oldest_open(self, route: str, partition: str) -> Delivery | None:
        """The lowest `seal_seq` delivery of the partition that is not applied, skipped or compensated (I2)."""
        ...

    def get(self, route: str, version_id: str) -> Delivery: ...
    def transition(
        self,
        route: str,
        version_id: str,
        to: DeliveryStatus,
        *,
        actor: str,
        reason: str | None = None,
        error: str | None = None,
        rows_applied: int = 0,
        rows_rejected: int = 0,
        code_sha: str | None = None,
    ) -> Delivery:
        """Move a delivery and write an attempt. `InvalidTransition` if the move is illegal (I3).

        Going to `applied` sets the row counts and clears `last_error`; any other move sets a count only
        when it is not zero, and `last_error` only when `error` is given.
        """
        ...

    def attempts(self, route: str, version_id: str) -> list[Attempt]: ...
    def list_deliveries(
        self,
        route: str | None = None,
        status: DeliveryStatus | None = None,
        partition: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> list[Delivery]:
        """In seal order. Pass `delivery_cursor(last)` as `cursor` to continue after `last`."""
        ...

    def all_applied(self) -> bool:
        """True when every delivery is applied, which includes there being none."""
        ...

    def pending_older_than(
        self, age: timedelta, *, now: datetime | None = None, limit: int = 1000
    ) -> list[Delivery]:
        """The pending deliveries that last changed more than `age` before `now` (the store's clock by default).

        In seal order. A delivery that is pending has nothing running it, or a task waiting out a retry delay.
        """
        ...

    def open_versions(self, version_ids: Iterable[str]) -> set[str]:
        """Those of `version_ids` with a delivery that isn't applied, skipped or compensated."""
        ...

    def delete_closed_before(self, cutoff: datetime) -> int:
        """Delete the applied, skipped and compensated deliveries last changed before `cutoff`, with their attempts.

        Return how many deliveries went.
        """
        ...


class RecordStateStore(Protocol):
    """The last known state of each record of a dataset, by key."""

    def get_many(self, dataset: str, keys: Iterable[str]) -> dict[str, StateRow]:
        """The rows that exist; a key with no row is left out."""
        ...

    def put_many(self, dataset: str, rows: Iterable[StateRow]) -> None: ...
    def delete_many(self, dataset: str, keys: Iterable[str]) -> None: ...


class QuarantineStore(Protocol):
    """Records a sink rejected, kept until a later version of the record is delivered."""

    def add(
        self,
        route: str | None,
        version_id: str | None,
        key: str,
        error: str,
        dataset: str | None = None,
        partition: str | None = None,
    ) -> None:
        """Adding the same (route, version, key, dataset) again replaces the entry and reopens it."""
        ...

    def open_extraction_keys(self, dataset: str, keys: Iterable[str], outside: str | None = None) -> set[str]:
        """Those of `keys` with an open extraction entry in the dataset, from a partition other than `outside`."""
        ...

    def close_extraction(self, dataset: str, keys: Iterable[str]) -> int:
        """Close the dataset's open extraction entries for `keys`. Returns how many it closed."""
        ...

    def supersede(self, route: str, key: str, before_seal_seq: int) -> int:
        """Close the route's open entries for `key` from versions sealed before `before_seal_seq`.

        Returns how many it closed.
        """
        ...

    def list_open(self, route: str | None = None) -> list[QuarantineRow]: ...


@dataclass(frozen=True)
class Window:
    """A limit of `units` per `seconds`."""

    units: int
    seconds: int


@dataclass(frozen=True)
class BudgetSpec:
    """A named rate budget: it is used up when any of its windows is. `reserve` maps a priority to the share of
    each window kept for it: `{"high": 0.6}` leaves 60% of every window to `high` requests alone."""

    name: str
    windows: tuple[Window, ...]
    reserve: Mapping[str, float] = field(default_factory=dict[str, float])


@dataclass(frozen=True)
class Grant:
    """The answer to an `acquire`. When it is refused, `wait` is how long until the same request would be granted."""

    granted: bool
    wait: timedelta = timedelta(0)


@dataclass(frozen=True)
class WindowUsage:
    """How many units of `window` are in use now, after refill."""

    window: Window
    used: float


_PERIODS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}


def parse_limits(limits: Sequence[str]) -> tuple[Window, ...]:
    """Read limits such as `"90/minute"` into windows. Raises `ValueError` for one that isn't of that form."""
    windows: list[Window] = []
    for limit in limits:
        count, _, period = limit.partition("/")
        if not count.strip().isdigit() or int(count) < 1 or period.strip() not in _PERIODS:
            raise ValueError(f"{limit!r} is not a limit: write it as units/period, such as '90/minute'")
        windows.append(Window(int(count), _PERIODS[period.strip()]))
    return tuple(windows)


class BudgetStore(Protocol):
    """Shared rate budgets. Each window refills continuously, as a token bucket, by the store's clock."""

    def define(self, spec: BudgetSpec) -> None:
        """Create the budget, full. Defining it again with the same spec changes nothing; with another, starts afresh."""
        ...

    def acquire(self, name: str, units: int, priority: Priority = "normal") -> Grant:
        """Take `units` from every window of the budget, or from none of them.

        `low` may use only `1 - reserve["high"]` of each window. Raises `KeyError` for an undefined budget, and
        `ValueError` when the request could never be granted, because it is more than a window can hold.
        """
        ...

    def usage(self, name: str) -> list[WindowUsage]: ...


@dataclass(frozen=True)
class FlowRow:
    """A flow as the catalogue knows it for one code version. `spec` is `Flow.spec_dict()`."""

    name: str
    project: str
    code_sha: str
    spec: Mapping[str, Any]
    registered_at: datetime


@dataclass(frozen=True)
class RouteRow:
    """A route of `flow` as the catalogue knows it for one code version. `spec` is its entry in the flow's."""

    name: str
    flow: str
    code_sha: str
    spec: Mapping[str, Any]
    registered_at: datetime


@dataclass(frozen=True)
class WorkerRow:
    """A worker and its latest heartbeat. `state` is alive, draining (finishing its work) or dead."""

    worker_id: str
    labels: Mapping[str, str]
    code_sha: str
    slots: int
    last_seen: datetime
    state: WorkerState


class CatalogStore(Protocol):
    """The flows and routes registered from code. A row is kept for each code SHA (design 6.11)."""

    def sync(self, project: str, code_sha: str, specs: Mapping[str, Any]) -> None:
        """Register the flows of `project` at `code_sha`; `specs` maps each flow name to its `Flow.spec_dict()`.

        Syncing a SHA again replaces its rows and makes them the latest. A flow the project had before and `specs`
        lacks is retired, along with its routes, until a sync brings it back.
        """
        ...

    def flows(self) -> list[FlowRow]:
        """Return the latest registration of each flow that isn't retired, by name."""
        ...

    def flow(self, name: str, code_sha: str | None = None) -> FlowRow:
        """Return the flow at `code_sha`, or its latest registration. Raises `KeyError` if there is none."""
        ...

    def routes(self) -> list[RouteRow]:
        """Return the latest registration of each route that isn't retired, by name."""
        ...


class WorkerRegistry(Protocol):
    """The workers that are connected, from their heartbeats (design 6.12)."""

    def heartbeat(self, worker_id: str, labels: Mapping[str, str], code_sha: str, slots: int) -> None:
        """Record that the worker is there now. A draining worker stays draining, and a dead one is alive again."""
        ...

    def list_workers(self) -> list[WorkerRow]:
        """Return every worker, by id."""
        ...

    def stale(self, older_than: timedelta) -> list[WorkerRow]:
        """Return the workers not marked dead whose last heartbeat is more than `older_than` ago, by id."""
        ...

    def mark(self, worker_id: str, state: WorkerState) -> None:
        """Set the worker's state. Raises `KeyError` for a worker that never sent a heartbeat."""
        ...


@dataclass(frozen=True)
class Override:
    """An operator's setting on `target` (`flow:name` or `route:name`) that stands over the code. In M1 the only `key`
    is `paused`."""

    target: str
    key: str
    value: Any
    reason: str | None
    created_by: str
    created_at: datetime
    expires_at: datetime | None


class OverrideStore(Protocol):
    """Settings made at run time. One that has expired is gone: nothing reads it, though it may still be stored."""

    def set(
        self,
        target: str,
        key: str,
        value: Any,
        *,
        actor: str,
        reason: str | None = None,
        expires_at: datetime | None = None,
    ) -> None:
        """Set the override, replacing one that is there."""
        ...

    def get(self, target: str, key: str) -> Any | None:
        """The value, or None when there is none or it has expired."""
        ...

    def claim(
        self,
        target: str,
        key: str,
        value: Any,
        *,
        actor: str,
        reason: str | None = None,
        expires_at: datetime | None = None,
    ) -> bool:
        """Set the override only if none is in force, and say whether it was set. Two callers claiming the same
        target and key at once cannot both win."""
        ...

    def list_all(self) -> list[Override]:
        """The overrides in force, by target and key."""
        ...

    def clear(self, target: str, key: str, *, actor: str, reason: str | None = None) -> None:
        """Remove the override. Nothing happens when there is none."""
        ...


def is_paused(overrides: OverrideStore, target: str) -> bool:
    """Whether `target` (`flow:name` or `route:name`) is paused now."""
    return bool(overrides.get(target, "paused"))


class ScheduleStore(Protocol):
    """When each trigger of each flow was last handled, so that a restart neither fires a tick twice nor loses it."""

    def last_tick(self, flow: str, trigger_id: str) -> datetime | None:
        """The time of the last tick handled, or None if the trigger has never been seen."""
        ...

    def set_last_tick(self, flow: str, trigger_id: str, at: datetime) -> None: ...


@dataclass(frozen=True)
class PartitionRow:
    """A partition of a dataset as its latest sealed version left it: `cells` maps each route to the status of that
    version's delivery there."""

    dataset: str
    partition: str
    version_id: str
    seal_seq: int
    sealed_at: datetime
    upserts: int
    deletes: int
    cells: Mapping[str, str]


@dataclass(frozen=True)
class FailureGroup:
    """Failed deliveries of one route that failed with the same error. `versions` holds up to five of them."""

    route: str
    error_class: str
    message: str
    count: int
    versions: tuple[str, ...]


DEFAULT_ERROR_CLASS = (
    "PermanentError"  # a delivery fails only on a permanent error, which stores its message alone
)
_CLASSED = re.compile(r"([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception)): (.*)", re.DOTALL)


def split_error(error: str | None) -> tuple[str, str]:
    """Split a delivery's stored error into its class and its message. The class is the name before the first colon
    when that looks like an exception, and `PermanentError` when it does not."""
    text = error or ""
    found = _CLASSED.fullmatch(text)
    return (found.group(1), found.group(2)) if found else (DEFAULT_ERROR_CLASS, text)


def failure_cursor(group: FailureGroup) -> str:
    """The cursor that continues `ReadStore.failure_groups` after `group`."""
    return json.dumps([group.route, group.error_class, group.message])


class ReadStore(Protocol):
    """Queries for the dashboard that join the ledger to versions."""

    def partition_grid(
        self, dataset: str, *, since: datetime | None = None, cursor: str | None = None, limit: int = 100
    ) -> list[PartitionRow]:
        """The dataset's partitions that have a sealed version, by partition name, last first. `since` keeps those
        whose latest version was sealed then or later. `cursor` is the `partition` of the last row of the page
        before."""
        ...

    def failure_groups(
        self, route: str | None = None, *, limit: int = 100, cursor: str | None = None
    ) -> list[FailureGroup]:
        """Failed deliveries grouped by route, error class and message, in that order. Pass `failure_cursor(last)`
        as `cursor` to continue after `last`."""
        ...

    def delivery_counts(self) -> dict[str, int]:
        """How many deliveries there are in each status that has any."""
        ...

    def retried_tasks(self, run_ids: Sequence[str]) -> dict[str, int]:
        """For each of the runs, how many of its tasks went beyond their first attempt. Runs with none are left out."""
        ...

    def waiting(self, route: str, partitions: Sequence[str]) -> dict[tuple[str, str], int]:
        """For each (dataset, partition) of `partitions` on the route, how many deliveries are pending or held. Those
        with none are left out."""
        ...


@dataclass(frozen=True)
class UserRow:
    user_id: str
    username: str
    password_hash: str
    created_at: datetime


@dataclass(frozen=True)
class TokenRow:
    """An API token as stored: the SHA-256 of the secret (hex), never the secret itself."""

    token_id: str
    name: str
    token_hash: str
    created_at: datetime
    expires_at: datetime | None
    revoked_at: datetime | None


class DuplicateUser(Exception):
    """A user with that name exists already."""


class UserStore(Protocol):
    """Local users and the API tokens they and automation use."""

    def create_user(self, username: str, password_hash: str) -> str:
        """Add the user and return the id. Raises `DuplicateUser` when the name is taken."""
        ...

    def get_user(self, username: str) -> UserRow | None: ...
    def set_password_hash(self, username: str, password_hash: str) -> None:
        """Give the user a new password hash. Raises `KeyError` when there is no such user."""
        ...

    def create_token(self, name: str, token_hash: str, expires_at: datetime | None) -> str:
        """Add the token, known by the SHA-256 of its secret, and return its id."""
        ...

    def find_token(self, token_hash: str) -> TokenRow | None:
        """The token with that hash, expired or revoked or not: the caller judges that."""
        ...

    def revoke_token(self, token_id: str) -> None:
        """Revoke the token, from now. Revoking one twice, or one that isn't there, changes nothing."""
        ...


@dataclass(frozen=True)
class AuditRow:
    audit_id: str
    at: datetime
    actor_type: str  # human, token or system
    actor: str
    action: str
    target: str | None
    request: Mapping[str, Any]  # what was asked, with the reason
    result: str


class AuditStore(Protocol):
    """Who did what. Rows are only added."""

    def record(
        self,
        actor_type: str,
        actor: str,
        action: str,
        target: str | None,
        request: Mapping[str, Any],
        result: str,
    ) -> str:
        """Add a row, stamped now, and return its id."""
        ...

    def list_audit(self, limit: int = 100, cursor: str | None = None) -> list[AuditRow]:
        """Newest first. `cursor` is the `audit_id` of the last row of the previous page."""
        ...


@dataclass(frozen=True)
class Store:
    """The ports together. A backend provides one of these."""

    runs: RunJournal
    versions: VersionStore
    ledger: Ledger
    state: RecordStateStore
    quarantine: QuarantineStore
    budgets: BudgetStore
    catalog: CatalogStore
    workers: WorkerRegistry
    schedules: ScheduleStore
    overrides: OverrideStore
    users: UserStore
    audit: AuditStore
    reads: ReadStore
