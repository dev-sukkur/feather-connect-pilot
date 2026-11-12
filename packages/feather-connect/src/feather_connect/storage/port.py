"""The store ports: the run journal, versions, delivery ledger, record state and quarantine.

Every backend must pass `feather_connect.testing.store_contract.StoreContract`. The values here are frozen
dataclasses so that they pickle, which the engine needs for step results.
"""

import json
import re
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Protocol

from packaging.utils import canonicalize_version

if TYPE_CHECKING:  # only as a type: the connector module reaches this one through the context's budget handle
    from feather_connect.core.connector import InboundRequest

Priority = Literal["low", "normal", "high"]
VersionStatus = Literal["staged", "sealed"]
DeliveryStatus = Literal["pending", "running", "applied", "failed", "held", "skipped", "compensated"]
QuarantineStatus = Literal["open", "superseded"]
WorkerState = Literal["alive", "draining", "dead"]
Role = Literal["admin", "developer", "operator", "viewer"]
ROLES: tuple[Role, ...] = ("admin", "developer", "operator", "viewer")  # highest first (design §13.2)
AccessGrant = Literal["approver", "pii-reader"]  # add-on grants beside a role: deciding holds, unmasking PII
GRANTS: tuple[AccessGrant, ...] = ("approver", "pii-reader")
TokenScope = Literal[
    "all", "publish"
]  # `publish`: it may upload to the built-in registry and do nothing else


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
STREAM_TRIGGER = (
    "stream"  # the trigger of a flow's daily run, which a poller's ticks and a push dataset's versions join
)
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
    result: Mapping[str, Any] | None = None  # what a run that answers a question found


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
    tick: str | None = (
        None  # of a daily stream run: the tick that read it, as its run holds many ticks' versions
    )


@dataclass(frozen=True)
class Version(VersionDraft):
    """A draft plus where it stands. `seal_seq` and `sealed_at` are set when it is sealed."""

    status: VersionStatus = "staged"
    seal_seq: int | None = None
    sealed_at: datetime | None = None


@dataclass(frozen=True)
class KeySighting:
    """One sealed version that held a record, from the key index that sealing writes (ruling 11).

    `op` is the spool's, upsert or delete; whether an upsert was an insert or an update is for the reader to say from
    the sightings before it. `chunk_no` is the data chunk that holds the record, and `row_hash` the hash of its payload.
    """

    version_id: str
    dataset: str
    partition: str
    key: str
    op: str
    row_hash: int
    chunk_no: int
    run_id: str
    seal_seq: int
    sealed_at: datetime


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
    payload: Mapping[str, Any] | None = None
    raw: str | None = None


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

    def set_run_result(self, run_id: str, result: Mapping[str, Any]) -> None:
        """Keep what the run found, such as a partition check's counts."""
        ...

    def get_run(self, run_id: str) -> Run: ...
    def list_runs(
        self, flow: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> list[Run]:
        """Newest first. `cursor` is the `run_id` of the last run of the previous page."""
        ...

    def tagged_versions(self, statuses: Sequence[str] = ("queued", "running")) -> set[tuple[str, str]]:
        """The package versions that runs with one of `statuses` are tagged with (`params.package` and
        `params.version`, as the catalogue's scheduler writes them), however many runs there are (flow packages §7.1)."""
        ...

    def queued_of(self, package: str, version: str, limit: int = 50) -> list[Run]:
        """The queued runs tagged with the package version, oldest first."""
        ...

    def count_runs(self, status: str | None = None, since: datetime | None = None) -> int:
        """How many runs have `status` (any, when None) and were created at `since` or later (any time, when None)."""
        ...

    def runs_by_day(self, since: datetime) -> dict[date, tuple[int, int]]:
        """For each UTC day with runs created at `since` or later: how many were created, and how many of those
        failed. A day with none is left out."""
        ...

    def find_runs(self, prefix: str, limit: int = 5) -> list[Run]:
        """The newest runs whose id starts with `prefix` (taken as upper case, as ids are), at most `limit`."""
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

    def stream_run(self, flow: str, day: str) -> str:
        """Return the flow's daily stream run of `day`, an ISO date, making it if there is none. Racing callers get the
        same run. A new one has trigger `stream` and status `succeeded`, having nothing to do yet."""
        ...

    def list_tasks(
        self,
        run_id: str,
        *,
        status: str | None = None,
        kinds: Sequence[str] | None = None,
        limit: int | None = None,
        after_task: str | None = None,
        newest_first: bool = False,
    ) -> list[TaskRow]:
        """The run's tasks in the order they were added, or the other way round with `newest_first`.

        A run can hold thousands of tasks, so whatever runs per task or per tick asks for what it needs here: its cost
        follows what it gets back. `after_task` is the id of the last task of the previous page.
        """
        ...

    def get_task(self, task_id: str) -> TaskRow:
        """Raises `KeyError` when there is no such task."""
        ...

    def task_by_ref(self, run_id: str, ref: str) -> TaskRow | None: ...
    def tasks_by_ref(self, run_id: str, prefix: str) -> list[TaskRow]:
        """The run's tasks whose `ref` starts with `prefix`, in the order they were added."""
        ...

    def count_tasks(self, run_id: str) -> Mapping[tuple[str, str], int]:
        """How many tasks the run has of each (kind, status)."""
        ...

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
        """Sealed versions whose stored watermark is `>= since`, newest first, up to the first one below `since`. A
        None watermark never matches.

        Stopping there keeps a tick's read to the lookback rather than the dataset's history. It loses nothing while
        the lookback stays the same: a read from `since` stamps its version with at least `since`, and `since` only
        grows, so a version below it is older than the lookback, and so is any version before it."""
        ...

    def key_history(self, dataset: str, key: str, limit: int = 100) -> list[KeySighting]:
        """The sealed versions that held `key`, oldest first: the newest `limit` of them. Read from the key index, which
        sealing writes in its own transaction; a version sealed before the index came in is not in it."""
        ...

    def keys_page(
        self,
        dataset: str,
        version_ids: Sequence[str],
        *,
        after: tuple[str, str] | None = None,
        prefix: str | None = None,
        limit: int = 50,
    ) -> list[KeySighting]:
        """A page of the keys that `version_ids` hold, in the key index, by key and then version in code point order:
        the first `limit` after (`key`, `version_id`) `after`, of those starting with `prefix` if given. Each version's
        keys are read from where the page starts, never all of them."""
        ...

    def earlier_sightings(
        self, dataset: str, keys: Sequence[str], before_seal_seq: int
    ) -> dict[str, KeySighting]:
        """For each of `keys` held by a version sealed before `before_seal_seq`, the newest such sighting."""
        ...

    def last_sealed_at(self, dataset: str) -> datetime | None:
        """When the dataset's newest sealed version, by staging, was sealed, in any partition; None if none is."""
        ...

    def last_sealed_many(self, datasets: Sequence[str]) -> dict[str, datetime]:
        """`last_sealed_at` of each of `datasets` in one read. A dataset with no sealed version is left out."""
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

    def staged_for_run(self, run_id: str, dataset: str, tick: str | None = None) -> list[Version]:
        """The run's staged versions of `dataset`, or with `tick` only that tick's."""
        ...

    def newest_sealed_for_run(self, run_id: str, dataset: str, limit: int) -> list[Version]:
        """The newest `limit` sealed versions of the run and dataset, newest first."""
        ...

    def sealed_for_run(self, run_id: str, dataset: str, tick: str | None = None) -> list[Version]:
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

        Their seal task has not run to the end: it was never submitted, or it was lost. A run that failed or was
        cancelled is not one: nothing will seal it (see `abandoned`).
        """
        ...

    def abandoned(self, limit: int = 1000) -> list[Version]:
        """Staged versions of runs that failed or were cancelled, which nothing will seal, oldest staged first."""
        ...

    def stream_leftovers(self, limit: int = 1000) -> list[Version]:
        """Staged versions of stream runs' ticks, oldest staged first. A tick seals its own, so one that is staged while
        no task of its run reads is a leftover of a tick that ended without discarding it (S12)."""
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

    def latest_before(
        self,
        route: str,
        partition: str,
        dataset: str,
        before_seq: int,
        status: DeliveryStatus | None = "applied",
    ) -> Delivery | None:
        """The latest delivery of `dataset` on the route's partition sealed before `before_seq`, of `status` unless
        that is None, or None. The schema policy compares with the last applied one (M2 plan 2 D1), and a route's
        first delivery in a partition is its bootstrap (D2)."""
        ...

    def delivered_routes(self, dataset: str, routes: Sequence[str]) -> set[str]:
        """Those of `routes` that have a delivery of `dataset`, of any status (M2 plan 2 D2)."""
        ...

    def has_deliveries(self, dataset: str) -> bool:
        """Whether any route has a delivery of `dataset`: whether it has a sealed history to bootstrap from."""
        ...

    def count_by_status(
        self, routes: Sequence[str], statuses: Sequence[DeliveryStatus]
    ) -> dict[tuple[str, DeliveryStatus], int]:
        """How many deliveries of each of `routes` have each of `statuses`, in one read. A pair with none is left
        out."""
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
        *,
        payload: Mapping[str, Any] | None = None,
        raw: str | None = None,
    ) -> None:
        """Adding the same (route, version, key, dataset) again replaces the entry and reopens it.

        `payload` is the record as the sink got it, and `raw` the text of a row its source could not read, so that
        the entry can be looked at and retried (ruling 11). Both are redacted before they get here."""
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

    def list_open(
        self, route: str | None = None, *, limit: int | None = None, offset: int = 0
    ) -> list[QuarantineRow]:
        """The open entries, oldest first: `limit` of them from `offset` on, or all."""
        ...

    def count_open(self, route: str | None = None) -> int:
        """How many entries are open, without reading them."""
        ...

    def of_versions(
        self,
        version_ids: Sequence[str],
        route: str | None,
        keys: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[QuarantineRow]:
        """The entries, open or closed, oldest first, from `version_ids`: those `route` made, or with `route` None those
        extraction made. With `keys`, only theirs. The first `limit` of them."""
        ...

    def of_key(self, dataset: str, key: str, version_ids: Sequence[str]) -> list[QuarantineRow]:
        """The entries of `key`, open or closed, oldest first: those a route made from one of `version_ids`, and those
        extraction made in `dataset`."""
        ...


@dataclass(frozen=True)
class ResponseRow:
    """What a destination answered for one record of a delivered chunk (ruling 5).

    `status` is the HTTP status, and `fields` the fields that the sink's `response_map` took from the answer, never
    the raw body or headers. `sent` is the body that a rejected record went with, so that it can be looked at and
    retried; it is None for a record the destination took. `at` is when the store recorded it.
    """

    route: str
    version_id: str
    chunk_no: int
    key: str
    status: int
    fields: Mapping[str, Any]
    sent: Mapping[str, Any] | None = None
    at: datetime | None = None


class ResponseStore(Protocol):
    """The answers of destinations, per record of a delivery. They go when their delivery goes (`delete_closed_before`)."""

    def record(self, rows: Sequence[ResponseRow]) -> None:
        """Keep the rows, stamped with the store's time. A row for the same route, version, chunk and key replaces the
        one before, so a chunk sent again keeps its latest answers, except that a row with no fields keeps the fields
        of the one before (a resend answered "applied already" often says nothing more)."""
        ...

    def replace_chunk(self, route: str, version_id: str, chunk_no: int, rows: Sequence[ResponseRow]) -> None:
        """Keep the rows of a chunk that was sent in full, as `record` does, and drop the chunk's rows of records
        that are not among them, so that the chunk shows the answers of its latest send only."""
        ...

    def of_delivery(self, route: str, version_id: str, limit: int = 1000) -> list[ResponseRow]:
        """The rows of one delivery, by chunk and then key, at most `limit` of them."""
        ...

    def for_keys(self, route: str, version_id: str, keys: Sequence[str]) -> dict[str, ResponseRow]:
        """The latest row of each of `keys` in a delivery that has one, as `find` gives it."""
        ...

    def find(self, route: str, version_id: str, key: str) -> ResponseRow | None:
        """The latest row of a record in a delivery: of its highest chunk, if the record was in several."""
        ...


@dataclass(frozen=True)
class InboxEvent:
    """A push event in the inbox. `event_seq` says the order of arrival, and `event_id` is what the sender calls it.

    `tries` counts the reads of it that failed in a way that may pass, and `retry_at` is when to read it again.
    """

    event_seq: int
    dataset: str
    event_id: str | None
    request: "InboundRequest"
    received_at: datetime
    tries: int = 0
    retry_at: datetime | None = None


@dataclass(frozen=True)
class InboxDay:
    """How a day of the inbox stands: how many events wait to be sealed, the first of them, and when the last sealed."""

    unsealed: int
    first_unsealed: int | None
    last_sealed: datetime | None


class InboxStore(Protocol):
    """Push events between arriving and being sealed, kept by the day of their arrival (design 6.3 and 7.4)."""

    def put(self, dataset: str, request: "InboundRequest", event_id: str | None = None) -> bool:
        """Store an event. False, storing nothing, when the dataset has had this `event_id` on a day it still holds."""
        ...

    def put_many(self, events: "Sequence[tuple[str, InboundRequest, str | None]]") -> list[bool]:
        """Store (dataset, request, event id) in that order, all or none, and say of each what `put` would have."""
        ...

    def take(self, dataset: str, limit: int, max_bytes: int | None = None) -> list[InboxEvent]:
        """The dataset's oldest events neither sealed nor held, in arrival order. They stay there until `seal_batch`.

        With `max_bytes`, the events stop at the first whose body takes their bodies past it, that one included: the
        first event always comes, however large (S20)."""
        ...

    def oldest_age(self, dataset: str, now: datetime) -> timedelta | None:
        """How long the oldest event neither sealed nor held has waited, or None when none waits."""
        ...

    def defer(self, event_seq: int, until: datetime) -> None:
        """Count a failed try of the event, and set when to read it again. An event sealed or held meanwhile stays so."""
        ...

    def hold(self, event_seq: int) -> None:
        """Set the event aside, unsealed, for someone to `reprocess` or `release`: `take` passes it by, and its day keeps.

        Raises `StaleWriter` if the event is unknown, sealed or held, since another sealer has dealt with it.
        """
        ...

    def reprocess(self, dataset: str, event_seq: int) -> bool:
        """Give a held event back to the sealer, with its tries forgotten. Returns whether it was held."""
        ...

    def release(self, dataset: str, event_seq: int) -> bool:
        """Seal a held event with no version, letting it go. Returns whether it was held."""
        ...

    def seal_batch(
        self,
        event_seqs: Sequence[int],
        drafts: Sequence[VersionDraft],
        routes: Sequence[str],
        writer: str | None = None,
    ) -> list["Version"]:
        """Mark the events sealed, then stage and seal the versions with a pending delivery per route, in one transaction.

        Either all of it happens or none. Raises `StaleWriter` if any event is unknown or already sealed, since another
        sealer has dealt with it, or if a `writer` doesn't hold the claim of a draft, and `VersionSealed` if a draft is
        sealed already. `drafts` can be empty, for events that gave no records.
        """
        ...

    def days(self) -> list[date]:
        """The days the inbox holds a place for, oldest first, with or without events."""
        ...

    def day_status(self, day: date) -> InboxDay: ...
    def drop_day(self, day: date) -> bool:
        """Forget the day's events and event ids, unless one of its events is not sealed, and return whether it did.

        This is the one check that counts: an unsealed event, held or not, is never dropped, whatever was read
        before.
        """
        ...

    def ensure_partitions(self, days_ahead: int = 7) -> None:
        """Make room for writes from today to `days_ahead` days on, for a backend that needs it."""
        ...


@dataclass(frozen=True)
class Window:
    """A limit of `units` per `seconds`."""

    units: int
    seconds: int


@dataclass(frozen=True)
class BudgetSpec:
    """A named rate budget: it is used up when any of its windows is. `reserve` maps a priority to the share of
    each window kept for it: `{"high": 0.6}` leaves 60% of every window to `high` requests alone.

    `account` names the vendor account the limits belong to, such as `zakya:60001` for a Zakya organisation. A
    budget with one is wider than its tenant (ADR-0013 rule 8): every budget, of any tenant, that names the same
    account takes from the same windows, as the vendor counts their calls together. Without one, the budget is the
    tenant's own."""

    name: str
    windows: tuple[Window, ...]
    reserve: Mapping[str, float] = field(default_factory=dict[str, float])
    account: str | None = None

    @property
    def key(self) -> str:
        """Where the windows are kept: the budget's name, or its account's, which tenants share."""
        return self.name if self.account is None else account_key(self.account)


SHARED_TENANT = (
    "*"  # the tenant id under which a vendor account's windows are kept: no tenant's, and every one's
)


def account_key(account: str) -> str:
    """The name under which the windows of a vendor account's budget are kept."""
    return f"account:{account}"


def account_mismatch(spec: "BudgetSpec", other: str, windows: Sequence["Window"]) -> str:
    """Why `spec` can't be defined: `other`, a budget of the same account, has other `windows`."""

    def text(ws: Sequence[Window]) -> str:
        return ", ".join(f"{w.units}/{w.seconds:g}s" for w in ws) or "none"

    return (
        f"budget {spec.name!r} gives account {spec.account!r} the windows {text(spec.windows)}, but {other} has "
        f"{text(windows)}; the budgets of one account share its windows, so give them all the same limits"
    )


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


@dataclass(frozen=True)
class BudgetWait:
    """A task that waits for a budget: since `first_at`, `waits` times, now until `until`."""

    budget: str
    task_ref: str
    run_id: str | None
    first_at: datetime
    until: datetime
    waits: int


WAITS_KEPT = timedelta(days=1)  # how long a wait's row stays after its `until`


class BudgetStore(Protocol):
    """Shared rate budgets. Each window refills continuously, as a token bucket, by the store's clock."""

    def define(self, spec: BudgetSpec) -> None:
        """Create the budget, full. Defining it again with the same spec changes nothing; with another, starts afresh.

        A budget of a vendor account takes the account's windows as they are, with what has been used of them. Raises
        `ValueError` when another budget of the account has other windows: neither is right to reset them.
        """
        ...

    def acquire(self, name: str, units: int, priority: Priority = "normal") -> Grant:
        """Take `units` from every window of the budget, or from none of them.

        `low` may use only `1 - reserve["high"]` of each window. Raises `KeyError` for an undefined budget, and
        `ValueError` when the request could never be granted, because it is more than a window can hold.
        """
        ...

    def usage(self, name: str) -> list[WindowUsage]: ...

    def specs(self) -> list[BudgetSpec]:
        """The budgets defined here, by name: the project's and those a package host defined (M2 plan 2 O1)."""
        ...

    def note_wait(self, name: str, task_ref: str, run_id: str | None, until: datetime) -> None:
        """Note that the task waits for the budget until `until` (M2 plan 2 O1): the first wait of a task makes its
        row, and each later one moves `until` and counts. Rows a day past their `until` go."""
        ...

    def waits(self, name: str) -> list[BudgetWait]:
        """The tasks still waiting for the budget by the store's clock, the soonest to go on first."""
        ...


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

    def forget_stopped(self, before: datetime) -> int:
        """Delete the dead and draining workers last seen before `before`, and return how many."""
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

    def delete_expired(self) -> int:
        """Delete the overrides that have expired, such as used previews and old idempotent answers; return how many."""
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

    def advance_tick(self, flow: str, trigger_id: str, seen: datetime | None, at: datetime) -> bool:
        """Set the last tick to `at` only if it is still `seen` (None: never set), and say whether it was. Of two
        schedulers that read the same tick, one wins and fires it (Stage 2 M9)."""
        ...


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
    """A local user, with their role in this tenant (ADR-0013: roles are stored per tenant and user)."""

    user_id: str
    username: str
    password_hash: str
    created_at: datetime
    role: Role
    grants: tuple[AccessGrant, ...] = ()


@dataclass(frozen=True)
class TokenRow:
    """An API token as stored: the SHA-256 of the secret (hex), never the secret itself."""

    token_id: str
    name: str
    token_hash: str
    created_at: datetime
    expires_at: datetime | None
    revoked_at: datetime | None
    role: Role
    created_by: str | None  # the user whose token it is; None for one the server's CLI made for automation
    scope: TokenScope = "all"


class DuplicateUser(Exception):
    """A user with that name exists already."""


class UserStore(Protocol):
    """Local users and the API tokens they and automation use."""

    def create_user(self, username: str, password_hash: str, role: Role) -> str:
        """Add the user and return the id. Raises `DuplicateUser` when the name is taken."""
        ...

    def get_user(self, username: str) -> UserRow | None: ...
    def list_users(self) -> list[UserRow]:
        """Every user of the tenant, by name in code point order."""
        ...

    def set_role(self, username: str, role: Role) -> None:
        """Give the user another role. Raises `KeyError` when there is no such user."""
        ...

    def set_grants(self, username: str, grants: Sequence[AccessGrant]) -> None:
        """Give the user these grants, and no others, in the order of `GRANTS`. Raises `KeyError` when there is no
        such user."""
        ...

    def set_password_hash(self, username: str, password_hash: str) -> None:
        """Give the user a new password hash. Raises `KeyError` when there is no such user."""
        ...

    def create_token(
        self,
        name: str,
        token_hash: str,
        expires_at: datetime | None,
        role: Role,
        created_by: str | None,
        scope: TokenScope = "all",
    ) -> str:
        """Add the token, known by the SHA-256 of its secret, and return its id. That its role is no higher than its
        user's is for the caller to check."""
        ...

    def find_token(self, token_hash: str) -> TokenRow | None:
        """The token with that hash, expired or revoked or not: the caller judges that."""
        ...

    def revoke_token(self, token_id: str) -> None:
        """Revoke the token, from now. Revoking one twice, or one that isn't there, changes nothing."""
        ...


@dataclass(frozen=True)
class RegistryFile:
    """A wheel in the built-in registry (flow packages §4.3). `project` is the package's normalised name (PEP 503).
    A registry keeps one file per version, and never replaces it: a version is yanked (PEP 592), not deleted."""

    project: str
    version: str
    filename: str
    sha256: str
    size: int
    manifest: Mapping[str, Any]
    uploaded_by: str
    uploaded_at: datetime
    yanked_at: datetime | None = None
    yanked_reason: str | None = None


@dataclass(frozen=True)
class RegistryRow:
    """Another Python package index this environment trusts. `token_ref` is a secret reference, never a value."""

    name: str
    index_url: str
    token_ref: str | None
    added_by: str
    added_at: datetime


def version_key(version: str) -> str:
    """The version as PEP 440 compares it, trailing zeros dropped: `1` for 1.0 and 1.0.0, so they are one version."""
    return canonicalize_version(version, strip_trailing_zero=True)


class DuplicateFile(Exception):
    """The registry has that version, or a file of that name, already: a published version is never replaced."""


class DuplicateRegistry(Exception):
    """A registry of that name exists already."""


class RegistryStore(Protocol):
    """The built-in registry's wheels, and the other registries the environment trusts, for one tenant."""

    def put_file(self, file: RegistryFile, content: bytes) -> None:
        """Keep the wheel. Raises `DuplicateFile` when the project has the version, by `version_key`, or the filename
        is taken."""
        ...

    def projects(self) -> list[str]:
        """The projects that have a file, by name in code point order."""
        ...

    def files(self, project: str) -> list[RegistryFile]:
        """The project's files, yanked ones too, oldest upload first."""
        ...

    def file(self, filename: str) -> RegistryFile | None: ...
    def content(self, filename: str) -> bytes | None:
        """The wheel's bytes, or None when there is no such file."""
        ...

    def yank(self, project: str, version: str, reason: str | None) -> bool:
        """Mark the version yanked, from now. False when there is no such version; yanking twice keeps the first."""
        ...

    def add_registry(self, name: str, index_url: str, token_ref: str | None, added_by: str) -> RegistryRow:
        """Raises `DuplicateRegistry` when the name is taken."""
        ...

    def registries(self) -> list[RegistryRow]:
        """By name."""
        ...

    def get_registry(self, name: str) -> RegistryRow | None: ...
    def remove_registry(self, name: str) -> bool:
        """False when there was none of that name."""
        ...


PackageState = Literal["installed", "preparing", "ready", "failed", "uninstalled"]
ActivationState = Literal["inactive", "activating", "active", "deactivating", "failed"]
EnvState = Literal["building", "ready", "failed"]
MAX_PACKAGE_ROWS = 500  # the most rows a package listing returns


@dataclass(frozen=True)
class PackageVersion:
    """An installed version of a flow package (flow packages §5, §9). `name` is the package's normalised name, and
    `sha256` names its wheel, which the store keeps once per sha256. `load_check` is what the install's load check
    found: the flows, datasets and routes the code defines, and how long it took."""

    name: str
    version: str
    sha256: str
    registry: str
    filename: str
    size: int
    manifest: Mapping[str, Any]
    fc_versions: str
    lock_hash: str
    state: PackageState
    load_check: Mapping[str, Any]
    installed_by: str
    installed_at: datetime
    checks: Sequence[str] = ()  # the install checks it passed, in order
    promoted_from: str | None = None  # the environment it was promoted from, by sha256
    promoted_by: str | None = None
    signed_by: str | None = None  # the id of the trusted key that signed it, where signing was required


@dataclass(frozen=True)
class FlowActivation:
    """Whether a flow of an installed package version is in service here (flow packages §6). `flow` carries the
    package prefix, `acme-invoices/invoices`."""

    flow: str
    package: str
    version: str
    state: ActivationState
    changed_by: str
    changed_at: datetime
    reason: str | None = None
    error: str | None = None
    last_active_at: datetime | None = None  # when it last served: became active, or stopped being so


def check_wheel(version: "PackageVersion", content: bytes) -> None:
    """Raise `ValueError` unless `content` is the wheel the version names by sha256 and size."""
    import hashlib

    if hashlib.sha256(content).hexdigest() != version.sha256 or len(content) != version.size:
        raise ValueError(
            f"the wheel stored for {version.name} {version.version} is not the one its sha256 names"
        )


HostState = Literal["starting", "ready", "crash_loop", "failed", "stopped"]


@dataclass(frozen=True)
class PackageHost:
    """How a worker's package host of a version stands (flow packages §7.1, task 9's Packages page): `restarts` counts
    its ends in the crash window, and `error` is the last thing it died of."""

    worker_id: str
    package: str
    version: str
    state: HostState
    restarts: int
    error: str | None
    changed_at: datetime


@dataclass(frozen=True)
class PackageEnv:
    """A dependency environment that a worker built for a lock hash (flow packages §5 step 3, task 8)."""

    worker_id: str
    lock_hash: str
    state: EnvState
    error: str | None
    built_at: datetime


class VersionNotReady(Exception):
    """A flow of a version that isn't `ready` can't be activated: it is being prepared, failed, or was uninstalled.
    `state` is the version's."""

    def __init__(self, message: str, state: str) -> None:
        super().__init__(message)
        self.state = state


class InUse(Exception):
    """A version can't be uninstalled while a flow of it is active or activating, or runs of it are queued or
    running."""


class DuplicateVersion(Exception):
    """That version of the package is installed already, by `version_key`: a version always means the same code."""


class PackageStore(Protocol):
    """The installed flow packages of one tenant: their versions, wheels and flows' activations."""

    def install(
        self, version: PackageVersion, content: bytes, flows: Sequence[str], reason: str | None = None
    ) -> None:
        """Record the version as installed in one transaction: the package if it is new, the wheel by its sha256 if
        the store hasn't it, the version, and each of `flows` (prefixed names) inactive. Raises `DuplicateVersion`
        when the package has the version, by `version_key`, and `ValueError` when `content` is not the bytes whose
        sha256 the version names; it changes nothing then."""
        ...

    def packages(self) -> list[str]:
        """The installed packages' names, in code point order, at most `MAX_PACKAGE_ROWS`."""
        ...

    def versions(self, name: str | None = None) -> list[PackageVersion]:
        """The installed versions, of one package or of all, by name and then oldest install first, at most
        `MAX_PACKAGE_ROWS`."""
        ...

    def version(self, name: str, version: str) -> PackageVersion | None:
        """The version, matched by `version_key`, or None."""
        ...

    def wheel(self, sha256: str) -> bytes | None:
        """The wheel's bytes, or None when the store has no wheel of that sha256."""
        ...

    def uninstall(self, name: str, version: str, by: str, at: datetime, reason: str | None = None) -> bool:
        """Mark the version `uninstalled` (§6) in one transaction, its flows' rows staying, inactive, as its history
        does, and delete its wheel unless a version still installed has the same sha256. Returns whether the wheel was
        deleted. Raises `KeyError` when the version isn't installed (or is uninstalled already), and `InUse` when a
        flow of it is active or activating, or a run tagged with it is queued or running; it changes nothing then."""
        ...

    def needed_lock_hashes(self) -> set[str]:
        """The lock hashes of every version that isn't uninstalled, however many there are: the environments a
        worker must keep."""
        ...

    def renames(self) -> dict[tuple[str, str], str]:
        """What the upgrade from the project mode renamed (§13): the prefixed name of each old one, by its kind
        (`flow`, `dataset` or `route`) and old name."""
        ...

    def activations(self, name: str, version: str | None = None) -> list[FlowActivation]:
        """The flows of the package's versions, or of one, by flow and then as they changed, at most
        `MAX_PACKAGE_ROWS`."""
        ...

    def activate(
        self, flow: str, name: str, version: str, by: str, at: datetime, reason: str | None = None
    ) -> FlowActivation:
        """Ask for `version` to be the active one of `flow` (§6): it is `activating`, and the version active now stays
        active, until a host of the new one is up and `promote` makes the switch. A version already active stays so,
        and another one activating is dropped (inactive). Tells every listener (NOTIFY). Raises `KeyError` when the
        version has no such flow, and `VersionNotReady` when the version isn't `ready`, checked in the same
        transaction, so that an uninstall can't slip in between."""
        ...

    def promote(self, name: str, version: str, at: datetime) -> list[FlowActivation]:
        """Make every flow that the version is `activating` active, and the version active before inactive, in one
        transaction, and tell the listeners: a host of it is up. Returns the flows it made active."""
        ...

    def activation_failed(self, name: str, version: str, error: str, at: datetime) -> list[FlowActivation]:
        """Mark every flow that the version is `activating` failed, saying why: its host didn't come up, and the
        version active before stays active. Tells the listeners, and returns those flows."""
        ...

    def activating(self) -> list[FlowActivation]:
        """Every flow waiting for a host of the version asked for, by name, at most `MAX_PACKAGE_ROWS`."""
        ...

    def deactivate(self, flow: str, by: str, at: datetime, reason: str | None = None) -> FlowActivation:
        """Make the active version of `flow` inactive, and one activating too, and tell the listeners. Returns the
        active one, or else the one activating. Raises `KeyError` when no version of it is either."""
        ...

    def active(self) -> list[FlowActivation]:
        """Every active flow, by name, at most `MAX_PACKAGE_ROWS`."""
        ...

    def listen(self, callback: "Callable[[], None]") -> "Callable[[], None]":
        """Call `callback` whenever a flow of this tenant is activated or deactivated, or a version is prepared, from
        this process or another. Returns what stops listening."""
        ...

    def prepared(
        self,
        name: str,
        version: str,
        state: PackageState,
        load_check: Mapping[str, Any],
        expect: Sequence[PackageState] = ("installed", "preparing"),
    ) -> bool:
        """Record what preparing the version found, its state (`ready` or `failed`) and its load check, if its state
        is still one of `expect`, and tell the listeners. Returns whether it did: a worker that prepared it too, later,
        changes nothing."""
        ...

    def record_env(
        self, worker_id: str, lock_hash: str, state: EnvState, at: datetime, error: str | None = None
    ) -> None:
        """Note the state of the environment that `worker_id` keeps for `lock_hash`."""
        ...

    def envs(self, worker_id: str | None = None) -> list[PackageEnv]:
        """The environments of one worker or of all, by worker and lock hash, at most `MAX_PACKAGE_ROWS`."""
        ...

    def record_host(
        self,
        worker_id: str,
        name: str,
        version: str,
        state: HostState,
        restarts: int,
        at: datetime,
        error: str | None = None,
    ) -> None:
        """Note how the worker's host of the version stands."""
        ...

    def hosts(self, worker_id: str | None = None) -> list[PackageHost]:
        """The hosts of one worker or of all, by worker, package and version, at most `MAX_PACKAGE_ROWS`."""
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

    def list_audit(
        self,
        limit: int = 100,
        cursor: str | None = None,
        *,
        actor: str | None = None,
        action: str | None = None,
        target: str | None = None,
    ) -> list[AuditRow]:
        """Newest first. `cursor` is the `audit_id` of the last row of the previous page. `actor` and `action` keep
        the rows that name them; `target` the rows whose target starts with it, as `route:` does every route's
        (M2 plan 2 O3)."""
        ...

    def for_target(self, target: str, since: datetime | None = None, limit: int = 500) -> list[AuditRow]:
        """The rows about `target`, such as `route:erp`, from `since` on, oldest first: the first `limit` of them."""
        ...


# Environment-level configuration (flow packages §8) and the secret store.

EnvKind = Literal["connection", "budget", "setting"]
ENV_KINDS: tuple[EnvKind, ...] = ("connection", "budget", "setting")
MAX_ENV_ROWS = 1000  # the most entries of an environment a listing returns


@dataclass(frozen=True)
class EnvEntry:
    """One entry of an environment's own configuration: a connection, a budget or a setting, by name. `value` holds
    no secret: a connection's secret fields are references."""

    env: str
    kind: EnvKind
    name: str
    value: Any
    changed_by: str
    changed_at: datetime


class EnvConfigStore(Protocol):
    """The environment-level configuration of this tenant, per environment."""

    def entries(self, env: str, kind: EnvKind | None = None) -> list[EnvEntry]:
        """The entries of `env`, of one kind or all, by kind and name, at most `MAX_ENV_ROWS`."""
        ...

    def entry(self, env: str, kind: EnvKind, name: str) -> EnvEntry | None: ...

    def put(self, entry: EnvEntry) -> EnvEntry | None:
        """Set an entry, and return the one it replaced, if any."""
        ...

    def remove(self, env: str, kind: EnvKind, name: str) -> EnvEntry | None:
        """Remove an entry, and return it, if there was one."""
        ...


@dataclass(frozen=True)
class SealedSecret:
    """A secret as stored: only its ciphertext, the nonce it was sealed with, and the id of the master key, never the
    value."""

    env: str
    name: str
    ciphertext: bytes
    nonce: bytes
    key_id: str
    updated_by: str
    updated_at: datetime


class KeyMismatch(Exception):
    """A secret sealed with a master key that isn't the environment's active one, or a rotation from a key that
    isn't active any more: another process rotated meanwhile."""

    def __init__(self, active: str | None, given: str | None) -> None:
        super().__init__(f"the active master key is {active}, not {given}")
        self.active, self.given = active, given


class SecretStore(Protocol):
    """The encrypted secrets of this tenant, per environment, and the id of each environment's active master key.
    Nothing here can decrypt them."""

    @property
    def tenant(self) -> str:
        """The tenant, which a secret's associated data names."""
        ...

    def put(self, secret: SealedSecret) -> bool:
        """Set or replace a secret. Returns whether it replaced one. Raises `KeyMismatch` when `secret.key_id` isn't
        the environment's active key; the first secret of an environment makes its key the active one."""
        ...

    def active_key(self, env: str) -> str | None:
        """The id of the environment's active master key, or None before its first secret."""
        ...

    def get(self, env: str, name: str) -> SealedSecret | None: ...

    def names(self, env: str) -> list[SealedSecret]:
        """The secrets of `env` by name, at most `MAX_ENV_ROWS`. A caller shows only names, times and who."""
        ...

    def remove(self, env: str, name: str) -> bool: ...

    def rotate(
        self, env: str, expected: str | None, new_key: str, reseal: Callable[[SealedSecret], SealedSecret]
    ) -> int:
        """A key rotation, in one transaction with the environment's key row locked, so that no secret is set
        meanwhile: raises `KeyMismatch` unless the active key is `expected`, writes `reseal(s)` over every secret `s`
        of `env` (all of them, however many), makes `new_key` the active key and returns how many were sealed again.
        If `reseal` raises, nothing changes."""
        ...


@dataclass(frozen=True)
class WebhookEvent:
    """A webhook event a package declared, verified by the server and kept raw until a worker parses it with the
    package's transform. `key` is what it is deduplicated by."""

    package: str
    webhook: str
    key: str
    stamp: str | None
    body: bytes
    headers: Mapping[str, str]
    received_at: datetime
    processed_at: datetime | None = None


class WebhookEventStore(Protocol):
    """The raw events of declared webhooks (flow packages §7.2), once each by key."""

    def add(self, event: WebhookEvent) -> bool:
        """Keep the event. Returns False, and keeps nothing, when an event of that webhook has its key already."""
        ...

    def keep_many(self, events: "Sequence[tuple[WebhookEvent, str, InboundRequest]]") -> list[bool]:
        """Keep each event as processed, and put each new one into the inbox of the push dataset named beside it, under
        its key: all in one transaction, all or none. Says of each whether it was new; a repeat of a key, of one kept
        before or earlier in these events, keeps nothing and goes into no inbox."""
        ...

    def events(self, package: str, webhook: str, limit: int = 100) -> list[WebhookEvent]:
        """The webhook's events, oldest first."""
        ...

    def processed(self, package: str, webhook: str, keys: Sequence[str], at: datetime) -> int:
        """Mark events as parsed by a worker, so that retention may delete them. Returns how many it marked."""
        ...

    def purge(self, before: datetime) -> int:
        """Delete the processed events received before `before`, and return how many. An event not yet processed
        is kept, however old."""
        ...


READY_CHANNEL = (
    "feather_ready"  # NOTIFY payload: the tenant whose versions sealed or deliveries closed (readiness)
)


@dataclass(frozen=True)
class PartitionClose:
    """Someone said a partition is complete (readiness design §4.1): it is ready once its last version is sealed."""

    dataset: str
    partition: str
    closed_at: datetime
    closed_by: str
    reason: str


@dataclass(frozen=True)
class Firing:
    """One run of a waiting flow for one (readiness set, period, scope), and `seq` 0 for the first, 1 and on for the
    reruns late data starts (readiness design §6). `ready_set` names the set within the flow; `period` is an ISO
    start (`2026-11-08`, `2026-11-08T13:00`) or `run:<id>` of an upstream run; `scope` is `field=value` or empty."""

    flow: str
    ready_set: str
    period: str
    scope: str
    seq: int
    reason: str  # ready, late data or manual
    versions: Mapping[str, Any]  # what the firing saw: dataset -> partition -> version id
    fired_at: datetime
    actor: str
    run_id: str | None = None


@dataclass(frozen=True)
class SealedPartition:
    """A partition's sealed version as readiness reads it: with its run's status and when the run was made, which is
    no later than when it started to read (design §4)."""

    partition: str
    version_id: str
    seal_seq: int
    run_id: str
    run_status: str
    run_created_at: datetime
    sealed_at: datetime


class ReadinessStore(Protocol):
    """Closed partitions and firings (readiness design §7.2). What is sealed and applied is read from the versions and
    the ledger, which already hold it."""

    def listen(self, callback: "Callable[[], None]") -> "Callable[[], None]":
        """Call `callback` when a version is sealed or a delivery is applied or skipped (`READY_CHANNEL`), from this
        process or another, so that the evaluator looks again at once. A store may never call it, and its evaluator
        then polls. Returns what stops listening."""
        ...

    def close(self, dataset: str, partition: str, actor: str, reason: str, at: datetime) -> PartitionClose:
        """Close the partition. A partition closed already stays as it was, and that close is returned."""
        ...

    def closes(self, dataset: str, partitions: Sequence[str]) -> dict[str, PartitionClose]:
        """The closes of those of `partitions` that are closed."""
        ...

    def fire(self, firing: Firing, params: Mapping[str, Any]) -> str | None:
        """Record the firing and queue its run, with `params` and the trigger `ready`, in one transaction, and return
        the run's id. None, recording and queueing nothing, when the firing's (flow, set, period, scope, seq) is there
        already: so a firing happens once however many evaluators race to it."""
        ...

    def firings(self, flow: str, ready_set: str | None = None, limit: int = 200) -> list[Firing]:
        """The flow's firings, newest first."""
        ...

    def latest_sealed(self, dataset: str, since: datetime) -> dict[str, SealedPartition]:
        """By partition, the newest sealed version of each partition of `dataset` that had a version sealed at `since`
        or later."""
        ...

    def sealed_since(self, dataset: str, since: datetime) -> list[SealedPartition]:
        """Every version of `dataset` sealed at `since` or later, in seal order: what `each="run"` groups by run."""
        ...

    def open_deliveries(self, route: str, dataset: str) -> dict[str, int]:
        """By partition, the lowest `seal_seq` of the route's deliveries of `dataset` that aren't closed (`CLOSED`)."""
        ...

    def oldest_unsealed(self, dataset: str) -> datetime | None:
        """When the oldest event of a push dataset that is neither sealed nor held arrived, or None."""
        ...


AlertState = Literal["firing", "cleared"]
SEVERITIES = ("warning", "critical")  # least first


@dataclass(frozen=True)
class AlertRow:
    """One alert (M2 plan 2 O6): a rule firing for a subject, such as a route's delivery or a budget's window. While it
    fires, it is the one row of its (rule, subject); it clears when the rule no longer finds it. `notified_at` is None
    until its channels were told, so a notice that failed is retried."""

    alert_id: str
    rule: str
    subject: str
    severity: str
    message: str
    state: AlertState
    fired_at: datetime
    last_seen_at: datetime
    cleared_at: datetime | None = None
    acked_by: str | None = None
    acked_at: datetime | None = None
    snoozed_until: datetime | None = None
    notified_at: datetime | None = None


@dataclass(frozen=True)
class AlertRuleRow:
    """A rule as the environment set it: on or off, its parameters, and the channels it tells."""

    name: str
    enabled: bool
    params: Mapping[str, Any]
    channels: tuple[str, ...]
    updated_at: datetime


class AlertStore(Protocol):
    def fire(self, rule: str, subject: str, severity: str, message: str) -> tuple[AlertRow, bool]:
        """Raise the alert of (rule, subject), or refresh the one firing. True when it is new, or critical now where it
        was a warning: then its channels are to be told (again)."""
        ...

    def clear(self, rule: str, firing: Collection[str]) -> list[AlertRow]:
        """Clear the rule's firing alerts whose subject is not in `firing`; return them, cleared."""
        ...

    def list_alerts(self, state: AlertState | None = None, limit: int = 100) -> list[AlertRow]:
        """Newest first."""
        ...

    def get_alert(self, alert_id: str) -> AlertRow:
        """Raises `KeyError` when there is no such alert."""
        ...

    def ack(self, alert_id: str, actor: str) -> AlertRow:
        """Acknowledge it: it stays until it clears, but its channels hear no more of it. Raises `KeyError`."""
        ...

    def snooze(self, alert_id: str, until: datetime | None) -> AlertRow:
        """Keep it quiet until then, or no longer with None. Raises `KeyError`."""
        ...

    def mark_notified(self, alert_id: str) -> None: ...

    def unnotified(self) -> list[AlertRow]:
        """The firing alerts whose channels haven't been told, oldest first."""
        ...

    def rules(self) -> list[AlertRuleRow]:
        """By name."""
        ...

    def set_rule(
        self, name: str, enabled: bool, params: Mapping[str, Any], channels: Sequence[str]
    ) -> AlertRuleRow: ...

    def delete_rule(self, name: str) -> None:
        """Back to the rule's default. Raises `KeyError` when the environment hasn't set it."""
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
    inbox: InboxStore
    responses: ResponseStore
    registry: RegistryStore
    packages: PackageStore
    env_config: EnvConfigStore
    secrets: SecretStore
    webhooks: WebhookEventStore
    readiness: ReadinessStore
    alerts: AlertStore

    def is_unavailable(self, error: BaseException) -> bool:
        """Whether `error` says that the store can't be reached, or can't keep up, just now, so that the same call may
        work later. Otherwise it refused what it was given, and will again."""
        return isinstance(error, ConnectionError | TimeoutError)
