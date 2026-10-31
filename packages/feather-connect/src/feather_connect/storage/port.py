"""The store ports: the run journal, versions, delivery ledger, record state and quarantine.

Every backend must pass `feather_connect.testing.store_contract.StoreContract`. The values here are frozen
dataclasses so that they pickle, which the engine needs for step results.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol

VersionStatus = Literal["staged", "sealed"]
DeliveryStatus = Literal["pending", "running", "applied", "failed", "held", "skipped", "compensated"]
QuarantineStatus = Literal["open", "superseded"]


class VersionSealed(Exception):
    """A sealed version was staged again. Sealed versions never change (I1)."""


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
    """A record a route's sink rejected. `route` and `version_id` are None for a record rejected at extraction."""

    route: str | None
    version_id: str | None
    key: str
    error: str
    status: QuarantineStatus


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
    ) -> str: ...
    def finish_task(self, task_id: str, status: str, error: str | None = None) -> None: ...
    def list_tasks(self, run_id: str) -> list[TaskRow]: ...
    def log(self, task_id: str, level: str, message: str) -> None: ...
    def task_logs(self, task_id: str) -> list[str]:
        """The task's log lines in order, each as `level: message`."""
        ...


class VersionStore(Protocol):
    """Versions, from staging to sealing, and the slices of a run."""

    def stage(self, draft: VersionDraft) -> Version:
        """Store a draft. Staging a version again replaces it, until it is sealed; then `VersionSealed`."""
        ...

    def seal(self, version_ids: Sequence[str], routes: Sequence[str]) -> list[Version]:
        """Seal each version, give it the next `seal_seq` and a pending delivery per route.

        Sealing a version that is already sealed changes nothing.
        """
        ...

    def discard_staged(self, version_id: str) -> None:
        """Remove a staged version, which an extract found it had no need of. An unknown id changes nothing.

        Raises `VersionSealed` for a sealed version.
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
    def finish_slice(self, run_id: str, dataset: str, partition: str) -> bool:
        """Mark a slice done. True when every registered slice of the run's dataset is done. Safe to repeat."""
        ...

    def staged_for_run(self, run_id: str, dataset: str) -> list[Version]: ...
    def sealed_for_run(self, run_id: str, dataset: str) -> list[Version]:
        """All of the run's sealed versions of the dataset, in seal order."""
        ...

    def list_versions(self, dataset: str, partition: str | None = None, limit: int = 100) -> list[Version]:
        """Newest first."""
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


class RecordStateStore(Protocol):
    """The last known state of each record of a dataset, by key."""

    def get_many(self, dataset: str, keys: Iterable[str]) -> dict[str, StateRow]:
        """The rows that exist; a key with no row is left out."""
        ...

    def put_many(self, dataset: str, rows: Iterable[StateRow]) -> None: ...
    def delete_many(self, dataset: str, keys: Iterable[str]) -> None: ...


class QuarantineStore(Protocol):
    """Records a sink rejected, kept until a later version of the record is delivered."""

    def add(self, route: str | None, version_id: str | None, key: str, error: str) -> None:
        """Adding the same (route, version, key) again replaces the entry and reopens it."""
        ...

    def supersede(self, route: str, key: str, before_seal_seq: int) -> int:
        """Close the route's open entries for `key` from versions sealed before `before_seal_seq`.

        Returns how many it closed.
        """
        ...

    def list_open(self, route: str | None = None) -> list[QuarantineRow]: ...


@dataclass(frozen=True)
class Store:
    """The ports together. A backend provides one of these."""

    runs: RunJournal
    versions: VersionStore
    ledger: Ledger
    state: RecordStateStore
    quarantine: QuarantineStore
