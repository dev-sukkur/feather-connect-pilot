"""The in-memory connector: a source that serves the rows it was given and a sink that keeps what it gets.

Flows and the runtime can be tested with it, without any real system.
"""

from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyRecord,
    ApplyResult,
    CheckResult,
    ConnectorSpec,
    Key,
    ReadRequest,
    Rejection,
    StreamSchema,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, RecordRejected
from feather_connect.core.strategies import read_stamp


def changed_since(row: Mapping[str, Any], req: ReadRequest) -> bool:
    """Return whether the row's stamp is `req.since` or later. A row without a stamp never is.

    A read that sets no `since` or no `stamp_field` takes every row. So does a stamp that can't be compared with
    `since`: it is the runtime that turns such a record down.
    """
    if req.since is None or req.stamp_field is None:
        return True
    stamp = read_stamp(row.get(req.stamp_field))
    try:
        return stamp is not None and stamp >= read_stamp(req.since)
    except TypeError:
        return True


class Source(connector.Source):
    """A source that serves the rows loaded into it, limited to the partition and stamp a read asks for."""

    spec = ConnectorSpec(
        name="memory.source",
        strategies=frozenset({"by_stamp", "snapshot_diff", "replace", "push"}),
    )

    def __init__(self, connection: str = "memory", rows: Iterable[Mapping[str, Any]] = ()) -> None:
        self.connection = connection
        self._rows: list[dict[str, Any]] = []
        self.load(rows)

    def load(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Make `rows` all of the source's contents, in place of what it held."""
        self._rows = [dict(row) for row in rows]

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def discover(self, ctx: Context) -> list[StreamSchema]:
        """Offer one stream, `rows`, with the fields of the loaded rows and the type of each first value."""
        fields: dict[str, str] = {}
        for row in self._rows:
            for name, value in row.items():
                if value is not None:
                    fields.setdefault(name, type(value).__name__)
        return [StreamSchema("rows", fields)]

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        """Yield the matching rows as a single batch, or nothing if none match. A read never resumes."""
        rows = [row for row in self._rows if req.contains(row) and changed_since(row, req)]
        if rows:
            yield rows


class Sink(connector.Sink):
    """A sink that keeps what it receives in memory, for tests to look at.

    `rows` holds the payloads, `deleted_keys` the key of every delete it received and `calls` every call to
    `apply` that returned, repeats included. `fail_next` and `reject` make it misbehave on purpose.
    """

    spec = ConnectorSpec(name="memory.sink", modes=frozenset({"upsert", "append", "replace_partition"}))

    def __init__(self, connection: str = "memory") -> None:
        self.connection = connection
        self.deleted_keys: list[Key] = []
        self.calls: list[ApplyBatch] = []
        # Upserts and replacements are held by key, each with the partition of the batch that wrote it.
        self._keyed: dict[Key, tuple[str, dict[str, Any]]] = {}
        self._appended: list[dict[str, Any]] = []
        self._results: dict[str, ApplyResult] = {}  # by idempotency key, to answer a repeat as the first time
        self._failures: deque[Exception] = deque()
        self._rejections: dict[Key, str] = {}

    @property
    def rows(self) -> list[dict[str, Any]]:
        """The payloads held: those written by key in the order their keys arrived, then the appended ones."""
        return [payload for _, payload in self._keyed.values()] + self._appended

    def fail_next(self, exc: Exception) -> None:
        """Make the next call to `apply` raise `exc` before changing anything. Failures queue, one per call."""
        self._failures.append(exc)

    def reject(self, key: Key, message: str) -> None:
        """Reject the next record that arrives with `key`, once, with `message`."""
        self._rejections[key] = message

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        if self._failures:
            raise self._failures.popleft()
        result = self._results.get(batch.idempotency_key)
        if result is None:  # a batch that was applied before is not applied again
            result = self._apply_new(batch)
            self._results[batch.idempotency_key] = result
        self.calls.append(batch)
        return result

    def _apply_new(self, batch: ApplyBatch) -> ApplyResult:
        if batch.mode not in self.spec.modes:
            raise PermanentError(f"{self.spec.name} can't apply in mode {batch.mode!r}")
        if batch.mode == "replace_partition" and batch.is_first:
            self._keyed = {key: row for key, row in self._keyed.items() if row[0] != batch.partition}
        rejected: list[Rejection] = []
        for record in batch.records:
            message = self._rejections.pop(record.key, None)
            if message is None:
                self._write(batch, record)
            else:
                rejected.append(Rejection(record.key, RecordRejected.__name__, message))
        return ApplyResult(applied=len(batch.records) - len(rejected), rejected=tuple(rejected))

    def _write(self, batch: ApplyBatch, record: ApplyRecord) -> None:
        if record.op == "delete":
            self.deleted_keys.append(record.key)
            self._keyed.pop(record.key, None)
        elif record.payload is None:
            raise PermanentError(f"the upsert of {record.key!r} has no payload")
        elif batch.mode == "append":
            self._appended.append(dict(record.payload))
        else:
            self._keyed[record.key] = (batch.partition, dict(record.payload))
