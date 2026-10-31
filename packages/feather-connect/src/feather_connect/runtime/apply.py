"""Applying versions to routes: deliver sealed versions to a destination in seal order, through the ledger.

The task body decides nothing outside a step, and reads no store or spool there. Step results hold counts and chunk
numbers, never records. Each chunk reaches the sink under a key that names the chunk, so running a chunk's step again
after a crash or a retry can't apply it twice. A replay of a delivery has a new key, and reaches the sink again.
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta
from itertools import count
from typing import Any, cast

import pyarrow as pa

from feather_connect.core.connector import ApplyBatch, ApplyRecord, ApplyResult, Key
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import BudgetDeferred, PermanentError, TransientError
from feather_connect.core.flow import BatchMap, RouteSpec
from feather_connect.core.records import Op, Record, canonical_json, decode_key
from feather_connect.core.transforms import apply_map, is_batch_map
from feather_connect.engine.port import RetryPolicy, TaskContext
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.storage.port import CLOSED, Delivery, Version, delivery_cursor
from feather_connect.storage.spool import ChunkKind, table_to_records

log = logging.getLogger(__name__)

_KEY, _OP, _HASH, _MOVED = "_fc_key", "_fc_op", "_fc_row_hash", "_fc_moved_from"
_ACTOR = "system"
# A wait shorter than this is `Context.budget`'s to handle. One that gets out here is left to the engine's retry.
_SLEEP_FROM = timedelta(seconds=60)
# Errors of these kinds keep their meaning wherever they come from. Any other error of a map is about the record.
_CLASSIFIED = (TransientError, PermanentError, BudgetDeferred)


@dataclass(frozen=True)
class _Plan:
    """Which chunks a delivery sends. `clears` means one empty batch, for a slice that has become empty."""

    kind: ChunkKind
    chunks: tuple[int, ...]
    clears: bool = False


@dataclass(frozen=True)
class _Counts:
    applied: int = 0
    rejected: int = 0


def _oldest_open(route: str, dataset: str, partition: str, deps: RuntimeDeps) -> Delivery | None:
    """Return the delivery next in line for one dataset. A route can carry several, and I2 orders each on its own."""
    ledger = deps.store.ledger
    oldest = ledger.oldest_open(route, partition)
    if oldest is None or oldest.dataset == dataset:
        return oldest
    page = ledger.list_deliveries(route=route, partition=partition)
    while page:
        for delivery in page:
            if delivery.dataset == dataset and delivery.status not in CLOSED:
                return delivery
        page = ledger.list_deliveries(route=route, partition=partition, cursor=delivery_cursor(page[-1]))
    return None


def _resume(delivery: Delivery, reason: str, deps: RuntimeDeps) -> None:
    """Move the delivery to running, unless it is there: a task that died, or a step run again, left it so."""
    ledger = deps.store.ledger
    if ledger.get(delivery.route, delivery.version_id).status == "pending":
        ledger.transition(
            delivery.route,
            delivery.version_id,
            "running",
            actor=_ACTOR,
            reason=reason,
            code_sha=deps.code_sha,
        )


def _plan_of(route: RouteSpec, version: Version, deps: RuntimeDeps) -> _Plan:
    """Work out what the delivery sends. A `replace_partition` route gets the whole slice, and the others the changes.

    A complete version with no `full` chunks and only deletes says that the slice is now empty, which the sink has to
    hear. With no deletes either nothing changed, so it hears nothing: the keys may be held for rejected rows.
    """
    kind: ChunkKind = "full" if route.mode == "replace_partition" else "data"
    chunks = tuple(chunk.chunk_no for chunk in deps.spool.list_chunks(version.version_id, kind))
    if kind == "full" and not chunks and version.complete and not version.upserts and version.deletes:
        return _Plan("data", (0,), clears=True)
    return _Plan(kind, chunks)


def _start(route: RouteSpec, delivery: Delivery, deps: RuntimeDeps) -> _Plan:
    _resume(delivery, "apply started", deps)
    return _plan_of(route, deps.store.versions.get(delivery.version_id), deps)


def _key_text(key: Key) -> str:
    """Undo `decode_key`, to name a record the way the store does."""
    return key if isinstance(key, str) else canonical_json(list(key)).decode("utf-8")


def _rows_where(table: pa.Table, mask: list[bool]) -> pa.Table:
    array: Any = pa.array(mask)  # pyright: ignore[reportUnknownMemberType]
    return table if all(mask) else table.filter(array)  # pyright: ignore[reportUnknownMemberType]


class _ChunkSender:
    """Sends the chunks of one delivery. The steps of the delivery share it, and only they may call it."""

    def __init__(
        self,
        ctx: TaskContext,
        dataset: Dataset,
        route: RouteSpec,
        delivery: Delivery,
        plan: _Plan,
        deps: RuntimeDeps,
    ) -> None:
        self._ctx = ctx
        self._dataset = dataset
        self._route = route
        self._delivery = delivery
        self._plan = plan
        self._deps = deps
        self._last_chunks: dict[str, int] | None = None

    def send(self, chunk_no: int) -> _Counts:
        """Send one chunk. A transient error marks the delivery pending for the engine's retry, until they run out."""
        delivery = self._delivery
        _resume(delivery, "apply retried", self._deps)
        try:
            return self._send(chunk_no)
        except TransientError as error:
            attempts = RetryPolicy().max_attempts
            if self._ctx.attempt >= attempts:
                raise PermanentError(f"still failing after {attempts} attempts: {error}") from error
            self._deps.store.ledger.transition(
                delivery.route,
                delivery.version_id,
                "pending",
                actor=_ACTOR,
                reason="retryable error",
                error=str(error),
                code_sha=self._deps.code_sha,
            )
            raise

    def _send(self, chunk_no: int) -> _Counts:
        route, delivery, plan = self._route, self._delivery, self._plan
        version = self._deps.store.versions.get(delivery.version_id)
        context = self._deps.make_context(route.sink.connection)
        records, rejected = ([], 0) if plan.clears else self._records(version, chunk_no, context)
        if not records and route.mode != "replace_partition":
            return _Counts(rejected=rejected)
        batch = ApplyBatch(
            route=route.name,
            version_id=delivery.version_id,
            chunk_no=chunk_no,
            partition=delivery.partition,
            mode=route.mode,
            records=records,
            idempotency_key=f"{route.name}:{delivery.version_id}:{chunk_no}:{delivery.replays}",
            is_first=chunk_no == plan.chunks[0],
            is_last=chunk_no == plan.chunks[-1],
        )
        result: ApplyResult = route.sink.apply(context, batch)
        for rejection in result.rejected:
            self._reject(_key_text(rejection.key), rejection.message)
        return _Counts(result.applied, rejected + len(result.rejected))

    def _records(self, version: Version, chunk_no: int, context: Context) -> tuple[list[ApplyRecord], int]:
        """Return what the sink gets of a chunk, and how many records were turned down on the way."""
        table = self._deps.spool.get_chunk(version.version_id, chunk_no, self._plan.kind)
        if self._plan.kind == "full":
            table = self._without_stale_rows(table, chunk_no)
        moves = self._moves(version.version_id)
        if is_batch_map(self._route.map):
            return self._batch_records(version, table, context, moves), 0
        out: list[ApplyRecord] = []
        rejected = 0
        for record in table_to_records(table):
            applied = self._record_outputs(version, record, context, moves)
            if applied is None:
                rejected += 1
            else:
                out += applied
        return out, rejected

    def _without_stale_rows(self, table: pa.Table, chunk_no: int) -> pa.Table:
        """Keep the rows of a `full` chunk that the replace should hold: a key's last, and none of a rejected key."""
        last_chunks = self._last_chunk_of_each_key()
        keys = cast("list[str]", table.column(_KEY).to_pylist())
        return _rows_where(table, [last_chunks.get(key) == chunk_no for key in keys])

    def _last_chunk_of_each_key(self) -> dict[str, int]:
        """Go through the `full` chunks once, which is a sealed version's and can't change, for a key per record."""
        if self._last_chunks is None:
            version_id = self._delivery.version_id
            spool = self._deps.spool
            last_chunks: dict[str, int] = {}
            for chunk_no in self._plan.chunks:
                keys = cast(
                    "list[str]", spool.get_chunk(version_id, chunk_no, "full").column(_KEY).to_pylist()
                )
                last_chunks.update(dict.fromkeys(keys, chunk_no))
            for chunk in spool.list_chunks(version_id, "rejected"):
                keys = cast(
                    "list[str]",
                    spool.get_chunk(version_id, chunk.chunk_no, "rejected").column(_KEY).to_pylist(),
                )
                for key in keys:
                    last_chunks.pop(key, None)
            self._last_chunks = last_chunks
        return self._last_chunks

    def _moves(self, version_id: str) -> dict[str, str]:
        moves: dict[str, str] = {}
        for chunk in self._deps.spool.list_chunks(version_id, "moves"):
            table = self._deps.spool.get_chunk(version_id, chunk.chunk_no, "moves")
            keys = cast("list[str]", table.column(_KEY).to_pylist())
            sources = cast("list[str]", table.column(_MOVED).to_pylist())
            moves.update(zip(keys, sources, strict=True))
        return moves

    def _record_outputs(
        self, version: Version, record: Record, context: Context, moves: Mapping[str, str]
    ) -> list[ApplyRecord] | None:
        """Return what one record becomes on this route, or None if it was turned down."""
        if record.op == "delete":
            return [self._apply_record(version, record.key, "delete", None)]
        moved_from = record.moved_from or moves.get(record.key)
        try:
            payloads = apply_map(self._route, replace(record, moved_from=moved_from), context)
        except _CLASSIFIED:
            raise
        except Exception as error:  # noqa: BLE001 - a map may raise anything, and that speaks of this record
            self._reject(record.key, str(error))
            return None
        return [self._apply_record(version, record.key, "upsert", p, moved_from) for p in payloads]

    def _batch_records(
        self, version: Version, table: pa.Table, context: Context, moves: Mapping[str, str]
    ) -> list[ApplyRecord]:
        """Run the upserts of a chunk through the batch map. Its deletes skip it, and go after them."""
        ops = cast("list[Op]", table.column(_OP).to_pylist())
        upserts = _rows_where(table, [op == "upsert" for op in ops])
        out: list[ApplyRecord] = []
        if upserts.num_rows:
            for record in table_to_records(self._mapped_table(upserts, context)):
                moved_from = record.moved_from or moves.get(record.key)
                out.append(self._apply_record(version, record.key, "upsert", record.payload, moved_from))
        if "delete" in ops:
            deletes = table_to_records(_rows_where(table, [op == "delete" for op in ops]))
            out += [self._apply_record(version, record.key, "delete", None) for record in deletes]
        return out

    def _mapped_table(self, upserts: pa.Table, context: Context) -> pa.Table:
        """Return what the batch map makes of the upserts, ready for `table_to_records`.

        Nothing ties a failure to one record of a batch, so a failure is the version's.
        """
        name = self._route.name
        try:
            result = cast(object, cast(BatchMap, self._route.map)(upserts, context))
        except _CLASSIFIED:
            raise
        except Exception as error:
            raise PermanentError(f"the batch map of route {name!r} failed: {error}") from error
        if not isinstance(result, pa.Table):
            raise PermanentError(f"the batch map of route {name!r} must return an Arrow table")
        mapped = result
        if _KEY not in mapped.column_names:
            raise PermanentError(f"the batch map of route {name!r} must keep the {_KEY} column of its input")
        for column, type_ in ((_OP, pa.string()), (_HASH, pa.uint64()), (_MOVED, pa.string())):
            if column not in mapped.column_names:
                nulls: Any = pa.nulls(mapped.num_rows, type_)  # pyright: ignore[reportUnknownMemberType]
                mapped = mapped.append_column(column, nulls)
        return mapped

    def _apply_record(
        self,
        version: Version,
        key: str,
        op: Op,
        payload: Mapping[str, Any] | None,
        moved_from: str | None = None,
    ) -> ApplyRecord:
        """Return a record for the sink. On an `append` route every record is a row, with where it came from."""
        if self._route.mode == "append":
            payload = {
                **(payload or {}),
                _KEY: key,
                _OP: op,
                "_fc_version": version.version_id,
                "_fc_run": version.run_id,
                "_fc_extracted_at": version.sealed_at,  # the time of the version, the same on a replay
                "_fc_partition": version.partition,
            }
        return ApplyRecord(decode_key(key, self._dataset.key), op, payload, moved_from)

    def _reject(self, key: str, message: str) -> None:
        if self._dataset.on_record_error == "fail":
            raise PermanentError(
                f"route {self._route.name!r}: record {key} was rejected and on_record_error is 'fail': {message}"
            )
        self._deps.store.quarantine.add(self._route.name, self._delivery.version_id, key, message)


def _fail(delivery: Delivery, error: str, deps: RuntimeDeps) -> None:
    ledger = deps.store.ledger
    if ledger.get(delivery.route, delivery.version_id).status == "running":  # not if a crash came after this
        ledger.transition(
            delivery.route,
            delivery.version_id,
            "failed",
            actor=_ACTOR,
            reason="permanent error",
            error=error,
            code_sha=deps.code_sha,
        )


def _supersede(delivery: Delivery, plan: _Plan, deps: RuntimeDeps) -> None:
    """Close what older versions left in quarantine for the keys this version delivered. It can be repeated."""
    open_rows = deps.store.quarantine.list_open(delivery.route)
    rejected_here = {row.key for row in open_rows if row.version_id == delivery.version_id}
    older = {row.key for row in open_rows} - rejected_here
    if not older:
        return
    for chunk in deps.spool.list_chunks(delivery.version_id, plan.kind):
        table = deps.spool.get_chunk(delivery.version_id, chunk.chunk_no, plan.kind)
        for key in older.intersection(cast("list[str]", table.column(_KEY).to_pylist())):
            deps.store.quarantine.supersede(delivery.route, key, delivery.seal_seq)


def _finish(
    dataset: Dataset, delivery: Delivery, plan: _Plan, totals: Sequence[_Counts], deps: RuntimeDeps
) -> None:
    """Mark the delivery applied, unless a crash came after that, and resolve what it supersedes."""
    ledger = deps.store.ledger
    if ledger.get(delivery.route, delivery.version_id).status != "applied":
        _resume(delivery, "apply retried", deps)
        ledger.transition(
            delivery.route,
            delivery.version_id,
            "applied",
            actor=_ACTOR,
            rows_applied=sum(counts.applied for counts in totals),
            rows_rejected=sum(counts.rejected for counts in totals),
            code_sha=deps.code_sha,
        )
    if dataset.keep_state:
        _supersede(delivery, plan, deps)


def _send_durably(ctx: TaskContext, name: str, sender: _ChunkSender, chunk_no: int) -> _Counts:
    """Run a chunk's step, and sleep off a budget deferral before running it again."""
    while True:
        try:
            return ctx.step(name, lambda: sender.send(chunk_no))
        except BudgetDeferred as deferred:
            if deferred.wait < _SLEEP_FROM:
                raise
            ctx.sleep(deferred.wait)


def _deliver(
    ctx: TaskContext, dataset: Dataset, route: RouteSpec, delivery: Delivery, deps: RuntimeDeps
) -> bool:
    """Apply one delivery and return True, or return False if it failed, which blocks its partition."""
    version_id = delivery.version_id
    plan = ctx.step(f"start:{version_id}", lambda: _start(route, delivery, deps))
    sender = _ChunkSender(ctx, dataset, route, delivery, plan, deps)
    totals: list[_Counts] = []
    try:
        for chunk_no in plan.chunks:
            totals.append(_send_durably(ctx, f"chunk:{version_id}:{chunk_no}", sender, chunk_no))
    except PermanentError as error:
        message = str(error)
        ctx.step(f"fail:{version_id}", lambda: _fail(delivery, message, deps))
        return False
    ctx.step(f"end:{version_id}", lambda: _finish(dataset, delivery, plan, totals, deps))
    return True


def run_apply(
    ctx: TaskContext, dataset: Dataset, route: RouteSpec, version_id: str, deps: RuntimeDeps
) -> None:
    """Bring `route` up to `version_id` in the version's partition, oldest open delivery first (I2).

    A failed or held delivery blocks its partition, and the task ends there. A permanent error fails the delivery, and
    the task still ends well, so that other partitions carry on. A transient error goes to the engine to retry.
    """
    target = ctx.step("target", lambda: deps.store.versions.get(version_id))
    if target.seal_seq is None:
        raise PermanentError(f"version {version_id} is not sealed, so it can't be applied")
    for n in count():
        delivery = ctx.step(
            f"next:{n}", lambda: _oldest_open(route.name, dataset.name, target.partition, deps)
        )
        if delivery is None or delivery.seal_seq > target.seal_seq:
            return
        if delivery.status in ("failed", "held"):
            log.info("route %s, version %s: blocked by %s", route.name, version_id, delivery.version_id)
            return
        if not _deliver(ctx, dataset, route, delivery, deps):
            return
