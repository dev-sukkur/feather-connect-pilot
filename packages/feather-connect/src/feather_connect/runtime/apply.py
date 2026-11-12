"""Applying versions to routes: deliver sealed versions to a destination in seal order, through the ledger.

The task body decides nothing outside a step, and reads no store or spool there. Step results hold counts and chunk
numbers, never records. Each chunk reaches the sink under a key that names the chunk, so running a chunk's step again
after a crash or a retry can't apply it twice. A replay of a delivery has a new key, and reaches the sink again.
"""

import json
import logging
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta
from functools import partial
from itertools import count
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

from feather_connect.core.approval import APPROVED
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ApplyResult, Key, RecordResponse
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import BudgetDeferred, PermanentError, TransientError
from feather_connect.core.flow import BatchMap, RouteSpec
from feather_connect.core.records import Op, Record, canonical_json, decode_key
from feather_connect.core.schema_policy import NULL, SCHEMA_CHANGE, judge
from feather_connect.core.transforms import apply_map, is_batch_map
from feather_connect.engine.port import RetryPolicy, TaskContext
from feather_connect.logs import redact
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import NOT_DELIVERED
from feather_connect.storage.port import (
    CLOSED,
    Delivery,
    DeliveryStatus,
    ResponseRow,
    Version,
    delivery_cursor,
    is_paused,
)
from feather_connect.storage.spool import JSON_COLUMNS_KEY, ChunkKind, table_to_records

log = logging.getLogger(__name__)

_KEY, _OP, _HASH, _MOVED = "_fc_key", "_fc_op", "_fc_row_hash", "_fc_moved_from"
_ACTOR = "system"
# A wait shorter than this is `Context.budget`'s to handle. One that gets out here is left to the engine's retry,
# until that is the last one the engine gives.
_SLEEP_FROM = timedelta(seconds=60)
# Errors of these kinds keep their meaning wherever they come from. Any other error of a map is about the record.
_CLASSIFIED = (TransientError, PermanentError, BudgetDeferred)
# A move out of these statuses starts a delivery's tries afresh: an operator retried or replayed it.
_RESTARTED_FROM: frozenset[DeliveryStatus] = frozenset({"failed", "held", "applied"})
# How many keys an error names, of those it is about.
_NAMED_KEYS = 5


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


def oldest_open(route: str, dataset: str, partition: str, deps: RuntimeDeps) -> Delivery | None:
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


def _chunk_keys(version_id: str, chunk_no: int, kind: ChunkKind, deps: RuntimeDeps) -> list[str]:
    return cast("list[str]", deps.spool.get_chunk(version_id, chunk_no, kind).column(_KEY).to_pylist())


def _plan_of(route: RouteSpec, version: Version, deps: RuntimeDeps) -> _Plan:
    """Work out what the delivery sends. A `replace_partition` route gets the whole slice, and the others the changes.

    A complete version with no `full` chunks, whose snapshot holds nothing that reached the destination, says that the
    slice is now empty there, because of deletes or moves, which the sink has to hear. Raises `PermanentError` if the
    snapshot holds delivered keys that the `full` chunks don't, since their rows were turned down at extraction and a
    replace would silently drop those records. A key that was never delivered can't be dropped, so it doesn't count.
    """
    version_id = version.version_id
    if route.mode != "replace_partition":
        return _Plan("data", tuple(chunk.chunk_no for chunk in deps.spool.list_chunks(version_id)))
    chunks = tuple(chunk.chunk_no for chunk in deps.spool.list_chunks(version_id, "full"))
    snapshots = deps.spool.list_chunks(version_id, "snapshot")
    held: set[str] = set()
    for chunk in snapshots:
        table = deps.spool.get_chunk(version_id, chunk.chunk_no, "snapshot")
        keys = cast("list[str]", table.column(_KEY).to_pylist())
        hashes = cast("list[int]", table.column(_HASH).to_pylist())
        held.update(key for key, hash_ in zip(keys, hashes, strict=True) if hash_ != NOT_DELIVERED)
    for chunk_no in chunks:
        held.difference_update(_chunk_keys(version_id, chunk_no, "full", deps))
    if held:
        named = ", ".join(sorted(held)[:_NAMED_KEYS])
        raise PermanentError(
            f"{len(held)} records of the slice were turned down at extraction (first keys: {named}), and replacing "
            "the slice would drop them: fix the source, then skip this version so that a newer one applies"
        )
    if snapshots and not chunks and version.complete and not version.upserts:
        return _Plan("data", (0,), clears=True)
    return _Plan("full", chunks)


def replay_gap(route: RouteSpec, version: Version, deps: RuntimeDeps) -> str | None:
    """Say why a replay of `version` on `route` would send nothing, or None if it has something to send.

    Retention drops a version's chunks long before its ledger row, and a plan with no chunks would pass for applied.
    """
    version_id = version.version_id
    try:
        plan = _plan_of(route, version, deps)
    except PermanentError as error:
        if deps.spool.list_chunks(version_id, "full"):
            return str(error)
        plan = _Plan("full", ())  # the records the snapshot names are gone from the `full` chunks
    if plan.chunks:
        return None
    if version.upserts or version.deletes or route.mode == "replace_partition":
        return (
            f"version {version_id} can't replay on route {route.name!r}: its data has aged out of retention, though "
            "its ledger row remains. Read the partition again from its source to deliver it afresh"
        )
    return f"version {version_id} can't replay on route {route.name!r}: it has nothing to send"


def _tries(delivery: Delivery, deps: RuntimeDeps) -> int:
    """Return how many times the delivery has started running since an operator last sent it back."""
    tries = 0
    for attempt in reversed(deps.store.ledger.attempts(delivery.route, delivery.version_id)):
        tries += attempt.to_status == "running"
        if attempt.from_status in _RESTARTED_FROM:
            break
    return tries


def _counting_a_try[T](delivery: Delivery, deps: RuntimeDeps, fn: Callable[[], T]) -> T:
    """Run part of a delivery.

    A budget deferral says nothing against the delivery. Any other error but a permanent one marks the delivery
    pending to be tried again, until the delivery has used up its tries, which makes the error permanent.
    """
    try:
        return fn()
    except (PermanentError, BudgetDeferred):
        raise
    except Exception as error:
        tries = _tries(delivery, deps)
        if tries >= deps.retry.max_attempts:
            raise PermanentError(f"still failing after {tries} attempts: {error}") from error
        deps.store.ledger.transition(
            delivery.route,
            delivery.version_id,
            "pending",
            actor=_ACTOR,
            reason="retryable error",
            error=redact(str(error)),
            code_sha=deps.code_sha,
        )
        raise


def _start(route: RouteSpec, delivery: Delivery, deps: RuntimeDeps) -> _Plan:
    _resume(delivery, "apply started", deps)
    version_id = delivery.version_id
    return _counting_a_try(delivery, deps, lambda: _plan_of(route, deps.store.versions.get(version_id), deps))


def _redacted(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy with every known secret value masked in the text it holds, at any depth. Not the token patterns
    of error text: stored data must stay what was sent, "Basic membership" and all (M4)."""

    def mask(item: Any) -> Any:
        if isinstance(item, str):
            return redact(item)
        if isinstance(item, Mapping):
            return {str(k): mask(v) for k, v in cast(Mapping[Any, Any], item).items()}
        if isinstance(item, list | tuple):
            return [mask(v) for v in cast(Sequence[Any], item)]
        return item

    return cast(dict[str, Any], mask(value))


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
        dataset: Dataset,
        route: RouteSpec,
        delivery: Delivery,
        plan: _Plan,
        deps: RuntimeDeps,
    ) -> None:
        self._dataset = dataset
        self._route = route
        self._delivery = delivery
        self._plan = plan
        self._deps = deps
        self._last_chunks: dict[str, int] | None = None
        self._moves: dict[str, str] | None = None

    def send(self, chunk_no: int) -> _Counts:
        """Send one chunk."""
        _resume(self._delivery, "apply retried", self._deps)
        return _counting_a_try(self._delivery, self._deps, lambda: self._send(chunk_no))

    def check_maps(self) -> None:
        """Run the map over every chunk and keep nothing, so that a bad record fails a replace before the first send.

        A replace can't leave a record out, and transforms are pure, so this says how the sends would go.
        """
        _resume(self._delivery, "apply retried", self._deps)
        _counting_a_try(self._delivery, self._deps, self._map_all_chunks)

    def _map_all_chunks(self) -> None:
        if self._route.map is None or self._plan.clears:
            return
        version = self._deps.store.versions.get(self._delivery.version_id)
        context = self._deps.make_context(self._route.sink.connection)
        for chunk_no in self._plan.chunks:
            self._records(version, chunk_no, context)

    def reverse(self, chunk_no: int) -> int:
        """Compensate one chunk: the sink reverses what it applied of it (design §6.5). Returns how many it reversed."""
        version = self._deps.store.versions.get(self._delivery.version_id)
        context = self._deps.make_context(self._route.sink.connection)
        records, _ = self._records(version, chunk_no, context)
        if not records:
            return 0
        route, delivery = self._route, self._delivery
        batch = ApplyBatch(
            route=route.name,
            version_id=delivery.version_id,
            chunk_no=chunk_no,
            partition=delivery.partition,
            mode=route.mode,
            records=records,
            idempotency_key=f"compensate:{route.name}:{delivery.version_id}:{chunk_no}",
            is_first=chunk_no == self._plan.chunks[0],
            is_last=chunk_no == self._plan.chunks[-1],
        )
        result = route.sink.compensate(context, batch)
        self._keep_responses(chunk_no, result.responses, complete=False)
        return result.applied

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
        try:
            result: ApplyResult = route.sink.apply(context, batch)
        except Exception:
            self._keep_responses(chunk_no, context.answers(), complete=False)  # then it goes up as it is
            raise
        if result.pending_confirmation is not None:
            raise PermanentError(
                f"{route.sink.spec.name} asked to confirm later, and confirm-later sinks arrive in M2, "
                "so this delivery can't be marked applied"
            )
        self._keep_responses(chunk_no, result.responses, complete=True)
        sent = {_key_text(r.key): r.payload for r in records}
        for rejection in result.rejected:
            key = _key_text(rejection.key)
            self._reject(key, rejection.message, sent.get(key))
        return _Counts(result.applied, rejected + len(result.rejected))

    def _keep_responses(self, chunk_no: int, responses: Sequence[RecordResponse], *, complete: bool) -> None:
        """Keep what the destination answered for each record, redacted, in the store, and a line of counts in the
        task's log (ruling 5).

        A chunk sent in full replaces the chunk's answers, so that records not sent this time show none (M8); one that
        failed part-way adds the answers it got. Before the rejections are quarantined, so that an entry can show the
        body its record was sent with.
        """
        name, version_id = self._route.name, self._delivery.version_id
        rows = [
            ResponseRow(
                name,
                version_id,
                chunk_no,
                _key_text(response.key),
                response.status,
                _redacted(response.fields),
                None if response.sent is None else _redacted(response.sent),
            )
            for response in responses
        ]
        if complete:
            self._deps.store.responses.replace_chunk(name, version_id, chunk_no, rows)
        elif rows:
            self._deps.store.responses.record(rows)
        if rows:
            counts = Counter(row.status for row in rows)
            by_status = ", ".join(f"HTTP {status}: {n}" for status, n in sorted(counts.items()))
            log.info("route %s, chunk %d: %d answers (%s)", name, chunk_no, len(rows), by_status)

    def _records(self, version: Version, chunk_no: int, context: Context) -> tuple[list[ApplyRecord], int]:
        """Return what the sink gets of a chunk, and how many records were turned down on the way."""
        table = self._deps.spool.get_chunk(version.version_id, chunk_no, self._plan.kind)
        if self._plan.kind == "full":
            table = self._without_stale_rows(table, chunk_no)
        moves = self._moves_of(version.version_id)
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
        """Keep the rows of a `full` chunk that the replace should hold: each key's last."""
        last_chunks = self._last_chunk_of_each_key()
        keys = cast("list[str]", table.column(_KEY).to_pylist())
        return _rows_where(table, [last_chunks[key] == chunk_no for key in keys])

    def _last_chunk_of_each_key(self) -> dict[str, int]:
        """Go through the `full` chunks once, which is a sealed version's and can't change, for a key per record."""
        if self._last_chunks is None:
            self._last_chunks = {}
            for chunk_no in self._plan.chunks:
                keys = _chunk_keys(self._delivery.version_id, chunk_no, "full", self._deps)
                self._last_chunks.update(dict.fromkeys(keys, chunk_no))
        return self._last_chunks

    def _moves_of(self, version_id: str) -> dict[str, str]:
        if self._moves is None:
            self._moves = {}
            for chunk in self._deps.spool.list_chunks(version_id, "moves"):
                table = self._deps.spool.get_chunk(version_id, chunk.chunk_no, "moves")
                keys = cast("list[str]", table.column(_KEY).to_pylist())
                sources = cast("list[str]", table.column(_MOVED).to_pylist())
                self._moves.update(zip(keys, sources, strict=True))
        return self._moves

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
        except Exception as error:  # a map may raise anything, and that speaks of this record
            if self._route.mode == "replace_partition":
                raise PermanentError(
                    f"the map of route {self._route.name!r} failed on record {record.key}, "
                    f"which replacing the slice can't leave out: {error}"
                ) from error
            self._reject(record.key, str(error), record.payload)
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
        if mapped.column(_KEY).null_count:
            raise PermanentError(f"the batch map of route {name!r} gave a row with no {_KEY}")
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

    def _reject(self, key: str, message: str, payload: Mapping[str, Any] | None = None) -> None:
        """Quarantine a record with its payload, redacted, unless the dataset says to fail or the route replaces whole
        slices."""
        name = self._route.name
        if self._dataset.on_record_error == "fail":
            raise PermanentError(
                f"route {name!r}: record {key} was rejected and on_record_error is 'fail': {message}"
            )
        if self._route.mode == "replace_partition":
            raise PermanentError(
                f"route {name!r}: the sink rejected record {key} and has already applied its batch, "
                f"so the destination may hold a partial slice: {message}"
            )
        kept = None if payload is None else _redacted(payload)
        self._deps.store.quarantine.add(name, self._delivery.version_id, key, redact(message), payload=kept)


def _fail(delivery: Delivery, error: str, deps: RuntimeDeps) -> None:
    ledger = deps.store.ledger
    if ledger.get(delivery.route, delivery.version_id).status == "running":  # not if a crash came after this
        ledger.transition(
            delivery.route,
            delivery.version_id,
            "failed",
            actor=_ACTOR,
            reason="permanent error",
            error=redact(error),
            code_sha=deps.code_sha,
        )


def fail_open(delivery: Delivery, error: str, deps: RuntimeDeps) -> None:
    """Fail a pending or running delivery that no task will finish, so that an operator can retry or skip it."""
    _resume(delivery, "its task ended", deps)
    _fail(delivery, error, deps)


def _supersede(delivery: Delivery, plan: _Plan, deps: RuntimeDeps) -> None:
    """Close what older versions of the dataset left in quarantine for the keys this version delivered.

    It can be repeated. The port closes a key's entries across the route, so a key with an entry from another dataset
    on the route stays open, rather than close a record that wasn't delivered.
    """
    older: set[str] = set()
    rejected_here: set[str] = set()
    other_datasets: set[str] = set()
    for row in deps.store.quarantine.list_open(delivery.route):
        if row.version_id == delivery.version_id:
            rejected_here.add(row.key)
        elif row.version_id is not None:
            same = deps.store.versions.get(row.version_id).dataset == delivery.dataset
            (older if same else other_datasets).add(row.key)
    older -= rejected_here | other_datasets
    if not older:
        return
    for chunk in deps.spool.list_chunks(delivery.version_id, plan.kind):
        for key in older.intersection(_chunk_keys(delivery.version_id, chunk.chunk_no, plan.kind, deps)):
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


def _step_durably[T](ctx: TaskContext, name: str, fn: Callable[[], T], retry: RetryPolicy) -> T:
    """Run a step, and sleep off a budget deferral before running it again.

    An error for the engine to retry goes to the engine. On the task's last attempt the engine won't, while a delivery
    can still have tries left, since a task can catch up on several. So the task sleeps and tries again itself, as
    long as the server asks (`TransientError.retry_after`) when it says, up to `retry.max_attempts` times: then the
    step's error is permanent, and fails the delivery rather than holding the task for good. A budget deferral is a
    wait, not a failure, and doesn't count.
    """
    last_attempt = ctx.attempt >= retry.max_attempts
    failed = 0
    while True:
        try:
            return ctx.step(name, fn)
        except PermanentError:
            raise
        except BudgetDeferred as deferred:
            if deferred.wait < _SLEEP_FROM and not last_attempt:
                raise
            ctx.sleep(deferred.wait)
        except Exception as error:
            if not last_attempt:
                raise
            failed += 1
            if failed >= retry.max_attempts:
                raise PermanentError(
                    f"{name}: still failing after {failed} more tries on the task's last attempt: {error}"
                ) from error
            wait = retry.delay(ctx.attempt + failed - 1, rand=lambda: 1.0)
            asked = error.retry_after if isinstance(error, TransientError) else None
            ctx.sleep(max(wait, timedelta(seconds=asked)) if asked is not None else wait)


def _hold_kind(reason: str | None) -> str:
    return SCHEMA_CHANGE if (reason or "").startswith(SCHEMA_CHANGE) else "approval"


def approved(route: str, version_id: str, deps: RuntimeDeps, kind: str = "approval") -> bool:
    """Whether an approver let the delivery go from a hold of `kind` ("approval" or "schema_change"): a move from
    held to pending that says so, after a hold of that kind. Each hold needs its own decision."""
    held: str | None = None
    for a in deps.store.ledger.attempts(route, version_id):
        if a.to_status == "held":
            held = _hold_kind(a.reason)
        elif (
            a.from_status == "held"
            and a.to_status == "pending"
            and (a.reason or "").startswith(APPROVED)
            and held == kind
        ):
            return True
    return False


_FIELDS: dict[str, dict[str, str] | None] = {}  # by version id: a sealed version's chunks never change
_FIELDS_KEPT = 512


def version_fields(version_id: str, deps: RuntimeDeps) -> dict[str, str] | None:
    """The fields of the version's upserted records and their Arrow types (`null` for one null in all of them), or
    None when it upserts nothing or its chunks are gone, which tells nothing of its schema."""
    if version_id in _FIELDS:
        return _FIELDS[version_id]
    found: dict[str, str] = {}
    try:
        for ref in deps.spool.list_chunks(version_id):
            table = deps.spool.get_chunk(version_id, ref.chunk_no)
            upserts = table.filter(pc.not_equal(table.column("_fc_op"), "delete"))  # pyright: ignore[reportAttributeAccessIssue]
            if not upserts.num_rows:
                continue
            metadata = table.schema.metadata or {}
            json_columns = set(json.loads(metadata.get(JSON_COLUMNS_KEY.encode(), b"[]")))
            for column in upserts.schema:
                if column.name.startswith("_fc_"):
                    continue
                if upserts.column(column.name).null_count == upserts.num_rows:
                    typed = NULL
                else:
                    typed = "json" if column.name in json_columns else str(column.type)
                if found.get(column.name, NULL) == NULL:
                    found[column.name] = typed
    except KeyError:
        return None
    fields = found or None
    if len(_FIELDS) >= _FIELDS_KEPT:
        _FIELDS.pop(next(iter(_FIELDS)))
    _FIELDS[version_id] = fields
    return fields


def _hold_for_schema(dataset: Dataset, route: RouteSpec, delivery: Delivery, deps: RuntimeDeps) -> bool:
    """Hold the delivery when its fields changed from the last version the route applied in a way the policy holds
    on (design §4.7, H12), unless an approver let it go; say whether it is held."""
    policy = route.schema_policy if route.schema_policy is not None else dataset.schema_policy
    ledger, version_id = deps.store.ledger, delivery.version_id
    if policy == "permissive":
        return False
    if ledger.get(route.name, version_id).status == "held":
        return True  # a step run again
    if approved(route.name, version_id, deps, SCHEMA_CHANGE):
        return False
    last = ledger.last_applied(route.name, delivery.partition, delivery.dataset, delivery.seal_seq)
    if last is None:
        return False
    before, after = version_fields(last.version_id, deps), version_fields(version_id, deps)
    if before is None or after is None:
        return False
    why = judge(policy, before, after)
    if why is None:
        return False
    _resume(delivery, "apply started", deps)
    ledger.transition(
        route.name,
        version_id,
        "held",
        actor=_ACTOR,
        reason=f"{why} (against version {last.version_id})",
        code_sha=deps.code_sha,
    )
    log.info("route %s, version %s: held for a schema change", route.name, version_id)
    return True


def _hold_for_approval(route: RouteSpec, delivery: Delivery, deps: RuntimeDeps) -> bool:
    """Hold the delivery for an approver when a record of it needs one and none has approved it; say whether it is
    held (design §5.3). It reads the records the sink would get, before the map, as the route's approval judges them."""
    approval, version_id = route.approval, delivery.version_id
    ledger = deps.store.ledger
    if approval is None:
        return False
    if ledger.get(route.name, version_id).status == "held":
        return True  # a step run again
    if approved(route.name, version_id, deps):
        return False
    plan = _plan_of(route, deps.store.versions.get(version_id), deps)
    wanted = sum(
        approval.wanted(record)
        for chunk_no in plan.chunks
        for record in table_to_records(deps.spool.get_chunk(version_id, chunk_no, plan.kind))
    )
    if not wanted:
        return False
    _resume(delivery, "apply started", deps)
    why = f" ({approval.reason})" if approval.reason else ""
    ledger.transition(
        route.name,
        version_id,
        "held",
        actor=_ACTOR,
        reason=f"approval: {wanted} record{'s' if wanted != 1 else ''} to approve{why}",
        code_sha=deps.code_sha,
    )
    log.info("route %s, version %s: held for approval", route.name, version_id)
    return True


def _deliver(
    ctx: TaskContext, dataset: Dataset, route: RouteSpec, delivery: Delivery, deps: RuntimeDeps
) -> bool:
    """Apply one delivery and return True, or return False if it failed or is held, which blocks its partition."""
    version_id = delivery.version_id
    totals: list[_Counts] = []
    try:
        schema = partial(_hold_for_schema, dataset, route, delivery, deps)
        if _step_durably(ctx, f"schema:{version_id}", schema, deps.retry):
            return False
        if route.approval is not None:
            hold = partial(_hold_for_approval, route, delivery, deps)
            if _step_durably(ctx, f"approval:{version_id}", hold, deps.retry):
                return False
        start = partial(_start, route, delivery, deps)
        plan = _step_durably(ctx, f"start:{version_id}", start, deps.retry)
        sender = _ChunkSender(dataset, route, delivery, plan, deps)
        if route.mode == "replace_partition":
            _step_durably(ctx, f"maps:{version_id}", sender.check_maps, deps.retry)
        for chunk_no in plan.chunks:
            send = partial(sender.send, chunk_no)
            totals.append(_step_durably(ctx, f"chunk:{version_id}:{chunk_no}", send, deps.retry))
    except PermanentError as error:
        message = str(error)
        ctx.step(f"fail:{version_id}", lambda: _fail(delivery, message, deps))
        return False
    ctx.step(f"end:{version_id}", lambda: _finish(dataset, delivery, plan, totals, deps))
    return True


def run_compensate(
    ctx: TaskContext,
    dataset: Dataset,
    route: RouteSpec,
    version_id: str,
    deps: RuntimeDeps,
    *,
    actor: str,
    reason: str | None,
) -> None:
    """Reverse an applied delivery of a side-effect route, chunk by chunk, then mark it `compensated` (design §6.5,
    H9). Each chunk's reversal carries its own idempotency key, so a task run again reverses nothing twice."""
    delivery = ctx.step("delivery", lambda: deps.store.ledger.get(route.name, version_id))
    if delivery.status == "compensated":
        return
    if delivery.status != "applied":
        raise PermanentError(
            f"only an applied delivery can be compensated, and this one is {delivery.status}"
        )
    plan = _step_durably(
        ctx,
        f"plan:{version_id}",
        lambda: _plan_of(route, deps.store.versions.get(version_id), deps),
        deps.retry,
    )
    sender = _ChunkSender(dataset, route, delivery, plan, deps)
    reversed_ = 0
    for chunk_no in plan.chunks:
        reversed_ += _step_durably(
            ctx, f"reverse:{version_id}:{chunk_no}", partial(sender.reverse, chunk_no), deps.retry
        )
    why = f"compensated {reversed_} records" + (f": {reason}" if reason else "")
    ctx.step(
        "compensated",
        lambda: deps.store.ledger.transition(
            route.name, version_id, "compensated", actor=actor, reason=why, code_sha=deps.code_sha
        ),
    )


def run_apply(
    ctx: TaskContext, dataset: Dataset, route: RouteSpec, version_id: str, deps: RuntimeDeps
) -> None:
    """Bring `route` up to `version_id` in the version's partition, oldest open delivery first (I2).

    A failed or held delivery blocks its partition, and the task ends there. So does a paused route, before it
    starts the next delivery, and leaves that delivery as it is. A permanent error fails the delivery, and
    the task still ends well, so that other partitions carry on. Any other error goes to the engine to retry, up to
    `deps.retry.max_attempts` tries for each delivery, after which it fails the delivery too.
    """
    target = ctx.step("target", lambda: deps.store.versions.get(version_id))
    if target.seal_seq is None:
        raise PermanentError(f"version {version_id} is not sealed, so it can't be applied")
    for n in count():
        delivery = ctx.step(
            f"next:{n}", lambda: oldest_open(route.name, dataset.name, target.partition, deps)
        )
        if delivery is None or delivery.seal_seq > target.seal_seq:
            return
        if delivery.status in ("failed", "held"):
            log.info("route %s, version %s: blocked by %s", route.name, version_id, delivery.version_id)
            return
        if ctx.step(f"paused:{n}", lambda: is_paused(deps.store.overrides, f"route:{route.name}")):
            log.info("route %s, version %s: the route is paused", route.name, version_id)
            return  # the delivery stays as it is; `resume_route` or the sweeper submits it again
        if not _deliver(ctx, dataset, route, delivery, deps):
            return
