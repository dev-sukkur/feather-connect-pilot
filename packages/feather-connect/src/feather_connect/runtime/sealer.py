"""The sealer: turns what waits in the inbox into versions, without a run per event (design 6.4).

A batch is due when the oldest event has waited `max_age` or there are `max_events` of them. Each partition the batch
touches gets one incomplete version, named after its first event, so that doing a batch over gives the same names:

    1. claim the versions, and write their chunks under the claim (a sealer it overtakes can write no more);
    2. `inbox.seal_batch`: in one transaction, mark the events sealed, seal the versions and open their deliveries;
    3. submit the applies. A crash before this leaves pending deliveries, which is what the sweeper looks for.

So two sealers, or a crash and a rerun, give one version per first event. Each loses in one of three ways: its claim is
refused because the version is sealed, its write or its seal is refused because another claimed the version after it, or
`seal_batch` is refused because an event is sealed already, whatever the name of the version.

What comes in a webhook is untrusted, and one bad event must not hold up the rest for ever. A record that can't be
stored goes to the quarantine and the rest of its event seals. An event that can't be read or stored goes to the
quarantine and is held, unsealed, for someone to reprocess or release, and the events after it seal. What the store is
known to refuse is checked for first, against the limits below, and a batch that fails all the same is split until the
event that fails is alone. A read that fails in a way that may pass, as `TransientError` or anything not a
`PermanentError` may, is tried again as the pull path tries a task: the event waits, and the events after it wait
behind it to keep their order, until a later tick after the retry policy's delay. Only when its tries are used up is it
held. All the records of a batch stay in the version in order, including several of one key: a keyed route ends up with
the last, and an append route is owed each of them.

The versions of a batch seal in the order of their first events, so a key whose records go to several partitions keeps
its order only while each record goes to a version no earlier than the last one's. An event that would break that, as a
key moving back to a partition it left in the same batch does, starts the next batch, so the key's last write wins.
Within one event, a key that goes to several partitions keeps only its last record.

Nothing logged here carries a message of an error, which may quote a request. The quarantine keeps the message, capped
and redacted, for someone who can see the data anyway.
"""

import logging
import math
import traceback
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, Literal, cast

from feather_connect.core.connector import Deleted, PushSource
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import PermanentError
from feather_connect.core.flow import Flow
from feather_connect.core.records import Record, make_key, row_hash
from feather_connect.ids import new_id, stable_id
from feather_connect.logs import redact
from feather_connect.runtime.extract import (
    check_columns,
    content_hash,
    mark_state,
    schema_hash,
    update_state,
    validate_row,
)
from feather_connect.runtime.references import STATE_MARK_KEY, state_marker
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage import spool as spool_settings
from feather_connect.storage.port import InboxEvent, StaleWriter, Version, VersionDraft, VersionSealed
from feather_connect.storage.spool import records_to_table

log = logging.getLogger(__name__)

_PUSH = "push"
_MAX_BATCHES = 20  # a tick that a backlog can't keep for long, so that the other datasets get their turn
_MAX_ERROR = 500
_MAX_NAME = 200

# What one record of a push event may be. Past these the store can't take it, and its batch would fail at every tick.
# Nesting counts objects and lists inside each other: Parquet reads back no more than 49 lists deep, or 98 objects. A
# partition key is in btree indexes, whose rows Postgres caps at about 2.7 KB, and so is a key, in the record state.
MAX_NESTING = 32
MAX_PARTITION_BYTES = 1024
MAX_KEY_BYTES = 1024


_Read = Literal["added", "later", "next"]
# What an item of an event comes to: its key, its partition, and its record or the error that turned it down.
_Found = tuple[str, str | None, Record | None, Exception | None]


@dataclass
class _Part:
    """The records of a batch that go to one partition."""

    first_seq: int
    records: list[Record] = field(default_factory=list[Record])


def _printable(text: str) -> str:
    """What a database will take of `text`: it refuses a NUL and a lone surrogate, and a request can hold both."""
    return text.encode("utf-8", "replace").decode("utf-8").replace("\x00", "")


def _check_storable(value: Any, depth: int = 0) -> None:
    """Raise unless each part of a payload can be stored: as JSON in the state, as text in the quarantine, and as Parquet
    in the spool."""
    if depth > MAX_NESTING:
        raise ValueError(f"the payload is nested more than {MAX_NESTING} deep")
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("a text holds a NUL character")
        value.encode("utf-8")  # a lone surrogate raises
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("a number is not finite")
    elif isinstance(value, Mapping):
        for name, item in cast(Mapping[Any, Any], value).items():
            _check_storable(name)
            _check_storable(item, depth + 1)
    elif isinstance(value, list | tuple):
        for item in cast(Iterable[Any], value):
            _check_storable(item, depth + 1)


def _log_failure(dataset: str, error: Exception) -> None:
    """Log where it failed and not what it said, since a database's or a connector's message can quote a request."""
    where = "".join(traceback.format_tb(error.__traceback__))
    log.error(
        "sealer: dataset %s failed with %s, and will be tried again\n%s", dataset, type(error).__name__, where
    )


class _Batch:
    """What the events of one batch come to."""

    def __init__(self, dataset: Dataset, deps: Any) -> None:
        self._dataset = dataset
        self._deps = deps
        self.parts: dict[str, _Part] = {}
        self.settled: set[str] = (
            set()
        )  # keys whose latest record is good, and whose earlier rejection is over
        self.held: list[int] = []  # events that are quarantined whole, to be held once the rest has sealed
        self._last_part: dict[str, str] = {}  # where each key's latest record of the batch goes

    def reject(self, name: str, partition: str | None, error: Exception) -> None:
        message = redact(_printable(f"{name[:_MAX_NAME]}: {type(error).__name__}: {error}"))[:_MAX_ERROR]
        self._deps.store.quarantine.add(
            None, None, _printable(name)[:_MAX_NAME], message, self._dataset.name, partition
        )

    def hold(self, event: InboxEvent, error: Exception) -> None:
        self.reject(f"event:{event.event_seq}", None, error)
        self.held.append(event.event_seq)

    def read(self, source: PushSource, ctx: Context, event: InboxEvent) -> _Read:
        """Add the records of the event, or say that it is to be read again later, or that it belongs in the next batch.
        Whatever is wrong with it costs that event, or that record, and no more."""
        seq = event.event_seq
        try:
            found = list(source.receive(ctx, event.request))
        except PermanentError as error:
            log.warning("sealer: event %s of %s not read: %s", seq, self._dataset.name, type(error).__name__)
            self.hold(event, error)
            return "added"
        except Exception as error:  # noqa: BLE001 - a connector fails however it fails, and its tries decide
            tries = event.tries + 1
            log.warning(
                "sealer: event %s of %s not read, try %s: %s",
                seq,
                self._dataset.name,
                tries,
                type(error).__name__,
            )
            if tries < self._deps.retry.max_attempts:
                return "later"
            self.hold(
                event,
                PermanentError(f"still failing after {tries} attempts: {type(error).__name__}: {error}"),
            )
            return "added"
        found_records = _last_of_each_moving_key([self._record_of(item, seq) for item in found])
        if self._breaks_order(found_records):
            return "next"
        for name, partition, record, error in found_records:
            if record is None:
                self.reject(name, partition, cast(Exception, error))
                self.settled.discard(name)
            else:
                self.parts.setdefault(cast(str, partition), _Part(seq)).records.append(record)
                self._last_part[name] = cast(str, partition)
                self.settled.add(name)
        return "added"

    def read_all(
        self, source: PushSource, ctx: Context, events: Sequence[InboxEvent]
    ) -> tuple[int, _Read] | None:
        """Read the events in order, and stop at one to be read again later or to start the next batch with. Return
        where it stopped and why, or None."""
        for n, event in enumerate(events):
            found = self.read(source, ctx, event)
            if found != "added":
                return n, found
        return None

    def _breaks_order(self, found: Sequence[_Found]) -> bool:
        """Whether a record of a key would go to a version that seals before the one its latest record went to."""
        order = {partition: n for n, partition in enumerate(self.parts)}
        for name, partition, record, _ in found:
            last = self._last_part.get(name)
            if record is None or last is None or partition == last or partition not in order:
                continue
            if order[partition] < order[last]:
                return True
        return False

    def _record_of(self, item: Any, seq: int) -> _Found:
        """Return the record an item makes, or the error that turns it down, with its key and partition."""
        dataset = self._dataset
        name, partition = f"event:{seq}", None
        try:
            deleted = isinstance(item, Deleted)
            row = item.payload if deleted else item
            if not isinstance(row, Mapping):
                raise TypeError(f"a record is a {type(row).__name__}, not a mapping")
            payload = cast(Mapping[str, Any], row)
            name = make_key(payload, dataset.key)
            partition = dataset.partition.key_of(payload)
            if len(partition.encode("utf-8", "replace")) > MAX_PARTITION_BYTES:
                partition = None
                raise ValueError(f"the partition key is longer than {MAX_PARTITION_BYTES} bytes")
            if len(name.encode("utf-8", "replace")) > MAX_KEY_BYTES:
                raise ValueError(f"the key is longer than {MAX_KEY_BYTES} bytes")
            _check_storable(name)
            _check_storable(payload)
            if not deleted:  # a delete has only to name its record
                validate_row(dataset.schema, payload)
            check_columns(payload)
            record = Record(name, "delete" if deleted else "upsert", payload, row_hash(payload))
        except Exception as error:  # noqa: BLE001 - a bad record of a push source can fail in any way at all
            log.warning(
                "sealer: a record of event %s of %s rejected: %s", seq, dataset.name, type(error).__name__
            )
            return name, partition, None, error
        return name, partition, record, None

    def drop_what_arrow_refuses(self) -> None:
        """Arrow has limits of its own, such as the size of an integer. A table per record would be most of the cost of a
        batch, so each part is tried whole, and only a part that is refused is gone through a record at a time."""
        for partition, part in list(self.parts.items()):
            try:
                records_to_table(part.records)
            except Exception:  # noqa: BLE001 - see `_keep_what_arrow_takes`
                self._keep_what_arrow_takes(partition, part)

    def _keep_what_arrow_takes(self, partition: str, part: _Part) -> None:
        kept: list[Record] = []
        for record in part.records:
            try:
                records_to_table([record])
            except Exception as error:  # noqa: BLE001 - a bad record of a push source can fail in any way at all
                log.warning("sealer: a record of %s rejected: %s", self._dataset.name, type(error).__name__)
                self.reject(record.key, partition, error)
                self.settled.discard(record.key)
            else:
                kept.append(record)
                self.settled.add(record.key)
        part.records = kept
        if not kept:
            del self.parts[partition]


def _last_of_each_moving_key(found: Sequence[_Found]) -> list[_Found]:
    """Leave out the records of a key that goes to several partitions in one event, but for its last."""
    partitions: dict[str, set[str | None]] = {}
    last: dict[str, int] = {}
    for n, (name, partition, record, _) in enumerate(found):
        if record is not None:
            partitions.setdefault(name, set()).add(partition)
            last[name] = n
    return [
        item
        for n, item in enumerate(found)
        if item[2] is None or len(partitions[item[0]]) == 1 or last[item[0]] == n
    ]


def _with_moves(dataset: Dataset, parts: Mapping[str, _Part], deps: Any) -> None:
    """Say on each upsert which partition its key leaves, when the dataset keeps enough state to know."""
    if not dataset.keep_state:
        return
    keys = {record.key for part in parts.values() for record in part.records}
    where = {key: row.partition for key, row in deps.store.state.get_many(dataset.name, keys).items()}
    in_order = sorted(parts.items(), key=lambda item: item[1].first_seq)
    for partition, part in in_order:
        for n, record in enumerate(part.records):
            before = where.get(record.key)
            if record.op == "delete":
                where.pop(record.key, None)
                continue
            if before not in (None, partition):
                part.records[n] = replace(record, moved_from=before)
            where[record.key] = partition


def _write(
    dataset: Dataset, run_id: str, partition: str, part: _Part, writer: str, deps: Any
) -> VersionDraft:
    """Claim the version and write its chunks, over whatever a crashed attempt left."""
    version_id = stable_id(dataset.name, partition, str(part.first_seq))
    deps.store.versions.claim(version_id, writer, run_id, dataset.name, partition)
    deps.spool.delete_version(version_id, writer)
    size = spool_settings.CHUNK_ROWS
    for chunk_no, start in enumerate(range(0, len(part.records), size)):
        table = records_to_table(part.records[start : start + size])
        deps.spool.put_chunk(version_id, chunk_no, "data", table, writer)
    return VersionDraft(
        version_id=version_id,
        dataset=dataset.name,
        partition=partition,
        run_id=run_id,
        complete=False,
        upserts=sum(r.op == "upsert" for r in part.records),
        deletes=sum(r.op == "delete" for r in part.records),
        content_hash=content_hash(part.records, {}),
        schema_hash=schema_hash(version_id, deps),
        watermark=None,
        code_sha=deps.code_sha,
    )


def _drop_own_chunks(version_ids: Iterable[str], writer: str, deps: Any) -> None:
    """Remove what this attempt wrote, having lost or failed. A version another sealer has claimed since is that
    sealer's."""
    for version_id in version_ids:
        try:
            deps.spool.delete_version(version_id, writer)
        except (StaleWriter, VersionSealed):
            continue


_STATE_LOOKBACK = 50  # sealed versions looked at for state a crash left unwritten


def _state_marker(dataset: Dataset) -> str:
    return state_marker(dataset.name)


def _catch_up_state(dataset: Dataset, deps: Any) -> None:
    """Write the record state of sealed versions whose state a crash between sealing and `update_state` left unwritten
    (S11), before a new batch works out its moves from that state. `update_state` keeps the newest state of each key,
    so writing a version's state again changes nothing."""
    if not dataset.keep_state:
        return
    done = deps.store.overrides.get(_state_marker(dataset), STATE_MARK_KEY)
    behind = sorted(
        (
            v
            for v in deps.store.versions.list_versions(dataset.name, limit=_STATE_LOOKBACK)
            if v.status == "sealed" and v.seal_seq is not None and (done is None or v.seal_seq > int(done))
        ),
        key=lambda v: v.seal_seq,
    )
    for version in behind:
        update_state(dataset, version, deps)
    if behind:
        _mark_state(dataset, behind, deps)


def _mark_state(dataset: Dataset, versions: Sequence[Version], deps: Any) -> None:
    mark_state(dataset, versions, deps)


def _after_seal(
    runtime: Runtime, dataset: Dataset, sealed: Sequence[Version], routes: Sequence[str], batch: _Batch
) -> None:
    """Do what follows a seal. The versions are safe by now, so nothing failing here may look like a failed seal: a
    delivery that isn't submitted is the sweeper's, and state that isn't updated is written by the next batch (S11)."""
    deps = runtime.deps
    stated = True
    for version in sealed:
        try:
            deps.store.runs.set_run_status(version.run_id, "running")
            if dataset.keep_state:
                update_state(dataset, version, deps)
        except Exception as error:  # noqa: BLE001 - see above
            stated = False
            _log_failure(dataset.name, error)
        for route in routes:
            try:
                runtime.submit_apply(route, version.version_id)
            except Exception as error:  # noqa: BLE001 - see above
                _log_failure(dataset.name, error)
    if dataset.keep_state and stated and sealed:
        try:
            _mark_state(dataset, sealed, deps)
        except Exception as error:  # noqa: BLE001 - the next batch writes the state again, which is harmless
            _log_failure(dataset.name, error)
    try:
        deps.store.quarantine.close_extraction(dataset.name, batch.settled)
    except Exception as error:  # noqa: BLE001 - see above
        _log_failure(dataset.name, error)


def _flows_of(runtime: Runtime, dataset: Dataset) -> list[Flow]:
    return [flow for flow in runtime.flows() if flow.enabled and dataset in flow.datasets]


def _seal_batch(
    runtime: Runtime,
    dataset: Dataset,
    flows: Sequence[Flow],
    batch: _Batch,
    events: Sequence[InboxEvent],
    now: datetime,
) -> list[Version]:
    """Seal `events`, read into `batch`, as one batch, then hold those it quarantined whole. Return the versions."""
    deps = runtime.deps
    held = set(batch.held)
    to_seal = [e.event_seq for e in events if e.event_seq not in held]
    if not to_seal and not batch.parts:
        _hold(batch, deps)
        return []
    batch.drop_what_arrow_refuses()
    _catch_up_state(dataset, deps)
    _with_moves(dataset, batch.parts, deps)
    routes = list(dict.fromkeys(route.name for flow in flows for route in flow.routes))
    run_id = runtime.stream_run(flows[0].name, now) if batch.parts else ""
    writer = new_id()
    drafts: list[VersionDraft] = []
    written: list[str] = []
    try:
        for partition, part in sorted(batch.parts.items(), key=lambda item: item[1].first_seq):
            written.append(stable_id(dataset.name, partition, str(part.first_seq)))
            drafts.append(_write(dataset, run_id, partition, part, writer, deps))
        sealed = deps.store.inbox.seal_batch(to_seal, drafts, routes, writer)
    except Exception:  # lost or failed, what this attempt wrote is no one's
        try:
            _drop_own_chunks(written, writer, deps)
        except Exception as error:  # noqa: BLE001 - what failed first is what counts
            _log_failure(dataset.name, error)
        raise
    _after_seal(runtime, dataset, sealed, routes, batch)
    _hold(batch, deps)
    return sealed


def _hold(batch: _Batch, deps: Any) -> None:
    """Hold the events `batch` quarantined whole. One held or sealed since is another sealer's doing, and done already.
    A crash before this leaves them to be read, and quarantined, again."""
    for event_seq in batch.held:
        try:
            deps.store.inbox.hold(event_seq)
        except StaleWriter:
            continue


def _seal_split(
    runtime: Runtime,
    dataset: Dataset,
    flows: Sequence[Flow],
    ctx: Context,
    events: Sequence[InboxEvent],
    now: datetime,
    sealed: list[Version],
) -> bool:
    """Seal `events`, and add the versions to `sealed`. If they fail as a batch, seal each half in turn, down to the
    event that fails alone: it goes to the quarantine with its reason, and is held.

    Return whether it stopped at an event to be read again later, which leaves that one and those after it waiting.
    Losing to another sealer, or the store being away, is no event's fault: that goes up as it is, and the events wait.
    """
    deps = runtime.deps
    batch = _Batch(dataset, deps)
    stop = batch.read_all(cast(PushSource, dataset.source), ctx, events)
    taken = events if stop is None else events[: stop[0]]
    try:
        sealed += _seal_batch(runtime, dataset, flows, batch, taken, now)
    except (StaleWriter, VersionSealed):
        raise
    except Exception as error:
        if deps.store.is_unavailable(error):
            raise
        if len(taken) == 1:
            _quarantine_event(runtime, dataset, taken[0], error)
        else:
            half = len(taken) // 2
            if _seal_split(runtime, dataset, flows, ctx, taken[:half], now, sealed):
                return True
            if _seal_split(runtime, dataset, flows, ctx, taken[half:], now, sealed):
                return True
    if stop is None:
        return False
    at, why = stop
    if why == "next":
        return _seal_split(runtime, dataset, flows, ctx, events[at:], now, sealed)
    event = events[at]
    deps.store.inbox.defer(event.event_seq, now + deps.retry.delay(event.tries + 1, rand=lambda: 1.0))
    return True


def _quarantine_event(runtime: Runtime, dataset: Dataset, event: InboxEvent, error: Exception) -> None:
    log.warning("sealer: event %s of %s not stored: %s", event.event_seq, dataset.name, type(error).__name__)
    batch = _Batch(dataset, runtime.deps)
    batch.hold(event, error)
    _hold(batch, runtime.deps)


MAX_BATCH_BYTES = 64 * 1024 * 1024  # the bodies one batch holds in memory, at most (S20)


def _is_full(events: Sequence[InboxEvent], max_events: int, max_bytes: int) -> bool:
    return len(events) >= max_events or sum(len(e.request.body) for e in events) >= max_bytes


def _is_due(age: timedelta, full: bool, max_age: timedelta) -> bool:
    return full or age >= max_age


def _seal_dataset(
    runtime: Runtime,
    dataset: Dataset,
    sealed: list[Version],
    *,
    now: datetime,
    max_events: int,
    max_age: timedelta,
    max_bytes: int = MAX_BATCH_BYTES,
) -> None:
    """Add to `sealed` as each batch seals, so that a later batch failing doesn't lose sight of the earlier ones."""
    flows = _flows_of(runtime, dataset)
    if not flows or any(runtime.is_flow_paused(flow.name) for flow in flows):
        return  # the events keep, and retention keeps their days
    inbox = runtime.deps.store.inbox
    ctx: Context | None = None
    for _ in range(_MAX_BATCHES):
        age = inbox.oldest_age(dataset.name, now)
        if age is None:
            break
        events = inbox.take(dataset.name, max_events, max_bytes)
        full = _is_full(events, max_events, max_bytes)
        if not events or not _is_due(age, full, max_age):
            break
        if events[0].retry_at is not None and events[0].retry_at > now:
            break  # the first is to be read again later, and the others wait behind it
        if ctx is None:  # outside the events: a secret not set is not the events' fault
            ctx = runtime.make_context(cast(PushSource, dataset.source).connection)
        if _seal_split(runtime, dataset, flows, ctx, events, now, sealed):
            break
        if not full:
            break


def _push_datasets(runtime: Runtime) -> list[Dataset]:
    found = {d.name: d for flow in runtime.flows() for d in flow.datasets if d.strategy.name == _PUSH}
    return list(found.values())


def seal_once(
    runtime: Runtime,
    *,
    now: datetime,
    max_events: int = 500,
    max_age: timedelta = timedelta(seconds=1),
    max_bytes: int = MAX_BATCH_BYTES,
) -> list[Version]:
    """Seal what is due of each push dataset as of `now`, and return the new versions.

    One dataset failing is logged and leaves its events for the next time, and the others go on. Losing to another sealer
    is no failure at all.
    """
    sealed: list[Version] = []
    for dataset in _push_datasets(runtime):
        try:
            _seal_dataset(
                runtime, dataset, sealed, now=now, max_events=max_events, max_age=max_age, max_bytes=max_bytes
            )
        except (StaleWriter, VersionSealed) as error:
            log.info("sealer: another sealer is at dataset %s: %s", dataset.name, type(error).__name__)
        except Exception as error:  # noqa: BLE001 - the next tick tries again, and the other datasets mustn't wait
            _log_failure(dataset.name, error)
    return sealed
