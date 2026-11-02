"""The sealer: turns what waits in the inbox into versions, without a run per event (design 6.4).

A batch is due when the oldest event has waited `max_age` or there are `max_events` of them. Each partition the batch
touches gets one incomplete version, named after its first event, so that doing a batch over gives the same names:

    1. claim the versions, and write their chunks under the claim (a sealer it overtakes can write no more);
    2. `inbox.seal_batch`: in one transaction, mark the events sealed, seal the versions and open their deliveries;
    3. submit the applies. A crash before this leaves pending deliveries, which is what the sweeper looks for.

So two sealers, or a crash and a rerun, give one version per first event. Each loses in one of three ways: its claim is
refused because the version is sealed, its write or its seal is refused because another claimed the version after it, or
`seal_batch` is refused because an event is sealed already, whatever the name of the version.

What comes in a webhook is untrusted, and one bad event must not hold up the rest for ever, so whatever can't be read or
stored goes to the quarantine and its event is sealed with the others. All the records of a batch stay in the version in
order, including several of one key: a keyed route ends up with the last, and an append route is owed each of them.

Nothing logged here carries a message of an error, which may quote a request. The quarantine keeps the message, capped
and redacted, for someone who can see the data anyway.
"""

import logging
import math
import traceback
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, cast

from feather_connect.core.connector import Deleted, PushSource
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset
from feather_connect.core.flow import Flow
from feather_connect.core.records import Record, make_key, row_hash
from feather_connect.ids import new_id, stable_id
from feather_connect.logs import redact
from feather_connect.runtime.extract import (
    check_columns,
    content_hash,
    schema_hash,
    update_state,
    validate_row,
)
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage import spool as spool_settings
from feather_connect.storage.port import InboxEvent, StaleWriter, Version, VersionDraft, VersionSealed
from feather_connect.storage.spool import records_to_table

log = logging.getLogger(__name__)

_PUSH = "push"
_MAX_BATCHES = 20  # a tick that a backlog can't keep for long, so that the other datasets get their turn
_MAX_ERROR = 500
_MAX_NAME = 200


@dataclass
class _Part:
    """The records of a batch that go to one partition."""

    first_seq: int
    records: list[Record] = field(default_factory=list[Record])


def _printable(text: str) -> str:
    """What a database will take of `text`: it refuses a NUL and a lone surrogate, and a request can hold both."""
    return text.encode("utf-8", "replace").decode("utf-8").replace("\x00", "")


def _check_storable(value: Any) -> None:
    """Raise unless each part of a payload can be stored: as JSON in the state, and as text in the quarantine."""
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
            _check_storable(item)
    elif isinstance(value, list | tuple):
        for item in cast(Iterable[Any], value):
            _check_storable(item)


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

    def _reject(self, name: str, partition: str | None, error: Exception) -> None:
        message = redact(_printable(f"{name}: {type(error).__name__}: {error}"))[:_MAX_ERROR]
        self._deps.store.quarantine.add(
            None, None, _printable(name)[:_MAX_NAME], message, self._dataset.name, partition
        )

    def read(self, source: PushSource, ctx: Context, event: InboxEvent) -> None:
        """Add the records of the event. Whatever is wrong with it costs that event, or that record, and no more."""
        seq = event.event_seq
        try:
            found = list(source.receive(ctx, event.request))
        except Exception as error:  # noqa: BLE001 - a connector fails on bad input however it fails
            log.warning("sealer: event %s of %s not read: %s", seq, self._dataset.name, type(error).__name__)
            self._reject(f"event:{seq}", None, error)
            return
        for item in found:
            self._add(item, seq)

    def _add(self, item: Any, seq: int) -> None:
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
            self._reject(name, partition, error)
            self.settled.discard(name)
            return
        self.parts.setdefault(partition, _Part(seq)).records.append(record)
        self.settled.add(name)

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
                self._reject(record.key, partition, error)
                self.settled.discard(record.key)
            else:
                kept.append(record)
                self.settled.add(record.key)
        part.records = kept
        if not kept:
            del self.parts[partition]


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
    """Remove what this attempt wrote, having lost. A version another sealer has claimed since is that sealer's."""
    for version_id in version_ids:
        try:
            deps.spool.delete_version(version_id, writer)
        except (StaleWriter, VersionSealed):
            continue


def _after_seal(
    runtime: Runtime, dataset: Dataset, sealed: Sequence[Version], routes: Sequence[str], batch: _Batch
) -> None:
    """Do what follows a seal. The versions are safe by now, so nothing failing here may look like a failed seal: a
    delivery that isn't submitted is the sweeper's, and state that isn't updated catches up with the key's next event."""
    deps = runtime.deps
    for version in sealed:
        try:
            deps.store.runs.set_run_status(version.run_id, "running")
            if dataset.keep_state:
                update_state(dataset, version, deps)
        except Exception as error:  # noqa: BLE001 - see above
            _log_failure(dataset.name, error)
        for route in routes:
            try:
                runtime.submit_apply(route, version.version_id)
            except Exception as error:  # noqa: BLE001 - see above
                _log_failure(dataset.name, error)
    try:
        deps.store.quarantine.close_extraction(dataset.name, batch.settled)
    except Exception as error:  # noqa: BLE001 - see above
        _log_failure(dataset.name, error)


def _flows_of(runtime: Runtime, dataset: Dataset) -> list[Flow]:
    return [flow for flow in runtime.flows() if flow.enabled and dataset in flow.datasets]


def _seal_batch(
    runtime: Runtime, dataset: Dataset, flows: Sequence[Flow], events: Sequence[InboxEvent], now: datetime
) -> list[Version]:
    """Seal `events` as one batch, and return the versions."""
    deps = runtime.deps
    source = cast(PushSource, dataset.source)
    ctx = runtime.make_context(
        source.connection
    )  # outside the events: a secret not set is not the events' fault
    batch = _Batch(dataset, deps)
    for event in events:
        batch.read(source, ctx, event)
    batch.drop_what_arrow_refuses()
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
        sealed = deps.store.inbox.seal_batch([e.event_seq for e in events], drafts, routes, writer)
    except (StaleWriter, VersionSealed):
        _drop_own_chunks(written, writer, deps)
        raise
    _after_seal(runtime, dataset, sealed, routes, batch)
    return sealed


def _is_due(age: timedelta, count: int, max_events: int, max_age: timedelta) -> bool:
    return count >= max_events or age >= max_age


def _seal_dataset(
    runtime: Runtime,
    dataset: Dataset,
    sealed: list[Version],
    *,
    now: datetime,
    max_events: int,
    max_age: timedelta,
) -> None:
    """Add to `sealed` as each batch seals, so that a later batch failing doesn't lose sight of the earlier ones."""
    flows = _flows_of(runtime, dataset)
    if not flows or any(runtime.is_flow_paused(flow.name) for flow in flows):
        return  # the events keep, and retention keeps their days
    inbox = runtime.deps.store.inbox
    for _ in range(_MAX_BATCHES):
        age = inbox.oldest_age(dataset.name, now)
        if age is None:
            break
        events = inbox.take(dataset.name, max_events)
        if not events or not _is_due(age, len(events), max_events, max_age):
            break
        sealed += _seal_batch(runtime, dataset, flows, events, now)
        if len(events) < max_events:
            break


def _push_datasets(runtime: Runtime) -> list[Dataset]:
    found = {d.name: d for flow in runtime.flows() for d in flow.datasets if d.strategy.name == _PUSH}
    return list(found.values())


def seal_once(
    runtime: Runtime, *, now: datetime, max_events: int = 500, max_age: timedelta = timedelta(seconds=1)
) -> list[Version]:
    """Seal what is due of each push dataset as of `now`, and return the new versions.

    One dataset failing is logged and leaves its events for the next time, and the others go on. Losing to another sealer
    is no failure at all.
    """
    sealed: list[Version] = []
    for dataset in _push_datasets(runtime):
        try:
            _seal_dataset(runtime, dataset, sealed, now=now, max_events=max_events, max_age=max_age)
        except (StaleWriter, VersionSealed) as error:
            log.info("sealer: another sealer is at dataset %s: %s", dataset.name, type(error).__name__)
        except Exception as error:  # noqa: BLE001 - the next tick tries again, and the other datasets mustn't wait
            _log_failure(dataset.name, error)
    return sealed
