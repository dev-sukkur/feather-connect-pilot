"""The diff of two versions of a partition (M2 plan 2 O5): the keys the later one added, removed and changed, and for
each changed key the fields that changed, before and after.

A version's data chunks hold what changed since the partition's version before it, so the diff nets the changes of
every version after the earlier one up to the later one, and compares each key with what the key index says it was
before: a key that didn't exist and now does is added, one that did and is now deleted is removed. PII fields are
masked in the changes as everywhere else (O4)."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc

from feather_connect.core.records import Record
from feather_connect.storage.port import KeySighting, Version
from feather_connect.storage.spool import table_to_records
from feather_server.services.pii import MASK

if TYPE_CHECKING:
    from feather_server.app import AppState

KEYS_READ = 10_000  # changed keys a diff reads: past these it says it is truncated
CHANGES_SHOWN = 500  # keys of each kind in an answer
VERSIONS_SPANNED = 100  # versions between the two that a diff nets


@dataclass(frozen=True)
class FieldChange:
    field: str
    before: Any
    after: Any


@dataclass(frozen=True)
class KeyChange:
    key: str
    fields: tuple[FieldChange, ...]


@dataclass(frozen=True)
class VersionDiff:
    """`before` is None for a partition's first version: all it holds is added."""

    before: Version | None
    after: Version
    added: tuple[str, ...]
    removed: tuple[str, ...]
    changed: tuple[KeyChange, ...]
    unchanged: int
    counts: Mapping[str, int]  # added, removed, changed: all of them, beyond the ones shown
    truncated: bool  # more keys changed than a diff reads


class NotComparable(ValueError):
    """The two versions are not of one partition, are one, are out of order, or one is not sealed."""


def _between(state: "AppState", before: Version | None, after: Version) -> list[Version]:
    """The partition's sealed versions after `before` up to `after`, oldest first."""
    if before is None:
        return [after]
    assert before.seal_seq is not None and after.seal_seq is not None
    found = [
        v
        for v in state.store.versions.list_versions(
            after.dataset, after.partition, limit=VERSIONS_SPANNED + 1
        )
        if v.seal_seq is not None and before.seal_seq < v.seal_seq <= after.seal_seq
    ]
    if len(found) > VERSIONS_SPANNED:
        raise NotComparable(f"more than {VERSIONS_SPANNED} versions lie between the two: compare closer ones")
    return sorted(found, key=lambda v: v.seal_seq or 0)


def _changes(state: "AppState", versions: list[Version]) -> tuple[dict[str, Record], bool]:
    """Each key's last change across the versions, oldest version first."""
    found: dict[str, Record] = {}
    spool = state.spool
    for version in versions:
        for chunk in spool.list_chunks(version.version_id):
            try:
                table = spool.get_chunk(version.version_id, chunk.chunk_no)
            except KeyError:  # retention took it
                continue
            for record in table_to_records(table):
                if record.key not in found and len(found) >= KEYS_READ:
                    return found, True
                found[record.key] = record
    return found, False


def _payloads(state: "AppState", sightings: Mapping[str, KeySighting]) -> dict[str, Mapping[str, Any]]:
    """The payload each sighting names, reading each chunk once."""
    by_chunk: dict[tuple[str, int], list[str]] = {}
    for key, s in sightings.items():
        by_chunk.setdefault((s.version_id, s.chunk_no), []).append(key)
    found: dict[str, Mapping[str, Any]] = {}
    for (version_id, chunk_no), keys in by_chunk.items():
        try:
            table = state.spool.get_chunk(version_id, chunk_no)
        except KeyError:
            continue
        wanted = pa.array(keys, pa.string())
        mine = table.filter(pc.is_in(table.column("_fc_key"), value_set=wanted))  # pyright: ignore[reportAttributeAccessIssue]
        found.update((r.key, r.payload) for r in table_to_records(mine))
    return found


def _fields(
    before: Mapping[str, Any], after: Mapping[str, Any], hidden: frozenset[str]
) -> tuple[FieldChange, ...]:
    def show(field: str, value: Any) -> Any:
        return MASK if field in hidden and value is not None else value

    names = list(before) + [n for n in after if n not in before]
    return tuple(
        FieldChange(n, show(n, before.get(n)), show(n, after.get(n)))
        for n in names
        if before.get(n) != after.get(n)
    )


def diff_versions(
    state: "AppState", version_id: str, against: str | None = None, *, hidden: frozenset[str] = frozenset()
) -> VersionDiff:
    """The diff of the version against the earlier `against`, or against the partition's version sealed before it.
    Raises `KeyError` for an unknown version and `NotComparable` for two that can't be compared."""
    versions = state.store.versions
    after = versions.get(version_id)
    if after.seal_seq is None:
        raise NotComparable(f"version {version_id!r} is not sealed")
    if against is None:
        before = versions.latest_sealed(after.dataset, after.partition, after.seal_seq)
    else:
        before = versions.get(against)
        if before.seal_seq is None:
            raise NotComparable(f"version {against!r} is not sealed")
        if (before.dataset, before.partition) != (after.dataset, after.partition):
            raise NotComparable("the two versions are not of one partition")
        if before.seal_seq >= after.seal_seq:
            raise NotComparable("compare a version with one sealed before it")
    changes, truncated = _changes(state, _between(state, before, after))
    floor = (
        after.seal_seq if before is None else (before.seal_seq or 0) + 1
    )  # what each key was as of `before`
    earlier = versions.earlier_sightings(after.dataset, list(changes), floor)
    held = {k: s for k, s in earlier.items() if s.op != "delete" and s.partition == after.partition}
    was = _payloads(
        state,
        {k: s for k, s in held.items() if changes[k].op != "delete" and s.row_hash != changes[k].row_hash},
    )
    added: list[str] = []
    removed: list[str] = []
    changed: list[KeyChange] = []
    unchanged = 0
    for key, record in sorted(changes.items()):
        if record.op == "delete":
            if key in held:
                removed.append(key)
        elif key not in held:
            added.append(key)
        elif held[key].row_hash != record.row_hash:
            changed.append(KeyChange(key, _fields(was.get(key, {}), record.payload, hidden)))
        else:
            unchanged += 1
    return VersionDiff(
        before,
        after,
        tuple(added[:CHANGES_SHOWN]),
        tuple(removed[:CHANGES_SHOWN]),
        tuple(changed[:CHANGES_SHOWN]),
        unchanged,
        {"added": len(added), "removed": len(removed), "changed": len(changed)},
        truncated,
    )


def as_json(diff: VersionDiff) -> dict[str, Any]:
    return {
        "dataset": diff.after.dataset,
        "partition": diff.after.partition,
        "before": None if diff.before is None else diff.before.version_id,
        "after": diff.after.version_id,
        "added": list(diff.added),
        "removed": list(diff.removed),
        "changed": [
            {
                "key": c.key,
                "fields": [{"field": f.field, "before": f.before, "after": f.after} for f in c.fields],
            }
            for c in diff.changed
        ],
        "unchanged": diff.unchanged,
        "counts": dict(diff.counts),
        "truncated": diff.truncated,
    }
