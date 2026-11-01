"""Retention: drop what has outlived its time (design 7.3 and 7.4).

Spool chunks go by whole days, and a day that still holds a version somebody may need is kept, with the reason. Only
the newest snapshot of each partition outlives its day, since `snapshot_diff` needs it. Journal rows, deliveries and
version manifests go by age. The inbox is not here, because nothing writes one yet.
"""

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from feather_connect.storage.port import Store, Version
from feather_connect.storage.spool import Spool

_LOGS = timedelta(days=30)
_JOURNAL = timedelta(days=180)  # runs, tasks, deliveries and their attempts
_MANIFESTS = timedelta(days=400)
_BATCH = 500


@dataclass(frozen=True)
class RetentionReport:
    """What a pass did. `kept_days` maps each old day it kept to why."""

    dropped_days: list[date] = field(default_factory=list[date])
    kept_days: Mapping[date, str] = field(default_factory=dict[date, str])
    deleted_runs: int = 0
    deleted_logs: int = 0
    deleted_deliveries: int = 0
    deleted_snapshots: int = 0
    deleted_versions: int = 0


def _batches(items: Sequence[str]) -> Iterator[Sequence[str]]:
    for start in range(0, len(items), _BATCH):
        yield items[start : start + _BATCH]


def _open_among(store: Store, version_ids: Sequence[str]) -> set[str]:
    found: set[str] = set()
    for batch in _batches(version_ids):
        found |= store.ledger.open_versions(batch)
    return found


def _versions_among(store: Store, version_ids: Sequence[str]) -> list[Version]:
    return [v for batch in _batches(version_ids) for v in store.versions.get_many(batch)]


def _why_kept(store: Store, version_ids: Sequence[str]) -> str | None:
    """Return why the versions' chunks must stay, or None if nothing needs them."""
    reasons: list[str] = []
    if open_ := sorted(_open_among(store, version_ids)):
        reasons.append(f"{len(open_)} versions with an open delivery, such as {open_[0]}")
    if unsealed := sorted(v.version_id for v in _versions_among(store, version_ids) if v.status != "sealed"):
        reasons.append(f"{len(unsealed)} versions not sealed, such as {unsealed[0]}")
    return "; ".join(reasons) or None


def _drop_old_days(store: Store, spool: Spool, last_kept: date) -> tuple[list[date], dict[date, str]]:
    dropped: list[date] = []
    kept: dict[date, str] = {}
    for day in spool.days():
        if day >= last_kept:
            continue
        why = _why_kept(store, spool.versions_on(day))
        if why is None:
            spool.drop_day(day)
            dropped.append(day)
        else:
            kept[day] = why
    return dropped, kept


def kept_days(store: Store, spool: Spool, *, now: datetime, keep_days: int = 14) -> dict[date, str]:
    """Return the days older than `keep_days` that a pass would keep as of `now`, each with why. Changes nothing."""
    last_kept = (now - timedelta(days=keep_days)).date()
    found = {day: _why_kept(store, spool.versions_on(day)) for day in spool.days() if day < last_kept}
    return {day: why for day, why in found.items() if why is not None}


def _drop_old_snapshots(store: Store, spool: Spool, cutoff: datetime) -> int:
    """Delete the snapshots that a newer one of the same partition has replaced, once they are old and unneeded.

    The newest snapshot is the newest sealed version that has one: a version that changed nothing has none.
    """
    holders = _versions_among(store, spool.snapshot_versions())
    newest: dict[tuple[str, str], int] = {}
    for v in holders:
        if v.status == "sealed" and v.seal_seq is not None:
            newest[(v.dataset, v.partition)] = max(v.seal_seq, newest.get((v.dataset, v.partition), 0))
    stale = [
        v.version_id
        for v in holders
        if v.status == "sealed"
        and v.sealed_at is not None
        and v.sealed_at < cutoff
        and (v.seal_seq or 0) < newest[(v.dataset, v.partition)]
    ]
    held = _open_among(store, stale)  # a replace_partition delivery still reads its version's snapshot
    gone = [version_id for version_id in stale if version_id not in held]
    for version_id in gone:
        spool.expire_chunks(version_id, "snapshot")
    return len(gone)


def _drop_old_manifests(store: Store, spool: Spool, cutoff: datetime) -> int:
    """Delete old manifests, except any that still hold a snapshot, which is the last one of its partition."""
    gone = [
        v.version_id
        for v in store.versions.sealed_before(cutoff)
        if not spool.list_chunks(v.version_id, "snapshot")
    ]
    for version_id in gone:
        spool.expire_version(version_id)
    return store.versions.delete_many(gone)


def retention_once(store: Store, spool: Spool, *, now: datetime, keep_days: int = 14) -> RetentionReport:
    """Do one pass of retention as of `now`.

    Chunk days older than `keep_days` are dropped, unless a version on the day has a delivery that isn't closed or isn't
    sealed. Older snapshots are deleted, keeping the latest per (dataset, partition). Runs, tasks and deliveries go after
    180 days, task logs after 30 and version manifests after 400. Doing it again changes nothing.
    """
    dropped, kept = _drop_old_days(store, spool, (now - timedelta(days=keep_days)).date())
    snapshots = _drop_old_snapshots(store, spool, now - timedelta(days=keep_days))
    logs = store.runs.delete_logs_before(now - _LOGS)
    deliveries = store.ledger.delete_closed_before(now - _JOURNAL)
    runs = store.runs.delete_runs_before(now - _JOURNAL)
    versions = _drop_old_manifests(store, spool, now - _MANIFESTS)
    return RetentionReport(dropped, kept, runs, logs, deliveries, snapshots, versions)
