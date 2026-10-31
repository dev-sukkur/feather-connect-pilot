"""The store in Postgres. Every method is one transaction that starts by setting `feather.tenant_id`.

It passes the same `StoreContract` as `MemoryStore`. Rows are read as dicts (`dict_row`), so the pool must be
made with that row factory; `connect` does.
"""

import json
from collections.abc import Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, LiteralString

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from feather_connect.clock import Clock, SystemClock
from feather_connect.ids import new_id
from feather_connect.storage.port import (
    CLOSED,
    Attempt,
    BudgetSpec,
    BudgetStore,
    Delivery,
    DeliveryStatus,
    Grant,
    Ledger,
    Priority,
    QuarantineRow,
    QuarantineStore,
    RecordStateStore,
    Run,
    RunJournal,
    StateRow,
    Store,
    TaskRow,
    Version,
    VersionDraft,
    VersionSealed,
    VersionStore,
    WindowUsage,
    check_transition,
)
from feather_connect.storage.postgres.support import Pool, ensure_partition
from feather_connect.storage.spool import decode_json_value, encode_json_value

# The oldest delivery of a partition that is still open (I2). `status <> 'applied'` is repeated outside the
# list of closed statuses so that Postgres can see that the partial index `delivery_open_idx` applies.
OLDEST_OPEN_SQL: LiteralString = """
    select * from feather.delivery
    where tenant_id = %(tenant)s and route = %(route)s and partition = %(partition)s
      and status <> 'applied' and status <> all(%(closed)s)
    order by seal_seq limit 1"""

Row = DictRow


def _json(value: Any) -> Jsonb:
    """A value for a `jsonb` column. Types that JSON can't hold, such as datetimes, are tagged."""
    return Jsonb(value, dumps=lambda v: json.dumps(v, default=encode_json_value))


def _unjson(value: Any) -> Any:
    """Undo `_json` for a value read back from a `jsonb` column."""
    return json.loads(json.dumps(value), object_hook=decode_json_value)


class _Db:
    """The pool, the clock and the tenant, shared by the repositories."""

    def __init__(self, pool: Pool, clock: Clock, tenant: str) -> None:
        self.pool = pool
        self.clock = clock
        self.tenant = tenant
        self._months: set[str] = set()

    @contextmanager
    def tx(self) -> Generator[psycopg.Connection[Row]]:
        """A transaction for this tenant. It commits when the block ends and rolls back if it raises."""
        with self.pool.connection() as conn:
            conn.execute("select set_config('feather.tenant_id', %s, true)", (self.tenant,))
            yield conn

    def ensure_month(self, parent: str, at: datetime) -> None:
        """Make sure the monthly partition of `parent` that holds `at` exists, before a transaction needs it."""
        start = datetime(at.year, at.month, 1, tzinfo=UTC)
        end = datetime(at.year + (at.month == 12), at.month % 12 + 1, 1, tzinfo=UTC)
        ensure_partition(self.pool, parent, f"{parent}_{start:%Y%m}", start, end, self._months)


def _run(r: Row) -> Run:
    return Run(
        r["run_id"],
        r["flow"],
        r["trigger"],
        r["actor"],
        _unjson(r["params"]),
        r["status"],
        r["reason"],
        r["created_at"],
        r["updated_at"],
    )


def _task(r: Row) -> TaskRow:
    return TaskRow(
        r["task_id"],
        r["run_id"],
        r["kind"],
        r["ref"],
        r["dataset"],
        r["partition"],
        r["route"],
        r["worker"],
        r["code_sha"],
        r["status"],
        r["error"],
        r["started_at"],
        r["finished_at"],
    )


def _version(r: Row) -> Version:
    return Version(
        r["version_id"],
        r["dataset"],
        r["partition"],
        r["run_id"],
        r["complete"],
        r["upserts"],
        r["deletes"],
        r["content_hash"],
        r["schema_hash"],
        None if r["watermark"] is None else _unjson(r["watermark"]),
        r["code_sha"],
        r["status"],
        r["seal_seq"],
        r["sealed_at"],
    )


def _delivery(r: Row) -> Delivery:
    return Delivery(
        r["route"],
        r["version_id"],
        r["dataset"],
        r["partition"],
        r["seal_seq"],
        r["status"],
        r["attempts"],
        r["last_error"],
        r["rows_applied"],
        r["rows_rejected"],
        r["replays"],
        r["retries"],
    )


class _Runs(RunJournal):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def create_run(
        self,
        flow: str,
        trigger: str,
        actor: str,
        params: Mapping[str, Any],
        status: str = "queued",
        reason: str | None = None,
    ) -> str:
        run_id = new_id()
        now = self._db.clock.now()
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.run (tenant_id, run_id, flow, trigger, actor, params, status, reason,
                                            created_at, updated_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    self._db.tenant,
                    run_id,
                    flow,
                    trigger,
                    actor,
                    _json(dict(params)),
                    status,
                    reason,
                    now,
                    now,
                ),
            )
        return run_id

    def set_run_status(self, run_id: str, status: str, reason: str | None = None) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                """update feather.run set status = %s, reason = coalesce(%s, reason), updated_at = %s
                   where tenant_id = %s and run_id = %s""",
                (status, reason, self._db.clock.now(), self._db.tenant, run_id),
            )
        if changed.rowcount == 0:
            raise KeyError(run_id)

    def get_run(self, run_id: str) -> Run:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.run where tenant_id = %s and run_id = %s", (self._db.tenant, run_id)
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return _run(row)

    def list_runs(
        self, flow: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> list[Run]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.run
                   where tenant_id = %(tenant)s
                     and (%(flow)s::text is null or flow = %(flow)s)
                     and (%(status)s::text is null or status = %(status)s)
                     and (%(cursor)s::text is null or run_id < %(cursor)s)
                   order by run_id desc limit %(limit)s""",
                {"tenant": self._db.tenant, "flow": flow, "status": status, "cursor": cursor, "limit": limit},
            ).fetchall()
        return [_run(r) for r in rows]

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
        with self._db.tx() as conn:
            row = conn.execute(
                """insert into feather.task (tenant_id, task_id, run_id, kind, ref, dataset, partition, route,
                                             worker, code_sha, status, started_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'running', %s)
                   on conflict (tenant_id, run_id, ref) do nothing
                   returning task_id""",
                (
                    self._db.tenant,
                    new_id(),
                    run_id,
                    kind,
                    ref,
                    dataset,
                    partition,
                    route,
                    worker,
                    code_sha,
                    self._db.clock.now(),
                ),
            ).fetchone()
            if row is None:  # the task exists: a replay keeps its row
                row = conn.execute(
                    "select task_id from feather.task where tenant_id = %s and run_id = %s and ref = %s",
                    (self._db.tenant, run_id, ref),
                ).fetchone()
        assert row is not None
        return row["task_id"]

    def finish_task(self, task_id: str, status: str, error: str | None = None) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                """update feather.task set status = %s, error = %s, finished_at = %s
                   where tenant_id = %s and task_id = %s""",
                (status, error, self._db.clock.now(), self._db.tenant, task_id),
            )
        if changed.rowcount == 0:
            raise KeyError(task_id)

    def list_tasks(self, run_id: str) -> list[TaskRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select * from feather.task where tenant_id = %s and run_id = %s order by seq",
                (self._db.tenant, run_id),
            ).fetchall()
        return [_task(r) for r in rows]

    def log(self, task_id: str, level: str, message: str) -> None:
        now = self._db.clock.now()
        self._db.ensure_month("task_log", now)
        with self._db.tx() as conn:
            conn.execute(
                "insert into feather.task_log (tenant_id, task_id, at, level, message) values (%s, %s, %s, %s, %s)",
                (self._db.tenant, task_id, now, level, message),
            )

    def task_logs(self, task_id: str) -> list[str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select level, message from feather.task_log where tenant_id = %s and task_id = %s order by log_id",
                (self._db.tenant, task_id),
            ).fetchall()
        return [f"{r['level']}: {r['message']}" for r in rows]


class _Versions(VersionStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def stage(self, draft: VersionDraft) -> Version:
        with self._db.tx() as conn:
            row = conn.execute(
                """insert into feather.version as v (tenant_id, version_id, dataset, partition, run_id, complete,
                                                upserts, deletes, content_hash, schema_hash, watermark, code_sha)
                   values (%(tenant)s, %(id)s, %(dataset)s, %(partition)s, %(run)s, %(complete)s, %(upserts)s,
                           %(deletes)s, %(content)s, %(schema)s, %(watermark)s, %(sha)s)
                   on conflict (tenant_id, version_id) do update set
                       dataset = excluded.dataset, partition = excluded.partition, run_id = excluded.run_id,
                       complete = excluded.complete, upserts = excluded.upserts, deletes = excluded.deletes,
                       content_hash = excluded.content_hash, schema_hash = excluded.schema_hash,
                       watermark = excluded.watermark, code_sha = excluded.code_sha
                   where v.status = 'staged'
                   returning *""",
                {
                    "tenant": self._db.tenant,
                    "id": draft.version_id,
                    "dataset": draft.dataset,
                    "partition": draft.partition,
                    "run": draft.run_id,
                    "complete": draft.complete,
                    "upserts": draft.upserts,
                    "deletes": draft.deletes,
                    "content": draft.content_hash,
                    "schema": draft.schema_hash,
                    "watermark": None if draft.watermark is None else _json(draft.watermark),
                    "sha": draft.code_sha,
                },
            ).fetchone()
        if row is None:  # the conflicting row is sealed, so the update did not apply
            raise VersionSealed(f"version {draft.version_id} is sealed")
        return _version(row)

    def seal(self, version_ids: Sequence[str], routes: Sequence[str]) -> list[Version]:
        with self._db.tx() as conn:
            # One sealer at a time, so that seal_seq follows commit order and a reader never sees a lower
            # number appear after a higher one.
            conn.execute("select pg_advisory_xact_lock(hashtext('feather.seal'))")
            found = {
                r["version_id"]: r
                for r in conn.execute(
                    """select * from feather.version where tenant_id = %s and version_id = any(%s)
                       order by version_id for update""",
                    (self._db.tenant, list(version_ids)),
                ).fetchall()
            }
            if missing := next((v for v in version_ids if v not in found), None):
                raise KeyError(f"no such version: {missing}")  # before sealing any, so it is all or nothing
            now = self._db.clock.now()
            for version_id in version_ids:
                if found[version_id]["status"] == "sealed":
                    continue
                sealed = conn.execute(
                    """update feather.version
                       set status = 'sealed', seal_seq = nextval('feather.seal_seq'), sealed_at = %s
                       where tenant_id = %s and version_id = %s returning *""",
                    (now, self._db.tenant, version_id),
                ).fetchone()
                assert sealed is not None
                found[version_id] = sealed
                for route in routes:
                    conn.execute(
                        """insert into feather.delivery (tenant_id, route, version_id, dataset, partition,
                                                         seal_seq, status, updated_at)
                           values (%s, %s, %s, %s, %s, %s, 'pending', %s)""",
                        (
                            self._db.tenant,
                            route,
                            version_id,
                            sealed["dataset"],
                            sealed["partition"],
                            sealed["seal_seq"],
                            now,
                        ),
                    )
        return [_version(found[v]) for v in version_ids]

    def discard_staged(self, version_id: str) -> None:
        with self._db.tx() as conn:
            conn.execute(
                "delete from feather.version where tenant_id = %s and version_id = %s and status = 'staged'",
                (self._db.tenant, version_id),
            )
            sealed = conn.execute(
                "select 1 from feather.version where tenant_id = %s and version_id = %s",
                (self._db.tenant, version_id),
            ).fetchone()
        if sealed is not None:
            raise VersionSealed(f"version {version_id} is sealed")

    def get(self, version_id: str) -> Version:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.version where tenant_id = %s and version_id = %s",
                (self._db.tenant, version_id),
            ).fetchone()
        if row is None:
            raise KeyError(version_id)
        return _version(row)

    def latest_sealed(
        self, dataset: str, partition: str, before_seal_seq: int | None = None
    ) -> Version | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select * from feather.version
                   where tenant_id = %(tenant)s and dataset = %(dataset)s and partition = %(partition)s
                     and status = 'sealed' and (%(before)s::bigint is null or seal_seq < %(before)s)
                   order by seal_seq desc limit 1""",
                {
                    "tenant": self._db.tenant,
                    "dataset": dataset,
                    "partition": partition,
                    "before": before_seal_seq,
                },
            ).fetchone()
        return None if row is None else _version(row)

    def since_watermark(self, dataset: str, partition: str, since: Any) -> list[Version]:
        # Watermarks are compared in Python, as the in-memory store does: they may be numbers, text or datetimes.
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.version
                   where tenant_id = %s and dataset = %s and partition = %s and status = 'sealed'
                     and watermark is not null
                   order by seal_seq desc""",
                (self._db.tenant, dataset, partition),
            ).fetchall()
        return [v for v in map(_version, rows) if v.watermark >= since]

    def latest_watermark(self, dataset: str) -> Any | None:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select watermark from feather.version
                   where tenant_id = %s and dataset = %s and status = 'sealed' and watermark is not null""",
                (self._db.tenant, dataset),
            ).fetchall()
        return max((_unjson(r["watermark"]) for r in rows), default=None)

    def register_slices(self, run_id: str, dataset: str, partitions: Iterable[str]) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.run_slice (tenant_id, run_id, dataset, partition)
                   select %s, %s, %s, unnest(%s::text[])
                   on conflict do nothing""",  # a replay must not undo finished slices
                (self._db.tenant, run_id, dataset, list(partitions)),
            )

    def finish_slice(self, run_id: str, dataset: str, partition: str) -> bool:
        with self._db.tx() as conn:
            # Lock the run's slices first: two slices finishing together must not each miss the other's.
            slices = conn.execute(
                """select partition, done from feather.run_slice
                   where tenant_id = %s and run_id = %s and dataset = %s order by partition for update""",
                (self._db.tenant, run_id, dataset),
            ).fetchall()
            if partition not in {s["partition"] for s in slices}:
                raise KeyError(f"slice {partition} is not registered for run {run_id}, dataset {dataset}")
            conn.execute(
                """update feather.run_slice set done = true
                   where tenant_id = %s and run_id = %s and dataset = %s and partition = %s""",
                (self._db.tenant, run_id, dataset, partition),
            )
        return all(s["done"] or s["partition"] == partition for s in slices)

    def slices_for_run(self, run_id: str, dataset: str) -> list[str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select partition from feather.run_slice where tenant_id = %s and run_id = %s and dataset = %s order by seq",
                (self._db.tenant, run_id, dataset),
            ).fetchall()
        return [r["partition"] for r in rows]

    def staged_for_run(self, run_id: str, dataset: str) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.version
                   where tenant_id = %s and run_id = %s and dataset = %s and status = 'staged'
                   order by stage_seq""",
                (self._db.tenant, run_id, dataset),
            ).fetchall()
        return [_version(r) for r in rows]

    def sealed_for_run(self, run_id: str, dataset: str) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.version
                   where tenant_id = %s and run_id = %s and dataset = %s and status = 'sealed'
                   order by seal_seq""",
                (self._db.tenant, run_id, dataset),
            ).fetchall()
        return [_version(r) for r in rows]

    def list_versions(self, dataset: str, partition: str | None = None, limit: int = 100) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.version
                   where tenant_id = %(tenant)s and dataset = %(dataset)s
                     and (%(partition)s::text is null or partition = %(partition)s)
                   order by stage_seq desc limit %(limit)s""",
                {"tenant": self._db.tenant, "dataset": dataset, "partition": partition, "limit": limit},
            ).fetchall()
        return [_version(r) for r in rows]


class _Ledger(Ledger):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def oldest_open(self, route: str, partition: str) -> Delivery | None:
        with self._db.tx() as conn:
            row = conn.execute(
                OLDEST_OPEN_SQL,
                {
                    "tenant": self._db.tenant,
                    "route": route,
                    "partition": partition,
                    "closed": sorted(CLOSED),
                },
            ).fetchone()
        return None if row is None else _delivery(row)

    def get(self, route: str, version_id: str) -> Delivery:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.delivery where tenant_id = %s and route = %s and version_id = %s",
                (self._db.tenant, route, version_id),
            ).fetchone()
        if row is None:
            raise KeyError((route, version_id))
        return _delivery(row)

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
        now = self._db.clock.now()
        self._db.ensure_month("delivery_attempt", now)
        with self._db.tx() as conn:
            row = conn.execute(
                """select * from feather.delivery where tenant_id = %s and route = %s and version_id = %s
                   for update""",
                (self._db.tenant, route, version_id),
            ).fetchone()
            if row is None:
                raise KeyError((route, version_id))
            current = _delivery(row)
            check_transition(current.status, to)
            applied = to == "applied"
            updated = conn.execute(
                """update feather.delivery set status = %(to)s, attempts = attempts + %(started)s::int,
                       last_error = case when %(applied)s then null else coalesce(%(error)s, last_error) end,
                       rows_applied = case when %(applied)s or %(rows_applied)s <> 0
                                           then %(rows_applied)s else rows_applied end,
                       rows_rejected = case when %(applied)s or %(rows_rejected)s <> 0
                                            then %(rows_rejected)s else rows_rejected end,
                       replays = replays + %(replayed)s::int, retries = retries + %(retried)s::int, updated_at = %(now)s
                   where tenant_id = %(tenant)s and route = %(route)s and version_id = %(version)s
                   returning *""",
                {
                    "to": to,
                    "started": to == "running",
                    "applied": applied,
                    "error": error,
                    "rows_applied": rows_applied,
                    "rows_rejected": rows_rejected,
                    "replayed": current.status == "applied" and to == "pending",
                    "retried": current.status == "failed" and to == "pending",
                    "now": now,
                    "tenant": self._db.tenant,
                    "route": route,
                    "version": version_id,
                },
            ).fetchone()
            conn.execute(
                """insert into feather.delivery_attempt (tenant_id, route, version_id, from_status, to_status,
                                                         actor, reason, error, rows_applied, rows_rejected,
                                                         code_sha, at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    self._db.tenant,
                    route,
                    version_id,
                    current.status,
                    to,
                    actor,
                    reason,
                    error,
                    rows_applied,
                    rows_rejected,
                    code_sha,
                    now,
                ),
            )
        assert updated is not None
        return _delivery(updated)

    def attempts(self, route: str, version_id: str) -> list[Attempt]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.delivery_attempt
                   where tenant_id = %s and route = %s and version_id = %s order by attempt_id""",
                (self._db.tenant, route, version_id),
            ).fetchall()
        return [
            Attempt(
                r["route"],
                r["version_id"],
                r["from_status"],
                r["to_status"],
                r["actor"],
                r["reason"],
                r["error"],
                r["rows_applied"],
                r["rows_rejected"],
                r["code_sha"],
                r["at"],
            )
            for r in rows
        ]

    def list_deliveries(
        self,
        route: str | None = None,
        status: DeliveryStatus | None = None,
        partition: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> list[Delivery]:
        after_seq: int | None = None
        after_route = ""
        if cursor is not None:
            seq, _, after_route = cursor.partition(":")
            after_seq = int(seq)
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.delivery
                   where tenant_id = %(tenant)s
                     and (%(route)s::text is null or route = %(route)s)
                     and (%(status)s::text is null or status = %(status)s)
                     and (%(partition)s::text is null or partition = %(partition)s)
                     and (%(after)s::bigint is null or (seal_seq, route) > (%(after)s, %(after_route)s))
                   order by seal_seq, route limit %(limit)s""",
                {
                    "tenant": self._db.tenant,
                    "route": route,
                    "status": status,
                    "partition": partition,
                    "after": after_seq,
                    "after_route": after_route,
                    "limit": limit,
                },
            ).fetchall()
        return [_delivery(r) for r in rows]

    def all_applied(self) -> bool:
        with self._db.tx() as conn:
            row = conn.execute(
                "select not exists (select 1 from feather.delivery where tenant_id = %s and status <> 'applied') as ok",
                (self._db.tenant,),
            ).fetchone()
        assert row is not None
        return row["ok"]


class _State(RecordStateStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def get_many(self, dataset: str, keys: Iterable[str]) -> dict[str, StateRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select * from feather.record_state where tenant_id = %s and dataset = %s and key = any(%s)",
                (self._db.tenant, dataset, list(keys)),
            ).fetchall()
        return {
            r["key"]: StateRow(
                r["key"], r["partition"], int(r["row_hash"]), _unjson(r["payload"]), r["last_version"]
            )
            for r in rows
        }

    def put_many(self, dataset: str, rows: Iterable[StateRow]) -> None:
        with self._db.tx() as conn, conn.cursor() as cursor:
            cursor.executemany(
                """insert into feather.record_state (tenant_id, dataset, key, partition, row_hash, payload,
                                                         last_version)
                       values (%s, %s, %s, %s, %s, %s, %s)
                       on conflict (tenant_id, dataset, key) do update set
                           partition = excluded.partition, row_hash = excluded.row_hash,
                           payload = excluded.payload, last_version = excluded.last_version""",
                [
                    (
                        self._db.tenant,
                        dataset,
                        r.key,
                        r.partition,
                        r.row_hash,
                        _json(dict(r.payload)),
                        r.last_version,
                    )
                    for r in rows
                ],
            )

    def delete_many(self, dataset: str, keys: Iterable[str]) -> None:
        with self._db.tx() as conn:
            conn.execute(
                "delete from feather.record_state where tenant_id = %s and dataset = %s and key = any(%s)",
                (self._db.tenant, dataset, list(keys)),
            )


class _Quarantine(QuarantineStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def add(
        self,
        route: str | None,
        version_id: str | None,
        key: str,
        error: str,
        dataset: str | None = None,
        partition: str | None = None,
    ) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.quarantine (tenant_id, route, version_id, key, dataset, partition, error,
                                                   status, updated_at)
                   values (%s, %s, %s, %s, %s, %s, %s, 'open', %s)
                   on conflict (tenant_id, route, version_id, key, dataset) do update set
                       partition = excluded.partition, error = excluded.error, status = 'open',
                       updated_at = excluded.updated_at""",
                (self._db.tenant, route, version_id, key, dataset, partition, error, self._db.clock.now()),
            )

    def open_extraction_keys(self, dataset: str, keys: Iterable[str], outside: str | None = None) -> set[str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct key from feather.quarantine
                   where tenant_id = %(tenant)s and status = 'open' and route is null and dataset = %(dataset)s
                     and key = any(%(keys)s)
                     and (%(outside)s::text is null or partition is distinct from %(outside)s)""",
                {"tenant": self._db.tenant, "dataset": dataset, "keys": list(keys), "outside": outside},
            ).fetchall()
        return {r["key"] for r in rows}

    def close_extraction(self, dataset: str, keys: Iterable[str]) -> int:
        with self._db.tx() as conn:
            closed = conn.execute(
                """update feather.quarantine set status = 'superseded', updated_at = %s
                   where tenant_id = %s and status = 'open' and route is null and dataset = %s
                     and key = any(%s)""",
                (self._db.clock.now(), self._db.tenant, dataset, list(keys)),
            )
        return closed.rowcount

    def supersede(self, route: str, key: str, before_seal_seq: int) -> int:
        with self._db.tx() as conn:
            closed = conn.execute(
                """update feather.quarantine q set status = 'superseded', updated_at = %s
                   from feather.version v
                   where q.tenant_id = %s and q.route = %s and q.key = %s and q.status = 'open'
                     and v.tenant_id = q.tenant_id and v.version_id = q.version_id and v.seal_seq < %s""",
                (self._db.clock.now(), self._db.tenant, route, key, before_seal_seq),
            )
        return closed.rowcount

    def list_open(self, route: str | None = None) -> list[QuarantineRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.quarantine
                   where tenant_id = %s and status = 'open' and (%s::text is null or route = %s)
                   order by quarantine_id""",
                (self._db.tenant, route, route),
            ).fetchall()
        return [
            QuarantineRow(
                r["route"], r["version_id"], r["key"], r["error"], r["status"], r["dataset"], r["partition"]
            )
            for r in rows
        ]


class _Budgets(BudgetStore):
    """Placeholder until the budgets task."""

    def __init__(self, db: _Db) -> None:
        self._db = db

    def define(self, spec: BudgetSpec) -> None:
        raise NotImplementedError

    def acquire(self, name: str, units: int, priority: Priority = "normal") -> Grant:
        raise NotImplementedError

    def usage(self, name: str) -> list[WindowUsage]:
        raise NotImplementedError


class PostgresStore(Store):
    """A `Store` that keeps everything in Postgres, for one tenant. `clock` defaults to the system clock."""

    def __init__(self, pool: Pool, clock: Clock | None = None, tenant: str = "default") -> None:
        db = _Db(pool, clock or SystemClock(), tenant)
        super().__init__(
            runs=_Runs(db),
            versions=_Versions(db),
            ledger=_Ledger(db),
            state=_State(db),
            quarantine=_Quarantine(db),
            budgets=_Budgets(db),
        )
