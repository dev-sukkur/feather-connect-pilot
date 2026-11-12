"""The store in Postgres. Every method is one transaction that starts by setting `feather.tenant_id`.

It passes the same `StoreContract` as `MemoryStore`. Rows are read as dicts (`dict_row`), so the pool must be
made with that row factory; `connect` does.
"""

import atexit
import json
import logging
import math
import threading
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
from typing import Any, Literal, LiteralString

import psycopg
from psycopg import sql
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from feather_connect.clock import Clock, SystemClock
from feather_connect.core.connector import InboundRequest
from feather_connect.ids import new_id, stable_id
from feather_connect.storage.port import (
    CLOSED,
    GRANTS,
    MAX_ENV_ROWS,
    MAX_PACKAGE_ROWS,
    READY_CHANNEL,
    SHARED_TENANT,
    STREAM_TRIGGER,
    WAITS_KEPT,
    AccessGrant,
    Attempt,
    AuditRow,
    AuditStore,
    BudgetSpec,
    BudgetStore,
    BudgetWait,
    CatalogStore,
    Delivery,
    DeliveryStatus,
    DuplicateFile,
    DuplicateRegistry,
    DuplicateUser,
    DuplicateVersion,
    EnvConfigStore,
    EnvEntry,
    EnvKind,
    EnvState,
    FailureGroup,
    Firing,
    FlowActivation,
    FlowRow,
    Grant,
    HostState,
    InboxDay,
    InboxEvent,
    InboxStore,
    InUse,
    KeyMismatch,
    KeySighting,
    Ledger,
    Override,
    OverrideStore,
    PackageEnv,
    PackageHost,
    PackageState,
    PackageStore,
    PackageVersion,
    PartitionClose,
    PartitionRow,
    Priority,
    QuarantineRow,
    QuarantineStore,
    ReadinessStore,
    ReadStore,
    RecordStateStore,
    RegistryFile,
    RegistryRow,
    RegistryStore,
    ResponseRow,
    ResponseStore,
    Role,
    RouteRow,
    Run,
    RunJournal,
    ScheduleStore,
    SealedPartition,
    SealedSecret,
    SecretStore,
    SliceFinished,
    StaleWriter,
    StateRow,
    Store,
    TaskRow,
    TokenRow,
    TokenScope,
    UserRow,
    UserStore,
    Version,
    VersionDraft,
    VersionNotReady,
    VersionSealed,
    VersionStore,
    WebhookEvent,
    WebhookEventStore,
    Window,
    WindowUsage,
    WorkerRegistry,
    WorkerRow,
    WorkerState,
    account_key,
    account_mismatch,
    check_transition,
    check_wheel,
    split_error,
    version_key,
)
from feather_connect.storage.postgres.spool import from_signed, index_keys
from feather_connect.storage.postgres.support import Pool, ensure_partition, guard_version
from feather_connect.storage.spool import decode_json_value, encode_json_value

log = logging.getLogger(__name__)
CATALOG_CHANNEL = "feather_catalog"  # NOTIFY payload: the tenant whose flows were activated or deactivated

# The oldest delivery of a partition that is still open (I2). `status <> 'applied'` is repeated outside the
# list of closed statuses so that Postgres can see that the partial index `delivery_open_idx` applies.
OLDEST_OPEN_SQL: LiteralString = """
    select * from feather.delivery
    where tenant_id = %(tenant)s and route = %(route)s and partition = %(partition)s
      and status <> 'applied' and status <> all(%(closed)s)
    order by seal_seq limit 1"""

# One sealer at a time, so that seal_seq follows commit order and a reader never sees a lower number appear after a
# higher one.
SEAL_LOCK: LiteralString = "select pg_advisory_xact_lock(hashtext('feather.seal'))"

Row = DictRow


def _json(value: Any) -> Jsonb:
    """A value for a `jsonb` column. Types that JSON can't hold, such as datetimes, are tagged."""
    return Jsonb(value, dumps=lambda v: json.dumps(v, default=encode_json_value))


def _unjson(value: Any) -> Any:
    """Undo `_json` for a value read back from a `jsonb` column."""
    return json.loads(json.dumps(value), object_hook=decode_json_value)


_WATERMARK_PAGE = 16  # versions within a lookback are usually few
_HIGHEST_CHARACTER = "\U0010ffff"  # sorts after any text that starts with a prefix, in the C collation


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
        None if r.get("result") is None else _unjson(r["result"]),
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
        r["attempt"],
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
        r["tick"],
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

    def set_run_result(self, run_id: str, result: Mapping[str, Any]) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                """update feather.run set result = %s, updated_at = %s where tenant_id = %s and run_id = %s""",
                (_json(dict(result)), self._db.clock.now(), self._db.tenant, run_id),
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

    def tagged_versions(self, statuses: Sequence[str] = ("queued", "running")) -> set[tuple[str, str]]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct params->>'package' as package, params->>'version' as version from feather.run
                   where tenant_id = %s and status = any(%s)
                     and params ? 'package' and params ? 'version'""",  # run_tagged_idx
                (self._db.tenant, list(statuses)),
            ).fetchall()
        return {(r["package"], r["version"]) for r in rows if r["package"] and r["version"]}

    def queued_of(self, package: str, version: str, limit: int = 50) -> list[Run]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.run
                   where tenant_id = %s and status = 'queued'
                     and params->>'package' = %s and params->>'version' = %s
                   order by created_at, run_id limit %s""",  # run_tagged_idx
                (self._db.tenant, package, version, limit),
            ).fetchall()
        return [_run(r) for r in rows]

    def count_runs(self, status: str | None = None, since: datetime | None = None) -> int:
        with self._db.tx() as conn:
            row = conn.execute(
                """select count(*) as n from feather.run
                   where tenant_id = %(tenant)s
                     and (%(status)s::text is null or status = %(status)s)
                     and (%(since)s::timestamptz is null or created_at >= %(since)s)""",
                {"tenant": self._db.tenant, "status": status, "since": since},
            ).fetchone()
        return 0 if row is None else int(row["n"])

    def runs_by_day(self, since: datetime) -> dict[date, tuple[int, int]]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select (created_at at time zone 'UTC')::date as day, count(*) as made,
                          count(*) filter (where status = 'failed') as failed
                   from feather.run where tenant_id = %s and created_at >= %s
                   group by 1""",  # run_created_idx
                (self._db.tenant, since),
            ).fetchall()
        return {r["day"]: (int(r["made"]), int(r["failed"])) for r in rows}

    def find_runs(self, prefix: str, limit: int = 5) -> list[Run]:
        escaped = prefix.upper().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.run
                   where tenant_id = %s and run_id like %s || '%%'
                   order by run_id desc limit %s""",  # the primary key
                (self._db.tenant, escaped, limit),
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

    def record_attempt(self, task_id: str, attempt: int, error: str) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                "update feather.task set attempt = %s, error = %s where tenant_id = %s and task_id = %s",
                (attempt, error, self._db.tenant, task_id),
            )
        if changed.rowcount == 0:
            raise KeyError(task_id)

    def finish_task(self, task_id: str, status: str, error: str | None = None) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                """update feather.task set status = %s, error = coalesce(%s, error), finished_at = %s
                   where tenant_id = %s and task_id = %s""",
                (status, error, self._db.clock.now(), self._db.tenant, task_id),
            )
        if changed.rowcount == 0:
            raise KeyError(task_id)

    def stream_run(self, flow: str, day: str) -> str:
        now = self._db.clock.now()
        with self._db.tx() as conn:
            row = conn.execute(
                """insert into feather.run (tenant_id, run_id, flow, trigger, actor, params, status, created_at,
                                            updated_at, stream_day)
                   values (%s, %s, %s, %s, 'system', %s, 'succeeded', %s, %s, %s)
                   on conflict (tenant_id, flow, stream_day) where stream_day is not null do nothing
                   returning run_id""",
                (self._db.tenant, new_id(), flow, STREAM_TRIGGER, _json({"day": day}), now, now, day),
            ).fetchone()
            if row is None:  # it was made earlier, or by a replica that won the race
                row = conn.execute(
                    "select run_id from feather.run where tenant_id = %s and flow = %s and stream_day = %s",
                    (self._db.tenant, flow, day),
                ).fetchone()
        assert row is not None
        return row["run_id"]

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
        # Each condition is there only when asked for, and the two statuses with a partial index are spelled out, so
        # that the plan can use the indexes of 0007 whatever psycopg prepares.
        query = "select * from feather.task where tenant_id = %(tenant)s and run_id = %(run)s"
        if status == "running":
            query += " and status = 'running'"
        elif status == "failed":
            query += " and status = 'failed'"
        elif status is not None:
            query += " and status = %(status)s"
        if kinds is not None:
            query += " and kind = any(%(kinds)s)"
        if after_task is not None:
            query += " and seq < " if newest_first else " and seq > "
            query += "(select seq from feather.task where tenant_id = %(tenant)s and task_id = %(after)s)"
        query += " order by seq desc" if newest_first else " order by seq"
        if limit is not None:
            query += " limit %(limit)s"
        with self._db.tx() as conn:
            rows = conn.execute(
                query,
                {
                    "tenant": self._db.tenant,
                    "run": run_id,
                    "status": status,
                    "kinds": None if kinds is None else list(kinds),
                    "after": after_task,
                    "limit": limit,
                },
            ).fetchall()
        return [_task(r) for r in rows]

    def get_task(self, task_id: str) -> TaskRow:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.task where tenant_id = %s and task_id = %s", (self._db.tenant, task_id)
            ).fetchone()
        if row is None:
            raise KeyError(task_id)
        return _task(row)

    def task_by_ref(self, run_id: str, ref: str) -> TaskRow | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.task where tenant_id = %s and run_id = %s and ref = %s",
                (self._db.tenant, run_id, ref),
            ).fetchone()
        return None if row is None else _task(row)

    def tasks_by_ref(self, run_id: str, prefix: str) -> list[TaskRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.task
                   where tenant_id = %s and run_id = %s and ref collate "C" >= %s and ref collate "C" < %s
                   order by seq""",
                (self._db.tenant, run_id, prefix, prefix + _HIGHEST_CHARACTER),
            ).fetchall()
        return [_task(r) for r in rows]

    def count_tasks(self, run_id: str) -> Mapping[tuple[str, str], int]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select kind, status, count(*) as n from feather.task
                   where tenant_id = %s and run_id = %s group by kind, status""",
                (self._db.tenant, run_id),
            ).fetchall()
        return {(r["kind"], r["status"]): r["n"] for r in rows}

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

    def delete_runs_before(self, cutoff: datetime) -> int:
        with self._db.tx() as conn:
            gone = [
                r["run_id"]
                for r in conn.execute(
                    """select run_id from feather.run r
                       where tenant_id = %(tenant)s and updated_at < %(cutoff)s
                         and status not in ('queued', 'running')
                         and not exists (
                             select 1 from feather.version v join feather.delivery d
                               on d.tenant_id = v.tenant_id and d.version_id = v.version_id
                             where v.tenant_id = r.tenant_id and v.run_id = r.run_id
                               and d.status <> 'applied' and d.status <> all(%(closed)s))""",
                    {"tenant": self._db.tenant, "cutoff": cutoff, "closed": sorted(CLOSED)},
                ).fetchall()
            ]
            if gone:
                params = (self._db.tenant, gone)
                conn.execute(
                    """delete from feather.task_log where tenant_id = %s and task_id in
                       (select task_id from feather.task where tenant_id = %s and run_id = any(%s))""",
                    (self._db.tenant, self._db.tenant, gone),
                )
                conn.execute("delete from feather.task where tenant_id = %s and run_id = any(%s)", params)
                conn.execute(
                    "delete from feather.run_slice where tenant_id = %s and run_id = any(%s)", params
                )
                conn.execute("delete from feather.run where tenant_id = %s and run_id = any(%s)", params)
        return len(gone)

    def delete_logs_before(self, cutoff: datetime) -> int:
        with self._db.tx() as conn:
            done = conn.execute(
                "delete from feather.task_log where tenant_id = %s and at < %s", (self._db.tenant, cutoff)
            )
        return done.rowcount


class _Versions(VersionStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def _hold_claim(self, conn: psycopg.Connection[DictRow], version_id: str, writer: str) -> None:
        """Make `writer` the writer of the version. The row stays locked until the transaction ends."""
        conn.execute(
            """insert into feather.spool_version (tenant_id, version_id, day, writer) values (%s, %s, %s, %s)
               on conflict (tenant_id, version_id) do update set writer = excluded.writer""",
            (self._db.tenant, version_id, self._db.clock.now().date(), writer),
        )

    def claim(self, version_id: str, writer: str, run_id: str, dataset: str, partition: str) -> None:
        with self._db.tx() as conn:
            done = conn.execute(
                """select done from feather.run_slice
                   where tenant_id = %s and run_id = %s and dataset = %s and partition = %s for share""",
                (self._db.tenant, run_id, dataset, partition),
            ).fetchone()
            if done is not None and done["done"]:
                raise SliceFinished(f"slice {partition} of run {run_id} is done")
            self._hold_claim(conn, version_id, writer)
            guard_version(conn, self._db.tenant, version_id, None)
            conn.execute(
                "delete from feather.version where tenant_id = %s and version_id = %s",
                (self._db.tenant, version_id),
            )

    def take_over(self, version_ids: Sequence[str], writer: str) -> None:
        with self._db.tx() as conn:
            for version_id in sorted(version_ids):
                self._hold_claim(conn, version_id, writer)
                guard_version(conn, self._db.tenant, version_id, None)

    def stage(self, draft: VersionDraft, writer: str | None = None) -> Version:
        with self._db.tx() as conn:
            return self.stage_in(conn, draft, writer)

    def stage_in(self, conn: psycopg.Connection[DictRow], draft: VersionDraft, writer: str | None) -> Version:
        """`stage`, in the caller's transaction."""
        if writer is not None:
            guard_version(conn, self._db.tenant, draft.version_id, writer)
        row = conn.execute(
            """insert into feather.version as v (tenant_id, version_id, dataset, partition, run_id, complete,
                                            upserts, deletes, content_hash, schema_hash, watermark, code_sha, tick)
               values (%(tenant)s, %(id)s, %(dataset)s, %(partition)s, %(run)s, %(complete)s, %(upserts)s,
                       %(deletes)s, %(content)s, %(schema)s, %(watermark)s, %(sha)s, %(tick)s)
               on conflict (tenant_id, version_id) do update set
                   dataset = excluded.dataset, partition = excluded.partition, run_id = excluded.run_id,
                   complete = excluded.complete, upserts = excluded.upserts, deletes = excluded.deletes,
                   content_hash = excluded.content_hash, schema_hash = excluded.schema_hash,
                   watermark = excluded.watermark, code_sha = excluded.code_sha, tick = excluded.tick
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
                "tick": draft.tick,
            },
        ).fetchone()
        if row is None:  # the conflicting row is sealed, so the update did not apply
            raise VersionSealed(f"version {draft.version_id} is sealed")
        return _version(row)

    def seal(
        self, version_ids: Sequence[str], routes: Sequence[str], writer: str | None = None
    ) -> list[Version]:
        with self._db.tx() as conn:
            return self.seal_in(conn, version_ids, routes, writer)

    def seal_in(
        self,
        conn: psycopg.Connection[DictRow],
        version_ids: Sequence[str],
        routes: Sequence[str],
        writer: str | None,
    ) -> list[Version]:
        """`seal`, in the caller's transaction."""
        conn.execute(SEAL_LOCK)
        owners = {
            r["version_id"]: r["writer"]
            for r in conn.execute(
                """select version_id, writer from feather.spool_version
                   where tenant_id = %s and version_id = any(%s) order by version_id for share""",
                (self._db.tenant, list(version_ids) if writer is not None else []),
            ).fetchall()
        }
        found = {
            r["version_id"]: r
            for r in conn.execute(
                """select * from feather.version where tenant_id = %s and version_id = any(%s)
                   order by version_id for update""",
                (self._db.tenant, list(version_ids)),
            ).fetchall()
        }
        if writer is not None:  # before the rest: a version that a later claim took away is not "missing"
            for version_id in version_ids:
                if version_id in found and found[version_id]["status"] == "sealed":
                    continue
                if owners.get(version_id) != writer:
                    raise StaleWriter(f"version {version_id} was claimed by a later execution")
        if missing := next((v for v in version_ids if v not in found), None):
            raise KeyError(f"no such version: {missing}")  # before sealing any, so it is all or nothing
        now = self._db.clock.now()
        if any(found[v]["status"] != "sealed" for v in version_ids):  # readiness looks again (M2 plan 1)
            conn.execute("select pg_notify(%s, %s)", (READY_CHANNEL, self._db.tenant))
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
            if sealed["watermark"] is not None:
                self._raise_watermark(conn, sealed["dataset"], _unjson(sealed["watermark"]))
            index_keys(conn, self._db.tenant, version_id, sealed["dataset"], sealed["partition"])
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

    def discard_staged(self, version_id: str, writer: str | None = None) -> None:
        with self._db.tx() as conn:
            if writer is not None:
                guard_version(conn, self._db.tenant, version_id, writer)
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
        # Watermarks are compared in Python, as the in-memory store does: they may be numbers, text or datetimes. So
        # the versions are read newest first, a page at a time, up to the first one below `since` (see the port).
        found: list[Version] = []
        before: int | None = None
        while True:
            with self._db.tx() as conn:
                rows = conn.execute(
                    """select * from feather.version
                       where tenant_id = %(tenant)s and dataset = %(dataset)s and partition = %(partition)s
                         and status = 'sealed' and watermark is not null
                         and (%(before)s::bigint is null or seal_seq < %(before)s)
                       order by seal_seq desc limit %(page)s""",
                    {
                        "tenant": self._db.tenant,
                        "dataset": dataset,
                        "partition": partition,
                        "before": before,
                        "page": _WATERMARK_PAGE,
                    },
                ).fetchall()
            for version in map(_version, rows):
                if version.watermark < since:
                    return found
                found.append(version)
            if len(rows) < _WATERMARK_PAGE:
                return found
            before = rows[-1]["seal_seq"]

    def keys_page(
        self,
        dataset: str,
        version_ids: Sequence[str],
        *,
        after: tuple[str, str] | None = None,
        prefix: str | None = None,
        limit: int = 50,
    ) -> list[KeySighting]:
        if not version_ids:
            return []
        start_key, start_version = after or ("", "")
        # Each version's keys from where the page starts, `limit` at most, by the (version, key) index: a page reads a
        # bounded slice of a version however many keys it holds.
        with self._db.tx() as conn:
            rows = conn.execute(
                """select r.*, v.run_id, v.seal_seq, v.sealed_at
                   from unnest(%(versions)s::text[]) as wanted(version_id)
                   cross join lateral (
                       select k.* from feather.record_key k
                       where k.tenant_id = %(tenant)s and k.version_id = wanted.version_id
                         and k.dataset = %(dataset)s
                         and (k.key collate "C" > %(key)s
                              or (k.key collate "C" = %(key)s and k.version_id collate "C" > %(version)s))
                         and (%(prefix)s::text is null
                              or (k.key collate "C" >= %(prefix)s and k.key collate "C" < %(upper)s))
                       order by k.key collate "C"
                       limit %(limit)s
                   ) r
                   join feather.version v on v.tenant_id = r.tenant_id and v.version_id = r.version_id
                   order by r.key collate "C", r.version_id collate "C"
                   limit %(limit)s""",
                {
                    "versions": list(version_ids),
                    "tenant": self._db.tenant,
                    "dataset": dataset,
                    "key": start_key,
                    "version": start_version,
                    "prefix": prefix or None,
                    "upper": (prefix or "") + "\U0010ffff",
                    "limit": limit,
                },
            ).fetchall()
        return [_sighting(r) for r in rows]

    def earlier_sightings(
        self, dataset: str, keys: Sequence[str], before_seal_seq: int
    ) -> dict[str, KeySighting]:
        if not keys:
            return {}
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct on (k.key) k.*, v.run_id, v.seal_seq, v.sealed_at
                   from feather.record_key k
                   join feather.version v on v.tenant_id = k.tenant_id and v.version_id = k.version_id
                   where k.tenant_id = %s and k.dataset = %s and k.key = any(%s) and v.seal_seq < %s
                   order by k.key, v.seal_seq desc""",
                (self._db.tenant, dataset, list(keys), before_seal_seq),
            ).fetchall()
        return {r["key"]: _sighting(r) for r in rows}

    def key_history(self, dataset: str, key: str, limit: int = 100) -> list[KeySighting]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from (
                       select k.*, v.run_id, v.seal_seq, v.sealed_at from feather.record_key k
                       join feather.version v on v.tenant_id = k.tenant_id and v.version_id = k.version_id
                       where k.tenant_id = %s and k.dataset = %s and k.key = %s
                       order by v.seal_seq desc limit %s
                   ) newest order by seal_seq""",
                (self._db.tenant, dataset, key, limit),
            ).fetchall()
        return [_sighting(r) for r in rows]

    def last_sealed_at(self, dataset: str) -> datetime | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select sealed_at as at from feather.version
                   where tenant_id = %s and dataset = %s and status = 'sealed'
                   order by stage_seq desc limit 1""",  # version_dataset_idx, past the few staged at its top
                (self._db.tenant, dataset),
            ).fetchone()
        return None if row is None else row["at"]

    def last_sealed_many(self, datasets: Sequence[str]) -> dict[str, datetime]:
        if not datasets:
            return {}
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct on (dataset) dataset, sealed_at as at from feather.version
                   where tenant_id = %s and dataset = any(%s) and status = 'sealed'
                   order by dataset, stage_seq desc""",
                (self._db.tenant, list(datasets)),
            ).fetchall()
        return {r["dataset"]: r["at"] for r in rows}

    def latest_watermark(self, dataset: str) -> Any | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select watermark from feather.dataset_watermark where tenant_id = %s and dataset = %s",
                (self._db.tenant, dataset),
            ).fetchone()
            if row is not None:
                return _unjson(row["watermark"])
            # Sealed before migration 0012, or not yet: work it out once, under the sealers' lock, and keep it.
            conn.execute(SEAL_LOCK)
            rows = conn.execute(
                """select watermark from feather.version
                   where tenant_id = %s and dataset = %s and status = 'sealed' and watermark is not null""",
                (self._db.tenant, dataset),
            ).fetchall()
            found = max((_unjson(r["watermark"]) for r in rows), default=None)
            if found is not None:
                self._raise_watermark(conn, dataset, found)
        return found

    def _raise_watermark(self, conn: psycopg.Connection[DictRow], dataset: str, watermark: Any) -> None:
        """Keep `watermark` as the dataset's if it is greater. The caller holds SEAL_LOCK."""
        row = conn.execute(
            "select watermark from feather.dataset_watermark where tenant_id = %s and dataset = %s",
            (self._db.tenant, dataset),
        ).fetchone()
        if row is not None and not watermark > _unjson(row["watermark"]):
            return
        conn.execute(
            """insert into feather.dataset_watermark (tenant_id, dataset, watermark) values (%s, %s, %s)
               on conflict (tenant_id, dataset) do update set watermark = excluded.watermark""",
            (self._db.tenant, dataset, _json(watermark)),
        )

    def register_slices(self, run_id: str, dataset: str, partitions: Iterable[str]) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.run_slice (tenant_id, run_id, dataset, partition)
                   select %s, %s, %s, unnest(%s::text[])
                   on conflict do nothing""",  # a replay must not undo finished slices
                (self._db.tenant, run_id, dataset, list(partitions)),
            )

    def finish_slice(self, run_id: str, dataset: str, partition: str, writer: str | None = None) -> bool:
        with self._db.tx() as conn:
            # Lock the run's slices first: two slices finishing together must not each miss the other's.
            slices = conn.execute(
                """select partition, done from feather.run_slice
                   where tenant_id = %s and run_id = %s and dataset = %s order by partition for update""",
                (self._db.tenant, run_id, dataset),
            ).fetchall()
            if partition not in {s["partition"] for s in slices}:
                raise KeyError(f"slice {partition} is not registered for run {run_id}, dataset {dataset}")
            this_one = next(s for s in slices if s["partition"] == partition)
            if writer is not None and not this_one["done"]:
                owner = conn.execute(
                    "select writer from feather.spool_version where tenant_id = %s and version_id = %s for share",
                    (self._db.tenant, stable_id(run_id, dataset, partition)),
                ).fetchone()
                if owner is None or owner["writer"] != writer:
                    raise StaleWriter(f"slice {partition} of run {run_id} was claimed by a later execution")
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

    def staged_for_run(self, run_id: str, dataset: str, tick: str | None = None) -> list[Version]:
        return self._of_run("staged", "stage_seq", run_id, dataset, tick)

    def newest_sealed_for_run(self, run_id: str, dataset: str, limit: int) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.version
                   where tenant_id = %s and run_id = %s and dataset = %s and status = 'sealed'
                   order by seal_seq desc limit %s""",
                (self._db.tenant, run_id, dataset, limit),
            ).fetchall()
        return [_version(r) for r in rows]

    def sealed_for_run(self, run_id: str, dataset: str, tick: str | None = None) -> list[Version]:
        return self._of_run("sealed", "seal_seq", run_id, dataset, tick)

    def _of_run(
        self,
        status: Literal["staged", "sealed"],
        order: Literal["stage_seq", "seal_seq"],
        run_id: str,
        dataset: str,
        tick: str | None,
    ) -> list[Version]:
        query = "select * from feather.version where tenant_id = %s and run_id = %s and dataset = %s"
        query += " and status = 'staged'" if status == "staged" else " and status = 'sealed'"
        query += "" if tick is None else " and tick is not null and tick = %s"
        query += " order by stage_seq" if order == "stage_seq" else " order by seal_seq"
        params = (self._db.tenant, run_id, dataset, *(() if tick is None else (tick,)))
        with self._db.tx() as conn:
            rows = conn.execute(query, params).fetchall()
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

    def get_many(self, version_ids: Iterable[str]) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select * from feather.version where tenant_id = %s and version_id = any(%s)",
                (self._db.tenant, list(version_ids)),
            ).fetchall()
        return [_version(r) for r in rows]

    def runs_ready_to_seal(self) -> list[tuple[str, str]]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct v.run_id, v.dataset from feather.version v
                   where v.tenant_id = %(tenant)s and v.status = 'staged'
                     and exists (select 1 from feather.run_slice s
                                 where s.tenant_id = v.tenant_id and s.run_id = v.run_id and s.dataset = v.dataset)
                     and not exists (select 1 from feather.run_slice s
                                     where s.tenant_id = v.tenant_id and s.run_id = v.run_id
                                       and s.dataset = v.dataset and not s.done)
                     and not exists (select 1 from feather.run r
                                     where r.tenant_id = v.tenant_id and r.run_id = v.run_id
                                       and r.status in ('failed', 'cancelled'))
                   order by v.run_id, v.dataset""",
                {"tenant": self._db.tenant},
            ).fetchall()
        return [(r["run_id"], r["dataset"]) for r in rows]

    def abandoned(self, limit: int = 1000) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select v.* from feather.version v join feather.run r
                     on r.tenant_id = v.tenant_id and r.run_id = v.run_id
                   where v.tenant_id = %s and v.status = 'staged' and r.status in ('failed', 'cancelled')
                     and r.trigger <> %s
                   order by v.stage_seq limit %s""",
                (self._db.tenant, STREAM_TRIGGER, limit),
            ).fetchall()
        return [_version(r) for r in rows]

    def stream_leftovers(self, limit: int = 1000) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select v.* from feather.version v join feather.run r
                     on r.tenant_id = v.tenant_id and r.run_id = v.run_id
                   where v.tenant_id = %s and v.status = 'staged' and v.tick is not null and r.trigger = %s
                   order by v.stage_seq limit %s""",
                (self._db.tenant, STREAM_TRIGGER, limit),
            ).fetchall()
        return [_version(r) for r in rows]

    def sealed_before(self, cutoff: datetime, limit: int = 1000) -> list[Version]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.version v
                   where v.tenant_id = %(tenant)s and v.status = 'sealed' and v.sealed_at < %(cutoff)s
                     and v.seal_seq < (select max(n.seal_seq) from feather.version n
                                       where n.tenant_id = v.tenant_id and n.dataset = v.dataset
                                         and n.partition = v.partition and n.status = 'sealed')
                     and not exists (select 1 from feather.delivery d
                                     where d.tenant_id = v.tenant_id and d.version_id = v.version_id)
                   order by v.seal_seq limit %(limit)s""",
                {"tenant": self._db.tenant, "cutoff": cutoff, "limit": limit},
            ).fetchall()
        return [_version(r) for r in rows]

    def delete_many(self, version_ids: Iterable[str]) -> int:
        with self._db.tx() as conn:
            done = conn.execute(
                "delete from feather.version where tenant_id = %s and version_id = any(%s)",
                (self._db.tenant, list(version_ids)),
            )
        return done.rowcount


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
            if to in ("applied", "skipped"):  # what is delivered through moved: readiness looks again
                conn.execute("select pg_notify(%s, %s)", (READY_CHANNEL, self._db.tenant))
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

    def latest_before(
        self,
        route: str,
        partition: str,
        dataset: str,
        before_seq: int,
        status: DeliveryStatus | None = "applied",
    ) -> Delivery | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select * from feather.delivery
                   where tenant_id = %(tenant)s and route = %(route)s and partition = %(partition)s
                     and dataset = %(dataset)s and seal_seq < %(before)s
                     and (%(status)s::text is null or status = %(status)s)
                   order by seal_seq desc limit 1""",
                {
                    "tenant": self._db.tenant,
                    "route": route,
                    "partition": partition,
                    "dataset": dataset,
                    "before": before_seq,
                    "status": status,
                },
            ).fetchone()
        return None if row is None else _delivery(row)

    def delivered_routes(self, dataset: str, routes: Sequence[str]) -> set[str]:
        if not routes:
            return set()
        with self._db.tx() as conn:
            rows = conn.execute(
                """select r.route from unnest(%(routes)s::text[]) as r(route)
                   where exists (select 1 from feather.delivery d
                                 where d.tenant_id = %(tenant)s and d.route = r.route and d.dataset = %(dataset)s)""",
                {"tenant": self._db.tenant, "routes": list(routes), "dataset": dataset},
            ).fetchall()
        return {r["route"] for r in rows}

    def has_deliveries(self, dataset: str) -> bool:
        with self._db.tx() as conn:
            row = conn.execute(
                """select exists (select 1 from feather.delivery where tenant_id = %(tenant)s and dataset = %(dataset)s)
                   as found""",
                {"tenant": self._db.tenant, "dataset": dataset},
            ).fetchone()
        return bool(row and row["found"])

    def count_by_status(
        self, routes: Sequence[str], statuses: Sequence[DeliveryStatus]
    ) -> dict[tuple[str, DeliveryStatus], int]:
        if not routes or not statuses:
            return {}
        with self._db.tx() as conn:
            rows = conn.execute(
                """select route, status, count(*) as n from feather.delivery
                   where tenant_id = %s and route = any(%s) and status = any(%s)
                   group by route, status""",  # delivery_open_idx, for statuses other than applied
                (self._db.tenant, list(routes), list(statuses)),
            ).fetchall()
        return {(r["route"], r["status"]): int(r["n"]) for r in rows}

    def all_applied(self) -> bool:
        with self._db.tx() as conn:
            row = conn.execute(
                "select not exists (select 1 from feather.delivery where tenant_id = %s and status <> 'applied') as ok",
                (self._db.tenant,),
            ).fetchone()
        assert row is not None
        return row["ok"]

    def pending_older_than(
        self, age: timedelta, *, now: datetime | None = None, limit: int = 1000
    ) -> list[Delivery]:
        cutoff = (self._db.clock.now() if now is None else now) - age
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from (select distinct on (route, dataset, partition) * from feather.delivery
                                  where tenant_id = %s and status <> 'applied' and status <> all(%s)
                                  order by route, dataset, partition, seal_seq) next_up
                   where status = 'pending' and updated_at < %s order by seal_seq, route limit %s""",
                (self._db.tenant, sorted(CLOSED), cutoff, limit),
            ).fetchall()
        return [_delivery(r) for r in rows]

    def open_versions(self, version_ids: Iterable[str]) -> set[str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct version_id from feather.delivery
                   where tenant_id = %s and version_id = any(%s) and status <> 'applied' and status <> all(%s)""",
                (self._db.tenant, list(version_ids), sorted(CLOSED)),
            ).fetchall()
        return {r["version_id"] for r in rows}

    def delete_closed_before(self, cutoff: datetime) -> int:
        with self._db.tx() as conn:
            gone = conn.execute(
                """delete from feather.delivery where tenant_id = %s and status = any(%s) and updated_at < %s
                   returning route, version_id""",
                (self._db.tenant, sorted(CLOSED), cutoff),
            ).fetchall()
            conn.execute(
                """delete from feather.delivery_attempt a using unnest(%s::text[], %s::text[]) as g(route, version_id)
                   where a.tenant_id = %s and a.route = g.route and a.version_id = g.version_id""",
                ([r["route"] for r in gone], [r["version_id"] for r in gone], self._db.tenant),
            )
            conn.execute(
                """delete from feather.delivery_response a using unnest(%s::text[], %s::text[]) as g(route, version_id)
                   where a.tenant_id = %s and a.route = g.route and a.version_id = g.version_id""",
                ([r["route"] for r in gone], [r["version_id"] for r in gone], self._db.tenant),
            )
        return len(gone)


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
        *,
        payload: Mapping[str, Any] | None = None,
        raw: str | None = None,
    ) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.quarantine (tenant_id, route, version_id, key, dataset, partition, error,
                                                   status, updated_at, payload, raw)
                   values (%s, %s, %s, %s, %s, %s, %s, 'open', %s, %s, %s)
                   on conflict (tenant_id, route, version_id, key, dataset) do update set
                       partition = excluded.partition, error = excluded.error, status = 'open',
                       updated_at = excluded.updated_at, payload = excluded.payload, raw = excluded.raw""",
                (
                    self._db.tenant,
                    route,
                    version_id,
                    key,
                    dataset,
                    partition,
                    error,
                    self._db.clock.now(),
                    None if payload is None else _json(dict(payload)),
                    raw,
                ),
            )

    def open_extraction_keys(self, dataset: str, keys: Iterable[str], outside: str | None = None) -> set[str]:
        # `keys` can be every row of a read and the open entries are few, so the keys don't go to the server.
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct key from feather.quarantine
                   where tenant_id = %(tenant)s and status = 'open' and route is null and dataset = %(dataset)s
                     and (%(outside)s::text is null or partition is distinct from %(outside)s)""",
                {"tenant": self._db.tenant, "dataset": dataset, "outside": outside},
            ).fetchall()
        return {r["key"] for r in rows}.intersection(keys)

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

    def count_open(self, route: str | None = None) -> int:
        with self._db.tx() as conn:
            row = conn.execute(
                """select count(*) as n from feather.quarantine
                   where tenant_id = %s and status = 'open' and (%s::text is null or route = %s)""",
                (self._db.tenant, route, route),
            ).fetchone()
        return 0 if row is None else int(row["n"])

    def list_open(
        self, route: str | None = None, *, limit: int | None = None, offset: int = 0
    ) -> list[QuarantineRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.quarantine
                   where tenant_id = %s and status = 'open' and (%s::text is null or route = %s)
                   order by quarantine_id limit %s offset %s""",
                (self._db.tenant, route, route, limit, offset),
            ).fetchall()
        return [_quarantine_row(r) for r in rows]

    def of_versions(
        self,
        version_ids: Sequence[str],
        route: str | None,
        keys: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[QuarantineRow]:
        if not version_ids or (keys is not None and not keys):
            return []
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.quarantine
                   where tenant_id = %s and version_id = any(%s) and route is not distinct from %s
                     and (%s::text[] is null or key = any(%s::text[]))
                   order by quarantine_id limit %s""",
                (
                    self._db.tenant,
                    list(version_ids),
                    route,
                    None if keys is None else list(keys),
                    None if keys is None else list(keys),
                    limit,
                ),
            ).fetchall()
        return [_quarantine_row(r) for r in rows]

    def of_key(self, dataset: str, key: str, version_ids: Sequence[str]) -> list[QuarantineRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.quarantine
                   where tenant_id = %s and key = %s
                     and (version_id = any(%s) or (route is null and dataset = %s))
                   order by quarantine_id""",
                (self._db.tenant, key, list(version_ids), dataset),
            ).fetchall()
        return [_quarantine_row(r) for r in rows]


def _sighting(r: Row) -> KeySighting:
    return KeySighting(
        r["version_id"],
        r["dataset"],
        r["partition"],
        r["key"],
        r["op"],
        from_signed(r["row_hash"]),
        r["chunk_no"],
        r["run_id"],
        r["seal_seq"],
        r["sealed_at"],
    )


def _quarantine_row(r: Row) -> QuarantineRow:
    return QuarantineRow(
        r["route"],
        r["version_id"],
        r["key"],
        r["error"],
        r["status"],
        r["dataset"],
        r["partition"],
        None if r["payload"] is None else _unjson(r["payload"]),
        r["raw"],
    )


def _response(r: Row) -> ResponseRow:
    return ResponseRow(
        r["route"],
        r["version_id"],
        r["chunk_no"],
        r["key"],
        r["status"],
        _unjson(r["fields"]),
        None if r["sent"] is None else _unjson(r["sent"]),
        r["at"],
    )


class _Responses(ResponseStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def record(self, rows: Sequence[ResponseRow]) -> None:
        if not rows:
            return
        with self._db.tx() as conn, conn.cursor() as cursor:
            self._upsert(cursor, rows)

    def replace_chunk(self, route: str, version_id: str, chunk_no: int, rows: Sequence[ResponseRow]) -> None:
        with self._db.tx() as conn, conn.cursor() as cursor:
            cursor.execute(
                """delete from feather.delivery_response
                   where tenant_id = %s and route = %s and version_id = %s and chunk_no = %s
                     and not (key = any(%s::text[]))""",
                (self._db.tenant, route, version_id, chunk_no, [row.key for row in rows]),
            )
            if rows:
                self._upsert(cursor, rows)

    def _upsert(self, cursor: psycopg.Cursor[Row], rows: Sequence[ResponseRow]) -> None:
        """A row with no fields keeps the fields of the row before it (I3)."""
        now = self._db.clock.now()
        cursor.executemany(
            """insert into feather.delivery_response
                   (tenant_id, route, version_id, chunk_no, key, status, fields, sent, at)
               values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               on conflict (tenant_id, route, version_id, chunk_no, key) do update set
                   status = excluded.status,
                   fields = case when excluded.fields = '{}'::jsonb then delivery_response.fields
                                 else excluded.fields end,
                   sent = excluded.sent,
                   at = excluded.at""",
            [
                (
                    self._db.tenant,
                    row.route,
                    row.version_id,
                    row.chunk_no,
                    row.key,
                    row.status,
                    _json(dict(row.fields)),
                    None if row.sent is None else _json(dict(row.sent)),
                    now,
                )
                for row in rows
            ],
        )

    def of_delivery(self, route: str, version_id: str, limit: int = 1000) -> list[ResponseRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.delivery_response
                   where tenant_id = %s and route = %s and version_id = %s
                   order by chunk_no, key collate "C" limit %s""",
                (self._db.tenant, route, version_id, limit),
            ).fetchall()
        return [_response(r) for r in rows]

    def for_keys(self, route: str, version_id: str, keys: Sequence[str]) -> dict[str, ResponseRow]:
        if not keys:
            return {}
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct on (key) * from feather.delivery_response
                   where tenant_id = %s and route = %s and version_id = %s and key = any(%s)
                   order by key, chunk_no desc""",
                (self._db.tenant, route, version_id, list(keys)),
            ).fetchall()
        return {r["key"]: _response(r) for r in rows}

    def find(self, route: str, version_id: str, key: str) -> ResponseRow | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select * from feather.delivery_response
                   where tenant_id = %s and route = %s and version_id = %s and key = %s
                   order by chunk_no desc limit 1""",
                (self._db.tenant, route, version_id, key),
            ).fetchone()
        return None if row is None else _response(row)


# A window's tokens now: the stored ones plus what has refilled since `refilled_at`, up to the window's size.
_REFILLED: LiteralString = """least(
    w.units,
    w.tokens + w.units * greatest(extract(epoch from (%(now)s::timestamptz - w.refilled_at)), 0) / w.seconds
)"""


class PostgresBudgets(BudgetStore):
    """Token buckets, one `budget_window` row per window (design 6.6).

    A grant locks all of the budget's windows in index order, so that concurrent callers queue up, then takes
    from every window or from none, in one transaction. Tokens are not leased to workers in blocks (not in M1).

    The tenant's `budget` row holds the reserve and, for a vendor account's budget, the `account`, whose windows are
    kept once under the tenant id `*` (ADR-0013 rule 8): the one place where tenants share rows, and only these.
    """

    def __init__(self, db: _Db) -> None:
        self._db = db

    def _owner(self, account: str | None, name: str) -> tuple[str, str]:
        """The tenant id and name the windows of a budget are kept under."""
        return (self._db.tenant, name) if account is None else (SHARED_TENANT, account_key(account))

    def define(self, spec: BudgetSpec) -> None:
        now = self._db.clock.now()
        tenant, key = self._owner(spec.account, spec.name)
        with self._db.tx() as conn:
            conn.execute(
                "select pg_advisory_xact_lock(hashtext(%s))",
                (f"feather.budget:{tenant}:{key}",),
            )
            budget = conn.execute(
                "select reserve, account from feather.budget where tenant_id = %s and name = %s",
                (self._db.tenant, spec.name),
            ).fetchone()
            windows = conn.execute(
                "select units, seconds from feather.budget_window where tenant_id = %s and name = %s order by idx",
                (tenant, key),
            ).fetchall()
            held = tuple(Window(w["units"], w["seconds"]) for w in windows)
            same_windows = held == spec.windows
            if spec.account is not None and windows and not same_windows:
                # Who else uses the account: the one read across tenants, of the account's own rows (ADR-0013
                # rule 8). Another tenant is not named to this one.
                other = conn.execute(
                    """select tenant_id, name from feather.budget
                       where account = %s and not (tenant_id = %s and name = %s)
                       order by tenant_id = %s desc, name limit 1""",
                    (spec.account, self._db.tenant, spec.name, self._db.tenant),
                ).fetchone()
                if other is not None:
                    who = (
                        repr(other["name"])
                        if other["tenant_id"] == self._db.tenant
                        else "a budget of another tenant"
                    )
                    raise ValueError(account_mismatch(spec, who, held))
            if (
                budget is not None
                and budget["reserve"] == dict(spec.reserve)
                and budget["account"] == spec.account
                and same_windows
            ):
                return  # the same spec keeps what has been used
            if budget is not None and budget["account"] is None and spec.account is not None:
                conn.execute(  # the budget was the tenant's own, and now is its account's
                    "delete from feather.budget_window where tenant_id = %s and name = %s",
                    (self._db.tenant, spec.name),
                )
            conn.execute(
                """insert into feather.budget (tenant_id, name, reserve, account) values (%s, %s, %s, %s)
                   on conflict (tenant_id, name) do update set reserve = excluded.reserve, account = excluded.account""",
                (self._db.tenant, spec.name, Jsonb(dict(spec.reserve)), spec.account),
            )
            if spec.account is not None and same_windows and windows:
                return  # another tenant's budget of the account has these windows already, and what they used
            conn.execute(
                "delete from feather.budget_window where tenant_id = %s and name = %s",
                (tenant, key),
            )
            for idx, window in enumerate(spec.windows):
                conn.execute(
                    """insert into feather.budget_window (tenant_id, name, idx, units, seconds, tokens, refilled_at)
                       values (%s, %s, %s, %s, %s, %s, %s)""",
                    (tenant, key, idx, window.units, window.seconds, window.units, now),
                )

    def _budget(self, conn: psycopg.Connection[DictRow], name: str) -> tuple[str, str, dict[str, Any]]:
        """The tenant id and name of the budget's windows, and its reserve. Raises `KeyError` for no such budget."""
        row = conn.execute(
            "select reserve, account from feather.budget where tenant_id = %s and name = %s",
            (self._db.tenant, name),
        ).fetchone()
        if row is None:
            raise KeyError(name)
        tenant, key = self._owner(row["account"], name)
        return tenant, key, row["reserve"]

    def acquire(self, name: str, units: int, priority: Priority = "normal") -> Grant:
        now = self._db.clock.now()
        with self._db.tx() as conn:
            tenant, key, reserve = self._budget(conn, name)
            rows = conn.execute(
                f"""select w.units, w.seconds, {_REFILLED} as tokens
                    from feather.budget_window w
                    where w.tenant_id = %(tenant)s and w.name = %(name)s
                    order by w.idx for update of w""",
                {"tenant": tenant, "name": key, "now": now},
            ).fetchall()
            if not rows:
                raise KeyError(name)
            kept = (
                Fraction(reserve.get("high", 0.0)).limit_denominator() if priority == "low" else Fraction(0)
            )
            wait = Fraction(0)
            for row in rows:
                held_back = row["units"] * kept
                if units > row["units"] - held_back:
                    raise ValueError(
                        f"{units} units are more than {priority} requests can ever take from {name!r}"
                    )
                short = units + held_back - Fraction(row["tokens"])
                if short > 0:
                    wait = max(wait, short * row["seconds"] / row["units"])
            if wait > 0:
                return Grant(False, timedelta(microseconds=math.ceil(wait * 1_000_000)))
            conn.execute(
                f"""update feather.budget_window w set tokens = {_REFILLED} - %(units)s, refilled_at = %(now)s
                    where w.tenant_id = %(tenant)s and w.name = %(name)s""",
                {"tenant": tenant, "name": key, "now": now, "units": units},
            )
        return Grant(True)

    def usage(self, name: str) -> list[WindowUsage]:
        with self._db.tx() as conn:
            tenant, key, _ = self._budget(conn, name)
            rows = conn.execute(
                f"""select w.units, w.seconds, {_REFILLED} as tokens from feather.budget_window w
                    where w.tenant_id = %(tenant)s and w.name = %(name)s order by w.idx""",
                {"tenant": tenant, "name": key, "now": self._db.clock.now()},
            ).fetchall()
        if not rows:
            raise KeyError(name)
        return [WindowUsage(Window(r["units"], r["seconds"]), float(r["units"] - r["tokens"])) for r in rows]

    def note_wait(self, name: str, task_ref: str, run_id: str | None, until: datetime) -> None:
        now = self._db.clock.now()
        with self._db.tx() as conn:
            conn.execute(
                "delete from feather.budget_wait where tenant_id = %(tenant)s and until < %(old)s",
                {"tenant": self._db.tenant, "old": now - WAITS_KEPT},
            )
            conn.execute(
                """insert into feather.budget_wait (tenant_id, budget, task_ref, run_id, first_at, until, waits)
                   values (%(tenant)s, %(budget)s, %(task)s, %(run)s, %(now)s, %(until)s, 1)
                   on conflict (tenant_id, budget, task_ref) do update
                   set until = excluded.until, waits = feather.budget_wait.waits + 1,
                       run_id = coalesce(excluded.run_id, feather.budget_wait.run_id)""",
                {
                    "tenant": self._db.tenant,
                    "budget": name,
                    "task": task_ref,
                    "run": run_id,
                    "now": now,
                    "until": until,
                },
            )

    def waits(self, name: str) -> list[BudgetWait]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.budget_wait
                   where tenant_id = %(tenant)s and budget = %(budget)s and until > %(now)s
                   order by until, task_ref""",
                {"tenant": self._db.tenant, "budget": name, "now": self._db.clock.now()},
            ).fetchall()
        return [
            BudgetWait(r["budget"], r["task_ref"], r["run_id"], r["first_at"], r["until"], r["waits"])
            for r in rows
        ]


class _Catalog(CatalogStore):
    """The catalogue tables hold a row for each (name, code SHA). The latest registration of a name is current,
    unless the name is retired, which marks every row of it."""

    def __init__(self, db: _Db) -> None:
        self._db = db

    def sync(self, project: str, code_sha: str, specs: Mapping[str, Any]) -> None:
        db = self._db
        now = db.clock.now()
        names = list(specs)
        with db.tx() as conn:
            conn.execute(
                "select pg_advisory_xact_lock(hashtext(%s))", (f"feather.catalog:{db.tenant}:{project}",)
            )
            conn.execute(
                """insert into feather.project (tenant_id, name, code_sha, registered_at) values (%s, %s, %s, %s)
                   on conflict (tenant_id, name, code_sha) do update set registered_at = excluded.registered_at""",
                (db.tenant, project, code_sha, now),
            )
            gone = conn.execute(
                """update feather.flow set retired_at = coalesce(retired_at, %s)
                   where tenant_id = %s and project = %s and name <> all(%s) returning name""",
                (now, db.tenant, project, names),
            ).fetchall()
            conn.execute(
                "update feather.route set retired_at = coalesce(retired_at, %s) where tenant_id = %s and flow = any(%s)",
                (now, db.tenant, [r["name"] for r in gone]),
            )
            for name, spec in specs.items():
                kept = [route["name"] for route in spec["routes"]]
                conn.execute(
                    """update feather.route set retired_at = coalesce(retired_at, %s)
                       where tenant_id = %s and flow = %s and name <> all(%s)""",
                    (now, db.tenant, name, kept),
                )
                conn.execute(
                    """insert into feather.flow (tenant_id, name, code_sha, project, spec, registered_at)
                       values (%s, %s, %s, %s, %s, %s)
                       on conflict (tenant_id, name, code_sha) do update
                       set project = excluded.project, spec = excluded.spec,
                           registered_at = excluded.registered_at, retired_at = null""",
                    (db.tenant, name, code_sha, project, _json(spec), now),
                )
                # A name that comes back is current again, whichever SHA retired it.
                conn.execute(
                    "update feather.flow set retired_at = null where tenant_id = %s and name = %s",
                    (db.tenant, name),
                )
                for route in spec["routes"]:
                    conn.execute(
                        """insert into feather.route (tenant_id, name, code_sha, flow, spec, registered_at)
                           values (%s, %s, %s, %s, %s, %s)
                           on conflict (tenant_id, name, code_sha) do update
                           set flow = excluded.flow, spec = excluded.spec,
                               registered_at = excluded.registered_at, retired_at = null""",
                        (db.tenant, route["name"], code_sha, name, _json(route), now),
                    )
                    conn.execute(
                        "update feather.route set retired_at = null where tenant_id = %s and name = %s",
                        (db.tenant, route["name"]),
                    )

    def flows(self) -> list[FlowRow]:
        return [_flow_row(r) for r in self._latest("flow")]

    def flow(self, name: str, code_sha: str | None = None) -> FlowRow:
        with self._db.tx() as conn:
            row = conn.execute(
                """select * from feather.flow where tenant_id = %s and name = %s and (%s::text is null or code_sha = %s)
                   order by registered_at desc limit 1""",
                (self._db.tenant, name, code_sha, code_sha),
            ).fetchone()
        if row is None:
            raise KeyError(f"no flow {name!r}" + ("" if code_sha is None else f" at {code_sha!r}"))
        return _flow_row(row)

    def routes(self) -> list[RouteRow]:
        return [_route_row(r) for r in self._latest("route")]

    def _latest(self, table: Literal["flow", "route"]) -> list[Row]:
        query: LiteralString = (
            "select * from (select distinct on (name) * from feather.flow where tenant_id = %s order by name, registered_at desc) latest "
            "where retired_at is null order by name"
            if table == "flow"
            else "select * from (select distinct on (name) * from feather.route where tenant_id = %s order by name, registered_at desc) latest "
            "where retired_at is null order by name"
        )
        with self._db.tx() as conn:
            return conn.execute(query, (self._db.tenant,)).fetchall()


def _flow_row(r: Row) -> FlowRow:
    return FlowRow(r["name"], r["project"], r["code_sha"], r["spec"], r["registered_at"])


def _route_row(r: Row) -> RouteRow:
    return RouteRow(r["name"], r["flow"], r["code_sha"], r["spec"], r["registered_at"])


def _worker(r: Row) -> WorkerRow:
    return WorkerRow(r["worker_id"], r["labels"], r["code_sha"], r["slots"], r["last_seen"], r["state"])


class _Workers(WorkerRegistry):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def heartbeat(self, worker_id: str, labels: Mapping[str, str], code_sha: str, slots: int) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.worker (tenant_id, worker_id, labels, code_sha, slots, last_seen, state)
                   values (%s, %s, %s, %s, %s, %s, 'alive')
                   on conflict (tenant_id, worker_id) do update
                   set labels = excluded.labels, code_sha = excluded.code_sha, slots = excluded.slots,
                       last_seen = excluded.last_seen,
                       state = case when feather.worker.state = 'draining' then 'draining' else 'alive' end""",
                (self._db.tenant, worker_id, Jsonb(dict(labels)), code_sha, slots, self._db.clock.now()),
            )

    def list_workers(self) -> list[WorkerRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select * from feather.worker where tenant_id = %s order by worker_id", (self._db.tenant,)
            ).fetchall()
        return [_worker(r) for r in rows]

    def stale(self, older_than: timedelta) -> list[WorkerRow]:
        cutoff = self._db.clock.now() - older_than
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.worker where tenant_id = %s and state <> 'dead' and last_seen < %s
                   order by worker_id""",
                (self._db.tenant, cutoff),
            ).fetchall()
        return [_worker(r) for r in rows]

    def forget_stopped(self, before: datetime) -> int:
        with self._db.tx() as conn:
            done = conn.execute(
                """delete from feather.worker
                   where tenant_id = %s and state in ('dead', 'draining') and last_seen < %s""",
                (self._db.tenant, before),
            )
        return done.rowcount

    def mark(self, worker_id: str, state: WorkerState) -> None:
        with self._db.tx() as conn:
            done = conn.execute(
                "update feather.worker set state = %s where tenant_id = %s and worker_id = %s",
                (state, self._db.tenant, worker_id),
            )
            if done.rowcount == 0:
                raise KeyError(f"no worker {worker_id!r}")


class _Schedules(ScheduleStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def last_tick(self, flow: str, trigger_id: str) -> datetime | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select last_tick from feather.schedule where tenant_id = %s and flow = %s and trigger_id = %s",
                (self._db.tenant, flow, trigger_id),
            ).fetchone()
        return None if row is None else row["last_tick"]

    def set_last_tick(self, flow: str, trigger_id: str, at: datetime) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.schedule (tenant_id, flow, trigger_id, last_tick) values (%s, %s, %s, %s)
                   on conflict (tenant_id, flow, trigger_id) do update set last_tick = excluded.last_tick""",
                (self._db.tenant, flow, trigger_id, at),
            )

    def advance_tick(self, flow: str, trigger_id: str, seen: datetime | None, at: datetime) -> bool:
        with self._db.tx() as conn:
            if seen is None:
                done = conn.execute(
                    """insert into feather.schedule (tenant_id, flow, trigger_id, last_tick) values (%s, %s, %s, %s)
                       on conflict (tenant_id, flow, trigger_id) do nothing""",
                    (self._db.tenant, flow, trigger_id, at),
                )
            else:
                done = conn.execute(
                    """update feather.schedule set last_tick = %s
                       where tenant_id = %s and flow = %s and trigger_id = %s and last_tick = %s""",
                    (at, self._db.tenant, flow, trigger_id, seen),
                )
        return done.rowcount == 1


class _Overrides(OverrideStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

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
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.override (tenant_id, target, key, value, reason, created_by, created_at,
                                                 expires_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s)
                   on conflict (tenant_id, target, key) do update
                   set value = excluded.value, reason = excluded.reason, created_by = excluded.created_by,
                       created_at = excluded.created_at, expires_at = excluded.expires_at""",
                (self._db.tenant, target, key, _json(value), reason, actor, self._db.clock.now(), expires_at),
            )

    def get(self, target: str, key: str) -> Any | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select value from feather.override
                   where tenant_id = %s and target = %s and key = %s and (expires_at is null or expires_at > %s)""",
                (self._db.tenant, target, key, self._db.clock.now()),
            ).fetchone()
        return None if row is None else _unjson(row["value"])

    def delete_expired(self) -> int:
        with self._db.tx() as conn:
            done = conn.execute(
                "delete from feather.override where tenant_id = %s and expires_at <= %s",
                (self._db.tenant, self._db.clock.now()),
            )
        return done.rowcount

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
        now = self._db.clock.now()
        with self._db.tx() as conn:
            claimed = conn.execute(
                """insert into feather.override (tenant_id, target, key, value, reason, created_by, created_at,
                                                 expires_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s)
                   on conflict (tenant_id, target, key) do update
                   set value = excluded.value, reason = excluded.reason, created_by = excluded.created_by,
                       created_at = excluded.created_at, expires_at = excluded.expires_at
                   where feather.override.expires_at is not null and feather.override.expires_at <= %s""",
                (self._db.tenant, target, key, _json(value), reason, actor, now, expires_at, now),
            )
        return claimed.rowcount == 1

    def list_all(self) -> list[Override]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.override
                   where tenant_id = %s and (expires_at is null or expires_at > %s) order by target, key""",
                (self._db.tenant, self._db.clock.now()),
            ).fetchall()
        return [
            Override(
                r["target"],
                r["key"],
                _unjson(r["value"]),
                r["reason"],
                r["created_by"],
                r["created_at"],
                r["expires_at"],
            )
            for r in rows
        ]

    def clear(self, target: str, key: str, *, actor: str, reason: str | None = None) -> None:
        with self._db.tx() as conn:
            conn.execute(
                "delete from feather.override where tenant_id = %s and target = %s and key = %s",
                (self._db.tenant, target, key),
            )


class _Users(UserStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def create_user(self, username: str, password_hash: str, role: Role) -> str:
        user_id = new_id()
        try:
            with self._db.tx() as conn:
                conn.execute(
                    """insert into feather.app_user (tenant_id, user_id, username, password_hash, created_at, role)
                       values (%s, %s, %s, %s, %s, %s)""",
                    (self._db.tenant, user_id, username, password_hash, self._db.clock.now(), role),
                )
        except psycopg.errors.UniqueViolation:
            raise DuplicateUser(username) from None
        return user_id

    @staticmethod
    def _user(row: Mapping[str, Any]) -> UserRow:
        return UserRow(
            row["user_id"],
            row["username"],
            row["password_hash"],
            row["created_at"],
            row["role"],
            tuple(g for g in GRANTS if g in (row["grants"] or ())),
        )

    def get_user(self, username: str) -> UserRow | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.app_user where tenant_id = %s and username = %s",
                (self._db.tenant, username),
            ).fetchone()
        return None if row is None else self._user(row)

    def list_users(self) -> list[UserRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                'select * from feather.app_user where tenant_id = %s order by username collate "C"',
                (self._db.tenant,),
            ).fetchall()
        return [self._user(row) for row in rows]

    def set_password_hash(self, username: str, password_hash: str) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                "update feather.app_user set password_hash = %s where tenant_id = %s and username = %s",
                (password_hash, self._db.tenant, username),
            )
        if changed.rowcount == 0:
            raise KeyError(username)

    def set_role(self, username: str, role: Role) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                "update feather.app_user set role = %s where tenant_id = %s and username = %s",
                (role, self._db.tenant, username),
            )
        if changed.rowcount == 0:
            raise KeyError(username)

    def set_grants(self, username: str, grants: Sequence[AccessGrant]) -> None:
        with self._db.tx() as conn:
            changed = conn.execute(
                "update feather.app_user set grants = %s where tenant_id = %s and username = %s",
                ([g for g in GRANTS if g in grants], self._db.tenant, username),
            )
        if changed.rowcount == 0:
            raise KeyError(username)

    def create_token(
        self,
        name: str,
        token_hash: str,
        expires_at: datetime | None,
        role: Role,
        created_by: str | None,
        scope: TokenScope = "all",
    ) -> str:
        token_id = new_id()
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.api_token
                       (tenant_id, token_id, name, token_hash, created_at, expires_at, role, created_by, scope)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    self._db.tenant,
                    token_id,
                    name,
                    token_hash,
                    self._db.clock.now(),
                    expires_at,
                    role,
                    created_by,
                    scope,
                ),
            )
        return token_id

    def find_token(self, token_hash: str) -> TokenRow | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.api_token where tenant_id = %s and token_hash = %s",
                (self._db.tenant, token_hash),
            ).fetchone()
        if row is None:
            return None
        return TokenRow(
            row["token_id"],
            row["name"],
            row["token_hash"],
            row["created_at"],
            row["expires_at"],
            row["revoked_at"],
            row["role"],
            row["created_by"],
            row["scope"],
        )

    def revoke_token(self, token_id: str) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """update feather.api_token set revoked_at = %s
                   where tenant_id = %s and token_id = %s and revoked_at is null""",
                (self._db.clock.now(), self._db.tenant, token_id),
            )


class _Registry(RegistryStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    _COLUMNS = """project, version, filename, sha256, size, manifest, uploaded_by, uploaded_at, yanked_at,
                  yanked_reason"""

    @staticmethod
    def _file(row: Mapping[str, Any]) -> RegistryFile:
        return RegistryFile(
            row["project"],
            row["version"],
            row["filename"],
            row["sha256"],
            row["size"],
            row["manifest"],
            row["uploaded_by"],
            row["uploaded_at"],
            row["yanked_at"],
            row["yanked_reason"],
        )

    def put_file(self, file: RegistryFile, content: bytes) -> None:
        try:
            with self._db.tx() as conn:
                conn.execute(
                    """insert into feather.registry_file (tenant_id, project, version, version_key, filename,
                           sha256, size, manifest, content, uploaded_by, uploaded_at)
                       values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        self._db.tenant,
                        file.project,
                        file.version,
                        version_key(file.version),
                        file.filename,
                        file.sha256,
                        file.size,
                        _json(dict(file.manifest)),
                        content,
                        file.uploaded_by,
                        file.uploaded_at,
                    ),
                )
        except psycopg.errors.UniqueViolation:
            raise DuplicateFile(f"{file.project} {file.version}") from None

    def projects(self) -> list[str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                'select distinct project collate "C" as project from feather.registry_file where tenant_id = %s order by 1',
                (self._db.tenant,),
            ).fetchall()
        return [row["project"] for row in rows]

    def files(self, project: str) -> list[RegistryFile]:
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""select {self._COLUMNS} from feather.registry_file where tenant_id = %s and project = %s
                    order by uploaded_at, filename""",
                (self._db.tenant, project),
            ).fetchall()
        return [self._file(row) for row in rows]

    def file(self, filename: str) -> RegistryFile | None:
        with self._db.tx() as conn:
            row = conn.execute(
                f"select {self._COLUMNS} from feather.registry_file where tenant_id = %s and filename = %s",
                (self._db.tenant, filename),
            ).fetchone()
        return None if row is None else self._file(row)

    def content(self, filename: str) -> bytes | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select content from feather.registry_file where tenant_id = %s and filename = %s",
                (self._db.tenant, filename),
            ).fetchone()
        return None if row is None else bytes(row["content"])

    def yank(self, project: str, version: str, reason: str | None) -> bool:
        with self._db.tx() as conn:
            conn.execute(
                """update feather.registry_file set yanked_at = %s, yanked_reason = %s
                   where tenant_id = %s and project = %s and version = %s and yanked_at is null""",
                (self._db.clock.now(), reason, self._db.tenant, project, version),
            )
            found = conn.execute(
                "select 1 from feather.registry_file where tenant_id = %s and project = %s and version = %s",
                (self._db.tenant, project, version),
            ).fetchone()
        return found is not None

    @staticmethod
    def _registry(row: Mapping[str, Any]) -> RegistryRow:
        return RegistryRow(row["name"], row["index_url"], row["token_ref"], row["added_by"], row["added_at"])

    def add_registry(self, name: str, index_url: str, token_ref: str | None, added_by: str) -> RegistryRow:
        row = RegistryRow(name, index_url, token_ref, added_by, self._db.clock.now())
        try:
            with self._db.tx() as conn:
                conn.execute(
                    """insert into feather.registry (tenant_id, name, index_url, token_ref, added_by, added_at)
                       values (%s, %s, %s, %s, %s, %s)""",
                    (self._db.tenant, name, index_url, token_ref, added_by, row.added_at),
                )
        except psycopg.errors.UniqueViolation:
            raise DuplicateRegistry(name) from None
        return row

    def registries(self) -> list[RegistryRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                'select * from feather.registry where tenant_id = %s order by name collate "C"',
                (self._db.tenant,),
            ).fetchall()
        return [self._registry(row) for row in rows]

    def get_registry(self, name: str) -> RegistryRow | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select * from feather.registry where tenant_id = %s and name = %s", (self._db.tenant, name)
            ).fetchone()
        return None if row is None else self._registry(row)

    def remove_registry(self, name: str) -> bool:
        with self._db.tx() as conn:
            gone = conn.execute(
                "delete from feather.registry where tenant_id = %s and name = %s", (self._db.tenant, name)
            )
        return gone.rowcount > 0


class _Packages(PackageStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    _COLUMNS = """name, version, sha256, registry, filename, size, manifest, fc_versions, lock_hash, state, load_check,
                  installed_by, installed_at, checks, promoted_from, promoted_by, signed_by"""

    @staticmethod
    def _version(row: Mapping[str, Any]) -> PackageVersion:
        return PackageVersion(
            row["name"],
            row["version"],
            row["sha256"],
            row["registry"],
            row["filename"],
            row["size"],
            row["manifest"],
            row["fc_versions"],
            row["lock_hash"],
            row["state"],
            row["load_check"],
            row["installed_by"],
            row["installed_at"],
            tuple(row["checks"] or ()),
            row["promoted_from"],
            row["promoted_by"],
            row["signed_by"],
        )

    def install(
        self, version: PackageVersion, content: bytes, flows: Sequence[str], reason: str | None = None
    ) -> None:
        tenant, key = self._db.tenant, version_key(version.version)
        check_wheel(version, content)
        try:
            with self._db.tx() as conn:
                conn.execute(
                    """insert into feather.package (tenant_id, name, created_at) values (%s, %s, %s)
                       on conflict (tenant_id, name) do nothing""",
                    (tenant, version.name, version.installed_at),
                )
                conn.execute(
                    """insert into feather.package_wheel (tenant_id, sha256, size, content, stored_at)
                       values (%s, %s, %s, %s, %s) on conflict (tenant_id, sha256) do nothing""",
                    (tenant, version.sha256, len(content), content, version.installed_at),
                )
                conn.execute(
                    f"""insert into feather.package_version (tenant_id, version_key, {self._COLUMNS})
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        tenant,
                        key,
                        version.name,
                        version.version,
                        version.sha256,
                        version.registry,
                        version.filename,
                        version.size,
                        _json(dict(version.manifest)),
                        version.fc_versions,
                        version.lock_hash,
                        version.state,
                        _json(dict(version.load_check)),
                        version.installed_by,
                        version.installed_at,
                        _json(list(version.checks)),
                        version.promoted_from,
                        version.promoted_by,
                        version.signed_by,
                    ),
                )
                for flow in flows:
                    conn.execute(
                        """insert into feather.flow_activation (tenant_id, flow, package, version_key, version, state,
                               changed_by, changed_at, reason)
                           values (%s, %s, %s, %s, %s, 'inactive', %s, %s, %s)""",
                        (
                            tenant,
                            flow,
                            version.name,
                            key,
                            version.version,
                            version.installed_by,
                            version.installed_at,
                            reason,
                        ),
                    )
        except psycopg.errors.UniqueViolation:
            raise DuplicateVersion(f"{version.name} {version.version}") from None

    def packages(self) -> list[str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select name collate "C" as name from feather.package where tenant_id = %s
                   order by 1 limit %s""",
                (self._db.tenant, MAX_PACKAGE_ROWS),
            ).fetchall()
        return [row["name"] for row in rows]

    def versions(self, name: str | None = None) -> list[PackageVersion]:
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""select {self._COLUMNS} from feather.package_version
                    where tenant_id = %s and (%s::text is null or name = %s)
                    order by name collate "C", installed_at limit %s""",
                (self._db.tenant, name, name, MAX_PACKAGE_ROWS),
            ).fetchall()
        return [self._version(row) for row in rows]

    def version(self, name: str, version: str) -> PackageVersion | None:
        with self._db.tx() as conn:
            row = conn.execute(
                f"""select {self._COLUMNS} from feather.package_version
                    where tenant_id = %s and name = %s and version_key = %s""",
                (self._db.tenant, name, version_key(version)),
            ).fetchone()
        return None if row is None else self._version(row)

    def wheel(self, sha256: str) -> bytes | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select content from feather.package_wheel where tenant_id = %s and sha256 = %s",
                (self._db.tenant, sha256),
            ).fetchone()
        return None if row is None else bytes(row["content"])

    def uninstall(self, name: str, version: str, by: str, at: datetime, reason: str | None = None) -> bool:
        tenant, key = self._db.tenant, version_key(version)
        with self._db.tx() as conn:
            row = conn.execute(
                """select version, sha256, state from feather.package_version
                   where tenant_id = %s and name = %s and version_key = %s for update""",
                (tenant, name, key),
            ).fetchone()
            if row is None or row["state"] == "uninstalled":
                raise KeyError(f"{name} {version} is not installed here")
            busy = conn.execute(
                """select flow, state from feather.flow_activation
                   where tenant_id = %s and package = %s and version_key = %s and state in ('active', 'activating')
                   order by flow collate "C" limit 1 for update""",
                (tenant, name, key),
            ).fetchone()
            if busy is not None:
                raise InUse(f"{name} {row['version']}: its flow {busy['flow']} is {busy['state']}")
            runs = conn.execute(
                """select status from feather.run
                   where tenant_id = %s and status in ('queued', 'running')
                     and params->>'package' = %s and params->>'version' = %s
                   limit 1""",  # run_tagged_idx
                (tenant, name, row["version"]),
            ).fetchone()
            if runs is not None:
                raise InUse(
                    f"{name} {row['version']} has {runs['status']} runs: let them finish, or cancel them"
                )
            conn.execute(
                """update feather.package_version set state = 'uninstalled'
                   where tenant_id = %s and name = %s and version_key = %s""",
                (tenant, name, key),
            )
            shared = conn.execute(
                """select 1 from feather.package_version
                   where tenant_id = %s and sha256 = %s and state <> 'uninstalled' limit 1""",
                (tenant, row["sha256"]),
            ).fetchone()
            if shared is None:
                conn.execute(
                    "delete from feather.package_wheel where tenant_id = %s and sha256 = %s",
                    (tenant, row["sha256"]),
                )
            conn.execute("select pg_notify(%s, %s)", (CATALOG_CHANNEL, tenant))
        return shared is None

    _ACTIVATION = "flow, package, version, state, changed_by, changed_at, reason, error, last_active_at"

    @staticmethod
    def _activation(r: Mapping[str, Any]) -> FlowActivation:
        return FlowActivation(
            r["flow"],
            r["package"],
            r["version"],
            r["state"],
            r["changed_by"],
            r["changed_at"],
            r["reason"],
            r["error"],
            r["last_active_at"],
        )

    def renames(self) -> dict[tuple[str, str], str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select kind, old_name, new_name from feather.renamed where tenant_id = %s",
                (self._db.tenant,),
            ).fetchall()
        return {(r["kind"], r["old_name"]): r["new_name"] for r in rows}

    def needed_lock_hashes(self) -> set[str]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select distinct lock_hash from feather.package_version
                   where tenant_id = %s and state <> 'uninstalled'""",
                (self._db.tenant,),
            ).fetchall()
        return {r["lock_hash"] for r in rows}

    def activations(self, name: str, version: str | None = None) -> list[FlowActivation]:
        key = None if version is None else version_key(version)
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""select {self._ACTIVATION}
                   from feather.flow_activation
                   where tenant_id = %s and package = %s and (%s::text is null or version_key = %s)
                   order by flow collate "C", changed_at, version collate "C" limit %s""",
                (self._db.tenant, name, key, key, MAX_PACKAGE_ROWS),
            ).fetchall()
        return [self._activation(r) for r in rows]

    def activate(
        self, flow: str, name: str, version: str, by: str, at: datetime, reason: str | None = None
    ) -> FlowActivation:
        tenant, key = self._db.tenant, version_key(version)
        with self._db.tx() as conn:
            installed = (
                conn.execute(  # shared: an uninstall, which takes it for update, waits or makes this wait
                    """select state from feather.package_version
                   where tenant_id = %s and name = %s and version_key = %s for share""",
                    (tenant, name, key),
                ).fetchone()
            )
            if installed is None or installed["state"] != "ready":
                state = "uninstalled" if installed is None else installed["state"]
                raise VersionNotReady(
                    f"{name} {version} is {state}: only a ready version can be activated", state
                )
            conn.execute(
                """update feather.flow_activation set state = 'inactive', changed_by = %s, changed_at = %s, reason = %s
                   where tenant_id = %s and flow = %s and state = 'activating'
                     and not (package = %s and version_key = %s)""",
                (by, at, reason, tenant, flow, name, key),
            )
            row = conn.execute(
                f"""update feather.flow_activation
                    set state = case when state = 'active' then 'active' else 'activating' end,
                        changed_by = %s, changed_at = %s, reason = %s, error = null
                    where tenant_id = %s and flow = %s and package = %s and version_key = %s
                    returning {self._ACTIVATION}""",
                (by, at, reason, tenant, flow, name, key),
            ).fetchone()
            if row is None:
                raise KeyError(f"{name} {version} has no flow {flow}")
            conn.execute("select pg_notify(%s, %s)", (CATALOG_CHANNEL, tenant))
        return self._activation(row)

    def deactivate(self, flow: str, by: str, at: datetime, reason: str | None = None) -> FlowActivation:
        tenant = self._db.tenant
        with self._db.tx() as conn:
            found = conn.execute(
                """select package, version_key, state from feather.flow_activation
                   where tenant_id = %s and flow = %s and state in ('active', 'activating')
                   order by (state <> 'active') for update""",
                (tenant, flow),
            ).fetchall()
            if not found:
                raise KeyError(f"no version of {flow} is active")
            rows = conn.execute(
                f"""update feather.flow_activation set state = 'inactive', changed_by = %s, changed_at = %s,
                        reason = %s, last_active_at = case when state = 'active' then %s else last_active_at end
                    where tenant_id = %s and flow = %s and state in ('active', 'activating')
                    returning {self._ACTIVATION}, version_key""",
                (by, at, reason, at, tenant, flow),
            ).fetchall()
            conn.execute("select pg_notify(%s, %s)", (CATALOG_CHANNEL, tenant))
        first = (found[0]["package"], found[0]["version_key"])  # the active one, if there is one
        return self._activation(next(r for r in rows if (r["package"], r["version_key"]) == first))

    def _settle(
        self, name: str, version: str, to: str, at: datetime, error: str | None
    ) -> list[FlowActivation]:
        tenant, key = self._db.tenant, version_key(version)
        with self._db.tx() as conn:
            flows = [
                r["flow"]
                for r in conn.execute(
                    """select flow from feather.flow_activation
                       where tenant_id = %s and package = %s and version_key = %s and state = 'activating'
                       for update""",
                    (tenant, name, key),
                ).fetchall()
            ]
            if not flows:
                return []
            if to == "active":
                conn.execute(
                    """update feather.flow_activation set state = 'inactive', changed_at = %s, last_active_at = %s
                       where tenant_id = %s and flow = any(%s) and state = 'active'
                         and not (package = %s and version_key = %s)""",
                    (at, at, tenant, flows, name, key),
                )
            rows = conn.execute(
                f"""update feather.flow_activation set state = %s, changed_at = %s, error = %s,
                        last_active_at = case when %s::text = 'active' then %s else last_active_at end
                    where tenant_id = %s and package = %s and version_key = %s and state = 'activating'
                    returning {self._ACTIVATION}""",
                (to, at, error, to, at, tenant, name, key),
            ).fetchall()
            conn.execute("select pg_notify(%s, %s)", (CATALOG_CHANNEL, tenant))
        return sorted((self._activation(r) for r in rows), key=lambda a: a.flow)

    def promote(self, name: str, version: str, at: datetime) -> list[FlowActivation]:
        return self._settle(name, version, "active", at, None)

    def activation_failed(self, name: str, version: str, error: str, at: datetime) -> list[FlowActivation]:
        return self._settle(name, version, "failed", at, error)

    def activating(self) -> list[FlowActivation]:
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""select {self._ACTIVATION} from feather.flow_activation
                    where tenant_id = %s and state = 'activating' order by flow collate "C" limit %s""",
                (self._db.tenant, MAX_PACKAGE_ROWS),
            ).fetchall()
        return [self._activation(r) for r in rows]

    def active(self) -> list[FlowActivation]:
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""select {self._ACTIVATION} from feather.flow_activation
                    where tenant_id = %s and state = 'active' order by flow collate "C" limit %s""",
                (self._db.tenant, MAX_PACKAGE_ROWS),
            ).fetchall()
        return [self._activation(r) for r in rows]

    def listen(self, callback: Callable[[], None]) -> Callable[[], None]:
        return _listen(self._db, CATALOG_CHANNEL, callback, "catalog-listen")

    def prepared(
        self,
        name: str,
        version: str,
        state: PackageState,
        load_check: Mapping[str, Any],
        expect: Sequence[PackageState] = ("installed", "preparing"),
    ) -> bool:
        with self._db.tx() as conn:
            row = conn.execute(
                """update feather.package_version set state = %s, load_check = %s
                   where tenant_id = %s and name = %s and version_key = %s and state = any(%s)
                   returning name""",
                (state, _json(dict(load_check)), self._db.tenant, name, version_key(version), list(expect)),
            ).fetchone()
            if row is not None:
                conn.execute("select pg_notify(%s, %s)", (CATALOG_CHANNEL, self._db.tenant))
        return row is not None

    def record_env(
        self, worker_id: str, lock_hash: str, state: EnvState, at: datetime, error: str | None = None
    ) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.package_env (tenant_id, worker_id, lock_hash, state, error, built_at)
                   values (%s, %s, %s, %s, %s, %s)
                   on conflict (tenant_id, worker_id, lock_hash)
                   do update set state = excluded.state, error = excluded.error, built_at = excluded.built_at""",
                (self._db.tenant, worker_id, lock_hash, state, error, at),
            )

    def envs(self, worker_id: str | None = None) -> list[PackageEnv]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select worker_id, lock_hash, state, error, built_at from feather.package_env
                   where tenant_id = %s and (%s::text is null or worker_id = %s)
                   order by worker_id collate "C", lock_hash collate "C" limit %s""",
                (self._db.tenant, worker_id, worker_id, MAX_PACKAGE_ROWS),
            ).fetchall()
        return [
            PackageEnv(r["worker_id"], r["lock_hash"], r["state"], r["error"], r["built_at"]) for r in rows
        ]

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
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.package_host
                       (tenant_id, worker_id, package, version, state, restarts, error, changed_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s)
                   on conflict (tenant_id, worker_id, package, version)
                   do update set state = excluded.state, restarts = excluded.restarts, error = excluded.error,
                                 changed_at = excluded.changed_at""",
                (self._db.tenant, worker_id, name, version, state, restarts, error, at),
            )

    def hosts(self, worker_id: str | None = None) -> list[PackageHost]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select worker_id, package, version, state, restarts, error, changed_at from feather.package_host
                   where tenant_id = %s and (%s::text is null or worker_id = %s)
                   order by worker_id collate "C", package collate "C", version collate "C" limit %s""",
                (self._db.tenant, worker_id, worker_id, MAX_PACKAGE_ROWS),
            ).fetchall()
        return [
            PackageHost(
                r["worker_id"],
                r["package"],
                r["version"],
                r["state"],
                r["restarts"],
                r["error"],
                r["changed_at"],
            )
            for r in rows
        ]


class _Audit(AuditStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def record(
        self,
        actor_type: str,
        actor: str,
        action: str,
        target: str | None,
        request: Mapping[str, Any],
        result: str,
    ) -> str:
        audit_id = new_id()
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.audit (tenant_id, audit_id, at, actor_type, actor, action, target, request,
                                              result)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    self._db.tenant,
                    audit_id,
                    self._db.clock.now(),
                    actor_type,
                    actor,
                    action,
                    target,
                    _json(dict(request)),
                    result,
                ),
            )
        return audit_id

    def list_audit(self, limit: int = 100, cursor: str | None = None) -> list[AuditRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.audit where tenant_id = %s and (%s::text is null or audit_id < %s)
                   order by audit_id desc limit %s""",
                (self._db.tenant, cursor, cursor, limit),
            ).fetchall()
        return [_audit_row(r) for r in rows]

    def for_target(self, target: str, since: datetime | None = None, limit: int = 500) -> list[AuditRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.audit
                   where tenant_id = %s and target = %s and (%s::timestamptz is null or at >= %s)
                   order by at, audit_id limit %s""",
                (self._db.tenant, target, since, since, limit),
            ).fetchall()
        return [_audit_row(r) for r in rows]


def _audit_row(r: Row) -> AuditRow:
    return AuditRow(
        r["audit_id"],
        r["at"],
        r["actor_type"],
        r["actor"],
        r["action"],
        r["target"],
        _unjson(r["request"]),
        r["result"],
    )


class _Reads(ReadStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def partition_grid(
        self, dataset: str, *, since: datetime | None = None, cursor: str | None = None, limit: int = 100
    ) -> list[PartitionRow]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from (
                       select distinct on (partition) partition, version_id, seal_seq, sealed_at, upserts, deletes
                       from feather.version
                       where tenant_id = %(tenant)s and dataset = %(dataset)s and status = 'sealed'
                       order by partition, seal_seq desc
                   ) latest
                   where (%(since)s::timestamptz is null or sealed_at >= %(since)s)
                     and (%(cursor)s::text is null or partition < %(cursor)s)
                   order by partition desc limit %(limit)s""",
                {
                    "tenant": self._db.tenant,
                    "dataset": dataset,
                    "since": since,
                    "cursor": cursor,
                    "limit": limit,
                },
            ).fetchall()
            cells: dict[str, dict[str, str]] = {r["version_id"]: {} for r in rows}
            if rows:
                found = conn.execute(
                    """select route, version_id, status from feather.delivery
                       where tenant_id = %s and version_id = any(%s) order by route""",
                    (self._db.tenant, list(cells)),
                ).fetchall()
                for d in found:
                    cells[d["version_id"]][d["route"]] = d["status"]
        return [
            PartitionRow(
                dataset,
                r["partition"],
                r["version_id"],
                r["seal_seq"],
                r["sealed_at"],
                r["upserts"],
                r["deletes"],
                cells[r["version_id"]],
            )
            for r in rows
        ]

    def failure_groups(
        self, route: str | None = None, *, limit: int = 100, cursor: str | None = None
    ) -> list[FailureGroup]:
        # The class is cut out of the message in Python, so the groups are made there, from one row per delivery.
        with self._db.tx() as conn:
            rows = conn.execute(
                """select route, coalesce(last_error, '') as error, version_id, seal_seq from feather.delivery
                   where tenant_id = %s and status = 'failed' and (%s::text is null or route = %s)
                   order by route, seal_seq""",
                (self._db.tenant, route, route),
            ).fetchall()
        found: dict[tuple[str, str, str], list[str]] = {}
        for r in rows:
            error_class, message = split_error(r["error"])
            found.setdefault((r["route"], error_class, message), []).append(r["version_id"])
        groups = [FailureGroup(r, c, m, len(vs), tuple(vs[:5])) for (r, c, m), vs in sorted(found.items())]
        if cursor is not None:
            after = tuple(json.loads(cursor))
            groups = [g for g in groups if (g.route, g.error_class, g.message) > after]
        return groups[:limit]

    def delivery_counts(self) -> dict[str, int]:
        with self._db.tx() as conn:
            rows = conn.execute(
                "select status, count(*) as n from feather.delivery where tenant_id = %s group by status",
                (self._db.tenant,),
            ).fetchall()
        return {r["status"]: r["n"] for r in rows}

    def retried_tasks(self, run_ids: Sequence[str]) -> dict[str, int]:
        if not run_ids:
            return {}
        with self._db.tx() as conn:
            rows = conn.execute(
                """select run_id, count(*) as n from feather.task
                   where tenant_id = %s and run_id = any(%s) and attempt > 1 group by run_id""",
                (self._db.tenant, list(run_ids)),
            ).fetchall()
        return {r["run_id"]: r["n"] for r in rows}

    def waiting(self, route: str, partitions: Sequence[str]) -> dict[tuple[str, str], int]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select dataset, partition, count(*) as n from feather.delivery
                   where tenant_id = %s and route = %s and partition = any(%s) and status in ('pending', 'held')
                   group by dataset, partition""",
                (self._db.tenant, route, list(partitions)),
            ).fetchall()
        return {(r["dataset"], r["partition"]): r["n"] for r in rows}


class _Inbox(InboxStore):
    def __init__(self, db: _Db, versions: _Versions) -> None:
        self._db = db
        self._versions = versions
        self._known: set[str] = set()

    @staticmethod
    def _partition(day: date) -> str:
        return f"inbox_{day:%Y%m%d}"

    def _ensure_day(self, day: date) -> None:
        ensure_partition(
            self._db.pool, "inbox", self._partition(day), day, day + timedelta(days=1), self._known
        )

    def ensure_partitions(self, days_ahead: int = 7) -> None:
        today = self._db.clock.now().date()
        for offset in range(days_ahead + 1):
            self._ensure_day(today + timedelta(days=offset))

    def put(self, dataset: str, request: InboundRequest, event_id: str | None = None) -> bool:
        day = self._db.clock.now().date()
        self._ensure_day(day)
        with self._db.tx() as conn:
            if event_id is not None:
                new = conn.execute(
                    """insert into feather.inbox_event_id (tenant_id, dataset, event_id, day)
                       values (%s, %s, %s, %s) on conflict do nothing returning day""",
                    (self._db.tenant, dataset, event_id, day),
                ).fetchone()
                if new is None:
                    return False
            conn.execute(
                """insert into feather.inbox (tenant_id, day, dataset, event_id, method, path, headers, body, received_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    self._db.tenant,
                    day,
                    dataset,
                    event_id,
                    request.method,
                    request.path,
                    Jsonb(dict(request.headers)),
                    request.body,
                    request.received_at,
                ),
            )
        return True

    def put_many(self, events: Sequence[tuple[str, InboundRequest, str | None]]) -> list[bool]:
        """One transaction and few round trips however many events, which is what lets ingress keep up with a burst."""
        day = self._db.clock.now().date()
        self._ensure_day(day)
        with self._db.tx() as conn:
            return self.put_many_in(conn, day, events)

    def put_many_in(
        self,
        conn: psycopg.Connection[DictRow],
        day: date,
        events: Sequence[tuple[str, InboundRequest, str | None]],
    ) -> list[bool]:
        """`put_many` in the caller's transaction, on a day whose partition is there."""
        named = [(dataset, event_id) for dataset, _, event_id in events if event_id is not None]
        new: set[tuple[str, str]] = set()
        if named:
            rows = conn.execute(
                """insert into feather.inbox_event_id (tenant_id, dataset, event_id, day)
                   select %s, n.dataset, n.event_id, %s from unnest(%s::text[], %s::text[]) n(dataset, event_id)
                   on conflict do nothing returning dataset, event_id""",
                (self._db.tenant, day, [n[0] for n in named], [n[1] for n in named]),
            ).fetchall()
            new = {(row["dataset"], row["event_id"]) for row in rows}
        stored: list[bool] = []
        kept: list[tuple[str, InboundRequest, str | None]] = []
        for dataset, request, event_id in events:
            is_new = event_id is None or (dataset, event_id) in new
            if event_id is not None:
                new.discard((dataset, event_id))  # a repeat within these events is a repeat too
            stored.append(is_new)
            if is_new:
                kept.append((dataset, request, event_id))
        if kept:
            conn.execute(
                """insert into feather.inbox (tenant_id, day, dataset, event_id, method, path, headers, body, received_at)
                   select %s, %s, t.dataset, t.event_id, t.method, t.path, t.headers, t.body, t.received_at
                   from unnest(
                     %s::text[], %s::text[], %s::text[], %s::text[], %s::jsonb[], %s::bytea[], %s::timestamptz[])
                   with ordinality as t(dataset, event_id, method, path, headers, body, received_at, n)
                   order by t.n""",
                (
                    self._db.tenant,
                    day,
                    [k[0] for k in kept],
                    [k[2] for k in kept],
                    [k[1].method for k in kept],
                    [k[1].path for k in kept],
                    [Jsonb(dict(k[1].headers)) for k in kept],
                    [k[1].body for k in kept],
                    [k[1].received_at for k in kept],
                ),
            )
        return stored

    def take(self, dataset: str, limit: int, max_bytes: int | None = None) -> list[InboxEvent]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """with firsts as (
                       select * from feather.inbox
                       where tenant_id = %s and dataset = %s and sealed_at is null and held_at is null
                       order by event_seq limit %s),
                   sized as (
                       select *, sum(octet_length(body)) over (order by event_seq) - octet_length(body) as before
                       from firsts)
                   select * from sized where %s::bigint is null or before < %s::bigint order by event_seq""",
                (self._db.tenant, dataset, limit, max_bytes, max_bytes),
            ).fetchall()
        return [
            InboxEvent(
                r["event_seq"],
                r["dataset"],
                r["event_id"],
                InboundRequest(r["method"], r["path"], r["headers"], bytes(r["body"]), r["received_at"]),
                r["received_at"],
                r["tries"],
                r["retry_at"],
            )
            for r in rows
        ]

    def oldest_age(self, dataset: str, now: datetime) -> timedelta | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select received_at from feather.inbox
                   where tenant_id = %s and dataset = %s and sealed_at is null and held_at is null
                   order by event_seq limit 1""",
                (self._db.tenant, dataset),
            ).fetchone()
        return None if row is None else now - row["received_at"]

    def defer(self, event_seq: int, until: datetime) -> None:
        with self._db.tx() as conn:
            conn.execute(
                """update feather.inbox set tries = tries + 1, retry_at = %s
                   where tenant_id = %s and event_seq = %s and sealed_at is null and held_at is null""",
                (until, self._db.tenant, event_seq),
            )

    def hold(self, event_seq: int) -> None:
        with self._db.tx() as conn:
            held = conn.execute(
                """update feather.inbox set held_at = %s
                   where tenant_id = %s and event_seq = %s and sealed_at is null and held_at is null
                   returning event_seq""",
                (self._db.clock.now(), self._db.tenant, event_seq),
            ).fetchone()
        if held is None:
            raise StaleWriter("another sealer has dealt with this event")

    def reprocess(self, dataset: str, event_seq: int) -> bool:
        with self._db.tx() as conn:
            found = conn.execute(
                """update feather.inbox set held_at = null, tries = 0, retry_at = null
                   where tenant_id = %s and dataset = %s and event_seq = %s and held_at is not null
                   returning event_seq""",
                (self._db.tenant, dataset, event_seq),
            ).fetchone()
        return found is not None

    def release(self, dataset: str, event_seq: int) -> bool:
        with self._db.tx() as conn:
            found = conn.execute(
                """update feather.inbox set held_at = null, sealed_at = %s
                   where tenant_id = %s and dataset = %s and event_seq = %s and held_at is not null
                   returning event_seq""",
                (self._db.clock.now(), self._db.tenant, dataset, event_seq),
            ).fetchone()
        return found is not None

    def seal_batch(
        self,
        event_seqs: Sequence[int],
        drafts: Sequence[VersionDraft],
        routes: Sequence[str],
        writer: str | None = None,
    ) -> list[Version]:
        with self._db.tx() as conn:
            # Before any row, as `seal_in` wants it first: a sealer that held a version's row and waited for this
            # would deadlock with one that held this and wanted the row.
            conn.execute(SEAL_LOCK)
            marked = conn.execute(
                """update feather.inbox set sealed_at = %s
                   where tenant_id = %s and event_seq = any(%s) and sealed_at is null and held_at is null
                   returning event_seq""",
                (self._db.clock.now(), self._db.tenant, list(event_seqs)),
            ).fetchall()
            if len(marked) != len(set(event_seqs)):
                raise StaleWriter("another sealer has dealt with an event of this batch")
            for draft in drafts:
                self._versions.stage_in(conn, draft, writer)
            return self._versions.seal_in(conn, [draft.version_id for draft in drafts], routes, writer)

    def days(self) -> list[date]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select c.relname from pg_inherits i join pg_class c on c.oid = i.inhrelid
                   where i.inhparent = 'feather.inbox'::regclass"""
            ).fetchall()
        return sorted(datetime.strptime(r["relname"], "inbox_%Y%m%d").date() for r in rows)

    def day_status(self, day: date) -> InboxDay:
        with self._db.tx() as conn:
            row = conn.execute(
                """select count(*) filter (where sealed_at is null) as unsealed,
                          min(event_seq) filter (where sealed_at is null) as first_unsealed,
                          max(sealed_at) as last_sealed
                   from feather.inbox where tenant_id = %s and day = %s""",
                (self._db.tenant, day),
            ).fetchone()
        assert row is not None
        return InboxDay(row["unsealed"], row["first_unsealed"], row["last_sealed"])

    def drop_day(self, day: date) -> bool:
        """Drop the day's partition, for every tenant, as the spool does: so any tenant's unsealed event keeps it."""
        partition = sql.Identifier("feather", self._partition(day))
        with self._db.tx() as conn:
            found = conn.execute(
                "select to_regclass(%s) as found", (f"feather.{self._partition(day)}",)
            ).fetchone()
            if found is not None and found["found"] is not None:
                # Nothing can write to the day between this look and the drop. The parent first, as
                # the drop takes it anyway: a `take` holding the parent can't then wait on us (S14).
                conn.execute("lock table feather.inbox in access exclusive mode")
                conn.execute(sql.SQL("lock table {} in access exclusive mode").format(partition))
                waiting = conn.execute(
                    sql.SQL("select 1 from {} where sealed_at is null limit 1").format(partition)
                ).fetchone()
                if waiting is not None:
                    return False
                conn.execute(sql.SQL("drop table {}").format(partition))
            conn.execute("delete from feather.inbox_event_id where day = %s", (day,))
        self._known.discard(self._partition(day))
        return True


class _EnvConfig(EnvConfigStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    @staticmethod
    def _entry(row: Mapping[str, Any]) -> EnvEntry:
        return EnvEntry(
            row["env"], row["kind"], row["name"], _unjson(row["value"]), row["changed_by"], row["changed_at"]
        )

    def entries(self, env: str, kind: EnvKind | None = None) -> list[EnvEntry]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select env, kind, name, value, changed_by, changed_at from feather.env_config
                   where tenant_id = %s and env = %s and (%s::text is null or kind = %s)
                   order by array_position(array['connection', 'budget', 'setting'], kind), name collate "C"
                   limit %s""",
                (self._db.tenant, env, kind, kind, MAX_ENV_ROWS),
            ).fetchall()
        return [self._entry(r) for r in rows]

    def entry(self, env: str, kind: EnvKind, name: str) -> EnvEntry | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select env, kind, name, value, changed_by, changed_at from feather.env_config
                   where tenant_id = %s and env = %s and kind = %s and name = %s""",
                (self._db.tenant, env, kind, name),
            ).fetchone()
        return None if row is None else self._entry(row)

    def put(self, entry: EnvEntry) -> EnvEntry | None:
        with self._db.tx() as conn:
            old = conn.execute(
                """select env, kind, name, value, changed_by, changed_at from feather.env_config
                   where tenant_id = %s and env = %s and kind = %s and name = %s for update""",
                (self._db.tenant, entry.env, entry.kind, entry.name),
            ).fetchone()
            conn.execute(
                """insert into feather.env_config (tenant_id, env, kind, name, value, changed_by, changed_at)
                   values (%s, %s, %s, %s, %s, %s, %s)
                   on conflict (tenant_id, env, kind, name) do update
                   set value = excluded.value, changed_by = excluded.changed_by, changed_at = excluded.changed_at""",
                (
                    self._db.tenant,
                    entry.env,
                    entry.kind,
                    entry.name,
                    _json(entry.value),
                    entry.changed_by,
                    entry.changed_at,
                ),
            )
        return None if old is None else self._entry(old)

    def remove(self, env: str, kind: EnvKind, name: str) -> EnvEntry | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """delete from feather.env_config where tenant_id = %s and env = %s and kind = %s and name = %s
                   returning env, kind, name, value, changed_by, changed_at""",
                (self._db.tenant, env, kind, name),
            ).fetchone()
        return None if row is None else self._entry(row)


class _Secrets(SecretStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    _COLUMNS = "env, name, ciphertext, nonce, key_id, updated_by, updated_at"

    @staticmethod
    def _secret(row: Mapping[str, Any]) -> SealedSecret:
        return SealedSecret(
            row["env"],
            row["name"],
            bytes(row["ciphertext"]),
            bytes(row["nonce"]),
            row["key_id"],
            row["updated_by"],
            row["updated_at"],
        )

    def _upsert(self, conn: psycopg.Connection[DictRow], secret: SealedSecret) -> bool:
        row = conn.execute(
            f"""insert into feather.secret (tenant_id, {self._COLUMNS}) values (%s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (tenant_id, env, name) do update
                set ciphertext = excluded.ciphertext, nonce = excluded.nonce, key_id = excluded.key_id,
                    updated_by = excluded.updated_by, updated_at = excluded.updated_at
                returning (xmax <> 0) as replaced""",
            (
                self._db.tenant,
                secret.env,
                secret.name,
                secret.ciphertext,
                secret.nonce,
                secret.key_id,
                secret.updated_by,
                secret.updated_at,
            ),
        ).fetchone()
        return bool(row and row["replaced"])

    @property
    def tenant(self) -> str:
        return self._db.tenant

    def _locked_key(
        self, conn: psycopg.Connection[DictRow], env: str, mode: Literal["share", "update"]
    ) -> str | None:
        query = (
            "select key_id from feather.secret_key where tenant_id = %s and env = %s for share"
            if mode == "share"
            else "select key_id from feather.secret_key where tenant_id = %s and env = %s for update"
        )
        row = conn.execute(query, (self._db.tenant, env)).fetchone()
        return None if row is None else str(row["key_id"])

    def put(self, secret: SealedSecret) -> bool:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.secret_key (tenant_id, env, key_id, changed_at) values (%s, %s, %s, %s)
                   on conflict (tenant_id, env) do nothing""",
                (self._db.tenant, secret.env, secret.key_id, secret.updated_at),
            )
            active = self._locked_key(conn, secret.env, "share")  # waits for a rotation in progress
            if active != secret.key_id:
                raise KeyMismatch(active, secret.key_id)
            return self._upsert(conn, secret)

    def active_key(self, env: str) -> str | None:
        with self._db.tx() as conn:
            row = conn.execute(
                "select key_id from feather.secret_key where tenant_id = %s and env = %s",
                (self._db.tenant, env),
            ).fetchone()
        return None if row is None else str(row["key_id"])

    def get(self, env: str, name: str) -> SealedSecret | None:
        with self._db.tx() as conn:
            row = conn.execute(
                f"select {self._COLUMNS} from feather.secret where tenant_id = %s and env = %s and name = %s",
                (self._db.tenant, env, name),
            ).fetchone()
        return None if row is None else self._secret(row)

    def names(self, env: str) -> list[SealedSecret]:
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""select {self._COLUMNS} from feather.secret where tenant_id = %s and env = %s
                    order by name collate "C" limit %s""",
                (self._db.tenant, env, MAX_ENV_ROWS),
            ).fetchall()
        return [self._secret(r) for r in rows]

    def remove(self, env: str, name: str) -> bool:
        with self._db.tx() as conn:
            done = conn.execute(
                "delete from feather.secret where tenant_id = %s and env = %s and name = %s",
                (self._db.tenant, env, name),
            )
        return done.rowcount > 0

    def rotate(
        self, env: str, expected: str | None, new_key: str, reseal: Callable[[SealedSecret], SealedSecret]
    ) -> int:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.secret_key (tenant_id, env, key_id, changed_at) values (%s, %s, %s, now())
                   on conflict (tenant_id, env) do nothing""",
                (self._db.tenant, env, expected or new_key),
            )
            active = self._locked_key(conn, env, "update")
            if active != (expected or new_key):
                raise KeyMismatch(active, expected)
            rows = conn.execute(
                f"""select {self._COLUMNS} from feather.secret where tenant_id = %s and env = %s
                    order by name collate "C" for update""",
                (self._db.tenant, env),
            ).fetchall()
            for row in rows:
                self._upsert(conn, reseal(self._secret(row)))
            conn.execute(
                "update feather.secret_key set key_id = %s, changed_at = now() where tenant_id = %s and env = %s",
                (new_key, self._db.tenant, env),
            )
        return len(rows)


class _Webhooks(WebhookEventStore):
    def __init__(self, db: _Db, inbox: _Inbox) -> None:
        self._db = db
        self._inbox = inbox

    def keep_many(self, events: Sequence[tuple[WebhookEvent, str, InboundRequest]]) -> list[bool]:
        """One transaction and few round trips however many events, as `_Inbox.put_many`: a burst comes through here."""
        if not events:
            return []
        day = self._db.clock.now().date()
        self._inbox._ensure_day(day)  # pyright: ignore[reportPrivateUsage]
        with self._db.tx() as conn:
            rows = conn.execute(
                """insert into feather.webhook_event (tenant_id, package, webhook, event_key, stamp, body, headers,
                       received_at, processed_at)
                   select %s, t.package, t.webhook, t.event_key, t.stamp, t.body, t.headers, t.received_at,
                       t.received_at
                   from unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::bytea[], %s::jsonb[],
                       %s::timestamptz[]) with ordinality as t(package, webhook, event_key, stamp, body, headers,
                       received_at, n)
                   order by t.n
                   on conflict (tenant_id, package, webhook, event_key) do nothing
                   returning package, webhook, event_key""",
                (
                    self._db.tenant,
                    [e.package for e, _, _ in events],
                    [e.webhook for e, _, _ in events],
                    [e.key for e, _, _ in events],
                    [e.stamp for e, _, _ in events],
                    [e.body for e, _, _ in events],
                    [_json(dict(e.headers)) for e, _, _ in events],
                    [e.received_at for e, _, _ in events],
                ),
            ).fetchall()
            new = {(r["package"], r["webhook"], r["event_key"]) for r in rows}
            kept: list[bool] = []
            for event, _, _ in events:
                found = (event.package, event.webhook, event.key)
                kept.append(found in new)
                new.discard(found)  # a repeat within these events is a repeat too
            into = [
                (dataset, request, event.key)
                for (event, dataset, request), k in zip(events, kept, strict=True)
                if k
            ]
            if into:
                self._inbox.put_many_in(conn, day, into)
        return kept

    def add(self, event: WebhookEvent) -> bool:
        with self._db.tx() as conn:
            done = conn.execute(
                """insert into feather.webhook_event (tenant_id, package, webhook, event_key, stamp, body, headers,
                       received_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s)
                   on conflict (tenant_id, package, webhook, event_key) do nothing""",
                (
                    self._db.tenant,
                    event.package,
                    event.webhook,
                    event.key,
                    event.stamp,
                    event.body,
                    _json(dict(event.headers)),
                    event.received_at,
                ),
            )
        return done.rowcount > 0

    def events(self, package: str, webhook: str, limit: int = 100) -> list[WebhookEvent]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select package, webhook, event_key, stamp, body, headers, received_at, processed_at
                   from feather.webhook_event
                   where tenant_id = %s and package = %s and webhook = %s order by received_at, event_key limit %s""",
                (self._db.tenant, package, webhook, max(0, min(limit, MAX_ENV_ROWS))),
            ).fetchall()
        return [
            WebhookEvent(
                r["package"],
                r["webhook"],
                r["event_key"],
                r["stamp"],
                bytes(r["body"]),
                _unjson(r["headers"]),
                r["received_at"],
                r["processed_at"],
            )
            for r in rows
        ]

    def processed(self, package: str, webhook: str, keys: Sequence[str], at: datetime) -> int:
        with self._db.tx() as conn:
            done = conn.execute(
                """update feather.webhook_event set processed_at = %s
                   where tenant_id = %s and package = %s and webhook = %s and event_key = any(%s)""",
                (at, self._db.tenant, package, webhook, list(keys)),
            )
        return done.rowcount

    def purge(self, before: datetime) -> int:
        with self._db.tx() as conn:
            done = conn.execute(
                """delete from feather.webhook_event
                   where tenant_id = %s and processed_at is not null and received_at < %s""",
                (self._db.tenant, before),
            )
        return done.rowcount


def _listen(db: "_Db", channel: str, callback: Callable[[], None], name: str) -> Callable[[], None]:
    """Call `callback` on each NOTIFY on `channel` for this tenant, and once each time it starts listening, for what it
    missed meanwhile; on a dedicated connection, on a daemon thread. Returns what stops it."""
    stop = threading.Event()
    given = db.pool.conninfo
    conninfo = given() if callable(given) else given
    tenant = db.tenant

    def run() -> None:
        while not stop.is_set():
            try:
                with psycopg.connect(conninfo, autocommit=True) as conn:
                    conn.execute(sql.SQL("listen {}").format(sql.Identifier(channel)))
                    callback()  # anything missed while not listening
                    while not stop.is_set():
                        for note in conn.notifies(timeout=1.0, stop_after=1):
                            if note.payload == tenant:
                                callback()
            except psycopg.Error:
                log.warning("listening on %s failed; trying again", channel, exc_info=True)
                stop.wait(5)

    threading.Thread(target=run, name=name, daemon=True).start()
    return stop.set


class _Readiness(ReadinessStore):
    def __init__(self, db: _Db) -> None:
        self._db = db

    def listen(self, callback: Callable[[], None]) -> Callable[[], None]:
        return _listen(self._db, READY_CHANNEL, callback, "ready-listen")

    def close(self, dataset: str, partition: str, actor: str, reason: str, at: datetime) -> PartitionClose:
        with self._db.tx() as conn:
            conn.execute(
                """insert into feather.readiness_close (tenant_id, dataset, partition, closed_at, closed_by, reason)
                   values (%s, %s, %s, %s, %s, %s) on conflict do nothing""",
                (self._db.tenant, dataset, partition, at, actor, reason),
            )
            row = conn.execute(
                """select * from feather.readiness_close where tenant_id = %s and dataset = %s and partition = %s""",
                (self._db.tenant, dataset, partition),
            ).fetchone()
            conn.execute("select pg_notify(%s, %s)", (READY_CHANNEL, self._db.tenant))
        assert row is not None
        return _close(row)

    def closes(self, dataset: str, partitions: Sequence[str]) -> dict[str, PartitionClose]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.readiness_close
                   where tenant_id = %s and dataset = %s and partition = any(%s)""",
                (self._db.tenant, dataset, list(partitions)),
            ).fetchall()
        return {r["partition"]: _close(r) for r in rows}

    def fire(self, firing: Firing, params: Mapping[str, Any]) -> str | None:
        run_id = new_id()
        now = self._db.clock.now()
        with self._db.tx() as conn:
            done = conn.execute(
                """insert into feather.readiness_firing (tenant_id, flow, ready_set, period, scope, seq, reason,
                       versions, fired_at, actor, run_id)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing""",
                (
                    self._db.tenant,
                    firing.flow,
                    firing.ready_set,
                    firing.period,
                    firing.scope,
                    firing.seq,
                    firing.reason,
                    _json(dict(firing.versions)),
                    firing.fired_at,
                    firing.actor,
                    run_id,
                ),
            )
            if done.rowcount == 0:
                return None
            conn.execute(
                """insert into feather.run (tenant_id, run_id, flow, trigger, actor, params, status, reason,
                                            created_at, updated_at)
                   values (%s, %s, %s, 'ready', %s, %s, 'queued', null, %s, %s)""",
                (self._db.tenant, run_id, firing.flow, firing.actor, _json(dict(params)), now, now),
            )
        return run_id

    def firings(self, flow: str, ready_set: str | None = None, limit: int = 200) -> list[Firing]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select * from feather.readiness_firing
                   where tenant_id = %s and flow = %s and (%s::text is null or ready_set = %s)
                   order by fired_at desc, seq desc limit %s""",
                (self._db.tenant, flow, ready_set, ready_set, max(0, min(limit, 1000))),
            ).fetchall()
        return [
            Firing(
                r["flow"],
                r["ready_set"],
                r["period"],
                r["scope"],
                r["seq"],
                r["reason"],
                _unjson(r["versions"]),
                r["fired_at"],
                r["actor"],
                r["run_id"],
            )
            for r in rows
        ]

    def latest_sealed(self, dataset: str, since: datetime) -> dict[str, SealedPartition]:
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""select distinct on (v.partition) * from ({_SEALED}
                        where v.tenant_id = %(t)s and v.dataset = %(d)s and v.status = 'sealed'
                          and v.partition in (select partition from feather.version
                                              where tenant_id = %(t)s and dataset = %(d)s and status = 'sealed'
                                                and sealed_at >= %(since)s)) v
                    order by v.partition, v.seal_seq desc""",
                {"t": self._db.tenant, "d": dataset, "since": since},
            ).fetchall()
        return {r["partition"]: _sealed(r) for r in rows}

    def sealed_since(self, dataset: str, since: datetime) -> list[SealedPartition]:
        with self._db.tx() as conn:
            rows = conn.execute(
                f"""{_SEALED} where v.tenant_id = %s and v.dataset = %s and v.status = 'sealed' and v.sealed_at >= %s
                    order by v.seal_seq""",
                (self._db.tenant, dataset, since),
            ).fetchall()
        return [_sealed(r) for r in rows]

    def open_deliveries(self, route: str, dataset: str) -> dict[str, int]:
        with self._db.tx() as conn:
            rows = conn.execute(
                """select partition, min(seal_seq) as seq from feather.delivery
                   where tenant_id = %s and route = %s and dataset = %s and status <> all(%s)
                   group by partition""",
                (self._db.tenant, route, dataset, sorted(CLOSED)),
            ).fetchall()
        return {r["partition"]: r["seq"] for r in rows}

    def oldest_unsealed(self, dataset: str) -> datetime | None:
        with self._db.tx() as conn:
            row = conn.execute(
                """select min(received_at) as at from feather.inbox
                   where tenant_id = %s and dataset = %s and sealed_at is null and held_at is null""",
                (self._db.tenant, dataset),
            ).fetchone()
        return None if row is None else row["at"]


_SEALED = """select v.partition, v.version_id, v.seal_seq, v.run_id, v.sealed_at, r.status as run_status,
                     r.created_at as run_created_at
              from feather.version v join feather.run r on r.tenant_id = v.tenant_id and r.run_id = v.run_id"""


def _sealed(r: Row) -> SealedPartition:
    return SealedPartition(
        r["partition"],
        r["version_id"],
        r["seal_seq"],
        r["run_id"],
        r["run_status"],
        r["run_created_at"],
        r["sealed_at"],
    )


def _close(r: Row) -> PartitionClose:
    return PartitionClose(r["dataset"], r["partition"], r["closed_at"], r["closed_by"], r["reason"])


class PostgresStore(Store):
    """A `Store` that keeps everything in Postgres, for one tenant. `clock` defaults to the system clock."""

    _pool: Pool
    _db: _Db

    def __init__(self, pool: Pool, clock: Clock | None = None, tenant: str = "default") -> None:
        db = _Db(pool, clock or SystemClock(), tenant)
        versions = _Versions(db)
        inbox = _Inbox(db, versions)
        super().__init__(
            runs=_Runs(db),
            versions=versions,
            ledger=_Ledger(db),
            state=_State(db),
            quarantine=_Quarantine(db),
            budgets=PostgresBudgets(db),
            catalog=_Catalog(db),
            workers=_Workers(db),
            schedules=_Schedules(db),
            overrides=_Overrides(db),
            users=_Users(db),
            audit=_Audit(db),
            reads=_Reads(db),
            inbox=inbox,
            responses=_Responses(db),
            registry=_Registry(db),
            packages=_Packages(db),
            env_config=_EnvConfig(db),
            secrets=_Secrets(db),
            webhooks=_Webhooks(db, inbox),
            readiness=_Readiness(db),
        )
        object.__setattr__(self, "_pool", pool)  # `Store` is frozen
        object.__setattr__(self, "_db", db)

    def ensure_partitions(self, days_ahead: int) -> None:
        """Make the inbox's daily partitions and the task logs' and delivery attempts' monthly ones for today through
        `days_ahead` days on, as a role that owns their parents. Hosts write as a role that can't make them."""
        self.inbox.ensure_partitions(days_ahead)
        now = self._db.clock.now()
        for parent in ("task_log", "delivery_attempt"):
            for at in (now, now + timedelta(days=days_ahead)):
                self._db.ensure_month(parent, at)

    def is_unavailable(self, error: BaseException) -> bool:
        """A lost or refused connection, a pool timeout, a cancelled statement or a deadlock, and not a value too big
        for an index, which Postgres counts among them but which the same row will meet again."""
        if isinstance(error, psycopg.errors.ProgramLimitExceeded):
            return False
        lost = isinstance(error, psycopg.OperationalError | psycopg.InterfaceError)
        return lost or super().is_unavailable(error)

    def close(self) -> None:
        """Close the pool, which the spool of the same `connect` shares. Closing it again changes nothing."""
        atexit.unregister(self._pool.close)
        self._pool.close()
