"""The spool in Postgres: zstd Parquet chunks, in tables partitioned by day (design 7.2)."""

import io
from datetime import date, timedelta
from typing import LiteralString, cast

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql
from psycopg.rows import DictRow

from feather_connect.clock import Clock, SystemClock
from feather_connect.storage.port import VersionStore
from feather_connect.storage.postgres.support import Pool, ensure_partition, guard_version
from feather_connect.storage.spool import ChunkKind, ChunkRef, Spool, is_working_kind

# The day of a version's chunks, which its first write records, so that a rewrite after midnight finds them.
_DAY_OF: LiteralString = (
    "(select day from feather.spool_version where tenant_id = %(tenant)s and version_id = %(version)s)"
)


def _partition(day: date) -> str:
    return f"spool_chunk_{day:%Y%m%d}"


_SIGNED = 2**63


def to_signed(row_hash: int) -> int:
    """A uint64 row hash as Postgres's signed bigint holds it."""
    return row_hash - 2 * _SIGNED if row_hash >= _SIGNED else row_hash


def from_signed(value: int) -> int:
    return value + 2 * _SIGNED if value < 0 else value


def index_keys(
    conn: psycopg.Connection[DictRow], tenant: str, version_id: str, dataset: str, partition: str
) -> int:
    """Write the key index of a version from its data chunks, in the caller's transaction: one insert for the version.

    Reads only the key, op and hash columns of each chunk. Returns how many keys it wrote."""
    keys: list[str] = []
    ops: list[str] = []
    hashes: list[int] = []
    chunk_nos: list[int] = []
    rows = conn.execute(
        """select c.chunk_no, c.data from feather.spool_chunk c
           join feather.spool_version s
             on s.tenant_id = c.tenant_id and s.version_id = c.version_id and s.day = c.day
           where c.tenant_id = %s and c.version_id = %s and c.kind = 'data' order by c.chunk_no""",
        (tenant, version_id),
    ).fetchall()
    for row in rows:
        table = pq.read_table(  # type: ignore[reportUnknownMemberType]
            io.BytesIO(bytes(row["data"])), columns=["_fc_key", "_fc_op", "_fc_row_hash"]
        )
        keys += cast("list[str]", table.column("_fc_key").to_pylist())
        ops += cast("list[str]", table.column("_fc_op").to_pylist())
        hashes += [to_signed(h) for h in cast("list[int]", table.column("_fc_row_hash").to_pylist())]
        chunk_nos += [row["chunk_no"]] * table.num_rows
    if not keys:
        return 0
    conn.execute(
        """insert into feather.record_key (tenant_id, dataset, key, version_id, partition, op, row_hash, chunk_no)
           select %s, %s, u.k, %s, %s, u.o, u.h, u.c
           from unnest(%s::text[], %s::text[], %s::bigint[], %s::int[]) as u(k, o, h, c)
           on conflict do nothing""",
        (tenant, dataset, version_id, partition, keys, ops, hashes, chunk_nos),
    )
    return len(keys)


class PostgresSpool(Spool):
    """Chunks as `bytea`. A snapshot goes to the plain table `spool_snapshot`, so that it outlives the day
    partition of its version; every other kind goes to `spool_chunk`, in the partition of that day."""

    def __init__(self, pool: Pool, clock: Clock | None = None, tenant: str = "default") -> None:
        self._pool = pool
        self._clock = clock or SystemClock()
        self._tenant = tenant
        self._known: set[str] = set()

    def bind(self, versions: VersionStore) -> None:
        """Nothing to do: the versions are in the same database, and each write checks them in its transaction."""

    def ensure_partitions(self, days_ahead: int = 7) -> None:
        """Create the day partitions from today to `days_ahead` days on."""
        today = self._clock.now().date()
        for offset in range(days_ahead + 1):
            self._ensure_day(today + timedelta(days=offset))

    def days(self) -> list[date]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "select distinct day from feather.spool_version where tenant_id = %s order by day",
                (self._tenant,),
            ).fetchall()
        return [r["day"] for r in rows]

    def versions_on(self, day: date) -> list[str]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "select version_id from feather.spool_version where tenant_id = %s and day = %s order by version_id",
                (self._tenant, day),
            ).fetchall()
        return [r["version_id"] for r in rows]

    def snapshot_versions(self) -> list[str]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "select distinct version_id from feather.spool_snapshot where tenant_id = %s order by version_id",
                (self._tenant,),
            ).fetchall()
        return [r["version_id"] for r in rows]

    def drop_empty_days(self, before: date) -> list[date]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """select to_date(substr(c.relname, 13), 'YYYYMMDD') as day from pg_inherits i
                   join pg_class c on c.oid = i.inhrelid
                   where i.inhparent = 'feather.spool_chunk'::regclass and c.relname ~ '^spool_chunk_[0-9]{8}$'
                   order by day"""
            ).fetchall()
            empty: list[date] = []
            for row in rows:
                day: date = row["day"]
                used = conn.execute(  # any tenant's version keeps it: the partition is shared
                    "select 1 from feather.spool_version where day = %s limit 1", (day,)
                ).fetchone()
                if day < before and used is None:
                    empty.append(day)
        for day in empty:
            self.drop_day(day)
        return empty

    def drop_day(self, day: date) -> None:
        """Drop the day's partition with the chunks in it, for every tenant. Snapshots stay."""
        with self._pool.connection() as conn:
            conn.execute(
                sql.SQL("drop table if exists {}").format(sql.Identifier("feather", _partition(day)))
            )
            conn.execute("delete from feather.spool_version where day = %s", (day,))
        self._known.discard(_partition(day))

    def put_chunk(
        self, version_id: str, chunk_no: int, kind: ChunkKind, table: pa.Table, writer: str | None = None
    ) -> ChunkRef:
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd")  # type: ignore[reportUnknownMemberType]
        data = sink.getvalue()
        params = {
            "tenant": self._tenant,
            "version": version_id,
            "no": chunk_no,
            "kind": kind,
            "data": data,
            "rows": table.num_rows,
            "size": len(data),
        }
        if kind == "snapshot":
            with self._pool.connection() as conn:
                guard_version(conn, self._tenant, version_id, writer)
                conn.execute(
                    """insert into feather.spool_snapshot (tenant_id, version_id, chunk_no, data, rows, size)
                       values (%(tenant)s, %(version)s, %(no)s, %(data)s, %(rows)s, %(size)s)
                       on conflict (tenant_id, version_id, chunk_no)
                       do update set data = excluded.data, rows = excluded.rows, size = excluded.size""",
                    params,
                )
        else:
            day = self._version_day(version_id)
            self._ensure_day(day)
            with self._pool.connection() as conn:
                guard_version(conn, self._tenant, version_id, writer)
                conn.execute(
                    """insert into feather.spool_chunk (tenant_id, day, version_id, chunk_no, kind, data, rows, size)
                       values (%(tenant)s, %(day)s, %(version)s, %(no)s, %(kind)s, %(data)s, %(rows)s, %(size)s)
                       on conflict (tenant_id, day, version_id, chunk_no, kind)
                       do update set data = excluded.data, rows = excluded.rows, size = excluded.size""",
                    params | {"day": day},
                )
        return ChunkRef(version_id, chunk_no, kind, table.num_rows, len(data))

    def get_chunk(self, version_id: str, chunk_no: int, kind: ChunkKind = "data") -> pa.Table:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""select data from {self._table(kind)}
                    where tenant_id = %(tenant)s and version_id = %(version)s and chunk_no = %(no)s{self._where(kind)}""",
                {"tenant": self._tenant, "version": version_id, "no": chunk_no, "kind": kind},
            ).fetchone()
        if row is None:
            raise KeyError(f"no {kind} chunk {chunk_no} for version {version_id}")
        return cast("pa.Table", pq.read_table(io.BytesIO(row["data"])))  # type: ignore[reportUnknownMemberType]

    def list_chunks(self, version_id: str, kind: ChunkKind = "data") -> list[ChunkRef]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""select chunk_no, rows, size from {self._table(kind)}
                    where tenant_id = %(tenant)s and version_id = %(version)s{self._where(kind)}
                    order by chunk_no""",
                {"tenant": self._tenant, "version": version_id, "kind": kind},
            ).fetchall()
        return [ChunkRef(version_id, r["chunk_no"], kind, r["rows"], r["size"]) for r in rows]

    def delete_chunks(
        self, version_id: str, kind: ChunkKind, from_chunk: int = 0, writer: str | None = None
    ) -> None:
        with self._pool.connection() as conn:
            if writer is not None or not is_working_kind(kind):
                guard_version(conn, self._tenant, version_id, writer)
            conn.execute(
                f"""delete from {self._table(kind)}
                    where tenant_id = %(tenant)s and version_id = %(version)s and chunk_no >= %(from)s{self._where(kind)}""",
                {"tenant": self._tenant, "version": version_id, "from": from_chunk, "kind": kind},
            )

    def delete_version(self, version_id: str, writer: str | None = None) -> None:
        with self._pool.connection() as conn:
            guard_version(conn, self._tenant, version_id, writer)
            self._delete_all(conn, version_id, keep_claim=writer is not None)

    def expire_chunks(self, version_id: str, kind: ChunkKind) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                f"""delete from {self._table(kind)}
                    where tenant_id = %(tenant)s and version_id = %(version)s{self._where(kind)}""",
                {"tenant": self._tenant, "version": version_id, "kind": kind},
            )

    def expire_version(self, version_id: str) -> None:
        with self._pool.connection() as conn:
            self._delete_all(conn, version_id, keep_claim=False)

    def _delete_all(self, conn: psycopg.Connection[DictRow], version_id: str, *, keep_claim: bool) -> None:
        params = {"tenant": self._tenant, "version": version_id}
        conn.execute(
            f"delete from feather.spool_chunk where tenant_id = %(tenant)s and version_id = %(version)s and day = {_DAY_OF}",
            params,
        )
        conn.execute(
            "delete from feather.spool_snapshot where tenant_id = %(tenant)s and version_id = %(version)s",
            params,
        )
        if not keep_claim:
            conn.execute(
                "delete from feather.spool_version where tenant_id = %(tenant)s and version_id = %(version)s",
                params,
            )

    @staticmethod
    def _table(kind: ChunkKind) -> LiteralString:
        return "feather.spool_snapshot" if kind == "snapshot" else "feather.spool_chunk"

    @staticmethod
    def _where(kind: ChunkKind) -> LiteralString:
        """The rest of the condition: for a chunk, its kind and its day, which lets Postgres prune partitions."""
        return "" if kind == "snapshot" else f" and kind = %(kind)s and day = {_DAY_OF}"

    def _version_day(self, version_id: str) -> date:
        """The day the version's chunks live on. Its first write records the current day."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """insert into feather.spool_version (tenant_id, version_id, day) values (%s, %s, %s)
                   on conflict (tenant_id, version_id) do update set day = feather.spool_version.day
                   returning day""",
                (self._tenant, version_id, self._clock.now().date()),
            ).fetchone()
        assert row is not None
        return row["day"]

    def _ensure_day(self, day: date) -> None:
        ensure_partition(
            self._pool, "spool_chunk", _partition(day), day, day + timedelta(days=1), self._known
        )
