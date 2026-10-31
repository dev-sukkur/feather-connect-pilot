"""The spool in Postgres: zstd Parquet chunks, in tables partitioned by day (design 7.2)."""

import io
from datetime import date, timedelta
from typing import LiteralString, cast

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql

from feather_connect.clock import Clock, SystemClock
from feather_connect.storage.postgres.support import Pool, ensure_partition
from feather_connect.storage.spool import ChunkKind, ChunkRef, Spool

# The day of a version's chunks, which its first write records, so that a rewrite after midnight finds them.
_DAY_OF: LiteralString = (
    "(select day from feather.spool_version where tenant_id = %(tenant)s and version_id = %(version)s)"
)


def _partition(day: date) -> str:
    return f"spool_chunk_{day:%Y%m%d}"


class PostgresSpool(Spool):
    """Chunks as `bytea`. A snapshot goes to the plain table `spool_snapshot`, so that it outlives the day
    partition of its version; every other kind goes to `spool_chunk`, in the partition of that day."""

    def __init__(self, pool: Pool, clock: Clock | None = None, tenant: str = "default") -> None:
        self._pool = pool
        self._clock = clock or SystemClock()
        self._tenant = tenant
        self._known: set[str] = set()

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

    def drop_day(self, day: date) -> None:
        """Drop the day's partition with the chunks in it, for every tenant. Snapshots stay."""
        with self._pool.connection() as conn:
            conn.execute(
                sql.SQL("drop table if exists {}").format(sql.Identifier("feather", _partition(day)))
            )
            conn.execute("delete from feather.spool_version where day = %s", (day,))
        self._known.discard(_partition(day))

    def put_chunk(self, version_id: str, chunk_no: int, kind: ChunkKind, table: pa.Table) -> ChunkRef:
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

    def delete_chunks(self, version_id: str, kind: ChunkKind, from_chunk: int = 0) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                f"""delete from {self._table(kind)}
                    where tenant_id = %(tenant)s and version_id = %(version)s and chunk_no >= %(from)s{self._where(kind)}""",
                {"tenant": self._tenant, "version": version_id, "from": from_chunk, "kind": kind},
            )

    def delete_version(self, version_id: str) -> None:
        params = {"tenant": self._tenant, "version": version_id}
        with self._pool.connection() as conn:
            conn.execute(
                f"delete from feather.spool_chunk where tenant_id = %(tenant)s and version_id = %(version)s and day = {_DAY_OF}",
                params,
            )
            conn.execute(
                "delete from feather.spool_snapshot where tenant_id = %(tenant)s and version_id = %(version)s",
                params,
            )
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
