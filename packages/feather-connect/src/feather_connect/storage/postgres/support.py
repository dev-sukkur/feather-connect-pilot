"""What the Postgres spool and store share: the pool type and partition creation."""

from datetime import date, datetime

import psycopg
from psycopg import sql
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

Pool = ConnectionPool[psycopg.Connection[DictRow]]

# Serialises partition creation, which would otherwise fail when two writers need the same new partition.
_PARTITION_LOCK = 7_262_410_002


def ensure_partition(
    pool: Pool, parent: str, name: str, start: date | datetime, end: date | datetime, known: set[str]
) -> None:
    """Create partition `name` of `feather.<parent>` for [start, end) unless it exists.

    Runs in a short transaction of its own, because creating a partition locks the parent until it commits.
    `known` remembers the partitions this process has seen, so that a write costs a lookup only the first time.
    """
    if name in known:
        return
    target = sql.Identifier("feather", name)
    with pool.connection() as conn:
        if conn.execute("select to_regclass(%s) as found", (f"feather.{name}",)).fetchone() == {
            "found": None
        }:
            conn.execute("select pg_advisory_xact_lock(%s)", (_PARTITION_LOCK,))
            conn.execute(
                sql.SQL("create table if not exists {} partition of {} for values from ({}) to ({})").format(
                    target, sql.Identifier("feather", parent), sql.Literal(start), sql.Literal(end)
                )
            )
    known.add(name)
