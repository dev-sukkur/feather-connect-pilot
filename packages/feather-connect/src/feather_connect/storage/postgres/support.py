"""What the Postgres spool and store share: the pool type, session settings and partition creation."""

from datetime import date, datetime
from typing import Any

import psycopg
from psycopg import conninfo, sql
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

from feather_connect.storage.port import StaleWriter, VersionSealed

Pool = ConnectionPool[psycopg.Connection[DictRow]]

# How a session of ours deals with a database or a network that has gone quiet. Without these a connect waits up to
# 130 s (psycopg's default), and a half-open connection blocks its query with no limit.
CONNECT_TIMEOUT_S = 5  # the database is local or close by: longer only hides a hung proxy
KEEPALIVES_IDLE_S = 30  # with the next two, a silent peer is given up on after about 60 s
KEEPALIVES_INTERVAL_S = 10
KEEPALIVES_COUNT = 3
TCP_USER_TIMEOUT_MS = (
    60_000  # the same limit while sent data waits for an acknowledgement, where keepalives don't run
)
# It limits one statement, not a transaction, so a long transaction of short statements is fine. Our statements take
# milliseconds; the slowest are retention's bulk deletes and a wait for the seal lock, and a task or the next pass tries
# those again. Migrations don't use these sessions (`migrate` opens its own engine), so they have no limit.
STATEMENT_TIMEOUT_MS = 120_000


def connection_timeouts() -> dict[str, int]:
    """Return how a connection deals with a network that has gone quiet, as libpq parameters."""
    return {
        "connect_timeout": CONNECT_TIMEOUT_S,
        "keepalives": 1,
        "keepalives_idle": KEEPALIVES_IDLE_S,
        "keepalives_interval": KEEPALIVES_INTERVAL_S,
        "keepalives_count": KEEPALIVES_COUNT,
        "tcp_user_timeout": TCP_USER_TIMEOUT_MS,
    }


def session_options(database_url: str) -> dict[str, Any]:
    """Return the connection settings of every session we open, on a pool or alone, as keywords for psycopg.

    Any `options` in the URL are kept, since a keyword would replace them.
    """
    options = conninfo.conninfo_to_dict(database_url).get("options")
    return {
        "application_name": "feather",
        **connection_timeouts(),
        "options": f"{options or ''} -c statement_timeout={STATEMENT_TIMEOUT_MS}".strip(),
    }


def guard_version(
    conn: psycopg.Connection[DictRow], tenant: str, version_id: str, writer: str | None
) -> None:
    """Refuse the transaction's change to a version that is sealed, or that `writer` doesn't hold the claim of.

    It locks both rows until the transaction ends, so a seal or a later claim waits for this change or comes before it.
    Every transaction takes `spool_version` first, then `version`, so that they can't deadlock.
    """
    owner = None
    if writer is not None:
        owner = conn.execute(
            "select writer from feather.spool_version where tenant_id = %s and version_id = %s for share",
            (tenant, version_id),
        ).fetchone()
    version = conn.execute(
        "select status from feather.version where tenant_id = %s and version_id = %s for share",
        (tenant, version_id),
    ).fetchone()
    if version is not None and version["status"] == "sealed":
        raise VersionSealed(f"version {version_id} is sealed")
    if writer is not None and (owner is None or owner["writer"] != writer):
        raise StaleWriter(f"version {version_id} was claimed by a later execution")


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
