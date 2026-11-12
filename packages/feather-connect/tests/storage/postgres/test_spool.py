import io
from datetime import timedelta

import psycopg
import pyarrow.parquet as pq
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

from feather_connect.clock import FrozenClock
from feather_connect.core.records import Record
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.spool import records_to_table, snapshot_table
from feather_connect.testing.store_contract import T0, SpoolContract

pytestmark = pytest.mark.postgres

TABLE = records_to_table([Record("a", "upsert", {"x": 1}, 1)])
TABLE2 = records_to_table([Record("b", "upsert", {"x": 2}, 2), Record("c", "upsert", {"x": 3}, 3)])
SNAP = snapshot_table([("a", 1), ("b", 2)])


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(T0)


@pytest.fixture
def spool(
    pg: psycopg.Connection[DictRow], pool: ConnectionPool[psycopg.Connection[DictRow]], clock: FrozenClock
) -> PostgresSpool:
    return PostgresSpool(pool, clock)


class TestPostgresSpool(SpoolContract):
    pass


class TestPostgresSpoolOfAnotherTenant(SpoolContract):
    """The contract as a second tenant, while the default tenant has chunks and snapshots of the same versions
    (ADR-0013 rule 6)."""

    @pytest.fixture
    def spool(
        self,
        pg: psycopg.Connection[DictRow],
        pool: ConnectionPool[psycopg.Connection[DictRow]],
        clock: FrozenClock,
    ) -> PostgresSpool:
        default = PostgresSpool(pool, clock)
        for vid in ("v1", "v2", "v3"):
            default.put_chunk(vid, 0, "data", TABLE2)
            default.put_chunk(vid, 0, "snapshot", SNAP)
        return PostgresSpool(pool, clock, "acme")


def count_chunks(pg: psycopg.Connection[DictRow], version_id: str) -> int:
    row = pg.execute("select count(*) as n from feather.spool_chunk where version_id = %s", (version_id,))
    return row.fetchone()["n"]  # type: ignore[index]


def test_chunk_is_zstd_parquet(spool: PostgresSpool, pg: psycopg.Connection[DictRow]):
    spool.put_chunk("v1", 0, "data", TABLE)
    row = pg.execute("select data from feather.spool_chunk where version_id = 'v1'").fetchone()
    assert row is not None
    metadata = pq.ParquetFile(io.BytesIO(row["data"])).metadata  # type: ignore[reportUnknownMemberType]
    assert metadata.row_group(0).column(0).compression == "ZSTD"


def test_rewrite_across_midnight_stays_on_the_same_day(
    spool: PostgresSpool, clock: FrozenClock, pg: psycopg.Connection[DictRow]
):
    spool.put_chunk("v1", 0, "data", TABLE)
    clock.advance(timedelta(days=1))
    spool.put_chunk("v1", 0, "data", TABLE2)
    assert spool.get_chunk("v1", 0).num_rows == TABLE2.num_rows
    assert count_chunks(pg, "v1") == 1


def test_snapshot_survives_dropping_its_day(spool: PostgresSpool, clock: FrozenClock):  # Review Focus 2
    spool.put_chunk("v1", 0, "snapshot", SNAP)
    spool.put_chunk("v1", 0, "data", TABLE)
    spool.drop_day(clock.now().date())
    assert spool.get_chunk("v1", 0, "snapshot").num_rows == SNAP.num_rows
    assert spool.list_chunks("v1") == []


def test_a_dropped_day_can_be_written_again(spool: PostgresSpool, clock: FrozenClock):
    spool.put_chunk("v1", 0, "data", TABLE)
    spool.drop_day(clock.now().date())
    spool.put_chunk("v1", 0, "data", TABLE2)
    assert spool.get_chunk("v1", 0).num_rows == TABLE2.num_rows


def test_missing_future_partition_is_created_on_write(spool: PostgresSpool, clock: FrozenClock):
    clock.advance(timedelta(days=30))
    spool.put_chunk("v9", 0, "data", TABLE)
    assert spool.get_chunk("v9", 0).num_rows == TABLE.num_rows


def test_ensure_partitions_creates_a_week_ahead(spool: PostgresSpool, pg: psycopg.Connection[DictRow]):
    spool.ensure_partitions()
    row = pg.execute(
        """select count(*) as n from pg_inherits i
           where i.inhparent = 'feather.spool_chunk'::regclass"""
    ).fetchone()
    assert row is not None and row["n"] >= 8


def test_an_empty_day_partition_is_dropped_once_old(spool: PostgresSpool, clock: FrozenClock):  # Stage 2 M6
    spool.ensure_partitions(days_ahead=1)
    today = clock.now().date()
    spool.put_chunk("v-today", 0, "data", TABLE)
    dropped = spool.drop_empty_days(today + timedelta(days=2))
    assert today + timedelta(days=1) in dropped and today not in dropped  # today holds a version
    spool.put_chunk("v-again", 0, "data", TABLE)  # and the day it kept still takes writes
