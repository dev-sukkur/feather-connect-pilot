import io
from datetime import timedelta

import pyarrow.parquet as pq
import pytest

from feather_connect.clock import FrozenClock, SystemClock
from feather_connect.core.records import Record
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import Store
from feather_connect.storage.spool import MemorySpool, Spool, records_to_table, table_to_records
from feather_connect.testing.store_contract import T0, StoreContract


class TestMemoryStore(StoreContract):
    @pytest.fixture
    def clock(self) -> FrozenClock:
        return FrozenClock(T0)

    @pytest.fixture
    def store(self, clock: FrozenClock) -> Store:
        return MemoryStore(clock)

    @pytest.fixture
    def spool(self) -> Spool:
        return MemorySpool()


def test_chunks_with_empty_objects_survive_a_parquet_write_with_zstd():
    recs = [
        Record("a", "upsert", {"o": {}, "n": {"x": {}}, "l": [{}]}, 1),
        Record("b", "upsert", {"o": {}, "n": {"x": {}}, "l": []}, 2),
    ]
    sink = io.BytesIO()
    pq.write_table(records_to_table(recs), sink, compression="zstd")  # type: ignore[reportUnknownMemberType]
    sink.seek(0)
    assert table_to_records(pq.read_table(sink)) == recs  # type: ignore[reportUnknownMemberType]


def test_the_default_clock_is_the_system_clock():
    store = MemoryStore()
    run = store.runs.get_run(store.runs.create_run("f", "manual", "ann", {}))
    assert abs(run.created_at - SystemClock().now()) < timedelta(minutes=1)
