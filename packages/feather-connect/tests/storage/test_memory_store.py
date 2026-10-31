from datetime import timedelta

import pytest

from feather_connect.clock import FrozenClock, SystemClock
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import Store
from feather_connect.storage.spool import MemorySpool, Spool
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


def test_the_default_clock_is_the_system_clock():
    store = MemoryStore()
    run = store.runs.get_run(store.runs.create_run("f", "manual", "ann", {}))
    assert abs(run.created_at - SystemClock().now()) < timedelta(minutes=1)
