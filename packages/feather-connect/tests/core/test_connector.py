import pytest

from feather_connect.core.connector import ConnectorSpec, ReadRequest
from feather_connect.core.errors import PartitionError
from feather_connect.core.partitions import by_day


def test_connector_spec_defaults():
    spec = ConnectorSpec("x")
    assert spec.config is None and spec.strategies == frozenset() and spec.modes == {"upsert"}
    assert spec.deletes == "none" and spec.cost == {}
    assert ConnectorSpec("y").cost is not spec.cost  # each spec has a dict of its own


def test_read_request_for_every_partition_never_asks_the_scheme():
    req = ReadRequest("*", by_day("date", tz="UTC"))
    assert req.contains({"date": "2026-10-18"}) and req.contains({})


def test_read_request_contains_only_records_of_its_partition():
    req = ReadRequest("date=2026-10-18", by_day("date", tz="UTC"))
    assert req.contains({"date": "2026-10-18T23:00:00"}) and not req.contains({"date": "2026-10-19"})
    with pytest.raises(PartitionError):
        req.contains({})
