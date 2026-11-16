# pyright: reportMissingImports=false
from benchmark_accept import dataset_of, delivered, fixture, flow_reading, key_of


def test_return_files_load_into_store_returns():
    flow = flow_reading("returns", "prod")
    returns = dataset_of(flow, "returns")
    assert key_of(returns) == ["return_id"]
    assert (returns.source.spec.name, returns.source.path, returns.source.format) == (
        "s3.files",
        "s3://landing/returns/*.jsonl",
        "jsonl",
    )
    assert [(r.sink.spec.name, r.sink.table) for r in flow.routes] == [("postgres.table", "store_returns")]


def test_every_return_is_delivered_in_dev():
    rows = fixture("returns")
    (sent,) = delivered(flow_reading("returns"), "returns", rows).values()
    assert sorted(r["return_id"] for r in sent) == ["RET-0001", "RET-0002"]
