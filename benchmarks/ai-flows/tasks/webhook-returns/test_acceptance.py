# pyright: reportMissingImports=false
from benchmark_accept import dataset_of, fixture, flow_reading, key_of, mapped


def test_return_events_are_put_to_the_erp():
    flow = flow_reading("returns", "prod")
    returns = dataset_of(flow, "returns")
    assert key_of(returns) == ["return_id"]
    assert returns.strategy.name == "push"
    assert returns.source.connection == "partner"
    (route,) = flow.routes
    assert (route.sink.spec.name, route.sink.connection, route.sink.method, route.sink.url) == (
        "http.call",
        "erp",
        "PUT",
        "/returns/{return_id}",
    )


def test_each_return_is_sent_in_the_erp_s_shape():
    (route,) = flow_reading("returns", "prod").routes
    sent = mapped(route, fixture("returns"), ["return_id"])
    assert sent == [
        {"external_id": "RET-0001", "order": "ORD-00001", "reason": "damaged", "amount": 145.0},
        {"external_id": "RET-0002", "order": "ORD-00002", "reason": "wrong item", "amount": 520.5},
    ]
