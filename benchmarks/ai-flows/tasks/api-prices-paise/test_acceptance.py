# pyright: reportMissingImports=false
from benchmark_accept import delivered, fixture, flow_reading


def test_each_price_carries_its_paise():
    rows = fixture("prices")
    (sent,) = delivered(flow_reading("prices"), "prices", rows).values()
    got = {r["price_id"]: (r["price"], r["price_paise"]) for r in sent}
    assert got == {
        "PR-0001": ("145.00", 14500),
        "PR-0002": ("149.5", 14950),
        "PR-0003": ("0.105", 11),
        "PR-0004": ("145.505", 14551),
    }
    assert all(type(r["price_paise"]) is int for r in sent)
