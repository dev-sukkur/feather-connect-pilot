"""H1-H4 and H6 of design §16 on the full stack (M1 exit criterion). H14, the webhook burst, is
tests/e2e/test_webhook_latency.py::test_burst_at_10x_loses_nothing_and_recovers."""

from typing import Any

import pytest

Cases = Any  # conftest.Cases

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(600)]

D1, D2 = "2026-11-01", "2026-11-02"
T0, T1 = "2026-11-01T10:00:00+00:00", "2026-11-01T10:30:00+00:00"


def test_h1_record_moves_partitions_leaves_no_stale_copy(cases: Cases):
    days = [f"day={D1}", f"day={D2}"]
    cases.serve(
        "h1", [{"id": "a", "day": D1, "v": 1}, {"id": "b", "day": D1, "v": 1}, {"id": "c", "day": D2, "v": 1}]
    )
    cases.run("h1", days)
    cases.wait_applied("h1-upsert", "h1-replace")
    cases.serve(
        "h1", [{"id": "a", "day": D2, "v": 2}, {"id": "b", "day": D1, "v": 1}, {"id": "c", "day": D2, "v": 1}]
    )
    cases.run("h1", days)
    cases.wait_applied("h1-upsert", "h1-replace")
    want = [("a", D2, 2), ("b", D1, 1), ("c", D2, 1)]
    assert cases.rows("select id, day, v from hard.h1_upsert order by id") == want
    assert cases.rows("select id, day, v from hard.h1_replace order by id") == want


def test_h2_snapshot_diff_on_a_stampless_table(cases: Cases):
    cases.serve("h2", [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}, {"id": "3", "v": "c"}])
    cases.run("h2")
    cases.wait_applied("h2-upsert")
    cases.serve("h2", [{"id": "1", "v": "a"}, {"id": "2", "v": "B"}, {"id": "4", "v": "d"}])
    cases.run("h2")
    cases.wait_applied("h2-upsert")
    assert cases.rows("select id, v from hard.h2 order by id") == [("1", "a"), ("2", "B"), ("4", "d")]


def test_h3_source_rewrites_a_key_in_place(cases: Cases):
    cases.serve("h3", [{"row_id": 10, "sku": "A", "p": 1}, {"row_id": 11, "sku": "B", "p": 2}])
    cases.run("h3")
    cases.wait_applied("h3-upsert")
    cases.serve("h3", [{"row_id": 99, "sku": "A", "p": 1}, {"row_id": 11, "sku": "B", "p": 2}])
    cases.run("h3")
    cases.wait_applied("h3-upsert")
    assert cases.rows("select sku, row_id, p from hard.h3 order by sku") == [("A", 99, 1), ("B", 11, 2)]


def test_h4_late_rows_inside_the_lookback_captured_once(cases: Cases):
    first = [{"id": "1", "u": T0, "v": 1}, {"id": "2", "u": T0, "v": 1}]
    cases.serve("h4", first)
    cases.run("h4")
    cases.wait_applied("h4-append")
    cases.serve(
        "h4", [*first, {"id": "3", "u": T1, "v": 1}]
    )  # the first two again, inside the hour's lookback
    cases.run("h4")
    cases.wait_applied("h4-append")
    assert cases.rows("select id, count(*) from hard.h4 group by id order by id") == [
        ("1", 1),
        ("2", 1),
        ("3", 1),
    ]


def test_h6_destination_down_during_the_read_window(cases: Cases):
    rows = [{"id": str(n), "v": f"v{n}"} for n in range(5)]
    cases.serve("h6", rows)
    cases.erp.go_down()
    try:
        cases.start("h6")  # the run ends only once the ERP's delivery is done, so it is not waited for
        cases.wait_applied("h6-warehouse")
        assert len(cases.rows("select id from hard.h6")) == len(
            rows
        )  # the warehouse did not wait for the ERP
        assert cases.erp.received() == {} and not cases.settled("h6-erp")
    finally:
        cases.erp.come_up()
    cases.wait_until(lambda: cases.settled("h6-erp"), "the ERP to catch up on its own retries", 420)
    assert set(cases.erp.received()) == {row["id"] for row in rows}
