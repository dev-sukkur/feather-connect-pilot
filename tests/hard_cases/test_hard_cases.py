"""The hard cases of design §16 on the full stack: H1-H4 and H6 (M1's exit criterion), and H5, H7, H8 and H10-H12 and
H15 (M2 plan 2). H14, the webhook burst, is tests/e2e/test_webhook_latency.py::test_burst_at_10x_loses_nothing_and_recovers;
H9's duplicate-free side is tests/chaos/test_kill_mid_apply.py, and its compensation tests/demo/test_zakya_demo.py's
chain to SAP. H13 is tests/hard_cases/test_readiness_cases.py."""

from typing import Any

import pytest

Cases = Any  # conftest.Cases

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(600)]

PACKAGE = "hard-cases"  # as conftest names it
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


# M2 plan 2.


def test_h5_a_back_dated_row_is_captured_by_the_scheduled_reconcile(cases: Cases):
    cases.serve("h5", [{"id": "1", "u": T1, "v": 1}])
    cases.run("h5")
    cases.wait_applied("h5-upsert")
    late = {"id": "2", "u": "2026-01-15T09:00:00+00:00", "v": 2}  # stamped ten months before the watermark
    cases.serve("h5", [{"id": "1", "u": T1, "v": 1}, late])
    cases.wait_until(lambda: ("2",) in cases.rows("select id from hard.h5"), "the reconcile to find row 2")
    runs = cases.stack.store.runs.list_runs(flow=f"{PACKAGE}/h5", limit=100)
    assert any(r.trigger.startswith("reconcile:") and r.status == "succeeded" for r in runs)
    assert cases.rows("select id, v from hard.h5 order by id") == [("1", 1), ("2", 2)]


def _budget(cases: Cases, name: str) -> dict[str, Any]:
    answer = cases.api.get(f"/budgets/{name}")
    assert answer.status_code == 200, answer.text
    return answer.json()


def test_h7_two_flows_share_a_budget_and_the_high_one_keeps_its_reserve(cases: Cases):
    high_ids, low_ids = {f"h7h-{n}" for n in range(3)}, {f"h7l-{n}" for n in range(8)}
    cases.serve("h7high", [{"id": i} for i in sorted(high_ids)])
    cases.serve("h7low", [{"id": i} for i in sorted(low_ids)])
    cases.start("h7-low")
    cases.start("h7-high")
    cases.wait_applied("h7-high")  # the high flow is never held up: half of each window is kept for it
    cases.wait_until(lambda: _budget(cases, "h7")["waiting"], "the low flow to wait for the budget h7")
    waiting = [w["task_ref"] for w in _budget(cases, "h7")["waiting"]]
    assert all("h7-low" in ref for ref in waiting), waiting
    taken = set(cases.erp.received())
    assert high_ids <= taken and len(low_ids & taken) <= 3  # the low flow got no more than its half of 6
    for window in _budget(cases, "h7")["windows"]:
        assert window["used"] <= window["units"]  # the cap is never passed


def test_h8_an_exhausted_budget_defers_the_delivery_visibly_and_it_does_not_fail(cases: Cases):
    cases.serve("h8", [{"id": f"h8-{n}"} for n in range(5)])
    run = cases.start("h8")
    cases.wait_until(lambda: _budget(cases, "h8")["waiting"], "a task to wait for the budget h8")
    [waiting] = _budget(cases, "h8")["waiting"]
    assert waiting["run_id"] == run and waiting["waits"] >= 1  # visible, with the run it belongs to
    assert cases.stack.store.runs.get_run(run).status == "running"  # deferred to the next window, not failed
    assert all(d.status in ("pending", "running") for d in cases.deliveries("h8-erp"))
    assert len({f"h8-{n}" for n in range(5)} & set(cases.erp.received())) == 2  # the two units of the hour


def test_h10_a_quarantined_record_is_superseded_by_a_newer_version(cases: Cases):
    route = f"{PACKAGE}/h10-erp"
    quarantine = cases.stack.store.quarantine
    cases.serve("h10", [{"id": "h10-1", "v": 1, "reject": True}, {"id": "h10-2", "v": 1}])
    cases.run("h10")
    cases.wait_applied("h10-erp")
    assert [q.key for q in quarantine.list_open(route)] == ["h10-1"]
    cases.serve("h10", [{"id": "h10-1", "v": 2, "reject": False}, {"id": "h10-2", "v": 1}])
    cases.run("h10")
    cases.wait_until(
        lambda: len(cases.deliveries("h10-erp")) == 2 and cases.settled("h10-erp"), "the second delivery"
    )
    assert quarantine.list_open(route) == []  # resolved as superseded
    assert cases.erp.received()["h10-1"]["v"] == 2


def test_h11_a_route_added_to_a_live_flow_is_bootstrapped_then_follows_live(cases: Cases):
    rows = [{"id": str(n), "u": f"2026-11-01T10:0{n}:00+00:00"} for n in range(1, 4)]
    cases.serve("h11", rows)
    cases.run("h11")
    cases.wait_applied("h11-a")
    cases.routes_of_h11("a", "b")
    cases.run("h11")  # nothing new: a gets nothing more, b gets the whole history
    cases.wait_applied("h11-a", "h11-b")
    assert cases.rows("select id from hard.h11_b order by id") == [("1",), ("2",), ("3",)]
    assert sum(d.rows_applied for d in cases.deliveries("h11-a")) == 3  # no row twice
    rows.append({"id": "4", "u": "2026-11-01T11:00:00+00:00"})
    cases.serve("h11", rows)
    cases.run("h11")
    cases.wait_applied("h11-a", "h11-b")
    assert [r[0] for r in cases.rows("select id from hard.h11_a order by id")] == ["1", "2", "3", "4"]
    assert [r[0] for r in cases.rows("select id from hard.h11_b order by id")] == ["1", "2", "3", "4"]
    assert sum(d.rows_applied for d in cases.deliveries("h11-b")) == 4


def test_h12_an_added_column_applies_and_a_dropped_one_holds(cases: Cases):
    cases.serve("h12", [{"id": "1", "v": "a"}])
    cases.run("h12")
    cases.wait_applied("h12-upsert")
    cases.serve("h12", [{"id": "1", "v": "a", "w": "x"}])  # a column more: it applies
    cases.run("h12")
    cases.wait_applied("h12-upsert")
    assert cases.rows("select id, v, w from hard.h12") == [("1", "a", "x")]
    cases.serve("h12", [{"id": "1", "w": "y"}])  # v is gone: the delivery holds
    cases.run("h12")

    def held() -> list[Any]:
        return [d for d in cases.deliveries("h12-upsert") if d.status == "held"]

    cases.wait_until(held, "the delivery to hold")
    [hold] = held()
    [shown] = [h for h in cases.api.get("/holds").json()["holds"] if h["version_id"] == hold.version_id]
    assert shown["kind"] == "schema" and shown["reason"].startswith("schema_change"), shown
    assert cases.rows("select id, v, w from hard.h12") == [("1", "a", "x")]  # nothing of it applied


def test_h15_new_code_deployed_mid_drain_loses_and_duplicates_nothing(cases: Cases):
    ids = {f"h15-{n}" for n in range(30)}
    cases.serve("h15", [{"id": i} for i in sorted(ids)])
    run = cases.start("h15")
    cases.wait_until(lambda: len(ids & set(cases.erp.received())) >= 5, "the drain to start")
    assert len(ids & set(cases.erp.received())) < len(ids), "the drain ended before the deploy: slow it down"
    cases.stack.reinstall()  # the next version takes the flows over while the old one drains
    cases.wait_applied("h15-erp")
    store = cases.stack.store
    cases.wait_until(lambda: store.runs.get_run(run).status == "succeeded", "the h15 run")
    assert ids <= set(cases.erp.received())  # nothing lost
    deliveries = cases.deliveries("h15-erp")
    assert sum(d.rows_applied for d in deliveries) == len(ids)  # nothing applied twice
    assert all(d.replays == 0 for d in deliveries)
