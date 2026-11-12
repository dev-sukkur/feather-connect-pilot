"""The readiness SDK (M2 plan 1 task 2): sets and triggers, their specs, prefixing and the project's checks."""

import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import feather_connect as fc
from feather_connect.connectors import memory
from feather_connect.core.readiness import ReadyInfo, readiness_problems
from feather_connect.project import prefixed


def dataset(name, partition=None):
    return fc.Dataset(
        name,
        source=memory.Source(connection=f"src_{name}"),
        key="id",
        strategy=fc.strategy.replace(),
        partition=partition or fc.partition.none(),
    )


STORE_DAY = fc.partition.by_fields("store_id", fc.partition.by_day("date", tz="Asia/Kolkata"))
invoices, returns, payments = (dataset(n, STORE_DAY) for n in ("invoices", "returns", "payments"))
ingest = fc.Flow(
    "ingest",
    reads=[invoices, returns, payments],
    deliver=[fc.to(memory.Sink(connection="staging"))],
    triggers=[fc.every("1h")],
)


def waiting(*triggers, reads=None):
    return fc.Flow(
        "posting",
        reads=reads or dataset("totals"),
        deliver=[fc.to(memory.Sink(connection="erp"))],
        triggers=list(triggers),
    )


def codes(*flows):
    return {p.code for p in readiness_problems(flows)}


def test_a_set_names_its_datasets_and_routes():
    sets = fc.ready([invoices, "returns"], delivered_to="staging", by="store_id", settle="10m")
    assert sets.datasets == ("invoices", "returns")
    assert sets.delivered_to == {"invoices": "staging", "returns": "staging"}
    assert (sets.by, sets.settle, sets.name) == (
        "store_id",
        timedelta(minutes=10),
        "invoices+returns@store_id",
    )
    assert fc.ready(invoices, delivered_to={"invoices": "staging"}).delivered_to == {"invoices": "staging"}


@pytest.mark.parametrize(
    ("call", "says"),
    [
        (lambda: fc.ready([]), "at least one"),
        (lambda: fc.ready([invoices, "invoices"]), "twice"),
        (lambda: fc.ready(invoices, delivered_to={"other": "staging"}), "aren't in the set"),
        (lambda: fc.ready(invoices, quarantine="maybe"), "passes"),  # type: ignore[arg-type]
        (lambda: fc.on_ready(fc.ready(invoices), each="week"), "each is one of"),  # type: ignore[arg-type]
        (lambda: fc.on_ready(fc.ready(invoices), on_late="later"), "on_late"),  # type: ignore[arg-type]
        (lambda: fc.on_ready(fc.ready(invoices), tz="Mars/Base"), "not a time zone"),
        (lambda: fc.on_ready(fc.ready(invoices), at_most_every="0s"), "above zero"),
    ],
)
def test_a_wrong_call_says_what_is_wrong(call, says):
    with pytest.raises(ValueError, match=says):
        call()


def test_the_trigger_and_its_datasets_are_in_the_spec_as_json():
    trigger = fc.on_ready(
        fc.ready([invoices, returns, payments], by="store_id"),
        each="day",
        tz="Asia/Kolkata",
        on_late="warn",
        start=date(2026, 11, 1),
    )
    flow = waiting(trigger)
    spec = json.loads(json.dumps(flow.spec_dict()))
    [found] = spec["triggers"]
    assert found["kind"] == "on_ready" and found["each"] == "day" and found["on_late"] == "warn"
    assert (
        found["ready"]["datasets"] == ["invoices", "returns", "payments"]
        and found["ready"]["by"] == "store_id"
    )
    assert datetime.fromisoformat(found["start"]) == datetime(2026, 11, 1, tzinfo=ZoneInfo("Asia/Kolkata"))
    [totals] = json.loads(json.dumps(ingest.spec_dict()))["datasets"][:1]
    assert totals["partition"] == {
        "kind": "fields",
        "parts": [
            {"kind": "field", "field": "store_id"},
            {"kind": "day", "field": "date", "tz": "Asia/Kolkata"},
        ],
    }
    assert totals["lookback"] is None


def test_prefixing_names_the_own_package_and_keeps_another_packages_names():
    trigger = fc.on_ready(
        fc.ready([invoices, "zakya/items"], delivered_to={"invoices": "staging"}), each="hour", tz="UTC"
    )
    [flow] = prefixed([waiting(trigger)], "acme")
    [found] = flow.triggers
    assert found.ready.datasets == ("acme/invoices", "zakya/items")  # type: ignore[union-attr]
    assert found.ready.delivered_to == {"acme/invoices": "acme/staging"}  # type: ignore[union-attr]


def test_a_sound_set_has_no_problems():
    trigger = fc.on_ready(
        fc.ready([invoices, returns], delivered_to="staging", by="store_id"), each="day", tz="UTC"
    )
    assert readiness_problems([ingest, waiting(trigger)]) == []


def test_each_mistake_is_found_and_says_its_fix():
    run_of_two = fc.on_ready(fc.ready([invoices, returns]), each="run")
    no_tz = fc.on_ready(fc.ready(invoices), each="day")
    unread = fc.on_ready(fc.ready("nobody_reads"), each="run")
    other_package = fc.on_ready(fc.ready("zakya/items"), each="run")  # checked at activation
    wrong_route = fc.on_ready(fc.ready(invoices, delivered_to="warehouse"), each="run")
    wrong_by = fc.on_ready(fc.ready(invoices, by="region"), each="run")
    found = readiness_problems(
        [ingest, waiting(run_of_two, no_tz, unread, other_package, wrong_route, wrong_by)]
    )
    assert {p.code for p in found} == {
        "ready_run_needs_one_dataset",
        "ready_needs_tz",
        "ready_dataset_unread",
        "ready_route_unknown",
        "ready_by_unpartitioned",
    }
    assert all(p.hint and p.path.startswith("posting.triggers[") for p in found)


def test_a_cycle_of_flows_waiting_on_each_other_is_found():
    a_data, b_data = dataset("a_data"), dataset("b_data")
    a = fc.Flow(
        "a",
        reads=a_data,
        deliver=[fc.to(memory.Sink(connection="x"))],
        triggers=[fc.on_ready(fc.ready(b_data))],
    )
    b = fc.Flow(
        "b",
        reads=b_data,
        deliver=[fc.to(memory.Sink(connection="y"))],
        triggers=[fc.on_ready(fc.ready(a_data))],
    )
    [problem] = readiness_problems([a, b])
    assert problem.code == "ready_cycle" and "a -> b -> a" in problem.message


def test_a_planner_that_takes_ready_is_told_what_the_firing_is_for():
    info = ReadyInfo.from_params(
        {
            "ready": {
                "start": "2026-11-08T00:00:00+05:30",
                "end": "2026-11-09T00:00:00+05:30",
                "scope": {"store_id": "1509"},
            }
        }
    )
    assert info is not None and info.scope == {"store_id": "1509"} and info.period_start is not None
    assert info.period_start.date() == date(2026, 11, 8)
    assert ReadyInfo.from_params({}) is None
