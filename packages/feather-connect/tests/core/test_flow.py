import json
import logging
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfoNotFoundError

import pytest

import feather_connect as fc
from feather_connect.connectors import memory
from feather_connect.core import partitions, strategies
from feather_connect.core.connector import ConnectorSpec
from feather_connect.core.transforms import apply_map, is_batch_map


def make_flow(strategy=None, **route):
    ds = fc.Dataset(
        "sap.price",
        source=memory.Source(),
        key=("material", "price_list"),
        strategy=strategy or fc.strategy.snapshot_diff(),
    )
    return fc.Flow(
        "prices",
        reads=ds,
        deliver=[fc.to(memory.Sink(connection="zakya"), **route)],
        triggers=[fc.cron("*/15 * * * *", tz="Asia/Kolkata"), fc.manual()],
    )


def codes(flow):
    return {p.code for p in flow.validate()}


def test_route_is_named_after_its_connection_unless_named():
    assert make_flow().routes[0].name == "zakya"
    assert make_flow(name="z2").routes[0].name == "z2"


def test_a_valid_flow_has_no_problems():
    assert make_flow().validate() == []


def test_validate_rejects_replace_partition_on_incomplete_strategy():
    assert "mode_needs_complete" in codes(make_flow(fc.strategy.by_stamp("u"), mode="replace_partition"))


def test_validate_rejects_mode_the_sink_does_not_support():
    assert "mode_unsupported" in codes(make_flow(mode="side_effect"))


def test_validate_rejects_duplicate_route_names():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    f = fc.Flow(
        "f",
        reads=ds,
        deliver=[fc.to(memory.Sink(connection="a")), fc.to(memory.Sink(connection="a"))],
    )
    assert "route_name_duplicate" in codes(f)


def test_on_event_needs_a_push_strategy():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    f = fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())], triggers=[fc.on_event(ds)])
    assert "trigger_needs_push" in codes(f)


def test_unenumerable_partitions_need_a_planner():
    ds = fc.Dataset(
        "d",
        source=memory.Source(),
        key="id",
        strategy=fc.strategy.snapshot_diff(),
        partition=fc.partition.by_fields("store_id", fc.partition.by_day("date", tz="UTC")),
    )
    assert "planner_required" in codes(fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())]))


def test_partitioned_complete_dataset_without_state_warns_about_moves():
    ds = fc.Dataset(
        "d",
        source=memory.Source(),
        key="id",
        strategy=fc.strategy.snapshot_diff(),
        partition=fc.partition.by_day("date", tz="UTC"),
    )
    p = {x.code: x for x in fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())]).validate()}
    assert p["moves_need_state"].severity == "warning"


def test_cron_requires_a_timezone():
    with pytest.raises(TypeError):
        fc.cron("* * * * *")  # pyright: ignore[reportCallIssue]


# Beyond the brief: datasets and routes


def test_a_dataset_has_these_defaults():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    assert ds.partition == fc.partition.none() and ds.keep_state is False and ds.priority == "normal"
    assert ds.schema is None and ds.on_record_error == "quarantine" and ds.allow_empty is False
    assert ds.planner is None


def test_a_dataset_takes_only_its_name_by_position():
    with pytest.raises(TypeError):
        fc.Dataset("d", memory.Source(), "id", fc.strategy.replace())  # pyright: ignore[reportCallIssue]


def test_a_route_defaults_to_upsert_at_normal_priority_without_a_map():
    route = fc.to(memory.Sink(connection="zakya"))
    assert (route.mode, route.priority, route.map) == ("upsert", "normal", None)


# Beyond the brief: flows


def test_a_flow_reads_one_dataset_or_several():
    a = fc.Dataset("a", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    b = fc.Dataset("b", source=memory.Source(), key="id", strategy=fc.strategy.by_stamp("u"))
    assert fc.Flow("f", reads=a, deliver=[fc.to(memory.Sink())]).datasets == [a]
    assert fc.Flow("f", reads=[a, b], deliver=[fc.to(memory.Sink())]).datasets == [a, b]


def test_a_flow_keeps_what_it_was_given_and_defaults_the_rest():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    route = fc.to(memory.Sink())
    plain = fc.Flow("f", reads=ds, deliver=[route])
    assert (plain.name, plain.routes, plain.triggers, plain.owner, plain.tags) == ("f", [route], (), None, ())
    assert (plain.overlap, plain.catch_up) == ("queue", "latest")
    full = fc.Flow(
        "g",
        reads=ds,
        deliver=[route],
        triggers=[fc.manual()],
        owner="retail",
        tags=["prices"],
        overlap="skip",
        catch_up="each",
    )
    assert (full.owner, full.tags, full.overlap, full.catch_up) == ("retail", ("prices",), "skip", "each")
    assert len(full.triggers) == 1


# Beyond the brief: validation


class StampOnlySource(memory.Source):
    spec = ConnectorSpec(name="stamp.only", strategies=frozenset({"by_stamp"}))


def test_validate_rejects_a_strategy_the_source_does_not_declare():
    ds = fc.Dataset("d", source=StampOnlySource(), key="id", strategy=fc.strategy.snapshot_diff())
    f = fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())])
    assert codes(f) == {"strategy_unsupported"}


def test_a_problem_says_where_it_is_and_what_to_do_about_it():
    [problem] = make_flow(mode="side_effect").validate()
    assert (problem.severity, problem.code, problem.path) == ("error", "mode_unsupported", "deliver[0].mode")
    assert "side_effect" in problem.message and "upsert" in problem.hint


def test_validate_reports_every_problem_not_just_the_first():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.by_stamp("u"))
    f = fc.Flow(
        "f",
        reads=ds,
        deliver=[
            fc.to(memory.Sink(connection="a"), mode="replace_partition"),
            fc.to(memory.Sink(connection="a"), mode="side_effect"),
        ],
    )
    problems = f.validate()
    assert {p.code for p in problems} == {"mode_needs_complete", "mode_unsupported", "route_name_duplicate"}
    assert {p.path for p in problems} == {"deliver[0].mode", "deliver[1].mode", "deliver[1].name"}
    assert all(p.severity == "error" and p.message and p.hint for p in problems)


def test_replace_partition_is_checked_against_every_dataset_the_flow_reads():
    a = fc.Dataset("a", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    b = fc.Dataset("b", source=memory.Source(), key="id", strategy=fc.strategy.by_stamp("u"))
    f = fc.Flow("f", reads=[a, b], deliver=[fc.to(memory.Sink(), mode="replace_partition")])
    [problem] = f.validate()
    assert problem.code == "mode_needs_complete" and "'b'" in problem.message


def test_on_event_is_accepted_for_a_push_dataset():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.push())
    f = fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())], triggers=[fc.on_event(ds)])
    assert f.validate() == []


def test_a_planner_and_record_state_clear_the_partition_problems():
    ds = fc.Dataset(
        "d",
        source=memory.Source(),
        key="id",
        strategy=fc.strategy.snapshot_diff(),
        partition=fc.partition.by_fields("store_id", fc.partition.by_day("date", tz="UTC")),
        keep_state=True,
        planner=lambda now: ["store_id=1509/date=2026-10-18"],
    )
    assert fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())]).validate() == []


def test_an_incremental_partitioned_dataset_does_not_warn_about_moves():
    ds = fc.Dataset(
        "d",
        source=memory.Source(),
        key="id",
        strategy=fc.strategy.by_stamp("u"),
        partition=fc.partition.by_day("date", tz="UTC"),
    )
    assert fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())]).validate() == []


# Beyond the brief: triggers


def test_triggers_hold_what_they_were_given():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.push())
    schedule = fc.cron("*/15 * * * *", tz="Asia/Kolkata")
    assert (schedule.expr, schedule.tz) == ("*/15 * * * *", "Asia/Kolkata")
    assert fc.every("30s").interval == timedelta(seconds=30)
    assert fc.on_event(ds).dataset is ds


def test_cron_rejects_a_timezone_that_does_not_exist():
    with pytest.raises(ZoneInfoNotFoundError):
        fc.cron("* * * * *", tz="Mars/Olympus")


@pytest.mark.parametrize("interval", ["0s", "soon"])
def test_every_needs_a_positive_interval(interval):
    with pytest.raises(ValueError):
        fc.every(interval)


# Beyond the brief: maps

CTX = fc.Context(config={}, log=logging.getLogger("test"), now=datetime(2026, 10, 18, tzinfo=UTC), tz="UTC")
REC = fc.Record("k", "upsert", {"a": 1}, 0)


def route_with(map=None):
    return fc.to(memory.Sink(connection="zakya"), map=map)


def test_without_a_map_a_record_goes_through_as_a_copy_of_its_payload():
    out = apply_map(route_with(), REC, CTX)
    assert out == [{"a": 1}] and out[0] is not REC.payload


@pytest.mark.parametrize(
    ("returned", "expected"),
    [
        (None, []),
        ({"x": 1}, [{"x": 1}]),
        ([{"x": 1}, {"x": 2}], [{"x": 1}, {"x": 2}]),
        ([], []),
    ],
)
def test_a_map_can_filter_a_record_out_pass_it_on_or_split_it(returned, expected):
    assert apply_map(route_with(lambda rec, ctx: returned), REC, CTX) == expected


def test_a_map_is_called_with_the_record_and_the_context():
    calls = []
    apply_map(route_with(lambda rec, ctx: calls.append((rec, ctx))), REC, CTX)
    assert len(calls) == 1 and calls[0][0] is REC and calls[0][1] is CTX


def test_a_map_can_return_any_mapping():
    assert apply_map(route_with(lambda rec, ctx: rec), REC, CTX) == [{"a": 1}]


@pytest.mark.parametrize("returned", ["text", 5, ("a",), [1], [{"x": 1}, None]])
def test_a_map_that_returns_anything_else_is_a_permanent_error(returned):
    with pytest.raises(fc.PermanentError, match="'zakya'"):
        apply_map(route_with(lambda rec, ctx: returned), REC, CTX)


def test_batch_map_marks_the_function_and_leaves_it_working():
    @fc.batch_map
    def whole(table, ctx):
        return table

    assert whole("rows", CTX) == "rows"
    assert is_batch_map(whole)
    assert not is_batch_map(lambda rec, ctx: None) and not is_batch_map(None)


def test_apply_map_refuses_a_batch_map():
    @fc.batch_map
    def whole(table, ctx):
        return table

    with pytest.raises(fc.PermanentError, match="batch"):
        apply_map(route_with(whole), REC, CTX)


# Beyond the brief: the public API


def test_the_public_api_has_what_flow_authors_import():
    names = [
        "Dataset",
        "Flow",
        "to",
        "Record",
        "Context",
        "cron",
        "every",
        "manual",
        "on_event",
        "batch_map",
        "strategy",
        "partition",
        "ConnectorSpec",
        "CheckResult",
        "Source",
        "PushSource",
        "Sink",
        "PermanentError",
        "TransientError",
        "RecordRejected",
    ]
    assert all(name in fc.__all__ and hasattr(fc, name) for name in names)
    assert fc.strategy is strategies and fc.partition is partitions


def test_spec_dict_describes_the_flow_as_json():
    flow = make_flow()
    flow.owner = "ann"
    spec = flow.spec_dict()
    assert json.loads(json.dumps(spec)) == spec
    assert (spec["name"], spec["owner"]) == ("prices", "ann")
    assert (
        spec["datasets"][0]["key"] == ["material", "price_list"]
        and spec["datasets"][0]["name"] == "sap.price"
    )
    assert spec["routes"] == [
        {
            "name": "zakya",
            "connection": "zakya",
            "connector": "memory.sink",
            "mode": "upsert",
            "priority": "normal",
        }
    ]
    assert spec["triggers"] == [
        {"kind": "cron", "expr": "*/15 * * * *", "tz": "Asia/Kolkata"},
        {"kind": "manual"},
    ]
