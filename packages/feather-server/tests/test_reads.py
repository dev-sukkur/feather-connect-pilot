from datetime import timedelta

import pytest

from feather_connect.core.errors import PermanentError
from feather_server.testing import SECRET, World, day


def fail(world: World, route: str, *messages: str) -> None:
    for message in messages:
        world.sink(route).fail_next(PermanentError(message))
    world.run()


def test_failures_group_by_error_class_and_message_per_route(world: World) -> None:
    fail(world, "warehouse", "no such table", "no such table")
    world.sink("gofrugal").fail_next(PermanentError("TimeoutError: sink is slow"))
    world.run([day(16)])
    world.run()  # a later run: the warehouse is behind its earlier failures, and the gofrugal one failed alone
    groups = world.reads.failures_grouped()
    assert [(g.route, g.error_class, g.message, g.count) for g in groups] == [
        ("gofrugal", "TimeoutError", "sink is slow", 1),
        ("warehouse", "PermanentError", "no such table", 2),
    ]
    assert len(groups[1].versions) == 2
    assert [g.route for g in world.reads.failures_grouped(route="warehouse")] == ["warehouse"]


def test_overview_shows_failures_stale_datasets_budgets_and_workers(world: World) -> None:
    fail(world, "warehouse", "no such table")
    world.store.workers.heartbeat("w1", {"site": "hq"}, "sha", 8)
    world.store.quarantine.add("gofrugal", "v1", "k1", "bad value")
    world.state.runtime.pause_route("gofrugal", actor="op")
    overview = world.reads.overview()
    assert overview.failed_deliveries == 1 and overview.deliveries["failed"] == 1
    assert overview.open_quarantine == 1 and overview.paused == ("route:gofrugal",)
    assert [w.worker_id for w in overview.workers] == ["w1"]
    assert [b.name for b in overview.budgets] == ["api"]
    assert overview.budgets[0].windows[0].units == 90 and overview.budgets[0].windows[0].used == 0
    assert overview.stale_datasets == ()  # just read, and the flow runs every 15 minutes
    world.clock.advance(timedelta(minutes=31))
    (stale,) = world.reads.overview().stale_datasets
    assert (stale.dataset, stale.flow, stale.expected_every) == ("orders", "orders", timedelta(minutes=15))
    assert stale.last_sealed_at is not None
    world.state.runtime.pause_flow("orders", actor="op")
    assert world.reads.overview().stale_datasets == ()  # a paused flow is not late


def test_a_dataset_never_read_is_stale_too(world: World) -> None:
    (stale,) = world.reads.overview().stale_datasets
    assert stale.dataset == "orders" and stale.last_sealed_at is None


def test_overview_counts_runs(world: World) -> None:
    fail(world, "warehouse", "no such table")
    world.state.runtime.start_run("orders")  # queued, never run
    overview = world.reads.overview()
    assert overview.failed_runs == 1


def test_partition_grid_cells_show_each_route_status(world: World) -> None:
    fail(world, "warehouse", "no such table")
    grid = world.reads.partition_grid("orders")
    assert grid.routes == ("warehouse", "gofrugal") and grid.next_cursor is None
    assert [r.label for r in grid.rows] == ["18 Oct 2026", "17 Oct 2026"]  # last first
    newest = grid.rows[0]
    assert [(c.route, c.status) for c in newest.cells] == [("warehouse", "applied"), ("gofrugal", "applied")]
    assert newest.records == 2
    oldest = grid.rows[1]  # the first delivery to the warehouse is the one that failed
    assert [(c.route, c.status) for c in oldest.cells] == [("warehouse", "failed"), ("gofrugal", "applied")]
    first = world.reads.partition_grid("orders", limit=1)
    assert [r.partition for r in first.rows] == [day(18)] and first.next_cursor == day(18)
    rest = world.reads.partition_grid("orders", cursor=first.next_cursor)
    assert [r.partition for r in rest.rows] == [day(17)]
    with pytest.raises(KeyError):
        world.reads.partition_grid("nothing")


def test_flow_detail_has_effective_config_with_layers_and_no_secret(world: World) -> None:
    world.run()
    detail = world.reads.flow_detail("orders")
    by_path = {v.path: v for v in detail.config}
    assert by_path["connections.warehouse.options.database"].layer == "feather.yaml"
    assert by_path["flows.orders.triggers.every"].layer == "env/dev.yaml"
    assert by_path["settings.timezone"].layer == "feather.yaml"
    assert by_path["connections.warehouse.auth.token"].value == "***"
    assert SECRET not in repr(detail)
    assert detail.summary.name == "orders" and detail.summary.owner == "ops"
    assert detail.summary.triggers == ("every 0:15:00",)
    assert [r.name for r in detail.summary.routes] == ["warehouse", "gofrugal"]
    assert [r.status for r in detail.recent_runs] == ["succeeded"]
    with pytest.raises(KeyError):
        world.reads.flow_detail("nothing")


def test_flows_list_each_flow_with_its_state(world: World) -> None:
    world.state.runtime.pause_route("gofrugal", actor="op")
    (flow,) = world.reads.flows()
    assert (flow.name, flow.enabled, flow.paused, flow.last_run) == ("orders", True, False, None)
    assert [(r.name, r.paused) for r in flow.routes] == [("warehouse", False), ("gofrugal", True)]
    assert flow.datasets[0].connection == "shop"


def test_run_detail_lists_tasks_versions_and_logs(world: World) -> None:
    run_id = world.run()
    detail = world.reads.run_detail(run_id)
    assert detail.run.status == "succeeded"
    assert {t.kind for t in detail.tasks} == {"plan", "extract", "seal_run", "apply"}
    assert {v.partition for v in detail.versions} == {day(17), day(18)}
    extract = next(t for t in detail.tasks if t.kind == "extract")
    world.store.runs.log(extract.task_id, "INFO", "read 2 rows")
    assert world.reads.task_logs(extract.task_id) == ["INFO: read 2 rows"]
    assert [r.run_id for r in world.reads.runs(flow="orders")] == [run_id]
    assert world.reads.runs(status="failed") == []
    with pytest.raises(KeyError):
        world.reads.run_detail("nothing")


def test_deliveries_and_quarantine_are_listed(world: World) -> None:
    fail(world, "warehouse", "no such table")
    assert [d.partition for d in world.reads.deliveries(route="warehouse", status="failed")] == [day(17)]
    world.store.quarantine.add("gofrugal", "v1", "k1", "bad value")
    assert [q.key for q in world.reads.quarantine()] == ["k1"]
    assert world.reads.quarantine("warehouse") == []


def test_connections_name_their_credentials_but_never_show_them(world: World) -> None:
    found = {c.name: c for c in world.reads.connections()}
    assert set(found) == {"shop", "warehouse", "gofrugal"}
    warehouse = found["warehouse"]
    assert (warehouse.connector, warehouse.budget, warehouse.credentials) == (
        "memory.sink",
        "api",
        ("token",),
    )
    assert warehouse.flows == ("orders",) and found["shop"].site == "hq"
    assert SECRET not in repr(found)
