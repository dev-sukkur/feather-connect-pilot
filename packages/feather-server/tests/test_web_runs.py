import re
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.storage.port import ResponseRow
from feather_server.testing import World, day


def test_runs_list_filters_by_flow_and_status(client: TestClient, world: World) -> None:
    good = world.run([day(18)])
    world.sink("warehouse").fail_next(PermanentError("no such table"))
    bad = world.run([day(17)])
    html = client.get("/runs").text
    assert f'href="/runs/{good}"' in html and f'href="/runs/{bad}"' in html
    assert "Succeeded" in html and "Failed" in html
    failed = client.get("/runs?status=failed").text
    assert f'href="/runs/{bad}"' in failed and f'href="/runs/{good}"' not in failed
    assert f'href="/runs/{good}"' in client.get("/runs?flow=orders&status=succeeded").text
    assert "No runs match" in client.get("/runs?flow=nothing").text
    assert '<form class="filters" method="get" action="/runs">' in html  # works without JavaScript
    assert 'href="/runs"' in html and "Runs" in html.split("<main")[0]  # in the navigation


def test_the_runs_list_is_paged_by_cursor(client: TestClient, world: World) -> None:
    ids = [world.state.runtime.start_run("orders") for _ in range(3)]
    world.engine.run_until_idle()
    page = client.get("/runs?limit=2").text
    assert "Next page" in page and ids[2] in page
    cursor = re.search(r'rel="next" href="([^"]+)"', page)
    assert cursor
    later = client.get(cursor.group(1).replace("&amp;", "&")).text
    assert ids[0] in later and ids[2] not in later


def test_skipped_runs_show_their_reason(client: TestClient, world: World) -> None:  # FR-E4
    skipped = world.store.runs.create_run(
        "orders", "every", "system", {}, status="skipped", reason="flow paused by op: bad data upstream"
    )
    listed = client.get("/runs").text
    assert "Skipped" in listed and "flow paused by op: bad data upstream" in listed
    detail = client.get(f"/runs/{skipped}").text
    assert "Skipped" in detail and "flow paused by op: bad data upstream" in detail


def test_detail_has_timeline_inputs_attempts_and_sha(client: TestClient, world: World) -> None:
    world.sink("warehouse").fail_next(PermanentError("no such table"))
    run_id = world.state.runtime.start_run("orders", partitions=[day(17)], actor="op")
    world.engine.run_until_idle()
    html = client.get(f"/runs/{run_id}").text
    assert "Timeline" in html and "extract" in html and "apply" in html  # one row per task
    assert "17 Oct 2026" in html and "manual · op" in html  # what it was asked to do
    assert "Attempts" in html and "no such table" in html  # deliveries with their attempts
    assert "<code>sha</code>" in html  # the code each task ran
    assert 'href="/routes/warehouse/deliveries?partition=date%3D2026-10-17"' in html
    assert 'href="/flows/orders"' in html
    assert client.get("/runs/nothing").status_code == 404


def _make_source_fail(world: World, times: int) -> None:
    """Make the orders source raise a transient error on its first `times` reads."""
    source: Any = world.state.runtime.flow("orders").datasets[0].source
    read, left = source.read, [times]

    def flaky(ctx: Any, req: Any) -> Any:
        if left[0]:
            left[0] -= 1
            raise TransientError(f"shop timed out ({times - left[0]})")
        return read(ctx, req)

    source.read = flaky


def test_a_task_that_retried_says_so_in_the_run_and_in_the_list(client: TestClient, world: World) -> None:
    _make_source_fail(world, 2)
    run_id = world.run([day(17)])
    detail = client.get(f"/runs/{run_id}").text
    assert "succeeded after 3 attempts" in detail
    assert "shop timed out (2)" in detail  # the latest error stays after the success
    assert "Attempt" in detail
    listed = client.get("/runs").text
    assert "1 task retried" in listed


def test_a_run_without_retries_has_nothing_about_attempts(client: TestClient, world: World) -> None:
    run_id = world.run([day(17)])
    assert "after 2 attempts" not in client.get(f"/runs/{run_id}").text
    assert "retried" not in client.get("/runs").text


def test_logs_are_shown_escaped_and_paged(client: TestClient, world: World) -> None:  # Review Focus 5
    run_id = world.run([day(18)])
    task = world.store.runs.list_tasks(run_id)[0]
    world.store.runs.log(task.task_id, "ERROR", "<script>alert(1)</script> & <b>x</b>")
    for n in range(250):
        world.store.runs.log(task.task_id, "INFO", f"line {n}")
    first = client.get(f"/tasks/{task.task_id}/logs").text
    assert "<script>alert" not in first and "&lt;script&gt;alert(1)&lt;/script&gt; &amp;" in first
    assert "line 198" in first and "line 199" not in first and "Next page" in first
    second = client.get(f"/tasks/{task.task_id}/logs?offset=200").text
    assert "line 249" in second and "line 198" not in second and "Next page" not in second
    assert f'href="/tasks/{task.task_id}/logs' in client.get(f"/runs/{run_id}").text
    assert client.get(f"/tasks/{task.task_id}/logs?offset=-1").status_code == 422
    assert "No log lines" in client.get("/tasks/none/logs").text


def test_active_run_refreshes_through_htmx(client: TestClient, world: World) -> None:
    run_id = world.state.runtime.start_run("orders")  # nothing has run yet
    page = client.get(f"/runs/{run_id}").text
    assert f'hx-get="/runs/{run_id}"' in page and 'hx-trigger="every 3s"' in page
    fragment = client.get(f"/runs/{run_id}", headers={"HX-Request": "true"}).text
    assert "<html" not in fragment and "Timeline" in fragment
    world.engine.run_until_idle()
    done = client.get(f"/runs/{run_id}", headers={"HX-Request": "true"}).text
    assert "Succeeded" in done and "hx-trigger" not in done  # a finished run stops polling
    assert "hx-trigger" not in client.get(f"/runs/{run_id}").text
    assert 'hx-trigger="every 5s"' in client.get("/runs").text
    assert "<html" not in client.get("/runs", headers={"HX-Request": "true"}).text


def test_cancel_asks_for_confirmation_and_records_the_reason(
    client: TestClient, world: World, csrf: Callable[[], str]
) -> None:
    run_id = world.state.runtime.start_run("orders")
    asked = client.post(f"/runs/{run_id}/cancel", data={"csrf_token": csrf()})
    assert asked.status_code == 200 and "Cancel this run?" in asked.text
    assert world.store.runs.get_run(run_id).status != "cancelled"  # asking changed nothing
    assert client.post(f"/runs/{run_id}/cancel", data={"confirm": "yes"}).status_code == 403
    done = client.post(
        f"/runs/{run_id}/cancel", data={"csrf_token": csrf(), "confirm": "yes", "reason": "wrong dates"}
    )
    assert done.status_code == 303 and done.headers["location"].startswith(f"/runs/{run_id}")
    run = world.store.runs.get_run(run_id)
    assert run.status == "cancelled" and run.reason and "wrong dates" in run.reason
    (row,) = [a for a in world.store.audit.list_audit(10) if a.action == "cancel_run"]
    assert (row.actor, row.request["reason"]) == ("op", "wrong dates")
    assert "Cancelled" in client.get(done.headers["location"]).text
    again = client.post(f"/runs/{run_id}/cancel", data={"csrf_token": csrf(), "confirm": "yes"})
    assert again.status_code == 409 and "already over" in again.text


def test_rerun_with_new_inputs_starts_a_new_run(
    client: TestClient, world: World, csrf: Callable[[], str]
) -> None:
    run_id = world.run([day(17)])
    form = {"csrf_token": csrf(), "partitions": f"{day(15)}, {day(16)}", "reason": "late rows"}
    response = client.post(f"/runs/{run_id}/rerun", data=form)
    assert response.status_code == 303
    new_id = response.headers["location"].split("?")[0].removeprefix("/runs/")
    assert new_id != run_id
    new = world.store.runs.get_run(new_id)
    assert (
        new.flow == "orders" and new.params["partitions"] == [day(15), day(16)] and new.reason == "late rows"
    )
    same = client.post(f"/runs/{run_id}/rerun", data={"csrf_token": csrf()})  # no new inputs: the same ones
    assert world.store.runs.get_run(same.headers["location"].split("?")[0].removeprefix("/runs/")).params == {
        "partitions": [day(17)]
    }
    bad = client.post(f"/runs/{run_id}/rerun", data={"csrf_token": csrf(), "partitions": "tomorrow"})
    assert bad.status_code == 422 and "tomorrow" in bad.text
    world.state.runtime.pause_flow("orders", actor="op")
    paused = client.post(f"/runs/{run_id}/rerun", data={"csrf_token": csrf()})
    assert paused.status_code == 409 and "paused" in paused.text
    assert client.post(f"/runs/{run_id}/rerun", data={}).status_code == 403


def _stream_run(world: World, tasks: int = 0) -> str:
    runs = world.store.runs
    run_id = world.state.runtime.stream_run("orders", world.clock.now())
    for n in range(tasks):
        runs.finish_task(runs.start_task(run_id, "plan", f"tick:{run_id}:{n}:plan"), "succeeded")
    return run_id


def test_a_stream_runs_page_shows_a_page_of_tasks_newest_first_and_counts_them_all(
    client: TestClient, world: World
) -> None:
    run_id = _stream_run(world, 250)
    runs = world.store.runs
    runs.finish_task(runs.start_task(run_id, "skip", f"tick:{run_id}:s:skip"), "skipped", "overlap")
    runs.finish_task(runs.start_task(run_id, "plan", f"tick:{run_id}:f:plan"), "failed", "no")
    detail = world.reads.run_detail(run_id)
    assert len(detail.tasks) == 100 and detail.tasks[0].kind == "plan" and detail.tasks[1].kind == "skip"
    assert detail.task_total == 252 and detail.tasks_failed == 1
    assert detail.ticks is not None
    assert (detail.ticks.total, detail.ticks.skipped, detail.ticks.failed) == (252, 1, 1)
    page = client.get(f"/runs/{run_id}").text
    assert "Ticks: 252" in page and "1 skipped" in page and "Next page" in page
    assert page.count("/logs?run=") == 100
    second = world.reads.run_detail(run_id, detail.next_task)
    assert len(second.tasks) == 100 and not {t.task_id for t in second.tasks} & {
        t.task_id for t in detail.tasks
    }
    assert client.get(f"/runs/{run_id}?cursor={detail.next_task}").text.count("/logs?run=") == 100


def test_a_skipped_tick_shows_its_reason(client: TestClient, world: World) -> None:
    world.state.runtime.skip_stream_tick(
        "orders", trigger_id="every:30s", at=world.clock.now(), reason="paused"
    )
    page = client.get(f"/runs/{_stream_run(world)}").text
    assert "Skipped: paused" in page


def test_a_stream_run_says_what_can_be_done_and_refuses_cancel_and_rerun(
    client: TestClient, world: World, csrf: Callable[[], str]
) -> None:
    run_id = _stream_run(world)
    world.store.runs.set_run_status(run_id, "running")
    page = client.get(f"/runs/{run_id}").text
    assert "/cancel" not in page and "/rerun" not in page and "pause the flow" in page
    assert 'hx-trigger="every 3s"' in page
    token = csrf()
    cancelled = client.post(f"/runs/{run_id}/cancel", data={"csrf_token": token, "confirm": "yes"})
    assert cancelled.status_code == 409 and "pause the flow" in cancelled.text
    assert world.store.runs.get_run(run_id).status == "running"
    before = len(world.store.runs.list_runs())
    rerun = client.post(f"/runs/{run_id}/rerun", data={"csrf_token": token})
    assert rerun.status_code == 409 and "no inputs to run again" in rerun.text
    assert len(world.store.runs.list_runs()) == before


def test_a_stream_run_lists_its_day_and_no_duration(client: TestClient, world: World) -> None:
    run_id = _stream_run(world)
    day = world.store.runs.get_run(run_id).params["day"]
    assert f"stream · system · {day}" in client.get("/runs").text


def test_a_stream_run_of_a_past_day_is_closed_and_stops_refreshing(client: TestClient, world: World) -> None:
    run_id = _stream_run(world)
    world.clock.advance(timedelta(days=1))
    page = client.get(f"/runs/{run_id}").text
    assert "closed" in page and 'hx-trigger="every 3s"' not in page


def test_a_delivery_shows_what_the_destination_answered_per_record(
    client: TestClient, world: World
) -> None:  # ruling 5
    run_id = world.run([day(17)])
    delivery = world.reads.run_detail(run_id).deliveries[0]
    route, version_id = delivery.route, delivery.version_id
    world.store.responses.record(
        [
            ResponseRow(route, version_id, 0, "o-1", 201, {"erp_id": "<e-1>"}),
            ResponseRow(route, version_id, 0, "o-2", 422, {}, sent={"customer": "c-9"}),
        ]
    )
    link = f"/routes/{route}/deliveries/{version_id}?run={run_id}"
    assert link in client.get(f"/runs/{run_id}").text
    html = client.get(link).text
    assert "What the destination answered" in html and "<code>o-1</code>" in html
    assert "<code>erp_id</code>: &lt;e-1&gt;" in html  # escaped
    assert "422" in html and "<code>customer</code>: c-9" in html  # a rejected record keeps what it was sent
    assert "Attempts" in html and f'href="/runs/{run_id}"' in html
    assert client.get(f"/routes/{route}/deliveries/nothing").status_code == 404


def test_a_run_page_says_how_many_versions_it_shows_and_lists_failed_deliveries_first(
    client: TestClient, world: World, monkeypatch: pytest.MonkeyPatch
) -> None:  # S15
    from feather_connect.core.errors import PermanentError
    from feather_server.services import reads

    monkeypatch.setattr(reads, "_VERSIONS_SHOWN", 2)
    world.sink("warehouse").fail_next(PermanentError("table is locked"))
    run_id = world.run([day(14), day(15), day(16), day(17)])
    failed = world.store.ledger.list_deliveries(route="warehouse", status="failed")
    assert [d.partition for d in failed] == [day(14)]  # the oldest, not among the newest 2
    page = client.get(f"/runs/{run_id}").text
    assert "newest 2 of 4 versions" in page
    assert "table is locked" in page
    assert page.index("table is locked") < page.index("Applied")
