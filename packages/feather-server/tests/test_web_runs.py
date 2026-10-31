import re
from collections.abc import Callable

from fastapi.testclient import TestClient

from feather_connect.core.errors import PermanentError
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
