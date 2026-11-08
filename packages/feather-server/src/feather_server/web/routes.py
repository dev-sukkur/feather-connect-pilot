"""The dashboard's pages. Each route asks `Reads` or `Actions` for its data and hands it to a template: nothing here
decides anything the service layer does not.
"""

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from feather_connect.core.errors import PermanentError
from feather_connect.runtime.runtime import FlowPaused
from feather_connect.storage.port import (
    STREAM_TRIGGER,
    DeliveryStatus,
    Run,
    delivery_cursor,
    failure_cursor,
)
from feather_server.api.errors import ApiProblem
from feather_server.api.paging import decode, encode
from feather_server.auth import Actor, CurrentActor, csrf_token, require_csrf
from feather_server.services import actions as action_limits
from feather_server.services.actions import Actions, DeliveryAction, DeliverySelection
from feather_server.services.previews import Preview, PreviewError, PreviewExpired, PreviewInvalid
from feather_server.services.reads import FlowSummary, Reads
from feather_server.services.text import span
from feather_server.services.trace import Tracer
from feather_server.web.charts import meter, sparkline
from feather_server.web.flow_graph import flow_graph

TEMPLATES = Path(__file__).parent / "templates"
NAV: list[tuple[str, str]] = [
    ("Overview", "/"),
    ("Flows", "/flows"),
    ("Runs", "/runs"),
    ("Failures", "/failures"),
    ("Quarantine", "/quarantine"),
    ("Connections", "/connections"),
    ("Trace a record", "/trace"),
]  # (label, path): a page that exists adds itself here
_RECENT_RUNS = 8
_FAILURE_GROUPS = 10

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES)  # autoescape is on for .html, and nothing is marked safe


def ago(moment: datetime | None, now: datetime) -> str:
    """How long before `now` the moment was, in words: `3 min ago`."""
    if moment is None:
        return "never"
    seconds = max(int((now - moment).total_seconds()), 0)
    for size, unit in ((86400, "d"), (3600, "h"), (60, "min")):
        if seconds >= size:
            return f"{seconds // size} {unit} ago"
    return "just now"


def stamp(moment: datetime) -> str:
    return moment.strftime("%d %b %Y %H:%M UTC")


def show(value: Any) -> str:
    """A configuration value as text. A secret is already `***` by the time it gets here."""
    return value if isinstance(value, str) else json.dumps(value, default=str)


def pretty(value: Any) -> str:
    """A value as indented JSON, for a record or a body on a page."""
    return json.dumps(value, indent=2, sort_keys=True, default=str, ensure_ascii=False)


templates.env.filters.update({"ago": ago, "span": span, "stamp": stamp, "show": show, "pretty": pretty})
templates.env.globals.update({"meter": meter, "sparkline": sparkline, "flow_graph": flow_graph})


def reads(request: Request) -> Reads:
    return request.app.state.reads


def actions(request: Request) -> Actions:
    return request.app.state.actions


def render(
    request: Request, template: str, *, fragment: str | None = None, status: int = 200, **context: Any
) -> HTMLResponse:
    """Render a page. A request from HTMX gets the `fragment` template instead, when the page has one, so that a
    refresh brings back the part that changes and not the whole page."""
    state = request.app.state.feather
    wanted = fragment if fragment and request.headers.get("hx-request") == "true" else template

    def label_of(dataset: str, partition: str) -> str:
        try:
            return state.runtime.dataset(dataset).partition.label_of(partition)
        except (KeyError, ValueError):
            return partition

    def url(**changes: str | None) -> str:
        """This page's address with some query parameters set, or removed when None."""
        query = {k: v for k, v in request.query_params.items() if k not in changes}
        query.update({k: v for k, v in changes.items() if v is not None})
        return request.url.path + (f"?{urlencode(query)}" if query else "")

    response = templates.TemplateResponse(
        request,
        wanted,
        {
            "now": state.clock.now(),
            "csrf_token": csrf_token(request),
            "nav": NAV,
            "path": request.url.path,
            "label_of": label_of,
            "url": url,
            "engine": state.settings.engine,
            **context,
        },
        status_code=status,
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Vary"] = "HX-Request"
    return response


# The overview.


@dataclass(frozen=True)
class Attention:
    """A flow that needs a person: its last run failed, or deliveries of its routes did."""

    flow: FlowSummary
    failed_routes: tuple[str, ...]


def _seconds(run: Run) -> float:
    return (run.updated_at - run.created_at).total_seconds()


@router.get("/", response_class=HTMLResponse)
def overview(request: Request, actor: CurrentActor) -> HTMLResponse:
    read = reads(request)
    summary = read.overview()
    flows = read.flows()
    groups = read.failures_grouped(limit=_FAILURE_GROUPS)
    failed_routes: dict[str, set[str]] = {}
    for group in groups:
        for flow in flows:
            if any(r.name == group.route for r in flow.routes):
                failed_routes.setdefault(flow.name, set()).add(group.route)
    attention = [
        Attention(flow, tuple(sorted(failed_routes.get(flow.name, ()))))
        for flow in flows
        if failed_routes.get(flow.name) or (flow.last_run and flow.last_run.status == "failed")
    ]
    runs = read.runs(limit=_RECENT_RUNS)
    # A stream run lasts a day whatever it does, so its span says nothing about how long runs take.
    finished = [
        _seconds(r)
        for r in reversed(runs)
        if r.status in ("succeeded", "failed") and r.trigger != STREAM_TRIGGER
    ]
    alive = [w for w in summary.workers if w.state == "alive"]
    return render(
        request,
        "overview.html",
        fragment="_overview_live.html",
        title="Overview",
        actor=actor,
        overview=summary,
        attention=attention,
        groups=groups,
        runs=runs,
        durations=finished,
        workers_alive=len(alive),
    )


# Flows.

Reason = Annotated[str, Form(max_length=500)]
_NOTICES = {
    "paused": "The flow is paused. Nothing new will start until you resume it.",
    "resumed": "The flow is resumed.",
}


def _reason(text: str) -> str | None:
    return text.strip() or None


def _state_of(flow: FlowSummary) -> str:
    """The one word a flow list filters and badges by."""
    if not flow.enabled:
        return "disabled"
    if flow.paused:
        return "paused"
    return flow.last_run.status if flow.last_run else "never"


def _good_failure_cursor(cursor: str) -> bool:
    try:
        found = json.loads(cursor)
    except ValueError:
        return False
    return isinstance(found, list) and len(found) == 3  # pyright: ignore[reportUnknownArgumentType]


def _good_delivery_cursor(cursor: str) -> bool:
    seq, _, route = cursor.partition(":")
    return seq.isascii() and seq.isdigit() and bool(route)


def _bad_cursor(request: Request) -> HTMLResponse:
    """A cursor that no page of ours made, which a store would choke on (M3)."""
    message = "The link's cursor is not one this page made: start again from the first page."
    return render(request, "error.html", status=422, title="Not a cursor", message=message)


def _not_found(request: Request, what: str) -> HTMLResponse:
    return render(request, "error.html", status=404, title="Not found", message=what)


@router.get("/flows", response_class=HTMLResponse)
def flows_list(
    request: Request, actor: CurrentActor, status: str = "", owner: str = "", tag: str = ""
) -> HTMLResponse:
    everything = reads(request).flows()
    shown = [
        f
        for f in everything
        if (not status or _state_of(f) == status)
        and (not owner or f.owner == owner)
        and (not tag or tag in f.tags)
    ]
    return render(
        request,
        "flows.html",
        fragment="_flows_table.html",
        title="Flows",
        actor=actor,
        flows=shown,
        state_of=_state_of,
        statuses=sorted({_state_of(f) for f in everything} | {"failed", "paused", "never"}),
        owners=sorted({f.owner for f in everything if f.owner}),
        tags=sorted({t for f in everything for t in f.tags}),
        chosen={"status": status, "owner": owner, "tag": tag},
        sha=request.app.state.feather.project.code_sha[:8],
    )


def _flow_page(
    request: Request,
    actor: Actor,
    name: str,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    preview: Preview | None = None,
    form: dict[str, str] | None = None,
) -> HTMLResponse:
    try:
        detail = reads(request).flow_detail(name)
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    state = request.app.state.feather
    started = request.query_params.get("started")
    done = _NOTICES.get(request.query_params.get("done", ""))
    if notice is None and done:
        notice = ("ok", done)
    return render(
        request,
        "flow_detail.html",
        fragment="_flow_runs.html",
        status=status,
        title=name,
        actor=actor,
        detail=detail,
        notice=notice,
        started=started,
        preview=preview,
        form=form or {},
        sha=state.project.code_sha[:8],
        today=state.clock.now().date().isoformat(),
    )


@router.get("/flows/{name}", response_class=HTMLResponse)
def flow_detail(request: Request, actor: CurrentActor, name: str) -> HTMLResponse:
    return _flow_page(request, actor, name)


def _back(name: str, **query: str) -> RedirectResponse:
    suffix = f"?{urlencode(query)}" if query else ""
    return RedirectResponse(f"/flows/{quote(name, safe='')}{suffix}", status_code=303)


@router.post("/flows/{name}/run", dependencies=[Depends(require_csrf)])
def run_flow(request: Request, actor: CurrentActor, name: str, reason: Reason = "") -> Response:
    try:
        run_id = actions(request).run_flow(actor, name, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    except FlowPaused as error:
        return _flow_page(request, actor, name, status=409, notice=("bad", str(error)))
    return _back(name, started=run_id)


@router.post("/flows/{name}/pause", dependencies=[Depends(require_csrf)])
def pause_flow(request: Request, actor: CurrentActor, name: str, reason: Reason = "") -> Response:
    try:
        actions(request).pause_flow(actor, name, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    return _back(name, done="paused")


@router.post("/flows/{name}/resume", dependencies=[Depends(require_csrf)])
def resume_flow(request: Request, actor: CurrentActor, name: str, reason: Reason = "") -> Response:
    try:
        actions(request).resume_flow(actor, name, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    return _back(name, done="resumed")


@router.post("/flows/{name}/backfill/preview", dependencies=[Depends(require_csrf)])
def backfill_preview(
    request: Request,
    actor: CurrentActor,
    name: str,
    start: Annotated[str, Form()] = "",
    end: Annotated[str, Form()] = "",
    max_concurrent: Annotated[str, Form()] = "4",
) -> Response:
    form = {"start": start, "end": end, "max_concurrent": max_concurrent}
    try:
        first, last, slices = date.fromisoformat(start), date.fromisoformat(end), int(max_concurrent)
    except ValueError:
        message = "Give the first and the last day as dates, and a whole number of slices at a time."
        return _flow_page(request, actor, name, status=422, notice=("bad", message), form=form)
    try:
        preview = actions(request).preview_backfill(actor, name, start=first, end=last, max_concurrent=slices)
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    except ValueError as error:
        return _flow_page(request, actor, name, status=422, notice=("bad", str(error)), form=form)
    return _flow_page(request, actor, name, preview=preview, form=form)


@router.post("/flows/{name}/backfill/confirm", dependencies=[Depends(require_csrf)])
def backfill_confirm(
    request: Request, actor: CurrentActor, name: str, token: Annotated[str, Form()], reason: Reason = ""
) -> Response:
    try:
        run_id = actions(request).backfill(actor, token, reason=_reason(reason))
    except PreviewError as error:
        status = (
            410 if isinstance(error, PreviewExpired) else 422 if isinstance(error, PreviewInvalid) else 409
        )
        return _flow_page(request, actor, name, status=status, notice=("bad", str(error)))
    except (KeyError, FlowPaused, ValueError) as error:
        return _flow_page(request, actor, name, status=409, notice=("bad", str(error)))
    return _back(name, started=run_id)


# The partition grid.

_GRID_PAGE = 100


def _preview_status(error: PreviewError) -> int:
    """The status of a refused confirmation: expired is gone, a token that is not ours is unprocessable, and one that
    was used or has gone stale conflicts with how things stand now."""
    if isinstance(error, PreviewExpired):
        return 410
    return 422 if isinstance(error, PreviewInvalid) else 409


def _flow_of(request: Request, dataset: str) -> str:
    """The flow that reads the dataset. Raises `KeyError` for an unknown dataset."""
    for flow in request.app.state.feather.runtime.flows():
        if any(d.name == dataset for d in flow.datasets):
            return flow.name
    raise KeyError(dataset)


def _grid_page(
    request: Request,
    actor: Actor,
    dataset: str,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    preview: Preview | None = None,
    confirm_action: str = "",
    confirm_label: str = "Confirm",
) -> HTMLResponse:
    since_text = request.query_params.get("since", "")
    try:
        since = datetime.combine(date.fromisoformat(since_text), time.min, UTC) if since_text else None
    except ValueError:
        return render(
            request,
            "error.html",
            status=422,
            title="Not a date",
            message="Give 'since' as a date, such as 2026-10-18.",
        )
    try:
        grid = reads(request).partition_grid(
            dataset, since=since, cursor=request.query_params.get("cursor") or None, limit=_GRID_PAGE
        )
        flow = _flow_of(request, dataset)
    except KeyError:
        return _not_found(request, f"No dataset named {dataset!r}.")
    return render(
        request,
        "partitions.html",
        status=status,
        title=f"{dataset} partitions",
        actor=actor,
        grid=grid,
        flow=flow,
        notice=notice,
        preview=preview,
        confirm_action=confirm_action,
        confirm_label=confirm_label,
        since=since_text,
        replayed=request.query_params.get("replayed", ""),
    )


@router.get("/datasets/{dataset}/partitions", response_class=HTMLResponse)
def partitions(request: Request, actor: CurrentActor, dataset: str) -> HTMLResponse:
    return _grid_page(request, actor, dataset)


def _partitions_url(dataset: str, **query: str) -> str:
    suffix = f"?{urlencode(query)}" if query else ""
    return f"/datasets/{quote(dataset, safe='')}/partitions{suffix}"


@router.post("/datasets/{dataset}/partitions/rerun/preview", dependencies=[Depends(require_csrf)])
def rerun_preview(
    request: Request, actor: CurrentActor, dataset: str, partition: Annotated[list[str] | None, Form()] = None
) -> Response:
    try:
        preview = actions(request).preview_rerun(actor, dataset, partition or [])
    except KeyError:
        return _not_found(request, f"No dataset named {dataset!r}.")
    except ValueError as error:
        return _grid_page(request, actor, dataset, status=422, notice=("bad", str(error)))
    action = f"/datasets/{quote(dataset, safe='')}/partitions/rerun/confirm"
    return _grid_page(
        request, actor, dataset, preview=preview, confirm_action=action, confirm_label="Read them again"
    )


@router.post("/datasets/{dataset}/partitions/rerun/confirm", dependencies=[Depends(require_csrf)])
def rerun_confirm(
    request: Request, actor: CurrentActor, dataset: str, token: Annotated[str, Form()], reason: Reason = ""
) -> Response:
    try:
        run_id = actions(request).rerun(actor, token, reason=_reason(reason))
        flow = _flow_of(request, dataset)
    except PreviewError as error:
        return _grid_page(request, actor, dataset, status=_preview_status(error), notice=("bad", str(error)))
    except (KeyError, FlowPaused, ValueError) as error:
        return _grid_page(request, actor, dataset, status=409, notice=("bad", str(error)))
    return _back(flow, started=run_id)


@router.post("/datasets/{dataset}/partitions/{partition:path}/verify", dependencies=[Depends(require_csrf)])
def verify_partition(request: Request, actor: CurrentActor, dataset: str, partition: str) -> Response:
    try:
        found = actions(request).verify_partition(actor, dataset, partition)
    except KeyError:
        return _not_found(request, f"No dataset named {dataset!r}.")
    except (ValueError, PermanentError) as error:
        return _grid_page(request, actor, dataset, status=422, notice=("bad", str(error)))
    label = request.app.state.feather.runtime.dataset(dataset).partition.label_of(partition)
    if not (found.upserts or found.deletes):
        return _grid_page(
            request, actor, dataset, notice=("ok", f"{label} matches its source. Nothing was changed.")
        )
    return _grid_page(
        request,
        actor,
        dataset,
        notice=(
            "warn",
            (
                f"{label} differs from its source: {found.upserts} records would be added or changed and "
                f"{found.deletes} deleted. Nothing was changed."
            ),
        ),
    )


@router.post(
    "/routes/{route}/partitions/{partition:path}/replay/preview", dependencies=[Depends(require_csrf)]
)
def replay_preview(
    request: Request,
    actor: CurrentActor,
    route: str,
    partition: str,
    dataset: Annotated[str, Form()],
) -> Response:
    try:
        preview = actions(request).preview_deliveries(
            actor, "replay", DeliverySelection(route=route, dataset=dataset, partition=partition)
        )
    except KeyError:
        return _not_found(request, f"No dataset named {dataset!r}.")
    except ValueError as error:
        return _grid_page(request, actor, dataset, status=422, notice=("bad", str(error)))
    action = f"/routes/{quote(route, safe='')}/partitions/{quote(partition, safe='/')}/replay/confirm"
    return _grid_page(
        request,
        actor,
        dataset,
        preview=preview,
        confirm_action=action + "?" + urlencode({"dataset": dataset}),
        confirm_label=f"Replay to {route}",
    )


@router.post(
    "/routes/{route}/partitions/{partition:path}/replay/confirm", dependencies=[Depends(require_csrf)]
)
def replay_confirm(
    request: Request,
    actor: CurrentActor,
    route: str,
    partition: str,
    dataset: str,
    token: Annotated[str, Form()],
    reason: Reason = "",
) -> Response:
    try:
        moved = actions(request).apply_deliveries(actor, token, reason=_reason(reason))
    except PreviewError as error:
        return _grid_page(request, actor, dataset, status=_preview_status(error), notice=("bad", str(error)))
    except (KeyError, ValueError) as error:
        return _grid_page(request, actor, dataset, status=409, notice=("bad", str(error)))
    return RedirectResponse(_partitions_url(dataset, replayed=str(len(moved))), status_code=303)


# Runs and tasks.

_RUNS_PAGE = 50
_LOG_PAGE = 200
_ACTIVE = ("queued", "running")
_RUN_NOTICES = {"cancelled": "The run is cancelled."}
_NO_CANCEL = (
    "This is a daily stream run, which can't be cancelled: pause the flow to stop its ticks. "
    "Its deliveries can each be retried or skipped on the failures page."
)
_NO_RERUN = (
    "This is a daily stream run, which has no inputs to run again: its next tick reads from the watermark. "
    "To read something again, re-run its partitions from the partition grid, or run the flow from its page."
)


@router.get("/runs", response_class=HTMLResponse)
def runs_list(
    request: Request,
    actor: CurrentActor,
    flow: str = "",
    status: str = "",
    cursor: str = "",
    limit: Annotated[int, Query(ge=1, le=100)] = _RUNS_PAGE,
) -> HTMLResponse:
    found = reads(request).runs(flow=flow or None, status=status or None, limit=limit, cursor=cursor or None)
    flows = reads(request).flows()
    return render(
        request,
        "runs.html",
        fragment="_runs_table.html",
        title="Runs",
        actor=actor,
        runs=found,
        retries=reads(request).retried_tasks([r.run_id for r in found]),
        flows=sorted(f.name for f in flows),
        statuses=["queued", "running", "succeeded", "failed", "cancelled", "skipped"],
        chosen={"flow": flow, "status": status},
        next_cursor=found[-1].run_id if len(found) == limit else None,
    )


def _run_page(
    request: Request,
    actor: Actor,
    run_id: str,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    confirm_cancel: bool = False,
    form: dict[str, str] | None = None,
) -> HTMLResponse:
    try:
        detail = reads(request).run_detail(run_id, request.query_params.get("cursor") or None)
    except KeyError:
        return _not_found(request, f"No run with the id {run_id!r}.")
    done = _RUN_NOTICES.get(request.query_params.get("done", ""))
    if notice is None and done:
        notice = ("ok", done)
    flow = next((f for f in request.app.state.feather.runtime.flows() if f.name == detail.run.flow), None)
    return render(
        request,
        "run_detail.html",
        fragment="_run_live.html",
        status=status,
        title=f"Run {run_id[:8]}",
        actor=actor,
        detail=detail,
        active=detail.run.status in _ACTIVE,
        live=(detail.run.status in _ACTIVE or (detail.ticks is not None and detail.ticks.day_open))
        and "cursor" not in request.query_params,
        notice=notice,
        confirm_cancel=confirm_cancel,
        form=form or {},
        flow_known=flow is not None,
        first_dataset=flow.datasets[0].name if flow and flow.datasets else None,
    )


@router.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(request: Request, actor: CurrentActor, run_id: str) -> HTMLResponse:
    return _run_page(request, actor, run_id)


@router.get("/tasks/{task_id}/logs", response_class=HTMLResponse)
def task_logs(
    request: Request,
    actor: CurrentActor,
    task_id: str,
    offset: Annotated[int, Query(ge=0)] = 0,
    run: str = "",
) -> HTMLResponse:
    lines = reads(request).task_logs(task_id)
    page = lines[offset : offset + _LOG_PAGE]
    return render(
        request,
        "task_logs.html",
        title="Task log",
        actor=actor,
        task_id=task_id,
        run=run,
        lines=page,
        first=offset + 1,
        total=len(lines),
        next_offset=offset + _LOG_PAGE if offset + _LOG_PAGE < len(lines) else None,
    )


@router.post("/runs/{run_id}/cancel", dependencies=[Depends(require_csrf)])
def cancel_run(
    request: Request,
    actor: CurrentActor,
    run_id: str,
    confirm: Annotated[str, Form()] = "",
    reason: Reason = "",
) -> Response:
    try:
        run = reads(request).run_detail(run_id).run
    except KeyError:
        return _not_found(request, f"No run with the id {run_id!r}.")
    if run.trigger == STREAM_TRIGGER:
        return _run_page(request, actor, run_id, status=409, notice=("bad", _NO_CANCEL))
    if run.status not in _ACTIVE:
        return _run_page(request, actor, run_id, status=409, notice=("bad", "The run is already over."))
    if confirm != "yes":  # the first step only asks
        return _run_page(request, actor, run_id, confirm_cancel=True)
    actions(request).cancel_run(actor, run_id, reason=_reason(reason))
    return RedirectResponse(f"/runs/{quote(run_id, safe='')}?done=cancelled", status_code=303)


@router.post("/runs/{run_id}/rerun", dependencies=[Depends(require_csrf)])
def rerun_run(
    request: Request,
    actor: CurrentActor,
    run_id: str,
    partitions: Annotated[str, Form(max_length=5000)] = "",
    reason: Reason = "",
) -> Response:
    try:
        run = reads(request).run_detail(run_id).run
    except KeyError:
        return _not_found(request, f"No run with the id {run_id!r}.")
    if run.trigger == STREAM_TRIGGER:
        return _run_page(request, actor, run_id, status=409, notice=("bad", _NO_RERUN))
    given = [p for p in re.split(r"[,\s]+", partitions) if p]
    chosen = given or run.params.get("partitions")  # no new inputs: the run's own
    slices = run.params.get("max_concurrent")
    form = {"partitions": partitions}
    try:
        new_id = actions(request).run_flow(
            actor, run.flow, reason=_reason(reason), partitions=chosen, max_concurrent=slices
        )
    except KeyError:
        return _not_found(request, f"No flow named {run.flow!r}.")
    except FlowPaused as error:
        return _run_page(request, actor, run_id, status=409, notice=("bad", str(error)), form=form)
    except ValueError as error:
        return _run_page(request, actor, run_id, status=422, notice=("bad", str(error)), form=form)
    return RedirectResponse(f"/runs/{quote(new_id, safe='')}", status_code=303)


# Failures, deliveries and quarantine.

_FAILURES_PAGE = 25
_DELIVERIES_PAGE = 100
_QUARANTINE_SHOWN = 200
_DELIVERY_STATUSES: tuple[DeliveryStatus, ...] = (
    "pending",
    "running",
    "applied",
    "failed",
    "held",
    "skipped",
    "compensated",
)
_DELIVERY_ACTIONS: tuple[DeliveryAction, ...] = ("retry", "skip", "replay")


def _deliveries_count(count: int) -> str:
    return "1 delivery" if count == 1 else f"{count} deliveries"


def _page_size(request: Request, default: int, most: int) -> int:
    try:
        return min(max(int(request.query_params.get("limit", default)), 1), most)
    except ValueError:
        return default


def _handled(request: Request, notice: tuple[str, str] | None) -> tuple[str, str] | None:
    done = request.query_params.get("done", "")
    if notice is None and done.isdigit():
        return ("ok", f"{_deliveries_count(int(done))} handled. Retries and replays run in the background.")
    return notice


def _reason_required(request: Request, preview: Preview | None) -> bool:
    return bool(preview and preview.action == "skip_deliveries" and actions(request).protected)


def _failures_page(
    request: Request,
    actor: Actor,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    preview: Preview | None = None,
) -> HTMLResponse:
    route = request.query_params.get("route") or None
    limit = _page_size(request, _FAILURES_PAGE, 100)
    cursor = request.query_params.get("cursor") or None
    if cursor is not None and not _good_failure_cursor(cursor):
        return _bad_cursor(request)
    views = reads(request).failure_views(route, cursor, limit)
    return render(
        request,
        "failures.html",
        status=status,
        title="Failures",
        actor=actor,
        views=views,
        route=route or "",
        routes=sorted({r.name for f in request.app.state.feather.runtime.flows() for r in f.routes}),
        next_cursor=failure_cursor(views[-1].group) if len(views) == limit else None,
        notice=_handled(request, notice),
        preview=preview,
        reason_required=_reason_required(request, preview),
    )


@router.get("/failures", response_class=HTMLResponse)
def failures(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _failures_page(request, actor)


def _deliveries_page(
    request: Request,
    actor: Actor,
    route: str,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    preview: Preview | None = None,
) -> HTMLResponse:
    try:
        spec = request.app.state.feather.runtime.route(route)
    except KeyError:
        return _not_found(request, f"No route named {route!r}.")
    chosen = request.query_params.get("status", "")
    if chosen and chosen not in _DELIVERY_STATUSES:
        return render(
            request,
            "error.html",
            status=422,
            title="Not a status",
            message=f"{chosen!r} is not a delivery status: use one of {', '.join(_DELIVERY_STATUSES)}.",
        )
    limit = _page_size(request, _DELIVERIES_PAGE, 500)
    partition = request.query_params.get("partition") or None
    cursor = request.query_params.get("cursor") or None
    if cursor is not None and not _good_delivery_cursor(cursor):
        return _bad_cursor(request)
    found = reads(request).deliveries(
        route, next((s for s in _DELIVERY_STATUSES if s == chosen), None), partition, limit, cursor
    )
    return render(
        request,
        "deliveries.html",
        status=status,
        title=f"{route} deliveries",
        actor=actor,
        route=route,
        destination=spec.sink.connection,
        deliveries=found,
        statuses=_DELIVERY_STATUSES,
        chosen={"status": chosen, "partition": partition or ""},
        next_cursor=delivery_cursor(found[-1]) if len(found) == limit else None,
        notice=_handled(request, notice),
        preview=preview,
        reason_required=_reason_required(request, preview),
    )


@router.get("/routes/{route}/deliveries", response_class=HTMLResponse)
def route_deliveries(request: Request, actor: CurrentActor, route: str) -> HTMLResponse:
    return _deliveries_page(request, actor, route)


@router.get("/routes/{route}/deliveries/{version_id}", response_class=HTMLResponse)
def delivery_detail(request: Request, actor: CurrentActor, route: str, version_id: str) -> HTMLResponse:
    """One delivery: its attempts and what the destination answered for each record (ruling 5)."""
    try:
        detail = reads(request).delivery_detail(route, version_id)
    except KeyError:
        return _not_found(request, f"No delivery of {version_id!r} to {route!r}.")
    return render(
        request,
        "delivery_detail.html",
        title=f"{route} delivery",
        actor=actor,
        detail=detail,
        run=request.query_params.get("run", ""),
    )


# Versions.

_RECORDS_SHOWN = 100
_CELL_CHARS = 60
_HIDDEN_COLUMNS = ("_fc_key", "_fc_op", "_fc_row_hash", "_fc_moved_from")


@dataclass(frozen=True)
class RecordRow:
    """One record of a version, as the page shows it: its key, its change, its cells and its JSON."""

    key: str
    op: str
    cells: tuple[tuple[str, str], ...]  # (text shown, whole text) per column
    json: str


def _cell(value: Any) -> tuple[str, str]:
    whole = "" if value is None else value if isinstance(value, str) else json.dumps(value, default=str)
    return (whole if len(whole) <= _CELL_CHARS else whole[: _CELL_CHARS - 1] + "…"), whole


def _record_rows(records: list[dict[str, Any]], key: tuple[str, ...]) -> tuple[list[str], list[RecordRow]]:
    """The columns, the dataset's key first and then each field in the order it first appears, and the rows."""
    columns = [k for k in key if any(k in r for r in records)]
    for record in records:
        columns.extend(c for c in record if c not in columns and c not in _HIDDEN_COLUMNS)
    rows = [
        RecordRow(
            str(r.get("_fc_key", "")),
            str(r.get("_fc_op", "")),
            tuple(_cell(r.get(c)) for c in columns),
            json.dumps(
                {k: v for k, v in r.items() if k not in _HIDDEN_COLUMNS},
                indent=2,
                default=str,
                ensure_ascii=False,
            ),
        )
        for r in records
    ]
    return columns, rows


@router.get("/versions/{version_id}", response_class=HTMLResponse)
def version_detail(
    request: Request,
    actor: CurrentActor,
    version_id: str,
    cursor: str = "",
    limit: Annotated[int, Query(ge=1, le=_RECORDS_SHOWN)] = _RECORDS_SHOWN,
) -> HTMLResponse:
    """What a version holds: its facts, and its records a page at a time, as read from the spool."""
    try:
        raw = decode(cursor or None)
        version, records, next_raw = reads(request).version_records(version_id, limit, raw)
    except KeyError:
        return _not_found(request, f"No version {version_id!r}.")
    except (ApiProblem, ValueError):
        return _bad_cursor(request)
    try:
        key = request.app.state.feather.runtime.dataset(version.dataset).key
    except KeyError:
        key = ()
    columns, rows = _record_rows(records, (key,) if isinstance(key, str) else tuple(key))
    return render(
        request,
        "version_detail.html",
        title=f"Version of {version.dataset}",
        actor=actor,
        version=version,
        columns=columns,
        rows=rows,
        next_cursor=encode(next_raw),
        first_page=not cursor,
    )


@router.get("/trace", response_class=HTMLResponse)
def record_trace(request: Request, actor: CurrentActor, dataset: str = "", key: str = "") -> HTMLResponse:
    """What happened to one record, by its dataset key, as one timeline; or the form that asks for one."""
    tracer = Tracer(request.app.state.feather)
    datasets = tracer.datasets()
    found = None
    if dataset and key:
        try:
            found = tracer.trace(dataset, key)
        except KeyError:
            return _not_found(request, f"No dataset {dataset!r}.")
    return render(
        request,
        "trace.html",
        title=f"Trace of {key}" if found else "Trace a record",
        actor=actor,
        datasets=datasets,
        dataset=dataset or (datasets[0] if datasets else ""),
        key=key,
        trace=found,
    )


def _delivery_page(
    request: Request,
    actor: Actor,
    origin: str,
    route: str,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    preview: Preview | None = None,
) -> HTMLResponse:
    """The page a delivery action came from: the failures, or the deliveries of one route."""
    if origin == "deliveries" and route:
        return _deliveries_page(request, actor, route, status=status, notice=notice, preview=preview)
    return _failures_page(request, actor, status=status, notice=notice, preview=preview)


Origin = Annotated[str, Form(alias="from")]


@router.post("/deliveries/preview", dependencies=[Depends(require_csrf)])
def deliveries_preview(
    request: Request,
    actor: CurrentActor,
    action: Annotated[str, Form()],
    origin: Origin = "failures",
    route: Annotated[str, Form()] = "",
    dataset: Annotated[str | None, Form()] = None,
    partition: Annotated[str | None, Form()] = None,
    error: Annotated[str | None, Form()] = None,
    error_class: Annotated[str | None, Form()] = None,
    version_id: Annotated[list[str] | None, Form()] = None,
) -> Response:
    def refuse(message: str) -> HTMLResponse:
        return _delivery_page(request, actor, origin, route, status=422, notice=("bad", message))

    chosen: DeliveryAction | None = None
    for known in _DELIVERY_ACTIONS:
        if known == action:
            chosen = known
    if chosen is None:
        return refuse(f"Choose one of: {', '.join(_DELIVERY_ACTIONS)}.")
    if origin == "deliveries" and not version_id:
        return refuse("Tick at least one delivery first.")
    selection = DeliverySelection(
        route or None, dataset, partition, error, error_class, tuple(version_id) if version_id else None
    )
    try:
        preview = actions(request).preview_deliveries(actor, chosen, selection)
    except (KeyError, ValueError) as problem:
        return refuse(str(problem))
    return _delivery_page(request, actor, origin, route, preview=preview)


@router.post("/deliveries/confirm", dependencies=[Depends(require_csrf)])
def deliveries_confirm(
    request: Request,
    actor: CurrentActor,
    token: Annotated[str, Form()],
    origin: Origin = "failures",
    route: Annotated[str, Form()] = "",
    reason: Reason = "",
) -> Response:
    try:
        moved = actions(request).apply_deliveries(actor, token, reason=_reason(reason))
    except PreviewError as error:
        return _delivery_page(
            request, actor, origin, route, status=_preview_status(error), notice=("bad", str(error))
        )
    except (KeyError, ValueError) as error:
        return _delivery_page(request, actor, origin, route, status=422, notice=("bad", str(error)))
    done = urlencode({"done": len(moved)})
    if origin == "deliveries" and route:
        return RedirectResponse(f"/routes/{quote(route, safe='')}/deliveries?{done}", status_code=303)
    return RedirectResponse(f"/failures?{done}", status_code=303)


def _quarantine_page(
    request: Request, actor: Actor, *, status: int = 200, notice: tuple[str, str] | None = None
) -> HTMLResponse:
    route = request.query_params.get("route") or None
    raw_offset = request.query_params.get("offset", "0")
    offset = int(raw_offset) if raw_offset.isascii() and raw_offset.isdigit() else 0
    entries = reads(request).quarantine_entries(route, limit=_QUARANTINE_SHOWN, offset=offset)
    total = reads(request).quarantine_count(route)  # a page at a time, and a count (Stage 2 M7)
    after = offset + len(entries)
    query = {"route": route} if route else {}
    if notice is None and request.query_params.get("dismissed"):
        notice = ("ok", "The entry is dismissed.")
    return render(
        request,
        "quarantine.html",
        status=status,
        title="Quarantine",
        actor=actor,
        entries=entries,
        total=total,
        first=offset + 1,
        last=after,
        next_href=f"/quarantine?{urlencode({**query, 'offset': after})}" if after < total else None,
        previous_href=(
            f"/quarantine?{urlencode({**query, 'offset': max(0, offset - _QUARANTINE_SHOWN)})}"
            if offset
            else None
        ),
        route=route or "",
        notice=notice,
    )


@router.get("/quarantine", response_class=HTMLResponse)
def quarantine(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _quarantine_page(request, actor)


@router.post("/quarantine/dismiss", dependencies=[Depends(require_csrf)])
def quarantine_dismiss(
    request: Request,
    actor: CurrentActor,
    route: Annotated[str, Form()],
    version_id: Annotated[str, Form()],
    key: Annotated[str, Form()],
    reason: Reason = "",
) -> Response:
    try:
        actions(request).dismiss_quarantine(actor, route, version_id, key, reason=_reason(reason))
    except (LookupError, ValueError) as error:  # LookupError: nothing is open for it now
        return _quarantine_page(request, actor, status=409, notice=("bad", str(error)))
    return RedirectResponse("/quarantine?dismissed=1", status_code=303)


# Connections.


@router.get("/connections", response_class=HTMLResponse)
def connections(request: Request, actor: CurrentActor) -> HTMLResponse:
    return render(
        request,
        "connections.html",
        title="Connections",
        actor=actor,
        connections=reads(request).connections(),
        timeout=f"{action_limits.CHECK_TIMEOUT_S:g}",
        notice=None,
    )


@router.post("/connections/{name}/check", dependencies=[Depends(require_csrf)])
def check_connection(request: Request, actor: CurrentActor, name: str) -> Response:
    try:
        actions(request).check_connection(actor, name)
    except KeyError:
        return _not_found(request, f"No connection named {name!r}.")
    return RedirectResponse(f"/connections#connection-{quote(name, safe='')}", status_code=303)
