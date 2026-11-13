"""The dashboard's pages. Each route asks `Reads` or `Actions` for its data and hands it to a template: nothing here
decides anything the service layer does not.
"""

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote, urlencode, urlsplit

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from feather_connect.core.errors import PermanentError
from feather_connect.runtime.runtime import FlowPaused
from feather_connect.storage.port import (
    GRANTS,
    ROLES,
    STREAM_TRIGGER,
    DeliveryStatus,
    DuplicateUser,
    InvalidTransition,
    Run,
    delivery_cursor,
    failure_cursor,
)
from feather_connect.vault import KeyConflict, VaultError
from feather_server.api.errors import ApiProblem
from feather_server.api.paging import decode, encode
from feather_server.auth import Actor, CurrentActor, csrf_token, current_actor, require_csrf
from feather_server.catalog_view import dataset_of, route_of
from feather_server.services import actions as action_limits
from feather_server.services import alerts, diff, pii
from feather_server.services.access import Capability, Forbidden, ReasonRequired, can
from feather_server.services.accounts import Accounts
from feather_server.services.actions import Actions, DeliveryAction, DeliverySelection, Requested, SamePerson
from feather_server.services.environment import Environment, coerce_setting
from feather_server.services.previews import Preview, PreviewError, PreviewExpired, PreviewInvalid
from feather_server.services.reads import FlowSummary, Reads
from feather_server.services.task_records import NoRecords, TaskRecordReader
from feather_server.services.text import span
from feather_server.services.trace import Tracer
from feather_server.web.charts import meter, sparkline
from feather_server.web.flow_graph import flow_graph
from feather_server.web.icons import icon

TEMPLATES = Path(__file__).parent / "templates"


@dataclass(frozen=True)
class NavItem:
    """A page of the sidebar: its label, address and icon, the addresses under it that mark it current, and the
    capability it needs to be shown (None: everyone's)."""

    label: str
    path: str
    icon: str
    under: tuple[str, ...] = ()
    needs: Capability | None = None

    def current(self, path: str) -> bool:
        if self.path == "/":
            return path == "/"
        return any(path == p or path.startswith(p + "/") for p in (self.path, *self.under))


NAV: list[NavItem] = [
    NavItem("Overview", "/", "overview"),
    NavItem("Flows", "/flows", "flows"),
    NavItem("Runs", "/runs", "runs", ("/tasks",)),
    NavItem("Datasets", "/datasets", "datasets", ("/versions",)),
    NavItem("Readiness", "/readiness", "readiness"),
    NavItem("Deliveries", "/deliveries", "deliveries", ("/routes",)),
    NavItem("Holds", "/holds", "holds"),
    NavItem("Failures", "/failures", "failures"),
    NavItem("Quarantine", "/quarantine", "quarantine"),
    NavItem("Connections", "/connections", "connections"),
    NavItem("Budgets", "/budgets", "budgets"),
    NavItem("Secrets", "/secrets", "secrets"),
    NavItem("Users", "/users", "users", needs="users.manage"),
    NavItem("Audit", "/audit", "audit", needs="audit.read"),
    NavItem("Alerts", "/alerts", "alerts"),
    NavItem("Packages", "/packages", "packages"),
    NavItem("Registries", "/registries", "registries"),
    NavItem("Trace a record", "/trace", "trace"),
]  # a page that exists adds itself here


def _version() -> str:
    try:
        return package_version("feather-server")
    except PackageNotFoundError:  # run from a source tree that was never installed
        return "dev"


VERSION = _version()
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


_STATUSES: dict[str, tuple[str, str, str]] = {  # status: (icon, word, tone)
    "succeeded": ("✓", "Succeeded", "ok"),
    "applied": ("✓", "Applied", "ok"),
    "alive": ("●", "Alive", "ok"),
    "failed": ("✕", "Failed", "bad"),
    "dead": ("✕", "Dead", "bad"),
    "rejected": ("✕", "Rejected", "bad"),
    "running": ("▶", "Running", "info"),
    "claimed": ("▶", "Applying", "info"),
    "draining": ("◐", "Draining", "warn"),
    "queued": ("…", "Queued", "idle"),
    "pending": ("…", "Pending", "idle"),
    "held": ("⏸", "Held", "warn"),
    "paused": ("⏸", "Paused", "warn"),
    "open": ("!", "Open", "warn"),
    "skipped": ("⤼", "Skipped", "idle"),
    "cancelled": ("⊘", "Cancelled", "idle"),
    "superseded": ("⤼", "Superseded", "idle"),
    "sealed": ("✓", "Sealed", "ok"),
    "staged": ("…", "Staged", "idle"),
    "never": ("○", "Never run", "idle"),
    "disabled": ("⊘", "Disabled", "idle"),
    "active": ("●", "Active", "ok"),
    # flow packages: versions, flows, environments and hosts
    "installed": ("…", "Installed", "idle"),
    "preparing": ("…", "Preparing", "info"),
    "ready": ("✓", "Ready", "ok"),
    "uninstalled": ("⊘", "Uninstalled", "idle"),
    "inactive": ("○", "Inactive", "idle"),
    "activating": ("◐", "Activating", "info"),
    "deactivating": ("◐", "Deactivating", "warn"),
    "building": ("…", "Building", "info"),
    "starting": ("…", "Starting", "info"),
    "crash_loop": ("✕", "Crash loop", "bad"),
    "stopped": ("○", "Stopped", "idle"),
    "passed": ("✓", "Passed", "ok"),
}


def connection_anchor(name: str) -> str:
    """The id of a connection's row on the Connections page, which the search links to."""
    return "connection-" + re.sub(r"[^A-Za-z0-9_-]", "-", name)


def status_info(status: str) -> tuple[str, str, str, str]:
    """A status's icon, word and tone, and the class name it gives its badge."""
    if status in _STATUSES:
        return (*_STATUSES[status], status)
    return ("•", status.capitalize(), "idle", "other")


templates.env.filters.update({"ago": ago, "span": span, "stamp": stamp, "show": show, "pretty": pretty})
templates.env.globals.update(
    {
        "meter": meter,
        "sparkline": sparkline,
        "flow_graph": flow_graph,
        "icon": icon,
        "can": can,
        "status_info": status_info,
        "connection_anchor": connection_anchor,
    }
)


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
            "env": request.app.state.environment.env,
            "protected": bool(state.project.settings.get("protected")),
            "version": VERSION,
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


# What every list page has: tiles at the top, a search box, and the actions it can do to many rows at once.

_BULK_MOST = 100  # rows one bulk action may name
Search = Annotated[str, Query(max_length=200)]
Names = Annotated[list[str] | None, Form()]


@dataclass(frozen=True)
class Tile:
    """A number at the top of a list page. `trend` is drawn under it, oldest first, when the charts are shown."""

    label: str
    value: int | str
    tone: str = ""  # "", "ok", "warn" or "bad"
    trend: tuple[float, ...] = ()
    note: str = ""


def _matches(query: str, *texts: str | None) -> bool:
    """Whether any of `texts` holds `query`, ignoring case. An empty query matches everything."""
    wanted = query.strip().lower()
    return not wanted or any(wanted in (text or "").lower() for text in texts)


def _plural(count: int, one: str, many: str | None = None) -> str:
    return f"{count} {one if count == 1 else many or one + 's'}"


def _polled(request: Request) -> bool:
    """Whether HTMX asks, to refresh or search a list: it takes the table, and the tiles above it are not read."""
    return request.headers.get("hx-request") == "true"


# Why a bulk action left a row as it was. The redirect names the rows and these codes, never text of its own.
_LEFT = {
    "running": "already running",
    "paused": "paused, so it can't be run",
    "missing": "not there",
    "over": "already over",
    "stream": "a daily stream run, which can't be cancelled",
}
_LEFT_SHOWN = 20


def _bulk_redirect(path: str, done: str, count: int, left: list[tuple[str, str]]) -> RedirectResponse:
    query = [("done", done), ("n", str(count))] + [("left", f"{code}:{name}") for name, code in left]
    return RedirectResponse(f"{path}?{urlencode(query)}", status_code=303)


def _bulk_notice(request: Request, noun: str, verbs: Iterable[str]) -> tuple[str, str] | None:
    """The notice a bulk action's redirect asks for: how many rows it did, and why it left any."""
    query = request.query_params
    done, count = query.get("done", ""), query.get("n", "")
    if done not in verbs or not (count.isascii() and count.isdigit()):
        return None
    left = []
    for item in query.getlist("left")[:_LEFT_SHOWN]:
        code, _, name = item.partition(":")
        if code in _LEFT and name:
            left.append(f"{name[:100]} is {_LEFT[code]}")
    message = f"{_plural(int(count), noun)} {done}."
    if not left:
        return ("ok", message)
    return ("warn", f"{message} Left as they were: " + "; ".join(left) + ".")


def _today(request: Request) -> datetime:
    now = request.app.state.feather.clock.now()
    return datetime.combine(now.date(), time.min, now.tzinfo or UTC)


_FLOW_BULK = {"run": "started", "pause": "paused", "resume": "resumed"}


def _flows_page(
    request: Request, actor: Actor, *, status: int = 200, notice: tuple[str, str] | None = None
) -> HTMLResponse:
    query = request.query_params
    chosen = {k: query.get(k, "")[:200] for k in ("status", "owner", "tag", "q", "active")}
    read = reads(request)
    everything = read.flows()
    shown = [
        f
        for f in everything
        if (not chosen["status"] or _state_of(f) == chosen["status"])
        and (not chosen["owner"] or f.owner == chosen["owner"])
        and (not chosen["tag"] or chosen["tag"] in f.tags)
        and (not chosen["active"] or (f.enabled and not f.paused))
        and _matches(chosen["q"], f.name, f.owner, *f.tags)
    ]
    tiles: list[Tile] = []
    if not _polled(request):  # a refresh of the table brings back the table only
        started, failed = read.run_activity()
        failing = sum(1 for f in everything if f.last_run and f.last_run.status == "failed")
        paused = sum(1 for f in everything if f.paused)
        active = sum(1 for f in everything if f.enabled and not f.paused)
        tiles = [
            Tile("Active", active, "", tuple(started), "runs a day"),
            Tile("Paused", paused, "warn" if paused else ""),
            Tile("Failing", failing, "bad" if failing else "", tuple(failed), "failed runs a day"),
        ]
    if notice is None:
        notice = _bulk_notice(request, "flow", _FLOW_BULK.values())
    return render(
        request,
        "flows.html",
        fragment="_flows_table.html",
        status=status,
        title="Flows",
        actor=actor,
        flows=shown,
        total=len(everything),
        tiles=tiles,
        state_of=_state_of,
        statuses=sorted({_state_of(f) for f in everything} | {"failed", "paused", "never"}),
        owners=sorted({f.owner for f in everything if f.owner}),
        tags=sorted({t for f in everything for t in f.tags}),
        chosen=chosen,
        notice=notice,
        sha=request.app.state.feather.project.code_sha[:8],
    )


@router.get("/flows", response_class=HTMLResponse)
def flows_list(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _flows_page(request, actor)


@router.post("/flows/bulk", dependencies=[Depends(require_csrf)])
def flows_bulk(
    request: Request,
    actor: CurrentActor,
    action: Annotated[str, Form()],
    name: Names = None,
    reason: Reason = "",
) -> Response:
    """Run, pause or resume the ticked flows, each through the action the flow page uses, so each is checked and
    audited alike. A refusal of the role stops at the first flow, before any has changed."""
    names = list(dict.fromkeys(name or []))
    if action not in _FLOW_BULK:
        return _flows_page(request, actor, status=422, notice=("bad", "Choose run, pause or resume."))
    if not names or len(names) > _BULK_MOST:
        message = f"Tick between 1 and {_BULK_MOST} flows first."
        return _flows_page(request, actor, status=422, notice=("bad", message))
    act, read, why = actions(request), reads(request), _reason(reason)
    left: list[tuple[str, str]] = []
    for flow in names:
        try:
            if action == "run":
                if read.has_active_run(flow):  # a second click, or a second tab: not a second run
                    left.append((flow, "running"))
                    continue
                act.run_flow(actor, flow, reason=why)
            elif action == "pause":
                act.pause_flow(actor, flow, reason=why)
            else:
                act.resume_flow(actor, flow, reason=why)
        except KeyError:
            left.append((flow, "missing"))
        except FlowPaused:
            left.append((flow, "paused"))
    return _bulk_redirect("/flows", _FLOW_BULK[action], len(names) - len(left), left)


@router.get("/flows/{name:path}/row", response_class=HTMLResponse)
def flow_row(request: Request, actor: CurrentActor, name: str) -> HTMLResponse:
    """What a flow's row opens to on the list: its datasets, its routes and its last runs."""
    try:
        detail = reads(request).flow_detail(name)
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    return render(request, "_flow_child.html", title=name, actor=actor, detail=detail)


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
        closed_windows=reads(request).closed_windows(name),
    )


@router.get("/flows/{name:path}", response_class=HTMLResponse)
def flow_detail(request: Request, actor: CurrentActor, name: str) -> HTMLResponse:
    return _flow_page(request, actor, name)


def _back(name: str, **query: str) -> RedirectResponse:
    suffix = f"?{urlencode(query)}" if query else ""
    return RedirectResponse(f"/flows/{quote(name, safe='/')}{suffix}", status_code=303)


@router.post("/flows/{name:path}/run", dependencies=[Depends(require_csrf)])
def run_flow(request: Request, actor: CurrentActor, name: str, reason: Reason = "") -> Response:
    try:
        run_id = actions(request).run_flow(actor, name, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    except FlowPaused as error:
        return _flow_page(request, actor, name, status=409, notice=("bad", str(error)))
    return _back(name, started=run_id)


@router.post("/flows/{name:path}/pause", dependencies=[Depends(require_csrf)])
def pause_flow(request: Request, actor: CurrentActor, name: str, reason: Reason = "") -> Response:
    try:
        actions(request).pause_flow(actor, name, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    return _back(name, done="paused")


@router.post("/flows/{name:path}/resume", dependencies=[Depends(require_csrf)])
def resume_flow(request: Request, actor: CurrentActor, name: str, reason: Reason = "") -> Response:
    try:
        actions(request).resume_flow(actor, name, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No flow named {name!r}.")
    return _back(name, done="resumed")


@router.post("/flows/{name:path}/backfill/preview", dependencies=[Depends(require_csrf)])
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


@router.post("/flows/{name:path}/backfill/confirm", dependencies=[Depends(require_csrf)])
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
    return dataset_of(request.app.state.feather, dataset).flow  # a packaged dataset, from the catalogue


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
    q = request.query_params.get("q", "")[:200]
    rows = [r for r in grid.rows if _matches(q, r.label, r.partition)]
    cells = [c.status for r in grid.rows for c in r.cells]
    failed = sum(1 for c in cells if c == "failed")
    applied = sum(1 for c in cells if c == "applied")
    tiles = [
        Tile("Partitions on this page", len(grid.rows)),
        Tile("Routes", len(grid.routes)),
        Tile("Applied", applied, "ok" if applied else ""),
        Tile("Failed", failed, "bad" if failed else ""),
    ]
    return render(
        request,
        "partitions.html",
        status=status,
        title=f"{dataset} partitions",
        actor=actor,
        grid=grid,
        rows=rows,
        tiles=tiles,
        q=q,
        flow=flow,
        notice=notice,
        preview=preview,
        confirm_action=confirm_action,
        confirm_label=confirm_label,
        since=since_text,
        replayed=request.query_params.get("replayed", ""),
    )


@router.get("/datasets/{dataset:path}/partitions", response_class=HTMLResponse)
def partitions(request: Request, actor: CurrentActor, dataset: str) -> HTMLResponse:
    return _grid_page(request, actor, dataset)


def _partitions_url(dataset: str, **query: str) -> str:
    suffix = f"?{urlencode(query)}" if query else ""
    return f"/datasets/{quote(dataset, safe='/')}/partitions{suffix}"


@router.post("/datasets/{dataset:path}/partitions/rerun/preview", dependencies=[Depends(require_csrf)])
def rerun_preview(
    request: Request, actor: CurrentActor, dataset: str, partition: Annotated[list[str] | None, Form()] = None
) -> Response:
    try:
        preview = actions(request).preview_rerun(actor, dataset, partition or [])
    except KeyError:
        return _not_found(request, f"No dataset named {dataset!r}.")
    except ValueError as error:
        return _grid_page(request, actor, dataset, status=422, notice=("bad", str(error)))
    action = f"/datasets/{quote(dataset, safe='/')}/partitions/rerun/confirm"
    return _grid_page(
        request, actor, dataset, preview=preview, confirm_action=action, confirm_label="Read them again"
    )


@router.post("/datasets/{dataset:path}/partitions/rerun/confirm", dependencies=[Depends(require_csrf)])
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


@router.post(
    "/datasets/{dataset:path}/partitions/{partition:path}/verify", dependencies=[Depends(require_csrf)]
)
def verify_partition(request: Request, actor: CurrentActor, dataset: str, partition: str) -> Response:
    try:
        found = actions(request).verify_partition(actor, dataset, partition)
    except KeyError:
        return _not_found(request, f"No dataset named {dataset!r}.")
    except (ValueError, PermanentError) as error:
        return _grid_page(request, actor, dataset, status=422, notice=("bad", str(error)))
    except TimeoutError as error:  # a packaged dataset's host is slow to answer: the run will hold the answer
        return _grid_page(request, actor, dataset, status=202, notice=("warn", str(error)))
    try:
        label = request.app.state.feather.runtime.dataset(dataset).partition.label_of(partition)
    except KeyError:  # a packaged dataset: its scheme, which labels partitions, is in its code
        label = partition
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
    action = f"/routes/{quote(route, safe='/')}/partitions/{quote(partition, safe='/')}/replay/confirm"
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


_RUN_STATUSES = ["queued", "running", "succeeded", "failed", "cancelled", "skipped"]


def _runs_page(
    request: Request,
    actor: Actor,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    confirm_cancel: list[str] | None = None,
) -> HTMLResponse:
    query = request.query_params
    chosen = {k: query.get(k, "")[:200] for k in ("flow", "status", "q", "active")}
    limit = _page_size(request, _RUNS_PAGE, 100)
    cursor = query.get("cursor") or None
    read = reads(request)
    if chosen["active"]:  # queued or running: never many, so both in full and no further page
        page = sorted(
            [r for s in _ACTIVE for r in read.runs(flow=chosen["flow"] or None, status=s, limit=limit)],
            key=lambda r: r.created_at,
            reverse=True,
        )[:limit]
        next_cursor = None
    else:
        page = read.runs(
            flow=chosen["flow"] or None, status=chosen["status"] or None, limit=limit, cursor=cursor
        )
        next_cursor = page[-1].run_id if len(page) == limit else None
    found = [r for r in page if _matches(chosen["q"], r.run_id, r.flow, r.actor, r.trigger, r.reason)]
    tiles: list[Tile] = []
    if not _polled(request):  # a refresh of the table brings back the table only
        started, failed = read.run_activity()
        today = _today(request)
        running, queued = read.count_runs("running"), read.count_runs("queued")
        failed_today = read.count_runs("failed", since=today)
        tiles = [
            Tile("Started today", read.count_runs(since=today), "", tuple(started), "a day"),
            Tile("Running", running, "ok" if running else ""),
            Tile("Failed today", failed_today, "bad" if failed_today else "", tuple(failed), "a day"),
            Tile("Queued", queued, "warn" if queued else ""),
        ]
    if notice is None:
        notice = _bulk_notice(request, "run", ("cancelled",))
    return render(
        request,
        "runs.html",
        fragment="_runs_table.html",
        status=status,
        title="Runs",
        actor=actor,
        runs=found,
        tiles=tiles,
        retries=read.retried_tasks([r.run_id for r in found]),
        flows=sorted(f.name for f in read.flows()),
        statuses=_RUN_STATUSES,
        chosen=chosen,
        next_cursor=next_cursor,
        notice=notice,
        confirm_cancel=confirm_cancel or [],
        stream=STREAM_TRIGGER,
        active_statuses=_ACTIVE,
    )


@router.get("/runs", response_class=HTMLResponse)
def runs_list(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _runs_page(request, actor)


@router.post("/runs/bulk", dependencies=[Depends(require_csrf)])
def runs_bulk(
    request: Request,
    actor: CurrentActor,
    action: Annotated[str, Form()],
    run_id: Names = None,
    confirm: Annotated[str, Form()] = "",
    reason: Reason = "",
) -> Response:
    """Cancel the ticked runs. The first step only asks, as on a run's page; a daily stream run and a run that is
    already over are left as they are, and said so."""
    ids = list(dict.fromkeys(run_id or []))
    if action != "cancel":
        return _runs_page(request, actor, status=422, notice=("bad", "Runs can only be cancelled in bulk."))
    if not ids or len(ids) > _BULK_MOST:
        message = f"Tick between 1 and {_BULK_MOST} runs first."
        return _runs_page(request, actor, status=422, notice=("bad", message))
    if confirm != "yes":
        return _runs_page(request, actor, confirm_cancel=ids)
    read, act = reads(request), actions(request)
    left: list[tuple[str, str]] = []
    for one in ids:
        try:
            run = read.run(one)
        except KeyError:
            left.append((one[:26], "missing"))
            continue
        if run.trigger == STREAM_TRIGGER:
            left.append((one[:8], "stream"))
        elif run.status not in _ACTIVE:
            left.append((one[:8], "over"))
        else:
            act.cancel_run(actor, one, reason=_reason(reason))
    return _bulk_redirect("/runs", "cancelled", len(ids) - len(left), left)


_CHILD_TASKS = 20


@router.get("/runs/{run_id}/row", response_class=HTMLResponse)
def run_row(request: Request, actor: CurrentActor, run_id: str) -> HTMLResponse:
    """What a run's row opens to on the list: its first tasks."""
    try:
        detail = reads(request).run_detail(run_id, limit=_CHILD_TASKS)
    except KeyError:
        return _not_found(request, f"No run with the id {run_id!r}.")
    return render(request, "_run_child.html", title=run_id[:8], actor=actor, detail=detail)


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


@router.get("/runs/{run_id}/tasks/{task_id}/records", response_class=HTMLResponse)
def task_records(
    request: Request, actor: CurrentActor, run_id: str, task_id: str, cursor: str = "", q: str = ""
) -> HTMLResponse:
    """The records of a task of a run, a page at a time: a page of its own, or the panel under the run's timeline when
    HTMX asks."""
    try:
        found = TaskRecordReader(request.app.state.feather).read(run_id, task_id, decode(cursor or None), q)
    except NoRecords as error:
        return _not_found(request, f"Task {task_id!r} holds no records: {error}.")
    except KeyError:
        return _not_found(request, f"Run {run_id!r} has no task {task_id!r}.")
    except (ApiProblem, ValueError):
        return _bad_cursor(request)
    shown = _masking(request, actor, found.dataset, f"task:{run_id}/{task_id}")
    if not isinstance(shown, pii.Masking):
        return shown
    found = pii.mask_task_records(found, shown.hidden)
    return render(
        request,
        "task_records.html",
        fragment="_task_records.html",
        title=f"Records of an {found.task.kind} task",
        actor=actor,
        found=found,
        run_id=run_id,
        next_cursor=encode(found.next_cursor),
        first_page=not cursor,
        pii=shown,
        unmask_href=_unmask_href(request),
    )


def _masking(request: Request, actor: Actor, dataset: str, target: str) -> "pii.Masking | HTMLResponse":
    """How a page shows the dataset's PII, `?unmask=1` asking to see it; a refusal is the page answered."""
    try:
        return pii.masking(
            request.app.state.feather,
            actor,
            dataset,
            unmask=request.query_params.get("unmask") == "1",
            target=target,
        )
    except Forbidden as error:
        return render(request, "error.html", status=403, title="Not allowed", message=str(error))


def _unmask_href(request: Request) -> str:
    query = {k: v for k, v in request.query_params.items() if k != "unmask"}
    return f"{request.url.path}?{urlencode({**query, 'unmask': '1'})}"


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
    return bool(preview and actions(request).protected)  # every change, in a protected environment


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
    q = request.query_params.get("q", "")[:200]
    shown = [v for v in views if _matches(q, v.group.route, v.group.error_class, v.group.message)]
    blocked = sum(len(v.blocked) for v in views)
    tiles = [
        Tile("Groups of failures", len(views), "bad" if views else ""),
        Tile("Failed deliveries", sum(v.group.count for v in views), "bad" if views else ""),
        Tile("Routes", len({v.group.route for v in views})),
        Tile("Partitions held up", blocked, "warn" if blocked else ""),
    ]
    return render(
        request,
        "failures.html",
        status=status,
        title="Failures",
        actor=actor,
        views=shown,
        tiles=tiles,
        q=q,
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
    state = request.app.state.feather
    try:
        destination = state.runtime.route(route).sink.connection
    except KeyError:
        try:
            destination = route_of(state, route).connection
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
    q = request.query_params.get("q", "")[:200]
    shown = [d for d in found if _matches(q, d.partition, d.version_id, d.last_error, d.status)]
    counts = {s: sum(1 for d in found if d.status == s) for s in ("applied", "failed", "held")}
    tiles = [
        Tile("On this page", len(found)),
        Tile("Applied", counts["applied"], "ok" if counts["applied"] else ""),
        Tile("Failed", counts["failed"], "bad" if counts["failed"] else ""),
        Tile("Held", counts["held"], "warn" if counts["held"] else ""),
    ]
    return render(
        request,
        "deliveries.html",
        status=status,
        title=f"{route} deliveries",
        actor=actor,
        route=route,
        destination=destination,
        deliveries=shown,
        tiles=tiles,
        q=q,
        statuses=_DELIVERY_STATUSES,
        chosen={"status": chosen, "partition": partition or ""},
        next_cursor=delivery_cursor(found[-1]) if len(found) == limit else None,
        notice=_handled(request, notice),
        preview=preview,
        reason_required=_reason_required(request, preview),
    )


@router.get("/routes/{route:path}/deliveries", response_class=HTMLResponse)
def route_deliveries(request: Request, actor: CurrentActor, route: str) -> HTMLResponse:
    return _deliveries_page(request, actor, route)


@router.get("/routes/{route:path}/deliveries/{version_id}", response_class=HTMLResponse)
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
        notice=("ok", "Compensating: the reversal runs as a task.")
        if request.query_params.get("done") == "compensating"
        else None,
    )


@router.post("/routes/{route:path}/deliveries/{version_id}/compensate", dependencies=[Depends(require_csrf)])
def delivery_compensate(
    request: Request, actor: CurrentActor, route: str, version_id: str, reason: Reason = ""
) -> Response:
    back = f"/routes/{quote(route, safe='/')}/deliveries/{quote(version_id, safe='')}"
    try:
        actions(request).compensate(actor, route, version_id, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No delivery of {version_id!r} to {route!r}.")
    except (ValueError, InvalidTransition) as error:
        detail = reads(request).delivery_detail(route, version_id)
        return render(
            request,
            "delivery_detail.html",
            status=422,
            title=f"{route} delivery",
            actor=actor,
            detail=detail,
            run="",
            notice=("bad", str(error)),
        )
    return RedirectResponse(f"{back}?done=compensating", status_code=303)


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
    shown = _masking(request, actor, version.dataset, f"version:{version_id}")
    if not isinstance(shown, pii.Masking):
        return shown
    records = [dict(r) for r in pii.mask_rows(records, shown.hidden)]
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
        pii=shown,
        unmask_href=_unmask_href(request),
    )


@router.get("/versions/{version_id}/diff", response_class=HTMLResponse)
def version_diff(request: Request, actor: CurrentActor, version_id: str, against: str = "") -> HTMLResponse:
    """What a version added, removed and changed against the partition's one before it, or `against`."""
    state = request.app.state.feather
    try:
        version = state.store.versions.get(version_id)
    except KeyError:
        return _not_found(request, f"No version {version_id!r}.")
    shown = _masking(request, actor, version.dataset, f"version:{version_id}")
    if not isinstance(shown, pii.Masking):
        return shown
    try:
        found = diff.diff_versions(state, version_id, against or None, hidden=shown.hidden)
    except KeyError:
        return _not_found(request, f"No version {against!r}.")
    except diff.NotComparable as error:
        return render(request, "error.html", status=422, title="Not comparable", message=str(error))
    counts = found.counts
    tiles = [
        Tile("Added", counts["added"], "ok" if counts["added"] else ""),
        Tile("Changed", counts["changed"], "warn" if counts["changed"] else ""),
        Tile("Removed", counts["removed"], "bad" if counts["removed"] else ""),
        Tile("Unchanged", found.unchanged),
    ]
    return render(
        request,
        "version_diff.html",
        title=f"Diff of {version.dataset}",
        actor=actor,
        diff=found,
        tiles=tiles,
        pii=shown,
        unmask_href=_unmask_href(request),
    )


@router.get("/trace", response_class=HTMLResponse)
def record_trace(request: Request, actor: CurrentActor, dataset: str = "", key: str = "") -> HTMLResponse:
    """What happened to one record, by its dataset key, as one timeline; or the form that asks for one."""
    tracer = Tracer(request.app.state.feather)
    datasets = tracer.datasets()
    found = None
    shown = None
    if dataset and key:
        try:
            found = tracer.trace(dataset, key)
        except KeyError:
            return _not_found(request, f"No dataset {dataset!r}.")
        shown = _masking(request, actor, dataset, f"record:{dataset}/{key}")
        if not isinstance(shown, pii.Masking):
            return shown
        found = pii.mask_trace(found, shown.hidden)
    return render(
        request,
        "trace.html",
        title=f"Trace of {key}" if found else "Trace a record",
        actor=actor,
        datasets=datasets,
        dataset=dataset or (datasets[0] if datasets else ""),
        key=key,
        trace=found,
        pii=shown,
        unmask_href=_unmask_href(request),
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
        return RedirectResponse(f"/routes/{quote(route, safe='/')}/deliveries?{done}", status_code=303)
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
    q = request.query_params.get("q", "")[:200]
    shown = [e for e in entries if _matches(q, e.row.route, e.row.key, e.row.error, e.dataset, e.partition)]
    tiles = [
        Tile("Open", total, "warn" if total else ""),
        Tile("Routes on this page", len({e.row.route for e in entries})),
        Tile("Datasets on this page", len({e.dataset for e in entries if e.dataset})),
    ]
    return render(
        request,
        "quarantine.html",
        status=status,
        title="Quarantine",
        actor=actor,
        entries=shown,
        tiles=tiles,
        q=q,
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


def _connections_page(
    request: Request, actor: Actor, *, status: int = 200, notice: tuple[str, str] | None = None
) -> HTMLResponse:
    environment: Environment = request.app.state.environment
    q = request.query_params.get("q", "")[:200]
    found = reads(request).connections()
    shown = [c for c in found if _matches(q, c.name, c.connector, c.site, *c.flows)]
    failing = sum(1 for c in found if c.last_check_ok is False)
    checked = sum(1 for c in found if c.last_check_ok)
    tiles = [
        Tile("Connections", len(found)),
        Tile("Checked OK", checked, "ok" if checked else ""),
        Tile("Check failed", failing, "bad" if failing else ""),
        Tile("Not checked yet", sum(1 for c in found if c.last_check_at is None)),
    ]
    return render(
        request,
        "connections.html",
        status=status,
        title="Connections",
        actor=actor,
        connections=shown,
        tiles=tiles,
        q=q,
        environment=[e for e in environment.connections() if _matches(q, e["name"], e["connector"])],
        env=environment.env,
        timeout=f"{action_limits.CHECK_TIMEOUT_S:g}",
        notice=notice,
    )


@router.get("/connections", response_class=HTMLResponse)
def connections(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _connections_page(request, actor)


@router.post("/connections/{name}/settings", dependencies=[Depends(require_csrf)])
async def set_connection_setting(request: Request, actor: CurrentActor, name: str) -> Response:
    form = await request.form()
    key, value, reason = (str(form.get(f) or "").strip() for f in ("key", "value", "reason"))
    environment: Environment = request.app.state.environment
    try:
        current = environment.entry("connection", name).value
        connector = str(current.get("connector") or "") if isinstance(current, dict) else ""
        typed = (
            None if not value else value if key.startswith("auth.") else coerce_setting(connector, key, value)
        )
        environment.update(actor, name, {key: typed}, reason=reason or None)
    except Forbidden as error:
        return _connections_page(request, actor, status=403, notice=("bad", str(error)))
    except (KeyError, ValueError) as error:
        return _connections_page(request, actor, status=422, notice=("bad", str(error)))
    return RedirectResponse(f"/connections#env-{quote(name, safe='')}", status_code=303)


@router.post("/secrets/{name:path}", dependencies=[Depends(require_csrf)])
async def replace_secret(request: Request, actor: CurrentActor, name: str) -> Response:
    form = await request.form()
    value, back = str(form.get("value") or ""), str(form.get("back") or "/connections")
    environment: Environment = request.app.state.environment
    try:
        environment.set_secret(actor, name, value)
    except Forbidden as error:
        return _connections_page(request, actor, status=403, notice=("bad", str(error)))
    except KeyConflict as error:
        return _connections_page(request, actor, status=409, notice=("bad", str(error)))
    except (VaultError, ValueError) as error:
        return _connections_page(request, actor, status=422, notice=("bad", str(error)))
    ours = back.startswith(("/connections", "/secrets")) and not back.startswith(("//", "/\\"))
    return RedirectResponse(back if ours else "/connections", status_code=303)


@router.get("/secrets", response_class=HTMLResponse)
def secrets(request: Request, actor: CurrentActor) -> HTMLResponse:
    """The environment's encrypted secrets, by name: when each was set and by whom, never a value."""
    environment: Environment = request.app.state.environment
    q = request.query_params.get("q", "")[:200]
    rows = [
        {**row, "updated_at": datetime.fromisoformat(row["updated_at"])}
        for row in environment.secrets()
        if _matches(q, row["name"], row["updated_by"])
    ]
    everything = environment.secrets()
    tiles = [
        Tile("Secrets", len(everything)),
        Tile(
            "Connections using them",
            len({r["name"].partition("/")[0] for r in everything if "/" in r["name"]}),
        ),
        Tile("Keys in use", len({r["key_id"] for r in everything})),
    ]
    return render(
        request,
        "secrets.html",
        title="Secrets",
        actor=actor,
        rows=rows,
        tiles=tiles,
        q=q,
        env=environment.env,
        manage=can(actor, "secrets.manage"),
    )


@router.post("/connections/{name}/check", dependencies=[Depends(require_csrf)])
def check_connection(request: Request, actor: CurrentActor, name: str) -> Response:
    try:
        actions(request).check_connection(actor, name)
    except KeyError:
        return _not_found(request, f"No connection named {name!r}.")
    return RedirectResponse(f"/connections#connection-{quote(name, safe='')}", status_code=303)


# Users and roles: admin only. The service refuses an operator; the page says so (see `install`).


def _users_page(
    request: Request, actor: Actor, *, status: int = 200, notice: tuple[str, str] | None = None
) -> HTMLResponse:
    accounts: Accounts = request.app.state.accounts
    everyone = accounts.users(actor)
    q = request.query_params.get("q", "")[:200]
    tiles = [
        Tile("Users", len(everyone)),
        Tile("Admins", sum(1 for u in everyone if u.role == "admin")),
        Tile("Approvers", sum(1 for u in everyone if "approver" in u.grants or u.role == "admin")),
    ]
    return render(
        request,
        "users.html",
        status=status,
        title="Users",
        actor=actor,
        users=[u for u in everyone if _matches(q, u.username, u.role)],
        tiles=tiles,
        q=q,
        roles=ROLES,
        grants=GRANTS,
        notice=notice,
    )


@router.get("/users", response_class=HTMLResponse)
def users(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _users_page(request, actor)


@router.post("/users", dependencies=[Depends(require_csrf)])
def add_user(
    request: Request,
    actor: CurrentActor,
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    role: Annotated[str, Form()],
    reason: Reason = "",
) -> Response:
    accounts: Accounts = request.app.state.accounts
    name = username.strip()
    try:
        accounts.create_user(actor, name, password, role, reason=_reason(reason))
    except DuplicateUser:
        return _users_page(
            request, actor, status=409, notice=("bad", f"There is a user named {name!r} already.")
        )
    except ValueError as error:
        return _users_page(request, actor, status=422, notice=("bad", str(error)))
    return _users_page(
        request,
        actor,
        notice=("ok", f"{name} can sign in now, as an {role}. Tell them the password another way."),
    )


@router.post("/users/{username}/password", dependencies=[Depends(require_csrf)])
def set_password(
    request: Request,
    actor: CurrentActor,
    username: str,
    password: Annotated[str, Form()],
    reason: Reason = "",
) -> Response:
    accounts: Accounts = request.app.state.accounts
    try:
        accounts.set_password(actor, username, password, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No user named {username!r}.")
    except ValueError as error:
        return _users_page(request, actor, status=422, notice=("bad", str(error)))
    return _users_page(request, actor, notice=("ok", f"{username} has a new password."))


@router.post("/users/{username}/role", dependencies=[Depends(require_csrf)])
def set_role(
    request: Request, actor: CurrentActor, username: str, role: Annotated[str, Form()], reason: Reason = ""
) -> Response:
    accounts: Accounts = request.app.state.accounts
    try:
        accounts.set_role(actor, username, role, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No user named {username!r}.")
    except ValueError as error:
        return _users_page(request, actor, status=422, notice=("bad", str(error)))
    return _users_page(request, actor, notice=("ok", f"{username} is now an {role}."))


@router.post("/users/{username}/grants", dependencies=[Depends(require_csrf)])
async def set_grants(request: Request, actor: CurrentActor, username: str) -> Response:
    accounts: Accounts = request.app.state.accounts
    form = await request.form()
    chosen = [str(g) for g in form.getlist("grant")]
    try:
        accounts.set_grants(actor, username, chosen, reason=_reason(str(form.get("reason") or "")))
    except KeyError:
        return _not_found(request, f"No user named {username!r}.")
    except ValueError as error:
        return _users_page(request, actor, status=422, notice=("bad", str(error)))
    said = ", ".join(chosen) if chosen else "no grants"
    return _users_page(request, actor, notice=("ok", f"{username} has {said} now."))


# Datasets and routes: the lists the sidebar's Datasets and Deliveries open.


@router.get("/datasets", response_class=HTMLResponse)
def datasets(request: Request, actor: CurrentActor) -> HTMLResponse:
    read = reads(request)
    everything = read.datasets()
    q = request.query_params.get("q", "")[:200]
    late = {s.dataset for s in read.stale_datasets()}
    never = sum(1 for d in everything if d.last_sealed_at is None)
    tiles = [
        Tile("Datasets", len(everything)),
        Tile("Late", len(late), "warn" if late else ""),
        Tile("Never read", never, "warn" if never else ""),
    ]
    return render(
        request,
        "datasets.html",
        title="Datasets",
        actor=actor,
        rows=[d for d in everything if _matches(q, d.name, d.flow, d.connection, d.strategy)],
        late=late,
        tiles=tiles,
        q=q,
    )


_READY_NOTICES = {
    "closed": "The partition is closed: what waits for it can fire once its last version is sealed."
}


def _readiness_page(
    request: Request, actor: Actor, *, status: int = 200, notice: tuple[str, str] | None = None
) -> HTMLResponse:
    sets = actions(request).readiness()
    q = request.query_params.get("q", "")[:200]
    rows = [
        {**row, "flow": s["flow"], "set": s["set"], "each": s["each"], "datasets": s["datasets"]}
        for s in sets
        for row in s["periods"]
        if _matches(q, s["flow"], row["period"], row["scope"], *s["datasets"])
    ]
    counts = {
        state: sum(1 for r in rows if r["state"] == state) for state in ("waiting", "ready", "fired", "late")
    }
    tiles = [
        Tile("Waiting flows", len(sets)),
        Tile("Waiting", counts["waiting"]),
        Tile("Fired", counts["fired"]),
        Tile("Late", counts["late"], "warn" if counts["late"] else ""),
    ]
    fired = request.query_params.get("fired")
    done = request.query_params.get("done")
    if notice is None and fired:
        notice = ("ok", f"Fired: run {fired} is queued.")
    elif notice is None and done in _READY_NOTICES:
        notice = ("ok", _READY_NOTICES[done])
    datasets = sorted({d for s in sets for d in s["datasets"]})
    return render(
        request,
        "readiness.html",
        status=status,
        title="Readiness",
        actor=actor,
        rows=rows,
        sets=sets,
        datasets=datasets,
        tiles=tiles,
        q=q,
        notice=notice,
    )


def _holds_page(
    request: Request, actor: Actor, *, status: int = 200, notice: tuple[str, str] | None = None
) -> HTMLResponse:
    q = request.query_params.get("q", "")[:200]
    rows = [
        h
        for h in actions(request).holds()
        if _matches(q, h["route"], h["version_id"], h["dataset"], h["partition"], h["reason"] or "")
    ]
    approvals = sum(1 for h in rows if h["kind"] == "approval")
    waiting = actions(request).requests()
    tiles = [Tile("Held", len(rows), "warn" if rows else ""), Tile("For approval", approvals)]
    if waiting or actions(request).protected:
        tiles.append(Tile("Asked for", len(waiting), "warn" if waiting else ""))
    done = request.query_params.get("done")
    if notice is None and done in ("request-approved", "request-rejected"):
        notice = (
            "ok",
            "Approved: it is done as asked." if done == "request-approved" else "Rejected: nothing is done.",
        )
    if notice is None and done in ("approved", "rejected"):
        word = (
            "it applies now" if done == "approved" else "it is skipped, and the versions behind it go ahead"
        )
        notice = ("ok", f"{done.capitalize()}: {word}.")
    return render(
        request,
        "holds.html",
        status=status,
        title="Holds",
        actor=actor,
        rows=rows,
        requests=waiting,
        tiles=tiles,
        q=q,
        notice=notice,
    )


def _alerts_page(
    request: Request, actor: Actor, *, status: int = 200, notice: tuple[str, str] | None = None
) -> HTMLResponse:
    state = request.app.state.feather
    shown = request.query_params.get("state", "firing")
    wanted = shown if shown in ("firing", "cleared") else None
    rows = state.store.alerts.list_alerts(wanted, 200)  # pyright: ignore[reportArgumentType]
    now = state.clock.now()
    firing = state.store.alerts.list_alerts("firing", 1000)
    tiles = [
        Tile(
            "Firing",
            len(firing),
            "bad" if any(a.severity == "critical" for a in firing) else "warn" if firing else "",
        ),
        Tile("Unacknowledged", sum(1 for a in firing if a.acked_by is None)),
        Tile("Snoozed", sum(1 for a in firing if a.snoozed_until is not None and a.snoozed_until > now)),
    ]
    return render(
        request,
        "alerts.html",
        status=status,
        title="Alerts",
        actor=actor,
        rows=rows,
        tiles=tiles,
        shown=shown,
        now=now,
        rules=alerts.rules(state),
        channels=sorted(alerts.channels_of(state)),
        notice=notice,
    )


@router.get("/alerts", response_class=HTMLResponse)
def alerts_page(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _alerts_page(request, actor)


@router.post("/alerts/{alert_id}/ack", dependencies=[Depends(require_csrf)])
def alerts_ack(request: Request, actor: CurrentActor, alert_id: str, reason: Reason = "") -> Response:
    try:
        alerts.AlertActions(request.app.state.feather).ack(actor, alert_id, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No alert {alert_id!r}.")
    except Forbidden as error:
        return _alerts_page(request, actor, status=403, notice=("bad", str(error)))
    return _alerts_page(request, actor, notice=("ok", "Acknowledged: its channels hear no more of it."))


@router.post("/alerts/{alert_id}/snooze", dependencies=[Depends(require_csrf)])
def alerts_snooze(
    request: Request,
    actor: CurrentActor,
    alert_id: str,
    minutes: Annotated[int, Form(ge=0, le=10080)],
    reason: Reason = "",
) -> Response:
    try:
        alerts.AlertActions(request.app.state.feather).snooze(
            actor, alert_id, minutes, reason=_reason(reason)
        )
    except KeyError:
        return _not_found(request, f"No alert {alert_id!r}.")
    except Forbidden as error:
        return _alerts_page(request, actor, status=403, notice=("bad", str(error)))
    except ValueError as error:
        return _alerts_page(request, actor, status=422, notice=("bad", str(error)))
    said = f"Snoozed for {minutes} minutes." if minutes else "No longer snoozed."
    return _alerts_page(request, actor, notice=("ok", said))


def _named_counts(raw: str) -> dict[str, int]:
    """`"acme/invoices=1, acme/orders=20"` as a mapping; raises `ValueError` for anything else."""
    found: dict[str, int] = {}
    for part in raw.split(","):
        name, sep, count = part.partition("=")
        if not sep or not name.strip():
            raise ValueError(part)
        found[name.strip()] = int(count.strip())
    return found


@router.post("/alerts/rules/{name}", dependencies=[Depends(require_csrf)])
async def alerts_rule(request: Request, actor: CurrentActor, name: str) -> Response:
    form = await request.form()
    channels = [str(c) for c in form.getlist("channel")]
    defaults = alerts.RULES.get(name, {})
    params: dict[str, Any] = {}
    for key, default in defaults.items():
        raw = str(form.get(key) or "").strip()
        if not raw:
            continue
        if isinstance(default, dict):  # `dataset_quiet`'s datasets: "name=5, other=1"
            try:
                params[key] = _named_counts(raw)
            except ValueError:
                said = f"{key} is written as name=count, separated by commas, such as acme/invoices=1."
                return _alerts_page(request, actor, status=422, notice=("bad", said))
            continue
        try:
            params[key] = type(default)(raw) if not isinstance(default, str) else raw
        except ValueError:
            return _alerts_page(request, actor, status=422, notice=("bad", f"{key} {raw!r} is not a number."))
    doing = alerts.AlertActions(request.app.state.feather)
    try:
        if form.get("reset"):
            doing.reset_rule(actor, name)
        else:
            doing.set_rule(actor, name, enabled=form.get("enabled") == "on", params=params, channels=channels)
    except KeyError:
        return _alerts_page(request, actor, notice=("ok", f"{name} has its default already."))
    except Forbidden as error:
        return _alerts_page(request, actor, status=403, notice=("bad", str(error)))
    except ValueError as error:
        return _alerts_page(request, actor, status=422, notice=("bad", str(error)))
    return _alerts_page(request, actor, notice=("ok", f"Rule {name} is saved."))


@router.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request, actor: CurrentActor) -> HTMLResponse:
    query = request.query_params
    wanted = {name: query.get(name, "")[:200].strip() for name in ("actor", "action", "target")}
    cursor = query.get("cursor") or None
    try:
        rows, next_cursor = reads(request).audit(
            actor, actor=wanted["actor"], action=wanted["action"], target=wanted["target"], cursor=cursor
        )
    except Forbidden as error:
        return render(request, "error.html", status=403, title="Not allowed", message=str(error))
    refused = sum(1 for r in rows if r.result.startswith("refused"))
    tiles = [Tile("Shown", len(rows)), Tile("Refused", refused, "warn" if refused else "")]
    later = {k: v for k, v in wanted.items() if v}
    return render(
        request,
        "audit.html",
        title="Audit",
        actor=actor,
        rows=rows,
        tiles=tiles,
        wanted=wanted,
        next_href=f"/audit?{urlencode({**later, 'cursor': next_cursor})}" if next_cursor else None,
    )


BUDGET_HIGH = 0.8  # a window this full shows as a warning


@router.get("/budgets", response_class=HTMLResponse)
def budgets_page(request: Request, actor: CurrentActor) -> HTMLResponse:
    details = reads(request).budget_details()
    waiting = sum(len(b.waiting) for b in details)
    busiest = max((w.used / w.units for b in details for w in b.windows if w.units), default=0.0)
    tiles = [
        Tile("Budgets", len(details)),
        Tile("Tasks waiting", waiting, "warn" if waiting else ""),
        Tile("Busiest window", f"{busiest:.0%}", "warn" if busiest >= BUDGET_HIGH else ""),
    ]
    return render(request, "budgets.html", title="Budgets", actor=actor, budgets=details, tiles=tiles)


@router.get("/budgets/{name}", response_class=HTMLResponse)
def budget_page(name: str, request: Request, actor: CurrentActor) -> HTMLResponse:
    try:
        detail = reads(request).budget_detail(name)
    except KeyError:
        return _not_found(request, f"No budget is named {name!r}.")
    tiles = [Tile("Windows", len(detail.windows)), Tile("Tasks waiting", len(detail.waiting))]
    return render(
        request, "budgets.html", title=f"Budget {name}", actor=actor, budgets=[detail], tiles=tiles, one=True
    )


@router.get("/holds", response_class=HTMLResponse)
def holds(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _holds_page(request, actor)


@router.post("/holds/requests/{request_id}/decide", dependencies=[Depends(require_csrf)])
def holds_decide_request(
    request: Request,
    actor: CurrentActor,
    request_id: str,
    decision: Annotated[str, Form(max_length=10)],
    reason: Reason = "",
) -> Response:
    try:
        actions(request).decide_request(
            actor, request_id, approve=decision == "approve", reason=_reason(reason)
        )
    except KeyError:
        return _not_found(request, f"No request {request_id!r}.")
    except SamePerson as error:
        return _holds_page(request, actor, status=403, notice=("bad", str(error)))
    except (ValueError, InvalidTransition) as error:
        return _holds_page(request, actor, status=422, notice=("bad", str(error)))
    word = "request-approved" if decision == "approve" else "request-rejected"
    return RedirectResponse(f"/holds?done={word}", status_code=303)


def asked_for(request: Request, asked: Requested) -> HTMLResponse:
    """A destructive action in a protected environment was asked for: say it waits for a second person."""
    return render(
        request,
        "error.html",
        status=202,
        title="Asked for",
        actor=current_actor(request),
        message=(
            "This is a protected environment, so this waits for a second person: an approver other than you decides "
            f"it on the Holds page (request {asked.request.request_id[:12]})."
        ),
    )


@router.post("/holds/decide", dependencies=[Depends(require_csrf)])
def holds_decide(
    request: Request,
    actor: CurrentActor,
    route: Annotated[str, Form(max_length=500)],
    version_id: Annotated[str, Form(max_length=200)],
    decision: Annotated[str, Form(max_length=10)],
    reason: Reason = "",
) -> Response:
    try:
        actions(request).decide_hold(
            actor, route, version_id, approve=decision == "approve", reason=_reason(reason)
        )
    except KeyError:
        return _not_found(request, f"No delivery of {version_id!r} on route {route!r}.")
    except (ValueError, InvalidTransition) as error:
        return _holds_page(request, actor, status=422, notice=("bad", str(error)))
    word = "approved" if decision == "approve" else "rejected"
    return RedirectResponse(f"/holds?done={word}", status_code=303)


@router.get("/readiness", response_class=HTMLResponse)
def readiness(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _readiness_page(request, actor)


@router.post("/readiness/fire", dependencies=[Depends(require_csrf)])
def readiness_fire(
    request: Request,
    actor: CurrentActor,
    flow: Annotated[str, Form(max_length=500)],
    period: Annotated[str, Form(max_length=100)] = "",
    scope: Annotated[str, Form(max_length=500)] = "",
    reason: Reason = "",
) -> Response:
    try:
        run_id = actions(request).fire_ready(actor, flow, period=period, scope=scope, reason=_reason(reason))
    except KeyError:
        return _not_found(request, f"No waiting flow named {flow!r}.")
    except (ValueError, FlowPaused) as error:
        return _readiness_page(request, actor, status=422, notice=("bad", str(error)))
    return RedirectResponse(f"/readiness?{urlencode({'fired': run_id})}", status_code=303)


@router.post("/readiness/close", dependencies=[Depends(require_csrf)])
def readiness_close(
    request: Request,
    actor: CurrentActor,
    dataset: Annotated[str, Form(max_length=500)],
    partition: Annotated[str, Form(max_length=500)] = "",
    reason: Reason = "",
) -> Response:
    try:
        actions(request).close_partition(actor, dataset, partition.strip(), reason=_reason(reason))
    except ValueError as error:
        return _readiness_page(request, actor, status=422, notice=("bad", str(error)))
    return RedirectResponse("/readiness?done=closed", status_code=303)


@router.get("/deliveries", response_class=HTMLResponse)
def deliveries(request: Request, actor: CurrentActor) -> HTMLResponse:
    everything = reads(request).routes()
    q = request.query_params.get("q", "")[:200]
    failing = sum(1 for r in everything if r.failed)
    tiles = [
        Tile("Routes", len(everything)),
        Tile(
            "Paused",
            sum(1 for r in everything if r.paused),
            "warn" if any(r.paused for r in everything) else "",
        ),
        Tile("With failures", failing, "bad" if failing else ""),
        Tile(
            "Held deliveries",
            sum(r.held for r in everything),
            "warn" if any(r.held for r in everything) else "",
        ),
    ]
    return render(
        request,
        "routes.html",
        title="Deliveries",
        actor=actor,
        rows=[r for r in everything if _matches(q, r.name, r.flow, r.connection)],
        tiles=tiles,
        q=q,
    )


# The shell: the status bar and the search of the command palette.

_READY_FOR = 10.0  # seconds a database check is reused for: every open page asks every 15 s


def _database(request: Request) -> dict[str, str]:
    from feather_server.app import readiness  # the app imports this module

    state = request.app.state.feather
    now = state.clock.now().timestamp()
    found: tuple[float, dict[str, str]] | None = getattr(request.app.state, "database_check", None)
    if found is None or not 0 <= now - found[0] < _READY_FOR:
        found = (now, readiness(state))
        request.app.state.database_check = found
    return found[1]


@router.get("/status", response_class=HTMLResponse)
def status_bar(request: Request, actor: CurrentActor) -> HTMLResponse:
    """The bottom bar: is the server up, does the database answer, who runs the work, which environment and
    version this is, and when this was looked at."""
    alive = sum(1 for w in reads(request).workers() if w.state == "alive")
    return render(
        request, "_status.html", title="Status", actor=actor, database=_database(request), alive=alive
    )


_SEARCH_MOST = 25
_RUN_ID_FROM = 4  # characters of a run id before the search looks for runs


@dataclass(frozen=True)
class Found:
    kind: str  # page, flow, run, dataset, route or connection
    label: str
    href: str
    detail: str = ""


def _search(request: Request, actor: Actor, q: str) -> list[Found]:
    wanted = q.strip()
    found = [
        Found("page", item.label, item.path, "Go to")
        for item in NAV
        if (item.needs is None or can(actor, item.needs)) and _matches(wanted, item.label)
    ]
    if not wanted:
        return found
    summaries = reads(request).flows()  # the flows here and the active flows of installed packages
    for flow in summaries:
        if _matches(wanted, flow.name, flow.owner, *flow.tags):
            found.append(Found("flow", flow.name, f"/flows/{quote(flow.name, safe='/')}", "Flow"))
    for flow in summaries:
        for dataset in flow.datasets:
            if _matches(wanted, dataset.name):
                href = f"/datasets/{quote(dataset.name, safe='/')}/partitions"
                found.append(Found("dataset", dataset.name, href, f"Dataset of {flow.name}"))
        for route in flow.routes:
            if _matches(wanted, route.name):
                href = f"/routes/{quote(route.name, safe='/')}/deliveries"
                found.append(Found("route", route.name, href, f"Route of {flow.name}"))
    names = set(request.app.state.feather.project.connections)
    names |= {e["name"] for e in request.app.state.environment.connections()}
    for name in sorted(n for n in names if _matches(wanted, n)):
        found.append(Found("connection", name, f"/connections#{connection_anchor(name)}", "Connection"))
    if len(wanted) >= _RUN_ID_FROM and re.fullmatch(r"[0-9A-Za-z_-]+", wanted):
        for run in reads(request).find_runs(wanted):
            href = f"/runs/{quote(run.run_id, safe='')}"
            found.append(Found("run", f"Run {run.run_id[:8]}", href, f"{run.flow} · {run.status}"))
    return found[:_SEARCH_MOST]


@router.get("/search.json")
def search_json(request: Request, actor: CurrentActor, q: Search = "") -> Response:
    """What the command palette offers for `q`: pages, flows, datasets, routes, connections and runs by id."""
    items = [
        {"kind": f.kind, "label": f.label, "href": f.href, "detail": f.detail}
        for f in _search(request, actor, q)
    ]
    return Response(
        json.dumps({"items": items}),
        media_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/search", response_class=HTMLResponse)
def search_page(request: Request, actor: CurrentActor, q: Search = "") -> HTMLResponse:
    """The same search as a page, for a browser without JavaScript."""
    return render(request, "search.html", title="Search", actor=actor, q=q, found=_search(request, actor, q))


async def ask_reason(request: Request, error: ReasonRequired) -> HTMLResponse:
    """A change in a protected environment came without a reason: ask for one, and send the same form again with
    it. A form with a file in it can't be sent again from here, so that one is only told to add a reason."""
    form = await request.form()
    files = any(not isinstance(value, str) for _, value in form.multi_items())
    fields = [
        (k, v) for k, v in form.multi_items() if isinstance(v, str) and k not in ("csrf_token", "reason")
    ]
    came = urlsplit(request.headers.get("referer", ""))
    back = (came.path or "/") if came.netloc == request.url.netloc else "/"  # only a page of ours
    return render(
        request,
        "reason.html",
        status=422,
        title="A reason is needed",
        actor=current_actor(request),
        action=request.url.path,
        fields=[] if files else fields,
        files=files,
        capability=error.capability,
        back=back,
    )


def refused(request: Request, error: Forbidden) -> HTMLResponse:
    """The page for an action the signed-in role may not do. The server's check is what counts: the dashboard only
    hides what an operator can't use."""
    actor = current_actor(request)
    return render(
        request,
        "error.html",
        status=403,
        title="Not allowed",
        actor=actor,
        message=f"This needs the {error.needed} role, and you have the {actor.role} role. Ask an admin.",
    )
