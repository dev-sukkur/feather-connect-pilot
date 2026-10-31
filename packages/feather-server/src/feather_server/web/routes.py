"""The dashboard's pages. Each route asks `Reads` or `Actions` for its data and hands it to a template: nothing here
decides anything the service layer does not.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from feather_connect.storage.port import Run
from feather_server.auth import CurrentActor, csrf_token
from feather_server.services.actions import Actions
from feather_server.services.reads import FlowSummary, Reads
from feather_server.web.charts import meter, sparkline

TEMPLATES = Path(__file__).parent / "templates"
NAV: list[tuple[str, str]] = [("Overview", "/")]  # (label, path): a page that exists adds itself here
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


def span(duration: timedelta | int) -> str:
    """A length of time (or a number of seconds) in words: `15 min`."""
    seconds = int(duration.total_seconds() if isinstance(duration, timedelta) else duration)
    for size, unit in ((86400, "d"), (3600, "h"), (60, "min")):
        if seconds >= size and seconds % size == 0:
            return f"{seconds // size} {unit}"
    return f"{seconds} s" if seconds < 60 else f"{seconds // 60} min {seconds % 60} s"


def stamp(moment: datetime) -> str:
    return moment.strftime("%d %b %Y %H:%M UTC")


templates.env.filters.update({"ago": ago, "span": span, "stamp": stamp})
templates.env.globals.update({"meter": meter, "sparkline": sparkline})


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
    finished = [_seconds(r) for r in reversed(runs) if r.status in ("succeeded", "failed")]
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
