"""The dashboard's Packages and Registries pages (flow packages §10, task 9): the installed packages, a package's
versions with their install and load checks, environments and hosts, a packaged flow's page, and the registries the
environment trusts. Every action goes through the service layer, which checks the role and writes the audit row.

Install, Activate, Deactivate, Switch version, Rollback and Uninstall ask first: with JavaScript in a dialog, and
without it on a second page, since the server acts only on a form that carries `confirm=yes`. The server never runs
package code: what a page shows of a package is what its manifest, its load check and the workers recorded.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Annotated, Any, cast
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from packaging.utils import canonicalize_name

from feather_connect.logs import redact
from feather_connect.storage.port import FlowActivation, InUse, PackageEnv, PackageHost, PackageVersion
from feather_server.auth import Actor, CurrentActor, require_csrf
from feather_server.services.access import can
from feather_server.services.packages import CHECKS, PREPARE_STEPS, Busy, InstallFailed, NotReady, Packages
from feather_server.services.registries import BUILT_IN, Registries, RegistryProblem
from feather_server.web.routes import Tile, render

router = APIRouter()

Text = Annotated[str, Form(max_length=500)]
_FAILING = ("crash_loop", "failed")


def _packages(request: Request) -> Packages:
    return request.app.state.packages


def _registries(request: Request) -> Registries:
    return request.app.state.registries


def _confirmed(confirm: str) -> bool:
    return confirm == "yes"


def _reason(text: str) -> str | None:
    return text.strip() or None


@dataclass(frozen=True)
class Ask:
    """A confirmation, for a browser without JavaScript: what the action does, and the form that does it."""

    title: str
    text: str
    action: str
    label: str
    fields: Mapping[str, str]
    danger: bool = False


@dataclass(frozen=True)
class FlowView:
    """One flow of a package: its row for each version, and what an operator can do with it."""

    name: str  # with the package prefix
    short: str
    rows: Sequence[FlowActivation]
    active: str | None
    activating: str | None
    failed: FlowActivation | None
    switch_to: Sequence[str]  # ready versions other than the active one, newest first
    rollback_to: str | None  # the version that was in service before, if it is ready


@dataclass(frozen=True)
class VersionView:
    row: PackageVersion
    hosts: Sequence[PackageHost]
    envs: Sequence[PackageEnv]
    steps: Sequence[tuple[str, str]]  # each prepare step and how it went: passed, failed, pending or skipped
    active_flows: Sequence[str] = field(default_factory=tuple[str, ...])


def _steps(row: PackageVersion) -> list[tuple[str, str]]:
    """How the version's preparing went, step by step, as its load check recorded it."""
    if row.state in ("installed", "preparing"):
        return [(step, "pending") for step in PREPARE_STEPS]
    failed_at = row.load_check.get("check") if row.state == "failed" else None
    found: list[tuple[str, str]] = []
    past = False
    for step in PREPARE_STEPS:
        if past:
            found.append((step, "skipped"))
        elif step == failed_at:
            found.append((step, "failed"))
            past = True
        else:
            found.append((step, "passed"))
    return found


def _key(version: str) -> tuple[int, Any]:
    from packaging.version import InvalidVersion, Version

    try:
        return (1, Version(version))
    except InvalidVersion:
        return (0, version)


def _flows(name: str, versions: Sequence[PackageVersion], rows: Sequence[FlowActivation]) -> list[FlowView]:
    ready = {v.version for v in versions if v.state == "ready"}
    by_flow: dict[str, list[FlowActivation]] = {}
    for row in rows:
        by_flow.setdefault(row.flow, []).append(row)
    found: list[FlowView] = []
    for flow, mine in sorted(by_flow.items()):
        mine.sort(key=lambda a: _key(a.version), reverse=True)
        active = next((a.version for a in mine if a.state == "active"), None)
        activating = next((a.version for a in mine if a.state == "activating"), None)
        failed = next((a for a in mine if a.state == "failed"), None)
        # A rollback goes back to the version that last served, other than the one serving now, if it is ready.
        before = sorted(
            (
                a
                for a in mine
                if a.state == "inactive"
                and a.version != active
                and a.version in ready
                and a.last_active_at is not None
            ),
            key=lambda a: cast(datetime, a.last_active_at),
            reverse=True,
        )
        found.append(
            FlowView(
                flow,
                flow.split("/", 1)[1] if "/" in flow else flow,
                mine,
                active,
                activating,
                failed,
                [a.version for a in mine if a.version in ready and a.version not in (active, activating)],
                before[0].version if active and before else None,
            )
        )
    return found


def _version_views(
    request: Request, versions: Sequence[PackageVersion], rows: Sequence[FlowActivation]
) -> list[VersionView]:
    store = request.app.state.feather.store
    hosts = store.packages.hosts()
    envs = store.packages.envs()
    return [
        VersionView(
            v,
            [h for h in hosts if h.package == v.name and h.version == v.version],
            [e for e in envs if e.lock_hash == v.lock_hash] if v.state != "uninstalled" else [],
            _steps(v),
            [a.flow for a in rows if a.version == v.version and a.state == "active"],
        )
        for v in sorted(versions, key=lambda v: _key(v.version), reverse=True)
    ]


# The list.


def _list_page(
    request: Request,
    actor: Actor,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    ask: Ask | None = None,
    form: Mapping[str, str] | None = None,
) -> HTMLResponse:
    store = request.app.state.feather.store
    q = request.query_params.get("q", "")[:200]
    versions = store.packages.versions()
    by_name: dict[str, list[PackageVersion]] = {}
    for v in versions:
        by_name.setdefault(v.name, []).append(v)
    active = store.packages.active()
    hosts = store.packages.hosts()
    failing = [h for h in hosts if h.state in _FAILING]
    rows = [
        {
            "name": name,
            "versions": sorted(mine, key=lambda v: _key(v.version), reverse=True),
            "active": sorted({(a.flow, a.version) for a in active if a.package == name}),
            "failing": [h for h in failing if h.package == name],
        }
        for name, mine in sorted(by_name.items())
        if not q or q.lower() in name
    ]
    installed = [n for n, mine in by_name.items() if any(v.state != "uninstalled" for v in mine)]
    tiles = [
        Tile("Installed", len(installed)),
        Tile("Active flows", len(active), "ok" if active else ""),
        Tile("Failing hosts", len(failing), "bad" if failing else ""),
        Tile("Preparing", sum(1 for v in versions if v.state in ("installed", "preparing"))),
    ]
    registries = [r.name for r in _registries(request).registries()]
    return render(
        request,
        "packages.html",
        status=status,
        title="Packages",
        actor=actor,
        tiles=tiles,
        packages=rows,
        q=q,
        notice=notice,
        ask=ask,
        registries=registries,
        form=form or {},
        checks=CHECKS,
    )


@router.get("/packages", response_class=HTMLResponse)
def packages_list(request: Request, actor: CurrentActor) -> HTMLResponse:
    done = request.query_params.get("done")
    notice = ("ok", _DONE[done]) if done in _DONE else None
    return _list_page(request, actor, notice=notice)


_DONE = {
    "uninstalled": "The version is uninstalled. Its history stays; its wheel and environment are removed.",
}


@router.post("/packages/install", dependencies=[Depends(require_csrf)])
def install_package(
    request: Request,
    actor: CurrentActor,
    name: Annotated[str, Form(max_length=200)],
    registry: Annotated[str, Form(max_length=64)] = "feather",
    version: Annotated[str, Form(max_length=64)] = "",
    reason: Text = "",
    confirm: Annotated[str, Form()] = "",
) -> Response:
    form = {"name": name, "registry": registry, "version": version, "reason": reason}
    wanted = f"{name.strip()} {version.strip()}".strip() or "the package"
    if not _confirmed(confirm):
        ask = Ask(
            f"Install {wanted}?",
            f"It is fetched from {registry} and checked; a worker then builds its environment and load-checks it. "
            "Its flows start inactive: nothing runs until one is activated.",
            "/packages/install",
            "Install",
            form,
        )
        return _list_page(request, actor, ask=ask, form=form)
    try:
        installed = _packages(request).install(
            actor, name.strip(), version.strip() or None, registry=registry, reason=_reason(reason)
        )
    except InstallFailed as error:
        return _list_page(
            request, actor, status=422, notice=("bad", f"The {error.check} check failed: {error}"), form=form
        )
    except Busy as error:
        return _list_page(request, actor, status=503, notice=("bad", str(error)), form=form)
    return RedirectResponse(f"/packages/{quote(installed.name, safe='')}?done=installed", status_code=303)


# A package.


def _package_page(
    request: Request,
    actor: Actor,
    name: str,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    ask: Ask | None = None,
) -> HTMLResponse:
    store = request.app.state.feather.store
    project = canonicalize_name(name)
    versions = store.packages.versions(project)
    if not versions:
        return render(
            request, "error.html", status=404, title="Not found", message=f"No package named {name!r}."
        )
    rows = store.packages.activations(project)
    done = request.query_params.get("done")
    if notice is None and done in _PACKAGE_DONE:
        notice = ("ok", _PACKAGE_DONE[done])
    return render(
        request,
        "package_detail.html",
        status=status,
        title=project,
        actor=actor,
        name=project,
        versions=_version_views(request, versions, rows),
        flows=_flows(project, versions, rows),
        notice=notice,
        ask=ask,
        checks=CHECKS,
        signing=_packages(request).signing(),
    )


_PACKAGE_DONE = {
    "installed": "Installed. A worker builds its environment and load-checks it; then its flows can be activated.",
    "activating": "Asked for. The flow moves to that version once a worker's host of it is up.",
    "deactivated": "The flow is inactive. Its runs in progress finish.",
}


@router.get("/packages/{name}", response_class=HTMLResponse)
def package_detail(request: Request, actor: CurrentActor, name: str) -> HTMLResponse:
    return _package_page(request, actor, name)


def _back(name: str, short: str, back: str, done: str) -> RedirectResponse:
    page = f"/packages/{quote(name, safe='')}"
    if back == "flow":
        page += f"/flows/{quote(short, safe='')}"
    return RedirectResponse(f"{page}?done={done}", status_code=303)


def _again(request: Request, actor: Actor, name: str, short: str, back: str, **kw: Any) -> HTMLResponse:
    if back == "flow":
        return _flow_page(request, actor, name, short, **kw)
    return _package_page(request, actor, name, **kw)


@router.post("/packages/{name}/flows/{short}/activate", dependencies=[Depends(require_csrf)])
def activate(
    request: Request,
    actor: CurrentActor,
    name: str,
    short: str,
    version: Annotated[str, Form(max_length=64)],
    reason: Text = "",
    back: Annotated[str, Form(max_length=16)] = "package",
    kind: Annotated[str, Form(max_length=16)] = "activate",
    confirm: Annotated[str, Form()] = "",
) -> Response:
    project = canonicalize_name(name)
    flow = f"{project}/{short}"
    if not _confirmed(confirm):
        verb = {"switch": "Switch", "rollback": "Roll back"}.get(kind, "Activate")
        ask = Ask(
            f"{verb} {flow} to {version}?",
            f"A worker starts a host of {version}; the flow's triggers move to it once the host is up, and the version "
            "in service until then keeps running. Runs in progress finish on the code they started with.",
            request.url.path,
            verb,
            {"version": version, "reason": reason, "back": back, "kind": kind},
        )
        return _again(request, actor, project, short, back, ask=ask)
    try:
        _packages(request).activate(actor, flow, version, reason=_reason(reason))
    except InstallFailed as error:  # signed packages are required, and this one isn't, or no longer
        text = f"The {error.check} check failed: {error}"
        return _again(request, actor, project, short, back, status=422, notice=("bad", text))
    except NotReady as error:
        return _again(request, actor, project, short, back, status=409, notice=("bad", str(error)))
    except KeyError as error:
        text = str(error.args[0] if error.args else error)
        return _again(request, actor, project, short, back, status=404, notice=("bad", redact(text)))
    return _back(project, short, back, "activating")


@router.post("/packages/{name}/flows/{short}/deactivate", dependencies=[Depends(require_csrf)])
def deactivate(
    request: Request,
    actor: CurrentActor,
    name: str,
    short: str,
    reason: Text = "",
    back: Annotated[str, Form(max_length=16)] = "package",
    confirm: Annotated[str, Form()] = "",
) -> Response:
    project = canonicalize_name(name)
    flow = f"{project}/{short}"
    if not _confirmed(confirm):
        ask = Ask(
            f"Deactivate {flow}?",
            "Its triggers stop within seconds and its webhooks answer 404. Runs in progress finish, with their "
            "deliveries and retries. Its history stays.",
            request.url.path,
            "Deactivate",
            {"reason": reason, "back": back},
            danger=True,
        )
        return _again(request, actor, project, short, back, ask=ask)
    try:
        _packages(request).deactivate(actor, flow, reason=_reason(reason))
    except KeyError as error:
        text = str(error.args[0] if error.args else error)
        return _again(request, actor, project, short, back, status=409, notice=("bad", redact(text)))
    return _back(project, short, back, "deactivated")


@router.post("/packages/{name}/versions/{version}/uninstall", dependencies=[Depends(require_csrf)])
def uninstall(
    request: Request,
    actor: CurrentActor,
    name: str,
    version: str,
    reason: Text = "",
    confirm: Annotated[str, Form()] = "",
) -> Response:
    project = canonicalize_name(name)
    if not _confirmed(confirm):
        ask = Ask(
            f"Uninstall {project} {version}?",
            "Its wheel and environment are removed; its history, runs and deliveries stay. This can't be undone: "
            "the version can't be installed here again.",
            request.url.path,
            "Uninstall",
            {"reason": reason},
            danger=True,
        )
        return _package_page(request, actor, project, ask=ask)
    try:
        _packages(request).uninstall(actor, project, version, reason=_reason(reason))
    except InUse as error:
        return _package_page(request, actor, project, status=409, notice=("bad", str(error)))
    except KeyError as error:
        text = str(error.args[0] if error.args else error)
        return _package_page(request, actor, project, status=404, notice=("bad", redact(text)))
    remaining = [
        v for v in request.app.state.feather.store.packages.versions(project) if v.state != "uninstalled"
    ]
    if not remaining:
        return RedirectResponse("/packages?done=uninstalled", status_code=303)
    return RedirectResponse(f"/packages/{quote(project, safe='')}?done=uninstalled", status_code=303)


_PACKAGE_DONE["uninstalled"] = _DONE["uninstalled"]


# A packaged flow's page.


def _flow_page(
    request: Request,
    actor: Actor,
    name: str,
    short: str,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    ask: Ask | None = None,
) -> HTMLResponse:
    store = request.app.state.feather.store
    project = canonicalize_name(name)
    versions = store.packages.versions(project)
    flow = f"{project}/{short}"
    rows = [a for a in store.packages.activations(project) if a.flow == flow]
    if not rows:
        return render(
            request, "error.html", status=404, title="Not found", message=f"No flow named {flow!r}."
        )
    [view] = _flows(project, versions, rows)
    done = request.query_params.get("done")
    if notice is None and done in _PACKAGE_DONE:
        notice = ("ok", _PACKAGE_DONE[done])
    states = {v.version: v for v in versions}
    return render(
        request,
        "package_flow.html",
        status=status,
        title=flow,
        actor=actor,
        name=project,
        flow=view,
        states=states,
        runs=store.runs.list_runs(flow=flow, limit=10),
        notice=notice,
        ask=ask,
    )


@router.get("/packages/{name}/flows/{short}", response_class=HTMLResponse)
def package_flow(request: Request, actor: CurrentActor, name: str, short: str) -> HTMLResponse:
    return _flow_page(request, actor, name, short)


# Registries.


def _registries_page(
    request: Request,
    actor: Actor,
    *,
    status: int = 200,
    notice: tuple[str, str] | None = None,
    ask: Ask | None = None,
    form: Mapping[str, str] | None = None,
) -> HTMLResponse:
    found = _registries(request).registries()
    if notice is None:
        notice = _registry_notice(request)
    tiles = [
        Tile("Registries", len(found)),
        Tile("Built-in", "on" if _registries(request).enabled else "off"),
        Tile("With a token", sum(1 for r in found if r.token_ref)),
    ]
    return render(
        request,
        "registries.html",
        status=status,
        title="Registries",
        actor=actor,
        tiles=tiles,
        registries=found,
        notice=notice,
        ask=ask,
        form=form or {},
        can_manage=can(actor, "registries.manage"),
    )


@router.get("/registries", response_class=HTMLResponse)
def registries(request: Request, actor: CurrentActor) -> HTMLResponse:
    return _registries_page(request, actor)


@router.post("/registries/add", dependencies=[Depends(require_csrf)])
def add_registry(
    request: Request,
    actor: CurrentActor,
    name: Annotated[str, Form(max_length=64)],
    index_url: Annotated[str, Form(max_length=500)],
    token_ref: Annotated[str, Form(max_length=500)] = "",
    reason: Text = "",
) -> Response:
    form = {
        "name": name,
        "index_url": index_url,
        "token_ref": token_ref if token_ref.startswith("${") else "",
    }
    try:
        added = _registries(request).add(
            actor, name.strip(), index_url.strip(), token_ref.strip() or None, reason=_reason(reason)
        )
    except ValueError as error:
        return _registries_page(request, actor, status=422, notice=("bad", redact(str(error))), form=form)
    return RedirectResponse(f"/registries?{urlencode({'added': added.name})}", status_code=303)


def _registry_notice(request: Request) -> tuple[str, str] | None:
    """What the last add or test did, after its redirect: a test's answer is read back from its audit row."""
    query = request.query_params
    registries = {r.name for r in _registries(request).registries()}
    if (name := query.get("added")) in registries:
        return ("ok", f"The registry {name} is added.")
    if (name := query.get("tested")) in registries:
        if name == BUILT_IN:
            return ("ok", f"{name} answers: it is built in.")
        since = request.app.state.feather.clock.now() - timedelta(minutes=10)
        rows = request.app.state.feather.store.audit.for_target(f"registry:{name}", since=since)
        tests = [r for r in rows if r.action == "test_registry"]
        if not tests:
            return None
        result = tests[-1].result
        if result == "ok":
            return ("ok", f"{name} answers.")
        return ("bad", f"{name}: {redact(result.removeprefix('failed: '))}")
    if (name := query.get("removed")) and name not in registries:
        return ("ok", f"The registry {name} is removed.")
    return None


@router.post("/registries/{name}/test", dependencies=[Depends(require_csrf)])
def test_registry(request: Request, actor: CurrentActor, name: str) -> Response:
    try:
        found = _registries(request).test(actor, name)
    except KeyError as error:
        text = str(error.args[0] if error.args else error)
        return _registries_page(request, actor, status=404, notice=("bad", redact(text)))
    except RegistryProblem as error:
        return _registries_page(request, actor, status=422, notice=("bad", f"{name}: {redact(str(error))}"))
    return RedirectResponse(f"/registries?{urlencode({'tested': str(found['registry'])})}", status_code=303)


@router.post("/registries/{name}/remove", dependencies=[Depends(require_csrf)])
def remove_registry(
    request: Request, actor: CurrentActor, name: str, confirm: Annotated[str, Form()] = "", reason: Text = ""
) -> Response:
    if not _confirmed(confirm):
        ask = Ask(
            f"Remove the registry {name}?",
            "Nothing can be installed from it any more. Versions installed from it stay, as they are kept here.",
            request.url.path,
            "Remove",
            {},
            danger=True,
        )
        return _registries_page(request, actor, ask=ask)
    try:
        _registries(request).remove(actor, name, reason=_reason(reason))
    except (KeyError, ValueError) as error:
        text = str(error.args[0] if isinstance(error, KeyError) and error.args else error)
        return _registries_page(request, actor, status=404, notice=("bad", redact(text)))
    return RedirectResponse(f"/registries?{urlencode({'removed': name})}", status_code=303)
