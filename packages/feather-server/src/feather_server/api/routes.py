"""The JSON API under `/api/v1` (design §9.2). Each route parses, calls the service layer and formats: the actions, their
checks, previews and audit rows are those of the dashboard.

A mass action takes two calls: `…:preview` returns the impact and a `confirmation_token`, and the action takes the
token. A state-changing call takes a `reason` in its body and an `Idempotency-Key` header: a repeat of the key by the
same caller within 24 hours gets the first answer again, and does nothing.
"""

import json
import re
import threading
from collections.abc import Callable, Iterator
from datetime import date, timedelta
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi import Path as Path_
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from feather_connect.storage.port import DeliveryStatus, EnvEntry, EnvKind, delivery_cursor
from feather_connect.vault import MasterKey
from feather_server.api.errors import ApiProblem, translate
from feather_server.api.paging import check_limit, decode, page
from feather_server.auth import Actor, CurrentActor, require_csrf
from feather_server.services import task_records as records
from feather_server.services.accounts import Accounts
from feather_server.services.actions import Actions, DeliveryAction, DeliverySelection
from feather_server.services.environment import Environment
from feather_server.services.packages import Packages
from feather_server.services.previews import Preview
from feather_server.services.reads import Reads
from feather_server.services.registries import Registries
from feather_server.services.trace import Tracer, as_json

PREFIX = "/api/v1"
IDEMPOTENCY_TTL = timedelta(hours=24)
_IDEMPOTENCY = "idempotency"
_MAX_KEY = 200

router = APIRouter(prefix=PREFIX, tags=["api"], dependencies=[Depends(require_csrf)])

Limit = Annotated[int, Query(description="Items on a page, from 1 to 500.")]
Cursor = Annotated[str | None, Query(description="The `next_cursor` of the page before.")]
IdempotencyKey = Annotated[str | None, Header(alias="Idempotency-Key")]


def _actions(request: Request) -> Actions:
    return request.app.state.actions


def _reads(request: Request) -> Reads:
    return request.app.state.reads


def _accounts(request: Request) -> Accounts:
    return request.app.state.accounts


Do = Annotated[Actions, Depends(_actions)]
Manage = Annotated[Accounts, Depends(_accounts)]


def _registries(request: Request) -> Registries:
    return request.app.state.registries


Trusted = Annotated[Registries, Depends(_registries)]


def _packages(request: Request) -> Packages:
    return request.app.state.packages


Installs = Annotated[Packages, Depends(_packages)]
See = Annotated[Reads, Depends(_reads)]


def call[T](fn: Callable[[], T]) -> T:
    """Call the service, turning what it raises into a problem."""
    try:
        return fn()
    except Exception as error:
        problem = translate(error)
        if problem is None:
            raise
        raise problem from error


def ok(data: Any, status: int = 200) -> JSONResponse:
    return JSONResponse(jsonable_encoder(data), status)


def _once(
    request: Request, actor: Actor, key: str | None, fn: Callable[[], Any], status: int = 200
) -> JSONResponse:
    """Do `fn` once per idempotency key and caller, keeping its answer, success or problem, for 24 hours. A request
    with a key that another request of this process is answering waits for that answer. A 503 is not kept: the
    request may be repeated with its key."""
    if key is None:
        return _answer(fn, status)
    if not key or len(key) > _MAX_KEY:
        raise ApiProblem(422, "Unprocessable Content", f"an Idempotency-Key has 1 to {_MAX_KEY} characters")
    target = f"{_IDEMPOTENCY}:{actor.kind}:{actor.name}"
    with _InFlight(f"{target}:{key}"):
        return _once_held(request, actor, target, key, fn, status)


_FLIGHTS: dict[str, tuple[threading.Lock, int]] = {}
_FLIGHTS_LOCK = threading.Lock()


class _InFlight:
    """A lock per idempotency key, kept while a request holds it or waits for it."""

    def __init__(self, key: str) -> None:
        self._key = key

    def __enter__(self) -> None:
        with _FLIGHTS_LOCK:
            lock, users = _FLIGHTS.get(self._key, (threading.Lock(), 0))
            _FLIGHTS[self._key] = (lock, users + 1)
        lock.acquire()

    def __exit__(self, *_: object) -> None:
        with _FLIGHTS_LOCK:
            lock, users = _FLIGHTS[self._key]
            lock.release()
            if users == 1:
                del _FLIGHTS[self._key]
            else:
                _FLIGHTS[self._key] = (lock, users - 1)


def _once_held(
    request: Request, actor: Actor, target: str, key: str, fn: Callable[[], Any], status: int
) -> JSONResponse:
    overrides = request.app.state.feather.store.overrides
    where = f"{request.method} {request.url.path}"
    kept = overrides.get(target, key)
    if kept is not None:
        if kept["request"] != where:
            raise ApiProblem(
                422, "Unprocessable Content", f"this Idempotency-Key was used for {kept['request']} already"
            )
        return JSONResponse(kept["body"], kept["status"], media_type=kept["media_type"])
    answer = _answer(fn, status)
    if answer.status_code == 503:
        return answer
    now = request.app.state.feather.clock.now()
    overrides.set(
        target,
        key,
        {
            "request": where,
            "status": answer.status_code,
            "body": json.loads(bytes(answer.body)),
            "media_type": answer.media_type,
        },
        actor=actor.name,
        reason="idempotency",
        expires_at=now + IDEMPOTENCY_TTL,
    )
    return answer


def _answer(fn: Callable[[], Any], status: int) -> JSONResponse:
    from feather_server.api.errors import problem_response

    try:
        return ok(call(fn), status)
    except ApiProblem as problem:
        return problem_response(problem)


_UNSAFE = re.compile(r"[^A-Za-z0-9._+-]")


def attachment(filename: str) -> str:
    """A Content-Disposition for `filename` (RFC 6266): a plain ASCII `filename` with anything unsafe replaced, and
    the name itself, percent-encoded, in `filename*`."""
    plain = _UNSAFE.sub("_", filename)[:200] or "download"
    star = "UTF-8" + "''" + quote(filename, safe="")
    return f'attachment; filename="{plain}"; filename*={star}'


# Bodies.


class Reason(BaseModel):
    reason: str | None = None


class RunBody(Reason):
    partitions: list[str] | None = None


class Confirm(Reason):
    confirmation_token: str | None = None


class RerunPreview(BaseModel):
    partitions: list[str] = Field(min_length=1)


class BackfillPreview(BaseModel):
    start: date
    end: date
    max_concurrent: int = 4


class DeliveriesPreview(BaseModel):
    route: str | None = None
    dataset: str | None = None
    partition: str | None = None
    error: str | None = None
    error_class: str | None = None
    version_ids: list[str] | None = None


def _preview(found: Preview) -> dict[str, Any]:
    return {"action": found.action, "confirmation_token": found.token, "impact": found.impact}


def _token(body: Confirm | None, preview_path: str) -> str:
    if body is None or not body.confirmation_token:
        raise ApiProblem(
            400,
            "Bad Request",
            "this is a mass action: call its preview first, and send the confirmation_token it returns",
            preview=f"{PREFIX}{preview_path}",
        )
    return body.confirmation_token


# Catalogue.


@router.get("/flows")
def flows(reads: See, _: CurrentActor) -> JSONResponse:
    return ok({"items": reads.flows(), "next_cursor": None})


@router.get("/flows/{flow}")
def flow(flow: str, reads: See, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: reads.flow_detail(flow)))


def _dataset(request: Request, name: str) -> dict[str, Any]:
    runtime = request.app.state.feather.runtime
    found = runtime.dataset(name)
    owner = next(f for f in runtime.flows() if found in f.datasets)
    key = found.key if isinstance(found.key, str) else list(found.key)
    return {
        "name": found.name,
        "flow": owner.name,
        "connection": found.source.connection,
        "connector": found.source.spec.name,
        "key": key,
        "strategy": found.strategy.name,
        "partition": type(found.partition).__name__,
        "priority": found.priority,
        "on_record_error": found.on_record_error,
        "routes": [r.name for r in owner.routes],
    }


@router.get("/datasets/{dataset}")
def dataset(dataset: str, request: Request, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: _dataset(request, dataset)))


def _route(request: Request, name: str) -> dict[str, Any]:
    runtime = request.app.state.feather.runtime
    found = runtime.route(name)
    owner = next(f for f in runtime.flows() if found in f.routes)
    return {
        "name": found.name,
        "flow": owner.name,
        "connection": found.sink.connection,
        "connector": found.sink.spec.name,
        "mode": found.mode,
        "priority": found.priority,
        "paused": runtime.is_route_paused(found.name),
        "datasets": [d.name for d in owner.datasets],
    }


@router.get("/routes/{route}")
def route(route: str, request: Request, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: _route(request, route)))


# Runs.


@router.get("/runs")
def runs(
    reads: See,
    _: CurrentActor,
    flow: str | None = None,
    status: str | None = None,
    limit: Limit = 50,
    cursor: Cursor = None,
) -> JSONResponse:
    found = reads.runs(flow, status, check_limit(limit), decode(cursor))
    return ok(page(found, found[-1].run_id if len(found) == limit else None))


@router.get("/runs/{run_id}")
def run(run_id: str, reads: See, _: CurrentActor) -> JSONResponse:
    detail = call(lambda: reads.run_detail(run_id, limit=1))
    return ok(
        {
            "run": detail.run,
            "tasks": detail.task_total,
            "tasks_failed": detail.tasks_failed,
            "versions": detail.versions,
            "deliveries": detail.deliveries,
            "ticks": detail.ticks,
        }
    )


@router.get("/runs/{run_id}/tasks")
def run_tasks(
    run_id: str, reads: See, _: CurrentActor, limit: Limit = 100, cursor: Cursor = None
) -> JSONResponse:
    detail = call(lambda: reads.run_detail(run_id, decode(cursor), check_limit(limit)))
    return ok(page(detail.tasks, detail.next_task))


@router.get("/runs/{run_id}/tasks/{task_id}/records")
def task_records(
    run_id: str,
    task_id: str,
    request: Request,
    _: CurrentActor,
    limit: Annotated[int, Query(description="Records on a page, from 1 to 500.")] = records.PAGE,
    cursor: Cursor = None,
    q: Annotated[str, Query(description="Only the keys that start with this.")] = "",
) -> JSONResponse:
    """The records of an extract task's versions, or of an apply task's version with what its route made of each, a
    page at a time by key. `not_indexed` says the version was sealed before records were indexed."""
    raw = decode(cursor)

    def read() -> dict[str, Any]:
        try:
            found = records.TaskRecordReader(request.app.state.feather).read(
                run_id, task_id, raw, q, check_limit(limit)
            )
        except ValueError:
            raise ApiProblem(
                422, "Unprocessable Content", "this cursor is not one this server handed out"
            ) from None
        return {
            **records.as_json(found),
            **page([records.item_json(r) for r in found.records], found.next_cursor),
        }

    return ok(call(read))


@router.get("/tasks/{task_id}/logs")
def task_logs(task_id: str, reads: See, _: CurrentActor) -> JSONResponse:
    return ok({"task_id": task_id, "lines": reads.task_logs(task_id)})


@router.post("/flows/{flow}:run", status_code=202)
def run_flow(
    flow: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: RunBody | None = None,
) -> JSONResponse:
    wanted = body or RunBody()
    return _once(
        request,
        actor,
        key,
        lambda: {"run_id": actions.run_flow(actor, flow, reason=wanted.reason, partitions=wanted.partitions)},
        202,
    )


@router.post("/runs/{run_id}:cancel")
def cancel_run(
    run_id: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Reason | None = None,
) -> JSONResponse:
    reason = (body or Reason()).reason

    def cancel() -> dict[str, Any]:
        actions.cancel_run(actor, run_id, reason=reason)
        return {"run_id": run_id, "status": "cancelled"}

    return _once(request, actor, key, cancel)


def _pause(
    request: Request, actor: Actor, key: str | None, fn: Callable[[], None], **answer: Any
) -> JSONResponse:
    def do() -> dict[str, Any]:
        fn()
        return answer

    return _once(request, actor, key, do)


@router.post("/flows/{flow}:pause")
def pause_flow(
    flow: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Reason | None = None,
) -> JSONResponse:
    reason = (body or Reason()).reason
    return _pause(
        request, actor, key, lambda: actions.pause_flow(actor, flow, reason=reason), flow=flow, paused=True
    )


@router.post("/flows/{flow}:resume")
def resume_flow(
    flow: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Reason | None = None,
) -> JSONResponse:
    reason = (body or Reason()).reason
    return _pause(
        request, actor, key, lambda: actions.resume_flow(actor, flow, reason=reason), flow=flow, paused=False
    )


@router.post("/routes/{route}:pause")
def pause_route(
    route: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Reason | None = None,
) -> JSONResponse:
    reason = (body or Reason()).reason
    return _pause(
        request,
        actor,
        key,
        lambda: actions.pause_route(actor, route, reason=reason),
        route=route,
        paused=True,
    )


@router.post("/routes/{route}:resume")
def resume_route(
    route: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Reason | None = None,
) -> JSONResponse:
    reason = (body or Reason()).reason
    return _pause(
        request,
        actor,
        key,
        lambda: actions.resume_route(actor, route, reason=reason),
        route=route,
        paused=False,
    )


# Partitions.


@router.get("/datasets/{dataset}/partitions")
def partitions(
    dataset: str, reads: See, _: CurrentActor, limit: Limit = 100, cursor: Cursor = None
) -> JSONResponse:
    grid = call(lambda: reads.partition_grid(dataset, cursor=decode(cursor), limit=check_limit(limit)))
    return ok({**page(grid.rows, grid.next_cursor), "routes": grid.routes})


@router.post("/datasets/{dataset}/partitions:rerun:preview")
def preview_rerun(dataset: str, body: RerunPreview, actions: Do, actor: CurrentActor) -> JSONResponse:
    return ok(_preview(call(lambda: actions.preview_rerun(actor, dataset, body.partitions))))


@router.post("/datasets/{dataset}/partitions:rerun", status_code=202)
def rerun(
    dataset: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Confirm | None = None,
) -> JSONResponse:
    token = _token(body, f"/datasets/{dataset}/partitions:rerun:preview")
    reason = body.reason if body else None
    return _once(request, actor, key, lambda: {"run_id": actions.rerun(actor, token, reason=reason)}, 202)


@router.post("/flows/{flow}:backfill:preview")
def preview_backfill(flow: str, body: BackfillPreview, actions: Do, actor: CurrentActor) -> JSONResponse:
    found = call(
        lambda: actions.preview_backfill(
            actor, flow, start=body.start, end=body.end, max_concurrent=body.max_concurrent
        )
    )
    return ok(_preview(found))


@router.post("/flows/{flow}:backfill", status_code=202)
def backfill(
    flow: str,
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Confirm | None = None,
) -> JSONResponse:
    token = _token(body, f"/flows/{flow}:backfill:preview")
    reason = body.reason if body else None
    return _once(request, actor, key, lambda: {"run_id": actions.backfill(actor, token, reason=reason)}, 202)


# Versions.


def _version_records(request: Request, version_id: str, limit: int, cursor: str | None) -> dict[str, Any]:
    reads: Reads = request.app.state.reads
    try:
        version, records, next_raw = reads.version_records(version_id, limit, cursor)
    except ValueError:
        raise ApiProblem(
            422, "Unprocessable Content", "this cursor is not one this server handed out"
        ) from None
    return {"version_id": version.version_id, "masked": False, **page(records, next_raw)}


@router.get("/versions/{version_id}")
def version(version_id: str, request: Request, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: request.app.state.feather.store.versions.get(version_id)))


@router.get("/versions/{version_id}/records")
def version_records(
    version_id: str, request: Request, _: CurrentActor, limit: Limit = 100, cursor: Cursor = None
) -> JSONResponse:
    """The version's records, a page at a time. Masking comes in M2: until then `masked` is false."""
    return ok(call(lambda: _version_records(request, version_id, check_limit(limit), decode(cursor))))


# Tracing a record.


@router.get("/datasets/{dataset}/records/{key:path}/trace")
def record_trace(dataset: str, key: str, request: Request, _: CurrentActor) -> JSONResponse:
    """What happened to one record, by its dataset key: each version that held it, oldest first, with what each route
    did with it. Only versions sealed since the key index came in are traced."""
    return ok(call(lambda: as_json(Tracer(request.app.state.feather).trace(dataset, key))))


# Deliveries.


@router.get("/routes/{route}/deliveries")
def deliveries(
    route: str,
    reads: See,
    _: CurrentActor,
    status: DeliveryStatus | None = None,
    partition: str | None = None,
    limit: Limit = 100,
    cursor: Cursor = None,
) -> JSONResponse:
    found = call(lambda: reads.deliveries(route, status, partition, check_limit(limit), decode(cursor)))
    return ok(page(found, delivery_cursor(found[-1]) if len(found) == limit else None))


def _deliveries_preview(
    action: DeliveryAction, body: DeliveriesPreview, actions: Actions, actor: Actor
) -> JSONResponse:
    selection = DeliverySelection(
        body.route,
        body.dataset,
        body.partition,
        body.error,
        body.error_class,
        None if body.version_ids is None else tuple(body.version_ids),
    )
    return ok(_preview(call(lambda: actions.preview_deliveries(actor, action, selection))))


def _apply(
    action: DeliveryAction,
    request: Request,
    actions: Actions,
    actor: Actor,
    key: str | None,
    body: Confirm | None,
) -> JSONResponse:
    token = _token(body, f"/deliveries:{action}:preview")
    reason = body.reason if body else None

    def apply() -> dict[str, Any]:
        if actions.tokens_action(token) != f"{action}_deliveries":
            raise ApiProblem(400, "Bad Request", f"this token doesn't confirm a {action}: preview a {action}")
        return {"moved": actions.apply_deliveries(actor, token, reason=reason)}

    return _once(request, actor, key, apply)


@router.post("/deliveries:retry:preview")
def preview_retry(body: DeliveriesPreview, actions: Do, actor: CurrentActor) -> JSONResponse:
    return _deliveries_preview("retry", body, actions, actor)


@router.post("/deliveries:skip:preview")
def preview_skip(body: DeliveriesPreview, actions: Do, actor: CurrentActor) -> JSONResponse:
    return _deliveries_preview("skip", body, actions, actor)


@router.post("/deliveries:replay:preview")
def preview_replay(body: DeliveriesPreview, actions: Do, actor: CurrentActor) -> JSONResponse:
    return _deliveries_preview("replay", body, actions, actor)


@router.post("/deliveries:retry")
def retry(
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Confirm | None = None,
) -> JSONResponse:
    return _apply("retry", request, actions, actor, key, body)


@router.post("/deliveries:skip")
def skip(
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Confirm | None = None,
) -> JSONResponse:
    return _apply("skip", request, actions, actor, key, body)


@router.post("/deliveries:replay")
def replay(
    request: Request,
    actions: Do,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Confirm | None = None,
) -> JSONResponse:
    return _apply("replay", request, actions, actor, key, body)


# Users, roles and tokens: admin only.


class NewUser(BaseModel):
    username: str
    password: str
    role: str = "operator"


class RoleBody(Reason):
    role: str


class NewToken(BaseModel):
    name: str
    role: str | None = None  # the caller's own when left out
    scope: str = "all"  # or publish: it may only upload packages to the built-in registry
    ttl_days: int | None = Field(90, ge=1, le=3650)  # None: it never expires


@router.get("/users")
def users(accounts: Manage, actor: CurrentActor) -> JSONResponse:
    return ok({"items": call(lambda: accounts.users(actor)), "next_cursor": None})


@router.post("/users", status_code=201)
def create_user(
    request: Request, accounts: Manage, actor: CurrentActor, body: NewUser, key: IdempotencyKey = None
) -> JSONResponse:
    return _once(
        request, actor, key, lambda: accounts.create_user(actor, body.username, body.password, body.role), 201
    )


@router.put("/users/{username}/role")
def set_role(
    username: str,
    request: Request,
    accounts: Manage,
    actor: CurrentActor,
    body: RoleBody,
    key: IdempotencyKey = None,
) -> JSONResponse:
    return _once(
        request, actor, key, lambda: accounts.set_role(actor, username, body.role, reason=body.reason)
    )


@router.post("/tokens", status_code=201)
def create_token(
    request: Request, accounts: Manage, actor: CurrentActor, body: NewToken, key: IdempotencyKey = None
) -> JSONResponse:
    ttl = None if body.ttl_days is None else timedelta(days=body.ttl_days)

    def create() -> dict[str, Any]:
        token = accounts.create_token(actor, body.name, ttl, body.role, scope=body.scope)
        return {"name": body.name, "token": token, "ttl_days": body.ttl_days, "scope": body.scope}

    return _once(request, actor, key, create, 201)


# Registries: the built-in one, `feather`, and the ones an admin adds (flow packages §4.3, §10).


class NewRegistry(BaseModel):
    name: str
    index_url: str
    token_ref: str | None = None  # a secret reference such as ${secret:env:ACME_REGISTRY_TOKEN}


@router.get("/registries")
def registries(trusted: Trusted, _: CurrentActor) -> JSONResponse:
    return ok({"items": trusted.registries(), "next_cursor": None})


@router.post("/registries", status_code=201)
def add_registry(
    request: Request, trusted: Trusted, actor: CurrentActor, body: NewRegistry, key: IdempotencyKey = None
) -> JSONResponse:
    return _once(
        request, actor, key, lambda: trusted.add(actor, body.name, body.index_url, body.token_ref), 201
    )


@router.delete("/registries/{name}")
def remove_registry(
    name: str, request: Request, trusted: Trusted, actor: CurrentActor, key: IdempotencyKey = None
) -> JSONResponse:
    def remove() -> dict[str, Any]:
        trusted.remove(actor, name)
        return {"registry": name, "removed": True}

    return _once(request, actor, key, remove)


@router.post("/registries/{name}:test")
def test_registry(name: str, trusted: Trusted, actor: CurrentActor) -> JSONResponse:
    return ok(call(lambda: trusted.test(actor, name)))


@router.get("/registries/{name}/packages")
def search_packages(
    name: str, trusted: Trusted, _: CurrentActor, q: Annotated[str, Query(max_length=200)] = ""
) -> JSONResponse:
    return ok({"items": call(lambda: trusted.search(name, q)), "next_cursor": None})


@router.post("/registries/feather/packages/{package}/versions/{version}:yank")
def yank_package(
    package: str,
    version: str,
    request: Request,
    trusted: Trusted,
    actor: CurrentActor,
    key: IdempotencyKey = None,
    body: Reason | None = None,
) -> JSONResponse:
    reason = (body or Reason()).reason

    def yank() -> dict[str, Any]:
        trusted.yank(actor, package, version, reason=reason)
        return {"package": package, "version": version, "yanked": True}

    return _once(request, actor, key, yank)


@router.get("/registries/{name}/packages/{package}/versions/{version}/wheel")
def fetch_wheel(name: str, package: str, version: str, installs: Installs, actor: CurrentActor) -> Response:
    """A version's wheel from a registry, its sha256 and manifest checked, for `feather package fetch`: an admin's, as
    an install is, and audited. It is sent in chunks."""
    found = call(lambda: installs.fetch(actor, name, package, version))
    content = found.content

    def chunks() -> Iterator[bytes]:
        for start in range(0, len(content), _CHUNK):
            yield content[start : start + _CHUNK]

    return StreamingResponse(
        chunks(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": attachment(found.filename),
            "Content-Length": str(len(content)),
            "X-Feather-Sha256": found.sha256,
        },
    )


_CHUNK = 256 * 1024


# Installed packages (flow packages §5, §10): installing needs an admin, and leaves every flow inactive.


class NewPackage(Reason):
    name: str = Field(max_length=200)
    version: str | None = Field(None, max_length=64)  # the registry's newest that isn't yanked when left out
    registry: str = Field("feather", max_length=64)
    allow_yanked: bool = False  # take a version the registry has yanked
    pre: bool = False  # with no version, a pre-release may be the newest


@router.post("/packages", status_code=201)
def install_package(
    request: Request, installs: Installs, actor: CurrentActor, body: NewPackage, key: IdempotencyKey = None
) -> JSONResponse:
    def install() -> Any:
        return installs.install(
            actor,
            body.name,
            body.version,
            registry=body.registry,
            reason=body.reason,
            allow_yanked=body.allow_yanked,
            pre=body.pre,
        )

    return _once(request, actor, key, install, 201)


@router.get("/packages")
def packages(installs: Installs, _: CurrentActor) -> JSONResponse:
    return ok({"items": installs.packages(), "next_cursor": None})


@router.get("/packages/{name}")
def package(name: str, installs: Installs, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: installs.package(name)))


@router.get("/packages/{name}/versions/{version}")
def package_version(name: str, version: str, installs: Installs, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: installs.version(name, version)))


# The environment's own configuration and secrets (flow packages §8): reading needs any role, changing an admin.


def _environment(request: Request) -> Environment:
    return request.app.state.environment


Envs = Annotated[Environment, Depends(_environment)]
Kind = Annotated[EnvKind, Path_(description="connection, budget or setting")]


def _entry(entry: EnvEntry) -> dict[str, Any]:
    return {
        "kind": entry.kind,
        "name": entry.name,
        "value": entry.value,
        "changed_by": entry.changed_by,
        "changed_at": entry.changed_at.isoformat(),
    }


class NewValue(Reason):
    value: Any


class Changes(Reason):
    changes: dict[str, Any]  # a None removes a setting; `auth.<field>` sets a secret field's reference


class Imported(Reason):
    entries: list[dict[str, Any]] = Field(max_length=1000)  # each {kind, name, value}


@router.get("/connections/{name}")
def connection(name: str, envs: Envs, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: envs.connection(name)))


@router.put("/connections/{name}")
def set_connection(
    request: Request, name: str, envs: Envs, actor: CurrentActor, body: NewValue, key: IdempotencyKey = None
) -> JSONResponse:
    def put() -> Any:
        envs.put(actor, "connection", name, body.value, reason=body.reason)
        return envs.connection(name)

    return _once(request, actor, key, put)


@router.patch("/connections/{name}")
def change_connection(
    request: Request, name: str, envs: Envs, actor: CurrentActor, body: Changes, key: IdempotencyKey = None
) -> JSONResponse:
    def change() -> Any:
        envs.update(actor, name, body.changes, reason=body.reason)
        return envs.connection(name)

    return _once(request, actor, key, change)


@router.delete("/connections/{name}")
def remove_connection(
    request: Request,
    name: str,
    envs: Envs,
    actor: CurrentActor,
    body: Reason | None = None,
    key: IdempotencyKey = None,
) -> JSONResponse:
    reason = (body or Reason()).reason

    def remove() -> Any:
        envs.remove(actor, "connection", name, reason=reason)
        return {"name": name, "removed": True}

    return _once(request, actor, key, remove)


@router.get("/environment")
def environment(envs: Envs, _: CurrentActor) -> JSONResponse:
    return ok({"env": envs.env, "items": [_entry(e) for e in envs.entries()], "next_cursor": None})


@router.put("/environment/{kind}/{name}")
def set_entry(
    request: Request,
    kind: Kind,
    name: str,
    envs: Envs,
    actor: CurrentActor,
    body: NewValue,
    key: IdempotencyKey = None,
) -> JSONResponse:
    return _once(
        request, actor, key, lambda: _entry(envs.put(actor, kind, name, body.value, reason=body.reason))
    )


@router.delete("/environment/{kind}/{name}")
def remove_entry(
    request: Request, kind: Kind, name: str, envs: Envs, actor: CurrentActor, key: IdempotencyKey = None
) -> JSONResponse:
    def remove() -> Any:
        envs.remove(actor, kind, name)
        return {"kind": kind, "name": name, "removed": True}

    return _once(request, actor, key, remove)


@router.post("/environment:import")
def import_entries(
    request: Request, envs: Envs, actor: CurrentActor, body: Imported, key: IdempotencyKey = None
) -> JSONResponse:
    def imported() -> Any:
        rows = [(str(e.get("kind")), str(e.get("name")), e.get("value")) for e in body.entries]
        done = envs.import_entries(actor, rows, reason=body.reason)
        return {"env": envs.env, "items": [_entry(e) for e in done]}

    return _once(request, actor, key, imported)


class NewSecret(Reason):
    value: str = Field(min_length=1, max_length=65536)


class NewKey(Reason):
    key: str = Field(min_length=1, max_length=200)  # the new master key, base64


@router.get("/secrets")
def secret_names(envs: Envs, _: CurrentActor) -> JSONResponse:
    """The names of the environment's secrets, when and by whom each was set: never a value."""
    return ok({"env": envs.env, "items": envs.secrets(), "next_cursor": None})


@router.put("/secrets/{name}")
def set_secret(request: Request, name: str, envs: Envs, actor: CurrentActor, body: NewSecret) -> JSONResponse:
    """Set or replace a secret. The answer never holds the value, and no Idempotency-Key keeps the request."""
    replaced = call(lambda: envs.set_secret(actor, name, body.value, reason=body.reason))
    return ok({"name": name, "replaced": replaced})


@router.delete("/secrets/{name}")
def delete_secret(
    request: Request, name: str, envs: Envs, actor: CurrentActor, key: IdempotencyKey = None
) -> JSONResponse:
    def remove() -> Any:
        envs.delete_secret(actor, name)
        return {"name": name, "deleted": True}

    return _once(request, actor, key, remove)


@router.post("/secrets:rotate-key")
def rotate_key(envs: Envs, actor: CurrentActor, body: NewKey) -> JSONResponse:
    """Seal every secret again under a new master key. Then set FEATHER_MASTER_KEY to it and restart the workers."""
    new = call(lambda: MasterKey.parse(body.key))
    count = call(lambda: envs.rotate_key(actor, new, reason=body.reason))
    return ok({"secrets": count, "key_id": new.key_id})


# Activating a package's flows (§6, the minimal step of task 5).


class Activation(Reason):
    version: str = Field(max_length=64)


@router.post("/flows/{flow:path}:activate")
def activate_flow(
    request: Request,
    flow: str,
    installs: Installs,
    actor: CurrentActor,
    body: Activation,
    key: IdempotencyKey = None,
) -> JSONResponse:
    return _once(
        request, actor, key, lambda: installs.activate(actor, flow, body.version, reason=body.reason)
    )


@router.post("/flows/{flow:path}:deactivate")
def deactivate_flow(
    request: Request,
    flow: str,
    installs: Installs,
    actor: CurrentActor,
    body: Reason | None = None,
    key: IdempotencyKey = None,
) -> JSONResponse:
    reason = (body or Reason()).reason
    return _once(request, actor, key, lambda: installs.deactivate(actor, flow, reason=reason))


# Budgets, connections and the summary.


@router.get("/budgets")
def budgets(reads: See, _: CurrentActor) -> JSONResponse:
    return ok({"items": reads.budgets(), "next_cursor": None})


@router.get("/budgets/{name}/usage")
def budget_usage(name: str, reads: See, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: reads.budget(name)))


@router.get("/connections")
def connections(reads: See, envs: Envs, _: CurrentActor) -> JSONResponse:
    """The connections the flows use, with their last check, and the environment's own, with their settings and
    whether each secret is set (`environment`)."""
    return ok(
        {
            "items": reads.connections(),
            "environment": envs.connections(),
            "env": envs.env,
            "next_cursor": None,
        }
    )


@router.post("/connections/{name}:check")
def check_connection(
    name: str, request: Request, actions: Do, actor: CurrentActor, key: IdempotencyKey = None
) -> JSONResponse:
    return _once(
        request, actor, key, lambda: {"connection": name, **vars(actions.check_connection(actor, name))}
    )


@router.get("/summary")
def summary(reads: See, _: CurrentActor) -> JSONResponse:
    found = reads.overview()
    return ok(
        {
            "deliveries": found.deliveries,
            "failed_deliveries": found.failed_deliveries,
            "open_quarantine": found.open_quarantine,
            "failed_runs": found.failed_runs,
            "running_runs": found.running_runs,
            "paused": found.paused,
            "stale_datasets": found.stale_datasets,
            "budgets": found.budgets,
            "workers": found.workers,
        }
    )
