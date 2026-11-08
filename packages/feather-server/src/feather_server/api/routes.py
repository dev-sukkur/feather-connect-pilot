"""The JSON API under `/api/v1` (design §9.2). Each route parses, calls the service layer and formats: the actions, their
checks, previews and audit rows are those of the dashboard.

A mass action takes two calls: `…:preview` returns the impact and a `confirmation_token`, and the action takes the
token. A state-changing call takes a `reason` in its body and an `Idempotency-Key` header: a repeat of the key by the
same caller within 24 hours gets the first answer again, and does nothing.
"""

import json
from collections.abc import Callable
from datetime import date, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from feather_connect.storage.port import DeliveryStatus, delivery_cursor
from feather_server.api.errors import ApiProblem, translate
from feather_server.api.paging import check_limit, decode, page
from feather_server.auth import Actor, CurrentActor, require_csrf
from feather_server.services.actions import Actions, DeliveryAction, DeliverySelection
from feather_server.services.previews import Preview
from feather_server.services.reads import Reads
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


Do = Annotated[Actions, Depends(_actions)]
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
    """Do `fn` once per idempotency key and caller, keeping its answer, success or problem, for 24 hours."""
    if key is None:
        return _answer(fn, status)
    if not key or len(key) > _MAX_KEY:
        raise ApiProblem(422, "Unprocessable Content", f"an Idempotency-Key has 1 to {_MAX_KEY} characters")
    overrides = request.app.state.feather.store.overrides
    target = f"{_IDEMPOTENCY}:{actor.kind}:{actor.name}"
    where = f"{request.method} {request.url.path}"
    kept = overrides.get(target, key)
    if kept is not None:
        if kept["request"] != where:
            raise ApiProblem(
                422, "Unprocessable Content", f"this Idempotency-Key was used for {kept['request']} already"
            )
        return JSONResponse(kept["body"], kept["status"], media_type=kept["media_type"])
    answer = _answer(fn, status)
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


# Budgets, connections and the summary.


@router.get("/budgets")
def budgets(reads: See, _: CurrentActor) -> JSONResponse:
    return ok({"items": reads.budgets(), "next_cursor": None})


@router.get("/budgets/{name}/usage")
def budget_usage(name: str, reads: See, _: CurrentActor) -> JSONResponse:
    return ok(call(lambda: reads.budget(name)))


@router.get("/connections")
def connections(reads: See, _: CurrentActor) -> JSONResponse:
    return ok({"items": reads.connections(), "next_cursor": None})


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
