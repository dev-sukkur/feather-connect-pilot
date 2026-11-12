"""Access control on every route (M3 plan 1 X2, design §13, OWASP ASVS V4): the routes are walked, not listed by
hand, so a route added later is checked too.

- Anonymous, every route is refused (401, 403 or a redirect to sign in) but the few that are public by design.
- A viewer changes nothing: every route that changes something, of the API (with a body that passes validation)
  and of the dashboard (with its CSRF token), is refused. Most answer 403 from the capability check itself; the
  others are refused before it, as unknown (404), invalid (422, 400) or a mass action without its preview's token,
  and a viewer never gets that token, since issuing one checks the capability (security review, finding X2-3).
- An operator can't do an admin's work: users, tokens, roles and grants, and registries answer 403.
"""

import re
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.testing import World, sign_in

CHANGES = {"POST", "PUT", "PATCH", "DELETE"}
# Public by design: signing in and out, the probes a load balancer calls (they say only up or not, and the database
# state), and ingress, which checks its webhook's own token or signature.
PUBLIC = {
    ("GET", "/login"),
    ("POST", "/login"),
    ("POST", "/logout"),
    ("GET", "/healthz"),
    ("GET", "/readyz"),
    ("POST", "/ingress/{tenant}/{source}"),
    ("POST", "/ingress/{tenant}/{package}/{webhook}"),
}
# What a viewer may send: signing in and out.
VIEWER_MAY = {("POST", "/logout"), ("POST", "/login")}
ADMIN_ONLY = {
    ("POST", "/api/v1/users"),
    ("PUT", "/api/v1/users/{username}/role"),
    ("PUT", "/api/v1/users/{username}/grants"),
    ("POST", "/api/v1/tokens"),
    ("POST", "/users/{username}/role"),
    ("POST", "/users/{username}/grants"),
}


def walk(routes: list[Any]) -> Iterator[APIRoute]:
    for route in routes:
        if hasattr(route, "original_router"):  # how FastAPI holds an included router
            yield from walk(route.original_router.routes)
        elif isinstance(route, APIRoute):
            yield route


def every(app: FastAPI) -> list[tuple[str, APIRoute]]:
    return [(m, r) for r in walk(app.routes) for m in sorted((r.methods or set()) - {"HEAD"})]


def concrete(path: str) -> str:
    """The route's path with a value for each parameter: one that its type accepts."""
    return re.sub(r"\{([^}:]+)(:[^}]+)?\}", lambda m: "connection" if m.group(1) == "kind" else "x", path)


def sample(schema: dict[str, Any], schemas: dict[str, Any]) -> Any:
    """A value for a JSON schema with every required field set, so that validation passes."""
    if "$ref" in schema:
        return sample(schemas[schema["$ref"].rsplit("/", 1)[-1]], schemas)
    for key in ("anyOf", "oneOf", "allOf"):
        if key in schema:
            options = [s for s in schema[key] if s.get("type") != "null"]
            return sample(options[0], schemas)
    if "enum" in schema:
        return schema["enum"][0]
    if "const" in schema:
        return schema["const"]
    kind = schema.get("type")
    if kind == "object" or "properties" in schema:
        props = schema.get("properties", {})
        return {
            k: sample(v, schemas)
            for k, v in props.items()
            if k in schema.get("required", []) or k == "reason"
        }
    if kind == "array":
        return [sample(schema["items"], schemas)] if schema.get("minItems") else []
    return {"string": "x", "integer": 1, "number": 1, "boolean": False}.get(str(kind), "x")


def api_request(app: FastAPI, method: str, route: APIRoute) -> dict[str, Any]:
    """The JSON body and query that the route's validation accepts."""
    spec = app.openapi()
    schemas = spec.get("components", {}).get("schemas", {})
    operation = spec["paths"].get(route.path_format, {}).get(method.lower(), {})
    body = operation.get("requestBody", {}).get("content", {}).get("application/json", {}).get("schema")
    query = {
        p["name"]: sample(p.get("schema", {}), schemas)
        for p in operation.get("parameters", [])
        if p.get("in") == "query" and p.get("required")
    }
    return {"json": sample(body, schemas) if body else {}, "params": query}


@pytest.fixture
def app(world: World) -> FastAPI:
    return create_app(state=world.state, loops=lambda s: [])


def test_the_walk_finds_the_api_and_the_dashboard(app: FastAPI) -> None:
    paths = {r.path for _, r in every(app)}
    assert len(paths) > 100 and "/api/v1/users" in paths and "/holds/decide" in paths


def test_anonymous_every_route_is_refused_but_the_public_ones(app: FastAPI) -> None:
    anonymous = TestClient(app, base_url="http://localhost:8000", follow_redirects=False)
    let_through = []
    for method, route in every(app):
        if (method, route.path) in PUBLIC:
            continue
        answer = anonymous.request(method, concrete(route.path), json={} if method in CHANGES else None)
        to_login = answer.status_code in (302, 303) and answer.headers.get("location", "").startswith(
            "/login"
        )
        if not (answer.status_code in (401, 403) or to_login):
            let_through.append((answer.status_code, method, route.path))
    assert not let_through, "; ".join(map(str, let_through))


def test_a_viewer_changes_nothing_through_the_api(app: FastAPI, world: World) -> None:
    viewer = TestClient(app, base_url="http://localhost:8000", follow_redirects=False)
    token = issue_token(world.store, "viewer-t", timedelta(days=1), role="viewer")
    answered, checked = [], []
    for method, route in every(app):
        if method not in CHANGES or not route.path.startswith("/api/") or (method, route.path) in VIEWER_MAY:
            continue
        sent = api_request(app, method, route)
        answer = viewer.request(
            method, concrete(route.path), headers={"Authorization": f"Bearer {token}"}, **sent
        )
        checked.append(answer.status_code)
        if not 400 <= answer.status_code < 500:
            answered.append((answer.status_code, method, route.path, answer.text[:120]))
    assert not answered, "; ".join(map(str, answered))
    assert checked.count(403) > len(checked) / 2, (checked.count(403), len(checked))  # the check refuses most


def test_a_viewer_changes_nothing_through_the_dashboard(app: FastAPI) -> None:
    browser = sign_in(app, "vic", "viewer")
    found = re.search(r'<meta name="csrf-token" content="([^"]+)"', browser.get("/").text)
    assert found
    answered, checked = [], []
    for method, route in every(app):
        path = route.path
        if method not in CHANGES or path.startswith(("/api/", "/ingress/")) or (method, path) in VIEWER_MAY:
            continue
        form = {"csrf_token": found.group(1), "reason": "a reason", "confirm": "yes"}
        answer = browser.request(method, concrete(path), data=form)
        checked.append(answer.status_code)
        if not 400 <= answer.status_code < 500:
            answered.append((answer.status_code, method, path))
    assert not answered, "; ".join(map(str, answered))
    assert checked.count(403) > 10, checked


def test_an_operator_cant_do_an_admins_work(app: FastAPI, world: World) -> None:
    operator = TestClient(app, base_url="http://localhost:8000", follow_redirects=False)
    token = issue_token(world.store, "operator-t", timedelta(days=1), role="operator")
    browser = sign_in(app, "opal", "operator")
    found = re.search(r'<meta name="csrf-token" content="([^"]+)"', browser.get("/").text)
    assert found
    by_path = {(m, r.path): r for m, r in every(app)}
    answered = []
    for method, path in sorted(ADMIN_ONLY):
        route = by_path[(method, path)]
        if path.startswith("/api/"):
            sent = api_request(app, method, route)
            answer = operator.request(
                method, concrete(path), headers={"Authorization": f"Bearer {token}"}, **sent
            )
        else:
            form = {"csrf_token": found.group(1), "reason": "a reason", "role": "admin", "grants": "approver"}
            answer = browser.request(method, concrete(path), data=form)
        if answer.status_code != 403:
            answered.append((answer.status_code, method, path))
    assert not answered, "; ".join(map(str, answered))
