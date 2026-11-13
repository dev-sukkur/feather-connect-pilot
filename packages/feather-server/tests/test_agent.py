"""The read-only MCP server (M3.1 plan 1 A1): only read tools, only a viewer token, no PII grants, audited, rate
limited, and what comes from records and logs wrapped as untrusted."""

import asyncio
import json
import re
from collections.abc import Iterator, Mapping
from datetime import timedelta
from typing import Any

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from feather_connect.storage.port import Role
from feather_server.agent import UNTRUSTED_NOTE, build_server
from feather_server.api.routes import router
from feather_server.app import create_app
from feather_server.auth import AGENT_HEADER, hash_password, issue_token
from feather_server.testing import PASSWORD, World

TOOLS = {
    "list_flows",
    "describe_flow",
    "list_runs",
    "get_run",
    "get_task_logs",
    "list_failures",
    "list_alerts",
    "list_holds",
    "partition_status",
    "trace_record",
    "version_sample",
    "budget_usage",
    "list_connections",
    "overview",
}
MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def token(world: World, role: Role, user: str | None = None) -> str:
    return issue_token(world.store, f"claude-{role}", timedelta(days=1), role=role, created_by=user)


@pytest.fixture
def app(world: World) -> Iterator[TestClient]:
    with TestClient(
        create_app(state=world.state, loops=lambda s: []), base_url="http://localhost:8000"
    ) as client:
        yield client


def rpc(client: TestClient, bearer: str | None, method: str, params: Mapping[str, Any] | None = None) -> Any:
    headers = {**MCP_HEADERS, **({"Authorization": f"Bearer {bearer}"} if bearer else {})}
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": dict(params or {})}
    return client.post("/mcp", json=body, headers=headers)


def call(client: TestClient, bearer: str, tool: str, **arguments: Any) -> Any:
    answer = rpc(client, bearer, "tools/call", {"name": tool, "arguments": arguments})
    assert answer.status_code == 200, answer.text
    result = answer.json()["result"]
    assert not result.get("isError"), result
    return json.loads(result["content"][0]["text"])


# The tools.


def recorded() -> tuple[list[tuple[str, str]], Any]:
    seen: list[tuple[str, str]] = []

    async def get(tool: str, path: str, params: Mapping[str, Any]) -> Any:
        seen.append((tool, path))
        return {"items": []}

    return seen, build_server(get)


def test_the_tools_are_the_read_tools_and_each_says_it_only_reads():
    _, server = recorded()
    tools = asyncio.run(server.list_tools())
    assert {t.name for t in tools} == TOOLS
    assert all(
        t.annotations and t.annotations.read_only_hint and not t.annotations.destructive_hint for t in tools
    )


def test_every_path_a_tool_calls_is_a_get_route_of_the_api():
    seen, server = recorded()
    arguments = {
        "describe_flow": {"flow": "orders"},
        "get_run": {"run_id": "r1"},
        "get_task_logs": {"task_id": "t1"},
        "partition_status": {"dataset": "pkg/orders"},
        "trace_record": {"dataset": "orders", "key": "A/1"},
        "version_sample": {"version_id": "v1"},
        "budget_usage": {"name": "api"},
    }
    for name in TOOLS:
        asyncio.run(server.call_tool(name, arguments.get(name, {})))
    gets = [
        re.compile("^" + re.sub(r"\{[^}]+:path\}", ".+", re.sub(r"\{[^}:]+\}", "[^/]+", r.path)) + "$")
        for r in router.routes
        if isinstance(r, APIRoute) and r.methods == {"GET"}
    ]
    assert {tool for tool, _ in seen} == TOOLS
    for tool, path in seen:
        assert any(g.match("/api/v1" + path) for g in gets), (tool, path)


# /mcp.


def test_a_viewer_token_lists_and_calls_the_tools(app: TestClient, world: World):
    world.run()
    viewer = token(world, "viewer")
    listed = rpc(app, viewer, "tools/list")
    assert listed.status_code == 200, listed.text
    assert {t["name"] for t in listed.json()["result"]["tools"]} == TOOLS
    assert "orders" in str(call(app, viewer, "list_runs"))


@pytest.mark.parametrize("role", ["operator", "developer", "admin"])
def test_a_token_that_could_change_things_is_refused(app: TestClient, world: World, role: Role):
    answer = rpc(app, token(world, role), "tools/list")
    assert answer.status_code == 403
    assert "viewer token" in answer.json()["detail"]


def test_no_token_no_mcp(app: TestClient):
    assert rpc(app, None, "tools/list").status_code == 401


def test_each_call_is_audited_as_the_agent_s(app: TestClient, world: World):
    viewer = token(world, "viewer")
    call(app, viewer, "list_flows")
    [row] = [a for a in world.store.audit.list_audit() if a.actor_type == "agent"]
    assert (row.actor, row.action, row.request["path"]) == (
        "claude-viewer",
        "mcp:list_flows",
        "/api/v1/flows",
    )


def test_logs_come_wrapped_as_untrusted(app: TestClient, world: World):
    run_id = world.run()
    task = world.store.runs.list_tasks(run_id)[0]
    result = call(app, token(world, "viewer"), "get_task_logs", task_id=task.task_id)
    assert result["untrusted_content_note"] == UNTRUSTED_NOTE


def test_lists_are_capped(app: TestClient, world: World, monkeypatch: pytest.MonkeyPatch):
    for _ in range(3):
        world.run()
    monkeypatch.setenv("FEATHER_MCP_MAX_ITEMS", "1")
    found = call(app, token(world, "viewer"), "list_runs")
    assert len(found["items"]) == 1


# The API's side, which `feather mcp` over stdio relies on.


def test_the_api_refuses_an_agent_call_with_more_than_a_viewer_token(app: TestClient, world: World):
    operator = token(world, "operator")
    answer = app.get(
        "/api/v1/flows", headers={"Authorization": f"Bearer {operator}", AGENT_HEADER: "list_flows"}
    )
    assert answer.status_code == 403 and "viewer token" in answer.json()["detail"]


def test_an_agent_has_no_pii_grant_even_when_its_user_has_one(app: TestClient, world: World):
    world.store.users.create_user("vi", hash_password(PASSWORD), "viewer")
    world.store.users.set_grants("vi", ["pii-reader"])
    viewer = token(world, "viewer", "vi")
    plain = app.get("/api/v1/me", headers={"Authorization": f"Bearer {viewer}"})
    assert plain.status_code == 200
    answer = app.get(
        "/api/v1/versions/none/records",
        params={"unmask": "true"},
        headers={"Authorization": f"Bearer {viewer}", AGENT_HEADER: "version_sample"},
    )
    assert answer.status_code in (403, 404)
    if answer.status_code == 403:
        assert "pii" in answer.json()["detail"].lower()


def test_an_agent_is_rate_limited(app: TestClient, world: World, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FEATHER_MCP_RATE_PER_MIN", "2")
    viewer = token(world, "viewer")
    headers = {"Authorization": f"Bearer {viewer}", AGENT_HEADER: "list_flows"}
    assert [app.get("/api/v1/flows", headers=headers).status_code for _ in range(3)] == [200, 200, 429]


def test_an_agent_only_reads(app: TestClient, world: World):
    viewer = token(world, "viewer")
    answer = app.post(
        "/api/v1/flows/orders:run",
        json={},
        headers={"Authorization": f"Bearer {viewer}", AGENT_HEADER: "list_flows"},
    )
    assert answer.status_code == 403
