"""`feather mcp` (M3.1 plan 1 A1): an MCP client over stdio sees the local author tools in a project, and the read
tools when it has a server; nothing it serves changes anything."""

import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from feather_cli.commands.mcp import add_local_tools
from feather_connect.clock import FrozenClock
from feather_server.agent import build_server, http_get
from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.testing import build_world


def test_a_client_over_stdio_validates_the_project(project: Path):
    async def session() -> tuple[set[str], dict[str, Any]]:
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "feather_cli.main", "mcp", "--project", str(project)],
            env={**os.environ, "FEATHER_SERVER": "", "FEATHER_TOKEN": ""},
        )
        async with stdio_client(params) as (read, write), ClientSession(read, write) as client:
            await client.initialize()
            tools = {t.name for t in (await client.list_tools()).tools}
            result = await client.call_tool("validate_project", {})
            return tools, json.loads(result.content[0].text)  # pyright: ignore[reportAttributeAccessIssue]

    tools, validated = asyncio.run(asyncio.wait_for(session(), 60))
    assert tools == {"describe_connector", "validate_project"}  # no server given: the local tools only
    assert validated["ok"] is True


def test_with_a_server_it_reads_through_the_api_as_the_viewer(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("WH_TOKEN", "x")
    world = build_world(tmp_path / "server", FrozenClock(datetime(2026, 10, 3, 12, 0, tzinfo=UTC)))
    world.run()
    token = issue_token(world.store, "claude", timedelta(days=1), role="viewer")
    app = create_app(state=world.state, loops=lambda s: [])

    async def flows() -> Any:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost:8000") as client:
            server = build_server(http_get(client, token))
            add_local_tools(server, tmp_path / "server", "dev")
            names = {t.name for t in await server.list_tools()}
            result = await server.call_tool("list_flows", {})
            return names, json.loads(result.content[0].text)  # pyright: ignore[reportAttributeAccessIssue]

    names, found = asyncio.run(flows())
    assert {"list_flows", "describe_connector", "validate_project"} <= names
    assert [f["name"] for f in found["items"]] == ["orders"]
    [row] = [a for a in world.store.audit.list_audit() if a.actor_type == "agent"]
    assert row.action == "mcp:list_flows"
