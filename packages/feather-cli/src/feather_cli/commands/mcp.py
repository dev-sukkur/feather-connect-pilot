"""`feather mcp`: Feather's read-only MCP server over stdio, for Claude Desktop, Claude Code or another MCP client
(M3.1 plan 1 A1).

With a server (`--server` and `--token`, or `FEATHER_SERVER` and `FEATHER_TOKEN`, or `server:` in env/<env>.yaml),
it serves the read tools, each calling that server's API with the token, which must be a viewer's. In a project
directory it also serves two tools that read the project on disk and need no server: `describe_connector` and
`validate_project`. Nothing it serves can change anything.
"""

from dataclasses import asdict
from pathlib import Path
from typing import Any

import httpx
from mcp.server.mcpserver import MCPServer

from feather_cli.commands.describe import _connector  # pyright: ignore[reportPrivateUsage]
from feather_cli.commands.remote import Server, Token, _from_env_file  # pyright: ignore[reportPrivateUsage]
from feather_cli.options import Env, ProjectDir
from feather_cli.output import fail
from feather_connect.core.flow import Problem
from feather_connect.core.registry import describe_connector
from feather_connect.project import ProjectError, load_project, validate_project
from feather_server.agent import INSTRUCTIONS, READ_ONLY, build_server, http_get


def mcp(
    project: ProjectDir = Path("."), env: Env = "dev", server: Server = None, token: Token = None
) -> None:
    """Serve Feather's read-only MCP tools over stdio. Give it a viewer token: feather server create-token claude
    --role viewer."""
    if server is None or token is None:
        try:
            file_url, file_token = _from_env_file(project, env)
        except ProjectError as error:
            fail(str(error), hint="set the secret, or pass --token", json_mode=False)
        token = token or file_token
        server = server or file_url
    local = (project / "feather.yaml").is_file()
    if server and not token:
        fail(
            "no token for the server",
            hint="pass a viewer token with --token or FEATHER_TOKEN",
            json_mode=False,
        )
    if not server and not local:
        fail(
            "nothing to serve: no server and no project here",
            hint="pass --server and --token, or run it in a project directory",
            json_mode=False,
        )
    build(project if local else None, env, server, token).run("stdio")


def build(project: Path | None, env: str, server: str | None, token: str | None) -> MCPServer:
    """The MCP server: the read tools when there is a server, and the local tools when there is a project."""
    if server and token:
        client = httpx.AsyncClient(base_url=server.rstrip("/"), timeout=30)
        found = build_server(http_get(client, token))
    else:
        found = MCPServer(name="feather-connect", instructions=INSTRUCTIONS)
    if project is not None:
        add_local_tools(found, project, env)
    return found


def add_local_tools(server: MCPServer, project: Path, env: str) -> None:
    """The author's tools, which read the project on disk and run nothing of it."""

    @server.tool(name="describe_connector", annotations=READ_ONLY)
    def describe_connector_tool(name: str) -> dict[str, Any]:
        """An installed connector's settings, as a JSON schema, and its capabilities: strategies, modes, deletes."""
        try:
            return _connector(describe_connector(name))
        except LookupError as error:
            return {"error": str(error)}

    @server.tool(name="validate_project", annotations=READ_ONLY)
    def validate_project_tool() -> dict[str, Any]:
        """`feather validate` on the project: each problem with its severity, path and a hint. Nothing is run."""
        try:
            problems = validate_project(load_project(project, env))
        except ProjectError as error:
            problems = [
                Problem("error", "project_invalid", str(error), "project", "fix the file the message names")
            ]
        return {
            "ok": not any(p.severity == "error" for p in problems),
            "env": env,
            "problems": [asdict(p) for p in problems],
        }
