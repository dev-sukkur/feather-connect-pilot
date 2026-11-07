"""The helper processes a project declares for `feather dev` under `dev.processes`: read by the loader, started in the
project directory on feather's interpreter with their output prefixed, refused early when a port is busy, and
stopped. No Docker and no stack: the parts are tested on their own."""

import socket
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from feather_cli.commands.helpers import HelperError, Helpers
from feather_cli.main import app
from feather_connect.project import DevProcess, ProjectError, load_project

ROOT = Path(__file__).resolve().parents[2]
SERVE = """import http.server, os, sys
print("serving from", os.path.basename(os.getcwd()), "on", sys.executable, flush=True)
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), http.server.SimpleHTTPRequestHandler).serve_forever()
"""


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def project(root: Path, dev: str = "") -> Path:
    (root / "feather.yaml").write_text(f"name: helpers\nmodules: []\n{dev}", encoding="utf-8")
    (root / "env").mkdir(exist_ok=True)
    (root / "env" / "dev.yaml").write_text("", encoding="utf-8")
    return root


def test_the_loader_reads_dev_processes(tmp_path: Path):
    loaded = load_project(
        project(
            tmp_path,
            'dev:\n  processes:\n    - {name: api, command: [python, sim.py, --port, "9"], port: 9}\n',
        ),
        "dev",
    )
    assert loaded.dev_processes == (DevProcess("api", ("python", "sim.py", "--port", "9"), 9),)
    assert load_project(project(tmp_path), "dev").dev_processes == ()


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ("{command: [python]}", "name: is required"),
        ("{name: a, command: []}", "command: give a list of words"),
        ("{name: a, command: [python], port: 0}", "port: must be a port number"),
        ("{name: a, command: [python], cwd: x}", "unknown key 'cwd'"),
    ],
)
def test_the_loader_says_what_is_wrong_with_a_process(tmp_path: Path, entry: str, message: str):
    with pytest.raises(ProjectError, match=message):
        load_project(project(tmp_path, f"dev:\n  processes:\n    - {entry}\n"), "dev")


def test_validate_accepts_the_zakya_demo_with_its_processes():
    result = CliRunner().invoke(app, ["validate", "--project", str(ROOT / "examples" / "zakya-demo")])
    assert result.exit_code == 0, result.output
    assert len(load_project(ROOT / "examples" / "zakya-demo", "dev").dev_processes) == 2


def test_a_helper_runs_in_the_project_on_this_interpreter_and_stops(tmp_path: Path):
    lines: list[str] = []
    (tmp_path / "serve.py").write_text(SERVE, encoding="utf-8")
    port = free_port()
    helpers = Helpers(tmp_path, lines.append)
    helpers.start([DevProcess("web", ("python", "serve.py", str(port)), port)])
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            pass
    finally:
        helpers.stop()
    assert f"[web] serving from {tmp_path.name} on {sys.executable}" in lines
    assert lines[-1] == "[web] stopped"
    with pytest.raises(OSError), socket.create_connection(("127.0.0.1", port), timeout=1):
        pass


def test_a_helper_that_ends_before_it_listens_is_an_error(tmp_path: Path):
    helpers = Helpers(tmp_path, print)
    with pytest.raises(HelperError, match="ended with exit code 3"):
        helpers.start([DevProcess("bad", ("python", "-c", "raise SystemExit(3)"), free_port())])


def test_feather_dev_refuses_a_busy_helper_port_before_starting_anything(tmp_path: Path):
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        root = project(
            tmp_path, f"dev:\n  processes:\n    - {{name: sim, command: [python, x.py], port: {port}}}\n"
        )
        result = CliRunner().invoke(
            app, ["dev", "--project", str(root), "--port", str(free_port()), "--no-browser"]
        )
    assert result.exit_code == 1
    assert f"port {port} of the helper process 'sim' is already in use" in result.output
