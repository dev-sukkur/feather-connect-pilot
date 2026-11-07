"""The files `feather new` writes. Each function returns text, or a mapping of relative paths to text."""

from textwrap import dedent
from typing import Any

AGENTS_MD = """\
# Working on this project

This is a Feather Connect project: flows written as Python, run by a Feather server and its workers.
These rules hold for people and for AI assistants alike.

## Rules

- **Transforms are pure.** A map takes a record and a context and returns a record. It calls no API, reads no file
  and keeps no state, so a replay gives the same result.
- **Budgets attach to connections.** Put a `budget:` on the connection in `feather.yaml`, never a sleep or a counter
  in a flow.
- **Never put data in task arguments.** Records travel in versions on the spool. A task gets names and ids only.
- **Every flow needs a test.** `tests/test_<flow>.py` runs it on simulation data. Add one with each new flow.
- **Secrets are references.** Write `${secret:env:NAME}` in YAML, never a value. Nothing secret goes in git.
- **Environments are overlays.** `env/dev.yaml` and `env/qa.yaml` run on simulation stand-ins with `fixtures/`;
  `uat` and `prod` name the real systems.

## Commands

```bash
feather validate --json             # static checks, with a hint for each problem
feather describe connector <name>   # a connector's settings and capabilities
feather run <flow> --local --sim    # run a flow here, on simulation data
feather test                        # the project's tests, with the connectors' conformance kits
feather new flow <name> --template fan-out-demo
feather dev                         # a local server, worker and dashboard
```
"""


def project_files(name: str, ident: str) -> dict[str, str]:
    """A project with the layout of design §5.1 and one blank flow, `hello`, which runs on simulation data."""
    return {
        "feather.yaml": dedent(
            f"""\
            # The project: its name, the modules holding its flows, its settings and its connections.
            name: {name}
            modules: [flows.hello]
            settings: {{timezone: UTC}}
            connections:
              hello_source: {{connector: simulation.source}}
              hello_sink: {{connector: simulation.sink}}
            """
        ),
        "env/dev.yaml": "# dev: overlays on feather.yaml for local work. Simulation connectors need nothing here.\n",
        "env/qa.yaml": "# qa: overlays on feather.yaml for the shared test environment.\n",
        "env/uat.yaml": "# uat: overlays on feather.yaml for user acceptance, on the real systems.\n",
        "env/prod.yaml": "# prod: overlays on feather.yaml for production.\n",
        "flows/__init__.py": "",
        "flows/hello.py": blank_flow("hello", "hello"),
        "connectors/__init__.py": "",
        "fixtures/.gitkeep": "",
        "tests/test_hello.py": flow_test("hello", fixtures=False),
        "AGENTS.md": AGENTS_MD,
        "README.md": f"# {name}\n\nA Feather Connect project. See AGENTS.md for the rules and the commands.\n",
        ".gitignore": ".data/\n__pycache__/\n.venv/\n",
        "pyproject.toml": dedent(
            f"""\
            [project]
            name = "{name}"
            version = "0.1.0"
            requires-python = ">=3.12"
            dependencies = ["feather-connect"]

            [tool.pytest.ini_options]
            testpaths = ["tests"]
            """
        ),
    }


BLANK_FLOW_CONNECTIONS: dict[str, Any] = {}
BLANK_FLOW_ENV: dict[str, dict[str, Any]] = {}


def blank_flow(name: str, ident: str) -> str:
    """A flow that reads simulated records and delivers them, ready to be pointed at real connections."""
    return dedent(
        f'''\
        """The {name} flow. Replace the simulation connectors with real ones: `feather describe connectors` lists them."""

        import feather_connect as fc
        from feather_connect.connectors import simulation

        {ident} = fc.Dataset(
            "{ident}",
            source=simulation.Source("hello_source", generator={{"id": simulation.seq()}}, rows_per_partition=10),
            key="id",
            strategy=fc.strategy.replace(),
        )

        flow = fc.Flow(
            "{name}",
            reads={ident},
            deliver=[fc.to(simulation.Sink("hello_sink"))],
            triggers=[fc.manual()],
        )
        '''
    )


_FLOW_TEST = """\"\"\"The flows of flows/IDENT.py: the project validates, and they run on simulation data.\"\"\"

import importlib
import json
from pathlib import Path

import feather_connect as fc
from feather_connect.project import load_project, validate_project
from feather_connect.testing.harness import FlowHarness

ROOT = Path(__file__).resolve().parents[1]
ENVS = [p.stem for p in (ROOT / "env").glob("*.yaml")]


def _flows(env: str) -> list[fc.Flow]:
    project = load_project(ROOT, env)
    module = importlib.import_module("flows.IDENT")
    names = {v.name for v in vars(module).values() if isinstance(v, fc.Flow)}
    return [f for f in project.flows if f.name in names]


def _fixture(name: str) -> list[dict] | None:
    for suffix in (".json", ".jsonl"):
        path = ROOT / "fixtures" / f"{name}{suffix}"
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            return json.loads(text) if suffix == ".json" else [json.loads(line) for line in text.splitlines() if line]
    return None


def test_every_environment_validates():
    for env in ENVS:
        problems = [p for p in validate_project(load_project(ROOT, env)) if p.severity == "error"]
        assert problems == [], env


def test_the_flows_run_in_dev():
    flows = _flows("dev")
    assert flows
    for flow in flows:
        if any(d.strategy.name == "push" for d in flow.datasets):
            continue  # events arrive through the ingress: the template's own tests show how to test that
        harness = FlowHarness(flow)
        for dataset in flow.datasets:
            rows = _fixture(dataset.name)
            if rows is not None:
                harness.source(dataset.name).load(rows)
        harness.run()
        failed = harness.ledger.list_deliveries(status="failed")
        assert failed == [], [d.last_error for d in failed]
"""


def flow_test(ident: str, *, fixtures: bool = True) -> str:
    """A test that the project validates in each environment and that the flows of `flows/<ident>.py` run on
    simulation data in dev, with what `fixtures/` holds for their datasets."""
    return _FLOW_TEST.replace("IDENT", ident)


_CONNECTOR = """\"\"\"The NAME connector: a source and a sink. Replace the in-memory parts with calls to the real system.\"\"\"

from collections.abc import Iterator, Mapping
from typing import Any

from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyResult,
    CheckResult,
    ConnectorSpec,
    ReadRequest,
    StreamSchema,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError


class Source(connector.Source):
    \"\"\"Reads records from NAME. `read` yields lists of rows; replace `_rows` with the system's API.\"\"\"

    spec = ConnectorSpec(name="SPEC.source", strategies=frozenset({"replace", "snapshot_diff"}))

    def __init__(self, connection: str = "IDENT") -> None:
        self.connection = connection
        self._rows: list[dict[str, Any]] = []

    def load(self, rows: list[dict[str, Any]]) -> None:
        \"\"\"For tests: make `rows` all that the source holds.\"\"\"
        self._rows = [dict(row) for row in rows]

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True, message="nothing to check yet")

    def discover(self, ctx: Context) -> list[StreamSchema]:
        \"\"\"The streams the system offers, with their fields and types.\"\"\"
        fields: dict[str, str] = {}
        for row in self._rows:
            for name, value in row.items():
                if value is not None:
                    fields.setdefault(name, type(value).__name__)
        return [StreamSchema("rows", fields)]

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        rows = [row for row in self._rows if req.contains(row)]
        if rows:
            yield rows


class Sink(connector.Sink):
    \"\"\"Writes records to NAME. A repeat of a batch (the same idempotency key) must change nothing.\"\"\"

    spec = ConnectorSpec(name="SPEC.sink", modes=frozenset({"upsert"}))

    def __init__(self, connection: str = "IDENT") -> None:
        self.connection = connection
        self.rows: dict[Any, dict[str, Any]] = {}
        self._done: dict[str, ApplyResult] = {}

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True, message="nothing to check yet")

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        if batch.idempotency_key in self._done:
            return self._done[batch.idempotency_key]
        if batch.mode not in self.spec.modes:
            raise PermanentError(f"{self.spec.name} can't apply in mode {batch.mode!r}")
        for record in batch.records:
            if record.op == "delete":
                self.rows.pop(record.key, None)
            elif record.payload is not None:
                self.rows[record.key] = dict(record.payload)
        result = ApplyResult(applied=len(batch.records))
        self._done[batch.idempotency_key] = result
        return result

    def read_back(self) -> Mapping[Any, Mapping[str, Any]]:
        \"\"\"For tests: what the destination holds, by `id`.\"\"\"
        return {row["id"]: row for row in self.rows.values()}
"""

_CONNECTOR_TEST = """\"\"\"The conformance kit on the NAME connector. A failure names what the connector must do.\"\"\"

import pytest

from connectors.IDENT import Sink, Source
from feather_connect.testing.conformance import SinkConformance, SourceConformance


class TestSource(SourceConformance):
    @pytest.fixture
    def source(self):
        return Source()

    @pytest.fixture
    def seed(self, source):
        return source.load


class TestSink(SinkConformance):
    restart_safe = False  # set True, and give `fresh_sink`, once the destination itself keeps what was applied

    @pytest.fixture
    def sink(self):
        return Sink()

    @pytest.fixture
    def read_back(self, sink):
        return sink.read_back
"""


def connector_files(name: str, ident: str) -> dict[str, str]:
    """A project connector: a source and a sink of `name`, with the conformance kit's tests."""
    spec = ident

    def fill(text: str) -> str:
        return text.replace("NAME", name).replace("SPEC", spec).replace("IDENT", ident)

    return {
        "connectors/__init__.py": "",
        f"connectors/{ident}.py": fill(_CONNECTOR),
        f"tests/test_{ident}_connector.py": fill(_CONNECTOR_TEST),
    }
