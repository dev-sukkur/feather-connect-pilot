import subprocess
from datetime import timedelta
from pathlib import Path
from textwrap import dedent

import pytest

from feather_connect.core.triggers import Cron, Every
from feather_connect.project import (
    ProjectError,
    effective_config,
    load_project,
    resolve_secret,
    validate_project,
)

FLOW_MODULE = """
    import feather_connect as fc
    import feather_connect as fc
    from feather_connect.connectors import memory

    orders = fc.Dataset("orders", source=memory.Source("shop"), key="id", strategy=fc.strategy.replace())
    demo = fc.Flow(
        "demo",
        reads=orders,
        deliver=[fc.to(memory.Sink("warehouse")), fc.to(memory.Sink("gofrugal"))],
        triggers=[fc.cron("0 * * * *", tz="UTC")],
    )
"""

BASE = """
    name: acme
    modules: [flows.demo]
    settings: {timezone: Asia/Kolkata}
    connections:
      warehouse:
        connector: motherduck
        database: dev_bronze
        threads: 2
        budget: api
      shop:
        connector: shop.api
        site: hq
    budgets:
      api: {limits: ["90/minute", "24000/day"], reserve: {high: 0.6}}
"""

OVERLAYS = {
    "prod": """
        connections:
          warehouse:
            database: prod_bronze
            auth: {token: "${secret:env:TOKEN}"}
        flows:
          demo:
            triggers: {every: 15m}
            routes: {gofrugal: {enabled: false}}
    """,
    "tz": "flows: {demo: {triggers: {cron: '*/5 * * * *'}}}",
    "bad-secret": "connections: {bad: {connector: x, auth: {token: '${secret:nothere:a/b}'}}}",
    "vault": "connections: {bad: {connector: x, auth: {token: '${secret:vault:kv/a#b}'}}}",
    "plain": "connections: {bad: {connector: x, auth: {token: hunter2}}}",
    "elsewhere": "connections: {bad: {connector: x, url: '${secret:env:A}'}}",
}


def write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text), encoding="utf-8")


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    write(tmp_path, "feather.yaml", BASE)
    write(tmp_path, "flows/__init__.py", "")
    write(tmp_path, "flows/demo.py", FLOW_MODULE)
    for env, text in OVERLAYS.items():
        write(tmp_path, f"env/{env}.yaml", text)
    return tmp_path


def test_later_layers_win(project_dir):
    p = load_project(project_dir, "prod")
    assert p.connections["warehouse"].options["database"] == "prod_bronze"
    assert p.connections["warehouse"].options["threads"] == 2  # from feather.yaml
    assert p.connections["shop"].site == "hq" and p.connections["warehouse"].budget == "api"
    assert p.budgets["api"].reserve == {"high": 0.6} and len(p.budgets["api"].windows) == 2
    assert p.name == "acme" and p.settings["timezone"] == "Asia/Kolkata"


def test_effective_config_names_the_layer_and_hides_secrets(project_dir, monkeypatch):
    monkeypatch.setenv("TOKEN", "s3cret")
    vals = {v.path: v for v in effective_config(load_project(project_dir, "prod"))}
    assert vals["connections.warehouse.options.database"].layer == "env/prod.yaml"
    assert vals["connections.warehouse.options.threads"].layer == "feather.yaml"
    assert vals["connections.warehouse.auth.token"].value == "***"
    assert vals["connections.warehouse.budget"].value == "api"
    assert not any(v.value == "s3cret" for v in vals.values())


def test_a_setting_nobody_sets_comes_from_the_code(tmp_path):
    write(tmp_path, "feather.yaml", "name: p\nmodules: [flows.demo]\n")
    write(tmp_path, "flows/demo.py", FLOW_MODULE)
    write(tmp_path, "env/dev.yaml", "")
    vals = {v.path: v for v in effective_config(load_project(tmp_path, "dev"))}
    assert (vals["settings.timezone"].value, vals["settings.timezone"].layer) == ("UTC", "defaults")


def test_secret_providers(tmp_path, monkeypatch):
    monkeypatch.setenv("A", "x")
    (tmp_path / "s").write_text("y\n")
    assert resolve_secret("${secret:env:A}") == "x"
    assert resolve_secret(f"${{secret:file:{tmp_path / 's'}}}") == "y"


def test_a_relative_file_secret_is_read_against_the_project(tmp_path, monkeypatch):
    (tmp_path / "secrets").mkdir()
    (tmp_path / "secrets" / "token.txt").write_text("t0ken\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path.parent)
    assert resolve_secret("${secret:file:secrets/token.txt}", tmp_path) == "t0ken"


def test_a_connection_resolves_its_credentials_only_when_asked(project_dir, monkeypatch):
    monkeypatch.delenv("TOKEN", raising=False)
    p = load_project(project_dir, "prod")  # TOKEN is not set, and loading does not mind
    with pytest.raises(ProjectError, match="TOKEN"):
        p.connections["warehouse"].resolve()
    monkeypatch.setenv("TOKEN", "s3cret")
    assert p.connections["warehouse"].resolve() == {
        "database": "prod_bronze",
        "threads": 2,
        "token": "s3cret",
    }


def test_unknown_secret_provider_names_the_key(project_dir):  # Review Focus 5
    with pytest.raises(ProjectError, match=r"env/bad-secret.yaml.*connections\.bad.*nothere"):
        load_project(project_dir, "bad-secret")
    with pytest.raises(ProjectError, match=r"connections\.bad.*vault.*M3"):
        load_project(project_dir, "vault")


def test_a_plain_credential_or_a_secret_outside_auth_is_refused(project_dir):
    with pytest.raises(ProjectError, match=r"connections\.bad\.auth\.token.*reference") as plain:
        load_project(project_dir, "plain")
    assert "hunter2" not in str(plain.value)
    with pytest.raises(ProjectError, match=r"connections\.bad\.url.*only under a connection's auth"):
        load_project(project_dir, "elsewhere")


def test_missing_flow_module_names_the_file(tmp_path):  # Review Focus 5
    write(tmp_path, "feather.yaml", "name: p\nmodules: [flows.nope]\n")
    write(tmp_path, "env/dev.yaml", "")
    with pytest.raises(ProjectError, match=r"feather\.yaml.*modules.*flows\.nope"):
        load_project(tmp_path, "dev")


PUSH_FLOW = """
    import feather_connect as fc
    from feather_connect.connectors import memory, simulation

    hooks = fc.Dataset("hooks", source=SOURCE, key="id", strategy=fc.strategy.push())
    demo = fc.Flow("demo", reads=hooks, deliver=[fc.to(memory.Sink("dst"))])
"""
SIMULATED = 'simulation.Source("hooks", generator={"id": simulation.seq()}%s)'


def push_project(root: Path, source: str, connection: str) -> None:
    write(
        root,
        "feather.yaml",
        f"name: p\nmodules: [flows.demo]\nconnections:\n  dst: {{connector: memory.sink}}\n{connection}",
    )
    write(root, "env/dev.yaml", "")
    write(root, "flows/__init__.py", "")
    write(root, "flows/demo.py", PUSH_FLOW.replace("SOURCE", source))


SIGNED = "  hooks: {connector: simulation.source, auth: {signature_secret: '${secret:env:HOOK_SECRET}'}}\n"
BARE = "  hooks: {connector: simulation.source}\n"


def test_a_push_source_with_its_secret_configured_loads_without_resolving_it(tmp_path, monkeypatch):
    monkeypatch.delenv("HOOK_SECRET", raising=False)
    push_project(tmp_path, SIMULATED % "", SIGNED)
    project = load_project(tmp_path, "dev")
    assert project.connections["hooks"].secret_refs["signature_secret"] == "${secret:env:HOOK_SECRET}"
    assert validate_project(project) == []


def test_a_push_source_whose_connection_has_no_secret_is_refused(tmp_path):
    push_project(tmp_path, SIMULATED % "", BARE)
    with pytest.raises(ProjectError, match=r"dataset 'hooks'.*auth\.signature_secret.*unsigned=True"):
        load_project(tmp_path, "dev")


def test_a_push_source_on_an_undefined_connection_is_refused(tmp_path):
    push_project(tmp_path, SIMULATED % "", "")
    with pytest.raises(ProjectError, match=r"auth\.signature_secret of connection 'hooks'"):
        load_project(tmp_path, "dev")


def test_a_source_that_can_not_verify_is_refused_as_a_push_source(tmp_path):
    push_project(tmp_path, 'memory.Source("hooks")', "  hooks: {connector: memory.source}\n")
    with pytest.raises(ProjectError, match=r"no signature to verify them by"):
        load_project(tmp_path, "dev")


def test_a_push_source_marked_unsigned_loads(tmp_path):
    push_project(tmp_path, SIMULATED % ", unsigned=True", BARE)
    assert validate_project(load_project(tmp_path, "dev")) == []


def test_a_missing_overlay_and_a_bad_env_name_are_refused(project_dir):
    with pytest.raises(ProjectError, match=r"env/qa.yaml"):
        load_project(project_dir, "qa")
    with pytest.raises(ProjectError, match="environment name"):
        load_project(project_dir, "../x")


def test_secret_value_never_appears_in_errors_or_repr(project_dir, monkeypatch, tmp_path):
    monkeypatch.setenv("TOKEN", "s3cret")
    project = load_project(project_dir, "prod")
    assert "s3cret" not in repr(project) and "s3cret" not in repr(project.connections)
    assert "s3cret" not in repr(effective_config(project))
    secret_file = tmp_path / "missing" / "s3cret"
    with pytest.raises(ProjectError) as error:
        resolve_secret(f"${{secret:file:{secret_file}}}")
    assert "s3cret" not in str(error.value).replace(str(secret_file), "")


def test_overlay_can_disable_a_route_and_change_a_trigger(project_dir):
    flow = load_project(project_dir, "prod").flows[0]
    assert [r.name for r in flow.routes] == ["warehouse"]
    assert [type(t) for t in flow.triggers] == [Every]
    base = load_project(project_dir, "tz").flows[0]
    assert [r.name for r in base.routes] == ["warehouse", "gofrugal"]
    assert base.triggers == (Cron("*/5 * * * *", "Asia/Kolkata"),)  # the time zone comes from settings


def test_overlay_can_disable_a_whole_flow(project_dir):
    assert load_project(project_dir, "tz").flows[0].enabled
    write(project_dir, "env/off.yaml", "flows: {demo: {enabled: false}}")
    flow = load_project(project_dir, "off").flows[0]
    assert not flow.enabled and flow.spec_dict()["enabled"] is False
    write(project_dir, "env/off.yaml", "flows: {demo: {enabled: nope}}")
    with pytest.raises(ProjectError, match=r"env/off.yaml.*flows\.demo\.enabled"):
        load_project(project_dir, "off")


def test_an_overlay_for_an_unknown_flow_or_route_is_refused(project_dir):
    write(project_dir, "env/typo.yaml", "flows: {demo: {routes: {nothere: {enabled: false}}}}")
    with pytest.raises(ProjectError, match=r"env/typo.yaml.*flows\.demo\.routes\.nothere"):
        load_project(project_dir, "typo")
    write(project_dir, "env/typo.yaml", "flows: {demp: {triggers: {manual: true}}}")
    with pytest.raises(ProjectError, match=r"flows\.demp"):
        load_project(project_dir, "typo")


def test_loading_twice_builds_the_flows_again(project_dir):
    first = load_project(project_dir, "prod").flows[0]
    assert load_project(project_dir, "prod").flows[0] is not first


def test_code_sha_comes_from_env_then_git(project_dir, monkeypatch):
    monkeypatch.delenv("FEATHER_CODE_SHA", raising=False)
    assert load_project(project_dir, "prod").code_sha == "unknown"
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com"]
    subprocess.run([*git, "init", "-q"], cwd=project_dir, check=True)
    subprocess.run([*git, "add", "."], cwd=project_dir, check=True)
    subprocess.run([*git, "commit", "-qm", "x"], cwd=project_dir, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=project_dir, capture_output=True, text=True, check=True
    ).stdout.strip()
    assert load_project(project_dir, "prod").code_sha == head
    monkeypatch.setenv("FEATHER_CODE_SHA", "fromenv")
    assert load_project(project_dir, "prod").code_sha == "fromenv"


# B2: an environment that names simulation runs the project's own flows on stand-ins.

REAL_FLOW = """
    from typing import ClassVar

    import feather_connect as fc
    import feather_connect as fc
    from feather_connect.connectors import memory
    from feather_connect.core.connector import ConnectorSpec


    class AcmeSource(memory.Source):
        spec: ClassVar[ConnectorSpec] = ConnectorSpec(name="acme.source", strategies=frozenset({"replace"}))


    class AcmeSink(memory.Sink):
        spec: ClassVar[ConnectorSpec] = ConnectorSpec(name="acme.sink", modes=frozenset({"upsert"}))


    orders = fc.Dataset("orders", source=AcmeSource("shop"), key="id", strategy=fc.strategy.replace())
    demo = fc.Flow("demo", reads=orders, deliver=[fc.to(AcmeSink("warehouse"))], triggers=[fc.manual()])
"""


def simulated_project(root: Path, *, fixture: bool = True) -> None:
    write(
        root,
        "feather.yaml",
        """
        name: p
        modules: [flows.demo]
        connections:
          shop: {connector: acme.source, url: https://shop.example.com}
          warehouse: {connector: acme.sink}
        """,
    )
    write(root, "env/prod.yaml", "")
    write(
        root,
        "env/dev.yaml",
        "connections: {shop: {connector: simulation.source}, warehouse: {connector: simulation.sink}}",
    )
    write(root, "flows/__init__.py", "")
    write(root, "flows/demo.py", REAL_FLOW)
    if fixture:
        write(root, "fixtures/orders.json", '[{"id": 1, "total": 5}, {"id": 2, "total": 7}]')


def test_an_env_that_names_simulation_runs_the_flow_on_stand_ins(tmp_path):  # B2
    from feather_connect.core.connector import ReadRequest
    from feather_connect.core.partitions import none

    simulated_project(tmp_path)
    dev = load_project(tmp_path, "dev")
    (flow,) = dev.flows
    source, sink = flow.datasets[0].source, flow.routes[0].sink
    assert (source.spec.name, sink.spec.name) == (
        "acme.source",
        "acme.sink",
    )  # the real specs, checked as they run
    assert getattr(source, "stands_in", False) and getattr(sink, "stands_in", False)
    assert (source.connection, sink.connection) == ("shop", "warehouse")
    rows = [row for batch in source.read(None, ReadRequest("*", none())) for row in batch]  # pyright: ignore
    assert rows == [{"id": 1, "total": 5}, {"id": 2, "total": 7}]
    assert validate_project(dev) == []
    (real,) = load_project(tmp_path, "prod").flows
    assert not getattr(real.datasets[0].source, "stands_in", False)


def test_a_simulated_dataset_without_fixtures_is_refused(tmp_path):  # B2
    simulated_project(tmp_path, fixture=False)
    with pytest.raises(ProjectError, match=r"dataset 'orders'.*fixtures/orders\.json"):
        load_project(tmp_path, "dev")


def test_a_connection_s_window_is_read_and_checked(project_dir):  # M3.1 F3
    write(
        project_dir,
        "env/windowed.yaml",
        """
        connections:
          shop:
            windows: {tz: Asia/Kolkata, open: ["22:00-05:00"], applies: [read], max_overrun: 30m}
        """,
    )
    shop = load_project(project_dir, "windowed").connections["shop"]
    assert shop.windows is not None and shop.windows.applies == {"read"}
    assert "windows" not in shop.options  # the connector isn't given it
    write(project_dir, "env/badwindow.yaml", 'connections:\n  shop:\n    windows: {tz: UTC, open: ["9-5"]}\n')
    with pytest.raises(ProjectError, match=r"connections\.shop\.windows.*not a span"):
        load_project(project_dir, "badwindow")


def test_two_flows_with_different_routes_or_datasets_of_one_name_are_refused_by_validate():
    import feather_connect as fc
    from feather_connect.connectors import memory
    from feather_connect.project import Project

    def flow(name: str, dataset: str, route: str | None = None) -> fc.Flow:
        found = fc.Dataset(
            dataset, source=memory.Source(f"{name}_src", []), key="id", strategy=fc.strategy.replace()
        )
        return fc.Flow(name, reads=found, deliver=[fc.to(memory.Sink("sap"), name=route)])

    def codes(*flows: fc.Flow) -> list[tuple[str, str]]:
        project = Project("p", Path("."), "dev", flows, {}, {}, {}, "sha")
        shared = ("route_name_shared", "dataset_name_shared")
        return [(p.code, p.path) for p in validate_project(project) if p.code in shared]

    assert codes(flow("b2b", "b2b_inv"), flow("b2c", "b2c_inv")) == [
        ("route_name_shared", "b2c.deliver[0].name")
    ]
    assert codes(flow("b2b", "b2b_inv", "sap-b2b"), flow("b2c", "b2c_inv", "sap-b2c")) == []
    assert codes(flow("b2b", "inv", "a"), flow("b2c", "inv", "b")) == [
        ("dataset_name_shared", "b2c.reads[0]")
    ]
    one = flow("b2b", "inv", "a")
    assert (
        codes(one, fc.Flow("again", reads=one.datasets[0], deliver=[fc.to(memory.Sink("pos"))])) == []
    )  # shared


def test_an_environment_flow_entry_replaces_the_overlays_triggers_and_turns_a_flow_off(project_dir):
    from types import SimpleNamespace

    from feather_connect.project import environment_layer

    def entry(name: str, value: object) -> SimpleNamespace:
        return SimpleNamespace(kind="flow", name=name, value=value)

    rows = [
        entry("acme/demo", {"triggers": {"cron": "0 6 * * *"}}),
        entry("other/demo", {"enabled": False}),
        entry("acme/broken", {"enabled": "nope"}),
        entry("acme/gone", {"enabled": False}),
    ]
    assert "flows" not in environment_layer(rows)  # only a package host asks for its own flows
    layer = environment_layer(rows, flows_of="acme")
    assert set(layer["flows"]) == {"demo", "gone"}  # the bad one is left out
    flow = load_project(project_dir, "prod", environment=layer).flows[0]  # prod's overlay says every
    assert flow.triggers == (Cron("0 6 * * *", "Asia/Kolkata"),)  # replaced, in the project's time zone
    off = load_project(project_dir, "prod", environment={"flows": {"demo": {"enabled": False}}}).flows[0]
    assert not off.enabled and [type(t) for t in off.triggers] == [Every]


def test_a_flow_entry_is_checked():
    from feather_connect.project import flow_override

    assert flow_override({"enabled": False}) == (False, None)
    assert flow_override({"triggers": {"every": "15m"}})[1] == (Every(timedelta(minutes=15)),)
    for bad in ({}, {"enabled": "no"}, {"triggers": {"cron": "not a cron"}}, {"triggers": {}}, {"paused": True}):
        with pytest.raises(ProjectError):
            flow_override(bad)
