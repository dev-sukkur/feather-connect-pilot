"""`feather package build`, `publish` and `search`, and `feather registry` (flow packages §4 and §10)."""

import json
import shutil
from pathlib import Path
from textwrap import dedent

import pytest

SAMPLE = Path(__file__).resolve().parents[3] / "examples" / "packages" / "acme-invoices"


@pytest.fixture
def sample(tmp_path: Path) -> Path:
    root = tmp_path / "acme-invoices"
    shutil.copytree(SAMPLE, root, ignore=shutil.ignore_patterns("dist", "__pycache__"))
    return root


def test_build_writes_the_same_wheel_twice(cli, sample: Path, tmp_path: Path):
    first = cli("package", "build", "--project", str(sample), "--json")
    second = cli("package", "build", "--project", str(sample), "--out", str(tmp_path / "again"), "--json")
    assert first.exit_code == second.exit_code == 0, first.output
    one, two = json.loads(first.stdout), json.loads(second.stdout)
    assert one["sha256"] == two["sha256"] and one["flows"] == ["invoices"]
    assert Path(one["wheel"]) == sample / "dist" / "acme_invoices-1.0.0-py3-none-any.whl"


def test_build_explains_a_project_without_a_version(cli, sample: Path):
    path = sample / "feather.yaml"
    path.write_text(path.read_text(encoding="utf-8").replace("version: 1.0.0\n", ""), encoding="utf-8")
    r = cli("package", "build", "--project", str(sample))
    assert r.exit_code == 1 and "version: is required" in r.stderr


# Against a running server, on the in-memory store.


@pytest.fixture
def server():
    from datetime import UTC, datetime, timedelta

    from feather_connect.clock import FrozenClock
    from feather_connect.engine.local import LocalEngine
    from feather_connect.project import Project
    from feather_connect.runtime.runtime import Runtime
    from feather_connect.storage.memory import MemoryStore
    from feather_connect.storage.spool import MemorySpool
    from feather_server.app import AppState, create_app
    from feather_server.auth import issue_token
    from feather_server.settings import Settings
    from feather_server.testing import serve

    clock = FrozenClock(datetime(2026, 11, 9, tzinfo=UTC))
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    runtime = Runtime([], store, spool, engine, code_sha="sha", clock=clock)
    project = Project("p", Path("."), "dev", (), {}, {}, {"timezone": "UTC"}, "sha")
    from feather_connect.vault import MasterKey, Vault, new_master_key

    key = MasterKey.parse(new_master_key())
    state = AppState(
        Settings(secret_key="k"),
        project,
        store,
        spool,
        engine,
        runtime,
        clock=clock,
        vault=Vault(store.secrets, "dev", key),
    )
    tokens = {
        role: issue_token(store, role, timedelta(days=1), role="admin" if role == "admin" else "operator",
                          scope="publish" if role == "publish" else "all", clock=clock)
        for role in ("admin", "operator", "publish")
    }  # fmt: skip
    with serve(create_app(state=state, loops=lambda s: [])) as base:
        yield base, tokens


def test_publish_uploads_the_built_wheel_once_and_search_finds_it(cli, sample: Path, server):
    base, tokens = server
    assert cli("package", "build", "--project", str(sample)).exit_code == 0
    args = ("package", "publish", "--project", str(sample), "--url", base, "--token", tokens["publish"])
    first = cli(*args, "--json")
    assert first.exit_code == 0, first.output
    assert json.loads(first.stdout)["published"][0]["wheel"] == "acme_invoices-1.0.0-py3-none-any.whl"
    again = cli(*args)
    assert again.exit_code == 1 and "never replaced" in again.stderr and "raise version:" in again.stderr
    refused = cli(
        "package", "publish", "--project", str(sample), "--url", base, "--token", tokens["operator"]
    )
    assert refused.exit_code == 1 and "publish scope or an admin" in refused.stderr
    found = cli("package", "search", "invoices", "--server", base, "--token", tokens["operator"], "--json")
    assert json.loads(found.stdout)["items"] == [
        {"name": "acme-invoices", "versions": ["1.0.0"], "yanked": []}
    ]
    human = cli("package", "search", "--server", base, "--token", tokens["operator"])
    assert "acme-invoices: 1.0.0" in human.stdout


def test_publish_without_a_built_wheel_says_to_build(cli, sample: Path, server):
    base, tokens = server
    r = cli("package", "publish", "--project", str(sample), "--url", base, "--token", tokens["publish"])
    assert r.exit_code == 1 and "feather package build" in r.stderr


def test_registry_add_list_test_and_remove(cli, sample: Path, server, monkeypatch: pytest.MonkeyPatch):
    from feather_server.testing import sample_wheel, static_index

    base, tokens = server
    wheel = sample_wheel(sample / "built")
    monkeypatch.setenv("ACME_REGISTRY_TOKEN", "company-secret")
    admin = ("--server", base)
    with static_index({"acme-invoices": {wheel.name: wheel.read_bytes()}}, "company-secret") as url:
        ref = "${secret:env:ACME_REGISTRY_TOKEN}"
        added = cli(
            "registry", "add", "acme", url, "--token", ref, *admin, "--api-token", tokens["admin"], "--json"
        )
        assert added.exit_code == 0, added.output
        assert json.loads(added.stdout)["token_ref"] == ref
        denied = cli("registry", "add", "other", url, *admin, "--api-token", tokens["operator"])
        assert denied.exit_code == 1 and "403" in denied.stderr
        listed = cli("registry", "list", *admin, "--token", tokens["operator"])
        assert "feather:" in listed.stdout and f"acme: {url} (token {ref})" in listed.stdout
        tested = cli("registry", "test", "acme", *admin, "--token", tokens["admin"])
        assert tested.exit_code == 0 and "acme: ok, 1 project(s)" in tested.stdout
        found = cli(
            "package", "search", "--registry", "acme", *admin, "--token", tokens["operator"], "--json"
        )
        assert json.loads(found.stdout)["items"][0]["versions"] == ["1.0.0"]
        monkeypatch.setenv("ACME_REGISTRY_TOKEN", "wrong")
        failing = cli("registry", "test", "acme", *admin, "--token", tokens["admin"])
        assert failing.exit_code == 1 and "refused the credentials" in failing.stderr
    removed = cli("registry", "remove", "acme", *admin, "--token", tokens["admin"])
    assert removed.exit_code == 0 and "removed registry acme" in removed.stdout


def test_install_list_show_and_fetch(cli, sample: Path, server, tmp_path: Path):
    import hashlib

    base, tokens = server
    assert cli("package", "build", "--project", str(sample)).exit_code == 0
    published = cli(
        "package", "publish", "--project", str(sample), "--url", base, "--token", tokens["publish"]
    )
    assert published.exit_code == 0, published.output
    admin, operator = (
        ("--server", base, "--token", tokens["admin"]),
        ("--server", base, "--token", tokens["operator"]),
    )
    denied = cli("package", "install", "acme-invoices==1.0.0", *operator)
    assert denied.exit_code == 1 and "403" in denied.stderr
    installed = cli("package", "install", "acme-invoices==1.0.0", "--reason", "first", *admin)
    assert installed.exit_code == 0, installed.output
    assert "installed acme-invoices 1.0.0 from feather" in installed.stdout
    assert ": inactive" in installed.stdout and "activate a flow to run it" in installed.stdout
    again = cli("package", "install", "acme-invoices==1.0", *admin)
    assert again.exit_code == 1 and "install a new version" in again.stderr
    missing = cli("package", "install", "acme-invoices==9.9.9", *admin)
    assert missing.exit_code == 1 and "feather package search" in missing.stderr
    assert cli("package", "install", "==1.0", *admin).exit_code != 0
    listed = cli("package", "list", *operator)
    assert "acme-invoices: 1.0.0 (installed); 0 flows active" in listed.stdout
    shown = json.loads(cli("package", "show", "acme-invoices==1.0.0", *operator, "--json").stdout)
    assert shown["load_check"]["ok"] and {f["state"] for f in shown["flows"]} == {"inactive"}
    assert "acme-invoices/" in cli("package", "show", "acme-invoices", *operator).stdout
    assert cli("package", "show", "nothing", *operator).exit_code == 1
    denied = cli("package", "fetch", "acme-invoices==1.0.0", "--out", str(tmp_path / "no"), *operator)
    assert denied.exit_code == 1 and "403" in denied.stderr  # a fetch is an admin's, as an install is
    fetched = cli(
        "package", "fetch", "acme-invoices==1.0.0", "--out", str(tmp_path / "got"), *admin, "--json"
    )
    assert fetched.exit_code == 0, fetched.output
    doc = json.loads(fetched.stdout)
    wheel = Path(doc["wheel"])
    assert wheel.name == "acme_invoices-1.0.0-py3-none-any.whl"
    assert hashlib.sha256(wheel.read_bytes()).hexdigest() == doc["sha256"]
    assert cli("package", "fetch", "acme-invoices", *admin).exit_code == 1  # a version is needed


def test_activate_and_deactivate_a_flow_of_an_installed_version(cli, sample: Path, server):
    base, tokens = server
    assert cli("package", "build", "--project", str(sample)).exit_code == 0
    publish = ("--project", str(sample), "--url", base, "--token", tokens["publish"])
    assert cli("package", "publish", *publish).exit_code == 0
    admin, operator = (
        ("--server", base, "--token", tokens["admin"]),
        ("--server", base, "--token", tokens["operator"]),
    )
    assert cli("package", "install", "acme-invoices==1.0.0", *admin).exit_code == 0
    flow = "acme-invoices/invoices"
    done = cli("flow", "activate", flow, "--version", "1.0.0", "--reason", "go live", *operator)
    assert done.exit_code == 0, done.output
    assert f"{flow} 1.0.0: active" in done.stdout
    assert "1 flows active" in cli("package", "list", *operator).stdout
    missing = cli("flow", "activate", flow, "--version", "9.9.9", *operator)
    assert missing.exit_code == 1 and "not installed" in missing.stderr
    off = cli("flow", "deactivate", flow, *operator, "--json")
    assert off.exit_code == 0 and json.loads(off.stdout)["state"] == "inactive"


def test_connections_and_secrets_of_the_environment(cli, server, tmp_path: Path):
    base, tokens = server
    admin, operator = (
        ("--server", base, "--token", tokens["admin"]),
        ("--server", base, "--token", tokens["operator"]),
    )
    env_file = tmp_path / "prod.yaml"
    env_file.write_text(
        dedent("""
            connections:
              zakya:
                connector: zakya
                organization_id: 60001
                client_id: abc
                auth: {client_secret: '${secret:env:ZAKYA_SECRET}'}  # an M1 reference: imported as a db one
            budgets:
              zakya-api: {limits: [100/minute]}
            settings:
              timezone: Asia/Kolkata
        """),
        encoding="utf-8",
    )
    denied = cli("config", "import", str(env_file), *operator)
    assert denied.exit_code == 1 and "403" in denied.stderr
    imported = cli("config", "import", str(env_file), *admin)
    assert imported.exit_code == 0, imported.output
    assert "3 entries in environment dev" in imported.stdout
    shown = cli("connection", "show", "zakya", *operator)
    assert "organization_id: 60001" in shown.stdout and "client_secret (secret): not set" in shown.stdout
    assert cli("secret", "set", "zakya/client_secret", *admin, input="s3cr3t-value" + chr(10)).exit_code == 0
    shown = cli("connection", "show", "zakya", *operator).stdout
    assert "client_secret (secret): set, updated" in shown and "s3cr3t" not in shown
    changed = cli("connection", "set", "zakya", "organization_id=60002", "client_id=", *admin, "--json")
    assert changed.exit_code == 0, changed.output
    settings = json.loads(changed.stdout)["settings"]
    assert settings["organization_id"] == 60002 and "client_id" not in settings
    plain = cli("connection", "set", "zakya", "client_secret=oops", *admin)
    assert plain.exit_code == 1 and "is secret" in plain.stderr and "oops" not in plain.stderr
    listed = cli("secret", "list", *operator)
    assert "zakya/client_secret: updated" in listed.stdout and "s3cr3t" not in listed.stdout
    assert "zakya" in cli("connection", "list", *operator).stdout
    assert len(json.loads(cli("secret", "new-key", "--json").stdout)["key"]) == 44  # 32 bytes, base64
    rotated = cli("secret", "rotate-key", "--reason", "yearly", *admin)
    assert rotated.exit_code == 0 and "sealed 1 secrets" in rotated.stdout
    assert cli("secret", "delete", "zakya/client_secret", *admin).exit_code == 0
    assert "no secret is set" in cli("secret", "list", *operator).stdout
