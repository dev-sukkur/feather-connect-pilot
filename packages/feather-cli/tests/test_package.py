"""`feather package build`, `publish` and `search`, and `feather registry` (flow packages §4 and §10)."""

import json
import shutil
from pathlib import Path

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
    state = AppState(Settings(secret_key="k"), project, store, spool, engine, runtime, clock=clock)
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
    fetched = cli(
        "package", "fetch", "acme-invoices==1.0.0", "--out", str(tmp_path / "got"), *operator, "--json"
    )
    assert fetched.exit_code == 0, fetched.output
    doc = json.loads(fetched.stdout)
    wheel = Path(doc["wheel"])
    assert wheel.name == "acme_invoices-1.0.0-py3-none-any.whl"
    assert hashlib.sha256(wheel.read_bytes()).hexdigest() == doc["sha256"]
    assert cli("package", "fetch", "acme-invoices", *operator).exit_code == 1  # a version is needed
