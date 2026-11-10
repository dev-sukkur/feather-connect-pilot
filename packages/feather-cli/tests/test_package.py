"""`feather package build`, `publish` and `search`, and `feather registry` (flow packages §4 and §10)."""

import hashlib
import json
import shutil
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent

import pytest

from feather_server.testing import host_up, prepare

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
    with _server("dev") as found:
        yield found


@pytest.fixture
def qa_and_prod():
    """Two environments, each its own server and store: what a promotion goes between."""
    with _server("qa") as qa, _server("prod") as prod:
        yield qa, prod


@contextmanager
def _server(env: str):
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
    project = Project("p", Path("."), env, (), {}, {}, {"timezone": "UTC"}, "sha")
    from feather_connect.vault import MasterKey, Vault, new_master_key

    key = MasterKey.parse(new_master_key())
    state = AppState(
        Settings(secret_key="k", env=env),
        project,
        store,
        spool,
        engine,
        runtime,
        clock=clock,
        vault=Vault(store.secrets, env, key),
    )
    tokens = {
        role: issue_token(store, role, timedelta(days=1), role="admin" if role == "admin" else "operator",
                          scope="publish" if role == "publish" else "all", clock=clock)
        for role in ("admin", "operator", "publish")
    }  # fmt: skip
    with serve(create_app(state=state, loops=lambda s: [])) as base:
        yield base, tokens, state


def test_publish_uploads_the_built_wheel_once_and_search_finds_it(cli, sample: Path, server):
    base, tokens, _ = server
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
    base, tokens, _ = server
    r = cli("package", "publish", "--project", str(sample), "--url", base, "--token", tokens["publish"])
    assert r.exit_code == 1 and "feather package build" in r.stderr


def test_registry_add_list_test_and_remove(cli, sample: Path, server, monkeypatch: pytest.MonkeyPatch):
    from feather_server.testing import sample_wheel, static_index

    base, tokens, _ = server
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

    base, tokens, state = server
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
    assert ": inactive" in installed.stdout and "it is preparing: a worker builds" in installed.stdout
    again = cli("package", "install", "acme-invoices==1.0", *admin)
    assert again.exit_code == 1 and "install a new version" in again.stderr
    missing = cli("package", "install", "acme-invoices==9.9.9", *admin)
    assert missing.exit_code == 1 and "feather package search" in missing.stderr
    assert cli("package", "install", "==1.0", *admin).exit_code != 0
    listed = cli("package", "list", *operator)
    assert "acme-invoices: 1.0.0 (preparing); 0 flows active" in listed.stdout
    prepare(state, "acme-invoices")  # a worker's supervisor
    assert "acme-invoices: 1.0.0 (ready); 0 flows active" in cli("package", "list", *operator).stdout
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
    base, tokens, state = server
    assert cli("package", "build", "--project", str(sample)).exit_code == 0
    publish = ("--project", str(sample), "--url", base, "--token", tokens["publish"])
    assert cli("package", "publish", *publish).exit_code == 0
    admin, operator = (
        ("--server", base, "--token", tokens["admin"]),
        ("--server", base, "--token", tokens["operator"]),
    )
    assert cli("package", "install", "acme-invoices==1.0.0", *admin).exit_code == 0
    flow = "acme-invoices/invoices"
    early = cli("flow", "activate", flow, "--version", "1.0.0", *operator)
    assert early.exit_code == 1 and "still preparing" in early.stderr
    prepare(state, "acme-invoices")
    done = cli("flow", "activate", flow, "--version", "1.0.0", "--reason", "go live", *operator)
    assert done.exit_code == 0, done.output
    assert f"{flow} 1.0.0: activating" in done.stdout and "once a worker's host" in done.stdout
    assert host_up(state, "acme-invoices") == [flow]  # as a worker supervisor does when its host is up
    assert "1 flows active" in cli("package", "list", *operator).stdout
    missing = cli("flow", "activate", flow, "--version", "9.9.9", *operator)
    assert missing.exit_code == 1 and "not installed" in missing.stderr
    off = cli("flow", "deactivate", flow, *operator, "--json")
    assert off.exit_code == 0 and json.loads(off.stdout)["state"] == "inactive"


def test_connections_and_secrets_of_the_environment(cli, server, tmp_path: Path):
    base, tokens, _ = server
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


# Promotion, uninstall and signing (tasks 10 and 11).


def _installed_ready(cli, sample: Path, base: str, tokens: dict[str, str], state) -> None:
    qa = sample / "env" / "qa.yaml"  # the sample has dev and prod: qa is like dev
    if not qa.exists():
        shutil.copyfile(sample / "env" / "dev.yaml", qa)
    assert cli("package", "build", "--project", str(sample)).exit_code == 0
    publish = ("--project", str(sample), "--url", base, "--token", tokens["publish"])
    assert cli("package", "publish", *publish).exit_code == 0
    installed = cli("package", "install", "acme-invoices==1.0.0", "--server", base, "--token", tokens["admin"])
    assert installed.exit_code == 0, installed.output


def test_promote_installs_in_prod_the_bytes_tested_in_qa(cli, sample: Path, qa_and_prod):
    (qa, qa_tokens, qa_state), (prod, prod_tokens, prod_state) = qa_and_prod
    _installed_ready(cli, sample, qa, qa_tokens, qa_state)
    ends = ("--from-server", qa, "--from-token", qa_tokens["admin"], "--to-server", prod)
    promote = ("package", "promote", "acme-invoices==1.0.0", "--from", "qa", "--to", "prod", *ends)
    early = cli(*promote, "--to-token", prod_tokens["admin"])
    assert early.exit_code == 1 and "only a version that is ready" in early.stderr  # not tested in qa yet
    prepare(qa_state, "acme-invoices")
    denied = cli(*promote, "--to-token", prod_tokens["operator"])
    assert denied.exit_code == 1 and "403" in denied.stderr
    done = cli(*promote, "--to-token", prod_tokens["admin"], "--reason", "release 1.0", "--json")
    assert done.exit_code == 0, done.output
    doc = json.loads(done.stdout)
    tested = qa_state.store.packages.version("acme-invoices", "1.0.0")
    assert doc["sha256"] == tested.sha256 and doc["state"] == "preparing" and doc["from"] == "qa"
    installed = prod_state.store.packages.version("acme-invoices", "1.0.0")
    assert installed is not None and installed.sha256 == tested.sha256
    prod_bytes = prod_state.store.packages.wheel(installed.sha256)
    assert prod_bytes == qa_state.store.packages.wheel(tested.sha256)  # byte for byte
    assert hashlib.sha256(prod_bytes).hexdigest() == tested.sha256
    kept = prod_state.store.registry.file(installed.filename)  # and prod's own registry has the copy
    assert kept is not None and kept.sha256 == tested.sha256
    [row] = [a for a in prod_state.store.audit.list_audit() if a.action == "promote_package"]
    assert row.result == "ok" and row.request["from"] == "qa" and row.request["sha256"] == tested.sha256
    assert any(a.action == "export_package" for a in qa_state.store.audit.list_audit())
    again = cli(*promote, "--to-token", prod_tokens["admin"])
    assert again.exit_code == 1 and "install a new version" in again.stderr


def test_a_promotion_whose_bytes_dont_match_the_sha256_is_refused(qa_and_prod, sample: Path, cli):
    import httpx

    (qa, qa_tokens, qa_state), (prod, prod_tokens, prod_state) = qa_and_prod
    _installed_ready(cli, sample, qa, qa_tokens, qa_state)
    prepare(qa_state, "acme-invoices")
    tested = qa_state.store.packages.version("acme-invoices", "1.0.0")
    content = qa_state.store.packages.wheel(tested.sha256)
    answer = httpx.post(
        f"{prod}/api/v1/packages:promote",
        params={"filename": tested.filename, "sha256": "0" * 64, "from": "qa"},
        content=content,
        headers={"Authorization": f"Bearer {prod_tokens['admin']}", "Content-Type": "application/octet-stream"},
    )
    assert answer.status_code == 422 and answer.json()["check"] == "sha256"
    assert prod_state.store.packages.version("acme-invoices", "1.0.0") is None


def test_uninstall_is_refused_while_a_flow_is_active_then_removes_the_wheel(cli, sample: Path, server):
    base, tokens, state = server
    _installed_ready(cli, sample, base, tokens, state)
    prepare(state, "acme-invoices")
    admin = ("--server", base, "--token", tokens["admin"])
    operator = ("--server", base, "--token", tokens["operator"])
    flow = "acme-invoices/invoices"
    assert cli("flow", "activate", flow, "--version", "1.0.0", *operator).exit_code == 0
    host_up(state, "acme-invoices")
    denied = cli("package", "uninstall", "acme-invoices==1.0.0", "--yes", *operator)
    assert denied.exit_code == 1 and "403" in denied.stderr
    busy = cli("package", "uninstall", "acme-invoices==1.0.0", "--yes", *admin)
    assert busy.exit_code == 1 and "is active" in busy.stderr and "deactivate its flows first" in busy.stderr
    assert cli("flow", "deactivate", flow, *operator).exit_code == 0
    sha = state.store.packages.version("acme-invoices", "1.0.0").sha256
    done = cli("package", "uninstall", "acme-invoices==1.0.0", "--yes", "--reason", "old", *admin, "--json")
    assert done.exit_code == 0, done.output
    assert json.loads(done.stdout) == {
        "name": "acme-invoices",
        "version": "1.0.0",
        "state": "uninstalled",
        "wheel_deleted": True,
    }
    assert state.store.packages.version("acme-invoices", "1.0.0").state == "uninstalled"  # its history stays
    assert state.store.packages.wheel(sha) is None
    assert state.store.packages.activations("acme-invoices")  # and its flows' rows
    rows = [a.result for a in state.store.audit.list_audit() if a.action == "uninstall_package"]
    assert "ok" in rows and any(r.startswith("refused:") for r in rows)
    gone = cli("package", "uninstall", "acme-invoices==1.0.0", "--yes", *admin)
    assert gone.exit_code == 1 and "not installed" in gone.stderr


def test_keygen_writes_a_key_that_signs_a_build(cli, sample: Path, tmp_path: Path):
    from feather_connect.packages.signing import check_trusted_keys, verify_wheel

    key = tmp_path / "signing.pem"
    made = cli("package", "keygen", "--out", str(key), "--json")
    assert made.exit_code == 0, made.output
    doc = json.loads(made.stdout)
    assert doc["public_key"].startswith("ed25519:") and key.is_file()
    assert cli("package", "keygen", "--out", str(key)).exit_code == 1  # never over a key
    built = cli("package", "build", "--project", str(sample), "--sign-key", str(key), "--json")
    assert built.exit_code == 0, built.output
    wheel = json.loads(built.stdout)
    assert wheel["signed_by"] == doc["key_id"]
    content = Path(wheel["wheel"]).read_bytes()
    assert verify_wheel(content, check_trusted_keys([doc["public_key"]])) == doc["key_id"]
    again = cli("package", "build", "--project", str(sample), "--sign-key", str(key), "--json")
    assert json.loads(again.stdout)["sha256"] == wheel["sha256"]  # still reproducible
