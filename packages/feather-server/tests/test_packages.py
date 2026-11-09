"""Installing flow packages (flow packages §5, task 4): each check's failure is named, nothing is kept of a failed
install, and an installed version's flows are inactive, so nothing runs."""

import hashlib
import io
import json
import zipfile
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from feather_connect.packages.check import LoadCheck
from feather_connect.storage.port import RegistryFile, Role
from feather_server.app import AppState, create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.access import Forbidden
from feather_server.services.packages import InstallFailed, Packages
from feather_server.services.registries import Registries
from feather_server.testing import sample_wheel, static_index

API = "/api/v1"
ADMIN = Actor("human", "root", "admin", "root")
OPERATOR = Actor("human", "ops", "operator", "ops")
NAME, MODULE = "acme-invoices", "acme_invoices"


@pytest.fixture
def wheel(tmp_path: Path) -> Path:
    return sample_wheel(tmp_path / "dist")


@pytest.fixture
def registries(state: AppState) -> Registries:
    return Registries(state.store, state.clock)


def _packages(
    state: AppState, registries: Registries, checker: Callable[..., LoadCheck] | None = None
) -> Packages:
    if checker is None:
        return Packages(state.store, state.clock, registries, env="dev")
    return Packages(state.store, state.clock, registries, env="dev", checker=checker)


def _sound(content: bytes, manifest: dict[str, Any], **_: Any) -> LoadCheck:
    """A load check that passes, saying what the manifest says: for the tests of the checks before it."""
    return LoadCheck(True, [f["name"] for f in manifest["flows"]], [], [], [], None, 0.1)


def _rewheel(data: bytes, changes: dict[str, Callable[[bytes], bytes]]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(data)) as source, zipfile.ZipFile(out, "w") as target:
        for name in source.namelist():
            body = source.read(name)
            target.writestr(name, changes[name](body) if name in changes else body)
    return out.getvalue()


def _put(state: AppState, content: bytes, version: str = "1.0.0", filename: str | None = None) -> None:
    """Keep a wheel in the built-in registry as it is, past the checks of a publish: what a registry could serve."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        manifest = json.loads(archive.read(f"{MODULE}/feather-package.json"))
    name = filename or f"{MODULE}-{version}-py3-none-any.whl"
    sha = hashlib.sha256(content).hexdigest()
    file = RegistryFile(NAME, version, name, sha, len(content), manifest, "ci", state.clock.now())
    state.store.registry.put_file(file, content)


def _manifest_edit(**changes: Any) -> Callable[[bytes], bytes]:
    def edit(raw: bytes) -> bytes:
        return json.dumps({**json.loads(raw), **changes}).encode()

    return edit


# A sound install.


def test_an_admin_installs_a_version_whose_flows_start_inactive_and_nothing_runs(
    state: AppState, registries: Registries, wheel: Path
):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    installed = _packages(state, registries).install(ADMIN, "Acme_Invoices", "1.0.0", reason="first release")
    sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert (installed.name, installed.version, installed.sha256, installed.state) == (
        NAME,
        "1.0.0",
        sha,
        "installed",
    )
    assert installed.checks == ["not_installed", "fetch", "size", "sha256", "manifest", "name_version",
                                "compatibility", "lock", "load_check"]  # fmt: skip
    assert installed.flows and all(f.startswith(f"{NAME}/") for f in installed.flows)
    assert installed.load_check["ok"] and installed.load_check["seconds"] > 0  # loaded in a child process
    flows = state.store.packages.activations(NAME, "1.0.0")
    assert {a.state for a in flows} == {"inactive"} and [a.flow for a in flows] == installed.flows
    assert state.store.packages.wheel(sha) == wheel.read_bytes()  # the stored copy, kept by sha256
    assert state.store.runs.list_runs() == [] and state.store.catalog.flows() == []  # nothing runs
    [row] = [a for a in state.store.audit.list_audit() if a.action == "install_package"]
    assert (row.actor, row.target, row.result) == ("root", f"package:{NAME}", "ok")
    assert row.request["reason"] == "first release" and row.request["sha256"] == sha


def test_without_a_version_the_newest_not_yanked_is_installed(
    state: AppState, registries: Registries, tmp_path: Path
):
    for version in ("1.0.0", "1.1.0", "1.2.0"):
        built = sample_wheel(tmp_path / version, version)
        registries.publish(ADMIN, built.name, built.read_bytes())
    registries.yank(ADMIN, NAME, "1.2.0", reason="broken")
    installed = _packages(state, registries, _sound).install(ADMIN, NAME)
    assert installed.version == "1.1.0"


def test_a_package_from_a_company_index_installs(
    state: AppState, registries: Registries, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ACME_REGISTRY_TOKEN", "company-secret")
    with static_index({NAME: {wheel.name: wheel.read_bytes()}}, "company-secret") as url:
        registries.add(ADMIN, "acme", url, "${secret:env:ACME_REGISTRY_TOKEN}")
        installed = _packages(state, registries, _sound).install(ADMIN, NAME, "1.0.0", registry="acme")
    assert installed.registry == "acme" and state.store.packages.version(NAME, "1.0.0") is not None


# Each check's failure, by name.


def _refused(state: AppState, packages: Packages, check: str, *args: Any, **kwargs: Any) -> InstallFailed:
    with pytest.raises(InstallFailed) as caught:
        packages.install(ADMIN, NAME, *args, **kwargs)
    assert caught.value.check == check, caught.value
    assert state.store.packages.packages() == []  # nothing kept of a failed install
    audited = [a for a in state.store.audit.list_audit() if a.action == "install_package"]
    assert audited[-1].result.startswith(f"failed: {check}:")
    return caught.value


def test_a_version_installed_before_is_refused_as_not_installed(
    state: AppState, registries: Registries, wheel: Path
):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    packages = _packages(state, registries, _sound)
    packages.install(ADMIN, NAME, "1.0.0")
    for version in ("1.0.0", "1.0"):  # 1.0 is 1.0.0 to every installer
        with pytest.raises(InstallFailed) as caught:
            packages.install(ADMIN, NAME, version)
        assert caught.value.check == "not_installed"


def test_an_unknown_version_or_registry_fails_the_fetch(state: AppState, registries: Registries, wheel: Path):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    packages = _packages(state, registries, _sound)
    _refused(state, packages, "fetch", "9.9.9")
    _refused(state, packages, "fetch", "1.0.0", registry="nowhere")
    _refused(state, packages, "fetch", "not a version")


def test_a_wheel_too_big_fails_the_size_check(
    state: AppState, registries: Registries, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    import feather_server.services.registries as module

    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    monkeypatch.setattr(module, "MAX_WHEEL", 100)
    _refused(state, _packages(state, registries, _sound), "size", "1.0.0")


def test_a_wheel_whose_bytes_changed_fails_the_sha256_check(
    state: AppState, registries: Registries, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ACME_REGISTRY_TOKEN", "company-secret")
    with static_index({NAME: {wheel.name: wheel.read_bytes()}}, "company-secret", tamper=True) as url:
        registries.add(ADMIN, "acme", url, "${secret:env:ACME_REGISTRY_TOKEN}")
        _refused(state, _packages(state, registries, _sound), "sha256", "1.0.0", registry="acme")


def test_a_wheel_with_a_bad_manifest_fails_the_manifest_check(
    state: AppState, registries: Registries, wheel: Path
):
    bad = _rewheel(wheel.read_bytes(), {f"{MODULE}/feather-package.json": _manifest_edit(flows="many")})
    _put(state, bad)
    _refused(state, _packages(state, registries, _sound), "manifest", "1.0.0")


def test_a_wheel_of_another_version_fails_the_name_version_check(
    state: AppState, registries: Registries, wheel: Path
):
    _put(state, wheel.read_bytes(), version="2.0.0", filename=wheel.name)  # listed as 2.0.0, holds 1.0.0
    _refused(state, _packages(state, registries, _sound), "name_version", "2.0.0")


def test_a_package_for_another_feather_connect_fails_the_compatibility_check(
    state: AppState, registries: Registries, wheel: Path
):
    _put(
        state,
        _rewheel(wheel.read_bytes(), {f"{MODULE}/feather-package.json": _manifest_edit(fc_versions=">=99")}),
    )
    error = _refused(state, _packages(state, registries, _sound), "compatibility", "1.0.0")
    assert "doesn't include this Feather Connect" in str(error)


def test_a_lock_that_isnt_the_manifests_fails_the_lock_check(
    state: AppState, registries: Registries, wheel: Path
):
    lock = f"{MODULE}-1.0.0.dist-info/feather-lock.txt"
    _put(state, _rewheel(wheel.read_bytes(), {lock: lambda raw: raw + b"evil==6.6.6 --hash=sha256:00\n"}))
    error = _refused(state, _packages(state, registries, _sound), "lock", "1.0.0")
    assert "lock_hash" in str(error)


def test_a_package_that_doesnt_load_fails_the_load_check(
    state: AppState, registries: Registries, wheel: Path
):
    lying = _rewheel(
        wheel.read_bytes(),
        {f"{MODULE}/feather.yaml": lambda raw: raw.replace(b"flows.invoices", b"flows.missing")},
    )
    _put(state, lying)
    error = _refused(state, _packages(state, registries), "load_check", "1.0.0")  # the real check, in a child
    assert "flows.missing" in str(error)


def test_an_operator_may_not_install(state: AppState, registries: Registries, wheel: Path):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    with pytest.raises(Forbidden):
        _packages(state, registries, _sound).install(OPERATOR, NAME, "1.0.0")
    assert state.store.packages.packages() == []


# The API and reading back.


def _client(state: AppState, role: Role) -> TestClient:
    token = issue_token(state.store, role, timedelta(days=1), role=role)
    return TestClient(
        create_app(state=state, loops=lambda s: []),
        base_url="http://localhost:8000",
        headers={"Authorization": f"Bearer {token}"},
    )


def test_the_api_installs_lists_shows_and_names_a_failed_check(state: AppState, wheel: Path):
    admin, operator = _client(state, "admin"), _client(state, "operator")
    Registries(state.store, state.clock).publish(ADMIN, wheel.name, wheel.read_bytes())
    denied = operator.post(f"{API}/packages", json={"name": NAME, "version": "1.0.0"})
    assert denied.status_code == 403 and denied.json()["capability"] == "packages.install"
    made = admin.post(f"{API}/packages", json={"name": NAME, "version": "1.0.0", "reason": "go"},
                      headers={"Idempotency-Key": "k1"})  # fmt: skip
    assert made.status_code == 201, made.text
    assert made.json()["flows"] and made.json()["state"] == "installed"
    again = admin.post(f"{API}/packages", json={"name": NAME, "version": "1.0.0"})
    assert again.status_code == 409 and again.json()["check"] == "not_installed"
    missing = admin.post(f"{API}/packages", json={"name": NAME, "version": "9.9.9"})
    assert missing.status_code == 422 and missing.json()["check"] == "fetch"
    listed = operator.get(f"{API}/packages").json()["items"]
    assert [(p["name"], [v["version"] for v in p["versions"]], p["active_flows"]) for p in listed] == [
        (NAME, ["1.0.0"], 0)
    ]
    shown = operator.get(f"{API}/packages/{NAME}").json()
    assert {f["state"] for f in shown["flows"]} == {"inactive"}
    version = operator.get(f"{API}/packages/{NAME}/versions/1.0").json()
    assert version["manifest"]["name"] == NAME and version["load_check"]["ok"]
    assert operator.get(f"{API}/packages/nothing").status_code == 404
    assert operator.get(f"{API}/packages/{NAME}/versions/2.0.0").status_code == 404


def test_the_api_serves_a_registrys_wheel_checked(state: AppState, wheel: Path):
    Registries(state.store, state.clock).publish(ADMIN, wheel.name, wheel.read_bytes())
    reader = _client(state, "operator")
    got = reader.get(f"{API}/registries/feather/packages/{NAME}/versions/1.0.0/wheel")
    assert got.status_code == 200 and got.content == wheel.read_bytes()
    assert got.headers["x-feather-sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert wheel.name in got.headers["content-disposition"]
    assert reader.get(f"{API}/registries/feather/packages/{NAME}/versions/9.9.9/wheel").status_code == 404
