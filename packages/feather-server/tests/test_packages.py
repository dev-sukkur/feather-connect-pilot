"""Installing flow packages (flow packages §5, task 4): each check's failure is named, nothing is kept of a failed
install, and an installed version's flows are inactive, so nothing runs. The server runs no package code: a version is
`preparing` until a worker has load-checked it (task 6), here `prepare`, and only a `ready` one can be activated."""

import hashlib
import io
import json
import threading
import time
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from feather_connect.packages.check import LoadCheck
from feather_connect.storage.port import RegistryFile, Role
from feather_server.app import AppState, create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.access import Forbidden
from feather_server.services.packages import (
    CHECKS,
    Busy,
    InstallFailed,
    NotReady,
    Packages,
    check_lock,
    check_wheels,
)
from feather_server.services.registries import Registries
from feather_server.testing import prepare, sample_wheel, static_index

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


def _packages(state: AppState, registries: Registries) -> Packages:
    return Packages(state.store, state.clock, registries, env="dev")


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
        "preparing",
    )
    assert installed.checks == list(CHECKS) and installed.load_check == {}  # no package code ran here
    assert installed.flows and all(f.startswith(f"{NAME}/") for f in installed.flows)
    with pytest.raises(NotReady, match="still preparing"):
        _packages(state, registries).activate(OPERATOR, installed.flows[0], "1.0.0")
    found = prepare(state, NAME)  # a worker's supervisor, in a child process
    assert found["ok"] and found["seconds"] > 0 and found["worker"] == "test"
    ready = state.store.packages.version(NAME, "1.0.0")
    assert ready is not None and ready.state == "ready" and ready.load_check == found
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
    installed = _packages(state, registries).install(ADMIN, NAME)
    assert installed.version == "1.1.0"


def test_a_package_from_a_company_index_installs(
    state: AppState, registries: Registries, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ACME_REGISTRY_TOKEN", "company-secret")
    with static_index({NAME: {wheel.name: wheel.read_bytes()}}, "company-secret") as url:
        registries.add(ADMIN, "acme", url, "${secret:env:ACME_REGISTRY_TOKEN}")
        installed = _packages(state, registries).install(ADMIN, NAME, "1.0.0", registry="acme")
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
    packages = _packages(state, registries)
    packages.install(ADMIN, NAME, "1.0.0")
    for version in ("1.0.0", "1.0"):  # 1.0 is 1.0.0 to every installer
        with pytest.raises(InstallFailed) as caught:
            packages.install(ADMIN, NAME, version)
        assert caught.value.check == "not_installed"


def test_an_unknown_version_or_registry_fails_the_fetch(state: AppState, registries: Registries, wheel: Path):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    packages = _packages(state, registries)
    _refused(state, packages, "fetch", "9.9.9")
    _refused(state, packages, "fetch", "1.0.0", registry="nowhere")
    _refused(state, packages, "fetch", "not a version")


def test_a_wheel_too_big_fails_the_size_check(
    state: AppState, registries: Registries, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    import feather_server.services.registries as module

    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    monkeypatch.setattr(module, "MAX_WHEEL", 100)
    _refused(state, _packages(state, registries), "size", "1.0.0")


def test_a_wheel_whose_bytes_changed_fails_the_sha256_check(
    state: AppState, registries: Registries, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ACME_REGISTRY_TOKEN", "company-secret")
    with static_index({NAME: {wheel.name: wheel.read_bytes()}}, "company-secret", tamper=True) as url:
        registries.add(ADMIN, "acme", url, "${secret:env:ACME_REGISTRY_TOKEN}")
        _refused(state, _packages(state, registries), "sha256", "1.0.0", registry="acme")


def test_a_wheel_with_a_bad_manifest_fails_the_manifest_check(
    state: AppState, registries: Registries, wheel: Path
):
    bad = _rewheel(wheel.read_bytes(), {f"{MODULE}/feather-package.json": _manifest_edit(flows="many")})
    _put(state, bad)
    _refused(state, _packages(state, registries), "manifest", "1.0.0")


def test_a_wheel_of_another_version_fails_the_name_version_check(
    state: AppState, registries: Registries, wheel: Path
):
    _put(state, wheel.read_bytes(), version="2.0.0", filename=wheel.name)  # listed as 2.0.0, holds 1.0.0
    _refused(state, _packages(state, registries), "name_version", "2.0.0")


def test_a_package_for_another_feather_connect_fails_the_compatibility_check(
    state: AppState, registries: Registries, wheel: Path
):
    _put(
        state,
        _rewheel(wheel.read_bytes(), {f"{MODULE}/feather-package.json": _manifest_edit(fc_versions=">=99")}),
    )
    error = _refused(state, _packages(state, registries), "compatibility", "1.0.0")
    assert "doesn't include this Feather Connect" in str(error)


def test_a_lock_that_isnt_the_manifests_fails_the_lock_check(
    state: AppState, registries: Registries, wheel: Path
):
    lock = f"{MODULE}-1.0.0.dist-info/feather-lock.txt"
    _put(state, _rewheel(wheel.read_bytes(), {lock: lambda raw: raw + b"evil==6.6.6 --hash=sha256:00\n"}))
    error = _refused(state, _packages(state, registries), "lock", "1.0.0")
    assert "lock_hash" in str(error)


def test_a_package_that_doesnt_load_fails_the_load_check(
    state: AppState, registries: Registries, wheel: Path
):
    lying = _rewheel(
        wheel.read_bytes(),
        {f"{MODULE}/feather.yaml": lambda raw: raw.replace(b"flows.invoices", b"flows.missing")},
    )
    _put(state, lying)
    installed = _packages(state, registries).install(
        ADMIN, NAME, "1.0.0"
    )  # the server can't tell: it runs no code
    found = prepare(state, NAME)  # the real check, in a child
    assert not found["ok"] and found["check"] == "load_check" and "flows.missing" in found["error"]
    row = state.store.packages.version(NAME, "1.0.0")
    assert row is not None and row.state == "failed"
    with pytest.raises(NotReady, match="flows.missing") as refused:
        _packages(state, registries).activate(OPERATOR, installed.flows[0], "1.0.0")
    assert refused.value.state == "failed"


def test_an_operator_may_not_install(state: AppState, registries: Registries, wheel: Path):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    with pytest.raises(Forbidden):
        _packages(state, registries).install(OPERATOR, NAME, "1.0.0")
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
    assert made.json()["flows"] and made.json()["state"] == "preparing"
    flow = made.json()["flows"][0]
    early = operator.post(f"{API}/flows/{flow}:activate", json={"version": "1.0.0"})
    assert early.status_code == 409 and early.json()["check"] == "not_ready"
    assert early.json()["state"] == "preparing"
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
    assert (
        version["manifest"]["name"] == NAME
        and version["state"] == "preparing"
        and version["load_check"] == {}
    )
    prepare(state, NAME)
    assert operator.get(f"{API}/packages/{NAME}/versions/1.0").json()["load_check"]["ok"]
    assert operator.post(f"{API}/flows/{flow}:activate", json={"version": "1.0.0"}).status_code == 200
    assert operator.get(f"{API}/packages/nothing").status_code == 404
    assert operator.get(f"{API}/packages/{NAME}/versions/2.0.0").status_code == 404


def test_the_api_serves_a_registrys_wheel_to_an_admin_audited(state: AppState, wheel: Path):
    Registries(state.store, state.clock).publish(ADMIN, wheel.name, wheel.read_bytes())
    path = f"{API}/registries/feather/packages/{NAME}/versions/1.0.0/wheel"
    assert _client(state, "operator").get(path).status_code == 403
    admin = _client(state, "admin")
    got = admin.get(path)
    assert got.status_code == 200 and got.content == wheel.read_bytes()
    assert got.headers["x-feather-sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert got.headers["content-disposition"] == (
        f"attachment; filename=\"{wheel.name}\"; filename*=UTF-8''{wheel.name}"
    )
    assert admin.get(path.replace("1.0.0", "9.9.9")).status_code == 404
    fetched = [a.result for a in state.store.audit.list_audit() if a.action == "fetch_package"]
    assert sorted(fetched) == ["failed: the registry has no acme-invoices 9.9.9", "ok"]


def test_a_filename_is_made_safe_for_content_disposition():
    from feather_server.api.routes import attachment

    header = attachment('evil"; x=1\r\né.whl')
    assert header.startswith("attachment; filename=\"evil___x_1___.whl\"; filename*=UTF-8''")
    assert "\r" not in header and "\n" not in header and "%C3%A9" in header


# Yanked versions, pre-releases and the lock's content.


def test_a_yanked_version_is_refused_unless_allowed(state: AppState, registries: Registries, wheel: Path):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    registries.yank(ADMIN, NAME, "1.0.0", reason="broken")
    packages = _packages(state, registries)
    _refused(state, packages, "not_yanked", "1.0.0")
    assert packages.install(ADMIN, NAME, "1.0.0", allow_yanked=True).version == "1.0.0"


def test_pre_releases_are_skipped_unless_pinned_or_asked_for(
    state: AppState, registries: Registries, tmp_path: Path
):
    for version in ("1.0.0", "1.1.0rc1"):
        built = sample_wheel(tmp_path / version, version)
        registries.publish(ADMIN, built.name, built.read_bytes())
    packages = _packages(state, registries)
    assert packages.install(ADMIN, NAME).version == "1.0.0"
    assert packages.install(ADMIN, NAME, pre=True).version == "1.1.0rc1"


def test_only_pinned_hashed_requirements_pass_the_lock_check():
    digest = "--hash=sha256:" + "a" * 64
    check_lock(b"")
    check_lock(
        f"# exported\nhttpx==0.28.1 \\\n    {digest} \\\n    {digest}\n    # via acme\n"
        f"anyio[trio]==4.6.0 ; python_version >= '3.12' {digest}\n".encode()
    )
    for bad, why in [
        ("--index-url https://evil.example/simple\n", "is an option"),
        (f"-e ./local {digest}\n", "is an option"),
        (f"httpx>=0.28 {digest}\n", "not pinned"),
        (f"httpx @ https://evil.example/httpx.whl {digest}\n", "not pinned"),
        (f"../vendored/httpx {digest}\n", "not pinned"),
        ("httpx==0.28.1\n", "--hash"),
        ("httpx==0.28.1 --hash=md5:00\n", "--hash"),
    ]:
        with pytest.raises(ValueError, match=why):
            check_lock(bad.encode())


def test_only_requirements_with_a_wheel_pass_the_wheels_check():
    wheel, sdist = "sha256:" + "a" * 64, "sha256:" + "b" * 64
    check_wheels(b"")
    check_wheels(
        f"httpx==0.28.1 \\\n    --hash={wheel} \\\n    --hash={sdist}\n    # wheels: {wheel}\n    # via acme\n".encode()
    )
    for bad in (
        f"httpx==0.28.1 --hash={sdist}\n    # wheels:\n",  # only a source distribution
        f"httpx==0.28.1 --hash={sdist}\n",  # nothing says which is a wheel
        f"httpx==0.28.1 --hash={sdist}\n    # wheels: {wheel}\n",  # a wheel it doesn't pin
    ):
        with pytest.raises(ValueError, match="only a source distribution"):
            check_wheels(bad.encode())


def test_a_lock_with_only_a_source_distribution_fails_the_wheels_check(
    state: AppState, registries: Registries, wheel: Path
):
    lock = f"httpx==0.28.1 --hash=sha256:{'b' * 64}\n    # wheels:\n".encode()
    _put(
        state,
        _rewheel(
            wheel.read_bytes(),
            {
                f"{MODULE}-1.0.0.dist-info/feather-lock.txt": lambda _: lock,
                f"{MODULE}/feather-package.json": _manifest_edit(lock_hash=hashlib.sha256(lock).hexdigest()),
            },
        ),
    )
    error = _refused(state, _packages(state, registries), "wheels", "1.0.0")
    assert "httpx==0.28.1 has no wheel" in str(error)


def test_a_lock_with_an_index_option_fails_the_lock_check(
    state: AppState, registries: Registries, wheel: Path
):
    lock = b"--extra-index-url https://evil.example/simple\n"
    _put(
        state,
        _rewheel(
            wheel.read_bytes(),
            {
                f"{MODULE}-1.0.0.dist-info/feather-lock.txt": lambda _: lock,
                f"{MODULE}/feather-package.json": _manifest_edit(lock_hash=hashlib.sha256(lock).hexdigest()),
            },
        ),
    )
    error = _refused(state, _packages(state, registries), "lock", "1.0.0")
    assert "is an option" in str(error)


def test_a_package_with_its_own_webhook_connector_is_refused(
    state: AppState, registries: Registries, wheel: Path
):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())

    def own(content: bytes, manifest: dict[str, Any], **_: Any) -> LoadCheck:
        sound = _sound(content, manifest)
        return LoadCheck(True, sound.flows, [], [], [], None, 0.1, ["hooks (AcmeHooks)"])

    _packages(state, registries).install(ADMIN, NAME, "1.0.0")
    found = prepare(state, NAME, checker=own)
    assert found["check"] == "webhook_connector" and "hooks (AcmeHooks)" in found["error"]
    assert state.store.packages.version(NAME, "1.0.0").state == "failed"  # type: ignore[union-attr]


def test_an_error_that_is_a_bug_is_audited_as_internal(
    state: AppState, registries: Registries, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())

    def broken(*_: Any, **__: Any) -> Any:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(registries, "fetch", broken)
    with pytest.raises(RuntimeError):
        _packages(state, registries).install(ADMIN, NAME, "1.0.0")
    [row] = [a for a in state.store.audit.list_audit() if a.action == "install_package"]
    assert row.result == "failed: internal"


# Installs at once.


def _waiting(
    registries: Registries, started: threading.Barrier | None = None, go: threading.Event | None = None
) -> Registries:
    """The registries, made to hold every wheel fetch until `started` and `go` let it through."""
    fetch = registries.fetch

    def held(*args: Any, **kwargs: Any) -> Any:
        found = fetch(*args, **kwargs)
        if started is not None:
            started.wait(timeout=10)
        if go is not None:
            assert go.wait(timeout=10)
        return found

    registries.fetch = held  # type: ignore[method-assign]
    return registries


def test_two_installs_of_a_version_at_once_install_it_once(
    state: AppState, registries: Registries, wheel: Path
):
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    packages = _packages(state, _waiting(registries, threading.Barrier(2)))  # both pass not_installed first
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(packages.install, ADMIN, NAME, "1.0.0") for _ in range(2)]
    outcomes = [f.exception() for f in futures]
    refused = [e for e in outcomes if e is not None]
    assert len(refused) == 1 and isinstance(refused[0], InstallFailed) and refused[0].check == "not_installed"
    assert [v.version for v in state.store.packages.versions(NAME)] == ["1.0.0"]


def test_an_install_past_the_limit_is_busy_and_the_api_answers_503(
    state: AppState, registries: Registries, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    import feather_server.services.packages as module

    for version in ("1.0.0", "1.1.0"):
        built = sample_wheel(tmp_path / version, version)
        registries.publish(ADMIN, built.name, built.read_bytes())
    go, started = threading.Event(), threading.Barrier(3)
    packages = _packages(state, _waiting(registries, started, go))
    with ThreadPoolExecutor(2) as pool:
        running = [pool.submit(packages.install, ADMIN, NAME, v) for v in ("1.0.0", "1.1.0")]
        started.wait(timeout=10)  # both installs hold their slots
        with pytest.raises(Busy):
            packages.install(ADMIN, NAME, "1.0.0")
        with pytest.raises(Busy):
            packages.fetch(ADMIN, "feather", NAME, "1.0.0")
        go.set()
    assert all(f.exception() is None for f in running)
    monkeypatch.setattr(module, "MAX_BUSY", 0)
    app = create_app(state=state, loops=lambda s: [])
    app.state.packages = _packages(state, registries)
    token = issue_token(state.store, "admin", timedelta(days=1), role="admin")
    client = TestClient(app, base_url="http://localhost:8000", headers={"Authorization": f"Bearer {token}"})
    path, key = f"{API}/packages", {"Idempotency-Key": "b1"}
    busy = client.post(path, json={"name": NAME, "version": "1.0.0"}, headers=key)
    assert busy.status_code == 503 and busy.headers["retry-after"] == "30"
    monkeypatch.setattr(module, "MAX_BUSY", 2)
    app.state.packages = _packages(state, registries)
    again = client.post(path, json={"name": NAME, "version": "2.0.0"}, headers=key)
    assert again.status_code == 422 and again.json()["check"] == "fetch"  # the 503 wasn't kept for the key


def test_requests_with_one_idempotency_key_at_once_install_once(
    state: AppState, registries: Registries, wheel: Path
):
    from feather_server.testing import serve

    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    app = create_app(state=state, loops=lambda s: [])
    app.state.packages = _packages(state, _waiting(registries, go=(go := threading.Event())))
    token = issue_token(state.store, "admin", timedelta(days=1), role="admin")
    headers = {"Authorization": f"Bearer {token}", "Idempotency-Key": "same"}
    with serve(app) as base, ThreadPoolExecutor(2) as pool:

        def post() -> httpx.Response:
            body = {"name": NAME, "version": "1.0.0"}
            return httpx.post(f"{base}{API}/packages", json=body, headers=headers, timeout=30)

        answers = [pool.submit(post) for _ in range(2)]
        time.sleep(0.5)
        go.set()
        first, second = (a.result() for a in answers)
    assert first.status_code == second.status_code == 201 and first.json() == second.json()
    assert len([a for a in state.store.audit.list_audit() if a.action == "install_package"]) == 1


# Signing (task 11): an environment may take only packages signed with a key it trusts.


def _require_signed(state: AppState, *keys: str) -> None:
    from feather_connect.storage.port import EnvEntry

    now = state.clock.now()
    state.store.env_config.put(EnvEntry("dev", "setting", "trusted_package_keys", list(keys), "root", now))
    state.store.env_config.put(EnvEntry("dev", "setting", "require_signed_packages", True, "root", now))


def _signed_wheel(out: Path, signer: Any) -> Path:
    from feather_connect.packages.build import build_package
    from feather_server.testing import SAMPLE_PACKAGE

    return build_package(SAMPLE_PACKAGE, out, signer=signer).path


def test_an_unsigned_package_is_refused_where_signing_is_required(
    state: AppState, registries: Registries, wheel: Path
):
    from feather_connect.packages.signing import Signer, public_key_text

    _require_signed(state, public_key_text(Signer.generate().public))
    registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    with pytest.raises(InstallFailed, match="not signed") as failed:
        _packages(state, registries).install(ADMIN, NAME, "1.0.0")
    assert failed.value.check == "signature"
    assert state.store.packages.version(NAME, "1.0.0") is None
    [row] = [a for a in state.store.audit.list_audit() if a.action == "install_package"]
    assert row.result.startswith("failed: signature:")


def test_a_package_signed_with_a_trusted_key_installs_and_another_key_or_a_changed_file_is_refused(
    state: AppState, registries: Registries, tmp_path: Path
):
    from feather_connect.packages.signing import Signer, public_key_text

    trusted, other = Signer.generate(), Signer.generate()
    _require_signed(state, public_key_text(trusted.public))
    good = _signed_wheel(tmp_path / "good", trusted).read_bytes()
    by_other = _signed_wheel(tmp_path / "other", other).read_bytes()
    changed = _rewheel(good, {f"{MODULE}/__init__.py": lambda b: b + b"print('changed')\n"})
    packages = _packages(state, registries)
    for content, why in ((by_other, "doesn't trust"), (changed, "not the file its signed RECORD names")):
        _put(state, content, "1.0.0")
        with pytest.raises(InstallFailed, match=why) as failed:
            packages.install(ADMIN, NAME, "1.0.0")
        assert failed.value.check == "signature"
        assert state.store.packages.version(NAME, "1.0.0") is None
        _forget(state)
    _put(state, good, "1.0.0")
    installed = packages.install(ADMIN, NAME, "1.0.0")
    assert installed.signed_by == trusted.key_id and "signature" in installed.checks
    assert packages.signing() == (True, [trusted.key_id])


def _forget(state: AppState) -> None:
    """Drop the built-in registry's files of the sample, so the next test wheel takes their place."""
    files = state.store.registry._d.registry_files  # pyright: ignore[reportAttributeAccessIssue] # the memory store's
    for name in [n for n in files if n.startswith(MODULE)]:
        del files[name]


def test_requiring_signed_packages_needs_trusted_keys_and_keys_must_be_ed25519(state: AppState):
    from feather_connect.packages.signing import Signer, public_key_text
    from feather_connect.vault import MasterKey, Vault, new_master_key
    from feather_server.services.environment import ConfigProblem, Environment

    vault = Vault(state.store.secrets, "dev", MasterKey.parse(new_master_key()))
    environment = Environment(state.store, state.clock, "dev", vault)
    with pytest.raises(ConfigProblem, match="needs trusted_package_keys"):
        environment.put(ADMIN, "setting", "require_signed_packages", True)
    with pytest.raises(ConfigProblem, match="not a public key"):
        environment.put(ADMIN, "setting", "trusted_package_keys", ["ssh-rsa AAAA"])
    with pytest.raises(ConfigProblem, match="true or false"):
        environment.put(ADMIN, "setting", "require_signed_packages", "yes")
    environment.put(ADMIN, "setting", "trusted_package_keys", [public_key_text(Signer.generate().public)])
    environment.put(ADMIN, "setting", "require_signed_packages", True)
