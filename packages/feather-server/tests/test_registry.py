"""The built-in package registry and the other registries an environment trusts (flow packages §4.3, task 2)."""

import base64
import hashlib
import io
import json
import re
import shutil
import subprocess
import zipfile
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from feather_connect.storage.port import Role, TokenScope
from feather_server.app import AppState, create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.registries import Registries, RegistryProblem
from feather_server.testing import sample_wheel, serve, static_index

API = "/api/v1"
ADMIN = Actor("human", "root", "admin", "root")
WHEEL = "acme_invoices-1.0.0-py3-none-any.whl"
SIMPLE_JSON = "application/vnd.pypi.simple.v1+json"


@pytest.fixture
def wheel(tmp_path: Path) -> Path:
    return sample_wheel(tmp_path / "dist")


def _token(state: AppState, role: Role = "operator", scope: TokenScope = "all") -> str:
    return issue_token(state.store, f"{role}-{scope}", timedelta(days=1), role=role, scope=scope)


def _client(state: AppState, token: str | None = None, *, basic: bool = False) -> TestClient:
    headers = {}
    if token and basic:
        headers["Authorization"] = "Basic " + base64.b64encode(f"__token__:{token}".encode()).decode()
    elif token:
        headers["Authorization"] = f"Bearer {token}"
    return TestClient(
        create_app(state=state, loops=lambda s: []), base_url="http://localhost:8000", headers=headers
    )


def _upload(client: TestClient, path: Path, data: bytes | None = None, **fields: str):
    content = path.read_bytes() if data is None else data
    form = {":action": "file_upload", "protocol_version": "1", "filetype": "bdist_wheel", **fields}
    return client.post(
        "/registry/", data=form, files={"content": (path.name, content, "application/octet-stream")}
    )


# Publishing and reading.


def test_a_publish_token_uploads_a_wheel_and_any_token_reads_it(state: AppState, wheel: Path):
    publisher = _client(state, _token(state, scope="publish"), basic=True)
    sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    answer = _upload(publisher, wheel, sha256_digest=sha, name="acme-invoices", version="1.0.0")
    assert answer.status_code == 200, answer.text
    reader = _client(state, _token(state))
    index = reader.get("/registry/simple/")
    assert index.status_code == 200 and '<a href="acme-invoices/">acme-invoices</a>' in index.text
    page = reader.get("/registry/simple/acme-invoices/")
    assert (
        f'<a href="../../files/{WHEEL}#sha256={sha}" data-requires-python="&gt;=3.12">{WHEEL}</a>'
        in page.text
    )
    assert reader.get("/registry/simple/Acme_Invoices/", follow_redirects=False).headers["location"] == (
        "/registry/simple/acme-invoices/"
    )
    assert reader.get("/registry/simple/nothing/").status_code == 404
    got = reader.get(f"/registry/files/{WHEEL}")
    assert got.status_code == 200 and hashlib.sha256(got.content).hexdigest() == sha
    as_json = reader.get("/registry/simple/acme-invoices/", headers={"Accept": SIMPLE_JSON})
    assert as_json.headers["content-type"] == SIMPLE_JSON
    (listed,) = as_json.json()["files"]
    assert (listed["filename"], listed["hashes"], listed["yanked"]) == (WHEEL, {"sha256": sha}, False)
    assert reader.get("/registry/simple/", headers={"Accept": SIMPLE_JSON}).json()["projects"] == [
        {"name": "acme-invoices"}
    ]
    [row] = [a for a in state.store.audit.list_audit() if a.action == "publish_package"]
    assert (row.actor, row.target, row.result, row.request["sha256"]) == (
        "operator-publish",
        "package:acme-invoices",
        "ok",
        sha,
    )


def test_a_version_is_never_overwritten(state: AppState, wheel: Path, tmp_path: Path):
    admin = _client(state, _token(state, "admin"))
    assert _upload(admin, wheel).status_code == 200
    again = _upload(admin, wheel)
    assert again.status_code == 409 and "never replaced" in again.json()["detail"]
    changed = sample_wheel(tmp_path / "changed", "1.0.0")  # the same version, built elsewhere: other bytes
    assert changed.read_bytes() != wheel.read_bytes()
    assert _upload(admin, changed).status_code == 409
    assert state.store.registry.content(WHEEL) == wheel.read_bytes()
    assert _upload(admin, sample_wheel(tmp_path / "next", "1.1.0")).status_code == 200


def test_what_isnt_a_flow_package_is_refused(state: AppState, wheel: Path, tmp_path: Path):
    admin = _client(state, _token(state, "admin"))
    plain = io.BytesIO()
    with zipfile.ZipFile(plain, "w") as archive:
        archive.writestr("x-1.0.0.dist-info/METADATA", "Name: x\nVersion: 1.0.0\n")
    stray = tmp_path / "x-1.0.0-py3-none-any.whl"
    stray.write_bytes(plain.getvalue())
    refused = _upload(admin, stray)
    assert refused.status_code == 400 and "not a flow package" in refused.json()["detail"]
    sdist = tmp_path / "acme-invoices-1.0.0.tar.gz"
    sdist.write_bytes(b"x")
    assert _upload(admin, sdist).status_code == 400
    wrong = _upload(admin, wheel, sha256_digest="0" * 64)
    assert wrong.status_code == 400 and "sha256" in wrong.json()["detail"]
    assert admin.post("/registry/", data={":action": "remove_pkg"}).status_code == 400
    assert state.store.registry.projects() == []


def test_who_may_publish_and_read(state: AppState, wheel: Path):
    nobody = _client(state)
    refused = nobody.get("/registry/simple/")
    assert refused.status_code == 401 and refused.headers["www-authenticate"].startswith("Basic")
    assert _upload(nobody, wheel).status_code == 401
    operator = _client(state, _token(state))
    denied = _upload(operator, wheel)
    assert denied.status_code == 403 and "needs the admin role" in denied.json()["detail"]
    publisher = _client(state, _token(state, scope="publish"))
    assert publisher.get("/registry/simple/").status_code == 200  # reading is any valid token's
    for path in ("/flows", "/users", "/registries", "/me"):  # a publish token can do nothing else
        assert publisher.get(API + path).status_code == 403
    assert (
        publisher.post(f"{API}/registries/feather/packages/acme-invoices/versions/1.0.0:yank").status_code
        == 403
    )


def test_an_admin_yanks_a_version_which_the_index_then_marks(state: AppState, wheel: Path):
    admin = _client(state, _token(state, "admin"))
    _upload(admin, wheel)
    operator = _client(state, _token(state))
    path = f"{API}/registries/feather/packages/acme-invoices/versions/1.0.0:yank"
    assert operator.post(path, json={"reason": "bad totals"}).status_code == 403
    assert admin.post(path, json={"reason": "bad totals"}).status_code == 200
    assert (
        admin.post(f"{API}/registries/feather/packages/acme-invoices/versions/9.0.0:yank").status_code == 404
    )
    page = operator.get("/registry/simple/acme-invoices/").text
    assert 'data-yanked="bad totals"' in page
    as_json = operator.get("/registry/simple/acme-invoices/", headers={"Accept": SIMPLE_JSON}).json()
    assert as_json["files"][0]["yanked"] == "bad totals"
    assert operator.get(f"/registry/files/{WHEEL}").status_code == 200  # yanked, never deleted
    found = operator.get(f"{API}/registries/feather/packages", params={"q": "invoice"}).json()["items"]
    assert found == [{"name": "acme-invoices", "versions": ["1.0.0"], "yanked": ["1.0.0"]}]


def test_the_registry_can_be_turned_off(state: AppState, wheel: Path):
    from dataclasses import replace

    off = AppState(
        replace(state.settings, registry=False),
        state.project,
        state.store,
        state.spool,
        state.engine,
        state.runtime,
        clock=state.clock,
    )
    admin = _client(off, _token(off, "admin"))
    assert admin.get("/registry/simple/").status_code == 404 and _upload(admin, wheel).status_code == 404
    assert admin.get(f"{API}/registries").json()["items"] == []


def test_feather_registry_setting():
    from feather_server.settings import Settings, SettingsError

    base = {"FEATHER_DATABASE_URL": "postgresql://x", "FEATHER_PROJECT": "."}
    assert Settings.from_env(base).registry is True
    assert Settings.from_env({**base, "FEATHER_REGISTRY": "0"}).registry is False
    with pytest.raises(SettingsError):
        Settings.from_env({**base, "FEATHER_REGISTRY": "maybe"})


# Other registries.


def test_an_admin_adds_tests_and_removes_a_company_registry(
    state: AppState, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ACME_REGISTRY_TOKEN", "company-secret")
    admin = _client(state, _token(state, "admin"))
    operator = _client(state, _token(state))
    with static_index({"acme-invoices": {WHEEL: wheel.read_bytes()}}, "company-secret") as url:
        body = {"name": "acme", "index_url": url, "token_ref": "${secret:env:ACME_REGISTRY_TOKEN}"}
        assert operator.post(f"{API}/registries", json=body).status_code == 403
        literal = admin.post(f"{API}/registries", json={**body, "token_ref": "company-secret"})
        assert literal.status_code == 422 and "secret reference" in literal.json()["detail"]
        assert admin.post(f"{API}/registries", json={**body, "name": "feather"}).status_code == 422
        assert admin.post(f"{API}/registries", json={**body, "index_url": "ftp://x/"}).status_code == 422
        added = admin.post(f"{API}/registries", json=body)
        assert added.status_code == 201 and added.json()["token_ref"] == body["token_ref"]
        assert admin.post(f"{API}/registries", json=body).status_code == 422
        listed = operator.get(f"{API}/registries").json()["items"]
        assert [(r["name"], r["built_in"]) for r in listed] == [("feather", True), ("acme", False)]
        assert "company-secret" not in json.dumps(listed)
        tested = admin.post(f"{API}/registries/acme:test").json()
        assert tested == {"registry": "acme", "ok": True, "projects": 1, "message": "the index answered"}
        found = operator.get(f"{API}/registries/acme/packages", params={"q": "acme"}).json()["items"]
        assert found == [{"name": "acme-invoices", "versions": ["1.0.0"], "yanked": []}]
        monkeypatch.setenv("ACME_REGISTRY_TOKEN", "wrong")
        failed = admin.post(f"{API}/registries/acme:test").json()
        assert failed["ok"] is False and "refused the credentials" in failed["message"]
        assert "wrong" not in failed["message"]
    assert admin.delete(f"{API}/registries/feather").status_code == 422
    assert admin.delete(f"{API}/registries/acme").status_code == 200
    assert admin.delete(f"{API}/registries/acme").status_code == 404
    assert operator.get(f"{API}/registries/acme/packages").status_code == 404
    actions = [a.action for a in state.store.audit.list_audit() if a.target == "registry:acme"]
    assert {"add_registry", "test_registry", "remove_registry"} <= set(actions)


def test_fetching_checks_the_sha256_from_the_built_in_and_a_company_index(
    state: AppState, wheel: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ACME_REGISTRY_TOKEN", "company-secret")
    registries = Registries(state.store, state.clock)
    registries.publish(ADMIN, WHEEL, wheel.read_bytes())
    sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    built_in = registries.fetch("feather", "Acme_Invoices", "1.0.0")
    assert (built_in.sha256, built_in.content, built_in.manifest["name"]) == (
        sha,
        wheel.read_bytes(),
        "acme-invoices",
    )
    with pytest.raises(KeyError):
        registries.fetch("feather", "acme-invoices", "9.9.9")
    files = {"acme-invoices": {WHEEL: wheel.read_bytes()}}
    with static_index(files, "company-secret") as url:
        registries.add(ADMIN, "acme", url, "${secret:env:ACME_REGISTRY_TOKEN}")
        fetched = registries.fetch("acme", "acme-invoices", "1.0.0")
        assert (fetched.registry, fetched.filename, fetched.sha256) == ("acme", WHEEL, sha)
        with pytest.raises(RegistryProblem, match="no wheel of acme-invoices 2.0.0"):
            registries.fetch("acme", "acme-invoices", "2.0.0")
    with static_index(files, "company-secret", tamper=True) as url:
        registries.remove(ADMIN, "acme")
        registries.add(ADMIN, "acme", url, "${secret:env:ACME_REGISTRY_TOKEN}")
        with pytest.raises(RegistryProblem, match="its sha256 is"):
            registries.fetch("acme", "acme-invoices", "1.0.0")
    monkeypatch.delenv("ACME_REGISTRY_TOKEN")
    with pytest.raises(RegistryProblem, match="ACME_REGISTRY_TOKEN"):
        registries.fetch("acme", "acme-invoices", "1.0.0")


# With the real tools.


@pytest.mark.skipif(shutil.which("uv") is None, reason="needs uv")
def test_uv_publishes_to_the_registry_and_installs_from_it(state: AppState, wheel: Path, tmp_path: Path):
    publish, read = _token(state, scope="publish"), _token(state)
    with serve(create_app(state=state, loops=lambda s: [])) as base:
        published = subprocess.run(
            ["uv", "publish", "--publish-url", f"{base}/registry/", "--token", publish,
             "--trusted-publishing", "never", str(wheel)],
            capture_output=True, text=True, timeout=120, check=False,
        )  # fmt: skip
        assert published.returncode == 0, published.stderr
        again = subprocess.run(
            ["uv", "publish", "--publish-url", f"{base}/registry/", "--token", publish,
             "--trusted-publishing", "never", str(wheel)],
            capture_output=True, text=True, timeout=120, check=False,
        )  # fmt: skip
        assert again.returncode != 0 and "never replaced" in again.stderr
        target = tmp_path / "installed"
        index = re.sub(r"^http://", f"http://__token__:{read}@", base) + "/registry/simple/"
        installed = subprocess.run(
            ["uv", "pip", "install", "--no-deps", "--no-cache", "--target", str(target), "--index-url", index,
             "acme-invoices==1.0.0"],
            capture_output=True, text=True, timeout=120, check=False,
        )  # fmt: skip
        assert installed.returncode == 0, installed.stderr
    manifest = json.loads((target / "acme_invoices" / "feather-package.json").read_text(encoding="utf-8"))
    assert (
        manifest["name"] == "acme-invoices" and (target / "acme_invoices" / "flows" / "invoices.py").is_file()
    )
