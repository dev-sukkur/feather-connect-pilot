"""The environment's own configuration and secrets, declared webhooks and activating flows, on the server (flow
packages §7.2 and §8, task 5a and 5)."""

import hashlib
import hmac
import json
import re
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from feather_connect.clock import FrozenClock
from feather_connect.packages.check import LoadCheck
from feather_connect.project import use_db_secrets
from feather_connect.runtime.catalogue import Catalogue
from feather_connect.storage.port import Role
from feather_connect.vault import MasterKey, new_master_key
from feather_server.app import AppState, create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.access import Forbidden
from feather_server.services.environment import ConfigProblem, Environment
from feather_server.services.packages import InstallFailed, Packages
from feather_server.services.registries import Registries, VersionExists
from feather_server.testing import sample_wheel, sign_in

API = "/api/v1"
ADMIN = Actor("human", "root", "admin", "root")
OPERATOR = Actor("human", "ops", "operator", "ops")
PACKAGE, FLOW = "acme-invoices", "acme-invoices/invoices"
ZAKYA = {
    "connector": "zakya",
    "base_url": "https://api.zakya.in",
    "organization_id": 60001,
    "client_id": "1000.ABC",
    "auth": {"client_secret": "${secret:db:zakya-secret}", "refresh_token": "${secret:env:ZAKYA_REFRESH}"},
}
HOOK = {
    "dataset": "invoices",
    "verify": {"scheme": "hmac-sha256", "header": "X-Signature"},
    "secret": "${secret:db:hook-secret}",
    "key": "$.id",
    "stamp": "$.updated_at",
    "ack": 200,
}


@pytest.fixture
def keyed(state: AppState) -> AppState:
    state.master_key = MasterKey.parse(new_master_key())
    return state


@pytest.fixture
def envs(keyed: AppState) -> Iterator[Environment]:
    environment = Environment(keyed.store, keyed.clock, "dev", keyed.master_key)
    use_db_secrets(environment.open)
    yield environment
    use_db_secrets(None)


def _client(state: AppState, role: Role) -> TestClient:
    token = issue_token(state.store, role, timedelta(days=1), role=role)
    return TestClient(
        create_app(state=state, loops=lambda s: []),
        base_url="http://localhost:8000",
        headers={"Authorization": f"Bearer {token}"},
    )


# The environment's own configuration (task 5a, the sponsor's decision B).


def test_a_connections_settings_are_plain_and_every_change_is_audited_with_old_and_new(envs: Environment):
    envs.put(ADMIN, "connection", "zakya", ZAKYA, reason="carried over")
    envs.update(ADMIN, "zakya", {"organization_id": 60002, "client_id": None}, reason="new org")
    view = envs.connection("zakya")
    assert view["settings"]["organization_id"] == 60002 and "client_id" not in view["settings"]
    assert view["secrets"]["client_secret"] == {"ref": "${secret:db:zakya-secret}", "set": False}
    assert view["secrets"]["refresh_token"] == {"ref": "${secret:env:ZAKYA_REFRESH}"}
    assert {"client_secret", "refresh_token"} <= set(view["secret_fields"]) and "client_id" not in view[
        "secret_fields"
    ]
    first, second = [a for a in envs._store.audit.list_audit() if a.action == "set_connection"][:2][::-1]  # pyright: ignore[reportPrivateUsage]
    assert first.request["old"] is None and first.request["new"]["organization_id"] == 60001
    assert (
        second.request["old"]["organization_id"] == 60001
        and second.request["new"]["organization_id"] == 60002
    )
    assert second.request["reason"] == "new org"


def test_a_plain_value_for_a_secret_field_is_refused_and_not_kept(envs: Environment):
    with pytest.raises(ConfigProblem, match="client_secret is secret") as caught:
        envs.put(ADMIN, "connection", "zakya", {**ZAKYA, "client_secret": "s3cr3t"})
    assert "s3cr3t" not in str(caught.value)
    with pytest.raises(ConfigProblem, match="must be a reference"):
        envs.update(ADMIN, "zakya", {"connector": "zakya", "auth.client_secret": "s3cr3t"})
    assert envs.entries() == []
    assert not any("s3cr3t" in json.dumps(a.request) for a in envs._store.audit.list_audit())  # pyright: ignore[reportPrivateUsage]


def test_an_operator_reads_but_may_not_change(envs: Environment):
    envs.put(ADMIN, "budget", "zakya-api", {"limits": ["100/minute"]})
    assert [e.name for e in envs.entries("budget")] == ["zakya-api"]
    with pytest.raises(Forbidden, match="environment.manage"):
        envs.put(OPERATOR, "setting", "timezone", "UTC")
    with pytest.raises(Forbidden, match="secrets.manage"):
        envs.set_secret(OPERATOR, "x", "v")


def test_an_m1_environment_file_seeds_the_tables_and_a_bad_entry_sets_none(envs: Environment, tmp_path: Path):
    path = tmp_path / "prod.yaml"
    path.write_text(
        "connections:\n  zakya: {connector: zakya, organization_id: 60001}\n"
        "budgets:\n  zakya-api: {limits: [100/minute]}\nsettings:\n  timezone: Asia/Kolkata\n",
        encoding="utf-8",
    )
    done = envs.import_file(ADMIN, path)
    assert [(e.kind, e.name) for e in done] == [
        ("connection", "zakya"),
        ("budget", "zakya-api"),
        ("setting", "timezone"),
    ]
    path.write_text(
        "connections:\n  a: {connector: zakya}\n  b: {connector: zakya, client_secret: x}\n", encoding="utf-8"
    )
    with pytest.raises(ConfigProblem):
        envs.import_file(ADMIN, path)
    assert envs._store.env_config.entry("dev", "connection", "a") is None  # pyright: ignore[reportPrivateUsage]


# Secrets (the sponsor's decision C).


def test_a_secret_is_shown_as_set_never_as_its_value_and_is_audited_by_name(envs: Environment):
    envs.put(ADMIN, "connection", "zakya", ZAKYA)
    assert envs.set_secret(ADMIN, "zakya-secret", "s3cr3t", reason="first") is False
    assert envs.set_secret(ADMIN, "zakya-secret", "n3w", reason="rotated by zakya") is True
    shown = envs.connection("zakya")["secrets"]["client_secret"]
    assert shown["set"] is True and shown["updated_by"] == "root" and "updated_at" in shown
    assert [s["name"] for s in envs.secrets()] == ["zakya-secret"]
    assert envs.open("zakya-secret") == "n3w"
    rows = [a for a in envs._store.audit.list_audit() if a.action == "set_secret"]  # pyright: ignore[reportPrivateUsage]
    assert sorted(r.request["replaced"] for r in rows) == [False, True]
    assert not any(v in json.dumps(r.request) for r in rows for v in ("s3cr3t", "n3w"))
    envs.delete_secret(ADMIN, "zakya-secret")
    assert envs.connection("zakya")["secrets"]["client_secret"]["set"] is False


def test_the_api_takes_secrets_write_only(keyed: AppState):
    admin, operator = _client(keyed, "admin"), _client(keyed, "operator")
    put = admin.put(f"{API}/connections/zakya", json={"value": ZAKYA, "reason": "seed"})
    assert put.status_code == 200, put.text
    assert put.json()["settings"]["client_id"] == "1000.ABC"
    refused = admin.patch(f"{API}/connections/zakya", json={"changes": {"client_secret": "s3cr3t"}})
    assert refused.status_code == 422 and "s3cr3t" not in refused.text
    assert operator.put(f"{API}/secrets/zakya-secret", json={"value": "s3cr3t"}).status_code == 403
    done = admin.put(f"{API}/secrets/zakya-secret", json={"value": "s3cr3t", "reason": "seed"})
    assert done.status_code == 200 and done.json() == {"name": "zakya-secret", "replaced": False}
    listed = operator.get(f"{API}/secrets")
    assert [s["name"] for s in listed.json()["items"]] == ["zakya-secret"] and "s3cr3t" not in listed.text
    every = operator.get(f"{API}/connections").json()
    assert every["env"] == "dev" and every["environment"][0]["secrets"]["client_secret"]["set"] is True
    assert "s3cr3t" not in operator.get(f"{API}/connections/zakya").text
    assert operator.get(f"{API}/secrets/zakya-secret").status_code in (404, 405)  # there is no read-back
    bad = admin.put(f"{API}/environment/budget/zakya-api", json={"value": {"limits": ["lots"]}})
    assert bad.status_code == 422
    assert admin.put(f"{API}/environment/setting/timezone", json={"value": "Asia/Kolkata"}).status_code == 200
    assert [e["name"] for e in operator.get(f"{API}/environment").json()["items"]] == ["zakya", "timezone"]
    key = new_master_key()
    rotated = admin.post(f"{API}/secrets:rotate-key", json={"key": key})
    assert rotated.status_code == 200 and rotated.json() == {
        "secrets": 1,
        "key_id": MasterKey.parse(key).key_id,
    }
    assert admin.post(f"{API}/secrets:rotate-key", json={"key": "short"}).status_code == 422
    assert admin.delete(f"{API}/secrets/zakya-secret").status_code == 200
    assert admin.delete(f"{API}/secrets/zakya-secret").status_code == 404


def test_without_a_master_key_the_server_runs_and_a_db_secret_fails_clearly(state: AppState):
    admin = _client(state, "admin")
    assert admin.get(f"{API}/secrets").status_code == 200
    refused = admin.put(f"{API}/secrets/x", json={"value": "v"})
    assert refused.status_code == 503 and "FEATHER_MASTER_KEY" in refused.text


def test_the_connections_page_shows_settings_and_replaces_a_secret(keyed: AppState):
    Environment(keyed.store, keyed.clock, "dev", keyed.master_key).put(ADMIN, "connection", "zakya", ZAKYA)
    browser = sign_in(create_app(state=keyed, loops=lambda s: []), "boss", "admin")
    page = browser.get("/connections").text
    assert "Environment connections" in page and "<code>organization_id</code>" in page and "1000.ABC" in page
    assert 'action="/secrets/zakya-secret"' in page and 'type="password"' in page and "not set" in page
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', page)
    assert csrf
    data = {"csrf_token": csrf.group(1), "value": "s3cr3t", "back": "/connections#env-zakya"}
    replaced = browser.post("/secrets/zakya-secret", data=data)
    assert replaced.status_code == 303 and replaced.headers["location"] == "/connections#env-zakya"
    after = browser.get("/connections").text
    assert "set, updated" in after and "by boss" in after and "s3cr3t" not in after
    saved = browser.post(
        "/connections/zakya/settings",
        data={"csrf_token": csrf.group(1), "key": "organization_id", "value": "7"},
    )
    assert saved.status_code == 303
    operator = sign_in(create_app(state=keyed, loops=lambda s: []), "ops2", "operator")
    token = re.search(r'name="csrf_token" value="([^"]+)"', operator.get("/connections").text)
    assert token
    denied = operator.post("/secrets/zakya-secret", data={"csrf_token": token.group(1), "value": "x"})
    assert denied.status_code == 403


# Declared webhooks (the sponsor's decision D) and activating flows (task 5).


def _spec(every_s: float = 3600) -> dict[str, Any]:
    return {
        "name": FLOW,
        "overlap": "queue",
        "catch_up": "latest",
        "enabled": True,
        "datasets": [{"name": f"{PACKAGE}/invoices"}],
        "routes": [],
        "triggers": [{"kind": "every", "seconds": every_s}],
    }


def _installed(state: AppState, tmp_path: Path, webhooks: dict[str, Any] | None = None) -> Packages:
    registries = Registries(state.store, state.clock)
    wheel = sample_wheel(tmp_path / "dist")
    try:
        registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    except VersionExists:
        pass  # published by an earlier call of this test

    def checker(content: bytes, manifest: dict[str, Any], **_: Any) -> LoadCheck:
        flows = [f["name"] for f in manifest["flows"]]
        return LoadCheck(True, flows, ["invoices"], [], [], None, 0.1, [], {FLOW: _spec()}, webhooks or {})

    packages = Packages(state.store, state.clock, registries, env="dev", checker=checker)
    packages.install(ADMIN, PACKAGE, "1.0.0")
    return packages


def test_a_declared_webhook_is_verified_deduplicated_and_kept_raw(
    keyed: AppState, envs: Environment, tmp_path: Path
):
    packages = _installed(keyed, tmp_path, {"invoice-paid": HOOK})
    envs.set_secret(ADMIN, "hook-secret", "whsec")
    client = TestClient(create_app(state=keyed, loops=lambda s: []))
    url = f"/ingress/default/{PACKAGE}/invoice-paid"
    body = b'{"id": "inv-1", "updated_at": "2026-11-09T09:00:00Z"}'
    signed = {"X-Signature": hmac.new(b"whsec", body, hashlib.sha256).hexdigest()}
    assert client.post(url, content=body, headers=signed).status_code == 404  # its flow isn't active
    packages.activate(OPERATOR, FLOW, "1.0.0", reason="live")
    first = client.post(url, content=body, headers=signed)
    assert (first.status_code, first.json()) == (200, {"status": "accepted"})
    again = client.post(url, content=body, headers=signed)
    assert (again.status_code, again.json()) == (200, {"status": "duplicate"})
    assert client.post(url, content=body, headers={"X-Signature": "00" * 32}).status_code == 401
    assert client.post(url, content=b'{"no": "id"}', headers={
        "X-Signature": hmac.new(b"whsec", b'{"no": "id"}', hashlib.sha256).hexdigest()
    }).status_code == 400  # fmt: skip
    assert client.post(f"/ingress/default/{PACKAGE}/other", content=body, headers=signed).status_code == 404
    [event] = keyed.store.webhooks.events(PACKAGE, "invoice-paid")
    assert (event.key, event.stamp, event.body) == ("inv-1", "2026-11-09T09:00:00Z", body)
    envs.delete_secret(ADMIN, "hook-secret")
    assert client.post(url, content=body, headers=signed).status_code == 503  # its secret can't be had


def test_a_declared_webhook_for_a_dataset_the_package_doesnt_define_is_refused(
    keyed: AppState, tmp_path: Path
):
    with pytest.raises(InstallFailed, match="doesn't define"):
        _installed(keyed, tmp_path, {"h": {**HOOK, "dataset": "orders"}})
    with pytest.raises(InstallFailed, match="not valid"):
        _installed(keyed, tmp_path / "again", {"h": {**HOOK, "verify": {"scheme": "md5"}}})


def test_a_flow_activated_while_the_server_runs_fires_on_time_with_no_restart(
    keyed: AppState, tmp_path: Path
):
    clock = keyed.clock
    assert isinstance(clock, FrozenClock)
    packages = _installed(keyed, tmp_path)
    catalogue = Catalogue(keyed.store, monotonic=lambda: 0.0)  # no periodic refresh: only a change is read
    catalogue.listen()
    assert catalogue.tick(clock.now()) == []
    done = packages.activate(OPERATOR, FLOW, "1.0.0", reason="go live")
    assert done["state"] == "active"
    assert [f.name for f in keyed.store.catalog.flows()] == [FLOW]  # the dashboard shows it
    assert catalogue.tick(clock.now()) == []  # the first tick only marks the start
    clock.advance(timedelta(minutes=59))
    assert catalogue.tick(clock.now()) == []
    clock.advance(timedelta(minutes=1))
    [run_id] = catalogue.tick(clock.now())
    run = keyed.store.runs.get_run(run_id)
    assert (run.flow, run.status, run.params["package"], run.params["version"]) == (
        FLOW,
        "queued",
        PACKAGE,
        "1.0.0",
    )
    packages.deactivate(OPERATOR, FLOW, reason="pause")
    clock.advance(timedelta(hours=2))
    assert catalogue.tick(clock.now()) == []
    audit = [a.action for a in keyed.store.audit.list_audit() if a.target == f"flow:{FLOW}"]
    assert "activate_flow" in audit and "deactivate_flow" in audit
    catalogue.close()


def test_the_api_activates_and_deactivates(keyed: AppState, tmp_path: Path):
    _installed(keyed, tmp_path)
    operator = _client(keyed, "operator")
    done = operator.post(f"{API}/flows/{FLOW}:activate", json={"version": "1.0.0", "reason": "live"})
    assert done.status_code == 200 and done.json()["state"] == "active"
    assert operator.post(f"{API}/flows/{FLOW}:activate", json={"version": "9.0.0"}).status_code == 404
    off = operator.post(f"{API}/flows/{FLOW}:deactivate", json={})
    assert off.status_code == 200 and off.json()["state"] == "inactive"
