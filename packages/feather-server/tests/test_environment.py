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
from feather_connect.vault import Keyring, MasterKey, Vault, new_master_key
from feather_server.app import AppState, create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.access import Forbidden
from feather_server.services.environment import ConfigProblem, Environment, coerce_setting
from feather_server.services.packages import InstallFailed, Packages
from feather_server.services.registries import Registries, VersionExists
from feather_server.testing import sample_wheel, sign_in

API = "/api/v1"
ADMIN = Actor("human", "root", "admin", "root")
OPERATOR = Actor("human", "ops", "operator", "ops")
PACKAGE, FLOW = "acme-invoices", "acme-invoices/invoices"
SECRET_REF = "${secret:db:zakya/client_secret}"
ZAKYA = {
    "connector": "zakya",
    "base_url": "https://api.zakya.in",
    "organization_id": 60001,
    "client_id": "1000.ABC",
    "auth": {"client_secret": SECRET_REF, "refresh_token": "${secret:db:zakya/refresh_token}"},
}
HOOK_SECRET = f"{PACKAGE}/webhooks/invoice-paid"
HOOK = {
    "dataset": "invoices",
    "verify": {"scheme": "hmac-sha256", "header": "X-Signature"},
    "secret": f"${{secret:db:{HOOK_SECRET}}}",
    "key": "$.id",
    "stamp": "$.updated_at",
    "ack": 200,
}


def _key() -> MasterKey:
    return MasterKey.parse(new_master_key())


@pytest.fixture
def keyed(state: AppState) -> AppState:
    state.vault = Vault(state.store.secrets, "dev", _key())
    return state


@pytest.fixture
def envs(keyed: AppState) -> Iterator[Environment]:
    assert keyed.vault is not None
    environment = Environment(keyed.store, keyed.clock, "dev", keyed.vault)
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


def _audit(envs: Environment, action: str) -> list[Any]:
    rows = [a for a in envs._store.audit.list_audit() if a.action == action]  # pyright: ignore[reportPrivateUsage]
    return rows[::-1]  # oldest first


# The environment's own configuration (task 5a, the sponsor's decision B).


def test_a_connections_settings_are_plain_and_every_change_is_audited_with_old_and_new(envs: Environment):
    envs.put(ADMIN, "connection", "zakya", ZAKYA, reason="carried over")
    envs.update(ADMIN, "zakya", {"organization_id": 60002, "client_id": None}, reason="new org")
    view = envs.connection("zakya")
    assert view["settings"]["organization_id"] == 60002 and "client_id" not in view["settings"]
    assert view["secrets"]["client_secret"] == {"ref": SECRET_REF, "set": False, "needs_reentry": False}
    assert {"client_secret", "refresh_token"} <= set(view["secret_fields"])
    assert "client_id" not in view["secret_fields"]
    first, second = _audit(envs, "set_connection")
    assert first.request["old"] is None and first.request["new"]["organization_id"] == 60001
    assert second.request["old"]["organization_id"] == 60001
    assert second.request["new"]["organization_id"] == 60002 and second.request["reason"] == "new org"


def test_a_plain_value_for_a_secret_field_is_refused_and_not_kept(envs: Environment):
    for value, why in (
        ({**ZAKYA, "client_secret": "s3cr3t"}, "client_secret is secret"),
        ({**ZAKYA, "oauth": {"extra": {"private_key": "s3cr3t"}}}, "oauth.extra.private_key is secret"),
        ({**ZAKYA, "api_token": "s3cr3t"}, "api_token is secret"),
        ({**ZAKYA, "auth": {"client_secret": "s3cr3t"}}, "must be"),
    ):
        with pytest.raises(ConfigProblem, match=why) as caught:
            envs.put(ADMIN, "connection", "zakya", value)
        assert "s3cr3t" not in str(caught.value)
    assert envs.entries() == []
    assert not any("s3cr3t" in json.dumps(a.request) for a in envs._store.audit.list_audit())  # pyright: ignore[reportPrivateUsage]


def test_an_admin_cant_point_a_connection_at_another_secret_or_a_variable(envs: Environment):
    for ref in (
        "${secret:db:erp/password}",  # another connection's
        "${secret:env:FEATHER_MASTER_KEY}",  # the server's own
        "${secret:env:ZAKYA_SECRET}",  # the database holds db references only
        "${secret:file:/etc/feather/master.key}",
    ):
        with pytest.raises(ConfigProblem, match=r"must be \$\{secret:db:zakya/client_secret\}"):
            envs.put(ADMIN, "connection", "zakya", {**ZAKYA, "auth": {"client_secret": ref}})


def test_an_entry_is_checked_as_a_server_would_load_it(envs: Environment):
    with pytest.raises(ConfigProblem, match="no connector 'nothing' is installed"):
        envs.put(ADMIN, "connection", "x", {"connector": "nothing"})
    with pytest.raises(ConfigProblem, match="no budget"):
        envs.put(ADMIN, "connection", "zakya", {**ZAKYA, "budget": "missing"})
    with pytest.raises(ConfigProblem, match="time zone"):
        envs.put(ADMIN, "setting", "timezone", "Mars/Olympus")
    with pytest.raises(ConfigProblem, match="not valid"):
        envs.put(ADMIN, "budget", "api", {"limits": ["lots"]})
    envs.put(ADMIN, "budget", "api", {"limits": ["100/minute"]})
    envs.put(ADMIN, "connection", "zakya", {**ZAKYA, "budget": "api"})
    with pytest.raises(ConfigProblem, match="used by the connections zakya"):
        envs.remove(ADMIN, "budget", "api")  # it would break the connection at the next start


def test_changing_where_a_connection_goes_clears_its_secrets_for_re_entry(envs: Environment):
    envs.put(ADMIN, "connection", "zakya", ZAKYA)
    envs.set_secret(ADMIN, "zakya/client_secret", "s3cr3t")
    envs.update(ADMIN, "zakya", {"organization_id": 7})  # not where it goes: the secret stays
    assert envs.connection("zakya")["secrets"]["client_secret"]["set"] is True
    envs.update(ADMIN, "zakya", {"base_url": "https://evil.example.com"}, reason="moved")
    shown = envs.connection("zakya")["secrets"]["client_secret"]
    assert shown == {"ref": SECRET_REF, "set": False, "needs_reentry": True}
    assert "_needs_reentry" not in envs.connection("zakya")["settings"]
    assert [s["name"] for s in envs.secrets()] == []
    [cleared] = _audit(envs, "clear_secrets")
    assert cleared.request["secrets"] == ["zakya/client_secret"] and cleared.request["reason"] == "moved"
    assert cleared.request["because"]["new"] == {"base_url": "https://evil.example.com"}
    envs.set_secret(ADMIN, "zakya/client_secret", "again")
    assert envs.connection("zakya")["secrets"]["client_secret"]["set"] is True
    assert envs.connection("zakya")["secrets"]["refresh_token"]["needs_reentry"] is True  # still to give


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
        "connections:\n  zakya: {connector: zakya, organization_id: 60001, budget: zakya-api,"
        " auth: {client_secret: '${secret:env:ZAKYA_SECRET}'}}\n"
        "budgets:\n  zakya-api: {limits: [100/minute]}\nsettings:\n  timezone: Asia/Kolkata\n",
        encoding="utf-8",
    )
    done = envs.import_file(ADMIN, path)
    assert [(e.kind, e.name) for e in done] == [
        ("connection", "zakya"),
        ("budget", "zakya-api"),
        ("setting", "timezone"),
    ]
    # Its secret now lives in the environment's store, to be set there.
    assert envs.connection("zakya")["secrets"]["client_secret"]["ref"] == SECRET_REF
    path.write_text(
        "connections:\n  a: {connector: zakya}\n  b: {connector: zakya, client_secret: x}\n", encoding="utf-8"
    )
    with pytest.raises(ConfigProblem):
        envs.import_file(ADMIN, path)
    assert envs._store.env_config.entry("dev", "connection", "a") is None  # pyright: ignore[reportPrivateUsage]


def test_a_setting_typed_in_a_form_takes_its_type_from_the_connector():
    assert coerce_setting("zakya", "organization_id", "60001") in (60001, "60001")
    assert coerce_setting("nothing", "page_size", "50") == 50
    assert coerce_setting("nothing", "verify_tls", "false") is False
    assert coerce_setting("nothing", "base_url", "https://x") == "https://x"
    assert coerce_setting("http", "timeout", "12.5") == 12.5


# Secrets (the sponsor's decision C).


def test_a_secret_is_shown_as_set_never_as_its_value_and_is_audited_by_name(envs: Environment):
    envs.put(ADMIN, "connection", "zakya", ZAKYA)
    assert envs.set_secret(ADMIN, "zakya/client_secret", "s3cr3t", reason="first") is False
    assert envs.set_secret(ADMIN, "zakya/client_secret", "n3w", reason="rotated by zakya") is True
    shown = envs.connection("zakya")["secrets"]["client_secret"]
    assert shown["set"] is True and shown["updated_by"] == "root" and "updated_at" in shown
    assert [s["name"] for s in envs.secrets()] == ["zakya/client_secret"]
    assert envs.open("zakya/client_secret") == "n3w"
    rows = _audit(envs, "set_secret")
    assert [r.request["replaced"] for r in rows] == [False, True]
    assert not any(v in json.dumps(r.request) for r in rows for v in ("s3cr3t", "n3w"))
    envs.delete_secret(ADMIN, "zakya/client_secret")
    assert envs.connection("zakya")["secrets"]["client_secret"]["set"] is False


def test_the_api_takes_secrets_write_only(keyed: AppState):
    admin, operator = _client(keyed, "admin"), _client(keyed, "operator")
    put = admin.put(f"{API}/connections/zakya", json={"value": ZAKYA, "reason": "seed"})
    assert put.status_code == 200, put.text
    assert put.json()["settings"]["client_id"] == "1000.ABC"
    refused = admin.patch(f"{API}/connections/zakya", json={"changes": {"client_secret": "s3cr3t"}})
    assert refused.status_code == 422 and "s3cr3t" not in refused.text
    url = f"{API}/secrets/zakya/client_secret"
    assert operator.put(url, json={"value": "s3cr3t"}).status_code == 403
    done = admin.put(url, json={"value": "s3cr3t", "reason": "seed"})
    assert done.status_code == 200 and done.json() == {"name": "zakya/client_secret", "replaced": False}
    listed = operator.get(f"{API}/secrets")
    assert [s["name"] for s in listed.json()["items"]] == [
        "zakya/client_secret"
    ] and "s3cr3t" not in listed.text
    every = operator.get(f"{API}/connections").json()
    assert every["env"] == "dev" and every["environment"][0]["secrets"]["client_secret"]["set"] is True
    assert "s3cr3t" not in operator.get(f"{API}/connections/zakya").text
    assert operator.get(url).status_code in (404, 405)  # there is no read-back
    bad = admin.put(f"{API}/environment/budget/zakya-api", json={"value": {"limits": ["lots"]}})
    assert bad.status_code == 422
    assert admin.put(f"{API}/environment/setting/timezone", json={"value": "Asia/Kolkata"}).status_code == 200
    assert [e["name"] for e in operator.get(f"{API}/environment").json()["items"]] == ["zakya", "timezone"]
    assert admin.delete(url).status_code == 200
    assert admin.delete(url).status_code == 404


def test_rotating_through_the_api_needs_the_new_key_current_and_refuses_a_stale_process(keyed: AppState):
    old, new = _key(), _key()
    keyed.vault = Vault(keyed.store.secrets, "dev", Keyring(old, (new,)))
    admin = _client(keyed, "admin")
    assert admin.put(f"{API}/secrets/zakya/client_secret", json={"value": "v"}).status_code == 200
    # The same database, as a process restarted with the new key current and the old one previous sees it.
    keyed.vault = Vault(keyed.store.secrets, "dev", Keyring(new, (old,)))
    fresh = _client(keyed, "admin")
    stale = fresh.put(f"{API}/secrets/zakya/client_secret", json={"value": "w"})
    assert stale.status_code == 409 and "rotate" in stale.text
    rotated = fresh.post(f"{API}/secrets:rotate-key", json={"reason": "yearly"})
    assert rotated.status_code == 200 and rotated.json() == {"secrets": 1, "key_id": new.key_id}
    assert fresh.put(f"{API}/secrets/zakya/client_secret", json={"value": "w"}).status_code == 200
    keyed.vault = Vault(keyed.store.secrets, "dev", Keyring(old, (new,)))
    assert _client(keyed, "admin").put(f"{API}/secrets/x/y", json={"value": "v"}).status_code == 409


def test_one_vault_serves_the_process(keyed: AppState):
    app = create_app(state=keyed, loops=lambda s: [])
    assert app.state.environment.vault is keyed.vault


def test_without_a_master_key_the_server_runs_and_a_db_secret_fails_clearly(state: AppState):
    admin = _client(state, "admin")
    assert admin.get(f"{API}/secrets").status_code == 200
    refused = admin.put(f"{API}/secrets/x", json={"value": "v"})
    assert refused.status_code == 503 and "FEATHER_MASTER_KEY" in refused.text


def test_the_connections_page_shows_settings_and_replaces_a_secret(keyed: AppState):
    assert keyed.vault is not None
    Environment(keyed.store, keyed.clock, "dev", keyed.vault).put(ADMIN, "connection", "zakya", ZAKYA)
    browser = sign_in(create_app(state=keyed, loops=lambda s: []), "boss", "admin")
    page = browser.get("/connections").text
    assert "Environment connections" in page and "<code>organization_id</code>" in page and "1000.ABC" in page
    assert 'action="/secrets/zakya/client_secret"' in page and 'type="password"' in page and "not set" in page
    assert "New value for client_secret of zakya" in page and "Setting of zakya" in page  # labels are unique
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', page)
    assert csrf
    data = {"csrf_token": csrf.group(1), "value": "s3cr3t", "back": "/connections#env-zakya"}
    replaced = browser.post("/secrets/zakya/client_secret", data=data)
    assert replaced.status_code == 303 and replaced.headers["location"] == "/connections#env-zakya"
    after = browser.get("/connections").text
    assert "set, updated" in after and "by boss" in after and "s3cr3t" not in after
    saved = browser.post(
        "/connections/zakya/settings",
        data={"csrf_token": csrf.group(1), "key": "page_size", "value": "7"},
    )
    assert saved.status_code == 303
    assert keyed.store.env_config.entry("dev", "connection", "zakya").value["page_size"] == 7  # pyright: ignore[reportOptionalMemberAccess]
    moved = browser.post(
        "/connections/zakya/settings",
        data={"csrf_token": csrf.group(1), "key": "base_url", "value": "https://elsewhere.example.com"},
    )
    assert moved.status_code == 303 and "needs re-entry" in browser.get("/connections").text
    operator = sign_in(create_app(state=keyed, loops=lambda s: []), "ops2", "operator")
    token = re.search(r'name="csrf_token" value="([^"]+)"', operator.get("/connections").text)
    assert token
    denied = operator.post("/secrets/zakya/client_secret", data={"csrf_token": token.group(1), "value": "x"})
    assert denied.status_code == 403


# Declared webhooks (the sponsor's decision D) and activating flows (task 5).


def _spec(every_s: float = 3600, **more: Any) -> dict[str, Any]:
    return {
        "name": FLOW,
        "overlap": "queue",
        "catch_up": "latest",
        "enabled": True,
        "datasets": [{"name": f"{PACKAGE}/invoices"}],
        "routes": [],
        "triggers": [{"kind": "every", "seconds": every_s}],
        **more,
    }


def _installed(
    state: AppState, tmp_path: Path, webhooks: dict[str, Any] | None = None, **spec: Any
) -> Packages:
    registries = Registries(state.store, state.clock)
    wheel = sample_wheel(tmp_path / "dist")
    try:
        registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    except VersionExists:
        pass  # published by an earlier call of this test

    def checker(content: bytes, manifest: dict[str, Any], **_: Any) -> LoadCheck:
        flows = [f["name"] for f in manifest["flows"]]
        return LoadCheck(
            True, flows, ["invoices"], [], [], None, 0.1, [], {FLOW: _spec(**spec)}, webhooks or {}
        )

    packages = Packages(state.store, state.clock, registries, env="dev", checker=checker)
    packages.install(ADMIN, PACKAGE, "1.0.0")
    return packages


def test_a_declared_webhook_is_verified_deduplicated_and_kept_raw(
    keyed: AppState, envs: Environment, tmp_path: Path
):
    packages = _installed(keyed, tmp_path, {"invoice-paid": HOOK})
    envs.set_secret(ADMIN, HOOK_SECRET, "whsec")
    client = TestClient(create_app(state=keyed, loops=lambda s: []))
    url = f"/ingress/default/{PACKAGE}/invoice-paid"
    body = b'{"id": "inv-1", "updated_at": "2026-11-09T09:00:00Z"}'
    signed = {"X-Signature": hmac.new(b"whsec", body, hashlib.sha256).hexdigest(), "X-Trace": "t-1"}
    assert client.post(url, content=body, headers=signed).status_code == 404  # its flow isn't active
    packages.activate(OPERATOR, FLOW, "1.0.0", reason="live")  # the catalogue hears of it at once
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
    assert event.headers["x-trace"] == "t-1" and "x-signature" not in event.headers  # never the signature
    envs.delete_secret(ADMIN, HOOK_SECRET)
    assert client.post(url, content=body, headers=signed).status_code == 503  # its secret can't be had


def test_a_bearer_webhook_keeps_no_token_and_an_unbound_secret_isnt_used(
    keyed: AppState, envs: Environment, tmp_path: Path
):
    bearer = {**HOOK, "verify": {"scheme": "bearer", "header": "X-Token"}}
    unbound = {**HOOK, "verify": {"scheme": "bearer"}, "secret": "${secret:db:zakya/client_secret}"}
    packages = _installed(keyed, tmp_path, {"invoice-paid": bearer, "stolen": unbound})
    packages.activate(OPERATOR, FLOW, "1.0.0")
    envs.set_secret(ADMIN, HOOK_SECRET, "tok")
    envs.set_secret(ADMIN, "zakya/client_secret", "zakya-value")
    client = TestClient(create_app(state=keyed, loops=lambda s: []))
    body = b'{"id": "1", "updated_at": "t"}'
    done = client.post(
        f"/ingress/default/{PACKAGE}/invoice-paid", content=body, headers={"X-Token": "Bearer tok"}
    )
    assert done.status_code == 200
    [event] = keyed.store.webhooks.events(PACKAGE, "invoice-paid")
    assert "x-token" not in event.headers
    refused = client.post(
        f"/ingress/default/{PACKAGE}/stolen", content=body, headers={"Authorization": "Bearer zakya-value"}
    )
    assert refused.status_code == 503 and keyed.store.webhooks.events(PACKAGE, "stolen") == []


def test_ingress_refuses_a_flood_before_reading_anything(keyed: AppState, monkeypatch: pytest.MonkeyPatch):
    from feather_server.ingress import RateLimiter

    app = create_app(state=keyed, loops=lambda s: [])
    app.state.ingress_rate = RateLimiter(2, keyed.clock)  # a frozen clock: two, then no more
    assert keyed.catalogue is not None
    reads: list[str] = []
    real = keyed.catalogue.webhook
    monkeypatch.setattr(keyed.catalogue, "webhook", lambda p, n: reads.append(n) or real(p, n))
    client = TestClient(app)
    answers = [client.post(f"/ingress/default/{PACKAGE}/x{i}", content=b"{}").status_code for i in range(6)]
    assert answers[:2] == [404, 404] and set(answers[2:]) == {429} and len(reads) == 2


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
    clock.advance(timedelta(hours=5))
    assert catalogue.tick(clock.now()) == []
    # Activated again: it fires an hour from now, with nothing caught up for the five hours it was off.
    packages.activate(OPERATOR, FLOW, "1.0.0", reason="back")
    assert catalogue.tick(clock.now()) == []
    clock.advance(timedelta(hours=1))
    assert len(catalogue.tick(clock.now())) == 1
    audit = [a.action for a in keyed.store.audit.list_audit() if a.target == f"flow:{FLOW}"]
    assert "activate_flow" in audit and "deactivate_flow" in audit
    catalogue.close()


def test_overlap_skip_counts_a_running_run(keyed: AppState, tmp_path: Path):
    clock = keyed.clock
    assert isinstance(clock, FrozenClock)
    packages = _installed(keyed, tmp_path, overlap="skip")
    packages.activate(OPERATOR, FLOW, "1.0.0")
    catalogue = Catalogue(keyed.store, monotonic=lambda: 0.0)
    catalogue.tick(clock.now())
    clock.advance(timedelta(hours=1))
    [first] = catalogue.tick(clock.now())
    keyed.store.runs.set_run_status(first, "running")
    clock.advance(timedelta(hours=1))
    [second] = catalogue.tick(clock.now())
    assert (keyed.store.runs.get_run(second).status, keyed.store.runs.get_run(second).reason) == (
        "skipped",
        "overlap",
    )


def test_a_disabled_flow_writes_no_run(keyed: AppState, tmp_path: Path):
    clock = keyed.clock
    assert isinstance(clock, FrozenClock)
    packages = _installed(keyed, tmp_path, enabled=False)
    packages.activate(OPERATOR, FLOW, "1.0.0")
    catalogue = Catalogue(keyed.store, monotonic=lambda: 0.0)
    for _ in range(3):
        clock.advance(timedelta(hours=1))
        assert catalogue.tick(clock.now()) == []
    assert keyed.store.runs.list_runs(FLOW) == []


def test_the_api_activates_and_deactivates(keyed: AppState, tmp_path: Path):
    _installed(keyed, tmp_path)
    operator = _client(keyed, "operator")
    done = operator.post(f"{API}/flows/{FLOW}:activate", json={"version": "1.0.0", "reason": "live"})
    assert done.status_code == 200 and done.json()["state"] == "active"
    assert operator.post(f"{API}/flows/{FLOW}:activate", json={"version": "9.0.0"}).status_code == 404
    off = operator.post(f"{API}/flows/{FLOW}:deactivate", json={})
    assert off.status_code == 200 and off.json()["state"] == "inactive"


def test_processed_webhook_events_are_deleted_after_the_retention_setting(keyed: AppState):
    from feather_connect.storage.port import WebhookEvent
    from feather_server.loops import default_loops, webhook_retention_days

    assert webhook_retention_days({}) == 30 and webhook_retention_days({"webhook_retention_days": 7}) == 7
    assert webhook_retention_days({"webhook_retention_days": 0}) == 30  # not a number of days: the default
    now = keyed.clock.now()
    for key, age, done in (("old", 40, True), ("waiting", 40, False), ("recent", 2, True)):
        keyed.store.webhooks.add(WebhookEvent(PACKAGE, "h", key, None, b"{}", {}, now - timedelta(days=age)))
        if done:
            keyed.store.webhooks.processed(PACKAGE, "h", [key], now)
    [retention] = [r for r in default_loops(keyed) if r.name == "retention"]
    retention._fn()  # pyright: ignore[reportPrivateUsage]
    assert sorted(e.key for e in keyed.store.webhooks.events(PACKAGE, "h")) == ["recent", "waiting"]
