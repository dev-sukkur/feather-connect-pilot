"""The roles viewer, operator, developer and admin, and the grants approver and pii-reader (design §13.2, flow packages
§11, M2 plan 2 O2): one check in the service layer, used alike by the API, the dashboard and the CLI; a token's role is
capped by its user's; refusals, role and grant changes are audited."""

import re
from datetime import timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

from feather_connect.clock import FrozenClock
from feather_connect.storage.port import Role, Store
from feather_server.app import create_app
from feather_server.auth import Actor, hash_password, issue_token, token_digest
from feather_server.services.access import NEEDS, Capability, Forbidden, can, lower, require
from feather_server.services.accounts import CLI, Accounts
from feather_server.services.actions import CAPABILITIES
from feather_server.testing import PASSWORD, World, sign_in

API = "/api/v1"
ADMIN = Actor("human", "root", "admin", "root")
OPERATOR = Actor("human", "op", "operator", "op")


def _bearer(world: World, role: Role, user: str | None) -> TestClient:
    token = issue_token(world.store, f"t-{role}-{user}", timedelta(days=1), role=role, created_by=user)
    return TestClient(
        create_app(state=world.state, loops=lambda s: []),
        base_url="http://localhost:8000",
        headers={"Authorization": f"Bearer {token}"},
    )


def _user(store: Store, name: str, role: Role) -> None:
    store.users.create_user(name, hash_password(PASSWORD), role)


def _refusals(store: Store) -> list[tuple[str, str, str]]:
    return [(a.actor, a.action, a.result) for a in store.audit.list_audit() if a.result.startswith("refused")]


# The check.


def test_an_operator_may_not_install_a_package_and_the_refusal_is_audited(store: Store) -> None:
    with pytest.raises(Forbidden, match="packages.install needs the developer role"):
        require(OPERATOR, "packages.install", store.audit, "package:shop")
    require(ADMIN, "packages.install", store.audit, "package:shop")
    [row] = [a for a in store.audit.list_audit() if a.action == "packages.install"]
    assert (row.actor, row.target, row.request, row.result) == (
        "op",
        "package:shop",
        {"role": "operator"},
        "refused: needs the developer role",
    )


VIEWER = Actor("human", "vi", "viewer", "vi")
DEVELOPER = Actor("human", "dev", "developer", "dev")


def test_the_capabilities_are_split_between_the_roles_by_design_13_2() -> None:  # M2 plan 2 O2
    admin_only: set[Capability] = {c for c, role in NEEDS.items() if role == "admin"}
    developer: set[Capability] = {c for c, role in NEEDS.items() if role == "developer"}
    assert admin_only == {"users.manage", "tokens.manage", "roles.manage", "packages.publish"}
    assert developer == {
        "packages.install",
        "packages.promote",
        "packages.uninstall",
        "registries.manage",
        "packages.yank",
        "environment.manage",
        "secrets.manage",
        "alerts.manage",
    }
    assert NEEDS["flows.activate"] == "operator"
    granted = {"holds.decide", "pii.unmask"}
    assert all(
        NEEDS[c] == "operator" for c in CAPABILITIES.values() if c not in granted
    )  # M1's stay an operator's
    assert all(can(ADMIN, c) for c in NEEDS) and not any(can(OPERATOR, c) for c in admin_only | developer)
    assert all(can(DEVELOPER, c) for c in developer) and not any(can(DEVELOPER, c) for c in admin_only)
    assert not any(can(VIEWER, c) for c in NEEDS)  # a viewer only sees
    assert lower("admin", "operator") == lower("operator", "admin") == "operator"
    assert lower("developer", "viewer") == "viewer"


def test_a_protected_environment_keeps_a_developers_rights_for_an_admin() -> None:
    protected = Actor("human", "dev", "developer", "dev", protected=True)
    assert not can(protected, "environment.manage") and not can(protected, "packages.install")
    assert can(protected, "runs.start")  # what an operator does stays
    assert can(Actor("human", "root", "admin", "root", protected=True), "environment.manage")


def test_deciding_holds_and_unmasking_need_their_grants(store: Store) -> None:
    assert not can(OPERATOR, "holds.decide") and not can(OPERATOR, "pii.unmask")
    approver = Actor("human", "ap", "viewer", "ap", grants=frozenset({"approver"}))
    assert (
        can(approver, "holds.decide") and not can(approver, "pii.unmask") and not can(approver, "runs.start")
    )
    reader = Actor("human", "pr", "operator", "pr", grants=frozenset({"pii-reader"}))
    assert can(reader, "pii.unmask") and not can(reader, "holds.decide")
    assert can(ADMIN, "holds.decide") and can(ADMIN, "pii.unmask")  # an admin, who can grant them anyway
    with pytest.raises(Forbidden, match="needs the approver grant"):
        require(OPERATOR, "holds.decide", store.audit, "route:sap")
    assert _refusals(store) == [("op", "holds.decide", "refused: needs the approver grant")]


def test_grants_are_set_audited_and_reach_the_users_tokens(world: World) -> None:
    accounts = Accounts(world.store, world.clock)
    accounts.create_user(CLI, "root", "pw")
    accounts.create_user(CLI, "ann", "pw")
    assert accounts.set_grants(ADMIN, "ann", ["approver"], reason="month end").grants == ("approver",)
    with pytest.raises(ValueError, match="not a grant"):
        accounts.set_grants(ADMIN, "ann", ["boss"])
    with pytest.raises(Forbidden):
        accounts.set_grants(OPERATOR, "ann", ["pii-reader"])
    [row] = [a for a in world.store.audit.list_audit() if a.action == "set_grants" and a.result == "ok"]
    assert row.request == {"from": [], "to": ["approver"], "reason": "month end"}
    api = _bearer(world, "operator", "ann")
    held = api.post(f"{API}/routes/r/deliveries/v:approve", json={})
    assert held.status_code == 404  # past the check: there is no such delivery, but ann may decide
    accounts.set_grants(ADMIN, "ann", [])
    assert api.post(f"{API}/routes/r/deliveries/v:approve", json={}).status_code == 403


def test_an_operator_does_what_an_m1_operator_does(world: World) -> None:
    run_id = world.actions.run_flow(OPERATOR, "orders")
    world.actions.pause_flow(OPERATOR, "orders")
    world.actions.resume_flow(OPERATOR, "orders")
    assert world.store.runs.get_run(run_id) is not None and _refusals(world.store) == []


# Users, roles and tokens in the service.


def test_the_first_user_is_an_admin_and_the_next_an_operator(world: World) -> None:
    accounts = Accounts(world.store, world.clock)
    assert accounts.create_user(CLI, "root", "pw").role == "admin"
    assert accounts.create_user(CLI, "ann", "pw").role == "operator"
    assert accounts.create_user(CLI, "bob", "pw", "admin").role == "admin"
    assert accounts.create_user(CLI, "cy", "pw", "viewer").role == "viewer"
    with pytest.raises(ValueError, match="not a role"):
        accounts.create_user(CLI, "dee", "pw", "owner")


def test_a_role_change_is_audited_and_the_last_admin_keeps_theirs(world: World) -> None:
    accounts = Accounts(world.store, world.clock)
    accounts.create_user(CLI, "root", "pw")
    accounts.create_user(CLI, "ann", "pw")
    with pytest.raises(ValueError, match="only admin"):
        accounts.set_role(ADMIN, "root", "operator")
    accounts.set_role(ADMIN, "ann", "admin", reason="on call")
    accounts.set_role(ADMIN, "root", "operator")
    with pytest.raises(KeyError):
        accounts.set_role(ADMIN, "nobody", "admin")
    changes = [
        (a.target, a.request, a.result) for a in world.store.audit.list_audit() if a.action == "set_role"
    ][::-1]
    assert changes == [
        (
            "user:root",
            {"from": "admin", "to": "operator", "reason": None},
            "failed: root is the only admin: make another user an admin first",
        ),
        ("user:ann", {"from": "operator", "to": "admin", "reason": "on call"}, "ok"),
        ("user:root", {"from": "admin", "to": "operator", "reason": None}, "ok"),
        ("user:nobody", {"from": None, "to": "admin", "reason": None}, "failed: \"no user named 'nobody'\""),
    ]


def test_an_operator_manages_no_users_roles_or_tokens(world: World) -> None:
    accounts = Accounts(world.store, world.clock)
    _user(world.store, "op", "operator")
    for attempt in (
        lambda: accounts.users(OPERATOR),
        lambda: accounts.create_user(OPERATOR, "eve", "pw", "admin"),
        lambda: accounts.set_role(OPERATOR, "op", "admin"),
        lambda: accounts.create_token(OPERATOR, "mine", None),
    ):
        with pytest.raises(Forbidden):
            attempt()
    assert [action for _, action, _ in _refusals(world.store)][::-1] == [
        "users.manage",
        "users.manage",
        "roles.manage",
        "tokens.manage",
    ]
    assert world.store.users.get_user("eve") is None
    assert world.store.users.get_user("op").role == "operator"  # type: ignore[union-attr]


def test_a_token_has_no_higher_a_role_than_its_user(world: World) -> None:
    accounts = Accounts(world.store, world.clock)
    _user(world.store, "root", "admin")
    _user(world.store, "ann", "operator")
    with pytest.raises(ValueError, match="higher role than its user ann, operator"):
        accounts.create_token(ADMIN, "ann-ci", None, "admin", user="ann")
    made = {
        "ann-ci": accounts.create_token(ADMIN, "ann-ci", None, user="ann"),  # takes ann's role
        "root-ci": accounts.create_token(ADMIN, "root-ci", None),  # takes root's
        "robot": accounts.create_token(CLI, "robot", None),  # no user: an operator
    }
    found = {name: world.store.users.find_token(token_digest(token)) for name, token in made.items()}
    assert {name: (t.role, t.created_by) for name, t in found.items() if t} == {
        "ann-ci": ("operator", "ann"),
        "root-ci": ("admin", "root"),
        "robot": ("operator", None),
    }


# Through the API: the remote CLI's way in.


def test_an_operator_token_gets_403_with_the_usual_envelope(world: World) -> None:
    _user(world.store, "op", "operator")
    api = _bearer(world, "operator", "op")
    for method, path, body in (
        ("GET", "/users", None),
        ("POST", "/users", {"username": "eve", "password": "pw", "role": "admin"}),
        ("PUT", "/users/op/role", {"role": "admin"}),
        ("POST", "/tokens", {"name": "more", "role": "admin"}),
    ):
        answer = api.request(method, API + path, json=body)
        assert answer.status_code == 403 and answer.headers["content-type"] == "application/problem+json"
        problem = answer.json()
        assert problem["title"] == "Forbidden" and problem["role"] == "admin"
        assert problem["detail"].endswith("needs the admin role")
    assert len(_refusals(world.store)) == 4 and world.store.users.get_user("eve") is None
    assert api.post(f"{API}/flows/orders:run").status_code == 202  # the operator's own actions still work


def test_an_admin_token_manages_users_roles_and_tokens(world: World) -> None:
    _user(world.store, "root", "admin")
    api = _bearer(world, "admin", "root")
    made = api.post(f"{API}/users", json={"username": "ann", "password": "pw"})
    assert made.status_code == 201 and made.json()["role"] == "operator"
    assert api.post(f"{API}/users", json={"username": "ann", "password": "pw"}).status_code == 409
    changed = api.put(f"{API}/users/ann/role", json={"role": "admin", "reason": "on call"})
    assert changed.status_code == 200 and changed.json()["role"] == "admin"
    assert api.put(f"{API}/users/ann/role", json={"role": "boss"}).status_code == 422
    assert api.put(f"{API}/users/nobody/role", json={"role": "admin"}).status_code == 404
    listed = api.get(f"{API}/users").json()["items"]
    assert [(u["username"], u["role"]) for u in listed] == [("ann", "admin"), ("root", "admin")]
    assert "password_hash" not in listed[0]
    token = api.post(f"{API}/tokens", json={"name": "ci", "role": "operator", "ttl_days": 30}).json()["token"]
    me = TestClient(api.app, headers={"Authorization": f"Bearer {token}"}).get(f"{API}/me").json()
    assert me == {"kind": "token", "name": "ci", "role": "operator", "user": "root"}


def test_a_demoted_users_tokens_are_demoted_too(world: World) -> None:
    _user(world.store, "root", "admin")
    _user(world.store, "ann", "admin")
    api = _bearer(world, "admin", "ann")
    assert api.get(f"{API}/me").json()["role"] == "admin" and api.get(f"{API}/users").status_code == 200
    world.store.users.set_role("ann", "operator")
    assert api.get(f"{API}/me").json()["role"] == "operator"
    assert api.get(f"{API}/users").status_code == 403


def test_a_token_whose_user_is_gone_is_not_valid(world: World) -> None:
    api = _bearer(world, "admin", "ghost")
    assert api.get(f"{API}/me").status_code == 401


def test_tokens_from_before_the_roles_keep_working_as_admins(world: World) -> None:
    api = _bearer(world, "admin", None)  # as migration 0015 leaves an M1 token: admin, with no user
    assert api.get(f"{API}/me").json() == {
        "kind": "token",
        "name": "t-admin-None",
        "role": "admin",
        "user": None,
    }
    assert api.get(f"{API}/users").status_code == 200


# In the dashboard.


def _csrf(client: TestClient) -> str:
    found = re.search(r'<meta name="csrf-token" content="([^"]+)"', client.get("/").text)
    assert found
    return found.group(1)


def test_an_operator_sees_their_role_but_no_users_page(world: World) -> None:
    browser = sign_in(create_app(state=world.state, loops=lambda s: []), "op", "operator")
    home = browser.get("/").text
    assert "· operator" in home and 'href="/users"' not in home
    page = browser.get("/users")
    assert (
        page.status_code == 403 and "This needs the admin role, and you have the operator role" in page.text
    )
    _user(world.store, "ann", "operator")
    refused = browser.post("/users/ann/role", data={"role": "admin", "csrf_token": _csrf(browser)})
    assert refused.status_code == 403 and world.store.users.get_user("ann").role == "operator"  # type: ignore[union-attr]


def test_an_admin_changes_a_role_on_the_users_page(world: World) -> None:
    browser = sign_in(create_app(state=world.state, loops=lambda s: []), "root", "admin")
    _user(world.store, "ann", "operator")
    page = browser.get("/users")
    assert page.status_code == 200 and 'href="/users" aria-current="page"' in page.text
    assert "· admin" in page.text and "(you)" in page.text
    done = browser.post("/users/ann/role", data={"role": "admin", "csrf_token": _csrf(browser)})
    assert done.status_code == 200 and "ann is now an admin." in done.text
    assert world.store.users.get_user("ann").role == "admin"  # type: ignore[union-attr]
    browser.post("/users/ann/role", data={"role": "operator", "csrf_token": _csrf(browser)})
    last = browser.post("/users/root/role", data={"role": "operator", "csrf_token": _csrf(browser)})
    assert last.status_code == 422 and "root is the only admin" in last.text
    assert (
        browser.post("/users/nobody/role", data={"role": "admin", "csrf_token": _csrf(browser)}).status_code
        == 404
    )


def _signs_in(app: Any, username: str, password: str) -> bool:
    browser = TestClient(app, base_url="http://localhost:8000", follow_redirects=False)
    found = re.search(r'name="csrf_token" value="([^"]+)"', browser.get("/login").text)
    assert found
    form = {"username": username, "password": password, "csrf_token": found.group(1)}
    browser.post("/login", data=form)
    return browser.get("/").status_code == 200


def test_an_admin_adds_a_user_and_sets_a_password_on_the_users_page(world: World) -> None:
    app = create_app(state=world.state, loops=lambda s: [])
    browser = sign_in(app, "root", "admin")
    page = browser.get("/users").text
    assert 'action="/users"' in page and "Add a user" in page
    form = {
        "username": " ann ",
        "password": "first password",
        "role": "operator",
        "csrf_token": _csrf(browser),
    }
    done = browser.post("/users", data=form)
    assert done.status_code == 200 and "ann can sign in now, as an operator." in done.text
    assert world.store.users.get_user("ann").role == "operator"  # type: ignore[union-attr]
    assert "first password" not in str([a.request for a in world.store.audit.list_audit()])
    again = browser.post("/users", data={**form, "csrf_token": _csrf(browser)})
    assert again.status_code == 409 and "There is a user named &#39;ann&#39; already." in again.text
    bad = browser.post(
        "/users", data={**form, "username": "bob", "role": "boss", "csrf_token": _csrf(browser)}
    )
    assert bad.status_code == 422 and world.store.users.get_user("bob") is None
    assert _signs_in(app, "ann", "first password")

    reset = browser.post(
        "/users/ann/password", data={"password": "second password", "csrf_token": _csrf(browser)}
    )
    assert reset.status_code == 200 and "ann has a new password." in reset.text
    assert _signs_in(app, "ann", "second password") and not _signs_in(app, "ann", "first password")
    gone = browser.post("/users/nobody/password", data={"password": "x", "csrf_token": _csrf(browser)})
    assert gone.status_code == 404
    actions = [(a.action, a.target, a.result) for a in world.store.audit.list_audit()]
    assert ("create_user", "user:ann", "ok") in actions and ("set_password", "user:ann", "ok") in actions


def test_an_operator_can_t_add_users_or_set_passwords(world: World) -> None:
    browser = sign_in(create_app(state=world.state, loops=lambda s: []), "op", "operator")
    _user(world.store, "ann", "operator")
    form = {"username": "eve", "password": "pw", "role": "admin", "csrf_token": _csrf(browser)}
    assert browser.post("/users", data=form).status_code == 403
    assert (
        browser.post("/users/ann/password", data={"password": "pw", "csrf_token": _csrf(browser)}).status_code
        == 403
    )
    assert world.store.users.get_user("eve") is None


# A second tenant (ADR-0013 rule 6).


@pytest.mark.postgres
def test_a_second_tenants_admin_cant_touch_the_first_tenants_users(
    pg: Any, pg_url: str, clock: FrozenClock, pg_connect: Any
) -> None:
    first, _ = pg_connect(pg_url, clock=clock, tenant="default")
    second, _ = pg_connect(pg_url, clock=clock, tenant="t2")
    Accounts(first, clock).create_user(CLI, "ann", "pw")
    Accounts(first, clock).create_user(CLI, "op", "pw")
    theirs = Accounts(second, clock)
    theirs.create_user(CLI, "boss", "pw")
    boss = Actor("human", "boss", "admin", "boss")
    assert [u.username for u in theirs.users(boss)] == ["boss"]
    with pytest.raises(KeyError):
        theirs.set_role(boss, "op", "admin")
    with pytest.raises(KeyError):
        theirs.create_token(boss, "steal", None, user="ann")
    assert first.users.get_user("op").role == "operator"
    assert second.users.get_user("ann") is None
