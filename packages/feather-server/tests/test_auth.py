import hashlib
import logging
import re
from datetime import timedelta

import httpx
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from feather_connect.clock import FrozenClock
from feather_server.app import AppState, create_app
from feather_server.auth import (
    CSP,
    CurrentActor,
    hash_password,
    issue_token,
    require_csrf,
    verify_password,
)

PASSWORD = "correct horse battery staple"


@pytest.fixture
def app(state: AppState) -> FastAPI:
    state.settings.secret_key = "test-secret-key"
    state.settings.base_url = "https://feather.test"
    state.store.users.create_user("op", hash_password(PASSWORD), "operator")
    app = create_app(state=state, loops=lambda s: [])

    # Stand-ins for the pages and API calls that later tasks add: one page, and one state-changing call.
    @app.get("/_t/page")
    def page(actor: CurrentActor) -> dict[str, str]:
        return {"hello": actor.name}

    @app.post("/_t/form", dependencies=[Depends(require_csrf)])
    def form(actor: CurrentActor) -> dict[str, str]:
        return {"did": actor.name}

    @app.post("/api/v1/echo", dependencies=[Depends(require_csrf)])
    def echo(actor: CurrentActor) -> dict[str, str]:
        return {"kind": actor.kind, "name": actor.name}

    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app, base_url="https://feather.test", follow_redirects=False)


def csrf_of(client: TestClient) -> str:
    html = client.get("/login").text
    match = re.search(r'name="csrf_token" value="([^"]+)"', html)
    assert match, html
    return match.group(1)


def login(client: TestClient, username: str = "op", password: str = PASSWORD) -> httpx.Response:
    return client.post(
        "/login", data={"username": username, "password": password, "csrf_token": csrf_of(client)}
    )


def test_login_sets_a_secure_http_only_cookie(client: TestClient) -> None:
    response = login(client)
    assert response.status_code == 303 and response.headers["location"] == "/"
    cookie = response.headers["set-cookie"].lower()
    assert "feather_session=" in cookie and "secure" in cookie and "httponly" in cookie
    assert "samesite=lax" in cookie
    assert PASSWORD not in cookie
    assert client.get("/_t/page").json() == {"hello": "op"}


def test_the_cookie_is_not_secure_on_localhost(state: AppState) -> None:
    state.settings.secret_key = "test-secret-key"
    state.settings.base_url = "http://localhost:8000"
    state.store.users.create_user("op", hash_password(PASSWORD), "operator")
    local = TestClient(
        create_app(state=state, loops=lambda s: []), base_url="http://localhost:8000", follow_redirects=False
    )
    response = login(local)
    assert response.status_code == 303 and "secure" not in response.headers["set-cookie"].lower()
    assert "httponly" in response.headers["set-cookie"].lower()


def test_a_tampered_cookie_is_not_a_login(client: TestClient) -> None:
    login(client)
    client.cookies.set(
        "feather_session", client.cookies["feather_session"][:-3] + "abc", domain="feather.test"
    )
    assert client.get("/_t/page").status_code == 303


def test_wrong_password_and_unknown_user_look_the_same(client: TestClient) -> None:  # Review Focus 2
    a = login(client, "op", "bad")
    b = login(client, "nobody", "bad")
    assert a.status_code == b.status_code == 401 and a.text == b.text
    assert "set-cookie" not in a.headers and "set-cookie" not in b.headers
    assert "feather_session" not in client.cookies or client.get("/_t/page").status_code == 303


def test_repeated_failures_are_slowed(client: TestClient, clock: FrozenClock) -> None:  # Review Focus 2
    for _ in range(5):
        assert login(client, "op", "bad").status_code == 401
    assert login(client, "op", "bad").status_code == 429
    assert login(client, "op", PASSWORD).status_code == 429  # the right one too, until the wait is over
    assert login(client, "someone-else", "bad").status_code == 401  # another user is not held back
    clock.advance(timedelta(minutes=16))
    assert login(client, "op", PASSWORD).status_code == 303


def test_failures_of_an_unknown_user_are_slowed_too(client: TestClient) -> None:
    for _ in range(5):
        login(client, "nobody", "bad")
    assert login(client, "nobody", "bad").status_code == 429


def test_a_good_login_forgets_earlier_failures(client: TestClient) -> None:
    for _ in range(4):
        login(client, "op", "bad")
    assert login(client).status_code == 303
    client.post("/logout", data={"csrf_token": csrf_of(client)})
    for _ in range(4):
        assert login(client, "op", "bad").status_code == 401


def test_login_without_csrf_is_403(client: TestClient) -> None:
    client.get("/login")
    response = client.post("/login", data={"username": "op", "password": PASSWORD})
    assert response.status_code == 403


def test_token_works_and_only_its_hash_is_stored(client: TestClient, state: AppState) -> None:
    token = issue_token(state.store, "ci", timedelta(days=1), role="operator", clock=state.clock)
    assert token.startswith("fc_") and len(token) > 30
    me = client.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200 and me.json() == {
        "kind": "token",
        "name": "ci",
        "role": "operator",
        "user": None,
    }
    stored = state.store.users.find_token(hashlib.sha256(token.encode()).hexdigest())
    assert stored is not None and stored.name == "ci"
    assert token not in repr(stored) and token.removeprefix("fc_") not in repr(stored)
    assert state.store.users.find_token(token) is None  # nothing is stored under the secret itself


def test_a_wrong_token_is_401(client: TestClient, state: AppState) -> None:
    issue_token(state.store, "ci", timedelta(days=1), role="operator", clock=state.clock)
    for header in ("Bearer fc_nope", "Bearer ", "Basic abc", "Bearer"):
        assert client.get("/api/v1/me", headers={"Authorization": header}).status_code == 401


def test_expired_and_revoked_tokens_are_401(
    client: TestClient, state: AppState, clock: FrozenClock
) -> None:  # Review Focus 3
    short = issue_token(state.store, "short", timedelta(hours=1), role="operator", clock=clock)
    revoked = issue_token(state.store, "revoked", timedelta(days=1), role="operator", clock=clock)

    def status(token: str) -> int:
        return client.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"}).status_code

    assert status(short) == 200 and status(revoked) == 200
    clock.advance(timedelta(hours=1))
    assert status(short) == 401 and status(revoked) == 200
    found = state.store.users.find_token(hashlib.sha256(revoked.encode()).hexdigest())
    assert found is not None
    state.store.users.revoke_token(found.token_id)
    assert status(revoked) == 401


def test_form_post_without_csrf_is_403(client: TestClient) -> None:
    login(client)
    assert client.post("/_t/form").status_code == 403
    assert client.post("/_t/form", data={"csrf_token": "forged"}).status_code == 403
    assert client.post("/logout").status_code == 403
    assert client.get("/_t/page").status_code == 200  # the failed logout did not log out
    token = csrf_of(client)
    assert client.post("/_t/form", data={"csrf_token": token}).json() == {"did": "op"}
    assert client.post("/_t/form", headers={"X-CSRF-Token": token}).json() == {"did": "op"}


def test_a_csrf_token_belongs_to_its_session(client: TestClient, app: FastAPI) -> None:
    login(client)
    mine = csrf_of(client)
    other = TestClient(app, base_url="https://feather.test", follow_redirects=False)
    login(other)
    assert other.post("/_t/form", data={"csrf_token": mine}).status_code == 403


def test_logout_ends_the_session(client: TestClient) -> None:
    login(client)
    response = client.post("/logout", data={"csrf_token": csrf_of(client)})
    assert response.status_code == 303 and response.headers["location"] == "/login"
    assert client.get("/_t/page").status_code == 303


def test_api_bearer_call_needs_no_csrf(client: TestClient, state: AppState) -> None:
    token = issue_token(state.store, "ci", timedelta(days=1), role="operator", clock=state.clock)
    call = client.post("/api/v1/echo", headers={"Authorization": f"Bearer {token}"})
    assert call.status_code == 200 and call.json() == {"kind": "token", "name": "ci"}


def test_a_session_still_needs_csrf_on_the_api(client: TestClient) -> None:
    login(client)
    assert client.post("/api/v1/echo").status_code == 403


def test_unauthenticated_page_redirects_to_login(client: TestClient) -> None:
    response = client.get("/_t/page")
    assert response.status_code == 303 and response.headers["location"] == "/login"


def test_unauthenticated_api_is_401_problem_json(client: TestClient) -> None:
    response = client.get("/api/v1/me")
    assert response.status_code == 401
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json()["status"] == 401 and response.json()["title"] == "Unauthorized"


def test_healthz_and_ingress_need_no_login(client: TestClient) -> None:
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code in (200, 503)  # not a redirect or a 401


def test_session_actor_is_a_human_and_a_removed_user_loses_access(
    client: TestClient, state: AppState
) -> None:
    login(client)
    assert client.get("/api/v1/me").json() == {
        "kind": "human",
        "name": "op",
        "role": "operator",
        "user": "op",
    }
    state.store.users._d.users.clear()  # type: ignore[attr-defined]  # the store has no delete
    assert client.get("/api/v1/me").status_code == 401


def test_logins_are_audited_without_the_password(client: TestClient, state: AppState) -> None:
    login(client, "op", "bad")
    login(client)
    rows = state.store.audit.list_audit()
    assert [(r.action, r.result) for r in rows] == [("login", "ok"), ("login", "failed")]
    assert PASSWORD not in repr(rows) and "bad" not in repr(rows)


def test_passwords_and_tokens_never_reach_the_logs(
    client: TestClient, state: AppState, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    token = issue_token(state.store, "ci", timedelta(days=1), role="operator", clock=state.clock)
    login(client, "op", "bad-password-1")
    login(client)
    client.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"})
    client.get("/api/v1/me", headers={"Authorization": "Bearer fc_wrong-token-1"})
    text = caplog.text + "".join(r.getMessage() for r in caplog.records)
    for secret in (PASSWORD, "bad-password-1", token, "fc_wrong-token-1", "test-secret-key"):
        assert secret not in text


def test_passwords_hash_and_verify() -> None:
    first, second = hash_password("pw"), hash_password("pw")
    assert first != second and first.startswith("$argon2")  # salted
    assert verify_password(first, "pw") and not verify_password(first, "other")
    assert not verify_password("not a hash", "pw")


def test_the_login_page_sets_a_csp_and_no_inline_script(client: TestClient) -> None:
    response = client.get("/login")
    assert response.headers["content-security-policy"] == CSP
    assert "<script" not in response.text and "style=" not in response.text
    assert response.headers["x-content-type-options"] == "nosniff"


def test_a_copied_cookie_stops_working_after_logout(client: TestClient, app: FastAPI) -> None:  # M1
    login(client)
    copied = client.cookies.get("feather_session")
    client.post("/logout", data={"csrf_token": csrf_of(client)})
    other = TestClient(app, base_url="https://feather.test", follow_redirects=False)
    other.cookies.set("feather_session", copied or "")
    assert other.get("/_t/page").status_code == 303


def test_a_password_change_ends_the_sessions_from_before(client: TestClient, state: AppState) -> None:  # M1
    login(client)
    assert client.get("/_t/page").status_code == 200
    state.store.users.set_password_hash("op", hash_password("a new password"))
    assert client.get("/_t/page").status_code == 303


def test_parallel_and_many_user_guesses_are_limited_by_address(client: TestClient) -> None:  # M2
    for n in range(20):
        login(client, username=f"user{n}", password="wrong")
    assert login(client).status_code == 429  # the right password, but this address has guessed too much


def test_the_throttle_keeps_a_bounded_number_of_keys(state: AppState) -> None:  # M2
    from feather_server import auth

    throttle = auth.LoginThrottle(state.clock)
    for n in range(auth._THROTTLE_KEYS + 50):  # pyright: ignore[reportPrivateUsage]
        throttle.fail((f"user{n}", "10.0.0.1"))
    assert len(throttle._failures) <= auth._THROTTLE_KEYS + 1  # pyright: ignore[reportPrivateUsage]


def test_a_very_long_password_is_refused_without_hashing(client: TestClient) -> None:  # M2
    assert login(client, password="x" * 5000).status_code == 401


def test_a_non_ascii_csrf_token_is_a_403(client: TestClient) -> None:  # M3
    login(client)
    response = client.post("/_t/form", data={"csrf_token": "é" * 64})
    assert response.status_code == 403
