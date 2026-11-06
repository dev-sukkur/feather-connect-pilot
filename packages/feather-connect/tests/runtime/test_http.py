"""The shared HTTP client: retries and `Retry-After`, metering, auth with a cached OAuth2 token, and redaction."""

import logging
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest
import respx

from feather_connect.clock import FrozenClock
from feather_connect.core.errors import BudgetDeferred, PermanentError, TransientError
from feather_connect.logs import redact as redact_registered
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.runtime.http import (
    ApiKey,
    Basic,
    Bearer,
    FeatherHttp,
    HttpRetry,
    HttpStatusError,
    OAuth2ClientCredentials,
    OAuth2RefreshToken,
    redact,
)
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import BudgetSpec, parse_limits

BASE = "https://api.example.test"
TOKEN_URL = "https://accounts.example.test/oauth/v2/token"
NOW = datetime(2026, 11, 6, 9, tzinfo=UTC)


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(sleeps: list[float]) -> Iterator[FeatherHttp]:
    made = FeatherHttp(base_url=BASE, sleep=sleeps.append, retry=HttpRetry(jitter=False), now=lambda: NOW)
    yield made
    made.close()


def budget(limits: list[str]) -> tuple[BudgetHandle, MemoryStore]:
    store = MemoryStore(FrozenClock(NOW))
    store.budgets.define(BudgetSpec("api", parse_limits(limits)))
    return BudgetHandle(store.budgets, "api", sleep=lambda s: None), store


def used(store: MemoryStore) -> float:
    return store.budgets.usage("api")[0].used


@respx.mock
def test_429_honours_retry_after_seconds_and_http_date(client, sleeps):
    route = respx.get(f"{BASE}/x").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(429, headers={"Retry-After": format_datetime(NOW + timedelta(seconds=12))}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    assert client.request("GET", "/x").json() == {"ok": True}
    assert route.call_count == 3
    assert sleeps == [7.0, 12.0]


@respx.mock
def test_5xx_and_connection_errors_back_off_and_retry(client, sleeps):
    respx.get(f"{BASE}/x").mock(
        side_effect=[
            httpx.Response(503),
            httpx.ConnectError("refused"),
            httpx.Response(502),
            httpx.Response(200),
        ]
    )
    assert client.request("GET", "/x").status_code == 200
    assert sleeps == [1.0, 2.0, 4.0]  # doubling from first_delay


def test_backoff_has_equal_jitter_and_a_ceiling():
    retry = HttpRetry(first_delay=1.0, max_delay=60.0)
    assert retry.delay(1, rand=lambda: 0.0) == 0.5
    assert retry.delay(1, rand=lambda: 1.0) == 1.0
    assert retry.delay(20, rand=lambda: 1.0) == 60.0


@respx.mock
def test_exhausted_retries_raise_transient_with_retry_after(client, sleeps):
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(429, headers={"Retry-After": "3"}))
    with pytest.raises(TransientError) as caught:
        client.request("GET", "/x")
    assert caught.value.retry_after == 3.0
    assert "6 attempts" in str(caught.value)
    assert len(sleeps) == 5


@respx.mock
def test_a_retry_after_over_a_minute_raises_transient_at_once(client, sleeps):  # ruling 4
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(429, headers={"Retry-After": "3600"}))
    with pytest.raises(TransientError) as caught:
        client.request("GET", "/x")
    assert caught.value.retry_after == 3600.0
    assert sleeps == []


@respx.mock
def test_4xx_raises_permanent_with_a_redacted_excerpt(sleeps):
    client = FeatherHttp(base_url=BASE, auth=Bearer("s3cret-token"), sleep=sleeps.append)
    respx.get(f"{BASE}/x").mock(
        return_value=httpx.Response(400, text="bad request for token s3cret-token: Bearer s3cret-token")
    )
    with pytest.raises(PermanentError) as e:
        client.request("GET", "/x")
    assert "s3cret" not in str(e.value) and "400" in str(e.value)
    assert isinstance(e.value, HttpStatusError) and e.value.status_code == 400
    assert sleeps == []


@respx.mock
def test_every_call_is_metered_with_its_cost(sleeps):
    handle, store = budget(["100/minute"])
    client = FeatherHttp(base_url=BASE, cost={"detail": 2}, sleep=sleeps.append)
    respx.get(f"{BASE}/list").mock(side_effect=[httpx.Response(503), httpx.Response(200)])
    respx.get(f"{BASE}/one").mock(return_value=httpx.Response(200))
    client.request("GET", "/list", budget=handle)  # two attempts, one unit each: a missing key costs 1
    client.request("GET", "/one", cost_key="detail", budget=handle)
    assert used(store) == 4


@respx.mock
def test_the_budget_of_the_call_wins_over_the_clients(sleeps):
    own, own_store = budget(["100/minute"])
    other, other_store = budget(["100/minute"])
    client = FeatherHttp(base_url=BASE, budget=own, sleep=sleeps.append)
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    client.request("GET", "/x")
    client.request("GET", "/x", budget=other)
    assert used(own_store) == 1 and used(other_store) == 1


@respx.mock
def test_budget_deferred_propagates(sleeps):
    handle, _ = budget(["1/day"])
    client = FeatherHttp(base_url=BASE, sleep=sleeps.append)
    route = respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    client.request("GET", "/x", budget=handle)
    with pytest.raises(BudgetDeferred) as deferred:
        client.request("GET", "/x", budget=handle)
    assert deferred.value.budget == "api"
    assert route.call_count == 1  # nothing went out without budget


@respx.mock
def test_secret_values_and_bearer_tokens_never_reach_logs(caplog, sleeps):
    caplog.set_level(logging.DEBUG)
    client = FeatherHttp(
        base_url=BASE,
        auth=ApiKey("X-Api-Key", "k3y-v4lue-xyz"),
        secrets=["k3y-v4lue-xyz"],
        sleep=sleeps.append,
    )
    respx.get(f"{BASE}/x").mock(
        side_effect=[httpx.Response(500), httpx.Response(401, text="key k3y-v4lue-xyz")]
    )
    with pytest.raises(PermanentError) as e:
        client.request("GET", "/x?api_key=k3y-v4lue-xyz")
    assert "k3y-v4lue-xyz" not in caplog.text and "k3y-v4lue-xyz" not in str(e.value)
    assert "trying again" in caplog.text
    assert not [r for r in caplog.records if r.name == "httpx" and r.levelno < logging.WARNING]
    assert "k3y" not in repr(client) and "k3y" not in repr(client.auth)


def test_redact_masks_given_secrets_token_patterns_and_registered_ones():
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl"
    text = f"Bearer abc.def Zoho-oauthtoken 1000.xyz {jwt} access_token=zzz9 mine-own"
    out = redact(text, ["mine-own"])
    assert "abc.def" not in out and "1000.xyz" not in out and jwt not in out and "zzz9" not in out
    assert "mine-own" not in out
    assert out.startswith("Bearer ***")


@respx.mock
def test_basic_and_bearer_set_the_authorization_header():
    seen: list[str] = []

    def capture(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["authorization"])
        return httpx.Response(200)

    respx.get(f"{BASE}/x").mock(side_effect=capture)
    FeatherHttp(base_url=BASE, auth=Basic("ann", "pw")).request("GET", "/x")
    FeatherHttp(base_url=BASE, auth=Bearer("tok", scheme="Zoho-oauthtoken")).request("GET", "/x")
    assert seen == ["Basic YW5uOnB3", "Zoho-oauthtoken tok"]


def refresh_auth() -> OAuth2RefreshToken:
    return OAuth2RefreshToken(TOKEN_URL, "cid", "csecret-123", "rtoken-456", scheme="Zoho-oauthtoken")


@respx.mock
def test_oauth2_token_is_fetched_cached_and_refreshed_on_401(sleeps):  # Review Focus 2
    tokens = iter(["tok-one", "tok-two"])
    token_route = respx.post(TOKEN_URL).mock(
        side_effect=lambda request: httpx.Response(
            200, json={"access_token": next(tokens), "expires_in": 3600}
        )
    )

    def api(request: httpx.Request) -> httpx.Response:
        ok = request.headers["authorization"] == "Zoho-oauthtoken tok-two" or api_calls[0] == 0
        api_calls[0] += 1
        return httpx.Response(200 if ok else 401)

    api_calls = [0]
    respx.get(f"{BASE}/x").mock(side_effect=api)
    client = FeatherHttp(base_url=BASE, auth=refresh_auth(), sleep=sleeps.append)
    client.request("GET", "/x")  # tok-one is fetched, and works
    client.request(
        "GET", "/x"
    )  # tok-one from the cache: the server now turns it down, one refresh, one retry
    client.request("GET", "/x")  # tok-two from the cache
    assert token_route.call_count == 2
    form = token_route.calls[0].request.content.decode()
    assert "grant_type=refresh_token" in form and "refresh_token=rtoken-456" in form
    assert redact_registered("tok-one tok-two") == "*** ***"  # fetched tokens are masked in every log line


@respx.mock
def test_oauth2_second_401_is_permanent(sleeps):
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "t1", "expires_in": 3600})
    )
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(401, text="invalid oauth token"))
    client = FeatherHttp(base_url=BASE, auth=refresh_auth(), sleep=sleeps.append)
    with pytest.raises(HttpStatusError, match="401 even after a new access token"):
        client.request("GET", "/x")


@respx.mock
def test_an_expiring_token_is_refreshed_before_use(monkeypatch):
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "short", "expires_in": 30})
    )
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    auth = refresh_auth()
    client = FeatherHttp(base_url=BASE, auth=auth)
    client.request("GET", "/x")
    client.request("GET", "/x")
    assert auth.fetches == 2  # 30 s is inside the minute kept in hand, so each call fetches


@respx.mock
def test_threads_share_one_token_fetch():
    gate = threading.Event()

    def slow_token(request: httpx.Request) -> httpx.Response:
        gate.wait(5)
        return httpx.Response(200, json={"access_token": "shared", "expires_in": 3600})

    respx.post(TOKEN_URL).mock(side_effect=slow_token)
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    auth = refresh_auth()
    client = FeatherHttp(base_url=BASE, auth=auth)
    threads = [threading.Thread(target=client.request, args=("GET", "/x")) for _ in range(6)]
    for thread in threads:
        thread.start()
    gate.set()
    for thread in threads:
        thread.join(10)
    assert auth.fetches == 1


@respx.mock
def test_client_credentials_send_the_scope():
    route = respx.post(TOKEN_URL).mock(return_value=httpx.Response(200, json={"access_token": "cc"}))
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    FeatherHttp(base_url=BASE, auth=OAuth2ClientCredentials(TOKEN_URL, "c", "s", "read")).request("GET", "/x")
    form = route.calls[0].request.content.decode()
    assert "grant_type=client_credentials" in form and "scope=read" in form


@respx.mock
def test_a_token_endpoint_that_refuses_is_permanent_and_redacted():
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(400, json={"error": "invalid_client", "client_secret": "csecret-123"})
    )
    with pytest.raises(PermanentError) as e:
        FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")
    assert "csecret-123" not in str(e.value) and "400" in str(e.value)


@respx.mock
def test_timeout_is_transient(client, sleeps):
    respx.get(f"{BASE}/x").mock(side_effect=httpx.ReadTimeout("slow"))
    with pytest.raises(TransientError, match="timed out"):
        client.request("GET", "/x")
    assert len(sleeps) == 5
