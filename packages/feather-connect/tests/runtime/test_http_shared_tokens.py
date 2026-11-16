"""OAuth2 access tokens shared by a tenant's processes through the store (the M1 open item "OAuth2 tokens").

A second process is played by `forget_tokens()`, which empties this process's own tokens as a new process starts
with none; what the store keeps stays.
"""

import logging
import os
import threading
import time
from collections.abc import Iterator

import httpx
import pytest
import respx
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from feather_connect.runtime import http
from feather_connect.runtime.http import FeatherHttp, OAuth2RefreshToken, forget_tokens, share_tokens
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import SharedToken

BASE = "https://api.example.test"
TOKEN_URL = "https://accounts.example.test/oauth/v2/token"


def refresh_auth(refresh_token: str = "rtoken-456") -> OAuth2RefreshToken:
    return OAuth2RefreshToken(TOKEN_URL, "cid", "csecret-123", refresh_token, scheme="Zoho-oauthtoken")


@pytest.fixture
def store() -> Iterator[MemoryStore]:
    made = MemoryStore()
    forget_tokens()
    share_tokens(made.oauth_tokens)
    yield made
    share_tokens(None)
    forget_tokens()


def tokens_from(*names: str) -> respx.Route:
    issued = iter(names)
    return respx.post(TOKEN_URL).mock(
        side_effect=lambda request: httpx.Response(
            200, json={"access_token": next(issued), "expires_in": 3600}
        )
    )


@respx.mock
def test_a_second_process_takes_the_token_the_first_fetched(store: MemoryStore):
    route = tokens_from("tok-one", "tok-two")
    api = respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")
    forget_tokens()  # another process, with no token of its own
    FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")
    assert route.call_count == 1
    assert [c.request.headers["authorization"] for c in api.calls] == ["Zoho-oauthtoken tok-one"] * 2
    name = refresh_auth()._share_key()[0]  # pyright: ignore[reportPrivateUsage]
    kept = store.oauth_tokens.get(name)
    assert kept is not None and b"tok-one" not in kept.sealed  # sealed, not in the clear
    assert "csecret" not in name and "rtoken" not in name and len(name) == 64  # a hash names it


@respx.mock
def test_another_grant_never_gets_the_token(store: MemoryStore):
    route = tokens_from("tok-one", "tok-two")
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")
    forget_tokens()
    FeatherHttp(base_url=BASE, auth=refresh_auth("another-refresh-token")).request("GET", "/x")
    assert route.call_count == 2


@respx.mock
def test_a_token_turned_down_is_dropped_for_every_process(store: MemoryStore):
    route = tokens_from("tok-one", "tok-two")
    revoked: set[str] = set()
    api = respx.get(f"{BASE}/x").mock(
        side_effect=lambda request: httpx.Response(
            401 if request.headers["authorization"].removeprefix("Zoho-oauthtoken ") in revoked else 200
        )
    )
    FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")  # tok-one works
    revoked.add("tok-one")
    forget_tokens()
    FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")  # tok-one, turned down: tok-two
    forget_tokens()
    FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")  # tok-two, from the store
    assert route.call_count == 2
    assert api.calls[-1].request.headers["authorization"] == "Zoho-oauthtoken tok-two"


@respx.mock
def test_a_process_waits_for_the_token_another_is_fetching(store: MemoryStore):
    route = tokens_from("tok-mine")
    api = respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    auth = refresh_auth()
    name, seal_key = auth._share_key()  # pyright: ignore[reportPrivateUsage]
    assert store.oauth_tokens.claim(name, 30)  # another process is fetching

    def the_other_process_finishes() -> None:
        time.sleep(0.3)
        nonce = os.urandom(12)
        store.oauth_tokens.put(
            name, nonce + AESGCM(seal_key).encrypt(nonce, b"tok-theirs", name.encode()), 3600
        )

    threading.Thread(target=the_other_process_finishes).start()
    FeatherHttp(base_url=BASE, auth=auth).request("GET", "/x")
    assert route.call_count == 0
    assert api.calls[0].request.headers["authorization"] == "Zoho-oauthtoken tok-theirs"


@respx.mock
def test_a_process_fetches_its_own_once_the_wait_runs_out(
    store: MemoryStore, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(http, "_CLAIM_S", 0.3)
    route = tokens_from("tok-mine")
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    auth = refresh_auth()
    assert store.oauth_tokens.claim(auth._share_key()[0], 30)  # pyright: ignore[reportPrivateUsage]
    FeatherHttp(base_url=BASE, auth=auth).request("GET", "/x")
    assert route.call_count == 1


class _Down:
    def get(self, key: str) -> SharedToken | None:
        raise ConnectionError("the database is down")

    def claim(self, key: str, seconds: float) -> bool:
        raise ConnectionError("the database is down")

    def put(self, key: str, sealed: bytes, expires_in: float) -> None:
        raise ConnectionError("the database is down")

    def release(self, key: str) -> None:
        raise ConnectionError("the database is down")

    def drop(self, key: str, sealed: bytes) -> None:
        raise ConnectionError("the database is down")


@respx.mock
def test_a_store_that_fails_leaves_each_process_its_own_token(caplog: pytest.LogCaptureFixture):
    forget_tokens()
    share_tokens(_Down())
    try:
        route = tokens_from("tok-one")
        respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
        with caplog.at_level(logging.WARNING, logger="feather_connect.runtime.http"):
            FeatherHttp(base_url=BASE, auth=refresh_auth()).request("GET", "/x")
        assert route.call_count == 1
        assert "fetches its own" in caplog.text and "tok-one" not in caplog.text
    finally:
        share_tokens(None)
        forget_tokens()


@respx.mock
def test_a_token_endpoint_that_fails_gives_up_the_claim(store: MemoryStore):
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(400, json={"error": "invalid_code"}))
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200))
    auth = refresh_auth()
    with pytest.raises(Exception, match="gave no access token"):
        FeatherHttp(base_url=BASE, auth=auth).request("GET", "/x")
    assert store.oauth_tokens.claim(auth._share_key()[0], 30)  # pyright: ignore[reportPrivateUsage]
