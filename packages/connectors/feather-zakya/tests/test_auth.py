"""Zoho OAuth: a token per tenant and connection, refreshed before it expires and once on a 401, the organisation on
every call, the region's hosts, and credentials kept out of every message."""

import logging
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from feather_connect.connectors import zakya  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.connectors.zakya.testing import ZakyaSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.partitions import none
from feather_connect.runtime import http as runtime_http

from .conftest import CONFIG, Clock


def _read(source: Any, ctx: Context) -> list[dict[str, Any]]:
    return [row for batch in source.read(ctx, ReadRequest("*", none())) for row in batch]


def _recording(sim: ZakyaSim, seen: list[httpx.Request]) -> httpx.MockTransport:
    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return sim.handle(request)

    return httpx.MockTransport(handle)


def test_one_token_signs_every_call_and_each_call_names_the_organisation(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):
    sim.add(3)
    seen: list[httpx.Request] = []
    source = wire(zakya.Source("zakya", "invoices"))
    source.transport = _recording(sim, seen)
    assert len(_read(source, ctx)) == 3
    assert len(_read(source, ctx)) == 3  # a second read keeps the token
    assert sim.token_requests == 1
    api = [r for r in seen if r.url.path.startswith("/inventory/")]
    assert len(api) == 8  # a list and three invoices, twice
    assert all(r.url.params["organization_id"] == "60001" for r in api)
    assert all(r.headers["authorization"].startswith("Zoho-oauthtoken ") for r in api)
    (token,) = [r for r in seen if r.url.path == "/oauth/v2/token"]
    assert str(token.url) == "https://accounts.zoho.test/oauth/v2/token"
    assert b"grant_type=refresh_token" in token.content and "authorization" not in token.headers


def test_an_expired_token_is_refreshed_once(sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]):
    sim.add(1)
    source = wire(zakya.Source("zakya", "invoices", details=False))
    _read(source, ctx)
    sim.expire_tokens()  # as an hour passing would: the next call gets a 401
    assert len(_read(source, ctx)) == 1
    assert sim.token_requests == 2
    sim.refresh_tokens = set()  # the refresh token is revoked: a new access token can't be had
    sim.expire_tokens()
    with pytest.raises(PermanentError, match="no access token"):
        _read(source, ctx)
    assert sim.token_requests == 3


def test_a_token_is_refreshed_a_minute_before_it_expires(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any], monkeypatch: pytest.MonkeyPatch
):
    sim.add(1)
    ticks = [1000.0]
    monkeypatch.setattr(
        runtime_http.time,  # pyright: ignore[reportPrivateImportUsage]
        "monotonic",
        lambda: ticks[0],
    )
    source = wire(zakya.Source("zakya", "invoices", details=False))
    _read(source, ctx)
    ticks[0] += 3600 - 90  # 90 seconds left: still good
    _read(source, ctx)
    assert sim.token_requests == 1
    ticks[0] += 45  # 45 seconds left: refreshed before it is sent
    _read(source, ctx)
    assert sim.token_requests == 2


def test_tokens_are_kept_per_tenant_and_connection(
    sim: ZakyaSim, make_ctx: Callable[..., Context], wire: Callable[[Any], Any]
):  # ADR-0013 rule 7
    sim.add(1)
    one, other = make_ctx(), make_ctx(tenant="t2")
    _read(wire(zakya.Source("zakya", "invoices", details=False)), one)
    _read(wire(zakya.Source("zakya", "items")), one)  # another dataset of the same connection: the same token
    assert sim.token_requests == 1
    _read(
        wire(zakya.Source("zakya", "invoices", details=False)), other
    )  # the same credentials, another tenant
    _read(wire(zakya.Source("zakya-eu", "invoices", details=False)), one)  # another connection
    assert sim.token_requests == 3


def test_the_domain_picks_the_region_hosts():
    def config(**more: Any) -> zakya.ZakyaConfig:
        return zakya.ZakyaConfig(organization_id=60001, **more)

    assert (config().api_url, config().token_url) == (
        "https://api.zakya.com",
        "https://accounts.zoho.com/oauth/v2/token",
    )
    india = config(domain="in")
    assert (india.api_url, india.token_url) == (
        "https://api.zakya.in",
        "https://accounts.zoho.in/oauth/v2/token",
    )
    assert config(domain="eu").token_url == "https://accounts.zoho.eu/oauth/v2/token"
    assert config().organization_id == "60001"
    with pytest.raises(ValueError):
        config(domain="cn")


def test_missing_credentials_or_settings_say_what_to_fix(
    wire: Callable[[Any], Any], make_ctx: Callable[..., Context]
):
    source = wire(zakya.Source("zakya", "invoices"))
    bare = Context({"organization_id": "60001"}, logging.getLogger("t"), make_ctx().now, "UTC")
    result = source.check(bare)
    assert not result.ok and "client_id, client_secret, refresh_token under auth" in result.message
    assert "secret:env" in result.message
    wrong = make_ctx(region="in")
    assert "region" in source.check(wrong).message
    assert source.check(make_ctx()).ok


def test_credentials_never_reach_an_error(
    sim: ZakyaSim, ctx: Context, clock: Clock, wire: Callable[[Any], Any]
):
    source = wire(zakya.Source("zakya", "invoices"))

    def echo(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/v2/token":
            return sim.handle(request)
        body = {
            "code": 9,
            "message": f"bad call with {request.headers['authorization']} {CONFIG['client_secret']}",
        }
        return httpx.Response(400, json=body)

    source.transport = httpx.MockTransport(echo)
    with pytest.raises(PermanentError) as caught:
        _read(source, ctx)
    text = str(caught.value)
    assert "HTTP 400" in text and CONFIG["client_secret"] not in text
    assert not [t for t in sim.tokens if t in text]
