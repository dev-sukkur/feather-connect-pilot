"""`http.Rest`: pagination of each style, the watermark in a query, list-then-detail, auth and the budget."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

import feather_connect as fc
from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ReadRequest
from feather_connect.core.errors import BudgetDeferred, PermanentError

BASE = "https://api.example.test"
TOKEN_URL = "https://accounts.example.test/oauth/v2/token"
ROWS = [{"id": i, "v": f"r{i}"} for i in range(1, 8)]


def everything(**kw: Any) -> ReadRequest:
    return ReadRequest("*", fc.partition.none(), **kw)


def read_all(source: Any, ctx: Any, req: ReadRequest | None = None) -> list[dict[str, Any]]:
    return [row for batch in source.read(ctx, req or everything()) for row in batch]


def offset_api(request: httpx.Request) -> httpx.Response:
    limit, offset = int(request.url.params["limit"]), int(request.url.params["offset"])
    return httpx.Response(200, json={"items": ROWS[offset : offset + limit]})


def page_api(request: httpx.Request) -> httpx.Response:
    page, size = int(request.url.params["page"]), int(request.url.params["per_page"])
    chunk = ROWS[(page - 1) * size : page * size]
    more = page * size < len(ROWS)
    return httpx.Response(200, json={"items": chunk, "page_context": {"page": page, "has_more_page": more}})


def cursor_api(request: httpx.Request) -> httpx.Response:
    start = int(request.url.params.get("cursor", "0"))
    following = str(start + 3) if start + 3 < len(ROWS) else None
    return httpx.Response(200, json={"items": ROWS[start : start + 3], "next": following})


def link_api(request: httpx.Request) -> httpx.Response:
    start = int(request.url.params.get("from", "0"))
    headers = {}
    if start + 3 < len(ROWS):
        headers["Link"] = f'<{BASE}/items?from={start + 3}>; rel="next"'
    return httpx.Response(200, json={"items": ROWS[start : start + 3]}, headers=headers)


STYLES = {
    "offset": (http.Offset(page_size=3), offset_api),
    "page": (http.Page(page_size=3, has_more_path="page_context.has_more_page"), page_api),
    "page-by-size": (http.Page(page_size=3), page_api),
    "cursor": (http.Cursor(next_path="next"), cursor_api),
    "link": (http.Link(), link_api),
}


@pytest.mark.parametrize("style", list(STYLES))
@respx.mock
def test_each_pagination_style_reads_every_page(style, make_ctx):
    pagination, api = STYLES[style]
    respx.get(f"{BASE}/items").mock(side_effect=api)
    source = http.Rest("api", path="/items", records_path="items", pagination=pagination)
    assert read_all(source, make_ctx()) == ROWS


@respx.mock
def test_resume_continues_from_the_saved_cursor(make_ctx):
    route = respx.get(f"{BASE}/items").mock(side_effect=cursor_api)
    source = http.Rest("api", path="/items", records_path="items", pagination=http.Cursor(next_path="next"))
    ctx = make_ctx()
    pages = source.read(ctx, everything())
    next(pages)
    assert ctx.checkpoint() is None  # the first page is not taken until the next is asked for
    next(pages)
    saved = ctx.checkpoint()
    assert saved == "3"
    rest = [row for batch in source.read(make_ctx(), everything(resume=saved)) for row in batch]
    assert rest == ROWS[3:]
    assert route.calls[-1].request.url.params["cursor"] == "6"


@respx.mock
def test_repeated_page_stops_with_a_clear_error(make_ctx):  # Review Focus 1
    respx.get(f"{BASE}/items").mock(
        return_value=httpx.Response(200, json={"items": ROWS[:3], "next": "again"})
    )
    source = http.Rest("api", path="/items", records_path="items", pagination=http.Cursor(next_path="next"))
    with pytest.raises(PermanentError, match=r"page 2.*repeat"):
        read_all(source, make_ctx())


@respx.mock
def test_page_limit_stops_a_runaway_cursor(make_ctx):  # Review Focus 1
    counter = iter(range(1, 1000))

    def runaway(request: httpx.Request) -> httpx.Response:
        n = next(counter)
        return httpx.Response(200, json={"items": [{"id": n}], "next": str(n)})

    respx.get(f"{BASE}/items").mock(side_effect=runaway)
    source = http.Rest(
        "api", path="/items", records_path="items", pagination=http.Cursor(next_path="next"), max_pages=5
    )
    with pytest.raises(PermanentError, match="after 5"):
        read_all(source, make_ctx())
    assert source.max_pages == 5 and http.DEFAULT_MAX_PAGES == 100_000


@respx.mock
def test_stamp_param_sends_the_watermark(make_ctx):
    route = respx.get(f"{BASE}/items").mock(return_value=httpx.Response(200, json={"items": []}))
    source = http.Rest(
        "api",
        path="/items",
        records_path="items",
        stamp_param="modified_since",
        stamp_format="%Y-%m-%dT%H:%M:%S%z",
    )
    since = datetime(2026, 11, 6, 8, 30, tzinfo=UTC)
    read_all(source, make_ctx(), everything(since=since, stamp_field="modified"))
    read_all(source, make_ctx())  # the first read has no watermark, and sends none
    assert route.calls[0].request.url.params["modified_since"] == "2026-11-06T08:30:00+0000"
    assert "modified_since" not in route.calls[1].request.url.params


@respx.mock
def test_records_path_digs_into_the_response(make_ctx):
    respx.get(f"{BASE}/items").mock(return_value=httpx.Response(200, json={"data": {"rows": ROWS[:2]}}))
    assert read_all(http.Rest("api", path="/items", records_path="data.rows"), make_ctx()) == ROWS[:2]


@respx.mock
def test_records_that_are_not_a_list_are_permanent(make_ctx):
    respx.get(f"{BASE}/items").mock(return_value=httpx.Response(200, json={"data": {"rows": "nope"}}))
    with pytest.raises(PermanentError, match="data.rows is not a list"):
        read_all(http.Rest("api", path="/items", records_path="data.rows"), make_ctx())


@respx.mock
def test_non_json_response_is_permanent_with_excerpt(make_ctx):
    respx.get(f"{BASE}/items").mock(return_value=httpx.Response(200, text="<html>maintenance</html>"))
    with pytest.raises(PermanentError, match="not JSON.*maintenance"):
        read_all(http.Rest("api", path="/items"), make_ctx())


@respx.mock
def test_connection_params_go_with_every_list_and_detail_call(make_ctx):
    lists = respx.get(f"{BASE}/invoices").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "invoices": [{"invoice_id": "a"}, {"invoice_id": "b"}]}
        )
    )
    details = respx.get(url__regex=rf"{BASE}/invoices/\w+").mock(
        side_effect=lambda request: httpx.Response(
            200,
            json={"invoice": {"invoice_id": request.url.path.rsplit("/", 1)[1], "line_items": [{"q": 1}]}},
        )
    )
    source = http.Rest(
        "zakya",
        path="/invoices",
        records_path="invoices",
        detail_path="/invoices/{invoice_id}",
        detail_records_path="invoice",
    )
    rows = read_all(source, make_ctx(params={"organization_id": "60001"}))
    assert rows == [
        {"invoice_id": "a", "line_items": [{"q": 1}]},
        {"invoice_id": "b", "line_items": [{"q": 1}]},
    ]
    assert lists.calls[0].request.url.params["organization_id"] == "60001"
    assert [c.request.url.params["organization_id"] for c in details.calls] == ["60001", "60001"]


@respx.mock
def test_a_detail_path_naming_a_missing_field_is_permanent(make_ctx):
    respx.get(f"{BASE}/invoices").mock(return_value=httpx.Response(200, json={"invoices": [{"other": 1}]}))
    source = http.Rest("z", path="/invoices", records_path="invoices", detail_path="/invoices/{invoice_id}")
    with pytest.raises(PermanentError, match="invoice_id"):
        read_all(source, make_ctx())


@respx.mock
def test_list_and_detail_calls_are_metered_by_their_cost_keys(make_ctx, make_budget):
    respx.get(f"{BASE}/invoices").mock(
        return_value=httpx.Response(200, json={"invoices": [{"invoice_id": "a"}, {"invoice_id": "b"}]})
    )
    respx.get(url__regex=rf"{BASE}/invoices/\w+").mock(return_value=httpx.Response(200, json={"invoice": {}}))
    budget = make_budget(["100/minute", "1000/day"])
    source = http.Rest(
        "z",
        path="/invoices",
        records_path="invoices",
        detail_path="/invoices/{invoice_id}",
        detail_records_path="invoice",
    )
    read_all(source, make_ctx(budget=budget.handle()))
    assert budget.used() == [3, 3]  # one list call and two details, in both windows


@respx.mock
def test_both_budget_windows_hold_a_read_back(make_ctx, make_budget):
    respx.get(f"{BASE}/items").mock(side_effect=page_api)
    source = http.Rest(
        "api",
        path="/items",
        records_path="items",
        pagination=http.Page(page_size=1, has_more_path="page_context.has_more_page"),
    )
    minute = make_budget(["2/minute", "100/day"])
    assert (
        len(read_all(source, make_ctx(budget=minute.handle()))) == 7
    )  # waits of under a minute are slept off
    assert minute.sleeps and all(s < 60 for s in minute.sleeps)
    day = make_budget(["100/minute", "3/day"])
    with pytest.raises(BudgetDeferred) as deferred:  # a long wait is the runtime's, to sleep durably
        read_all(source, make_ctx(budget=day.handle()))
    assert deferred.value.taken == 3


@respx.mock
def test_token_expiry_mid_read_refreshes_and_continues(make_ctx):  # Review Focus 2
    tokens = iter(["first", "second"])
    token_route = respx.post(TOKEN_URL).mock(
        side_effect=lambda r: httpx.Response(200, json={"access_token": next(tokens), "expires_in": 3600})
    )

    def api(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        if page == 2 and request.headers["authorization"] == "Zoho-oauthtoken first":
            return httpx.Response(401, json={"code": 57, "message": "token expired"})
        return page_api(request)

    respx.get(f"{BASE}/items").mock(side_effect=api)
    source = http.Rest(
        "zakya",
        path="/items",
        records_path="items",
        pagination=http.Page(page_size=3, has_more_path="page_context.has_more_page"),
    )
    ctx = make_ctx(
        auth_scheme="oauth2_refresh_token",
        token_url=TOKEN_URL,
        token_prefix="Zoho-oauthtoken",
        client_id="cid",
        client_secret="csecret",
        refresh_token="rtoken",
    )
    assert read_all(source, ctx) == ROWS
    assert token_route.call_count == 2


@respx.mock
def test_the_client_is_kept_across_reads_and_made_anew_when_settings_change(make_ctx):
    respx.get(f"{BASE}/items").mock(return_value=httpx.Response(200, json=[]))
    source = http.Rest("api", path="/items")
    first = source.client(make_ctx(timeout=10))
    assert source.client(make_ctx(timeout=10)) is first
    assert source.client(make_ctx(timeout=11)) is not first


def test_missing_credentials_are_permanent_and_named(make_ctx):
    source = http.Rest("api", path="/items")
    with pytest.raises(PermanentError, match="refresh_token"):
        source.client(
            make_ctx(
                auth_scheme="oauth2_refresh_token", token_url=TOKEN_URL, client_id="c", client_secret="s"
            )
        )
    with pytest.raises(PermanentError, match="not set up right.*no_such_setting"):
        source.client(make_ctx(no_such_setting=1))


@respx.mock
def test_a_partition_read_keeps_the_rows_of_that_partition(make_ctx):
    rows = [{"id": 1, "store": "a"}, {"id": 2, "store": "b"}, {"id": 3}]
    respx.get(f"{BASE}/items").mock(return_value=httpx.Response(200, json=rows))
    req = ReadRequest("store=a", fc.partition.by_fields("store"))
    got = read_all(http.Rest("api", path="/items"), make_ctx(), req)
    assert got == [{"id": 1, "store": "a"}, {"id": 3}]  # the runtime turns down the row with no partition


@respx.mock
def test_check_reports_ok_and_failure(make_ctx):
    respx.get(f"{BASE}/items").mock(
        side_effect=[httpx.Response(200, json=[]), httpx.Response(403, text="no")]
    )
    source = http.Rest("api", path="/items")
    assert source.check(make_ctx()).ok
    failed = source.check(make_ctx())
    assert not failed.ok and "403" in failed.message


@respx.mock
def test_discover_lists_a_stream_with_the_fields_of_a_record(make_ctx):
    respx.get(f"{BASE}/items").mock(return_value=httpx.Response(200, json=[{"id": 1, "name": "x"}]))
    (stream,) = http.Rest("api", path="/items").discover(make_ctx())
    assert stream.name == "/items" and stream.fields == {"id": "int", "name": "str"}


def test_the_spec_declares_the_strategies_and_the_config():
    spec = http.Rest.spec
    assert spec.name == "http.rest" and {"by_stamp", "replace"} <= spec.strategies
    assert spec.config is http.HttpConfig
    assert "client_secret" in spec.config.model_json_schema()["properties"]
    secret = http.HttpConfig(base_url=BASE, client_secret="hidden-value")
    assert "hidden-value" not in repr(secret)
