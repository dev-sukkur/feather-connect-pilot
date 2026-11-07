"""`http.Rest` and `http.Call` against the conformance kit, each in front of a small fake API."""

import json
import logging
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.context import Context
from feather_connect.testing.conformance import SinkConformance, SourceConformance

BASE = "https://api.example.test"
PAGE = 100


def no_wait(seconds: float) -> None:
    pass


class FakeApi:
    """A list endpoint over `rows`, paged by `page`/`per_page`, and an upsert by id that keeps what it is sent.
    `fail_with` makes every call answer that status."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.held: dict[str, dict[str, Any]] = {}
        self.fail_with: int | None = None

    def list(self, request: httpx.Request) -> httpx.Response:
        if self.fail_with is not None:
            return httpx.Response(self.fail_with, json={"error": "no"})
        page, size = int(request.url.params["page"]), int(request.url.params["per_page"])
        chunk = self.rows[(page - 1) * size : page * size]
        return httpx.Response(200, json={"items": chunk, "more": page * size < len(self.rows)})

    def put(self, request: httpx.Request) -> httpx.Response:
        if self.fail_with is not None:
            return httpx.Response(self.fail_with, json={"error": "no"})
        body = json.loads(request.content)
        self.held[request.url.path.rsplit("/", 1)[1]] = body
        return httpx.Response(200, json={"id": body["id"]})


@pytest.fixture
def api() -> Iterator[FakeApi]:
    fake = FakeApi()
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE}/items").mock(side_effect=fake.list)
        router.put(url__startswith=f"{BASE}/items/").mock(side_effect=fake.put)
        yield fake


class _Connection:
    @pytest.fixture
    def ctx(self) -> Context:
        config = {"base_url": BASE, "retry_attempts": 2}
        return Context(config, logging.getLogger("conformance"), datetime(2026, 11, 7, tzinfo=UTC), "UTC")


class TestRest(_Connection, SourceConformance):
    @pytest.fixture
    def source(self, api: FakeApi) -> Any:
        rest = http.Rest(
            "api",
            path="/items",
            records_path="items",
            pagination=http.Page(page_size=PAGE, has_more_path="more"),
        )
        rest.sleep = no_wait
        return rest

    @pytest.fixture
    def seed(self, api: FakeApi) -> Callable[[list[dict[str, Any]]], None]:
        def load(rows: list[dict[str, Any]]) -> None:
            api.rows = rows

        return load

    @pytest.fixture
    def large_read_rows(self) -> int:
        return 20_000  # 200 pages: the fake keeps every call, which the kit's 200,000 would make the test's memory

    @pytest.fixture
    def transient_fault(self, api: FakeApi) -> Callable[[], None]:
        return lambda: setattr(api, "fail_with", 503)

    @pytest.fixture
    def permanent_fault(self, api: FakeApi) -> Callable[[], None]:
        return lambda: setattr(api, "fail_with", 400)


class TestCall(_Connection, SinkConformance):
    deletes_supported = False  # `on_delete="reject"`, ruling 7

    def make(self) -> Any:
        call = http.Call("api", method="PUT", url="/items/{id}")
        call.sleep = no_wait
        return call

    @pytest.fixture
    def sink(self, api: FakeApi) -> Any:
        return self.make()

    @pytest.fixture
    def fresh_sink(self) -> Callable[[], Any]:
        return self.make

    @pytest.fixture
    def read_back(self, api: FakeApi) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        return lambda: dict(api.held)

    @pytest.fixture
    def transient_fault(self, api: FakeApi) -> Callable[[], None]:
        return lambda: setattr(api, "fail_with", 503)

    @pytest.fixture
    def permanent_fault(self, api: FakeApi) -> Callable[[], None]:
        return lambda: setattr(api, "fail_with", 401)
