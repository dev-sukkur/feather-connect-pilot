"""`graphql.Source` and `graphql.Mutation` against the conformance kit and a small fake GraphQL server."""

import json
import logging
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

import feather_connect as fc
from feather_connect.connectors import graphql  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.testing.conformance import SinkConformance, SourceConformance
from feather_connect.testing.readme import run_readme_examples

BASE = "https://api.example.test"
QUERY = """query Items($first: Int!, $after: String, $since: String) {
  shop { items(first: $first, after: $after, updatedSince: $since) {
    edges { node { id n name updated } } pageInfo { hasNextPage endCursor } } }
}"""
MUTATION = """mutation Upsert($id: ID!, $input: ItemInput) {
  itemUpsert(id: $id, input: $input) { item { id } userErrors { field message } }
}"""


def no_wait(seconds: float) -> None:
    pass


class FakeGraphQL:
    """`shop.items`, a connection over `rows` with the row's index as its cursor, and `itemUpsert`, which keeps what
    it is sent and deletes an id sent with no input. `answer` replaces every answer when set."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.held: dict[str, dict[str, Any]] = {}
        self.answer: httpx.Response | None = None
        self.sent: list[dict[str, Any]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.answer is not None:
            return self.answer
        body = json.loads(request.content)
        self.sent.append({"body": body, "headers": dict(request.headers)})
        variables = body["variables"]
        if body["query"].startswith("mutation"):
            if variables.get("input") is None:
                self.held.pop(variables["id"], None)
            elif variables["input"].get("name") == "bad":
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "itemUpsert": {
                                "item": None,
                                "userErrors": [
                                    {"field": ["input", "name"], "message": "name is not allowed"}
                                ],
                            }
                        }
                    },
                )
            else:
                self.held[variables["id"]] = variables["input"]
            return httpx.Response(
                200, json={"data": {"itemUpsert": {"item": {"id": variables["id"]}, "userErrors": []}}}
            )
        start = int(variables["after"]) + 1 if variables.get("after") is not None else 0
        rows = [r for r in self.rows if variables.get("since") is None or r["updated"] >= variables["since"]]
        chunk = rows[start : start + variables["first"]]
        edges = [{"node": row, "cursor": str(start + i)} for i, row in enumerate(chunk)]
        info = {"hasNextPage": start + len(chunk) < len(rows), "endCursor": str(start + len(chunk) - 1)}
        return httpx.Response(200, json={"data": {"shop": {"items": {"edges": edges, "pageInfo": info}}}})


@pytest.fixture
def server() -> Iterator[FakeGraphQL]:
    fake = FakeGraphQL()
    with respx.mock(assert_all_called=False) as router:
        router.post(f"{BASE}/graphql").mock(side_effect=fake.handle)
        yield fake


def context() -> Context:
    config = {"base_url": BASE, "retry_attempts": 2}
    return Context(config, logging.getLogger("graphql-test"), datetime(2026, 11, 7, tzinfo=UTC), "UTC")


def source(**options: Any) -> Any:
    made = graphql.Source(
        "shop", query=QUERY, connection_path="shop.items", stamp_variable="since", **options
    )
    made.sleep = no_wait
    return made


def variables_of(record: ApplyRecord) -> dict[str, Any]:
    return {"id": record.key, "input": None if record.op == "delete" else dict(record.payload or {})}


def mutation() -> Any:
    made = graphql.Mutation("shop", mutation=MUTATION, variables=variables_of)
    made.sleep = no_wait
    return made


def errors(*entries: dict[str, Any], data: Any = None) -> httpx.Response:
    return httpx.Response(200, json={"data": data, "errors": list(entries)})


class _Connection:
    @pytest.fixture
    def ctx(self) -> Context:
        return context()


class TestSource(_Connection, SourceConformance):
    @pytest.fixture
    def source(self, server: FakeGraphQL) -> Any:
        return source()

    @pytest.fixture
    def seed(self, server: FakeGraphQL) -> Callable[[list[dict[str, Any]]], None]:
        return lambda rows: setattr(server, "rows", rows)

    @pytest.fixture
    def large_read_rows(self) -> int:
        return 20_000  # as for http.Rest: the fake keeps every call

    @pytest.fixture
    def transient_fault(self, server: FakeGraphQL) -> Callable[[], None]:
        return lambda: setattr(
            server, "answer", errors({"message": "Throttled", "extensions": {"code": "THROTTLED"}})
        )

    @pytest.fixture
    def permanent_fault(self, server: FakeGraphQL) -> Callable[[], None]:
        return lambda: setattr(server, "answer", errors({"message": "Field 'nope' doesn't exist"}))


class TestMutation(_Connection, SinkConformance):
    restart_safe = (
        False  # the fake server keeps no idempotency keys; a real one tells repeats by `Idempotency-Key`
    )

    @pytest.fixture
    def sink(self, server: FakeGraphQL) -> Any:
        return mutation()

    @pytest.fixture
    def fresh_sink(self) -> Callable[[], Any]:
        return mutation

    @pytest.fixture
    def read_back(self, server: FakeGraphQL) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        return lambda: dict(server.held)

    @pytest.fixture
    def transient_fault(self, server: FakeGraphQL) -> Callable[[], None]:
        return lambda: setattr(server, "answer", errors({"message": "slow down", "type": "RATE_LIMITED"}))

    @pytest.fixture
    def permanent_fault(self, server: FakeGraphQL) -> Callable[[], None]:
        return lambda: setattr(server, "answer", httpx.Response(401, json={"errors": [{"message": "no"}]}))


def read(made: Any, req: ReadRequest | None = None) -> list[dict[str, Any]]:
    return [
        row for page in made.read(context(), req or ReadRequest("*", fc.partition.none())) for row in page
    ]


def test_walks_cursor_connections(server: FakeGraphQL):
    server.rows = [{"id": f"i{n}", "updated": f"2026-11-0{1 + n % 5}"} for n in range(7)]
    rows = read(source(page_size=3))
    assert [r["id"] for r in rows] == [f"i{n}" for n in range(7)]
    afters = [s["body"]["variables"]["after"] for s in server.sent]
    assert afters == [None, "2", "5"]
    since = ReadRequest("*", fc.partition.none(), since="2026-11-04", stamp_field="updated")
    assert {r["id"] for r in read(source(page_size=3), since)} == {"i3", "i4"}
    assert server.sent[-1]["body"]["variables"]["since"] == "2026-11-04"


def test_a_page_of_nodes_reads_too(server: FakeGraphQL):
    nodes = {"nodes": [{"id": "a"}], "pageInfo": {"hasNextPage": False, "endCursor": None}}
    server.answer = httpx.Response(200, json={"data": {"shop": {"items": nodes}}})
    assert read(source()) == [{"id": "a"}]


def test_graphql_errors_with_http_200_raise_permanent(server: FakeGraphQL):
    server.answer = errors(
        {"message": "Cannot query field 'nope'", "extensions": {"code": "GRAPHQL_VALIDATION_FAILED"}}
    )
    with pytest.raises(PermanentError, match="Cannot query field 'nope'.*GRAPHQL_VALIDATION_FAILED"):
        read(source())


def test_rate_limit_error_code_is_transient(server: FakeGraphQL):
    server.answer = errors({"message": "Throttled", "extensions": {"code": "THROTTLED"}})
    with pytest.raises(TransientError, match="Throttled"):
        read(source())
    server.answer = errors(
        {"message": "API rate limit exceeded", "type": "RATE_LIMITED"}
    )  # GitHub's spelling
    with pytest.raises(TransientError):
        read(source())


def test_partial_data_with_errors_fails_the_read(server: FakeGraphQL):
    page = {"edges": [{"node": {"id": "a"}}], "pageInfo": {"hasNextPage": False, "endCursor": "0"}}
    server.answer = errors(
        {"message": "price is hidden", "path": ["shop", "items"]}, data={"shop": {"items": page}}
    )
    with pytest.raises(PermanentError, match="price is hidden"):
        read(source())


def test_a_repeated_cursor_stops_the_read(server: FakeGraphQL):
    page = {"edges": [{"node": {"id": "a"}}], "pageInfo": {"hasNextPage": True, "endCursor": "same"}}
    server.answer = httpx.Response(200, json={"data": {"shop": {"items": page}}})
    with pytest.raises(PermanentError, match="read already"):
        read(source())


def test_mutation_user_error_rejects_that_record(server: FakeGraphQL):
    records = [ApplyRecord("a", "upsert", {"name": "ok"}), ApplyRecord("b", "upsert", {"name": "bad"})]
    batch = ApplyBatch("r", "v1", 0, "*", "upsert", records, "r:v1:0:0", True, True)
    result = mutation().apply(context(), batch)
    assert result.applied == 1 and [(r.key, r.error_class) for r in result.rejected] == [("b", "UserError")]
    assert result.rejected[0].message == "input.name: name is not allowed"
    assert set(server.held) == {"a"}
    keys = [s["headers"]["idempotency-key"] for s in server.sent]
    assert len(set(keys)) == 2  # one key per record, the same each time the chunk is sent


def test_readme_example_runs(tmp_path: Path):
    names = run_readme_examples(Path(__file__).resolve().parents[1] / "README.md", tmp_path)
    assert isinstance(names["products"].source, graphql.Source)
    (route,) = names["products_to_shop"].routes
    assert isinstance(route.sink, graphql.Mutation)
