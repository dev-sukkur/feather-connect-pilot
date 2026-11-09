"""GraphQL connectors: `Source`, which walks a cursor connection page by page, and `Mutation`, a sink that sends a
mutation per record.

They share `http.Rest`'s connection settings (`base_url`, the auth scheme at the top level and the secrets under
`auth`) and its client: one per set of settings, with retries, budget metering and redaction. A GraphQL server
answers most failures with a 200 whose body holds an `errors` array, so every answer's `errors` is checked: a rate
limit (`extensions.code` or `type` such as `THROTTLED` or `RATE_LIMITED`) raises `TransientError`, and any other
error `PermanentError`, partial `data` or not, as a page with errors may be missing records.

    connections:
      shop:
        connector: graphql.source
        base_url: https://shop.example.com
        auth_scheme: api_key
        api_key_header: X-Shopify-Access-Token
        auth: {api_key: "${secret:env:SHOP_TOKEN}"}
"""

import hashlib
from collections.abc import Callable, Iterator, Mapping
from typing import Any, cast

from feather_connect.connectors.http import (  # pyright: ignore[reportMissingImports]
    DEFAULT_MAX_PAGES,
    VERSION_HEADER,
    HttpConfig,
    _HttpConnector,  # pyright: ignore[reportPrivateUsage]
    _in_partition,  # pyright: ignore[reportPrivateUsage]
    _json_of,  # pyright: ignore[reportPrivateUsage]
    _key_text,  # pyright: ignore[reportPrivateUsage]
    dig,
    format_stamp,
)
from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyRecord,
    ApplyResult,
    ConnectorSpec,
    ReadRequest,
    Rejection,
    StreamSchema,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.records import canonical_json

__all__ = ["Mutation", "Source", "check_errors"]

DEFAULT_PATH = "/graphql"
# The codes servers give a rate limit in an `errors` entry: Shopify's, GitHub's and the common spellings.
RATE_LIMIT_CODES = frozenset(
    {"THROTTLED", "RATE_LIMITED", "RATE_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS", "MAX_COST_EXCEEDED"}
)

SECRET_SETTINGS = frozenset(
    {"client_secret", "refresh_token", "token", "api_key", "password"}
)  # never shown, refs only


def check_errors(body: Any, what: str) -> None:
    """Raise for an answer whose `errors` is not empty: `TransientError` for a rate limit, else `PermanentError`."""
    if not isinstance(body, dict):
        raise PermanentError(f"{what}: the answer is not a JSON object")
    errors = cast(dict[str, Any], body).get("errors")
    if not errors:
        return
    entries = cast(list[Any], errors) if isinstance(errors, list) else [errors]
    messages: list[str] = []
    limited = False
    for entry in entries:
        if not isinstance(entry, dict):
            messages.append(str(entry))
            continue
        item = cast(dict[str, Any], entry)
        extensions = item.get("extensions")
        code = cast(dict[str, Any], extensions).get("code") if isinstance(extensions, dict) else None
        limited = limited or str(code or item.get("type") or "").upper() in RATE_LIMIT_CODES
        messages.append(str(item.get("message", item)) + (f" ({code})" if code else ""))
    text = f"{what}: the server answered with errors: " + "; ".join(messages)
    if limited:
        raise TransientError(text)
    raise PermanentError(text)


class Source(_HttpConnector, connector.Source):
    """A source that walks the cursor connection at `connection_path` (dotted, under `data`) of `query`.

    The query takes `$first` and `$after` and returns the connection's `pageInfo { hasNextPage endCursor }` and its
    records as `edges { node }` or as `nodes`. `variables` go with every page. For `by_stamp`, `stamp_variable` names
    the variable that takes the watermark. Each page's `endCursor` is saved as the checkpoint, and a read given
    `resume` starts after it. A cursor seen before in the same read stops it with an error.
    """

    spec = ConnectorSpec(
        secrets=SECRET_SETTINGS,
        name="graphql.source",
        config=HttpConfig,
        strategies=frozenset({"by_stamp", "replace", "snapshot_diff"}),
        cost={"default": 1},
        resumable=True,
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        query: str,
        connection_path: str,
        page_size: int = 100,
        variables: Mapping[str, Any] | None = None,
        stamp_variable: str | None = None,
        path: str = DEFAULT_PATH,
        cost_key: str = "default",
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> None:
        super().__init__()
        self.connection = connection
        self.query = query
        self.connection_path = connection_path
        self.page_size = page_size
        self.variables = dict(variables or {})
        self.stamp_variable = stamp_variable
        self.path = path
        self.cost_key = cost_key
        self.max_pages = max_pages

    def _probe(self, ctx: Context) -> None:
        next(self._pages(ctx, None, None), None)

    def discover(self, ctx: Context) -> list[StreamSchema]:
        """Offer one stream, named by the connection path, with the fields of the first record."""
        for records in self._pages(ctx, None, None):
            first = records[0] if records else {}
            return [StreamSchema(self.connection_path, {k: type(v).__name__ for k, v in first.items()})]
        return [StreamSchema(self.connection_path, {})]

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        since = None
        if req.since is not None and self.stamp_variable is not None:
            since = format_stamp(req.since, None)
        for records in self._pages(ctx, since, req.resume):
            rows = [row for row in records if _in_partition(req, row)]
            if rows:
                yield rows

    def _pages(self, ctx: Context, since: str | None, cursor: str | None) -> Iterator[list[dict[str, Any]]]:
        client = self.client(ctx)
        seen: set[str | None] = {cursor}
        for number in range(1, self.max_pages + 2):
            if number > self.max_pages:
                raise PermanentError(f"{self.connection_path}: still more pages after {self.max_pages}")
            variables = {**self.variables, "first": self.page_size, "after": cursor}
            if since is not None and self.stamp_variable is not None:
                variables[self.stamp_variable] = since
            what = f"{self.connection_path}, page {number}"
            response = client.request(
                "POST",
                self.path,
                content=canonical_json({"query": self.query, "variables": variables}),
                headers={"Content-Type": "application/json"},
                cost_key=self.cost_key,
                budget=ctx.budget,
            )
            body = _json_of(response, client, what)
            check_errors(body, what)
            found = dig(cast(dict[str, Any], body).get("data"), self.connection_path)
            if not isinstance(found, dict):
                raise PermanentError(f"{what}: data.{self.connection_path} is not a connection")
            page = cast(dict[str, Any], found)
            records = _nodes(page, what)
            yield records
            info = page.get("pageInfo")
            if not isinstance(info, dict):
                raise PermanentError(f"{what}: the connection has no pageInfo")
            info = cast(dict[str, Any], info)
            if not info.get("hasNextPage"):
                return
            following = info.get("endCursor")
            if not following or not records:
                raise PermanentError(f"{what}: says more follow but gives no endCursor or no records")
            if following in seen:
                raise PermanentError(
                    f"{what}: cursor {following!r} was read already, so the read would not end"
                )
            seen.add(following)
            cursor = str(following)
            ctx.checkpoint(cursor)


def _nodes(page: Mapping[str, Any], what: str) -> list[dict[str, Any]]:
    if isinstance(page.get("edges"), list):
        items = [
            edge.get("node") if isinstance(edge, dict) else None for edge in cast(list[Any], page["edges"])
        ]
    elif isinstance(page.get("nodes"), list):
        items = cast(list[Any], page["nodes"])
    else:
        raise PermanentError(f"{what}: the connection has neither edges nor nodes")
    if not all(isinstance(item, dict) for item in items):
        raise PermanentError(f"{what}: a record is not a JSON object")
    return [cast(dict[str, Any], item) for item in items]


def _user_errors(data: Any) -> list[str]:
    """The `userErrors` of each mutation's payload in `data`, as text: what the server turned down in the input."""
    if not isinstance(data, dict):
        return []
    found: list[str] = []
    for payload in cast(dict[str, Any], data).values():
        errors = cast(dict[str, Any], payload).get("userErrors") if isinstance(payload, dict) else None
        for entry in cast(list[Any], errors) if isinstance(errors, list) else []:
            if isinstance(entry, dict):
                item = cast(dict[str, Any], entry)
                field = item.get("field")
                where = ".".join(map(str, cast(list[Any], field))) + ": " if isinstance(field, list) else ""
                found.append(where + str(item.get("message", item)))
            else:
                found.append(str(entry))
    return found


class Mutation(_HttpConnector, connector.Sink):
    """A sink that sends `mutation` once per record, with the variables `variables(record)` returns; a delete record
    is passed too, so that `variables` can pick a delete's input, or the mutation can be one that deletes.

    Each request carries `key_header` with a key made from the chunk's idempotency key and the record's key. A
    payload's non-empty `userErrors` rejects that record. An `errors` array fails the chunk: `TransientError` for a
    rate limit, `PermanentError` for anything else, which every record would get the same.
    """

    spec = ConnectorSpec(
        secrets=SECRET_SETTINGS,
        name="graphql.mutation",
        config=HttpConfig,
        modes=frozenset({"upsert", "append"}),
        cost={"default": 1},
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        mutation: str,
        variables: Callable[[ApplyRecord], Mapping[str, Any]],
        path: str = DEFAULT_PATH,
        key_header: str = "Idempotency-Key",
        cost_key: str = "default",
    ) -> None:
        super().__init__()
        self.connection = connection
        self.mutation = mutation
        self.variables = variables
        self.path = path
        self.key_header = key_header
        self.cost_key = cost_key

    def _probe(self, ctx: Context) -> None:
        self.client(ctx)  # checks the settings: a mutation would change the destination

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        client = self.client(ctx)
        applied = 0
        rejected: list[Rejection] = []
        for record in batch.records:
            try:
                variables = dict(self.variables(record))
            except (KeyError, IndexError, TypeError) as error:
                rejected.append(Rejection(record.key, "VariablesError", f"variables failed: {error!r}"))
                continue
            key = hashlib.sha256(f"{batch.idempotency_key}:{_key_text(record.key)}".encode()).hexdigest()
            response = client.request(
                "POST",
                self.path,
                content=canonical_json({"query": self.mutation, "variables": variables}),
                headers={
                    "Content-Type": "application/json",
                    self.key_header: key,
                    VERSION_HEADER: batch.version_id,
                },
                cost_key=self.cost_key,
                budget=ctx.budget,
            )
            what = f"mutation for record {_key_text(record.key)!r}"
            body = _json_of(response, client, what)
            check_errors(body, what)
            problems = _user_errors(cast(dict[str, Any], body).get("data"))
            if problems:
                rejected.append(Rejection(record.key, "UserError", "; ".join(problems)))
            else:
                applied += 1
        return ApplyResult(applied, rejected)
