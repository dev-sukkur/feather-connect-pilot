"""HTTP connectors: `Rest`, a source that pages through a JSON API, and `Call`, a sink that sends a request per record.

Both read their connection from `Context.config`: `base_url`, the auth scheme and its settings at the top level, and
the credentials under `auth`, which must be secret references (ruling 3). A connector keeps one `FeatherHttp` for as
long as the settings stay the same, so that an OAuth2 token outlives one read or one chunk, and passes the context's
budget with every call (ruling 2).

    connections:
      zakya:
        connector: http.rest
        budget: zoho-api
        base_url: https://api.zakya.in
        auth_scheme: oauth2_refresh_token
        token_url: https://accounts.zoho.in/oauth/v2/token
        token_prefix: Zoho-oauthtoken
        params: {organization_id: "60001"}
        auth:
          client_id: ${secret:env:ZAKYA_CLIENT_ID}
          client_secret: ${secret:env:ZAKYA_CLIENT_SECRET}
          refresh_token: ${secret:env:ZAKYA_REFRESH_TOKEN}
"""

import hashlib
import json
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, ClassVar, Literal, Protocol, cast

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from feather_connect.core import connector
from feather_connect.core.connector import CheckResult, ConnectorSpec, ReadRequest, Secret, StreamSchema
from feather_connect.core.context import Context
from feather_connect.core.errors import PartitionError, PermanentError, TransientError
from feather_connect.runtime.http import (
    ApiKey,
    Auth,
    Basic,
    Bearer,
    FeatherHttp,
    HttpRetry,
    OAuth2ClientCredentials,
    OAuth2RefreshToken,
)

__all__ = ["Cursor", "HttpConfig", "Link", "Offset", "Page", "Rest"]

AuthScheme = Literal[
    "none", "api_key", "basic", "bearer", "oauth2_client_credentials", "oauth2_refresh_token"
]
DEFAULT_MAX_PAGES = 100_000
_EXCERPT = 200


class HttpConfig(BaseModel):
    """The settings of an HTTP connection. The secrets come from the connection's `auth`.

    `auth_scheme` picks how requests are signed. `token_prefix` is the word before a token in `Authorization`
    (`Zoho-oauthtoken` for Zoho). `params` and `headers` go with every request, such as Zoho's `organization_id`.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: str
    auth_scheme: AuthScheme = "none"
    token_url: str | None = None
    token_prefix: str = "Bearer"
    scope: str | None = None
    api_key_header: str = "X-API-Key"
    params: dict[str, str] = {}
    headers: dict[str, str] = {}
    timeout: float = 30.0
    retry_attempts: int = 6
    # From `auth`:
    client_id: Secret | None = None
    client_secret: Secret | None = None
    refresh_token: Secret | None = None
    token: Secret | None = None
    api_key: Secret | None = None
    username: Secret | None = None
    password: Secret | None = None


def _need(config: HttpConfig, *names: str) -> list[str]:
    values: list[str] = []
    for name in names:
        value = getattr(config, name)
        if value is None:
            raise PermanentError(f"auth_scheme {config.auth_scheme!r} needs {name} (a secret under auth)")
        values.append(value.get_secret_value() if isinstance(value, Secret) else str(value))
    return values


def _auth_of(config: HttpConfig) -> Auth | None:
    scheme = config.auth_scheme
    if scheme == "none":
        return None
    if scheme == "api_key":
        return ApiKey(config.api_key_header, *_need(config, "api_key"))
    if scheme == "basic":
        return Basic(*_need(config, "username", "password"))
    if scheme == "bearer":
        return Bearer(*_need(config, "token"), scheme=config.token_prefix)
    if config.token_url is None:
        raise PermanentError(f"auth_scheme {scheme!r} needs token_url")
    if scheme == "oauth2_client_credentials":
        client_id, client_secret = _need(config, "client_id", "client_secret")
        return OAuth2ClientCredentials(
            config.token_url, client_id, client_secret, config.scope, scheme=config.token_prefix
        )
    client_id, client_secret, refresh_token = _need(config, "client_id", "client_secret", "refresh_token")
    return OAuth2RefreshToken(
        config.token_url, client_id, client_secret, refresh_token, scheme=config.token_prefix
    )


class _HttpConnector:
    """What both connectors share: one client per set of settings, made on first use."""

    spec: ClassVar[ConnectorSpec]
    connection: str

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: dict[str, FeatherHttp] = {}
        self.transport: httpx.BaseTransport | None = None  # for tests, as is `sleep`
        self.sleep: Callable[[float], None] = time.sleep

    def _config(self, ctx: Context) -> HttpConfig:
        try:
            return HttpConfig.model_validate(dict(ctx.config))
        except ValidationError as error:
            problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in error.errors())
            raise PermanentError(f"connection {self.connection!r} is not set up right: {problems}") from None

    def client(self, ctx: Context) -> FeatherHttp:
        """Return the client for the context's settings, the same one while they stay the same."""
        config = self._config(ctx)
        plain = {k: v.get_secret_value() if isinstance(v, Secret) else v for k, v in config}
        digest = hashlib.sha256(json.dumps(plain, sort_keys=True, default=str).encode()).hexdigest()
        with self._lock:
            found = self._clients.get(digest)
            if found is None:
                secrets = [v.get_secret_value() for _, v in config if isinstance(v, Secret)]
                found = FeatherHttp(
                    base_url=config.base_url,
                    auth=_auth_of(config),
                    cost=self.spec.cost,
                    retry=HttpRetry(attempts=config.retry_attempts),
                    secrets=secrets,
                    log=ctx.log,
                    timeout=config.timeout,
                    headers=config.headers,
                    sleep=self.sleep,
                    transport=self.transport,
                )
                # Settings that changed leave the old client to the calls still using it, and it is not kept.
                self._clients = {digest: found}
            return found

    def check(self, ctx: Context) -> CheckResult:
        try:
            self._probe(ctx)
        except (PermanentError, TransientError) as error:
            return CheckResult(ok=False, message=str(error))
        return CheckResult(ok=True)

    def _probe(self, ctx: Context) -> None:
        raise NotImplementedError


def dig(body: Any, path: str | None) -> Any:
    """Return what `path`, dotted keys such as `data.items`, names in `body`. None or an empty path is the body."""
    if not path:
        return body
    found = body
    for part in path.split("."):
        if not isinstance(found, dict) or part not in found:
            return None
        found = cast(dict[str, Any], found)[part]
    return found


def _json_of(response: httpx.Response, client: FeatherHttp, what: str) -> Any:
    try:
        return response.json()
    except ValueError:
        excerpt = client.redact(response.text[:_EXCERPT])
        kind = response.headers.get("content-type", "no content type")
        raise PermanentError(f"{what} is not JSON ({kind}): {excerpt!r}") from None


# Pagination.


class Pagination(Protocol):
    """How to ask for a page, and how to tell the next one from a page. A token says where a page starts, and it is
    what a read saves as its checkpoint. The first page has the token None."""

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        """The URL and the query parameters of the page that `token` names."""
        ...

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        """The token of the page after this one, which held `count` records, or None if it was the last."""
        ...


@dataclass(frozen=True)
class _Single:
    """No pagination: one request holds everything."""

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        return path, {}

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        return None


@dataclass(frozen=True)
class Offset:
    """`?limit=100&offset=200`. A page shorter than `page_size` is the last."""

    limit_param: str = "limit"
    offset_param: str = "offset"
    page_size: int = 100

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        return path, {self.limit_param: self.page_size, self.offset_param: int(token or 0)}

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        return None if count < self.page_size else str(int(token or 0) + count)


@dataclass(frozen=True)
class Page:
    """`?page=2&per_page=100`, from `start`. With `has_more_path`, such as Zoho's `page_context.has_more_page`, the
    response says whether another page follows; without it, a page shorter than `page_size` is the last."""

    page_param: str = "page"
    size_param: str = "per_page"
    page_size: int = 100
    start: int = 1
    has_more_path: str | None = None

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        return path, {self.page_param: int(token or self.start), self.size_param: self.page_size}

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        more = bool(dig(body, self.has_more_path)) if self.has_more_path else count >= self.page_size
        return str(int(token or self.start) + 1) if more else None


@dataclass(frozen=True)
class Cursor:
    """`?cursor=abc`, the cursor of the next page read from `next_path` in each response. None or empty ends it."""

    cursor_param: str = "cursor"
    next_path: str = "next_cursor"

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        return path, {} if token is None else {self.cursor_param: token}

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        found = dig(body, self.next_path)
        return str(found) if found not in (None, "") else None


@dataclass(frozen=True)
class Link:
    """The URL of the next page in the `Link` header (RFC 8288), `rel="next"`."""

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        return token or path, {}

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        return response.links.get("next", {}).get("url")


def format_stamp(value: Any, how: str | Callable[[Any], str] | None) -> str:
    """Write a watermark the way the API wants it in a query: by `how`, a strftime pattern or a function, or else
    ISO 8601."""
    if callable(how):
        return how(value)
    if isinstance(value, datetime | date):
        return value.strftime(how) if how else value.isoformat()
    return str(value)


class Rest(_HttpConnector, connector.Source):
    """A source that reads a JSON API, page after page.

    `path` is the list endpoint and `records_path` where its records are in each page (the page itself when None).
    `pagination` is `Offset`, `Page`, `Cursor` or `Link`; without one a single request holds everything. `params` go
    with every list request, after the connection's own.

    For `by_stamp`, `stamp_param` names the query parameter that takes the watermark, written by `stamp_format`.
    `detail_path`, such as `/invoices/{invoice_id}`, fetches each listed record again on its own, filled in from the
    listed record's fields, and `detail_records_path` says where the record is in that response: an API that lists
    summaries and serves the whole record on its own (Zoho does). `cost_key` and `detail_cost_key` name each kind of
    call in the spec's costs, for the budget.

    A page whose body repeats the one before stops the read, and so does a read past `max_pages`, each with an error
    naming the page. The token of each next page is saved as the checkpoint, and a read given `resume` starts there.
    """

    spec = ConnectorSpec(
        name="http.rest",
        config=HttpConfig,
        strategies=frozenset({"by_stamp", "replace", "snapshot_diff"}),
        cost={"default": 1, "detail": 1},
    )

    def __init__(
        self,
        connection: str,
        *,
        path: str,
        records_path: str | None = None,
        pagination: Pagination | None = None,
        params: Mapping[str, Any] | None = None,
        stamp_param: str | None = None,
        stamp_format: str | Callable[[Any], str] | None = None,
        detail_path: str | None = None,
        detail_records_path: str | None = None,
        cost_key: str = "default",
        detail_cost_key: str = "detail",
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> None:
        super().__init__()
        self.connection = connection
        self.path = path
        self.records_path = records_path
        self.pagination: Pagination = pagination or _Single()
        self.params = dict(params or {})
        self.stamp_param = stamp_param
        self.stamp_format = stamp_format
        self.detail_path = detail_path
        self.detail_records_path = detail_records_path
        self.cost_key = cost_key
        self.detail_cost_key = detail_cost_key
        self.max_pages = max_pages

    def _probe(self, ctx: Context) -> None:
        next(self._pages(ctx, None, None), None)

    def discover(self, ctx: Context) -> list[StreamSchema]:
        """Offer one stream, named by the path, with the fields of the first record of the first page."""
        fields: dict[str, str] = {}
        for records in self._pages(ctx, None, None):
            for name, value in (records[0] if records else {}).items():
                fields[name] = type(value).__name__
            break
        return [StreamSchema(self.path, fields)]

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        """Yield the records of each page, fetched in detail if `detail_path` says so, in the requested partition."""
        since = None
        if req.since is not None and self.stamp_param is not None:
            since = format_stamp(req.since, self.stamp_format)
        client = self.client(ctx)
        for records in self._pages(ctx, since, req.resume):
            rows = [self._detail(ctx, client, record) for record in records] if self.detail_path else records
            rows = [row for row in rows if _in_partition(req, row)]
            if rows:
                yield rows

    def _pages(self, ctx: Context, since: str | None, resume: str | None) -> Iterator[list[dict[str, Any]]]:
        client = self.client(ctx)
        config = self._config(ctx)
        token = resume
        last_digest: str | None = None
        for number in range(1, self.max_pages + 2):
            if number > self.max_pages:
                raise PermanentError(
                    f"{self.path}: still more pages after {self.max_pages}, the most a read takes; "
                    "the API may be paging in a loop. Raise max_pages if it really has that many"
                )
            url, page_params = self.pagination.request(self.path, token)
            params = {**config.params, **self.params, **page_params}
            if since is not None and self.stamp_param is not None:
                params[self.stamp_param] = since
            # Merged into the URL, which may carry a query of its own (a `Link` does), as `params=` would replace it.
            full = httpx.URL(url).copy_merge_params(params)
            response = client.request("GET", full, cost_key=self.cost_key, budget=ctx.budget)
            digest = hashlib.sha256(response.content).hexdigest()
            if digest == last_digest:
                raise PermanentError(
                    f"{self.path}: page {number} (token {token!r}) repeats the page before it, "
                    "so the pagination would never end"
                )
            last_digest = digest
            body = _json_of(response, client, f"{self.path}, page {number}")
            found = dig(body, self.records_path)
            if not isinstance(found, list):
                raise PermanentError(
                    f"{self.path}, page {number}: {self.records_path or 'the body'} is not a list of records"
                )
            records = [cast(dict[str, Any], r) for r in cast(list[Any], found) if isinstance(r, dict)]
            if len(records) != len(cast(list[Any], found)):
                raise PermanentError(f"{self.path}, page {number}: a record is not a JSON object")
            yield records
            following = self.pagination.next_token(token, body, response, len(records))
            if following is None:
                return
            if following == token:
                raise PermanentError(
                    f"{self.path}: page {number} points to itself as the next page ({token!r})"
                )
            token = following
            ctx.checkpoint(token)

    def _detail(self, ctx: Context, client: FeatherHttp, record: Mapping[str, Any]) -> dict[str, Any]:
        assert self.detail_path is not None
        try:
            url = self.detail_path.format_map(dict(record))
        except (KeyError, IndexError, ValueError) as error:
            raise PermanentError(
                f"{self.detail_path}: a listed record has no {error} to fill it in"
            ) from None
        config = self._config(ctx)
        response = client.request(
            "GET", url, params=dict(config.params), cost_key=self.detail_cost_key, budget=ctx.budget
        )
        found = dig(_json_of(response, client, url), self.detail_records_path)
        if not isinstance(found, dict):
            raise PermanentError(f"{url}: {self.detail_records_path or 'the body'} is not a record")
        return cast(dict[str, Any], found)


def _in_partition(req: ReadRequest, row: Mapping[str, Any]) -> bool:
    """Whether a row is in the requested partition. One with no partition is kept, for the runtime to turn down."""
    try:
        return req.contains(row)
    except PartitionError:
        return True
