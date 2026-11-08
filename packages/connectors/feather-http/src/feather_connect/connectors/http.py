"""HTTP connectors: `Rest`, a source that pages through a JSON API, `Webhook`, a push source that takes signed
webhooks, and `Call`, a sink that sends a request per record.

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
from collections.abc import Callable, Collection, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Literal, Protocol, cast
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyRecord,
    ApplyResult,
    CheckResult,
    ConnectorSpec,
    Deleted,
    InboundRequest,
    Key,
    ReadRequest,
    RecordResponse,
    Rejection,
    Secret,
    StreamSchema,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import BudgetDeferred, PartitionError, PermanentError, TransientError
from feather_connect.core.records import canonical_json
from feather_connect.core.strategies import parse_duration
from feather_connect.core.webhooks import SignatureInvalid, verify_hmac
from feather_connect.runtime.http import (
    ApiKey,
    Auth,
    Basic,
    Bearer,
    FeatherHttp,
    HttpRetry,
    HttpStatusError,
    OAuth2ClientCredentials,
    OAuth2RefreshToken,
)

__all__ = ["Call", "Cursor", "HttpConfig", "Link", "Offset", "Page", "Rest", "SignatureInvalid", "Webhook"]

AuthScheme = Literal[
    "none", "api_key", "basic", "bearer", "oauth2_client_credentials", "oauth2_refresh_token"
]
DEFAULT_MAX_PAGES = 100_000
_EXCERPT = 200
# Secret fields that name who calls rather than prove it: not masked in text, where a short one would blot out words.
_NOT_REDACTED = frozenset({"username", "client_id"})


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
        self._retired: list[FeatherHttp] = []
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
                secrets = [
                    v.get_secret_value()
                    for k, v in config
                    if isinstance(v, Secret) and k not in _NOT_REDACTED
                ]
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
                # Settings that changed leave the old client to the calls still using it, and close it at the next
                # change, by when they are done with it.
                for old in self._retired:
                    old.close()
                self._retired = list(self._clients.values())
                self._clients = {digest: found}
            return found

    def check(self, ctx: Context) -> CheckResult:
        try:
            self._probe(ctx)
        except (PermanentError, TransientError, BudgetDeferred) as error:  # a check never waits for budget
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


def _has(body: Any, path: str) -> bool:
    """Whether the dotted `path` names something in `body`, None included."""
    found = body
    for part in path.split("."):
        if not isinstance(found, dict) or part not in found:
            return False
        found = cast(dict[str, Any], found)[part]
    return True


def _filled(template: str, record: Mapping[str, Any]) -> str:
    """Fill a URL template in from a record, each value percent-encoded, so that `A/B` or `..` stays one segment."""
    return template.format_map({str(k): quote(str(v), safe="") for k, v in record.items()})


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
    """`?limit=100&offset=200`. An empty page is the last: a server may give fewer than `page_size` and still have
    more."""

    limit_param: str = "limit"
    offset_param: str = "offset"
    page_size: int = 100

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        return path, {self.limit_param: self.page_size, self.offset_param: int(token or 0)}

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        return None if count == 0 else str(int(token or 0) + count)


@dataclass(frozen=True)
class Page:
    """`?page=2&per_page=100`, from `start`. With `has_more_path`, such as Zoho's `page_context.has_more_page`, the
    response says whether another page follows, and a response without it is an error; without it, a page shorter
    than `page_size` is the last."""

    page_param: str = "page"
    size_param: str = "per_page"
    page_size: int = 100
    start: int = 1
    has_more_path: str | None = None

    def request(self, path: str, token: str | None) -> tuple[str, dict[str, Any]]:
        return path, {self.page_param: int(token or self.start), self.size_param: self.page_size}

    def next_token(self, token: str | None, body: Any, response: httpx.Response, count: int) -> str | None:
        if self.has_more_path is None:
            more = count >= self.page_size
        elif not _has(body, self.has_more_path):
            raise PermanentError(
                f"page {token or self.start} has no {self.has_more_path} to say if more follow"
            )
        else:
            more = bool(dig(body, self.has_more_path))
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
    """The URL of the next page in the `Link` header (RFC 8288), `rel="next"`. A read refuses one on another origin
    than the connection's `base_url`, which would get its credentials."""

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

    A `by_stamp` read with `Page` or `Offset` pages by stamp: after a full page it asks again from the first page with
    the stamp filter set to the last record's stamp, and skips the records it has read already (by their content,
    which holds key and stamp). A list sorted by stamp moves a record changed during the read to its end, so paging
    by page number would skip a record for good; by stamp, none is missed. The list must be sorted by the stamp,
    ascending, and the stamp filter must keep records at the stamp it is given (`>=`).

    A page whose body repeats the one before stops the read, and so do a cursor seen before, an empty page that says
    more follow and a read past `max_pages`, each with an error naming the page. The token of each next page (the
    stamp, when paging by stamp) is saved as the checkpoint, and a read given `resume` starts there.
    """

    spec = ConnectorSpec(
        name="http.rest",
        config=HttpConfig,
        strategies=frozenset({"by_stamp", "replace", "snapshot_diff"}),
        cost={"default": 1, "detail": 1},
        resumable=True,
        maturity="beta",
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
        by_stamp = (
            req.stamp_field is not None
            and self.stamp_param is not None
            and isinstance(self.pagination, Page | Offset)
        )
        pages = (
            self._pages_by_stamp(ctx, cast(str, req.stamp_field), req.resume or since)
            if by_stamp
            else self._pages(ctx, since, req.resume)
        )
        for records in pages:
            rows = [self._detail(ctx, client, record) for record in records] if self.detail_path else records
            rows = [row for row in rows if _in_partition(req, row)]
            if rows:
                yield rows

    def _pages(self, ctx: Context, since: str | None, resume: str | None) -> Iterator[list[dict[str, Any]]]:
        token = resume
        last_digest: str | None = None
        seen_tokens: set[str | None] = {token}
        for number in range(1, self.max_pages + 2):
            self._within_max_pages(number)
            records, body, response = self._page(ctx, token, since, number)
            digest = hashlib.sha256(response.content).hexdigest()
            if digest == last_digest:
                raise PermanentError(
                    f"{self.path}: page {number} (token {token!r}) repeats the page before it, "
                    "so the pagination would never end"
                )
            last_digest = digest
            yield records
            following = self._next_token(token, body, response, records, number)
            if following is None:
                return
            if following in seen_tokens:
                raise PermanentError(
                    f"{self.path}: page {number} points to cursor {following!r}, read already in this read, "
                    "so the pagination would never end"
                )
            seen_tokens.add(following)
            token = following
            ctx.checkpoint(token)

    def _pages_by_stamp(
        self, ctx: Context, stamp_field: str, since: str | None
    ) -> Iterator[list[dict[str, Any]]]:
        """Page by stamp (I1): each full page is followed by the first page from its last record's stamp. A page whose
        last stamp is the stamp it was asked from (more records share a stamp than a page holds) is followed by the
        next page from the same stamp."""
        page_size = cast(Page | Offset, self.pagination).page_size
        seen: set[str] = set()
        token: str | None = None
        for number in range(1, self.max_pages + 2):
            self._within_max_pages(number)
            records, body, response = self._page(ctx, token, since, number)
            new = [r for r in records if _digest(r) not in seen]
            seen.update(_digest(r) for r in new)
            if new:
                yield new
            following = self._next_token(token, body, response, records, number)
            if following is None or (not new and len(records) < page_size):
                return
            if stamp_field not in records[-1]:
                raise PermanentError(f"{self.path}, page {number}: a listed record has no {stamp_field}")
            last = format_stamp(records[-1][stamp_field], self.stamp_format)
            if last == since:
                token = following  # still at the same stamp: on to the next page
            else:
                since, token = last, None
                ctx.checkpoint(since)

    def _within_max_pages(self, number: int) -> None:
        if number > self.max_pages:
            raise PermanentError(
                f"{self.path}: still more pages after {self.max_pages}, the most a read takes; "
                "the API may be paging in a loop. Raise max_pages if it really has that many"
            )

    def _page(
        self, ctx: Context, token: str | None, since: str | None, number: int
    ) -> tuple[list[dict[str, Any]], Any, httpx.Response]:
        """Fetch the page that `token` names, and return its records, its body and the response."""
        client = self.client(ctx)
        config = self._config(ctx)
        url, page_params = self.pagination.request(self.path, token)
        params = {**config.params, **self.params, **page_params}
        if since is not None and self.stamp_param is not None:
            params[self.stamp_param] = since
        # Merged into the URL, which may carry a query of its own (a `Link` does), as `params=` would replace it.
        full = httpx.URL(url).copy_merge_params(params)
        base = httpx.URL(config.base_url)
        if full.is_absolute_url and (full.scheme, full.host, full.port) != (
            base.scheme,
            base.host,
            base.port,
        ):
            raise PermanentError(
                f"{self.path}: page {number} is on {full.scheme}://{full.host}, not on the connection's "
                f"{base.scheme}://{base.host}, and would get its credentials"
            )
        response = client.request("GET", full, cost_key=self.cost_key, budget=ctx.budget)
        body = _json_of(response, client, f"{self.path}, page {number}")
        found = dig(body, self.records_path)
        if not isinstance(found, list):
            raise PermanentError(
                f"{self.path}, page {number}: {self.records_path or 'the body'} is not a list of records"
            )
        records = [cast(dict[str, Any], r) for r in cast(list[Any], found) if isinstance(r, dict)]
        if len(records) != len(cast(list[Any], found)):
            raise PermanentError(f"{self.path}, page {number}: a record is not a JSON object")
        return records, body, response

    def _next_token(
        self,
        token: str | None,
        body: Any,
        response: httpx.Response,
        records: list[dict[str, Any]],
        number: int,
    ) -> str | None:
        try:
            following = self.pagination.next_token(token, body, response, len(records))
        except PermanentError as error:
            raise PermanentError(f"{self.path}: {error}") from None
        if following is not None and not records:
            raise PermanentError(
                f"{self.path}: page {number} has no records but says more follow, "
                "so the pagination would never end"
            )
        return following

    def _detail(self, ctx: Context, client: FeatherHttp, record: Mapping[str, Any]) -> dict[str, Any]:
        assert self.detail_path is not None
        try:
            url = _filled(self.detail_path, record)
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


def _digest(record: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(record))).hexdigest()


def _in_partition(req: ReadRequest, row: Mapping[str, Any]) -> bool:
    """Whether a row is in the requested partition. One with no partition is kept, for the runtime to turn down."""
    try:
        return req.contains(row)
    except PartitionError:
        return True


# The webhook push source.

CustomVerify = Callable[[str, InboundRequest, datetime], None]


class Webhook:
    """A push source for webhooks a sender signs with HMAC-SHA256, such as an order system calling on each change.

    `signature_secret` names the entry under the connection's `auth` that holds the shared secret; `unsigned=True`
    instead says that anybody may send, and loading a project refuses a webhook with neither (design 13.5). The
    signature is read from `signature_header` and covers `<timestamp>.<body>` with the timestamp of
    `timestamp_header`, which must be within `tolerance` of when the request arrived (not when it is verified, S4).
    `timestamp_header=None` is a scheme without one, which a replay can pass. `verify`, a function of the secret, the
    request and when it arrived, is a scheme of another kind; it raises `SignatureInvalid` to refuse.

    `receive` only parses the body, a JSON object or list, and never calls out: `records_path` (dotted) is where the
    records are, and a record whose `deleted_field` is true is a delete. The event id, by which a repeat is stored
    once, is the `event_id_header` or what `event_id_path` names in the body. Header names are in lower case.
    """

    spec = ConnectorSpec(
        name="http.webhook", strategies=frozenset({"push"}), deletes="explicit", maturity="beta"
    )

    def __init__(
        self,
        connection: str,
        *,
        signature_secret: str | None = None,
        unsigned: bool = False,
        signature_header: str = "x-signature",
        timestamp_header: str | None = "x-timestamp",
        tolerance: str = "5m",
        records_path: str | None = None,
        event_id_path: str | None = None,
        event_id_header: str | None = None,
        deleted_field: str | None = None,
        verify: CustomVerify | None = None,
    ) -> None:
        if unsigned and signature_secret is not None:
            raise ValueError("a webhook is signed with signature_secret or unsigned, not both")
        self.connection = connection
        self.unsigned = unsigned
        self.signature_secret = signature_secret
        self.signature_header = signature_header.lower()
        self.timestamp_header = timestamp_header.lower() if timestamp_header else None
        self.tolerance: timedelta = parse_duration(tolerance)
        self.records_path = records_path
        self.event_id_path = event_id_path
        self.event_id_header = event_id_header.lower() if event_id_header else None
        self.deleted_field = deleted_field
        self.custom_verify = verify

    def _secret(self, ctx: Context) -> str:
        if self.signature_secret is None:
            raise PermanentError(f"webhook on {self.connection!r} names no signature_secret")
        return ctx.secret(self.signature_secret)

    def check(self, ctx: Context) -> CheckResult:
        if self.unsigned:
            return CheckResult(ok=True)
        try:
            self._secret(ctx)
        except PermanentError as error:
            return CheckResult(ok=False, message=str(error))
        return CheckResult(ok=True)

    def discover(self, ctx: Context) -> list[StreamSchema]:
        return [StreamSchema(self.connection, {})]

    def verify(self, ctx: Context, request: InboundRequest) -> None:
        """Raise `SignatureInvalid` unless the request is signed with the connection's secret, and `PermanentError` if
        the secret is not configured, which tells the sender to retry."""
        if self.unsigned:
            return
        secret = self._secret(ctx)
        if self.custom_verify is not None:
            self.custom_verify(secret, request, request.received_at)
            return
        timestamp = request.headers.get(self.timestamp_header) if self.timestamp_header else None
        verify_hmac(
            secret,
            request.body,
            request.headers.get(self.signature_header, ""),
            timestamp,
            now=request.received_at,
            tolerance=self.tolerance,
            require_timestamp=self.timestamp_header is not None,
        )

    def event_id(self, request: InboundRequest) -> str | None:
        """The event's id: the `event_id_header`, else the text or number at `event_id_path` in the body."""
        if self.event_id_header is not None:
            found = request.headers.get(self.event_id_header)
            if found:
                return found
        if self.event_id_path is None:
            return None
        value = dig(json.loads(request.body), self.event_id_path)
        return str(value) if isinstance(value, str | int) and not isinstance(value, bool) else None

    def receive(self, ctx: Context, request: InboundRequest) -> list[Mapping[str, Any] | Deleted]:
        """Return the records in the body. Raises `PermanentError` for a body that holds anything but objects, which
        sends that event to quarantine, with no part of the body in the message."""
        try:
            body: Any = json.loads(request.body)
        except ValueError:
            raise PermanentError("the webhook's body is not JSON") from None
        found = dig(body, self.records_path)
        if found is None and self.records_path is not None:
            raise PermanentError(f"the webhook's body has no {self.records_path}")
        items = cast(list[Any], found) if isinstance(found, list) else [found]
        records: list[Mapping[str, Any] | Deleted] = []
        for n, item in enumerate(items):
            if not isinstance(item, dict):
                raise PermanentError(f"record {n} of the webhook is not a JSON object")
            row = cast(dict[str, Any], item)
            records.append(
                Deleted(row) if self.deleted_field and row.get(self.deleted_field) is True else row
            )
        return records


# The outbound call sink.

Template = str | Callable[[Mapping[str, Any]], str]
BodyTemplate = Mapping[str, Any] | Callable[[Mapping[str, Any]], Mapping[str, Any]]
_ALREADY_APPLIED = 409
# Answers that say the call is set up wrong rather than the record is: they fail the chunk, and the delivery.
DEFAULT_FAIL_STATUSES = frozenset({401, 403, 404, 405, 407})
# Names the version a request comes from, beside the idempotency key, so that a destination's logs lead back to a
# trace of the record.
VERSION_HEADER = "X-Feather-Version"


def _fill(template: Any, record: Mapping[str, Any]) -> Any:
    """Fill a body template in from a record: a string that is one `{field}` alone takes the field's value as it is,
    any other string is formatted, and mappings and lists are filled in item by item."""
    if isinstance(template, str):
        if template.startswith("{") and template.endswith("}") and template[1:-1].isidentifier():
            return record[template[1:-1]]
        return template.format_map(dict(record))
    if isinstance(template, Mapping):
        return {str(k): _fill(v, record) for k, v in cast(Mapping[Any, Any], template).items()}
    if isinstance(template, list | tuple):
        return [_fill(v, record) for v in cast(list[Any], template)]
    return template


def _plain(value: Mapping[str, Any]) -> dict[str, Any]:
    """The JSON a body is sent as, read back: decimals as text and dates in ISO 8601, as `canonical_json` writes them."""
    return cast(dict[str, Any], json.loads(canonical_json(dict(value))))


def _key_text(key: Key) -> str:
    return key if isinstance(key, str) else canonical_json(list(key)).decode("utf-8")


class Call(_HttpConnector, connector.Sink):
    """A sink that sends one HTTP request per record, such as a POST of each order to an ERP.

    `url` is a template filled in from the record (`/orders/{order_id}`) or a function of the record, and so is
    `body`: a mapping whose strings are filled in (a string that is just `{field}` keeps the field's type), or a
    function that returns the body. Without `body` the record is sent as it is. `headers` go with every request.

    Each request carries `key_header` with a key made from the chunk's idempotency key and the record's key, the same
    each time the record is sent again, so that the destination can tell a repeat, and `X-Feather-Version` with the
    version it comes from, so that the destination's logs lead back to a trace of the record. A 2xx applies the record, and so
    does a 409, which says it was applied already. Any other 4xx rejects the record, which keeps the body it was sent
    with. A 429, a 5xx or a timeout is tried again by the client, and then fails the chunk for the runtime to retry.
    A status in `fail_statuses` (by default 401, 403, 404, 405 and 407: a wrong token, path or right) and any 3xx
    fail the chunk with `PermanentError`, so that the delivery fails and blocks rather than every record being
    rejected. A 409 means "applied already" only if the destination says so for the same content; an upsert by the
    record's own id (`PUT /invoices/{invoice_id}`) suits a record that changes better than a POST.

    `response_map` names fields to keep from each JSON answer, a 409's included, `{"erp_id": "data.id"}`, by dotted
    path; the runtime keeps them with the delivery, and each as it arrives. Values filled into `url` are
    percent-encoded. A delete is rejected unless `on_delete="skip"`, which counts it as applied and sends
    nothing.
    """

    spec = ConnectorSpec(
        name="http.call",
        config=HttpConfig,
        modes=frozenset({"upsert", "append"}),
        cost={"default": 1},
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        method: str,
        url: Template,
        body: BodyTemplate | None = None,
        headers: Mapping[str, str] | None = None,
        response_map: Mapping[str, str] | None = None,
        key_header: str = "Idempotency-Key",
        on_delete: Literal["reject", "skip"] = "reject",
        fail_statuses: Collection[int] = DEFAULT_FAIL_STATUSES,
    ) -> None:
        super().__init__()
        self.connection = connection
        self.method = method.upper()
        self.url = url
        self.body = body
        self.headers = dict(headers or {})
        self.response_map = dict(response_map or {})
        self.key_header = key_header
        self.on_delete = on_delete
        self.fail_statuses = frozenset(fail_statuses)

    def _probe(self, ctx: Context) -> None:
        self.client(ctx)  # checks the settings: a call would change the destination

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        client = self.client(ctx)
        applied = 0
        rejected: list[Rejection] = []
        responses: list[RecordResponse] = []
        for record in batch.records:
            if record.op == "delete" or record.payload is None:
                if self.on_delete == "skip":
                    applied += 1
                else:
                    rejected.append(
                        Rejection(record.key, "DeleteNotSupported", "http.Call doesn't send deletes")
                    )
                continue
            try:
                url, body = self._render(record)
            except PermanentError as error:  # this record lacks a field the templates need
                rejected.append(Rejection(record.key, "TemplateError", str(error)))
                continue
            key = hashlib.sha256(f"{batch.idempotency_key}:{_key_text(record.key)}".encode()).hexdigest()
            try:
                response = client.request(
                    self.method,
                    url,
                    content=canonical_json(body),
                    headers={
                        **self.headers,
                        "Content-Type": "application/json",
                        self.key_header: key,
                        VERSION_HEADER: batch.version_id,
                    },
                    budget=ctx.budget,
                )
            except HttpStatusError as error:
                if error.status_code in self.fail_statuses or error.status_code < 400:
                    raise PermanentError(
                        f"{error}; every record would get the same, so the chunk fails"
                    ) from error
                if error.status_code == _ALREADY_APPLIED:
                    applied += 1
                    answer = RecordResponse(record.key, error.status_code, self._mapped(error.response))
                else:
                    rejected.append(Rejection(record.key, "HttpStatusError", str(error)))
                    answer = RecordResponse(record.key, error.status_code, sent=body)
            else:
                applied += 1
                answer = RecordResponse(record.key, response.status_code, self._mapped(response))
            ctx.answered(answer)  # kept even if a later record fails the chunk
            responses.append(answer)
        return ApplyResult(applied, rejected, responses=responses)

    def _render(self, record: ApplyRecord) -> tuple[str, dict[str, Any]]:
        payload = cast(Mapping[str, Any], record.payload)
        try:
            url = self.url(payload) if callable(self.url) else _filled(self.url, payload)
            if self.body is None:
                body: Mapping[str, Any] = payload
            elif callable(self.body):
                body = self.body(payload)
            else:
                body = cast(Mapping[str, Any], _fill(self.body, payload))
        except (KeyError, IndexError) as error:
            raise PermanentError(
                f"http.Call: record {_key_text(record.key)!r} has no field {error}"
            ) from None
        return url, _plain(body)

    def _mapped(self, response: httpx.Response | None) -> dict[str, Any]:
        """The fields of `response_map` from a JSON answer; none from an answer that isn't JSON."""
        if not self.response_map or response is None:
            return {}
        try:
            answer = response.json()
        except ValueError:
            return {}
        return {name: dig(answer, path) for name, path in self.response_map.items()}
