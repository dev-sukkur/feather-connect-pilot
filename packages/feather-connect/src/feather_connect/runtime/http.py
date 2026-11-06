"""The HTTP client that HTTP-based connectors share: auth, retries, budget metering and redaction.

A connector keeps one `FeatherHttp` for as long as its connection's settings stay the same, so that an OAuth2 access
token outlives the context of one read or one chunk. The budget is the context's, and is passed with each call: every
attempt, retries included, takes its units from it before it goes out. A wait for budget is the handle's business, and
`BudgetDeferred` passes through untouched.

Retries cover a 429, a 5xx, a timeout and a connection that fails, with equal-jitter backoff. A `Retry-After` is
honoured; one longer than 60 seconds raises `TransientError` at once, so that the engine waits instead of a worker
thread. Any other 4xx raises `HttpStatusError`, a `PermanentError` with the status and a redacted excerpt of the body.
"""

import base64
import email.utils
import logging
import random
import re
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast

import httpx

from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.logs import redact as redact_known
from feather_connect.logs import register_secrets
from feather_connect.runtime.budgets import BudgetHandle

__all__ = [
    "ApiKey",
    "Auth",
    "Basic",
    "Bearer",
    "FeatherHttp",
    "HttpRetry",
    "HttpStatusError",
    "OAuth2ClientCredentials",
    "OAuth2RefreshToken",
    "redact",
]

# httpx logs every request with its URL at INFO, query string and all.
logging.getLogger("httpx").setLevel(logging.WARNING)

DEFAULT_TIMEOUT = 30.0
_LONGEST_RETRY_AFTER = 60.0  # longer than this is the engine's to wait, not a worker thread's
_EXCERPT = 300  # characters of a response body that an error quotes
_MASK = "***"
# Each pattern, with what replaces it: a token after its scheme, a JWT, and a credential named in JSON or a form.
_TOKEN_PATTERNS = (
    (re.compile(r"(?i)\b(bearer|basic|zoho-oauthtoken)\s+[^\s\"',;]+"), r"\1 ***"),
    (re.compile(r"eyJ[\w-]+\.[\w-]+(?:\.[\w-]+)?"), "***"),
    (
        re.compile(
            r"(?i)(\"?(?:access_token|refresh_token|client_secret|api_key|password)\"?\s*[:=]\s*\"?)[^\s\"&,]+"
        ),
        r"\1***",
    ),
)
_EARLY_REFRESH_S = 60.0  # a token this close to expiry is refreshed before use


def redact(text: str, secrets: Sequence[str] = ()) -> str:
    """Return `text` with `secrets`, common token patterns and every registered secret replaced by `***`."""
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, _MASK)
    for pattern, replacement in _TOKEN_PATTERNS:
        text = pattern.sub(replacement, text)
    return redact_known(text)


@dataclass(frozen=True)
class HttpRetry:
    """How a request is tried again: `attempts` in all, waits from `first_delay` doubling up to `max_delay`, cut by
    equal jitter to between half and all of that wait unless `jitter` is off."""

    attempts: int = 6
    first_delay: float = 1.0
    max_delay: float = 60.0
    jitter: bool = True

    def delay(self, failed_attempt: int, rand: Callable[[], float] = random.random) -> float:
        wait = min(self.first_delay * 2 ** (failed_attempt - 1), self.max_delay)
        return wait * (0.5 + rand() / 2) if self.jitter else wait


class HttpStatusError(PermanentError):
    """The server answered with a status that trying again won't change, such as a 400 or a 422."""

    def __init__(self, message: str, status_code: int, excerpt: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.excerpt = excerpt


class Auth(Protocol):
    """How a request is signed. `headers` gives what to add; `secrets` what must never be shown."""

    def headers(self, client: httpx.Client) -> Mapping[str, str]: ...
    def secrets(self) -> Sequence[str]: ...


@dataclass(frozen=True, repr=False)
class ApiKey:
    """A key sent in a header of its own, such as `X-API-Key`."""

    header: str
    value: str

    def headers(self, client: httpx.Client) -> Mapping[str, str]:
        return {self.header: self.value}

    def secrets(self) -> Sequence[str]:
        return (self.value,)

    def __repr__(self) -> str:
        return f"ApiKey(header={self.header!r}, value='***')"


@dataclass(frozen=True, repr=False)
class Basic:
    """HTTP basic auth."""

    user: str
    password: str

    def headers(self, client: httpx.Client) -> Mapping[str, str]:
        pair = base64.b64encode(f"{self.user}:{self.password}".encode()).decode("ascii")
        return {"Authorization": f"Basic {pair}"}

    def secrets(self) -> Sequence[str]:
        return (self.password,)

    def __repr__(self) -> str:
        return f"Basic(user={self.user!r}, password='***')"


@dataclass(frozen=True, repr=False)
class Bearer:
    """A fixed token, sent as `Authorization: <scheme> <token>`."""

    token: str
    scheme: str = "Bearer"

    def headers(self, client: httpx.Client) -> Mapping[str, str]:
        return {"Authorization": f"{self.scheme} {self.token}"}

    def secrets(self) -> Sequence[str]:
        return (self.token,)

    def __repr__(self) -> str:
        return "Bearer(token='***')"


class _OAuth2:
    """An access token fetched from `token_url`, kept until shortly before it expires, and fetched again once if the
    server turns it down. One lock covers the fetch, so threads that find it expired together fetch it once."""

    def __init__(self, token_url: str, client_id: str, client_secret: str, scheme: str) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self._client_secret = client_secret
        self.scheme = scheme
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at = 0.0
        self.fetches = 0

    def _form(self) -> dict[str, str]:
        raise NotImplementedError

    def headers(self, client: httpx.Client) -> Mapping[str, str]:
        with self._lock:
            if self._token is None or time.monotonic() >= self._expires_at - _EARLY_REFRESH_S:
                self._fetch(client)
            assert self._token is not None
            return {"Authorization": f"{self.scheme} {self._token}"}

    def expire(self, token_sent: str | None) -> None:
        """Forget the token that a 401 came back for, unless another thread has fetched a new one already."""
        with self._lock:
            if token_sent is None or f"{self.scheme} {self._token}" == token_sent:
                self._token = None

    def _fetch(self, client: httpx.Client) -> None:
        form = {"client_id": self.client_id, "client_secret": self._client_secret, **self._form()}
        try:
            response = client.post(self.token_url, data=form, headers={"Accept": "application/json"})
        except httpx.TimeoutException as error:
            raise TransientError(f"the token request to {self.token_url} timed out") from error
        except httpx.TransportError as error:
            raise TransientError(
                f"the token request to {self.token_url} failed: {type(error).__name__}"
            ) from error
        self.fetches += 1
        found = _json_or_none(response)
        body = cast(dict[str, Any], found) if isinstance(found, dict) else {}
        token = body.get("access_token")
        if response.status_code == 429 or response.status_code >= 500:
            raise TransientError(f"the token endpoint answered {response.status_code}")
        if response.status_code >= 400 or not isinstance(token, str) or not token:
            excerpt = redact(response.text[:_EXCERPT], self.secrets())
            raise PermanentError(
                f"the token endpoint {self.token_url} gave no access token (HTTP {response.status_code}): {excerpt}"
            )
        register_secrets(token)
        expires_in = body.get("expires_in")
        lifetime = float(expires_in) if isinstance(expires_in, int | float) else 3600.0
        self._token = token
        self._expires_at = time.monotonic() + lifetime

    def secrets(self) -> Sequence[str]:
        return tuple(s for s in (self._client_secret, self._token) if s)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(token_url={self.token_url!r}, client_id={self.client_id!r}, secrets='***')"


class OAuth2ClientCredentials(_OAuth2):
    """The OAuth2 client credentials grant."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
        *,
        scheme: str = "Bearer",
    ) -> None:
        super().__init__(token_url, client_id, client_secret, scheme)
        self.scope = scope

    def _form(self) -> dict[str, str]:
        return {"grant_type": "client_credentials", **({"scope": self.scope} if self.scope else {})}


class OAuth2RefreshToken(_OAuth2):
    """The OAuth2 refresh token grant, as Zoho uses it: a long-lived refresh token buys short-lived access tokens.

    `scheme` is the word before the token in `Authorization`; Zoho's is `Zoho-oauthtoken`.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        *,
        scheme: str = "Bearer",
    ) -> None:
        super().__init__(token_url, client_id, client_secret, scheme)
        self._refresh_token = refresh_token

    def _form(self) -> dict[str, str]:
        return {"grant_type": "refresh_token", "refresh_token": self._refresh_token}

    def secrets(self) -> Sequence[str]:
        return (*super().secrets(), self._refresh_token)


def _json_or_none(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None


def _retry_after(response: httpx.Response, now: Callable[[], datetime]) -> float | None:
    """Read `Retry-After` as seconds from now. It is a number of seconds or an HTTP date."""
    value = response.headers.get("retry-after")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - now()).total_seconds())


def _transient_status(status: int) -> bool:
    return status in (408, 425, 429) or status >= 500


class FeatherHttp:
    """An HTTP client for one connection.

    `base_url` is what relative URLs are joined to, and `auth` signs each request. `cost` gives the budget units of each
    kind of call, by the `cost_key` a request names; a key it lacks costs 1. `budget` meters calls that pass none of
    their own. `secrets` are kept out of errors and logs, with the auth's own. `log` takes a line per retry.
    `sleep` and `transport` are for tests.
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        auth: Auth | None = None,
        budget: BudgetHandle | None = None,
        cost: Mapping[str, int] | None = None,
        retry: HttpRetry | None = None,
        secrets: Sequence[str] = (),
        log: logging.Logger | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        headers: Mapping[str, str] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        transport: httpx.BaseTransport | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.auth = auth
        self._budget = budget
        self._cost = dict(cost or {})
        self.retry = retry or HttpRetry()
        self._secrets = tuple(s for s in secrets if s)
        register_secrets(*self._secrets)
        if auth is not None:
            register_secrets(*auth.secrets())
        self._log = log or logging.getLogger(__name__)
        self._sleep = sleep
        self._now = now
        self._client = httpx.Client(
            base_url=base_url or "", timeout=timeout, headers=dict(headers or {}), transport=transport
        )

    def close(self) -> None:
        self._client.close()

    def secrets(self) -> tuple[str, ...]:
        return (*self._secrets, *(self.auth.secrets() if self.auth is not None else ()))

    def redact(self, text: str) -> str:
        return redact(text, self.secrets())

    def request(
        self,
        method: str,
        url: str | httpx.URL,
        *,
        cost_key: str = "default",
        budget: BudgetHandle | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request and return a 2xx or 3xx response.

        Raises `TransientError` when the retries run out or a `Retry-After` is too long to wait here, and
        `HttpStatusError` for any other 4xx. A 401 with OAuth2 fetches a new token once and tries again.
        """
        meter = budget or self._budget
        units = self._cost.get(cost_key, 1)
        refreshed = False
        failed = 0
        while True:
            if meter is not None:
                meter.acquire(units)  # BudgetDeferred goes up as it is
            response, error = self._send(method, url, kwargs)
            if response is not None and response.status_code == 401 and isinstance(self.auth, _OAuth2):
                if refreshed:
                    raise self._status_error(method, url, response, "even after a new access token")
                refreshed = True
                self.auth.expire(response.request.headers.get("authorization"))
                continue
            if response is not None and not _transient_status(response.status_code):
                if response.status_code >= 400:
                    raise self._status_error(method, url, response)
                return response
            failed += 1
            wait = self.retry.delay(failed)
            retry_after = None if response is None else _retry_after(response, self._now)
            if retry_after is not None:
                wait = retry_after
            why = error or f"HTTP {response.status_code if response is not None else '?'}"
            if retry_after is not None and retry_after > _LONGEST_RETRY_AFTER:
                raise TransientError(
                    f"{method} {self._where(url)}: {why}, and the server asks to wait {retry_after:.0f} s",
                    retry_after=retry_after,
                )
            if failed >= self.retry.attempts:
                raise TransientError(
                    f"{method} {self._where(url)}: {why}, still after {failed} attempts",
                    retry_after=retry_after,
                )
            self._log.info(
                "%s %s: %s; trying again in %.1f s (attempt %d of %d)",
                method,
                self._where(url),
                why,
                wait,
                failed + 1,
                self.retry.attempts,
            )
            self._sleep(wait)

    def _send(
        self, method: str, url: str | httpx.URL, kwargs: Mapping[str, Any]
    ) -> tuple[httpx.Response | None, str | None]:
        """Send once. Returns the response, or None and what went wrong when none came back."""
        call: dict[str, Any] = dict(kwargs)
        headers: dict[str, str] = dict(call.get("headers") or {})
        if self.auth is not None:
            headers.update(self.auth.headers(self._client))
        call["headers"] = headers
        try:
            return self._client.request(method, url, **call), None
        except httpx.TimeoutException as error:
            return None, f"timed out ({type(error).__name__})"
        except httpx.TransportError as error:
            return None, f"the connection failed ({type(error).__name__})"

    def _where(self, url: str | httpx.URL) -> str:
        return self.redact(str(url))

    def _status_error(
        self, method: str, url: str | httpx.URL, response: httpx.Response, note: str = ""
    ) -> HttpStatusError:
        excerpt = self.redact(response.text[:_EXCERPT])
        suffix = f" {note}" if note else ""
        return HttpStatusError(
            f"{method} {self._where(url)}: HTTP {response.status_code}{suffix}: {excerpt}",
            response.status_code,
            excerpt,
        )

    def __repr__(self) -> str:
        return f"FeatherHttp(base_url={str(self._client.base_url)!r}, auth={self.auth!r})"
