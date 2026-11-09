"""A Zakya connection's settings, and the HTTP client the Zakya connectors share.

Zakya is Zoho's retail edition of Zoho Inventory, and speaks its API: OAuth2 with a refresh token from Zoho's accounts
server of the organisation's region, an `Authorization: Zoho-oauthtoken <token>` header, and the `organization_id` as
a query parameter of every call. The access token is kept in the process per tenant and connection (ADR-0013 rule 7),
refreshed a minute before it expires and once on a 401. A 429 is waited out for its `Retry-After`; one longer than a
minute, such as the daily cap's, goes back to the runtime as a `BudgetDeferred` that it sleeps off durably. A daily
cap answered with no `Retry-After` (Zoho's code 45) waits until midnight in the organisation's time zone.

A POST that creates (an item, a stock adjustment) is never sent twice by the client: an answer lost after Zakya
carried it out would make a second one. It goes up instead, the chunk is tried again, and the sink looks up the SKU
or the reference number before it posts anything.
"""

import email.utils
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import Secret
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.runtime.http import HttpStatusError

__all__ = ["API_PREFIX", "DOMAINS", "STAMP_FORMAT", "ZakyaConfig", "ZakyaError"]

# Each region's API and accounts hosts: an organisation lives in one, and its tokens work only there.
DOMAINS: Mapping[str, tuple[str, str]] = {
    "com": ("https://api.zakya.com", "https://accounts.zoho.com"),
    "in": ("https://api.zakya.in", "https://accounts.zoho.in"),
    "eu": ("https://api.zakya.eu", "https://accounts.zoho.eu"),
}
API_PREFIX = "/inventory/v1"
# How Zakya writes `last_modified_time`, and takes it back in a filter: 2026-11-07T10:15:00+0530.
STAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
TOKEN_PREFIX = "Zoho-oauthtoken"
DAILY_CAP_CODE = 45  # Zoho's code for a day's API calls spent
_CREDENTIALS = ("client_id", "client_secret", "refresh_token")


class ZakyaError(PermanentError):
    """Zakya answered with an error of its own, `code` (Zoho's, not HTTP's) and its message."""

    def __init__(self, message: str, code: int | None, status: int) -> None:
        super().__init__(message)
        self.code, self.status = code, status


class ZakyaConfig(BaseModel):
    """The settings of a Zakya connection. The three credentials come from the connection's `auth`, as secret
    references.

    `organization_id` is sent with every call. `domain` picks the region (`com`, `in` or `eu`), which sets the API
    host and Zoho's accounts host; `base_url` and `accounts_url` override them, for a simulator or a proxy.
    `timezone` is the organisation's (an IANA name such as Asia/Kolkata), whose midnight ends a spent daily cap; the
    flow's time zone by default.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, coerce_numbers_to_str=True)

    organization_id: str
    domain: Literal["com", "in", "eu"] = "com"
    base_url: str | None = None
    accounts_url: str | None = None
    timezone: str | None = None
    timeout: float = 30.0
    retry_attempts: int = 6
    # From `auth`:
    client_id: Secret | None = None
    client_secret: Secret | None = None
    refresh_token: Secret | None = None

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str | None) -> str | None:
        if value is not None:
            try:
                ZoneInfo(value)
            except (ZoneInfoNotFoundError, ValueError):
                raise ValueError(f"{value!r} is not a time zone, such as Asia/Kolkata") from None
        return value

    @property
    def api_url(self) -> str:
        return (self.base_url or DOMAINS[self.domain][0]).rstrip("/")

    @property
    def token_url(self) -> str:
        return (self.accounts_url or DOMAINS[self.domain][1]).rstrip("/") + "/oauth/v2/token"


class Zakya(http._HttpConnector):  # pyright: ignore[reportPrivateUsage]
    """What the Zakya connectors share: the connection's settings, read as Zakya's and run as an OAuth2 REST client,
    and a call that carries the organisation and reads Zakya's own error codes."""

    @staticmethod
    def account_of(options: Mapping[str, Any]) -> str | None:
        """The vendor account a connection's settings name, `zakya:<organization_id>`: the one its budget's
        `account` must be, as Zoho counts calls per organisation."""
        organization = options.get("organization_id")
        return None if organization in (None, "") else f"zakya:{organization}"

    def _retry_after_of(self, ctx: Context) -> Callable[[httpx.Response], float | None]:
        zone = ZoneInfo(self.zakya_config(ctx).timezone or ctx.tz)

        def daily_cap(response: httpx.Response) -> float | None:
            if response.status_code != 429 or _zoho_error(response)[0] != DAILY_CAP_CODE:
                return None
            return until_midnight(_answered_at(response) or datetime.now(zone), zone)

        return daily_cap

    def zakya_config(self, ctx: Context) -> ZakyaConfig:
        try:
            return ZakyaConfig.model_validate(dict(ctx.config))
        except ValidationError as error:
            problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in error.errors())
            raise PermanentError(f"connection {self.connection!r} is not set up right: {problems}") from None

    def _config(self, ctx: Context) -> Any:
        config = self.zakya_config(ctx)
        missing = [name for name in _CREDENTIALS if getattr(config, name) is None]
        if missing:
            raise PermanentError(
                f"connection {self.connection!r} needs {', '.join(missing)} under auth, as secret references "
                "such as ${secret:env:ZAKYA_CLIENT_ID}"
            )
        return http.HttpConfig(
            base_url=config.api_url,
            auth_scheme="oauth2_refresh_token",
            token_url=config.token_url,
            token_prefix=TOKEN_PREFIX,
            params={"organization_id": config.organization_id},
            timeout=config.timeout,
            retry_attempts=config.retry_attempts,
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
        )

    def call(
        self,
        ctx: Context,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
        cost_key: str = "default",
    ) -> dict[str, Any]:
        """Call Zakya at `path` (under /inventory/v1) with the organisation, and return the JSON answer. Raises
        `ZakyaError` for an answer whose `code` is not 0 and for a 4xx, with Zoho's code and message."""
        client = self.client(ctx)
        organization = self.zakya_config(ctx).organization_id
        try:
            response = client.request(
                method,
                f"{API_PREFIX}{path}",
                params={"organization_id": organization, **(params or {})},
                json=None if body is None else dict(body),
                cost_key=cost_key,
                budget=ctx.budget,
                resend=method.upper() != "POST",  # a POST creates: never twice, see the module's docstring
            )
        except HttpStatusError as error:
            code, message = _zoho_error(error.response)
            raise ZakyaError(
                f"{method} {path}: HTTP {error.status_code}, Zakya code {code}: {client.redact(message)}",
                code,
                error.status_code,
            ) from error
        try:
            found: Any = response.json()
        except ValueError:
            raise PermanentError(f"{method} {path}: the answer is not JSON") from None
        answer = cast(dict[str, Any], found) if isinstance(found, dict) else {}
        code = answer.get("code", 0)
        if code != 0:
            raise ZakyaError(
                f"{method} {path}: Zakya code {code}: {client.redact(str(answer.get('message', '')))}",
                code if isinstance(code, int) else None,
                response.status_code,
            )
        return answer


def _answered_at(response: httpx.Response) -> datetime | None:
    """When the server answered, by its `Date` header: its clock, not ours, says when its day ends."""
    try:
        return email.utils.parsedate_to_datetime(response.headers["date"])
    except (KeyError, TypeError, ValueError):
        return None


def until_midnight(now: datetime, zone: ZoneInfo) -> float:
    """The seconds from `now` to the next midnight in `zone`, when a spent daily cap is given back."""
    here = now.astimezone(zone)
    midnight = datetime.combine(here.date() + timedelta(days=1), datetime.min.time(), zone)
    return max(1.0, (midnight - here).total_seconds())


def _zoho_error(response: httpx.Response | None) -> tuple[int | None, str]:
    """Zoho's `code` and `message` from an error answer."""
    try:
        found: Any = response.json() if response is not None else None
    except ValueError:
        found = None
    if not isinstance(found, dict):
        return None, "" if response is None else response.text[:200]
    body = cast(dict[str, Any], found)
    code = body.get("code")
    return (code if isinstance(code, int) else None), str(body.get("message", ""))
