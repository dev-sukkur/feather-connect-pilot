"""Local users, API tokens, browser sessions and CSRF (design 14).

A person signs in with a password and gets a signed session cookie. Automation sends `Authorization: Bearer fc_...`.
Only the SHA-256 of a token is stored, so a copy of the database can't be used to call the API. A user and a token
each have a role, `admin` or `operator` (see `services.access`); a token acts with the lower of its own role and its
user's current one.

A request that carries an `Authorization` header is judged by that header alone, never by a cookie it also has, and
needs no CSRF token: a browser doesn't add that header on its own. A request authenticated by cookie needs the
session's CSRF token, in the `csrf_token` form field or the `X-CSRF-Token` header.
"""

import base64
import hashlib
import hmac
import logging
import os
import re
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal
from urllib.parse import urlparse

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import APIRouter, Depends, FastAPI, Form, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, URLSafeTimedSerializer
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from feather_connect.clock import Clock, SystemClock
from feather_connect.storage.port import ROLES, AccessGrant, Role, Store, TokenScope

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

SESSION_COOKIE = "feather_session"
REGISTRY_PREFIX = (
    "/registry/"  # the built-in package registry, which package tools call with Basic credentials
)
SESSION_TTL = timedelta(hours=12)
TOKEN_PREFIX = "fc_"
MAX_FAILURES = 5  # failed logins per user and address before the next one is refused
FAILURE_WINDOW = timedelta(minutes=15)
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_MAX_NAME = 200
CSP = "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; frame-ancestors 'none'"
_THROTTLE_KEYS = (
    10_000  # failures of more distinct users than this are forgotten early, oldest first, to bound memory
)
MAX_ADDRESS_FAILURES = 20  # failed logins from one address, whatever the user, before the next one is refused
MAX_PASSWORD = 1024  # characters: a longer password is refused before Argon2 spends time on it
_ANY_USER = "*"

_templates = Jinja2Templates(directory=Path(__file__).parent / "web" / "templates")


@dataclass(frozen=True)
class Actor:
    """Who is acting: a person (`human`), a token (`token`, by its name) or the server itself (`system`), with the role
    it acts in. `user` is the person behind it: the user for `human`, the token's user for `token`, if it has one."""

    kind: Literal["human", "system", "token"]
    name: str
    role: Role = "operator"
    user: str | None = None
    scope: TokenScope = "all"  # `publish`: a token that may only upload to the built-in registry
    grants: frozenset[AccessGrant] = (
        frozenset()
    )  # the user's add-on grants: `approver`, `pii-reader` (design §13.2)
    protected: bool = (
        False  # whether it acts in a protected environment, where a developer's rights are an admin's
    )


class AuthError(Exception):
    """The request may not go on. An API call gets a problem+json answer; a page, a redirect to the login page."""

    def __init__(self, status: int, title: str, detail: str) -> None:
        super().__init__(title)
        self.status, self.title, self.detail = status, title, detail


# Passwords.

_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    """Hash a password with Argon2id and a new salt."""
    return _hasher.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    """Whether `password` is the one `password_hash` was made from. A hash that can't be read is no match."""
    try:
        return _hasher.verify(password_hash, password)
    except (VerificationError, InvalidHashError):
        return False


@cache
def _decoy_hash() -> str:
    """What an unknown user's password is checked against, so that the answer takes as long as for a real one."""
    return hash_password(secrets.token_urlsafe(16))


# Tokens.


def token_digest(token: str) -> str:
    """What the store keeps of a token: its SHA-256, in hex."""
    return hashlib.sha256(token.encode()).hexdigest()


def issue_token(
    store: Store,
    name: str,
    ttl: timedelta | None,
    *,
    role: Role,
    created_by: str | None = None,
    scope: TokenScope = "all",
    clock: Clock | None = None,
) -> str:
    """Create an API token that lasts `ttl` (for ever when None) and return it. This is the only time it is seen:
    the store keeps its SHA-256. That `role` is no higher than `created_by`'s is the caller's to check."""
    token = TOKEN_PREFIX + secrets.token_urlsafe(32)
    now = (clock or SystemClock()).now()
    expires = None if ttl is None else now + ttl
    store.users.create_token(name, token_digest(token), expires, role, created_by, scope)
    return token


def _sent_token(header: str) -> str | None:
    """The token of an `Authorization` header: `Bearer <token>`, or HTTP Basic with the token as the password, as
    `uv`, `pip` and `twine` send it to a package index (the user name, often `__token__`, is not looked at)."""
    scheme, _, value = header.partition(" ")
    value = value.strip()
    if scheme.lower() == "bearer":
        return value or None
    if scheme.lower() == "basic":
        try:
            _, _, password = base64.b64decode(value, validate=True).decode("utf-8").partition(":")
        except (ValueError, UnicodeDecodeError):
            return None
        return password or None
    return None


def _bearer_actor(request: Request, state: "AppState") -> Actor:
    token = _sent_token(request.headers["authorization"])
    if token is None:
        raise AuthError(401, "Unauthorized", "send the token as 'Authorization: Bearer <token>'")
    row = state.store.users.find_token(token_digest(token))
    now = state.clock.now()
    if row is None or row.revoked_at is not None or (row.expires_at is not None and row.expires_at <= now):
        raise AuthError(401, "Unauthorized", "the token is not valid")
    protected = _protected(state)
    if row.created_by is None:
        return Actor("token", row.name, row.role, scope=row.scope, protected=protected)
    user = state.store.users.get_user(row.created_by)
    if user is None:  # its user is gone, and with them what the token may do
        raise AuthError(401, "Unauthorized", "the token is not valid")
    if row.scope == "publish" and user.role != "admin":  # publishing is an admin's, checked at each use
        raise AuthError(
            403, "Forbidden", f"this publish token's user, {user.username}, is no longer an admin"
        )
    role = row.role if ROLES.index(row.role) >= ROLES.index(user.role) else user.role  # the lower
    return Actor("token", row.name, role, user.username, row.scope, frozenset(user.grants), protected)


def _protected(state: "AppState") -> bool:
    """Whether the server's environment is protected (`settings.protected` in its project)."""
    return bool(state.project.settings.get("protected"))


# Sessions. The cookie holds a random id and, once signed in, the user name; it is signed, not encrypted.


@dataclass
class _Session:
    sid: str
    user: str | None = None
    epoch: str | None = (
        None  # see `_epoch`: a session from before a logout or a new password no longer matches
    )


def _epoch(state: "AppState", user: str, password_hash: str) -> str:
    """What a signed-in session must carry to stay valid: it changes when the user logs out or gets a new password,
    so that a copied cookie stops working then, not 12 hours later (M1)."""
    logouts = state.store.overrides.get(f"session:{user}", "logouts") or 0
    key = state.settings.secret_key.encode()
    return hmac.new(key, f"epoch:{user}:{logouts}:{password_hash}".encode(), hashlib.sha256).hexdigest()[:32]


def _serializer(state: "AppState") -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(state.settings.secret_key, salt="feather-session")


def _session(request: Request) -> _Session | None:
    return request.state.session


def _start_session(request: Request, user: str | None = None, epoch: str | None = None) -> _Session:
    """Replace the session with a new one, so that a CSRF token or a cookie from before is of no use."""
    request.state.session = session = _Session(secrets.token_urlsafe(24), user, epoch)
    request.state.session_dirty = True
    return session


def csrf_token(request: Request) -> str:
    """The CSRF token of this request's session, which starts an anonymous one if there is none yet."""
    session = _session(request) or _start_session(request)
    key = request.app.state.feather.settings.secret_key.encode()
    return hmac.new(key, b"csrf:" + session.sid.encode(), hashlib.sha256).hexdigest()


async def require_csrf(request: Request) -> None:
    """Refuse a state-changing request that has no valid CSRF token. A bearer call and a safe method pass."""
    if request.method in _SAFE_METHODS or "authorization" in request.headers:
        return
    sent = request.headers.get("x-csrf-token")
    if sent is None and "form" in request.headers.get("content-type", ""):
        value = (await request.form()).get("csrf_token")
        sent = value if isinstance(value, str) else None
    if _session(request) is None or sent is None or not _same(sent, csrf_token(request)):
        raise AuthError(403, "Forbidden", "the CSRF token is missing or wrong")


def _same(sent: str, expected: str) -> bool:
    """`compare_digest` on bytes, as on text it raises for a non-ASCII value, which must be a refusal (M3)."""
    return hmac.compare_digest(sent.encode("utf-8", "surrogateescape"), expected.encode())


def current_actor(request: Request) -> Actor:
    """The caller, from the bearer token or else the session cookie. Raises `AuthError` (401) when there is none."""
    state: AppState = request.app.state.feather
    if "authorization" in request.headers:
        actor = _bearer_actor(request, state)
        if actor.scope == "publish" and not request.url.path.startswith(REGISTRY_PREFIX):
            raise AuthError(403, "Forbidden", "a publish token can only upload packages to the registry")
        if AGENT_HEADER in request.headers:
            return _as_agent(request, actor, state)
        return actor
    session = _session(request)
    if session is not None and session.user is not None:
        user = state.store.users.get_user(session.user)
        if (
            user is not None
            and session.epoch is not None
            and _same(session.epoch, _epoch(state, user.username, user.password_hash))
        ):
            return Actor(
                "human",
                session.user,
                user.role,
                user.username,
                grants=frozenset(user.grants),
                protected=_protected(state),
            )
    raise AuthError(401, "Unauthorized", "sign in, or send a bearer token")


CurrentActor = Annotated[Actor, Depends(current_actor)]


# Agents (M3.1 A1): an MCP tool's calls carry `X-Feather-Agent-Tool`, and are read only, three times over.

AGENT_HEADER = "x-feather-agent-tool"
_AGENT_TOOL = re.compile(r"^[a-z_]{1,64}$")


class AgentLimiter:
    """Calls per token in the last minute, in this process. An agent that calls more often is refused (429)."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._calls: dict[str, list[datetime]] = {}
        self._lock = threading.Lock()

    def take(self, token: str, per_minute: int) -> bool:
        with self._lock:
            cutoff = self._clock.now() - timedelta(minutes=1)
            recent = [at for at in self._calls.get(token, []) if at > cutoff]
            if len(recent) >= per_minute:
                self._calls[token] = recent
                return False
            self._calls[token] = [*recent, self._clock.now()]
            return True


def _as_agent(request: Request, actor: Actor, state: "AppState") -> Actor:
    """The actor of an agent's call: a viewer token's, with no grants, so PII stays masked. Anything else is refused:
    an agent reads, and a token that could do more is not one to give it. Each call is audited as the agent's."""
    tool = request.headers[AGENT_HEADER]
    if not _AGENT_TOOL.match(tool):
        raise AuthError(400, "Bad Request", "X-Feather-Agent-Tool names the MCP tool, such as list_runs")
    if actor.kind != "token" or actor.role != "viewer":
        raise AuthError(
            403,
            "Forbidden",
            "an agent's calls need a viewer token: feather server create-token claude --role viewer",
        )
    if request.method not in _SAFE_METHODS:
        raise AuthError(403, "Forbidden", "an agent only reads")
    limiter: AgentLimiter = request.app.state.agent_limiter
    if not limiter.take(actor.name, int(os.environ.get("FEATHER_MCP_RATE_PER_MIN", "120"))):
        raise AuthError(429, "Too Many Requests", "the agent calls too often: wait a minute")
    path = request.url.path + (f"?{request.url.query}" if request.url.query else "")
    state.store.audit.record("agent", actor.name, f"mcp:{tool}", None, {"path": path}, "ok")
    return Actor(actor.kind, actor.name, actor.role, actor.user, actor.scope, frozenset(), actor.protected)


# Slowing down guesses.


class LoginThrottle:
    """Counts failed logins by user and address, within a window. Once a key has `MAX_FAILURES` it is refused until
    the oldest of them leaves the window. It lives in one process: with several servers each counts for itself."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._failures: dict[tuple[str, str], list[datetime]] = {}  # oldest key first, as dicts keep order
        self._lock = threading.Lock()  # the login route runs in a thread pool

    def _recent(self, key: tuple[str, str]) -> list[datetime]:
        cutoff = self._clock.now() - FAILURE_WINDOW
        recent = [at for at in self._failures.get(key, []) if at > cutoff]
        if recent:
            self._failures[key] = recent
        else:
            self._failures.pop(key, None)
        return recent

    def blocked(self, key: tuple[str, str]) -> bool:
        """Whether the user and address, or the address with any user, has failed too often (M2)."""
        with self._lock:
            return (
                len(self._recent(key)) >= MAX_FAILURES
                or len(self._recent((_ANY_USER, key[1]))) >= MAX_ADDRESS_FAILURES
            )

    def fail(self, key: tuple[str, str]) -> None:
        with self._lock:
            for counted in (key, (_ANY_USER, key[1])):
                self._failures[counted] = [*self._recent(counted), self._clock.now()]
                self._failures[counted] = self._failures.pop(counted)  # now the newest key
            while len(self._failures) > _THROTTLE_KEYS:
                del self._failures[next(iter(self._failures))]

    def clear(self, key: tuple[str, str]) -> None:
        with self._lock:
            self._failures.pop(key, None)


# The routes.

router = APIRouter()


def _login_page(request: Request, message: str | None = None, status: int = 200) -> Response:
    response = _templates.TemplateResponse(
        request, "login.html", {"csrf_token": csrf_token(request), "message": message}, status_code=status
    )
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/login")
def login_form(request: Request) -> Response:
    return _login_page(request)


@router.post("/login", dependencies=[Depends(require_csrf)])
def login(request: Request, username: Annotated[str, Form()], password: Annotated[str, Form()]) -> Response:
    state: AppState = request.app.state.feather
    throttle: LoginThrottle = request.app.state.login_throttle
    key = (username.lower()[:_MAX_NAME], request.client.host if request.client else "")
    if throttle.blocked(key):
        log.warning("login refused for too many failures from %s", key[1])
        return _login_page(request, "Too many failed attempts. Try again later.", 429)
    if len(password) > MAX_PASSWORD:
        throttle.fail(key)
        return _login_page(request, "The username or the password is wrong.", 401)
    user = state.store.users.get_user(username) if len(username) <= _MAX_NAME else None
    # An unknown user is checked against a decoy, so that nothing tells the two failures apart.
    valid = verify_password(user.password_hash if user else _decoy_hash(), password)
    ip = {"ip": key[1]}
    if user is None or not valid:
        throttle.fail(key)
        state.store.audit.record("human", "anonymous", "login", None, ip, "failed")
        return _login_page(request, "The username or the password is wrong.", 401)
    throttle.clear(key)
    _start_session(request, user.username, _epoch(state, user.username, user.password_hash))
    state.store.audit.record("human", user.username, "login", None, ip, "ok")
    return RedirectResponse("/", status_code=303)


@router.post("/logout", dependencies=[Depends(require_csrf)])
def logout(request: Request, actor: CurrentActor) -> Response:
    state: AppState = request.app.state.feather
    if actor.kind == "human":  # every session of the user ends, a copied cookie's too (M1)
        target = f"session:{actor.name}"
        logouts = state.store.overrides.get(target, "logouts") or 0
        state.store.overrides.set(target, "logouts", logouts + 1, actor=actor.name, reason="logout")
    _start_session(request)
    state.store.audit.record(actor.kind, actor.name, "logout", None, {}, "ok")
    return RedirectResponse("/login", status_code=303)


@router.get("/api/v1/me")
def me(actor: CurrentActor) -> dict[str, str | None]:
    return {"kind": actor.kind, "name": actor.name, "role": actor.role, "user": actor.user}


# Wiring.


def _is_api(request: Request) -> bool:
    path = request.url.path
    return path.startswith(("/api/", REGISTRY_PREFIX)) or "authorization" in request.headers


def _refuse(request: Request, error: AuthError) -> Response:
    if _is_api(request):
        problem = {
            "type": "about:blank",
            "title": error.title,
            "status": error.status,
            "detail": error.detail,
        }
        challenge = (
            'Basic realm="feather registry"' if request.url.path.startswith(REGISTRY_PREFIX) else "Bearer"
        )
        headers = {"WWW-Authenticate": challenge} if error.status == 401 else None
        return JSONResponse(problem, error.status, headers, media_type="application/problem+json")
    if error.status == 401:
        return RedirectResponse("/login", status_code=303)
    return PlainTextResponse(f"{error.title}: {error.detail}", status_code=error.status)


class _SessionCookie:
    """Reads the session cookie, writes it back if it changed, and adds the security headers.

    Plain ASGI, as `BaseHTTPMiddleware` starts a task and streams per request, which ingress can't afford at a burst.
    """

    def __init__(self, app: ASGIApp, state: "AppState", secure: bool) -> None:
        self._app = app
        self._state = state
        self._secure = secure

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        request = Request(scope)
        request.state.session = None
        request.state.session_dirty = False
        raw = request.cookies.get(SESSION_COOKIE)
        if raw:
            try:
                data = _serializer(self._state).loads(raw, max_age=int(SESSION_TTL.total_seconds()))
                request.state.session = _Session(str(data["sid"]), data.get("user"), data.get("epoch"))
            except (BadSignature, KeyError, TypeError):
                pass  # a cookie that can't be trusted is as good as none

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                self._add_headers(request, MutableHeaders(scope=message))
            await send(message)

        await self._app(scope, receive, send_with_headers)

    def _add_headers(self, request: Request, headers: MutableHeaders) -> None:
        session: _Session | None = request.state.session
        if request.state.session_dirty and session is not None:
            cookie = Response()
            cookie.set_cookie(
                SESSION_COOKIE,
                _serializer(self._state).dumps(
                    {"sid": session.sid, "user": session.user, "epoch": session.epoch}
                ),
                max_age=int(SESSION_TTL.total_seconds()),
                httponly=True,
                secure=self._secure,
                samesite="lax",
                path="/",
            )
            headers.append("set-cookie", cookie.headers["set-cookie"])
        headers.setdefault("Content-Security-Policy", CSP)
        headers.setdefault("X-Content-Type-Options", "nosniff")
        headers.setdefault("X-Frame-Options", "DENY")
        headers.setdefault("Referrer-Policy", "same-origin")


def install(app: FastAPI, state: "AppState") -> None:
    """Add the login routes, the session cookie and the security headers to `app`."""
    secure = (urlparse(state.settings.base_url).hostname or "") not in _LOCAL_HOSTS
    app.state.login_throttle = LoginThrottle(state.clock)
    app.state.agent_limiter = AgentLimiter(state.clock)
    app.include_router(router)
    app.add_exception_handler(AuthError, lambda request, error: _refuse(request, error))  # type: ignore[arg-type]

    app.add_middleware(_SessionCookie, state=state, secure=secure)
