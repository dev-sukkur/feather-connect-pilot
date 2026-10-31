"""Local users, API tokens, browser sessions and CSRF (design 14).

A person signs in with a password and gets a signed session cookie. Automation sends `Authorization: Bearer fc_...`.
Only the SHA-256 of a token is stored, so a copy of the database can't be used to call the API. Until RBAC lands in
M2, every user is an administrator.

A request that carries an `Authorization` header is judged by that header alone, never by a cookie it also has, and
needs no CSRF token: a browser doesn't add that header on its own. A request authenticated by cookie needs the
session's CSRF token, in the `csrf_token` form field or the `X-CSRF-Token` header.
"""

import hashlib
import hmac
import logging
import secrets
from collections.abc import Awaitable, Callable
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

from feather_connect.clock import Clock, SystemClock
from feather_connect.storage.port import Store

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

SESSION_COOKIE = "feather_session"
SESSION_TTL = timedelta(hours=12)
TOKEN_PREFIX = "fc_"
MAX_FAILURES = 5  # failed logins per user and address before the next one is refused
FAILURE_WINDOW = timedelta(minutes=15)
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_MAX_NAME = 200
_THROTTLE_KEYS = 10_000  # failures of more distinct users than this are forgotten early, to bound memory

_templates = Jinja2Templates(directory=Path(__file__).parent / "web" / "templates")


@dataclass(frozen=True)
class Actor:
    """Who is acting: a person (`human`), a token (`token`, by its name) or the server itself (`system`)."""

    kind: Literal["human", "system", "token"]
    name: str


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


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def issue_token(store: Store, name: str, ttl: timedelta | None, *, clock: Clock | None = None) -> str:
    """Create an API token that lasts `ttl` (for ever when None) and return it. This is the only time it is seen:
    the store keeps its SHA-256."""
    token = TOKEN_PREFIX + secrets.token_urlsafe(32)
    now = (clock or SystemClock()).now()
    store.users.create_token(name, _digest(token), None if ttl is None else now + ttl)
    return token


def _bearer_actor(request: Request, state: "AppState") -> Actor:
    scheme, _, token = request.headers["authorization"].partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        raise AuthError(401, "Unauthorized", "send the token as 'Authorization: Bearer <token>'")
    row = state.store.users.find_token(_digest(token))
    now = state.clock.now()
    if row is None or row.revoked_at is not None or (row.expires_at is not None and row.expires_at <= now):
        raise AuthError(401, "Unauthorized", "the token is not valid")
    return Actor("token", row.name)


# Sessions. The cookie holds a random id and, once signed in, the user name; it is signed, not encrypted.


@dataclass
class _Session:
    sid: str
    user: str | None = None


def _serializer(state: "AppState") -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(state.settings.secret_key, salt="feather-session")


def _session(request: Request) -> _Session | None:
    return request.state.session


def _start_session(request: Request, user: str | None = None) -> _Session:
    """Replace the session with a new one, so that a CSRF token or a cookie from before is of no use."""
    request.state.session = session = _Session(secrets.token_urlsafe(24), user)
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
    if _session(request) is None or sent is None or not hmac.compare_digest(sent, csrf_token(request)):
        raise AuthError(403, "Forbidden", "the CSRF token is missing or wrong")


def current_actor(request: Request) -> Actor:
    """The caller, from the bearer token or else the session cookie. Raises `AuthError` (401) when there is none."""
    state: AppState = request.app.state.feather
    if "authorization" in request.headers:
        return _bearer_actor(request, state)
    session = _session(request)
    if session is not None and session.user is not None and state.store.users.get_user(session.user):
        return Actor("human", session.user)
    raise AuthError(401, "Unauthorized", "sign in, or send a bearer token")


CurrentActor = Annotated[Actor, Depends(current_actor)]


# Slowing down guesses.


class LoginThrottle:
    """Counts failed logins by user and address, within a window. Once a key has `MAX_FAILURES` it is refused until
    the oldest of them leaves the window. It lives in one process: with several servers each counts for itself."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._failures: dict[tuple[str, str], list[datetime]] = {}

    def _recent(self, key: tuple[str, str]) -> list[datetime]:
        cutoff = self._clock.now() - FAILURE_WINDOW
        recent = [at for at in self._failures.get(key, []) if at > cutoff]
        if recent:
            self._failures[key] = recent
        else:
            self._failures.pop(key, None)
        return recent

    def blocked(self, key: tuple[str, str]) -> bool:
        return len(self._recent(key)) >= MAX_FAILURES

    def fail(self, key: tuple[str, str]) -> None:
        if len(self._failures) >= _THROTTLE_KEYS:
            for stale in list(self._failures):
                self._recent(stale)
        self._failures[key] = [*self._recent(key), self._clock.now()]

    def clear(self, key: tuple[str, str]) -> None:
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
    user = state.store.users.get_user(username) if len(username) <= _MAX_NAME else None
    # An unknown user is checked against a decoy, so that nothing tells the two failures apart.
    valid = verify_password(user.password_hash if user else _decoy_hash(), password)
    ip = {"ip": key[1]}
    if user is None or not valid:
        throttle.fail(key)
        state.store.audit.record("human", "anonymous", "login", None, ip, "failed")
        return _login_page(request, "The username or the password is wrong.", 401)
    throttle.clear(key)
    _start_session(request, user.username)
    state.store.audit.record("human", user.username, "login", None, ip, "ok")
    return RedirectResponse("/", status_code=303)


@router.post("/logout", dependencies=[Depends(require_csrf)])
def logout(request: Request, actor: CurrentActor) -> Response:
    state: AppState = request.app.state.feather
    _start_session(request)
    state.store.audit.record(actor.kind, actor.name, "logout", None, {}, "ok")
    return RedirectResponse("/login", status_code=303)


@router.get("/api/v1/me")
def me(actor: CurrentActor) -> dict[str, str]:
    return {"kind": actor.kind, "name": actor.name}


# Wiring.


def _is_api(request: Request) -> bool:
    return request.url.path.startswith("/api/") or "authorization" in request.headers


def _refuse(request: Request, error: AuthError) -> Response:
    if _is_api(request):
        problem = {
            "type": "about:blank",
            "title": error.title,
            "status": error.status,
            "detail": error.detail,
        }
        headers = {"WWW-Authenticate": "Bearer"} if error.status == 401 else None
        return JSONResponse(problem, error.status, headers, media_type="application/problem+json")
    if error.status == 401:
        return RedirectResponse("/login", status_code=303)
    return PlainTextResponse(f"{error.title}: {error.detail}", status_code=error.status)


def install(app: FastAPI, state: "AppState") -> None:
    """Add the login routes, the session cookie and the security headers to `app`."""
    secure = (urlparse(state.settings.base_url).hostname or "") not in _LOCAL_HOSTS
    app.state.login_throttle = LoginThrottle(state.clock)
    app.include_router(router)
    app.add_exception_handler(AuthError, lambda request, error: _refuse(request, error))  # type: ignore[arg-type]

    @app.middleware("http")
    async def session_cookie(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request.state.session = None
        request.state.session_dirty = False
        raw = request.cookies.get(SESSION_COOKIE)
        if raw:
            try:
                data = _serializer(state).loads(raw, max_age=int(SESSION_TTL.total_seconds()))
                request.state.session = _Session(str(data["sid"]), data.get("user"))
            except (BadSignature, KeyError, TypeError):
                pass  # a cookie that can't be trusted is as good as none
        response = await call_next(request)
        session: _Session | None = request.state.session
        if request.state.session_dirty and session is not None:
            response.set_cookie(
                SESSION_COOKIE,
                _serializer(state).dumps({"sid": session.sid, "user": session.user}),
                max_age=int(SESSION_TTL.total_seconds()),
                httponly=True,
                secure=secure,
                samesite="lax",
                path="/",
            )
        response.headers.setdefault("Content-Security-Policy", "default-src 'self'")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        return response
