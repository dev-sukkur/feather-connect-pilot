"""A client of a Python package index's "simple" API (PEP 503, and PEP 691's JSON form), for the registries an
environment trusts besides the built-in one: list projects, list a project's files, and fetch a wheel with its sha256
checked (flow packages §4.3).

A token is sent as HTTP Basic credentials, `__token__` and the token, as PyPI-style indexes take it; a token of the
form `user:password` is sent as those two, for indexes that want a user name (GitLab deploy tokens, Nexus). As pip
does, it goes only to the index's own origin (scheme, host and port): a file the index links on another host is
fetched without it.
"""

import hashlib
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Self, cast
from urllib.parse import urldefrag, urljoin

import httpx
from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from packaging.version import Version

from feather_connect.packages.wheel import MAX_WHEEL

JSON = "application/vnd.pypi.simple.v1+json"
HTML = "application/vnd.pypi.simple.v1+html"
_ACCEPT = f"{JSON}, {HTML};q=0.2, text/html;q=0.1"
TIMEOUT_S = 30.0


class IndexReadError(Exception):
    """The index can't be read, or what it serves is not what was asked for. The message never holds the token.
    `check` names what a fetched file failed, `size` or `sha256`, for an install to report."""

    def __init__(self, message: str, check: str | None = None) -> None:
        super().__init__(message)
        self.check = check


@dataclass(frozen=True)
class IndexFile:
    """A file a project page lists. `yanked` is False, True, or the reason given (PEP 592)."""

    filename: str
    url: str
    sha256: str | None
    yanked: bool | str = False

    @property
    def version(self) -> str | None:
        """The version of a wheel, from its name; None for any other file."""
        try:
            return str(parse_wheel_filename(self.filename)[1])
        except InvalidWheelFilename:
            return None


class _Links(HTMLParser):
    """The `<a>` elements of a PEP 503 page."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, dict[str, str | None]]] = []
        self._open: dict[str, str | None] | None = None
        self._text = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self._open, self._text = dict(attrs), ""

    def handle_data(self, data: str) -> None:
        if self._open is not None:
            self._text += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._open is not None:
            self.links.append((self._text.strip(), self._open))
            self._open = None


def credentials(token: str | None) -> tuple[str, str] | None:
    if not token:
        return None
    user, sep, password = token.partition(":")
    return (user, password) if sep and not token.startswith("fc_") else ("__token__", token)


def _origin(url: httpx.URL) -> tuple[str, str, int | None]:
    """A URL's scheme, host and port, the port filled in when it is the scheme's own."""
    return url.scheme, url.host, url.port or {"http": 80, "https": 443}.get(url.scheme)


def _check(url: str, answer: httpx.Response) -> None:
    if answer.status_code in (401, 403):
        raise IndexReadError(
            f"{url} refused the credentials ({answer.status_code}): check the registry's token"
        )
    if answer.status_code == 404:
        raise IndexReadError(f"{url} has no such page (404)")
    if not answer.is_success:
        raise IndexReadError(f"{url} answered {answer.status_code}")


class SimpleIndex:
    """The simple API of the index at `url`, such as `https://pkgs.example.com/simple/`."""

    def __init__(
        self,
        url: str,
        token: str | None = None,
        *,
        timeout: float = TIMEOUT_S,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.url = url if url.endswith("/") else url + "/"
        self._origin = _origin(httpx.URL(self.url))
        found = credentials(token)
        self._auth = None if found is None else httpx.BasicAuth(*found)
        # Credentials go per request, to the index's origin only; httpx drops them on a redirect to another origin.
        self._http = httpx.Client(timeout=timeout, transport=transport, follow_redirects=True)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _auth_for(self, url: str) -> httpx.BasicAuth | None:
        """The credentials for `url`: the token only when it is on the index's own origin."""
        return self._auth if _origin(httpx.URL(url)) == self._origin else None

    def _get(self, url: str, accept: str = _ACCEPT) -> httpx.Response:
        try:
            answer = self._http.get(url, headers={"Accept": accept}, auth=self._auth_for(url))
        except httpx.HTTPError as error:
            raise IndexReadError(f"can't reach {url}: {type(error).__name__}") from None
        _check(url, answer)
        return answer

    @contextmanager
    def _stream(self, url: str) -> Generator[httpx.Response]:
        try:
            with self._http.stream("GET", url, headers={"Accept": "*/*"}, auth=self._auth_for(url)) as answer:
                _check(url, answer)
                yield answer
        except httpx.HTTPError as error:
            raise IndexReadError(f"can't reach {url}: {type(error).__name__}") from None

    def _page(self, url: str) -> tuple[list[tuple[str, dict[str, str | None]]], str]:
        """The page's links as (text, attributes), or its JSON as one link per entry, and the URL it came from."""
        answer = self._get(url)
        base = str(answer.url)
        if answer.headers.get("content-type", "").split(";")[0].strip() == JSON:
            found = cast(Mapping[str, Any], answer.json())
            if "projects" in found:
                return [(str(p["name"]), {}) for p in cast(list[Mapping[str, Any]], found["projects"])], base
            links: list[tuple[str, dict[str, str | None]]] = []
            for f in cast(list[Mapping[str, Any]], found.get("files", [])):
                hashes = cast(Mapping[str, str], f.get("hashes") or {})
                url_ = str(f["url"]) + (f"#sha256={hashes['sha256']}" if "sha256" in hashes else "")
                yanked = f.get("yanked", False)
                attrs: dict[str, str | None] = {"href": url_}
                if yanked:
                    attrs["data-yanked"] = yanked if isinstance(yanked, str) else ""
                links.append((str(f["filename"]), attrs))
            return links, base
        parser = _Links()
        parser.feed(answer.text)
        return parser.links, base

    def projects(self) -> list[str]:
        """The projects the index lists, by their names as it gives them."""
        links, _ = self._page(self.url)
        return [text for text, _ in links if text]

    def files(self, project: str) -> list[IndexFile]:
        """The files of a project, with their sha256 when the index gives one."""
        links, base = self._page(urljoin(self.url, canonicalize_name(project) + "/"))
        found: list[IndexFile] = []
        for text, attrs in links:
            href = attrs.get("href")
            if not href:
                continue
            url, fragment = urldefrag(urljoin(base, href))
            sha256 = fragment.removeprefix("sha256=") if fragment.startswith("sha256=") else None
            filename = text or url.rsplit("/", 1)[-1]
            yanked: bool | str = False
            if "data-yanked" in attrs:
                yanked = attrs["data-yanked"] or True
            found.append(IndexFile(filename, url, sha256, yanked))
        return found

    def versions(self, project: str) -> list[str]:
        """The wheel versions of a project, newest first."""
        found = {f.version for f in self.files(project) if f.version is not None}
        return sorted(found, key=Version, reverse=True)

    def find(self, project: str, version: str) -> IndexFile:
        """The wheel of the project's version. Raises `IndexReadError` when there is none, or more than one, since a flow
        package has one wheel per version."""
        wheels = [f for f in self.files(project) if f.version == version and f.filename.endswith(".whl")]
        if not wheels:
            raise IndexReadError(f"{self.url} has no wheel of {project} {version}")
        if len(wheels) > 1:
            raise IndexReadError(
                f"{self.url} has {len(wheels)} wheels of {project} {version}; a flow package has one"
            )
        return wheels[0]

    def fetch(self, file: IndexFile, max_bytes: int = MAX_WHEEL) -> bytes:
        """The file's bytes, once their sha256 matches the one the index gave. A file without one is refused: nothing
        else says the bytes are the ones published."""
        if file.sha256 is None:
            raise IndexReadError(f"{file.filename}: the index gives no sha256 for it, so it can't be checked")
        too_big = IndexReadError(f"{file.filename} has more than the {max_bytes} bytes allowed", "size")
        chunks: list[bytes] = []
        size = 0
        with self._stream(file.url) as answer:
            if int(answer.headers.get("content-length") or 0) > max_bytes:
                raise too_big
            for chunk in answer.iter_bytes():
                size += len(chunk)
                if size > max_bytes:  # counted as the bytes come, whatever the header said
                    raise too_big
                chunks.append(chunk)
        data = b"".join(chunks)
        digest = hashlib.sha256(data).hexdigest()
        if digest != file.sha256.lower():
            raise IndexReadError(
                f"{file.filename}: its sha256 is {digest}, not the {file.sha256} the index gives", "sha256"
            )
        return data
