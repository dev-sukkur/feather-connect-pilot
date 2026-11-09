"""`SimpleIndex`: reading a PEP 503 or PEP 691 index, with Basic credentials and every wheel's sha256 checked."""

import base64
import hashlib

import httpx
import pytest

from feather_connect.packages.index import IndexReadError, SimpleIndex, credentials

WHEEL = b"wheel bytes"
SHA = hashlib.sha256(WHEEL).hexdigest()
JSON = "application/vnd.pypi.simple.v1+json"


def _index(handler, token: str | None = "t0k") -> SimpleIndex:
    return SimpleIndex("https://pkgs.example.com/simple", token, transport=httpx.MockTransport(handler))


def test_credentials_are_basic_with_the_token_as_password():
    assert credentials(None) is None and credentials("") is None
    assert credentials("abc") == ("__token__", "abc")
    assert credentials("deploy:s3cret") == ("deploy", "s3cret")
    assert credentials("fc_with:colon") == ("__token__", "fc_with:colon")  # a Feather token is one piece


def test_the_html_form_lists_projects_files_versions_and_yanks():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["authorization"])
        if request.url.path == "/simple/":
            return httpx.Response(
                200, html='<a href="acme-invoices/">acme-invoices</a><a href="zed/">Zed</a>'
            )
        return httpx.Response(
            200,
            html=(
                f'<a href="../../f/acme_invoices-1.0.0-py3-none-any.whl#sha256={SHA}">acme_invoices-1.0.0-py3-none-any.whl</a>'
                f'<a href="../../f/acme_invoices-1.1.0-py3-none-any.whl#sha256={SHA}" data-yanked="">'
                "acme_invoices-1.1.0-py3-none-any.whl</a>"
                '<a href="../../f/acme-invoices-1.1.0.tar.gz">acme-invoices-1.1.0.tar.gz</a>'
            ),
        )

    with _index(handler) as index:
        assert index.projects() == ["acme-invoices", "Zed"]
        files = index.files("Acme_Invoices")
        assert [(f.version, f.yanked, f.sha256) for f in files] == [
            ("1.0.0", False, SHA),
            ("1.1.0", True, SHA),
            (None, False, None),
        ]
        assert files[0].url == "https://pkgs.example.com/f/acme_invoices-1.0.0-py3-none-any.whl"
        assert index.versions("acme-invoices") == ["1.1.0", "1.0.0"]
    assert set(seen) == {"Basic " + base64.b64encode(b"__token__:t0k").decode()}


def test_the_json_form_and_fetching_with_the_sha256_checked():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/simple/acme-invoices/":
            files = [
                {
                    "filename": "acme_invoices-1.0.0-py3-none-any.whl",
                    "url": "/f/a.whl",
                    "hashes": {"sha256": SHA},
                },
                {
                    "filename": "acme_invoices-2.0.0-py3-none-any.whl",
                    "url": "/f/b.whl",
                    "hashes": {},
                    "yanked": "bad",
                },
                {
                    "filename": "acme_invoices-3.0.0-py3-none-any.whl",
                    "url": "/f/c.whl",
                    "hashes": {"sha256": "0" * 64},
                },
            ]
            return httpx.Response(
                200, json={"meta": {"api-version": "1.1"}, "files": files}, headers={"content-type": JSON}
            )
        return httpx.Response(200, content=WHEEL)

    with _index(handler) as index:
        one = index.find("acme-invoices", "1.0.0")
        assert index.fetch(one) == WHEEL
        two = index.find("acme-invoices", "2.0.0")
        assert two.yanked == "bad"
        with pytest.raises(IndexReadError, match="gives no sha256"):
            index.fetch(two)
        with pytest.raises(IndexReadError, match="its sha256 is"):
            index.fetch(index.find("acme-invoices", "3.0.0"))
        with pytest.raises(IndexReadError, match="more than the 3 bytes allowed"):
            index.fetch(one, max_bytes=3)
        with pytest.raises(IndexReadError, match="no wheel of acme-invoices 9.0.0"):
            index.find("acme-invoices", "9.0.0")


def test_a_refusal_or_an_outage_says_so_without_the_token():
    def refuse(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    def down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with (
        _index(refuse, "secret-token") as index,
        pytest.raises(IndexReadError, match="refused the credentials") as e,
    ):
        index.projects()
    assert "secret-token" not in str(e.value)
    with _index(down) as index, pytest.raises(IndexReadError, match="can't reach"):
        index.projects()


def test_the_token_goes_only_to_the_index_origin():  # as pip does: a file on another host gets no credentials
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen[f"{request.url.scheme}://{request.url.netloc.decode()}"] = request.headers.get("authorization")
        if request.url.path == "/simple/acme-invoices/":
            files = [
                {"filename": "acme_invoices-1.0.0-py3-none-any.whl", "url": "https://cdn.evil.test/a.whl"},
                {
                    "filename": "acme_invoices-2.0.0-py3-none-any.whl",
                    "url": "https://pkgs.example.com:8443/b.whl",
                },
                {"filename": "acme_invoices-3.0.0-py3-none-any.whl", "url": "http://pkgs.example.com/c.whl"},
                {
                    "filename": "acme_invoices-4.0.0-py3-none-any.whl",
                    "url": "https://pkgs.example.com:443/d.whl",
                },
            ]
            files = [{**f, "hashes": {"sha256": SHA}} for f in files]
            return httpx.Response(200, json={"files": files}, headers={"content-type": JSON})
        return httpx.Response(200, content=WHEEL)

    with _index(handler) as index:
        for version in ("1.0.0", "2.0.0", "3.0.0", "4.0.0"):
            assert index.fetch(index.find("acme-invoices", version)) == WHEEL
    assert seen["https://pkgs.example.com"] is not None  # the index page and d.whl, on the default port
    assert seen["https://cdn.evil.test"] is None
    assert seen["https://pkgs.example.com:8443"] is None
    assert seen["http://pkgs.example.com"] is None


def test_a_fetch_stops_at_the_cap_whatever_the_length_header_says():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/simple/acme-invoices/":
            files = [
                {
                    "filename": "acme_invoices-1.0.0-py3-none-any.whl",
                    "url": "/f/a.whl",
                    "hashes": {"sha256": SHA},
                }
            ]
            return httpx.Response(200, json={"files": files}, headers={"content-type": JSON})
        return httpx.Response(200, content=iter([b"x" * 4] * 1000))  # streamed, no Content-Length

    with _index(handler) as index, pytest.raises(IndexReadError, match="more than the 10 bytes allowed"):
        index.fetch(index.find("acme-invoices", "1.0.0"), max_bytes=10)
