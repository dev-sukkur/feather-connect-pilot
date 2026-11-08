"""The URL given to DBOS: our timeouts added, and everything else in it kept (S22). No database needed."""

from urllib.parse import parse_qs, parse_qsl, unquote, urlsplit

from feather_connect.storage.postgres import support
from feather_engine_dbos.engine import system_database_url


def test_blank_and_repeated_query_keys_are_kept() -> None:  # S22
    url = system_database_url("postgresql://u:p@h:1/d?options=&target_session_attrs=any&host=a&host=b")
    pairs = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    assert ("options", "") in pairs and [v for k, v in pairs if k == "host"] == ["a", "b"]
    assert dict(pairs)["connect_timeout"] == str(support.CONNECT_TIMEOUT_S)


def test_a_keyword_dsn_becomes_a_url() -> None:  # S22
    url = system_database_url(
        "host=db.local port=6543 dbname=feather user=ann password='p w' sslmode=require"
    )
    parts = urlsplit(url)
    assert parts.scheme == "postgresql" and parts.hostname == "db.local" and parts.port == 6543
    assert parts.path == "/feather" and parts.username == "ann" and unquote(parts.password or "") == "p w"
    assert parse_qs(parts.query)["sslmode"] == ["require"]
