"""The `dev` user of `feather dev`: made once with a generated password, and given a new one on request."""

from datetime import UTC, datetime

import pytest

from feather_cli.commands.dev import DEV_USER, _announce, _ensure_dev_user
from feather_connect.clock import FrozenClock
from feather_connect.storage.memory import MemoryStore
from feather_server.auth import verify_password


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore(FrozenClock(datetime(2026, 10, 18, tzinfo=UTC)))


def hash_of(store: MemoryStore) -> str:
    user = store.users.get_user(DEV_USER)
    assert user is not None
    return user.password_hash


def test_the_user_is_made_once_and_its_password_is_given_only_then(store):
    password = _ensure_dev_user(store)
    assert password and verify_password(hash_of(store), password)
    assert _ensure_dev_user(store) is None
    assert verify_password(hash_of(store), password)  # untouched


def test_a_reset_gives_a_new_password_to_the_same_user(store):
    old = _ensure_dev_user(store)
    assert old
    before = store.users.get_user(DEV_USER)
    new = _ensure_dev_user(store, reset=True)
    assert new and new != old
    after = store.users.get_user(DEV_USER)
    assert before is not None and after is not None and after.user_id == before.user_id
    assert verify_password(after.password_hash, new) and not verify_password(after.password_hash, old)


def test_a_reset_with_no_user_makes_it(store):
    password = _ensure_dev_user(store, reset=True)
    assert password and verify_password(hash_of(store), password)


def test_the_message_for_an_existing_user_names_the_flag_before_a_wipe(capsys):
    document = {"url": "http://localhost:1", "user": "dev", "password": None, "project": "p"}
    _announce(document, own_database=False)
    out = capsys.readouterr().out
    assert "--reset-password" in out and "already exists" in out
    assert "password:" not in out  # nothing that looks like a password is shown
    assert out.index("--reset-password") < out.index("docker rm")  # the gentler way comes first
