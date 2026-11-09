"""The environment's own configuration (flow packages §8): a layer over a package's files, the package prefix on every
name, and encrypted `db` secrets."""

import textwrap
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from feather_connect.clock import FrozenClock
from feather_connect.engine.local import LocalEngine
from feather_connect.project import (
    ConnectionConfig,
    ProjectError,
    check_environment,
    environment_layer,
    load_project,
    resolve_secret,
    use_db_secrets,
)
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import EnvEntry, SecretStore
from feather_connect.storage.spool import MemorySpool
from feather_connect.vault import (
    KeyConflict,
    Keyring,
    MasterKey,
    NoMasterKey,
    Vault,
    VaultError,
    _legacy_aad,  # pyright: ignore[reportPrivateUsage]
    keyring_from_env,
    new_master_key,
)

T0 = datetime(2026, 11, 9, 9, tzinfo=UTC)
FLOW = """
    import feather_connect as fc
    from feather_connect.connectors import memory

    invoices = fc.Dataset("invoices", source=memory.Source("zoho", [{"id": 1}, {"id": 2}]), key="id",
                          strategy=fc.strategy.replace())
    flow = fc.Flow("invoices", reads=invoices, deliver=[fc.to(memory.Sink("erp"), name="erp")],
                   triggers=[fc.every("1h")])
"""


def _package(root: Path, name: str) -> Path:
    (root / "flows").mkdir(parents=True)
    (root / "env").mkdir()
    (root / "feather.yaml").write_text(f"name: {name}\nmodules: [flows.invoices]\n", encoding="utf-8")
    (root / "env" / "dev.yaml").write_text("", encoding="utf-8")
    (root / "flows" / "__init__.py").write_text("", encoding="utf-8")
    (root / "flows" / "invoices.py").write_text(textwrap.dedent(FLOW), encoding="utf-8")
    return root


ENVIRONMENT = [
    EnvEntry("dev", "connection", "zoho", {"connector": "memory.source", "budget": "zoho-api"}, "root", T0),
    EnvEntry("dev", "connection", "erp", {"connector": "memory.sink", "budget": "zoho-api"}, "root", T0),
    EnvEntry("dev", "budget", "zoho-api", {"limits": ["3/hour"]}, "root", T0),
    EnvEntry("dev", "setting", "timezone", "Asia/Kolkata", "root", T0),
]


def test_two_packages_with_one_short_flow_name_run_side_by_side_on_shared_config(tmp_path: Path):
    layer = environment_layer(ENVIRONMENT)
    a = load_project(_package(tmp_path / "a", "acme-a"), "dev", environment=layer, package="acme-a")
    b = load_project(_package(tmp_path / "b", "acme-b"), "dev", environment=layer, package="acme-b")
    assert [f.name for f in a.flows] == ["acme-a/invoices"] and [f.name for f in b.flows] == [
        "acme-b/invoices"
    ]
    assert [d.name for d in a.flows[0].datasets] == ["acme-a/invoices"]
    assert [r.name for r in b.flows[0].routes] == ["acme-b/erp"]
    assert a.connections["zoho"].budget == b.connections["zoho"].budget == "zoho-api"  # not prefixed: shared
    assert a.settings["timezone"] == "Asia/Kolkata" and a.layers["settings.timezone"] == "environment"

    clock = FrozenClock(T0)
    store, engine = MemoryStore(clock), LocalEngine(clock)
    store.budgets.define(a.budgets["zoho-api"])
    runtime = Runtime([*a.flows, *b.flows], store, MemorySpool(clock), engine, clock=clock)
    engine.register(runtime.tasks())
    runs = [runtime.start_run(f.name) for f in (*a.flows, *b.flows)]
    engine.run_until_idle()
    assert [store.runs.get_run(r).status for r in runs] == ["succeeded", "succeeded"]
    assert {r.flow for r in store.runs.list_runs()} == {"acme-a/invoices", "acme-b/invoices"}

    # One budget, however many packages call through it: 3 units an hour between them.
    first = store.budgets.acquire(a.connections["zoho"].budget or "", 2)
    second = store.budgets.acquire(b.connections["zoho"].budget or "", 2)
    assert first.granted and not second.granted


def test_an_environment_connection_overrides_the_packages_own(tmp_path: Path):
    root = _package(tmp_path / "a", "acme-a")
    (root / "feather.yaml").write_text(
        "name: acme-a\nmodules: [flows.invoices]\nconnections:\n  zoho: {connector: memory.source, timeout: 5}\n"
        "  erp: {connector: memory.sink}\n",
        encoding="utf-8",
    )
    layer = environment_layer([replace(ENVIRONMENT[0], value={"connector": "memory.source", "timeout": 30})])
    project = load_project(root, "dev", environment=layer)
    assert project.connections["zoho"].options["timeout"] == 30
    assert project.layers["connections.zoho.timeout"] == "environment"


class _OtherTenant:
    """A secret store of another tenant: the same rows, another tenant in the associated data."""

    tenant = "t2"

    def __init__(self, inner: SecretStore) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _key() -> MasterKey:
    return MasterKey.parse(new_master_key())


def test_a_db_secret_resolves_only_with_a_vault():
    store = MemoryStore(FrozenClock(T0))
    vault = Vault(store.secrets, "dev", _key())
    vault.set("zoho/client_secret", "s3cr3t", "root", T0)
    try:
        use_db_secrets(None)
        with pytest.raises(ProjectError, match="only by a server or worker"):
            resolve_secret("${secret:db:zoho/client_secret}")
        use_db_secrets(vault.open)
        assert resolve_secret("${secret:db:zoho/client_secret}") == "s3cr3t"
        with pytest.raises(ProjectError, match="is not set"):
            resolve_secret("${secret:db:other}")
        # A connection opens only the secret named for it and the field.
        own = ConnectionConfig(
            "zoho", "memory.source", {}, {"client_secret": "${secret:db:zoho/client_secret}"}
        )
        assert own.resolve()["client_secret"] == "s3cr3t"
        other = ConnectionConfig("erp", "memory.sink", {}, {"password": "${secret:db:zoho/client_secret}"})
        with pytest.raises(ProjectError, match="may refer only to"):
            other.resolve()
    finally:
        use_db_secrets(None)


def test_no_secret_is_read_from_a_feather_variable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FEATHER_MASTER_KEY", new_master_key())
    with pytest.raises(ProjectError, match="FEATHER_"):
        resolve_secret("${secret:env:FEATHER_MASTER_KEY}")
    root = _package(tmp_path / "a", "acme-a")
    (root / "feather.yaml").write_text(
        "name: acme-a\nmodules: [flows.invoices]\nconnections:\n"
        "  zoho: {connector: memory.source, auth: {token: '${secret:env:feather_master_key}'}}\n"
        "  erp: {connector: memory.sink}\n",
        encoding="utf-8",
    )
    with pytest.raises(ProjectError, match="FEATHER_"):
        load_project(root, "dev")


def test_a_package_may_point_a_connection_only_at_its_own_db_secret(tmp_path: Path):
    root = _package(tmp_path / "a", "acme-a")
    (root / "feather.yaml").write_text(
        "name: acme-a\nmodules: [flows.invoices]\nconnections:\n"
        "  zoho: {connector: memory.source, auth: {token: '${secret:db:erp/password}'}}\n"
        "  erp: {connector: memory.sink}\n",
        encoding="utf-8",
    )
    with pytest.raises(ProjectError, match="may refer only to"):
        load_project(root, "dev")


def test_the_environment_is_checked_as_a_server_loads_it():
    check_environment(
        {
            "budgets": {"api": {"limits": ["3/hour"]}},
            "connections": {"zoho": {"connector": "x", "budget": "api"}},
        }
    )
    for bad, why in (
        ({"budgets": {"api": {"limits": ["lots"]}}}, "not valid"),
        ({"connections": {"zoho": {"connector": "x", "budget": "none"}}}, "no budget"),
        ({"settings": {"timezone": "Mars/Olympus"}}, "time zone"),
        ({"connections": {"zoho": {"connector": "x", "url": "${secret:db:zoho/url}"}}}, "only under"),
    ):
        with pytest.raises(ProjectError, match=why):
            check_environment(bad)


def test_a_bad_entry_is_left_out_with_a_warning_and_the_rest_load(caplog: pytest.LogCaptureFixture):
    rows = [
        *ENVIRONMENT,
        EnvEntry("dev", "budget", "broken", {"limits": ["lots"]}, "root", T0),
        EnvEntry("dev", "connection", "orphan", {"connector": "x", "budget": "broken"}, "root", T0),
        EnvEntry("dev", "setting", "timezone", "Mars/Olympus", "root", T0),
    ]
    with caplog.at_level("WARNING"):
        layer = environment_layer(rows[:3] + rows[4:])
    assert set(layer["budgets"]) == {"zoho-api"} and set(layer["connections"]) == {"zoho", "erp"}
    assert "settings" not in layer  # the one timezone given was bad
    assert "broken" in caplog.text and "orphan" in caplog.text and "timezone" in caplog.text


def test_secrets_are_sealed_and_open_only_with_their_key():
    store = MemoryStore(FrozenClock(T0))
    key = _key()
    vault = Vault(store.secrets, "prod", key)
    assert vault.set("zakya/client_secret", "client-secret-value", "root", T0) is False
    sealed = store.secrets.get("prod", "zakya/client_secret")
    assert sealed is not None and b"client-secret-value" not in sealed.ciphertext and len(sealed.nonce) == 12
    assert vault.open("zakya/client_secret") == "client-secret-value"
    assert vault.set("zakya/client_secret", "client-secret-value", "ops", T0) is True
    again = store.secrets.get("prod", "zakya/client_secret")
    assert again is not None and again.nonce != sealed.nonce  # a fresh nonce each time

    wrong = Vault(store.secrets, "prod", _key())
    with pytest.raises(VaultError, match="sealed with master key"):
        wrong.open("zakya/client_secret")
    # Bytes copied under another name don't open: the name is associated data, and so are env and tenant.
    store.secrets.put(replace(again, name="other"))
    with pytest.raises(VaultError, match="doesn't open"):
        vault.open("other")
    store.secrets.put(
        replace(again, name="tampered", ciphertext=bytes([again.ciphertext[0] ^ 1]) + again.ciphertext[1:])
    )
    with pytest.raises(VaultError, match="doesn't open"):
        vault.open("tampered")
    other_tenant = _OtherTenant(MemoryStore(FrozenClock(T0)).secrets)
    other_tenant.put(again)  # the bytes copied to another tenant's store
    with pytest.raises(VaultError, match="doesn't open"):
        Vault(cast(SecretStore, other_tenant), "prod", key).open("zakya/client_secret")
    assert key.key_id in repr(key) and str(key.key) not in repr(key)


def test_a_secret_sealed_before_the_tenant_was_associated_data_opens_and_a_rotation_seals_it_anew():
    import os

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    from feather_connect.storage.port import SealedSecret

    store = MemoryStore(FrozenClock(T0))
    key = _key()
    nonce = os.urandom(12)
    old = AESGCM(key.key).encrypt(nonce, b"legacy", _legacy_aad("prod", "s"))
    store.secrets.put(SealedSecret("prod", "s", old, nonce, key.key_id, "root", T0))
    vault = Vault(store.secrets, "prod", key)
    assert vault.open("s") == "legacy"
    assert vault.rotate("root", T0) == 1
    resealed = store.secrets.get("prod", "s")
    assert resealed is not None
    with pytest.raises(Exception):  # noqa: B017 - no longer the legacy associated data
        AESGCM(key.key).decrypt(resealed.nonce, resealed.ciphertext, _legacy_aad("prod", "s"))
    assert vault.open("s") == "legacy"


def test_without_a_master_key_db_secrets_fail_clearly_and_listing_works():
    store = MemoryStore(FrozenClock(T0))
    Vault(store.secrets, "prod", _key()).set("s", "v", "root", T0)
    keyless = Vault(store.secrets, "prod", None)
    assert [s.name for s in keyless.names()] == ["s"]
    with pytest.raises(NoMasterKey, match="FEATHER_MASTER_KEY"):
        keyless.open("s")
    with pytest.raises(NoMasterKey):
        keyless.set("t", "v", "root", T0)


def test_a_rotation_across_processes_with_a_keyring():
    """Two processes: the old one has the old key current and the new one previous; the new one the other way round.
    Until the rotation the old key is active and the new process may open but not set; after it, the other way
    round, and a process restarted with only the new key opens everything."""
    store = MemoryStore(FrozenClock(T0))
    old, new = _key(), _key()
    before = Vault(store.secrets, "prod", Keyring(old, (new,)))
    after = Vault(store.secrets, "prod", Keyring(new, (old,)))
    for i in range(3):
        before.set(f"s{i}", f"v{i}", "root", T0)
    assert after.open("s1") == "v1"
    with pytest.raises(KeyConflict, match="sealed with master key"):
        after.set("s9", "v9", "root", T0)
    assert after.rotate("root", T0) == 3
    assert store.secrets.active_key("prod") == new.key_id
    with pytest.raises(KeyConflict):
        before.set("s1", "stale", "root", T0)  # a process that still has the old key current can't write
    assert before.open("s2") == "v2"  # but it opens, with the new key as a previous one
    after.set("s3", "v3", "root", T0)
    restarted = Vault(store.secrets, "prod", Keyring(new))
    assert [restarted.open(f"s{i}") for i in range(4)] == ["v0", "v1", "v2", "v3"]
    with pytest.raises(VaultError):
        Vault(store.secrets, "prod", old).open("s0")


def test_a_set_during_a_rotation_waits_and_then_is_refused():
    import threading
    import time

    store = MemoryStore(FrozenClock(T0))
    old, new = _key(), _key()
    before = Vault(store.secrets, "prod", Keyring(old, (new,)))
    after = Vault(store.secrets, "prod", Keyring(new, (old,)))
    before.set("a", "1", "root", T0)
    sealing, release = threading.Event(), threading.Event()
    real = after._seal  # pyright: ignore[reportPrivateUsage]

    def slow(*args: object, **kwargs: object) -> object:
        sealing.set()
        release.wait(5)
        return real(*args, **kwargs)  # pyright: ignore[reportArgumentType]

    after._seal = slow  # type: ignore[method-assign]  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage]
    rotating = threading.Thread(target=after.rotate, args=("root", T0))
    rotating.start()
    assert sealing.wait(5)
    outcome: list[object] = []
    setter = threading.Thread(target=lambda: outcome.append(_try(lambda: before.set("b", "2", "root", T0))))
    setter.start()
    time.sleep(0.2)
    assert outcome == []  # waiting for the rotation
    release.set()
    rotating.join(5)
    setter.join(5)
    assert isinstance(outcome[0], KeyConflict) and store.secrets.get("prod", "b") is None


def _try(fn: object) -> object:
    try:
        return fn()  # type: ignore[operator]  # pyright: ignore[reportCallIssue]
    except Exception as error:  # noqa: BLE001
        return error


def test_a_rotation_seals_more_than_a_listing_holds():
    from feather_connect.storage.port import MAX_ENV_ROWS

    store = MemoryStore(FrozenClock(T0))
    old, new = _key(), _key()
    vault = Vault(store.secrets, "prod", old)
    for i in range(MAX_ENV_ROWS + 1):
        vault.set(f"s{i}", "v", "root", T0)
    assert Vault(store.secrets, "prod", Keyring(new, (old,))).rotate("root", T0) == MAX_ENV_ROWS + 1
    assert Vault(store.secrets, "prod", new).open(f"s{MAX_ENV_ROWS}") == "v"


def test_the_keyring_comes_from_the_environment_or_files(tmp_path: Path):
    key, previous = new_master_key(), new_master_key()
    assert keyring_from_env({}) is None
    assert keyring_from_env({"FEATHER_MASTER_KEY": key}) == Keyring(MasterKey.parse(key))
    (tmp_path / "key").write_text(key + "\n", encoding="utf-8")
    (tmp_path / "old").write_text(previous + "\n", encoding="utf-8")
    ring = keyring_from_env(
        {
            "FEATHER_MASTER_KEY_FILE": str(tmp_path / "key"),
            "FEATHER_MASTER_KEY_PREVIOUS_FILE": str(tmp_path / "old"),
        }
    )
    assert ring == Keyring(MasterKey.parse(key), (MasterKey.parse(previous),))
    for bad in ("not base64!", "c2hvcnQ="):
        with pytest.raises(VaultError, match="feather secret new-key"):
            keyring_from_env({"FEATHER_MASTER_KEY": bad})
    with pytest.raises(VaultError, match="without FEATHER_MASTER_KEY"):
        keyring_from_env({"FEATHER_MASTER_KEY_PREVIOUS": previous})


def test_a_config_field_is_secret_by_its_type_not_its_repr():
    from typing import Annotated

    from pydantic import BaseModel, SecretStr

    from feather_connect.core.connector import ConnectorSpec, secret_fields

    class Config(BaseModel):
        token: SecretStr
        maybe: SecretStr | None = None
        noted: Annotated[SecretStr | None, "x"] = None
        SecretStrName: str = ""  # a name that looks like one is not one

    spec = ConnectorSpec(name="t.source", config=Config)
    assert secret_fields(spec) == {"token", "maybe", "noted"}
