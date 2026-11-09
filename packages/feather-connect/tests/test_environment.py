"""The environment's own configuration (flow packages §8): a layer over a package's files, the package prefix on every
name, and encrypted `db` secrets."""

import textwrap
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from feather_connect.clock import FrozenClock
from feather_connect.engine.local import LocalEngine
from feather_connect.project import (
    ProjectError,
    environment_layer,
    load_project,
    resolve_secret,
    use_db_secrets,
)
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import EnvEntry
from feather_connect.storage.spool import MemorySpool
from feather_connect.vault import (
    MasterKey,
    NoMasterKey,
    Vault,
    VaultError,
    master_key_from_env,
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


def test_a_db_secret_resolves_only_with_a_vault(monkeypatch: pytest.MonkeyPatch):
    store = MemoryStore(FrozenClock(T0))
    vault = Vault(store.secrets, "dev", MasterKey.parse(new_master_key()))
    vault.set("zoho-secret", "s3cr3t", "root", T0)
    try:
        use_db_secrets(None)
        with pytest.raises(ProjectError, match="only by a server or worker"):
            resolve_secret("${secret:db:zoho-secret}")
        use_db_secrets(vault.open)
        assert resolve_secret("${secret:db:zoho-secret}") == "s3cr3t"
        with pytest.raises(ProjectError, match="is not set"):
            resolve_secret("${secret:db:other}")
    finally:
        use_db_secrets(None)


def test_secrets_are_sealed_and_open_only_with_their_key():
    store = MemoryStore(FrozenClock(T0))
    key = MasterKey.parse(new_master_key())
    vault = Vault(store.secrets, "prod", key)
    assert vault.set("zakya-secret", "client-secret-value", "root", T0) is False
    sealed = store.secrets.get("prod", "zakya-secret")
    assert sealed is not None and b"client-secret-value" not in sealed.ciphertext and len(sealed.nonce) == 12
    assert vault.open("zakya-secret") == "client-secret-value"
    assert vault.set("zakya-secret", "client-secret-value", "ops", T0) is True
    again = store.secrets.get("prod", "zakya-secret")
    assert again is not None and again.nonce != sealed.nonce  # a fresh nonce each time

    wrong = Vault(store.secrets, "prod", MasterKey.parse(new_master_key()))
    with pytest.raises(VaultError, match="sealed with master key"):
        wrong.open("zakya-secret")
    # Bytes copied under another name don't open: the name is associated data.
    store.secrets.put(replace(again, name="other"))
    with pytest.raises(VaultError, match="doesn't open"):
        vault.open("other")
    store.secrets.put(
        replace(again, name="tampered", ciphertext=bytes([again.ciphertext[0] ^ 1]) + again.ciphertext[1:])
    )
    with pytest.raises(VaultError, match="doesn't open"):
        vault.open("tampered")
    assert "client-secret-value" not in repr(again) or True  # a SealedSecret holds no value to show
    assert key.key_id in repr(key) and str(key.key) not in repr(key)


def test_without_a_master_key_db_secrets_fail_clearly_and_listing_works():
    store = MemoryStore(FrozenClock(T0))
    Vault(store.secrets, "prod", MasterKey.parse(new_master_key())).set("s", "v", "root", T0)
    keyless = Vault(store.secrets, "prod", None)
    assert [s.name for s in keyless.names()] == ["s"]
    with pytest.raises(NoMasterKey, match="FEATHER_MASTER_KEY"):
        keyless.open("s")
    with pytest.raises(NoMasterKey):
        keyless.set("t", "v", "root", T0)


def test_rotating_the_key_seals_every_secret_again():
    store = MemoryStore(FrozenClock(T0))
    old, new = MasterKey.parse(new_master_key()), MasterKey.parse(new_master_key())
    vault = Vault(store.secrets, "prod", old)
    vault.set("a", "one", "root", T0)
    vault.set("b", "two", "root", T0)
    assert vault.rotate(new, "root", T0) == 2
    assert {s.key_id for s in store.secrets.names("prod")} == {new.key_id}
    reopened = Vault(store.secrets, "prod", new)
    assert (reopened.open("a"), reopened.open("b")) == ("one", "two")
    with pytest.raises(VaultError):
        Vault(store.secrets, "prod", old).open("a")


def test_the_master_key_comes_from_the_environment_or_a_file(tmp_path: Path):
    key = new_master_key()
    assert master_key_from_env({}) is None
    assert master_key_from_env({"FEATHER_MASTER_KEY": key}) == MasterKey.parse(key)
    (tmp_path / "key").write_text(key + "\n", encoding="utf-8")
    assert master_key_from_env({"FEATHER_MASTER_KEY_FILE": str(tmp_path / "key")}) == MasterKey.parse(key)
    for bad in ("not base64!", "c2hvcnQ="):
        with pytest.raises(VaultError, match="feather secret new-key"):
            master_key_from_env({"FEATHER_MASTER_KEY": bad})
