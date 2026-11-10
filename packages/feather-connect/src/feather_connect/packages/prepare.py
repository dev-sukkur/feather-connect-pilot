"""Preparing an installed version (flow packages §5 steps 3 and 4), on a worker: build its dependency environment, then
load-check it in that environment, and record the version `ready` or `failed`.

This runs in the worker supervisor, as the worker's user, never in the server. The server records an install
`preparing` once the checks it can make without running package code pass (fetch, sha256, manifest, compatibility,
lock); the first supervisor to prepare the version records what it found, and others change nothing (`prepared`
compares and sets). A failed version says which step failed in its load check's `check`:

- `environment`: the version's wheel or lock is not what it names. An environment that can't be built is not a
  failure of the version, as the network or an index may be away: `prepare_version` raises `EnvError`, the version
  stays `preparing`, and the supervisor tries again later;
- `load_check`: in a child process, in that environment, the code doesn't import, load for this environment, or
  define what its manifest says (`check.load_check`);
- `webhook_connector`: a push dataset reads with a source class of the package's own, or a declared webhook isn't
  valid: ingress runs in the server, which never loads package code (§7.2, §12).
"""

import hashlib
import logging
from collections.abc import Callable, Iterable
from typing import Any

from feather_connect.clock import Clock
from feather_connect.packages import manifest as m
from feather_connect.packages.build import LOCK_FILE
from feather_connect.packages.check import DEFAULT_TIMEOUT, LoadCheck, load_check
from feather_connect.packages.envs import EnvError, Environments
from feather_connect.packages.webhooks import parse_webhooks
from feather_connect.packages.wheel import read_entry
from feather_connect.project import environment_layer
from feather_connect.storage.port import PackageVersion, Store, check_wheel

__all__ = ["PREPARE_STEPS", "lock_of", "prepare_version"]

log = logging.getLogger(__name__)

PREPARE_STEPS = ("environment", "load_check", "webhook_connector")
Checker = Callable[..., LoadCheck]


def lock_of(content: bytes, row: PackageVersion) -> bytes:
    """The locked requirements in the version's wheel, checked against its lock hash. Raises `ValueError`."""
    where = f"{m.module_of(row.name)}-{row.version}.dist-info/{LOCK_FILE}"
    lock = read_entry(content, where) or b""
    if hashlib.sha256(lock).hexdigest() != row.lock_hash:
        raise ValueError(f"{where} is not the lock the version's lock_hash names")
    return lock


def _judged(found: LoadCheck) -> tuple[str | None, str | None]:
    """The step that failed and why, or (None, None)."""
    if not found.ok:
        return "load_check", found.error or "the package doesn't load"
    if found.own_push_sources:
        return "webhook_connector", (
            f"the push datasets {', '.join(found.own_push_sources)} read with a source of the package's own: "
            "webhooks are verified and parsed in the server, which runs only Feather Connect's connectors"
        )
    try:
        declared = parse_webhooks(found.webhooks)
    except ValueError as error:
        return "webhook_connector", f"a declared webhook is not valid: {error}"
    unknown = sorted(h.dataset for h in declared.values() if h.dataset not in found.datasets)
    if unknown:
        return "webhook_connector", f"declared webhooks feed datasets it doesn't define: {unknown}"
    return None, None


def prepare_version(
    store: Store,
    row: PackageVersion,
    *,
    env: str,
    envs: Environments | None,
    worker_id: str,
    clock: Clock,
    checker: Checker = load_check,
    timeout: float = DEFAULT_TIMEOUT,
    keep: Iterable[str] = (),
) -> dict[str, Any]:
    """Build the version's environment and load-check it there, record the version `ready` or `failed`, and return
    the load check as recorded. With no `envs`, the check runs on this process's Python, as tests do. The environment
    is held in use while the check runs, and `keep` names lock hashes that making room for it doesn't remove. Raises
    `EnvError` when the environment can't be built, recording that against the worker, not the version."""

    def record(result: dict[str, Any]) -> dict[str, Any]:
        state = "ready" if result.get("ok") else "failed"
        if store.packages.prepared(row.name, row.version, state, result):
            log.info("%s %s is %s", row.name, row.version, state)
        return result

    def failed(step: str, error: str, found: dict[str, Any] | None = None) -> dict[str, Any]:
        return record({**(found or {}), "ok": False, "check": step, "error": error, "worker": worker_id})

    content = store.packages.wheel(row.sha256)
    if content is None:
        return failed("environment", "the store has no wheel of this version's sha256")
    try:
        check_wheel(row, content)
        lock = lock_of(content, row)
    except ValueError as error:
        return failed("environment", str(error))
    environment = environment_layer(store.env_config.entries(env))
    options: dict[str, Any] = {"env": env, "timeout": timeout, "environment": environment}
    if envs is None:
        found = checker(content, row.manifest, **options)
    else:
        store.packages.record_env(worker_id, row.lock_hash, "building", clock.now())
        try:
            use = envs.use(lock, row.lock_hash, keep=keep)
        except EnvError as error:
            store.packages.record_env(worker_id, row.lock_hash, "failed", clock.now(), str(error))
            raise
        store.packages.record_env(worker_id, row.lock_hash, "ready", clock.now())
        with use:
            found = checker(content, row.manifest, **options, python=str(use.python))
    step, error = _judged(found)
    if step is not None:
        return failed(step, error or "", found.as_json())
    return record({**found.as_json(), "worker": worker_id})
