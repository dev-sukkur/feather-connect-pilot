"""The flow project loader: reads `feather.yaml` and an environment overlay, imports the flow modules and
applies the overlay to the flows.

Configuration is layered, and a later layer wins (design §5.6): the defaults in code, `feather.yaml`, then
`env/<env>.yaml`. Tenant files come in M3 and runtime overrides later. Dictionaries merge key by key, and any
other value, a list included, is replaced as a whole.

Secrets are references of the form `${secret:<provider>:<path>}`, and they may stand only under a connection's
`auth`. Loading checks that each reference names a provider that exists, but never resolves one: that happens
when a connection is used, in `ConnectionConfig.resolve` or `resolve_secret`. A secret's value therefore never
sits in a `Project`, in its repr or in an error message.
"""

import copy
import importlib
import logging
import os
import re
import subprocess
import sys
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

from feather_connect.core.connector import is_unsigned, signature_secret_of
from feather_connect.core.flow import Flow, Problem, RouteSpec
from feather_connect.core.triggers import OnEvent, Trigger, cron, every, manual
from feather_connect.storage.port import BudgetSpec, parse_limits

__all__ = [
    "ConfigValue",
    "ConnectionConfig",
    "DevProcess",
    "Project",
    "ProjectError",
    "check_binding",
    "check_environment",
    "connection_configs",
    "db_secret_name",
    "effective_config",
    "environment_layer",
    "is_secret_ref",
    "load_project",
    "resolve_secret",
    "use_secret_broker",
    "validate_project",
    "webhook_secret_name",
]

log = logging.getLogger(__name__)

# The connectors an environment names to run a connection on simulation stand-ins (B2).
SIMULATED = frozenset({"simulation.source", "simulation.sink"})

# What the code itself decides when no file does.
DEFAULTS: dict[str, Any] = {"settings": {"timezone": "UTC"}}
_DEFAULTS_LAYER = "defaults"
_PROJECT_FILE = "feather.yaml"
_SECRET_PROVIDERS = frozenset({"env", "file", "db", "vault", "aws-sm", "azure-kv", "gcp-sm"})
_LOCAL_PROVIDERS = frozenset({"env", "file", "db"})
_ENVIRONMENT_LAYER = "environment"  # the environment's own configuration, from the database (§8)
# Opens `${secret:db:<name>}`: set by a server or worker that has the environment's secret store.
_db_secrets: Callable[[str], str] | None = None
# Resolves every credential of a connection, `(connection, field, reference) -> value`, in a process that holds none of
# them: a package host, whose supervisor resolves them (flow packages §11). None resolves them here.
_secret_broker: Callable[[str, str, str], str] | None = None
_SECRET = re.compile(r"^\$\{secret:([^:{}]+):(.+)\}$")
_CONNECTION_KEYS = frozenset({"connector", "site", "budget", "auth"})
_ENV_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_MASK = "***"


class ProjectError(Exception):
    """The project can't be loaded, or a secret can't be resolved. The message says which file and key, and never
    holds a secret's value."""


@dataclass(frozen=True)
class ConnectionConfig:
    """A named connection. `options` is its settings, `secret_refs` maps each credential to its reference (not its
    value), `site` says where its worker runs (None for the cloud) and `budget` names the budget that meters it."""

    name: str
    connector: str
    options: Mapping[str, Any]
    secret_refs: Mapping[str, str]
    site: str | None = None
    budget: str | None = None
    root: Path | None = None
    """The project directory, which a relative `file` secret's path is read against."""

    def resolve(self) -> dict[str, Any]:
        """Return the settings with every credential resolved, as a connector's `Context.config` holds them. A `db`
        secret opens only for the connection and field it is named for, `<connection>/<field>`."""
        for key, ref in self.secret_refs.items():
            check_binding(ref, self.name, key)
        broker = _secret_broker
        if broker is not None:
            return {
                **self.options,
                **{key: broker(self.name, key, ref) for key, ref in self.secret_refs.items()},
            }
        return {
            **self.options,
            **{key: resolve_secret(ref, self.root) for key, ref in self.secret_refs.items()},
        }


@dataclass(frozen=True)
class DevProcess:
    """A helper process that `feather dev` runs beside the stack, such as a simulator of an external system: its
    `name`, the `command` it runs in the project directory (a leading `python` is the interpreter feather runs on),
    and the `port` it listens on, if any. Other commands ignore them."""

    name: str
    command: tuple[str, ...]
    port: int | None = None


@dataclass(frozen=True)
class Project:
    """A loaded project for one environment.

    `flows` have the overlay applied. `config` is the merged configuration and `layers` says, for each leaf of
    it, which layer set it. Both are what `effective_config` reads. `code_sha` identifies the code.
    """

    name: str
    root: Path
    env: str
    flows: tuple[Flow, ...]
    connections: Mapping[str, ConnectionConfig]
    budgets: Mapping[str, BudgetSpec]
    settings: Mapping[str, Any]
    code_sha: str
    config: Mapping[str, Any] = field(default_factory=dict[str, Any], repr=False)
    layers: Mapping[str, str] = field(default_factory=dict[str, str], repr=False)
    dev_processes: tuple[DevProcess, ...] = ()


@dataclass(frozen=True)
class ConfigValue:
    """One effective knob: its dotted path, its value (`***` for a secret) and the layer that set it."""

    path: str
    value: Any
    layer: str


def is_secret_ref(value: object) -> bool:
    """Whether `value` is written as a secret reference, `${secret:<provider>:<path>}`, rather than a value."""
    return isinstance(value, str) and _SECRET.match(value) is not None


def db_secret_name(connection: str, field: str) -> str:
    """The name a connection's `db` secret must have: it opens for that connection and field only."""
    return f"{connection}/{field}"


def webhook_secret_name(package: str | None, webhook: str) -> str:
    """The name a declared webhook's `db` secret must have."""
    return f"{package}/webhooks/{webhook}" if package else f"webhooks/{webhook}"


def check_binding(ref: str, connection: str, field: str) -> None:
    """Refuse a `db` reference that isn't named for this connection's field, so that no connection can be pointed at
    another's secret."""
    match = _SECRET.match(ref)
    if match is not None and match.group(1) == "db" and match.group(2) != db_secret_name(connection, field):
        raise ProjectError(
            f"connection {connection}: auth.{field} may refer only to ${{secret:db:{db_secret_name(connection, field)}}}"
        )


# The variables that hold feather's own secrets: no connection may read them. Others, such as a deployment's
# FEATHER_DATABASE_URL, may be given to a connection in the project's files, which the operator controls.
RESERVED_VARIABLES = frozenset(
    {
        "FEATHER_MASTER_KEY",
        "FEATHER_MASTER_KEY_FILE",
        "FEATHER_MASTER_KEY_PREVIOUS",
        "FEATHER_MASTER_KEY_PREVIOUS_FILE",
        "FEATHER_SECRET_KEY",
    }
)
_KEY_FILES = ("FEATHER_MASTER_KEY_FILE", "FEATHER_MASTER_KEY_PREVIOUS_FILE")


def _check_reserved(provider: str, path: str) -> None:
    if provider == "env" and path.upper() in RESERVED_VARIABLES:
        raise ProjectError(
            f"a secret may not be read from {path.upper()}: it holds feather's own secret, such as its master key"
        )


def _check_key_file(file: Path) -> None:
    """Refuse a `file` reference to the master key's own file."""
    for name in _KEY_FILES:
        if (where := os.environ.get(name)) and Path(where).resolve() == file.resolve():
            raise ProjectError(
                f"a secret may not be read from the file {name} names: it is feather's master key"
            )


def resolve_secret(ref: str, root: Path | None = None) -> str:
    """Return the value that the reference `${secret:env:NAME}` or `${secret:file:PATH}` points to.

    A relative file path is read against `root`, the project directory, when one is given.

    The other providers arrive in M3. An error names the reference, never the value.
    """
    match = _SECRET.match(ref)
    if match is None:
        raise ProjectError("not a secret reference: write it as ${secret:<provider>:<path>}")
    provider, path = match.groups()
    _check_provider(provider)
    _check_reserved(provider, path)
    if provider == "db":
        if _db_secrets is None:
            raise ProjectError(
                f"secret {ref}: db secrets are opened only by a server or worker with a secret store"
            )
        try:
            return _db_secrets(path)
        except Exception as error:  # noqa: BLE001 - whatever the opener raises names the secret, never its value
            raise ProjectError(f"secret {ref}: {error}") from None
    if provider == "env":
        value = os.environ.get(path)
        if value is None:
            raise ProjectError(f"secret {ref}: the environment variable {path!r} is not set")
        return value
    try:
        file = Path(path)
        if root is not None and not file.is_absolute():
            file = root / file
        _check_key_file(file)
        return file.read_text(encoding="utf-8").rstrip("\r\n")
    except OSError as error:
        raise ProjectError(f"secret {ref}: can't read the file ({error.strerror})") from None


def use_db_secrets(opener: Callable[[str], str] | None) -> None:
    """Open `${secret:db:<name>}` references with `opener`, or refuse them with None."""
    global _db_secrets
    _db_secrets = opener


def use_secret_broker(broker: Callable[[str, str, str], str] | None) -> None:
    """Resolve every connection credential with `broker(connection, field, reference)`, or here again with None."""
    global _secret_broker
    _secret_broker = broker


def _check_provider(provider: str) -> None:
    if provider not in _SECRET_PROVIDERS:
        known = ", ".join(sorted(_SECRET_PROVIDERS))
        raise ProjectError(f"unknown secret provider {provider!r}: use one of {known}")
    if provider not in _LOCAL_PROVIDERS:
        raise ProjectError(f"the secret provider {provider!r} arrives in M3: use env or file for now")


def load_project(
    root: Path,
    env: str,
    *,
    simulate: bool = False,
    environment: Mapping[str, Any] | None = None,
    package: str | None = None,
) -> Project:
    """Load the project in `root` for the environment `env`. Raises `ProjectError` with the file and key at fault.

    `environment` is the environment's own configuration (`connections`, `budgets` and `settings`, as
    `environment_layer` makes them from the database), merged over the project's files as the layer `environment`, so
    that a shared connection or budget overrides a package's own of the same name. `package` names an installed flow
    package: every flow, dataset and route then carries its prefix, `<package>/<name>` (§8).

    `simulate` runs every connection on simulation stand-ins, as if each one's `connector:` named a simulation
    connector: what `feather run --sim` does. Sources then read `fixtures/<dataset>.json[l]`, and no secret is needed.
    """
    config, layers = _layered(root, env, environment, package)
    name = _text(config, "name", layers)
    budgets = _budgets(config, layers)
    connections = _connections(config, layers, budgets, root)
    dev_processes = _dev_processes(config, layers)
    settings: Mapping[str, Any] = config.get("settings", {})
    flows = _apply_overlay(_import_flows(root, config, layers), config, layers, str(settings["timezone"]))
    if simulate:
        connections = _all_simulated(flows, connections)
    flows = _simulated(flows, connections, root)
    _check_push_sources(flows, connections, env)
    if package is not None:
        flows = prefixed(flows, package)
    return Project(
        name,
        root,
        env,
        flows,
        connections,
        budgets,
        settings,
        _code_sha(root),
        copy.deepcopy(config),
        layers,
        dev_processes,
    )


def effective_config(project: Project) -> list[ConfigValue]:
    """Return every knob with its effective value and the layer that set it. Secrets show as `***`."""
    values: list[ConfigValue] = []
    for path, value in _leaves(project.config, ""):
        layer = project.layers[path]
        parts = path.split(".")
        if parts[0] == "connections" and len(parts) > 2 and parts[2] not in _CONNECTION_KEYS:
            parts.insert(2, "options")
        shown = _MASK if isinstance(value, str) and _SECRET.match(value) else value
        values.append(ConfigValue(".".join(parts), shown, layer))
    return values


# Reading and merging the layers.


def _strkeys(mapping: Any) -> dict[str, Any]:
    """Return a copy of a mapping read from YAML, with its keys as text."""
    return {str(key): value for key, value in cast(dict[Any, Any], mapping).items()}


def _read_yaml(label: str, path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        raise ProjectError(f"{label}: no such file in {path.parent}") from None
    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise ProjectError(f"{label}: not valid YAML ({error})") from None
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ProjectError(f"{label}: the top level must be a mapping")
    return _strkeys(loaded)


def _merge(
    target: dict[str, Any], layers: dict[str, str], layer: Mapping[str, Any], label: str, at: str
) -> None:
    """Merge `layer` into `target`, and record `label` for every leaf it sets."""
    for key, value in layer.items():
        path = f"{at}.{key}" if at else key
        if isinstance(value, dict):
            if not isinstance(target.get(key), dict):
                target[key] = {}
            _merge(target[key], layers, _strkeys(value), label, path)
        else:
            target[key] = copy.deepcopy(value)
            layers[path] = label


def _leaves(node: Mapping[str, Any], at: str) -> Iterator[tuple[str, Any]]:
    for key, value in node.items():
        path = f"{at}.{key}" if at else key
        if isinstance(value, dict):
            yield from _leaves(_strkeys(value), path)
        else:
            yield path, value


def _where(layers: Mapping[str, str], path: str) -> str:
    """Return `file: path`, naming the layer that set the path or, for a mapping, something under it."""
    layer = layers.get(path)
    if layer is None:
        layer = next((v for k, v in layers.items() if k.startswith(path + ".")), _PROJECT_FILE)
    return f"{layer}: {path}"


def _mapping(config: Mapping[str, Any], key: str, layers: Mapping[str, str]) -> dict[str, Any]:
    value = config.get(key, {})
    if not isinstance(value, dict):
        raise ProjectError(f"{_where(layers, key)}: must be a mapping")
    return _strkeys(value)


def _text(config: Mapping[str, Any], key: str, layers: Mapping[str, str]) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value:
        raise ProjectError(f"{_PROJECT_FILE}: {key} is required and must be text")
    return value


# Secrets.


def _check_secrets(config: Mapping[str, Any], layers: Mapping[str, str], package: str | None = None) -> None:
    """Allow secret references under a connection's `auth` only, and only with a provider that exists. Require
    that everything under `auth` is a reference, so that no credential is committed as plain text. A `db` secret must
    be named for its connection and field (or its webhook), and no secret is read from a `FEATHER_` variable."""
    for path, value in _leaves(config, ""):
        parts = path.split(".")
        under_auth = parts[0] == "connections" and len(parts) > 3 and parts[2] == "auth"
        # A declared webhook's secret (`webhooks.<name>.secret`) is a reference too (flow packages §7.2).
        under_auth = under_auth or (parts[0] == "webhooks" and len(parts) == 3 and parts[2] == "secret")
        is_ref = isinstance(value, str) and "${secret:" in value
        if is_ref and not under_auth:
            raise ProjectError(f"{_where(layers, path)}: secrets may stand only under a connection's auth")
        if under_auth and not (isinstance(value, str) and _SECRET.match(value)):
            raise ProjectError(
                f"{_where(layers, path)}: a credential must be a reference such as ${{secret:env:NAME}}"
            )
        if under_auth:
            match = _SECRET.match(str(value))
            assert match is not None
            try:
                _check_provider(match.group(1))
                _check_reserved(match.group(1), match.group(2))
                if parts[0] == "connections":
                    check_binding(str(value), parts[1], parts[3])
                elif match.group(1) == "db" and match.group(2) != webhook_secret_name(package, parts[1]):
                    raise ProjectError(
                        f"a webhook's db secret is ${{secret:db:{webhook_secret_name(package, parts[1])}}}"
                    )
            except ProjectError as error:
                raise ProjectError(f"{_where(layers, path)}: {error}") from None


# Connections and budgets.


def _budgets(config: Mapping[str, Any], layers: Mapping[str, str]) -> dict[str, BudgetSpec]:
    found: dict[str, BudgetSpec] = {}
    for name, raw in _mapping(config, "budgets", layers).items():
        path = f"budgets.{name}"
        if not isinstance(raw, dict):
            raise ProjectError(f"{_where(layers, path)}: must be a mapping")
        body = _strkeys(raw)
        limits = body.get("limits")
        if not isinstance(limits, list) or not limits:
            raise ProjectError(
                f"{_where(layers, path + '.limits')}: give a list of limits such as '90/minute'"
            )
        account = body.get("account")
        if account is not None and (not isinstance(account, str | int) or not str(account).strip()):
            raise ProjectError(
                f"{_where(layers, path + '.account')}: name the vendor account, such as 'zakya:60001'"
            )
        try:
            reserve = {k: float(v) for k, v in _strkeys(body.get("reserve", {})).items()}
            found[name] = BudgetSpec(
                name,
                parse_limits([str(limit) for limit in cast(list[Any], limits)]),
                reserve,
                None if account is None else str(account),
            )
        except (ValueError, TypeError, AttributeError):
            raise ProjectError(f"{_where(layers, path)}: limits and reserve are not valid") from None
    return found


def _dev_processes(config: Mapping[str, Any], layers: Mapping[str, str]) -> tuple[DevProcess, ...]:
    """Read `dev.processes`: a list of `{name, command, port}`, names unique."""
    raw = _mapping(config, "dev", layers).get("processes", [])
    where = _where(layers, "dev.processes")
    if not isinstance(raw, list):
        raise ProjectError(f"{where}: must be a list of {{name, command, port}}")
    found: list[DevProcess] = []
    for i, item in enumerate(cast(list[Any], raw)):
        if not isinstance(item, dict):
            raise ProjectError(f"{where}[{i}]: must be a mapping with name, command and port")
        body = _strkeys(item)
        name, command, port = body.get("name"), body.get("command"), body.get("port")
        if unknown := sorted(body.keys() - {"name", "command", "port"}):
            raise ProjectError(f"{where}[{i}]: unknown key {unknown[0]!r}; use name, command and port")
        if not isinstance(name, str) or not name:
            raise ProjectError(f"{where}[{i}].name: is required and must be text")
        if name in {p.name for p in found}:
            raise ProjectError(f"{where}[{i}].name: {name!r} is used twice")
        words = cast(list[Any], command) if isinstance(command, list) else []
        if not words or not all(isinstance(w, str | int) and not isinstance(w, bool) for w in words):
            raise ProjectError(f"{where}[{i}].command: give a list of words, such as [python, sims/api.py]")
        if port is not None and (isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536):
            raise ProjectError(f"{where}[{i}].port: must be a port number from 1 to 65535")
        found.append(DevProcess(name, tuple(str(w) for w in words), port))
    return tuple(found)


def _layered(
    root: Path, env: str, environment: Mapping[str, Any] | None, package: str | None
) -> tuple[dict[str, Any], dict[str, str]]:
    """The project's configuration for `env`, its files and the environment's own layer merged, and where each value
    came from. Reads files only: no code of the project runs."""
    if not _ENV_NAME.match(env):
        raise ProjectError(f"{env!r} is not an environment name: use letters, digits, - and _")
    layer_files = [(_PROJECT_FILE, root / _PROJECT_FILE), (f"env/{env}.yaml", root / "env" / f"{env}.yaml")]
    config: dict[str, Any] = {}
    layers: dict[str, str] = {}
    _merge(config, layers, DEFAULTS, _DEFAULTS_LAYER, "")
    for label, path in layer_files:
        _merge(config, layers, _read_yaml(label, path), label, "")
    if environment:
        _merge(config, layers, _strkeys(environment), _ENVIRONMENT_LAYER, "")
    _check_secrets(config, layers, package)
    return config, layers


def connection_configs(
    root: Path, env: str, *, environment: Mapping[str, Any] | None = None, package: str | None = None
) -> dict[str, ConnectionConfig]:
    """The project's connections for `env`, as `load_project` makes them, from its files and the environment's layer
    alone: no code of the project is imported, so a process that must not run it, such as a worker supervisor, can
    tell which credentials a package's connections name."""
    config, layers = _layered(root, env, environment, package)
    return _connections(config, layers, _budgets(config, layers), root)


def _connections(
    config: Mapping[str, Any], layers: Mapping[str, str], budgets: Mapping[str, BudgetSpec], root: Path
) -> dict[str, ConnectionConfig]:
    found: dict[str, ConnectionConfig] = {}
    for name, raw in _mapping(config, "connections", layers).items():
        path = f"connections.{name}"
        if not isinstance(raw, dict):
            raise ProjectError(f"{_where(layers, path)}: must be a mapping")
        body = _strkeys(raw)
        connector = body.get("connector")
        if not isinstance(connector, str):
            raise ProjectError(f"{_where(layers, path + '.connector')}: required, e.g. 'motherduck'")
        budget = body.get("budget")
        if budget is not None and budget not in budgets:
            raise ProjectError(f"{_where(layers, path + '.budget')}: no budget is defined as {budget!r}")
        auth = {k: str(v) for k, v in _strkeys(body.get("auth", {})).items()}
        options = {k: v for k, v in body.items() if k not in _CONNECTION_KEYS}
        site = body.get("site")
        found[name] = ConnectionConfig(
            name, connector, options, auth, None if site is None else str(site), budget, root
        )
    return found


# Flows.


@contextmanager
def _importable(root: Path) -> Generator[None]:
    sys.path.insert(0, str(root))
    importlib.invalidate_caches()
    try:
        yield
    finally:
        sys.path.remove(str(root))


def _import_flows(root: Path, config: Mapping[str, Any], layers: Mapping[str, str]) -> list[Flow]:
    modules = config.get("modules", [])
    if not isinstance(modules, list) or not all(isinstance(m, str) for m in cast(list[Any], modules)):
        raise ProjectError(f"{_where(layers, 'modules')}: give a list of module names such as flows.orders")
    found: dict[str, Flow] = {}
    with _importable(root):
        for index, module_name in enumerate(cast(list[str], modules)):
            where = f"{_where(layers, 'modules')}[{index}] {module_name!r}"
            for stale in [m for m in sys.modules if m.split(".")[0] == module_name.split(".")[0]]:
                del sys.modules[stale]  # a project loaded twice gets its flows built twice
            try:
                module = importlib.import_module(module_name)
            except Exception as error:
                raise ProjectError(f"{where}: can't import it ({type(error).__name__}: {error})") from error
            in_module = [v for v in vars(module).values() if isinstance(v, Flow)]
            if not in_module:
                raise ProjectError(f"{where}: the module defines no Flow")
            for flow in in_module:
                if found.setdefault(flow.name, flow) is not flow:
                    raise ProjectError(f"{where}: two different flows are named {flow.name!r}")
    return list(found.values())


def _apply_overlay(
    flows: Sequence[Flow], config: Mapping[str, Any], layers: Mapping[str, str], timezone: str
) -> tuple[Flow, ...]:
    """Return the flows with the overlay's `flows:` section applied: the flow or its routes enabled or not, triggers replaced."""
    overlay = _mapping(config, "flows", layers)
    by_name = {flow.name: flow for flow in flows}
    for name in overlay:
        if name not in by_name:
            raise ProjectError(f"{_where(layers, f'flows.{name}')}: the project has no flow named {name!r}")
    result: list[Flow] = []
    for flow in flows:
        knobs = overlay.get(flow.name)
        if knobs is None:
            result.append(flow)
            continue
        path = f"flows.{flow.name}"
        if not isinstance(knobs, dict) or not set(cast(dict[str, Any], knobs)) <= {
            "triggers",
            "routes",
            "enabled",
        }:
            raise ProjectError(f"{_where(layers, path)}: may hold only triggers, routes and enabled")
        changed = copy.copy(flow)
        knobs = cast(dict[str, Any], knobs)
        if "enabled" in knobs:
            if not isinstance(knobs["enabled"], bool):
                raise ProjectError(f"{_where(layers, f'{path}.enabled')}: must be true or false")
            changed.enabled = knobs["enabled"]
        if "routes" in knobs:
            changed.routes = _enabled_routes(flow, knobs["routes"], path, layers)
        if "triggers" in knobs:
            kept = tuple(
                t for t in flow.triggers if isinstance(t, OnEvent)
            )  # what a flow is started by in code
            changed.triggers = kept + _triggers(knobs["triggers"], f"{path}.triggers", layers, timezone)
        result.append(changed)
    return tuple(result)


def _enabled_routes(flow: Flow, raw: Any, path: str, layers: Mapping[str, str]) -> list[RouteSpec]:
    if not isinstance(raw, dict):
        raise ProjectError(f"{_where(layers, path + '.routes')}: must be a mapping")
    routes = cast(dict[str, Any], raw)
    known = {route.name for route in flow.routes}
    for name, knobs in routes.items():
        where = _where(layers, f"{path}.routes.{name}")
        if name not in known:
            raise ProjectError(f"{where}: the flow has no route named {name!r}")
        if not isinstance(knobs, dict) or set(cast(dict[str, Any], knobs)) != {"enabled"}:
            raise ProjectError(f"{where}: may hold only enabled: true or false")
        if not isinstance(cast(dict[str, Any], knobs)["enabled"], bool):
            raise ProjectError(f"{where}.enabled: must be true or false")
    return [r for r in flow.routes if cast(dict[str, Any], routes.get(r.name, {"enabled": True}))["enabled"]]


def _triggers(raw: Any, path: str, layers: Mapping[str, str], timezone: str) -> tuple[Trigger, ...]:
    if not isinstance(raw, dict) or not set(cast(dict[str, Any], raw)) <= {"cron", "tz", "every", "manual"}:
        raise ProjectError(f"{_where(layers, path)}: use cron (with tz), every or manual")
    knobs = cast(dict[str, Any], raw)
    found: list[Trigger] = []
    try:
        if "cron" in knobs:
            found.append(cron(str(knobs["cron"]), tz=str(knobs.get("tz", timezone))))
        if "every" in knobs:
            found.append(every(str(knobs["every"])))
        if knobs.get("manual"):
            found.append(manual())
    except (ValueError, KeyError) as error:  # a bad duration, or a time zone that doesn't exist
        raise ProjectError(f"{_where(layers, path)}: {error}") from None
    return tuple(found)


def _fixture_of(root: Path, dataset: str, connection: str) -> Path:
    for suffix in (".json", ".jsonl"):
        path = root / "fixtures" / f"{dataset}{suffix}"
        if path.is_file():
            return path
    raise ProjectError(
        f"connection {connection!r} runs on simulation, and dataset {dataset!r} reads it: "
        f"add its rows as fixtures/{dataset}.json or fixtures/{dataset}.jsonl"
    )


def _simulated(
    flows: Sequence[Flow], connections: Mapping[str, ConnectionConfig], root: Path
) -> tuple[Flow, ...]:
    """Swap the sources and sinks of each connection whose `connector:` is `simulation.source` or `simulation.sink`
    for simulation stand-ins that keep the real spec (B2): dev and qa run the project's own flows on fixtures from
    `fixtures/<dataset>.json[l]`. A connector that is a simulation one already stays as it is."""
    from feather_connect.connectors import simulation

    def simulated(connection: str, name: str) -> bool:
        config = connections.get(connection)
        return config is not None and config.connector == name

    result: list[Flow] = []
    for flow in flows:
        datasets = {
            d.name: replace(
                d, source=simulation.stand_in_source(d.source, _fixture_of(root, d.name, d.source.connection))
            )
            for d in flow.datasets
            if simulated(d.source.connection, "simulation.source")
            and d.source.spec.name != "simulation.source"
        }
        routes = [
            replace(r, sink=simulation.stand_in_sink(r.sink))
            if simulated(r.sink.connection, "simulation.sink") and r.sink.spec.name != "simulation.sink"
            else r
            for r in flow.routes
        ]
        if not datasets and routes == list(flow.routes):
            result.append(flow)
            continue
        changed = copy.copy(flow)
        changed.datasets = [datasets.get(d.name, d) for d in flow.datasets]
        changed.routes = routes
        changed.triggers = tuple(_retargeted(t, datasets) for t in flow.triggers)
        result.append(changed)
    return tuple(result)


def _all_simulated(
    flows: Sequence[Flow], connections: Mapping[str, ConnectionConfig]
) -> dict[str, ConnectionConfig]:
    """The connections as simulation ones, with no secrets: those the flows read from as `simulation.source`, the
    others as `simulation.sink`."""
    sources = {d.source.connection for f in flows for d in f.datasets}
    return {
        name: replace(
            config,
            connector="simulation.source" if name in sources else "simulation.sink",
            secret_refs={},
        )
        for name, config in connections.items()
    }


def _retargeted(trigger: Trigger, datasets: Mapping[str, Any]) -> Trigger:
    """An `on_event` trigger names its dataset: point it at the dataset with the stand-in."""
    if isinstance(trigger, OnEvent):
        found = datasets.get(trigger.dataset.name)
        if found is not None:
            return replace(trigger, dataset=found)
    return trigger


def _check_push_sources(flows: Sequence[Flow], connections: Mapping[str, ConnectionConfig], env: str) -> None:
    """Refuse a push source that would take events from anybody, unless it says that is what it means (design 13.5).

    It must be able to verify, and the secret it verifies against must be a reference under its connection's `auth`. The
    reference is not resolved here: one that doesn't resolve is found when an event arrives, which is then told to retry.
    """
    for flow in flows:
        for dataset in flow.datasets:
            source = dataset.source
            if dataset.strategy.name != "push" or is_unsigned(source):
                continue
            where = f"flow {flow.name!r}, dataset {dataset.name!r}"
            secret = signature_secret_of(source)
            if secret is None or not callable(getattr(source, "verify", None)):
                raise ProjectError(
                    f"{where}: {source.spec.name} takes events but has no signature to verify them by. "
                    "Give it a signature_secret, or mark it unsigned=True if anybody may really send them"
                )
            connection = connections.get(source.connection)
            if connection is None or secret not in connection.secret_refs:
                raise ProjectError(
                    f"{where}: events are verified with auth.{secret} of connection {source.connection!r}, which "
                    f"{env!r} does not define. Add it as a reference such as ${{secret:env:NAME}}, or mark the "
                    "source unsigned=True if anybody may really send them"
                )


def _account_problem(project: Project, flow: str, connection: str, obj: Any, path: str) -> Problem | None:
    """A budget whose `account` is not the vendor account that its connection's settings name, when the connector
    can tell (`account_of`): budgets of one account share its windows, so a wrong one shares another's."""
    config = project.connections[connection]
    budget = project.budgets.get(config.budget) if config.budget else None
    account_of = getattr(obj, "account_of", None)
    if budget is None or budget.account is None or not callable(account_of):
        return None
    expected = account_of(config.options)
    if expected is None or expected == budget.account:
        return None
    return Problem(
        "error",
        "budget_account_mismatch",
        f"flow {flow!r} uses {connection!r}, whose settings name the account {expected!r}, but its budget "
        f"{config.budget!r} names {budget.account!r}",
        path,
        f"make account: {expected!r} in budgets.{config.budget}, or leave it out for a budget of the tenant's own",
    )


# The code.


def _code_sha(root: Path) -> str:
    """The code's identity: `FEATHER_CODE_SHA` if set, else the git commit of the project, else `unknown`."""
    from_env = os.environ.get("FEATHER_CODE_SHA")
    if from_env:
        return from_env
    try:
        done = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, timeout=10, check=True
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return done.stdout.strip() or "unknown"


# Checks across the whole project.


def validate_project(project: Project) -> list[Problem]:
    """Return every mistake in a loaded project that shows without running it: each flow's own problems
    (`Flow.validate`, with the flow's name in front of the path), a flow that uses a connection the project
    doesn't define, a connection whose connector is not the one the code uses, a budget whose vendor account is not
    its connection's, and a budget no connection uses.

    Nothing is run, no connection is opened and no secret is resolved.
    """
    problems: list[Problem] = []
    for flow in project.flows:
        problems += [replace(p, path=f"{flow.name}.{p.path}") for p in flow.validate()]
        uses: list[tuple[str, Any]] = [(f"reads[{i}]", d.source) for i, d in enumerate(flow.datasets)]
        uses += [(f"deliver[{i}]", r.sink) for i, r in enumerate(flow.routes)]
        for where, obj in uses:
            connection, connector = obj.connection, obj.spec.name
            path = f"{flow.name}.{where}"
            config = project.connections.get(connection)
            if config is None:
                problems.append(
                    Problem(
                        "error",
                        "connection_undefined",
                        f"flow {flow.name!r} uses the connection {connection!r}, which {project.env!r} does not define",
                        path,
                        f"add {connection!r} under connections: in feather.yaml or env/{project.env}.yaml",
                    )
                )
            elif (
                config.connector != connector
                and not connector.startswith(
                    f"{config.connector}."
                )  # `zakya` serves zakya.source, zakya.items...
                and not (config.connector in SIMULATED and getattr(obj, "stands_in", False))
            ):
                problems.append(
                    Problem(
                        "error",
                        "connector_mismatch",
                        f"flow {flow.name!r} uses {connection!r} as {connector}, but the project declares "
                        f"it as {config.connector}",
                        path,
                        f"make connector: {connector} in the connection's entry, or use the other connector's class",
                    )
                )
            elif (account := _account_problem(project, flow.name, connection, obj, path)) is not None:
                problems.append(account)
    metered = {c.budget for c in project.connections.values()}
    for name in sorted(project.budgets.keys() - metered):
        problems.append(
            Problem(
                "warning",
                "budget_unused",
                f"the budget {name!r} is not used by any connection",
                f"budgets.{name}",
                f"set budget: {name} on the connection it should meter, or remove it",
            )
        )
    return problems


# Installed packages.


def prefixed(flows: Sequence[Flow], package: str) -> tuple[Flow, ...]:
    """The flows with every flow, dataset and route named `<package>/<name>`, as an installed package's are (§8).
    Connections keep their names: an environment's are shared on purpose."""
    found: list[Flow] = []
    for flow in flows:
        datasets = {d.name: replace(d, name=f"{package}/{d.name}") for d in flow.datasets}
        routes = [replace(r, name=f"{package}/{r.name}") for r in flow.routes]
        changed = copy.copy(flow)
        changed.name = f"{package}/{flow.name}"
        changed.datasets = list(datasets.values())
        changed.routes = routes
        changed.triggers = tuple(_retargeted(t, datasets) for t in flow.triggers)
        found.append(changed)
    return tuple(found)


_PLURAL = {"connection": "connections", "budget": "budgets", "setting": "settings"}


def check_environment(environment: Mapping[str, Any]) -> None:
    """Check the environment's own configuration as `load_project` reads it, so that an entry accepted into the
    database can't stop a server or worker from starting. Raises `ProjectError`. A connection's budget must be one of
    the environment's own."""
    config: dict[str, Any] = {}
    layers: dict[str, str] = {}
    _merge(config, layers, DEFAULTS, _DEFAULTS_LAYER, "")
    _merge(config, layers, _strkeys(environment), _ENVIRONMENT_LAYER, "")
    _check_secrets(config, layers)
    budgets = _budgets(config, layers)
    _connections(config, layers, budgets, Path("."))
    timezone = _mapping(config, "settings", layers).get("timezone")
    try:
        ZoneInfo(str(timezone))
    except (ValueError, ZoneInfoNotFoundError):
        raise ProjectError(
            f"{_where(layers, 'settings.timezone')}: {timezone!r} is not a time zone"
        ) from None


def _entry_value(kind: str, value: Any) -> Any:
    """An entry's value as configuration: a connection's own bookkeeping (keys starting `_`) left out."""
    if kind == "connection" and isinstance(value, dict):
        return {
            k: copy.deepcopy(v) for k, v in cast(dict[str, Any], value).items() if not str(k).startswith("_")
        }
    return copy.deepcopy(value)


def environment_layer(entries: Iterable[Any]) -> dict[str, Any]:
    """The configuration layer of an environment's own entries (`EnvEntry` rows): its connections, budgets and
    settings, by name. An entry that doesn't pass `check_environment` is left out with a warning, so that one bad row
    never stops a server or worker from starting."""
    rows = list(entries)
    layer: dict[str, Any] = {}
    for kind in ("budget", "setting", "connection"):  # a connection is checked against the budgets kept
        for entry in (e for e in rows if e.kind == kind):
            value = _entry_value(kind, entry.value)
            candidate = {k: dict(v) for k, v in layer.items()}
            candidate.setdefault(_PLURAL[kind], {})[entry.name] = value
            try:
                check_environment(
                    {"budgets": candidate.get("budgets", {}), _PLURAL[kind]: {entry.name: value}}
                )
            except ProjectError as error:
                log.warning("the environment's %s %s is left out: %s", kind, entry.name, error)
                continue
            layer = candidate
    return layer
