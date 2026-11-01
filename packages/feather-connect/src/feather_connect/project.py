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
import os
import re
import subprocess
import sys
from collections.abc import Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import yaml

from feather_connect.core.connector import is_unsigned, signature_secret_of
from feather_connect.core.flow import Flow, Problem, RouteSpec
from feather_connect.core.triggers import OnEvent, Trigger, cron, every, manual
from feather_connect.storage.port import BudgetSpec, parse_limits

__all__ = [
    "ConfigValue",
    "ConnectionConfig",
    "Project",
    "ProjectError",
    "effective_config",
    "load_project",
    "resolve_secret",
    "validate_project",
]

# What the code itself decides when no file does.
DEFAULTS: dict[str, Any] = {"settings": {"timezone": "UTC"}}
_DEFAULTS_LAYER = "defaults"
_PROJECT_FILE = "feather.yaml"
_SECRET_PROVIDERS = frozenset({"env", "file", "vault", "aws-sm", "azure-kv", "gcp-sm"})
_LOCAL_PROVIDERS = frozenset({"env", "file"})
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

    def resolve(self) -> dict[str, Any]:
        """Return the settings with every credential resolved, as a connector's `Context.config` holds them."""
        return {**self.options, **{key: resolve_secret(ref) for key, ref in self.secret_refs.items()}}


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


@dataclass(frozen=True)
class ConfigValue:
    """One effective knob: its dotted path, its value (`***` for a secret) and the layer that set it."""

    path: str
    value: Any
    layer: str


def resolve_secret(ref: str) -> str:
    """Return the value that the reference `${secret:env:NAME}` or `${secret:file:PATH}` points to.

    The other providers arrive in M3. An error names the reference, never the value.
    """
    match = _SECRET.match(ref)
    if match is None:
        raise ProjectError("not a secret reference: write it as ${secret:<provider>:<path>}")
    provider, path = match.groups()
    _check_provider(provider)
    if provider == "env":
        value = os.environ.get(path)
        if value is None:
            raise ProjectError(f"secret {ref}: the environment variable {path!r} is not set")
        return value
    try:
        return Path(path).read_text(encoding="utf-8").rstrip("\r\n")
    except OSError as error:
        raise ProjectError(f"secret {ref}: can't read the file ({error.strerror})") from None


def _check_provider(provider: str) -> None:
    if provider not in _SECRET_PROVIDERS:
        known = ", ".join(sorted(_SECRET_PROVIDERS))
        raise ProjectError(f"unknown secret provider {provider!r}: use one of {known}")
    if provider not in _LOCAL_PROVIDERS:
        raise ProjectError(f"the secret provider {provider!r} arrives in M3: use env or file for now")


def load_project(root: Path, env: str) -> Project:
    """Load the project in `root` for the environment `env`. Raises `ProjectError` with the file and key at fault."""
    if not _ENV_NAME.match(env):
        raise ProjectError(f"{env!r} is not an environment name: use letters, digits, - and _")
    layer_files = [(_PROJECT_FILE, root / _PROJECT_FILE), (f"env/{env}.yaml", root / "env" / f"{env}.yaml")]
    config: dict[str, Any] = {}
    layers: dict[str, str] = {}
    _merge(config, layers, DEFAULTS, _DEFAULTS_LAYER, "")
    for label, path in layer_files:
        _merge(config, layers, _read_yaml(label, path), label, "")
    _check_secrets(config, layers)
    name = _text(config, "name", layers)
    budgets = _budgets(config, layers)
    connections = _connections(config, layers, budgets)
    settings: Mapping[str, Any] = config.get("settings", {})
    flows = _apply_overlay(_import_flows(root, config, layers), config, layers, str(settings["timezone"]))
    _check_push_sources(flows, connections, env)
    return Project(
        name, root, env, flows, connections, budgets, settings, _code_sha(root), copy.deepcopy(config), layers
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


def _check_secrets(config: Mapping[str, Any], layers: Mapping[str, str]) -> None:
    """Allow secret references under a connection's `auth` only, and only with a provider that exists. Require
    that everything under `auth` is a reference, so that no credential is committed as plain text."""
    for path, value in _leaves(config, ""):
        parts = path.split(".")
        under_auth = parts[0] == "connections" and len(parts) > 3 and parts[2] == "auth"
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
        try:
            reserve = {k: float(v) for k, v in _strkeys(body.get("reserve", {})).items()}
            found[name] = BudgetSpec(
                name, parse_limits([str(limit) for limit in cast(list[Any], limits)]), reserve
            )
        except (ValueError, TypeError, AttributeError):
            raise ProjectError(f"{_where(layers, path)}: limits and reserve are not valid") from None
    return found


def _connections(
    config: Mapping[str, Any], layers: Mapping[str, str], budgets: Mapping[str, BudgetSpec]
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
            name, connector, options, auth, None if site is None else str(site), budget
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
    doesn't define, a connection whose connector is not the one the code uses, and a budget no connection uses.

    Nothing is run, no connection is opened and no secret is resolved.
    """
    problems: list[Problem] = []
    for flow in project.flows:
        problems += [replace(p, path=f"{flow.name}.{p.path}") for p in flow.validate()]
        uses = [(f"reads[{i}]", d.source.connection, d.source.spec.name) for i, d in enumerate(flow.datasets)]
        uses += [(f"deliver[{i}]", r.sink.connection, r.sink.spec.name) for i, r in enumerate(flow.routes)]
        for where, connection, connector in uses:
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
            elif config.connector != connector:
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
