"""The manifest of a flow package, `feather-package.json` (flow packages design §4.2), and its checks.

The JSON Schema `manifest.schema.json` is the contract: a wheel whose manifest doesn't match it is not a flow package.
"""

import json
import re
from collections.abc import Mapping
from functools import cache
from importlib import metadata, resources
from typing import Any, cast

from jsonschema import Draft202012Validator
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from feather_connect.project import Project

MANIFEST = "feather-package.json"
ENTRY_POINTS = "feather_connect.packages"  # the group a package's wheel registers its manifest under
SCHEMA_VERSION = 1
_NAME = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")
MAX_NAME = 64


class ManifestError(ValueError):
    """The manifest, or the project it is made from, is not a valid flow package. The message says what to fix."""


def fc_version() -> str:
    """The installed Feather Connect's version."""
    return metadata.version("feather-connect")


def default_fc_versions(version: str | None = None) -> str:
    """The Feather Connect versions a package works with when its feather.yaml doesn't say: from this release's minor
    version up to the next major one, such as `>=0.1,<1` for 0.1.3."""
    found = Version(version or fc_version())
    return f">={found.major}.{found.minor},<{found.major + 1}"


def check_name(name: object) -> str:
    """A package name: lower case letters and digits in words joined by `-`, such as `acme-invoices`. It prefixes
    every flow, dataset and route the package defines, so it is kept plain."""
    if not isinstance(name, str) or len(name) > MAX_NAME or not _NAME.match(name):
        raise ManifestError(
            f"{name!r} is not a package name: use lower case letters and digits in words joined by '-', "
            f"such as acme-invoices, at most {MAX_NAME} characters"
        )
    return name


def check_version(version: object) -> str:
    """A PEP 440 version in its normal form, such as `1.4.0`, with no local part such as `+build`."""
    try:
        parsed = Version(str(version)) if isinstance(version, str | int | float) else None
    except InvalidVersion:
        parsed = None
    if parsed is None or str(parsed) != str(version):
        raise ManifestError(
            f"{version!r} is not a version: write one such as 1.4.0 (PEP 440, in its normal form)"
        )
    if parsed.local is not None:  # 1.0.0+build sorts as 1.0.0 and would stand in for it
        raise ManifestError(
            f"{version!r} has a local part (+{parsed.local}): a package version can't have one"
        )
    return str(parsed)


def check_fc_versions(spec: object, current: str | None = None) -> str:
    """A PEP 440 specifier that the running Feather Connect satisfies."""
    try:
        found = SpecifierSet(str(spec)) if isinstance(spec, str) and spec.strip() else None
    except InvalidSpecifier:
        found = None
    if found is None:
        raise ManifestError(f"fc_versions {spec!r} is not a version range: write one such as >=0.1,<1")
    running = current or fc_version()
    if not found.contains(running, prereleases=True):
        raise ManifestError(f"fc_versions {spec} doesn't include this Feather Connect, {running}")
    return str(spec)


def module_of(name: str) -> str:
    """The import name a package's project sits under in its wheel: `acme_invoices` for `acme-invoices`."""
    return name.replace("-", "_")


def describe(project: Project) -> dict[str, Any]:
    """What the manifest says of the project's flows, datasets, routes and connections, in a stable order."""
    flows: list[dict[str, Any]] = []
    datasets: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    for flow in sorted(project.flows, key=lambda f: f.name):
        flows.append(
            {
                "name": flow.name,
                "datasets": [d.name for d in flow.datasets],
                "routes": [r.name for r in flow.routes],
            }
        )
        for d in flow.datasets:
            datasets.append(
                {
                    "name": d.name,
                    "flow": flow.name,
                    "connection": d.source.connection,
                    "connector": d.source.spec.name,
                    "push": d.strategy.name == "push",
                }
            )
        for r in flow.routes:
            routes.append(
                {
                    "name": r.name,
                    "flow": flow.name,
                    "connection": r.sink.connection,
                    "connector": r.sink.spec.name,
                }
            )
    connections = [{"name": c.name, "connector": c.connector} for _, c in sorted(project.connections.items())]
    return {"flows": flows, "datasets": datasets, "routes": routes, "connections": connections}


@cache
def schema() -> Mapping[str, Any]:
    """The manifest's JSON Schema."""
    text = resources.files("feather_connect.packages").joinpath("manifest.schema.json").read_text("utf-8")
    return cast(dict[str, Any], json.loads(text))


def validate(manifest: object) -> dict[str, Any]:
    """Check a manifest against the schema, and its name, version and range by their own rules. Returns it, or raises
    `ManifestError` naming the first thing wrong."""
    validator = Draft202012Validator(schema())
    errors = sorted(validator.iter_errors(cast(Any, manifest)), key=lambda e: list(e.absolute_path))  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType, reportUnknownMemberType]
    if errors:
        first = errors[0]
        where = "/".join(str(p) for p in first.absolute_path) or "the manifest"  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportUnknownArgumentType]
        raise ManifestError(f"{MANIFEST}: {where}: {first.message}")
    found = cast(dict[str, Any], manifest)
    check_name(found["name"])
    check_version(found["version"])
    if found["module"] != module_of(found["name"]):
        raise ManifestError(f"{MANIFEST}: module must be {module_of(found['name'])!r} for {found['name']!r}")
    try:
        SpecifierSet(found["fc_versions"])
    except InvalidSpecifier:
        raise ManifestError(
            f"{MANIFEST}: fc_versions {found['fc_versions']!r} is not a version range"
        ) from None
    return found


def dumps(manifest: Mapping[str, Any]) -> bytes:
    """The manifest as the wheel holds it: sorted keys, two-space indent, a final newline, so a build is
    reproducible."""
    return (json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
