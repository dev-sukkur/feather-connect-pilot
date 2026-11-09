"""The install's load check (flow packages design §5, step 4): load a flow package in a throwaway child process and say
what it defines, so that the process that installs it never runs its code.

The parent unpacks the wheel into a temporary directory, with caps on its size and its entries and nothing written
outside it, and starts `python -I -m feather_connect.packages.check` there with a clean environment: none of the
server's own variables, so neither its database URL nor its secrets reach package code. The child finds the package by
its `feather_connect.packages` entry point, imports it, loads its project for the environment as `feather validate
--env` does, and writes what it found to a file: the flows, datasets and routes, and the problems. It is killed when it
runs past the timeout. The parent reads at most `MAX_RESULT` bytes back, and a few lines of the child's stderr.

What the code defines must be what the manifest says: a package whose code and manifest disagree fails the check.
"""

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, cast

from feather_connect.packages import manifest as m

__all__ = ["DEFAULT_TIMEOUT", "LoadCheck", "load_check", "unpack"]

DEFAULT_TIMEOUT = 60.0  # seconds for the child to import and load the package
MAX_UNPACKED = 200 * 1024 * 1024  # bytes a wheel may unzip to
MAX_ENTRIES = 10_000  # files a wheel may hold
MAX_RESULT = 1024 * 1024  # bytes of the child's answer that are read
_STDERR_TAIL = 2000  # characters of the child's stderr kept for a failure
# What the child gets of the environment: enough to start Python and find a temporary directory, nothing of the
# server's settings or secrets. A package's secrets are references, not resolved by the check.
_KEPT_VARIABLES = ("PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "LANG", "LC_ALL", "TZ")


@dataclass(frozen=True)
class LoadCheck:
    """What a load check found. `ok` when the package loaded, has no error-level problem and defines what its
    manifest says; otherwise `error` says why."""

    ok: bool
    flows: list[str] = field(default_factory=list[str])
    datasets: list[str] = field(default_factory=list[str])
    routes: list[str] = field(default_factory=list[str])
    problems: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    error: str | None = None
    seconds: float = 0.0

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


def unpack(content: bytes, into: Path) -> None:
    """Unzip a wheel into `into`: regular files only, every one inside it, at most `MAX_ENTRIES` of them and
    `MAX_UNPACKED` bytes in all, counted as they unzip. Raises `ValueError` for a wheel that breaks a rule."""
    total = 0
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        entries = archive.infolist()
        if len(entries) > MAX_ENTRIES:
            raise ValueError(f"the wheel holds {len(entries)} files, more than {MAX_ENTRIES}")
        for entry in entries:
            path = PurePosixPath(entry.filename)
            if path.is_absolute() or ".." in path.parts or ":" in entry.filename or "\\" in entry.filename:
                raise ValueError(f"the wheel's entry {entry.filename!r} would land outside it")
            if entry.is_dir():
                continue
            if (entry.external_attr >> 16) & 0o170000 not in (
                0,
                0o100000,
            ):  # a symlink or a device, not a file
                raise ValueError(f"the wheel's entry {entry.filename!r} is not a regular file")
            target = into.joinpath(*path.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(entry) as source, target.open("wb") as sink:
                while chunk := source.read(1024 * 1024):
                    total += len(chunk)
                    if total > MAX_UNPACKED:
                        raise ValueError(f"the wheel unzips to more than {MAX_UNPACKED} bytes")
                    sink.write(chunk)


def _child_environment(home: Path) -> dict[str, str]:
    kept = {k: v for k, v in os.environ.items() if k.upper() in _KEPT_VARIABLES}
    return {
        **kept,
        "HOME": str(home),
        "USERPROFILE": str(home),
        "TEMP": str(home),
        "TMP": str(home),
        "TMPDIR": str(home),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONIOENCODING": "utf-8",
    }


def _tail(path: Path) -> str:
    try:
        with path.open("rb") as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(max(0, size - _STDERR_TAIL * 4))
            text = file.read().decode("utf-8", "replace")
    except OSError:
        return ""
    return text[-_STDERR_TAIL:].strip()


def load_check(
    content: bytes,
    manifest: Mapping[str, Any],
    *,
    env: str,
    timeout: float = DEFAULT_TIMEOUT,
    python: str = sys.executable,
) -> LoadCheck:
    """Load the package in a child process for `env`, and check that it defines what `manifest` says."""
    started = time.monotonic()

    def failed(error: str, **found: Any) -> LoadCheck:
        return LoadCheck(False, error=error, seconds=round(time.monotonic() - started, 3), **found)

    work = Path(tempfile.mkdtemp(prefix="feather-load-check-"))
    try:
        site, home = work / "site", work / "home"
        site.mkdir()
        home.mkdir()
        try:
            unpack(content, site)
        except (ValueError, zipfile.BadZipFile) as error:
            return failed(str(error))
        result, stderr = work / "result.json", work / "stderr.txt"
        command = [
            python,
            "-I",
            "-m",
            "feather_connect.packages.check",
            str(site),
            manifest["name"],
            env,
            str(result),
        ]
        with stderr.open("wb") as errors:
            try:
                done = subprocess.run(
                    command,
                    cwd=home,
                    env=_child_environment(home),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=errors,
                    timeout=timeout,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                return failed(f"loading the package took more than {timeout:g} s")
        try:
            with result.open("rb") as file:
                raw = file.read(MAX_RESULT + 1)
        except OSError:
            raw = b""
        if not raw or len(raw) > MAX_RESULT:
            why = "gave no answer" if not raw else f"answered more than {MAX_RESULT} bytes"
            tail = _tail(stderr)
            return failed(f"the check {why} (exit {done.returncode})" + (f": {tail}" if tail else ""))
        try:
            found = cast(dict[str, Any], json.loads(raw))
        except ValueError:
            return failed("the check's answer is not JSON")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return _judged(found, manifest, round(time.monotonic() - started, 3))


def _names(value: object) -> list[str]:
    return [str(x) for x in cast(list[object], value)] if isinstance(value, list) else []


def _judged(found: Mapping[str, Any], manifest: Mapping[str, Any], seconds: float) -> LoadCheck:
    """The child's answer, judged: an error, an error-level problem, or code that disagrees with the manifest fails."""
    flows, datasets, routes = (_names(found.get(k)) for k in ("flows", "datasets", "routes"))
    named = {"flows": flows, "datasets": datasets, "routes": routes}
    listed = cast(list[object], found.get("problems")) if isinstance(found.get("problems"), list) else []
    problems = [cast(dict[str, Any], p) for p in listed if isinstance(p, dict)]

    def result(ok: bool, error: str | None = None) -> LoadCheck:
        return LoadCheck(ok, flows, datasets, routes, problems, error, seconds)

    if found.get("error"):
        return result(False, str(found["error"]))
    errors = [p for p in problems if p.get("level") == "error"]
    if errors:
        more = f" ({len(errors) - 1} more)" if len(errors) > 1 else ""
        return result(False, f"{errors[0].get('code')}: {errors[0].get('message')}{more}")
    for kind, defined in named.items():
        items = cast(list[object], manifest.get(kind)) if isinstance(manifest.get(kind), list) else []
        declared = sorted(str(cast(dict[str, Any], i).get("name")) for i in items if isinstance(i, dict))
        if sorted(defined) != declared:
            return result(
                False, f"the code defines the {kind} {sorted(defined)}, but the manifest says {declared}"
            )
    return result(True)


# The child.


def _find(site: Path, name: str) -> Any:
    from importlib import metadata

    for dist in metadata.distributions(path=[str(site)]):
        for point in dist.entry_points:
            if point.group == m.ENTRY_POINTS and point.name == name:
                return point
    raise LookupError(f"the wheel registers no {m.ENTRY_POINTS} entry point named {name!r}")


def _check(site: Path, name: str, env: str) -> dict[str, Any]:
    from feather_connect.project import load_project, validate_project

    sys.path.insert(0, str(site))
    module = _find(site, name).load()
    where = getattr(module, "__file__", None)
    if not where:
        raise LookupError(f"the entry point of {name} is not a module with a file")
    project = load_project(Path(where).parent, env)
    described = m.describe(project)
    return {
        "flows": [f["name"] for f in described["flows"]],
        "datasets": [d["name"] for d in described["datasets"]],
        "routes": [r["name"] for r in described["routes"]],
        "problems": [
            {"level": p.severity, "code": p.code, "message": p.message, "path": p.path, "hint": p.hint}
            for p in validate_project(project)
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Run in the child: `python -I -m feather_connect.packages.check SITE NAME ENV RESULT`."""
    site, name, env, result = (argv if argv is not None else sys.argv[1:])[:4]
    try:
        answer = _check(Path(site), name, env)
    except BaseException as error:  # noqa: BLE001 - whatever the package does wrong, SystemExit too, is the answer
        last = traceback.format_exception_only(type(error), error)[-1].strip()
        answer = {"error": f"the package doesn't load: {last}"}
    Path(result).write_text(json.dumps(answer, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
