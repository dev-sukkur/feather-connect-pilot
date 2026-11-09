"""The install's load check (flow packages design §5, step 4): load a flow package in a throwaway child process and say
what it defines, so that the process that installs it never runs its code.

The parent unpacks the wheel into a temporary directory, with caps on its size and its entries and nothing written
outside it, and starts `python -I -m feather_connect.packages.check` there with a clean environment: none of the
server's own variables, so neither its database URL nor its secrets reach package code. Before it imports anything of
the package, the child's bootstrap reads a nonce and the result path from its stdin, which it then closes. It finds the
package by its `feather_connect.packages` entry point, imports it, loads its project for the environment as `feather
validate --env` does, and writes what it found, with the nonce, to the result file: the flows, datasets and routes, the
problems, and the push datasets whose source class is the package's own. The child runs in its own process group (a
Job Object on Windows), and the whole tree is killed when the check ends, however it ends, or runs past the timeout.
The parent keeps the last few KiB of the child's stderr as it comes and reads at most `MAX_RESULT` bytes of the answer.

What the code defines must be what the manifest says: a package whose code and manifest disagree fails the check.

**A correctness gate, not a security boundary.** Until task 6 moves the check to the package hosts, the child runs as
the server's user, on the server's machine, with the server's Python and packages:

- Package code can do what that user can: read its files, reach the network, start a process that leaves the process
  group. On Linux the server sets itself not dumpable, so that a child can't read `/proc/<server>/environ`.
- The nonce keeps a package that exits early from answering for the bootstrap by mistake or by a naive write; code
  that sets out to find it in the bootstrap's memory can.
- A package's own dependencies are not installed: environments come with task 8. Until then a package may import only
  what the server has, and one that imports anything else fails the check, which says so.
"""

import io
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import IO, Any, cast

from feather_connect.packages import manifest as m

__all__ = ["DEFAULT_TIMEOUT", "LoadCheck", "load_check", "protect_server_process", "unpack"]

DEFAULT_TIMEOUT = 60.0  # seconds for the child to import and load the package
MAX_UNPACKED = 200 * 1024 * 1024  # bytes a wheel may unzip to
MAX_ENTRIES = 10_000  # files a wheel may hold
MAX_RESULT = 1024 * 1024  # bytes of the child's answer that are read
MAX_STDERR = 8 * 1024  # bytes of the child's stderr kept, the last ones, however much it writes
_STDERR_TAIL = 2000  # characters of that stderr put in a failure
# What the child gets of the environment: enough to start Python and find a temporary directory, nothing of the
# server's settings or secrets. A package's secrets are references, not resolved by the check.
_KEPT_VARIABLES = ("PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "LANG", "LC_ALL", "TZ")
_NO_DEPENDENCIES = (
    "a package's own dependencies are not installed yet (they come with its environment, task 8): "
    "until then it may import only what the server has"
)


@dataclass(frozen=True)
class LoadCheck:
    """What a load check found. `ok` when the package loaded, has no error-level problem and defines what its
    manifest says; otherwise `error` says why. `own_push_sources` names the push datasets whose source class is the
    package's own code, which an install refuses (§12)."""

    ok: bool
    flows: list[str] = field(default_factory=list[str])
    datasets: list[str] = field(default_factory=list[str])
    routes: list[str] = field(default_factory=list[str])
    problems: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    error: str | None = None
    seconds: float = 0.0
    own_push_sources: list[str] = field(default_factory=list[str])

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


def protect_server_process() -> None:
    """On Linux, make this process not dumpable, so that a process of the same user, such as a load check's child,
    can't read its `/proc/<pid>/environ` or attach to it. Elsewhere it does nothing."""
    if sys.platform != "linux":
        return
    import ctypes

    pr_set_dumpable = 4
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl(pr_set_dumpable, 0, 0, 0, 0)


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


class _Tail:
    """The last `MAX_STDERR` bytes of a stream, read on a thread as they come, so that the child never blocks on a
    full pipe and never fills memory or disk."""

    def __init__(self, stream: IO[bytes]) -> None:
        self._stream = stream
        self._kept = b""
        self._thread = threading.Thread(target=self._read, name="load-check-stderr", daemon=True)
        self._thread.start()

    def _read(self) -> None:
        try:
            while chunk := self._stream.read(4096):
                self._kept = (self._kept + chunk)[-MAX_STDERR:]
        except (OSError, ValueError):
            pass

    def text(self) -> str:
        self._thread.join(timeout=5)
        return self._kept.decode("utf-8", "replace")[-_STDERR_TAIL:].strip()


class _Tree:
    """The child and every process it starts: a process group on POSIX, a Job Object on Windows that kills them all
    when it is closed."""

    def __init__(self, command: list[str], **options: Any) -> None:
        if sys.platform == "win32":
            self.process = subprocess.Popen(
                command, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP, **options
            )
            self._job = _job_for(self.process)
        else:
            self.process = subprocess.Popen(command, start_new_session=True, **options)

    def kill(self) -> None:
        """Kill the whole tree and reap the child. Safe to call more than once."""
        if sys.platform == "win32":
            if self._job is not None:
                _close_job(self._job)
                self._job = None
            if self.process.poll() is None:
                self.process.kill()
        else:
            import signal

            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - a process SIGKILL doesn't end
            pass


if sys.platform == "win32":  # defined only where they run, so a type checker for another platform skips them

    def _job_for(process: "subprocess.Popen[Any]") -> int | None:
        """A Job Object holding `process`, set to kill every process in it when its handle is closed."""
        import ctypes
        from ctypes import wintypes

        class Basic(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class Extended(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", Basic),
                ("IoInfo", ctypes.c_uint64 * 6),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            process.kill()
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
        limits = Extended()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        extended_limit_information = 9
        set_ok = kernel32.SetInformationJobObject(
            job, extended_limit_information, ctypes.byref(limits), ctypes.sizeof(limits)
        )
        handle = int(cast(Any, process)._handle)  # pyright: ignore[reportPrivateUsage]
        if not set_ok or not kernel32.AssignProcessToJobObject(job, handle):
            error = ctypes.get_last_error()
            _close_job(job)
            process.kill()
            raise OSError(error, "the load check's process could not be put in a Job Object")
        return int(job)

    def _close_job(job: int) -> None:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.TerminateJobObject(job, 1)
        kernel32.CloseHandle(job)


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
        result, nonce = work / "result.json", secrets.token_hex(16)
        command = [
            python,
            "-I",
            "-m",
            "feather_connect.packages.check",
            str(site),
            str(manifest["name"]),
            env,
        ]
        tree = _Tree(
            command,
            cwd=home,
            env=_child_environment(home),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            assert tree.process.stdin is not None and tree.process.stderr is not None
            stderr = _Tail(tree.process.stderr)
            try:
                tree.process.stdin.write(f"{nonce}\n{result}\n".encode())
                tree.process.stdin.close()
            except OSError:
                pass  # it ended already: its answer, or the lack of one, says why
            try:
                code = tree.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                return failed(f"loading the package took more than {timeout:g} s")
        finally:
            tree.kill()
        answer = _answer(result, nonce)
        if isinstance(answer, str):
            tail = stderr.text()
            return failed(f"the check {answer} (exit {code})" + (f": {tail}" if tail else ""))
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return _judged(answer, manifest, round(time.monotonic() - started, 3))


def _answer(result: Path, nonce: str) -> dict[str, Any] | str:
    """The bootstrap's answer, or what is wrong with it."""
    try:
        if result.stat().st_size > MAX_RESULT:
            return f"answered more than {MAX_RESULT} bytes"
        with result.open("rb") as file:
            raw = file.read(MAX_RESULT + 1)
    except OSError:
        raw = b""
    if not raw:
        return "gave no answer"
    if len(raw) > MAX_RESULT:
        return f"answered more than {MAX_RESULT} bytes"
    try:
        found = json.loads(raw)
    except ValueError:
        return "answered something that is not JSON"
    if not isinstance(found, dict) or cast(dict[str, Any], found).get("nonce") != nonce:
        return "answer is not the bootstrap's: the package wrote it, or ended before it loaded"
    return cast(dict[str, Any], found)


def _names(value: object) -> list[str]:
    return [str(x) for x in cast(list[object], value)] if isinstance(value, list) else []


def _judged(found: Mapping[str, Any], manifest: Mapping[str, Any], seconds: float) -> LoadCheck:
    """The child's answer, judged: an error, an error-level problem, or code that disagrees with the manifest fails."""
    flows, datasets, routes = (_names(found.get(k)) for k in ("flows", "datasets", "routes"))
    own = _names(found.get("own_push_sources"))
    named = {"flows": flows, "datasets": datasets, "routes": routes}
    listed = cast(list[object], found.get("problems")) if isinstance(found.get("problems"), list) else []
    problems = [cast(dict[str, Any], p) for p in listed if isinstance(p, dict)]

    def result(ok: bool, error: str | None = None) -> LoadCheck:
        return LoadCheck(ok, flows, datasets, routes, problems, error, seconds, own)

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


def _is_under(module: str, site: Path) -> bool:
    where = getattr(sys.modules.get(module), "__file__", None)
    if not where:
        return False
    try:
        Path(where).resolve().relative_to(site.resolve())
    except ValueError:
        return False
    return True


def _check(site: Path, name: str, env: str) -> dict[str, Any]:
    from feather_connect.project import load_project, validate_project

    sys.path.insert(0, str(site))
    module = _find(site, name).load()
    where = getattr(module, "__file__", None)
    if not where:
        raise LookupError(f"the entry point of {name} is not a module with a file")
    project = load_project(Path(where).parent, env)
    described = m.describe(project)
    own = sorted(
        f"{d.name} ({type(d.source).__qualname__})"
        for flow in project.flows
        for d in flow.datasets
        if d.strategy.name == "push" and any(_is_under(c.__module__, site) for c in type(d.source).__mro__)
    )
    return {
        "flows": [f["name"] for f in described["flows"]],
        "datasets": [d["name"] for d in described["datasets"]],
        "routes": [r["name"] for r in described["routes"]],
        "own_push_sources": own,
        "problems": [
            {"level": p.severity, "code": p.code, "message": p.message, "path": p.path, "hint": p.hint}
            for p in validate_project(project)
        ],
    }


def _why(error: BaseException, name: str) -> str:
    last = traceback.format_exception_only(type(error), error)[-1].strip()
    cause: BaseException | None = error
    while cause is not None and not isinstance(
        cause, ModuleNotFoundError
    ):  # load_project wraps what it can't import
        cause = cause.__cause__ or cause.__context__
    missing = cause.name if isinstance(cause, ModuleNotFoundError) else None
    if missing and missing.split(".")[0] != m.module_of(name):
        return (
            f"the package doesn't load: it imports {missing}, which isn't installed here: {_NO_DEPENDENCIES}"
        )
    return f"the package doesn't load: {last}"


def _writer(result: Path, nonce: str) -> Callable[[dict[str, Any]], None]:
    def write(answer: dict[str, Any]) -> None:
        result.write_text(json.dumps({**answer, "nonce": nonce}, default=str), encoding="utf-8")

    return write


def main(argv: Sequence[str] | None = None) -> int:
    """Run in the child: `python -I -m feather_connect.packages.check SITE NAME ENV`, with the nonce and the result
    path on stdin, read before any package code runs."""
    site, name, env = (argv if argv is not None else sys.argv[1:])[:3]
    nonce = sys.stdin.readline().strip()
    write = _writer(Path(sys.stdin.readline().strip()), nonce)
    del nonce
    sys.stdin.close()
    del sys.argv[1:]
    try:
        answer = _check(Path(site), name, env)
    except BaseException as error:  # noqa: BLE001 - whatever the package does wrong, SystemExit too, is the answer
        answer = {"error": _why(error, name)}
    write(answer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
