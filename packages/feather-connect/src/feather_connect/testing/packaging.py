"""Test helpers for flow packages with dependencies: tiny library wheels, a lock that pins them by hash, and a package
built with that lock, so that environments can be built with no index (`--no-index --find-links`)."""

import base64
import hashlib
import shutil
import zipfile
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

from feather_connect.packages import build

__all__ = ["built_with_lock", "library_wheel", "lock_for"]


def _record_line(name: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{name},sha256={digest},{len(data)}"


def library_wheel(out_dir: Path, name: str, version: str, files: Mapping[str, str]) -> Path:
    """A pure-Python wheel of `name` at `version`, holding `files` (path in the wheel to its text)."""
    module = name.replace("-", "_")
    info = f"{module}-{version}.dist-info"
    entries = {
        **{path: text.encode() for path, text in files.items()},
        f"{info}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n".encode(),
        f"{info}/WHEEL": b"Wheel-Version: 1.0\nGenerator: feather-tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    record = [_record_line(path, data) for path, data in entries.items()] + [f"{info}/RECORD,,"]
    entries[f"{info}/RECORD"] = ("\n".join(record) + "\n").encode()
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{module}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for entry, data in entries.items():
            archive.writestr(zipfile.ZipInfo(entry, (1980, 1, 1, 0, 0, 0)), data)
    return path


def lock_for(wheels: Sequence[Path]) -> bytes:
    """A lock, as `uv export` writes one, that pins each wheel by name, version and sha256."""
    lines: list[str] = []
    for wheel in wheels:
        name, version = wheel.name.split("-")[:2]
        digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
        lines.append(f"{name.replace('_', '-')}=={version} \\\n    --hash=sha256:{digest}")
    return ("\n".join(lines) + "\n").encode()


@contextmanager
def _lock_given(lock: bytes) -> Generator[None]:
    real = build._lock  # pyright: ignore[reportPrivateUsage]
    build._lock = lambda root, dependencies: lock  # pyright: ignore[reportPrivateUsage]
    try:
        yield
    finally:
        build._lock = real  # pyright: ignore[reportPrivateUsage]


def built_with_lock(
    source: Path, out_dir: Path, *, name: str, version: str, dependencies: Sequence[str], lock: bytes
) -> build.Built:
    """Build a copy of the project in `source` as the package `name` at `version`, depending on `dependencies` with
    `lock` as its locked requirements."""
    root = out_dir / f"src-{name}-{version}"
    shutil.rmtree(root, ignore_errors=True)
    shutil.copytree(source, root, ignore=shutil.ignore_patterns("dist", "__pycache__"))
    config = (root / "feather.yaml").read_text(encoding="utf-8")
    lines = [line for line in config.splitlines() if not line.startswith(("name:", "version:"))]
    (root / "feather.yaml").write_text(
        f"name: {name}\nversion: {version}\n" + "\n".join(lines) + "\n", encoding="utf-8"
    )
    deps = ", ".join(f'"{d}"' for d in ["feather-connect", *dependencies])
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "{name}"\nversion = "{version}"\nrequires-python = ">=3.12"\n'
        f"dependencies = [{deps}]\n",
        encoding="utf-8",
    )
    with _lock_given(lock):
        return build.build_package(root, out_dir)
