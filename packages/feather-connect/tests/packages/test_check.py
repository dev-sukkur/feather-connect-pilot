"""The install's load check: the package loaded in a child process, never in this one, and what it defines compared
with its manifest (flow packages §5, step 4)."""

import io
import os
import shutil
import sys
import zipfile
from pathlib import Path

import pytest

from feather_connect.packages.build import build_package
from feather_connect.packages.check import load_check, unpack

SAMPLE = Path(__file__).resolve().parents[4] / "examples" / "packages" / "acme-invoices"


@pytest.fixture
def sample(tmp_path: Path) -> Path:
    root = tmp_path / "acme-invoices"
    shutil.copytree(SAMPLE, root, ignore=shutil.ignore_patterns("dist", "__pycache__"))
    return root


def _built(root: Path) -> tuple[bytes, dict[str, object]]:
    built = build_package(root)
    return built.path.read_bytes(), dict(built.manifest)


def test_a_sound_package_loads_and_defines_what_its_manifest_says(sample: Path):
    content, manifest = _built(sample)
    before = set(sys.modules)
    found = load_check(content, manifest, env="dev")
    assert found.ok, found.error
    assert found.flows and found.datasets and found.routes
    assert sorted(found.flows) == sorted(f["name"] for f in manifest["flows"])  # type: ignore[index]
    assert found.seconds > 0
    assert not {
        name for name in set(sys.modules) - before if name.startswith("acme_invoices")
    }  # not loaded here


def test_a_package_whose_code_fails_to_import_says_why(sample: Path):
    flows = sample / "flows" / "invoices.py"
    flows.write_text(
        flows.read_text(encoding="utf-8") + "\nraise RuntimeError('no billing today')\n", encoding="utf-8"
    )
    broken, manifest = _built_without_check(sample)
    found = load_check(broken, manifest, env="dev")
    assert not found.ok and "no billing today" in (found.error or "")


def test_a_package_that_hangs_is_stopped_at_the_timeout(sample: Path):
    flows = sample / "flows" / "invoices.py"
    flows.write_text("import time\ntime.sleep(60)\n" + flows.read_text(encoding="utf-8"), encoding="utf-8")
    content, manifest = _built_without_check(sample)
    found = load_check(content, manifest, env="dev", timeout=3)
    assert not found.ok and "more than 3 s" in (found.error or "")


def _built_without_check(root: Path) -> tuple[bytes, dict[str, object]]:
    """Build a sound copy, then put the changed flows into its wheel: the build itself loads the project."""
    clean = root.parent / "clean"
    shutil.copytree(SAMPLE, clean, ignore=shutil.ignore_patterns("dist", "__pycache__"))
    content, manifest = _built(clean)
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(content)) as source, zipfile.ZipFile(out, "w") as target:
        for name in source.namelist():
            data = source.read(name)
            if name == "acme_invoices/flows/invoices.py":
                data = (root / "flows" / "invoices.py").read_bytes()
            target.writestr(name, data)
    return out.getvalue(), manifest


def test_a_manifest_that_disagrees_with_the_code_fails(sample: Path):
    content, manifest = _built(sample)
    lying = {
        **manifest,
        "routes": [{"name": "elsewhere", "flow": "x", "connection": "erp", "connector": "y"}],
    }
    found = load_check(content, lying, env="dev")
    assert not found.ok and "the manifest says ['elsewhere']" in (found.error or "")


def test_an_environment_the_package_has_no_file_for_fails(sample: Path):
    content, manifest = _built(sample)
    found = load_check(content, manifest, env="staging")
    assert not found.ok and "env/staging.yaml" in (found.error or "")


def test_the_child_gets_none_of_this_process_secrets(sample: Path, monkeypatch: pytest.MonkeyPatch):
    flows = sample / "flows" / "invoices.py"
    leak = "import os\nif os.environ.get('FEATHER_SECRET_KEY'):\n    raise RuntimeError('saw the key')\n"
    flows.write_text(leak + flows.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setenv("FEATHER_SECRET_KEY", "server-secret-0123456789")
    content, manifest = _built_without_check(sample)
    found = load_check(content, manifest, env="dev")
    assert found.ok, found.error
    assert os.environ["FEATHER_SECRET_KEY"]  # still ours


@pytest.mark.parametrize(
    ("name", "why"),
    [("../escape.py", "outside"), ("/abs.py", "outside"), ("C:/win.py", "outside")],
)
def test_a_wheel_entry_outside_the_directory_is_refused(tmp_path: Path, name: str, why: str):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr(name, b"x")
    with pytest.raises(ValueError, match=why):
        unpack(out.getvalue(), tmp_path)
    assert not (tmp_path.parent / "escape.py").exists()


def test_a_wheel_that_unzips_too_big_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from feather_connect.packages import check

    monkeypatch.setattr(check, "MAX_UNPACKED", 1000)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("big.bin", b"\0" * 5000)
    with pytest.raises(ValueError, match="more than 1000 bytes"):
        unpack(out.getvalue(), tmp_path)


def _prefixed(content: bytes, code: str, target: str = "acme_invoices/flows/invoices.py") -> bytes:
    """The wheel with `code` run first in `target`."""
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(content)) as source, zipfile.ZipFile(out, "w") as archive:
        for name in source.namelist():
            data = source.read(name)
            archive.writestr(name, code.encode() + data if name == target else data)
    return out.getvalue()


@pytest.mark.parametrize("hangs", [True, False], ids=["timeout", "loads"])
def test_processes_the_package_starts_die_with_the_check(sample: Path, tmp_path: Path, hangs: bool):
    import time

    content, manifest = _built(sample)
    marker = tmp_path / "alive.txt"
    beat = f"import pathlib, time\nfor i in range(100):\n    pathlib.Path({str(marker)!r}).write_text(str(i)); time.sleep(0.2)\n"
    code = f"import subprocess, sys\nsubprocess.Popen([sys.executable, '-c', {beat!r}])\nimport time\ntime.sleep(1)\n"
    found = load_check(
        _prefixed(content, code + ("time.sleep(60)\n" if hangs else "")), manifest, env="dev", timeout=4
    )
    assert found.ok is not hangs, found.error
    time.sleep(0.5)
    before = marker.read_text() if marker.exists() else None
    time.sleep(1.5)
    assert before is not None and marker.read_text() == before  # it beat, then it was killed with the tree


def test_a_package_that_floods_stderr_is_kept_to_its_last_bytes(sample: Path):
    from feather_connect.packages import check

    content, manifest = _built(sample)
    code = "import sys\nfor _ in range(64):\n    sys.stderr.write('x' * 1_000_000)\nsys.stderr.write('THE END')\nsys.stderr.flush()\nimport os\nos._exit(3)\n"
    found = load_check(_prefixed(content, code), manifest, env="dev")
    assert not found.ok and (found.error or "").endswith("THE END") and "exit 3" in (found.error or "")
    assert len(found.error or "") < check.MAX_STDERR


def test_an_answer_the_package_writes_itself_is_refused(sample: Path):
    content, manifest = _built(sample)
    names = {k: [i["name"] for i in manifest[k]] for k in ("flows", "datasets", "routes")}  # type: ignore[union-attr]
    spoof = (
        "import json, os, pathlib\n"
        "for p in pathlib.Path.cwd().parent.iterdir():\n"
        f"    pass\npathlib.Path.cwd().parent.joinpath('result.json').write_text(json.dumps({names!r}))\nos._exit(0)\n"
    )
    found = load_check(_prefixed(content, spoof, "acme_invoices/__init__.py"), manifest, env="staging")
    assert not found.ok and "not the bootstrap's" in (found.error or "")


def test_a_package_that_imports_what_the_server_lacks_says_dependencies_come_later(sample: Path):
    content, manifest = _built(sample)
    found = load_check(_prefixed(content, "import acme_billing_sdk\n"), manifest, env="dev")
    assert not found.ok
    assert "imports acme_billing_sdk" in (found.error or "") and "task 8" in (found.error or "")


def test_a_push_source_of_the_packages_own_is_named(sample: Path):
    content, manifest = _built(sample)
    own = (
        "from feather_connect.connectors import simulation as _sim\n"
        "class Hooks(_sim.Source):\n    unsigned = True\n"
    )
    out = io.BytesIO()
    target = "acme_invoices/flows/invoices.py"
    with zipfile.ZipFile(io.BytesIO(content)) as source, zipfile.ZipFile(out, "w") as archive:
        for name in source.namelist():
            data = source.read(name)
            if name == target:
                text = data.decode().replace('simulation.Source("billing"', 'Hooks("billing"')
                text = text.replace("fc.strategy.replace()", "fc.strategy.push()")
                text = text.replace("fixture=FIXTURE)", "fixture=FIXTURE, unsigned=True)")
                data = (own + text).encode()
            archive.writestr(name, data)
    found = load_check(out.getvalue(), manifest, env="dev")
    assert found.own_push_sources == ["invoices (Hooks)"], found.error
