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
