"""`build_package`: a project made into a flow package's wheel, reproducibly, and read back (flow packages §4, task 1)."""

import base64
import hashlib
import io
import json
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from feather_connect.packages import manifest as m
from feather_connect.packages.build import LOCK_FILE, build_package, wheel_name
from feather_connect.packages.manifest import ManifestError
from feather_connect.packages.wheel import MAX_ENTRY, MAX_WHEEL, read_wheel

SAMPLE = Path(__file__).resolve().parents[4] / "examples" / "packages" / "acme-invoices"
FIXTURE_LOCK = Path(__file__).parent / "fixtures" / "acme-invoices.uv.lock"


@pytest.fixture
def sample(tmp_path: Path) -> Path:
    """A copy of the sample package's repository, to build and change."""
    root = tmp_path / "acme-invoices"
    shutil.copytree(SAMPLE, root, ignore=shutil.ignore_patterns("dist", "__pycache__"))
    return root


def _entries(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def test_building_twice_gives_the_same_sha256(sample: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "1794787200")
    first = build_package(sample, tmp_path / "one")
    (sample / "flows" / "invoices.py").touch()  # a newer file time changes nothing
    second = build_package(sample, tmp_path / "two")
    assert first.sha256 == second.sha256 == hashlib.sha256(second.path.read_bytes()).hexdigest()
    assert first.path.name == "acme_invoices-1.0.0-py3-none-any.whl" == wheel_name("acme-invoices", "1.0.0")
    assert first.manifest["built_at"] == "2026-11-16T00:00:00Z"
    with zipfile.ZipFile(first.path) as archive:
        assert {i.date_time for i in archive.infolist()} == {(1980, 1, 1, 0, 0, 0)}


def test_a_change_to_the_code_changes_the_sha256(sample: Path, tmp_path: Path):
    first = build_package(sample, tmp_path / "one")
    with (sample / "flows" / "invoices.py").open("a", encoding="utf-8") as file:
        file.write("# changed\n")
    assert build_package(sample, tmp_path / "two").sha256 != first.sha256


def test_the_wheel_holds_the_project_the_manifest_and_an_entry_point(sample: Path):
    built = build_package(sample)
    assert built.path.parent == sample / "dist"
    files = _entries(built.path)
    info = "acme_invoices-1.0.0.dist-info"
    assert sorted(n for n in files if n.startswith("acme_invoices/")) == [
        "acme_invoices/__init__.py",
        "acme_invoices/env/dev.yaml",
        "acme_invoices/env/prod.yaml",
        "acme_invoices/feather-package.json",
        "acme_invoices/feather.yaml",
        "acme_invoices/fixtures/invoices.json",
        "acme_invoices/flows/__init__.py",
        "acme_invoices/flows/invoices.py",
        "acme_invoices/tests/test_invoices.py",
    ]  # no secrets/, dist/ or pyproject.toml
    assert (
        files[f"{info}/entry_points.txt"].decode()
        == "[feather_connect.packages]\nacme-invoices = acme_invoices\n"
    )
    metadata = files[f"{info}/METADATA"].decode()
    assert "Name: acme-invoices\nVersion: 1.0.0\n" in metadata
    assert f"Requires-Dist: feather-connect{m.default_fc_versions()}\n" in metadata
    assert files[f"{info}/{LOCK_FILE}"] == b""
    for line in files[f"{info}/RECORD"].decode().splitlines()[:-1]:  # every file's hash is right
        path, digest, size = line.split(",")
        data = files[path]
        expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        assert digest == f"sha256={expected}" and int(size) == len(data)
    assert files[f"{info}/RECORD"].decode().splitlines()[-1] == f"{info}/RECORD,,"


def test_the_manifest_says_what_the_package_defines_and_matches_the_schema(sample: Path):
    built = build_package(sample)
    manifest = json.loads(_entries(built.path)["acme_invoices/feather-package.json"])
    Draft202012Validator.check_schema(m.schema())
    assert not list(Draft202012Validator(m.schema()).iter_errors(manifest))
    assert manifest == dict(built.manifest)
    assert (manifest["name"], manifest["version"], manifest["module"]) == (
        "acme-invoices",
        "1.0.0",
        "acme_invoices",
    )
    assert manifest["flows"] == [{"name": "invoices", "datasets": ["invoices"], "routes": ["erp"]}]
    assert manifest["datasets"] == [
        {
            "name": "invoices",
            "flow": "invoices",
            "connection": "billing",
            "connector": "simulation.source",
            "push": False,
        }
    ]
    assert manifest["routes"] == [
        {"name": "erp", "flow": "invoices", "connection": "erp", "connector": "simulation.sink"}
    ]
    assert manifest["connections"] == [
        {"name": "billing", "connector": "simulation.source"},
        {"name": "erp", "connector": "simulation.sink"},
    ]
    assert manifest["lock_hash"] == hashlib.sha256(b"").hexdigest() and manifest["dependencies"] == []


def test_read_wheel_gives_back_what_was_built(sample: Path):
    built = build_package(sample)
    found = read_wheel(built.path.name, built.path.read_bytes())
    assert (found.name, found.version, found.sha256, found.size) == (
        "acme-invoices",
        "1.0.0",
        built.sha256,
        built.size,
    )
    assert found.manifest == built.manifest


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda t: t.replace("version: 1.0.0\n", ""), "version: is required"),
        (lambda t: t.replace("version: 1.0.0", "version: one"), "is not a version"),
        (lambda t: t.replace("version: 1.0.0", "version: v1.0.0"), "normal form"),
        (lambda t: t.replace("version: 1.0.0", "version: 1.0.0+evil"), "has a local part"),
        (lambda t: t.replace("name: acme-invoices", "name: Acme_Invoices"), "is not a package name"),
        (
            lambda t: t.replace("version: 1.0.0", "version: 1.0.0\nfc_versions: '>=99'"),
            "doesn't include this",
        ),
        (
            lambda t: t.replace("  erp: {connector: simulation.sink}\n", ""),
            "feather validate --env dev finds",
        ),
        (lambda t: t.replace("modules: [flows.invoices]", "modules: [flows.nothing]"), "doesn't load"),
    ],
)
def test_a_project_that_isnt_a_package_says_what_to_fix(sample: Path, change, message: str):
    path = sample / "feather.yaml"
    path.write_text(change(path.read_text(encoding="utf-8")), encoding="utf-8")
    with pytest.raises(ManifestError, match=message):
        build_package(sample)
    assert not (sample / "dist").exists() or not list((sample / "dist").iterdir())


def test_pyproject_must_agree_and_dependencies_need_a_lock(sample: Path):
    path = sample / "pyproject.toml"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace('version = "1.0.0"', 'version = "1.0.1"'), encoding="utf-8")
    with pytest.raises(ManifestError, match="pyproject.toml says version 1.0.1"):
        build_package(sample)
    path.write_text(
        text.replace('["feather-connect"]', '["feather-connect", "requests>=2"]'), encoding="utf-8"
    )
    with pytest.raises(ManifestError, match="no uv.lock: run uv lock"):
        build_package(sample)


@pytest.mark.skipif(shutil.which("uv") is None, reason="needs uv")
def test_a_uv_lock_is_exported_into_the_wheel_without_feather_connect(sample: Path):
    # The sample's own pyproject, dependencies = ["feather-connect"], with a checked-in lock: `uv export --frozen` needs
    # neither the network nor a cache.
    shutil.copyfile(FIXTURE_LOCK, sample / "uv.lock")
    built = build_package(sample)
    lock = _entries(built.path)[f"acme_invoices-1.0.0.dist-info/{LOCK_FILE}"]
    assert built.manifest["lock_hash"] == hashlib.sha256(lock).hexdigest()
    lines = lock.decode().splitlines()
    requirements = [line for line in lines if line and not line[0].isspace() and not line.startswith("#")]
    assert requirements and not [r for r in requirements if r.startswith(("feather-", "-e", "."))]
    assert sum("--hash=sha256:" in line for line in lines) >= len(requirements)
    assert built.manifest["dependencies"] == []


def test_a_lock_without_hashes_is_refused(sample: Path, monkeypatch: pytest.MonkeyPatch):
    (sample / "uv.lock").write_text("", encoding="utf-8")
    exported = (
        b"pyyaml==6.0.2 \\\n    --hash=sha256:aa\n-e ./vendored/thing\nrequests==2.32.0\n    # via acme\n"
    )

    def export(*_: object, **__: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess([], 0, exported, b"")

    monkeypatch.setattr(subprocess, "run", export)
    with pytest.raises(ManifestError, match=r"without hashes \(-e, requests==2.32.0\)"):
        build_package(sample)


def test_symlinks_stay_out_of_the_wheel(sample: Path, tmp_path: Path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "stolen.txt").write_text("secret", encoding="utf-8")
    try:
        (sample / "flows" / "linked").symlink_to(outside, target_is_directory=True)
        (sample / "fixtures" / "link.json").symlink_to(outside / "stolen.txt")
    except OSError:
        pytest.skip("this machine can't make symlinks")
    names = _entries(build_package(sample).path)
    assert not [n for n in names if "linked" in n or "link.json" in n]


@pytest.mark.skipif(shutil.which("git") is None, reason="needs git")
def test_the_manifest_says_whether_the_tree_was_dirty(sample: Path):
    assert build_package(sample).manifest["dirty"] is None  # not in git
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"]
    subprocess.run([*git, "init", "-q"], cwd=sample, check=True)
    subprocess.run([*git, "add", "."], cwd=sample, check=True)
    subprocess.run([*git, "commit", "-qm", "x"], cwd=sample, check=True)
    clean = build_package(sample).manifest
    assert clean["dirty"] is False and clean["commit"]
    with (sample / "flows" / "invoices.py").open("a", encoding="utf-8") as file:
        file.write("# uncommitted\n")
    assert build_package(sample).manifest["dirty"] is True


def test_a_secret_written_as_its_value_is_warned_about(sample: Path):
    assert build_package(sample).warnings == ()
    (sample / "env" / "prod.yaml").write_text(
        "settings: {timezone: UTC}\nconnections:\n  erp:\n    config: {api_token: abc123, password: '${secret:env:ERP}'}\n",
        encoding="utf-8",
    )
    warnings = build_package(sample).warnings
    assert len(warnings) == 1 and warnings[0].startswith(
        "env/prod.yaml.connections.erp.config.api_token looks"
    )


def _rezip(data: bytes, drop: str = "", replace: tuple[str, bytes] | None = None) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(data)) as source, zipfile.ZipFile(out, "w") as target:
        for name in source.namelist():
            if name == drop:
                continue
            target.writestr(name, replace[1] if replace and replace[0] == name else source.read(name))
    return out.getvalue()


def test_read_wheel_refuses_what_isnt_a_flow_package(sample: Path):
    built = build_package(sample)
    data, name = built.path.read_bytes(), built.path.name
    manifest_path = "acme_invoices/feather-package.json"
    other = json.dumps({**built.manifest, "version": "2.0.0"}).encode()
    for filename, payload, message in (
        ("acme_invoices-1.0.0.tar.gz", data, "is not a wheel's name"),
        (name, b"not a zip", "is not a zip"),
        ("other-1.0.0-py3-none-any.whl", data, "has no other-1.0.0.dist-info"),
        (name, _rezip(data, drop=manifest_path), "not a flow package"),
        (name, _rezip(data, replace=(manifest_path, b"{")), "is not JSON"),
        (name, _rezip(data, replace=(manifest_path, b'{"schema": 1}')), "is a required property"),
        (name, _rezip(data, replace=(manifest_path, other)), "not the wheel's"),
        (name, b"x" * (MAX_WHEEL + 1), "more than the"),
        ("../acme_invoices-1.0.0-py3-none-any.whl", data, "use only letters"),
        ("acme_invoices-1.0.0-py3-none-any%2F.whl", data, "use only letters"),
        (name, _rezip(data, replace=(manifest_path, b" " * (MAX_ENTRY + 1))), "is more than 1048576 bytes"),
        (
            name,
            _rezip(data, replace=("acme_invoices-1.0.0.dist-info/METADATA", b"A" * (MAX_ENTRY + 1))),
            "METADATA is more",
        ),
    ):
        with pytest.raises(ManifestError, match=message):
            read_wheel(filename, payload)


def test_every_workspace_distribution_is_left_out_of_a_lock():
    import tomllib

    from feather_connect.packages.build import PLATFORM_DISTRIBUTIONS

    root = Path(__file__).resolve().parents[4]
    found = [*root.glob("packages/*/pyproject.toml"), *root.glob("packages/*/*/pyproject.toml")]
    names = {tomllib.loads(p.read_text(encoding="utf-8"))["project"]["name"] for p in found}
    assert set(PLATFORM_DISTRIBUTIONS) == names


def test_a_lock_says_which_of_its_hashes_are_wheels(tmp_path: Path):
    from feather_connect.packages.build import _wheel_hashes, with_wheel_hashes  # pyright: ignore[reportPrivateUsage]

    wheel, sdist = "sha256:" + "a" * 64, "sha256:" + "b" * 64
    (tmp_path / "uv.lock").write_text(
        f"""version = 1
[[package]]
name = "httpx"
version = "0.28.1"
sdist = {{ url = "https://x/httpx.tar.gz", hash = "{sdist}" }}
wheels = [{{ url = "https://x/httpx.whl", hash = "{wheel}" }}]

[[package]]
name = "only-source"
version = "1.0"
sdist = {{ url = "https://x/only.tar.gz", hash = "{sdist}" }}
""",
        encoding="utf-8",
    )
    lock = (
        f"httpx==0.28.1 \\\n    --hash={sdist} \\\n    --hash={wheel}\n    # via acme\n"
        f"only-source==1.0 \\\n    --hash={sdist}\n"
    )
    noted = with_wheel_hashes(lock.encode(), _wheel_hashes(tmp_path / "uv.lock")).decode()
    assert f"    # wheels: {wheel}\n" in noted
    assert noted.rstrip().endswith("# wheels:")  # only a source distribution: nothing named
    assert noted.replace(f"    # wheels: {wheel}\n", "").replace("\n    # wheels:", "").strip() == lock.strip()
