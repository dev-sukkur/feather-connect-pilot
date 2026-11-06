"""The package installs into the namespace `feather_connect.connectors` beside the connectors of feather-connect."""

from pathlib import Path


def test_the_module_imports_from_the_shared_namespace():
    # pyright takes `feather_connect` from the first folder that has it as a regular package, and so never looks
    # for `connectors.http` in this package's folder, though Python does (see feather_connect/__init__.py).
    from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
    from feather_connect.connectors import memory

    package_src = Path(__file__).resolve().parents[1] / "src"
    assert Path(http.__file__).resolve().is_relative_to(package_src)
    assert Path(memory.__file__).resolve().parent != Path(http.__file__).resolve().parent
