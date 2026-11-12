from typer.testing import CliRunner

from feather_cli.main import app


def test_version_prints_the_installed_version() -> None:
    result = CliRunner().invoke(app, ["--version"])
    assert result.exit_code == 0 and result.output.startswith("feather ")
