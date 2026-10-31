import json
from pathlib import Path

import pytest


def edit(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new), encoding="utf-8")


def test_validate_clean_project_exits_zero(cli, project):
    r = cli("validate")
    assert r.exit_code == 0, r.output
    assert "no problems" in r.stdout


def test_validate_reports_problems_as_json_with_hints(cli, bad_project):
    r = cli("validate", "--json")
    doc = json.loads(r.stdout)
    assert r.exit_code == 1 and doc["ok"] is False
    assert doc["problems"][0]["code"] == "mode_needs_complete" and doc["problems"][0]["hint"]
    assert doc["problems"][0]["path"] == "tiny.deliver[0].mode"


def test_validate_prints_each_problem_with_its_hint_for_a_person(cli, bad_project):
    r = cli("validate")
    assert r.exit_code == 1
    assert "mode_needs_complete" in r.stdout and "hint:" in r.stdout


def test_validate_checks_budgets_partitions_and_config_references(cli, make_project, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_project = make_project

    # a partition scheme that cannot list its partitions, with no planner: Flow.validate's own check
    write_project(tmp_path, partition='by_fields("id")')
    r = cli("validate", "--json")
    codes = [p["code"] for p in json.loads(r.stdout)["problems"]]
    assert r.exit_code == 1 and "planner_required" in codes

    # a route to a connection the project does not define
    write_project(tmp_path, sink="nowhere")
    r = cli("validate", "--json")
    [problem] = json.loads(r.stdout)["problems"]
    assert r.exit_code == 1 and problem["code"] == "connection_undefined"
    assert "nowhere" in problem["message"] and "feather.yaml" in problem["hint"]

    # a connection whose connector is not the one the code uses
    write_project(tmp_path)
    edit(tmp_path / "feather.yaml", "connector: simulation.sink", "connector: postgres.sink")
    r = cli("validate", "--json")
    [problem] = json.loads(r.stdout)["problems"]
    assert r.exit_code == 1 and problem["code"] == "connector_mismatch"
    assert "postgres.sink" in problem["message"] and "simulation.sink" in problem["message"]

    # a budget that is not defined: the load itself refuses, and validate reports it
    write_project(tmp_path)
    edit(
        tmp_path / "feather.yaml", "connector: simulation.sink}", "connector: simulation.sink, budget: gone}"
    )
    r = cli("validate", "--json")
    [problem] = json.loads(r.stdout)["problems"]
    assert r.exit_code == 1 and problem["code"] == "project_invalid"
    assert "gone" in problem["message"] and "feather.yaml" in problem["message"]

    # a module that does not import
    write_project(tmp_path)
    edit(tmp_path / "feather.yaml", "flows.tiny", "flows.missing")
    r = cli("validate", "--json")
    [problem] = json.loads(r.stdout)["problems"]
    assert r.exit_code == 1 and problem["code"] == "project_invalid" and "flows.missing" in problem["message"]


def test_warnings_do_not_fail_validate(cli, warn_project):
    r = cli("validate", "--json")
    doc = json.loads(r.stdout)
    assert r.exit_code == 0 and doc["ok"] is True
    [warning] = doc["problems"]
    assert warning["severity"] == "warning" and warning["code"] == "budget_unused"
    assert "idle" in warning["message"]


def test_json_mode_keeps_stdout_clean(cli, project):
    r = cli("validate", "--json")
    assert json.loads(r.stdout)["ok"] is True  # stdout is one JSON document and nothing else
    assert "importing the tiny flow" in r.stderr  # the flow module logged while it was imported


def test_env_selects_the_overlay(cli, project):
    (project / "env" / "qa.yaml").write_text("flows:\n  nope: {enabled: false}\n")
    r = cli("validate", "--env", "qa", "--json")
    [problem] = json.loads(r.stdout)["problems"]
    assert r.exit_code == 1 and problem["code"] == "project_invalid" and "nope" in problem["message"]


def test_no_project_here_explains_what_to_do(cli, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = cli("validate")
    assert r.exit_code == 1 and "feather.yaml" in r.stderr and "--project" in r.stderr
    assert r.stdout == ""


def test_project_option_names_the_directory(cli, make_project, tmp_path):
    other = make_project(tmp_path / "other")
    r = cli("validate", "--project", str(other), "--json")
    assert r.exit_code == 0 and json.loads(r.stdout)["ok"] is True


COMMANDS = ["validate"]


def test_help_lists_every_command(cli):
    r = cli("--help")
    assert r.exit_code == 0
    for command in COMMANDS:
        assert command in r.stdout


@pytest.mark.parametrize("command", COMMANDS)
def test_every_command_takes_env_and_json(cli, command):
    r = cli(command, "--help")
    assert "--env" in r.stdout and "--json" in r.stdout
