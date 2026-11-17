"""The G2 benchmark's harness (benchmarks/ai-flows): every task can be done (its reference answer passes), and its
acceptance test tells a flow that does the task from one that doesn't (the template left as it was fails)."""

import importlib.util
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = [pytest.mark.timeout(900)]


def _bench():
    spec = importlib.util.spec_from_file_location(
        "ai_flow_benchmark", ROOT / "benchmarks" / "ai-flows" / "run.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bench = _bench()
TASKS = [name for name, _ in bench.tasks()]


def test_the_task_set_covers_every_template_twice():
    used = [task["template"] for _, task in bench.tasks()]
    templates = sorted(p.name for p in (ROOT / "templates").iterdir() if p.is_dir())
    assert {t: used.count(t) for t in templates} == dict.fromkeys(templates, 2)


def test_every_reference_answer_passes():
    report = bench.summary(bench.score(bench.REFERENCE))
    assert [r for r in report["results"] if not r["passed"]] == []
    assert report["met"] and report["pass_rate"] == 1.0


def test_the_template_left_as_it_was_does_not_pass(tmp_path):
    """It validates and its own tests pass: only the task's acceptance test fails."""
    chosen = ["db-categories", "api-prices-paise", "webhook-returns"]
    for task in chosen:
        template = ROOT / "templates" / dict(bench.tasks())[task]["template"]
        answer = tmp_path / "answers" / task
        shutil.copytree(template, answer, ignore=shutil.ignore_patterns("fixtures", "__pycache__"))
    results = bench.score(tmp_path / "answers", chosen)
    assert [(r.task, r.stage) for r in results] == [(task, "acceptance") for task in sorted(chosen)], [
        r.detail for r in results
    ]


def test_an_answer_without_tests_or_files_fails_before_running_anything(tmp_path):
    (tmp_path / "answers" / "db-categories" / "flows").mkdir(parents=True)
    (tmp_path / "answers" / "db-categories" / "flows" / "items.py").write_text("", encoding="utf-8")
    results = {r.task: r for r in bench.score(tmp_path / "answers", ["db-categories", "fan-out-channel"])}
    assert (results["db-categories"].stage, results["fan-out-channel"].stage) == ("tests", "answer")
    report = bench.summary(list(results.values()))
    assert (report["passed"], report["met"]) == (0, False)


def test_the_prompts_name_the_template_and_what_passing_means(tmp_path):
    written = bench.write_prompts(tmp_path)
    assert sorted(p.stem for p in written) == sorted(TASKS)
    text = (tmp_path / "api-stock.md").read_text(encoding="utf-8")
    assert "templates/api-to-warehouse" in text and "inventory_api" in text
    assert "feather validate" in text and "tests/test_*.py" in text
