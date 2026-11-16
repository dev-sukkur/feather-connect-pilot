"""The G2 benchmark: do AI-written flows, started from a template, pass `feather validate` and their tests unaided?

    python benchmarks/ai-flows/run.py prompts OUT_DIR       # one prompt per task, for the AI under test
    python benchmarks/ai-flows/run.py score ANSWERS_DIR     # score its answers: ANSWERS_DIR/<task>/...
    python benchmarks/ai-flows/run.py score --reference     # score the reference answers (all should pass)

An answer is the files the AI wrote or changed, laid out as in the project (`flows/…`, `feather.yaml`, `env/…`,
`tests/…`). Each task is scored on a fresh copy of its template, without the template's own tests, with the task's
fixtures and the answer laid over it. It passes when:

1. `feather validate` finds no error in any of the four environments;
2. the answer brings tests of its own (`tests/test_*.py`), and they pass;
3. the task's hidden acceptance test passes: it checks that the flow does what the task asked.

Nothing is fixed by hand between the answer and the score. The report (`--report`, JSON) has each task's result
and where it failed; the run exits 1 when the pass rate is below the target (80%, discovery G2).
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
TEMPLATES = REPO / "templates"
TASKS = HERE / "tasks"
REFERENCE = HERE / "reference"
ENVS = ("dev", "qa", "uat", "prod")
TARGET = 0.8
STEP_TIMEOUT_S = 300
# What `feather validate --env <env>` checks, for every environment in one process: it prints each error and exits 1.
VALIDATE = """
import sys
from pathlib import Path
from feather_connect.project import ProjectError, load_project, validate_project

failed = False
for env in sys.argv[2:]:
    try:
        problems = [p for p in validate_project(load_project(Path(sys.argv[1]), env)) if p.severity == "error"]
    except ProjectError as error:
        problems = [error]
    for p in problems:
        failed = True
        print(f"{env}: {p}")
sys.exit(1 if failed else 0)
"""

INSTRUCTIONS = """\
You are working in a Feather Connect project made from the `{template}` template (`templates/{template}` in the
Feather Connect repository: read its README.md, feather.yaml, env/ and flows/). Its flows call the template's library
function in `feather_connect.templates.generic`; use it where it fits, and Feather's own API (`feather_connect`, the
connectors in `feather_connect.connectors`) where it doesn't.

The task:

{prompt}
Answer with the files you write or change, laid out as in the project (for example `flows/<name>.py`,
`feather.yaml`, `env/dev.yaml`), with tests of your own in `tests/test_*.py`: the template's tests are not part of
the answer. Don't change `fixtures/`: the benchmark puts its own there. Your answer passes when `feather validate`
finds no error in dev, qa, uat and prod, your tests pass, and so does a hidden test of what the task asks.
"""


@dataclass
class Result:
    task: str
    passed: bool
    stage: str  # where it stopped: answer, validate, tests, acceptance, or passed
    detail: str = ""


def tasks(only: list[str] | None = None) -> list[tuple[str, dict[str, Any]]]:
    found = [
        (p.name, yaml.safe_load((p / "task.yaml").read_text(encoding="utf-8")))
        for p in sorted(TASKS.iterdir())
    ]
    if only:
        unknown = set(only) - {name for name, _ in found}
        if unknown:
            raise SystemExit(f"no such task: {', '.join(sorted(unknown))}")
        found = [(name, task) for name, task in found if name in only]
    return found


def prompt_of(task: dict[str, Any]) -> str:
    return INSTRUCTIONS.format(template=task["template"], prompt=task["prompt"])


def write_prompts(out: Path, only: list[str] | None = None) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name, task in tasks(only):
        path = out / f"{name}.md"
        path.write_text(f"# {task['title']}\n\n{prompt_of(task)}", encoding="utf-8", newline="\n")
        written.append(path)
    return written


def _run(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=STEP_TIMEOUT_S,
        check=False,
    )


def _tail(done: subprocess.CompletedProcess[str]) -> str:
    return (done.stdout + done.stderr)[-3000:]


def project_for(name: str, task: dict[str, Any], answer: Path, into: Path) -> Path:
    """The template, without its tests, with the task's fixtures and the answer laid over it."""
    root = into / name
    shutil.copytree(TEMPLATES / task["template"], root, ignore=shutil.ignore_patterns("tests", "__pycache__"))
    (root / "tests").mkdir()
    fixtures = TASKS / name / "fixtures"
    if fixtures.is_dir():
        shutil.copytree(fixtures, root / "fixtures", dirs_exist_ok=True)
    for path in sorted(answer.rglob("*")):
        rel = path.relative_to(answer)
        if path.is_file() and rel.parts[0] != "fixtures" and "__pycache__" not in rel.parts:
            target = root / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
    return root


def score_task(name: str, task: dict[str, Any], answer: Path, work: Path) -> Result:
    if not answer.is_dir() or not any(p.is_file() for p in answer.rglob("*")):
        return Result(name, False, "answer", f"no answer at {answer}")
    root = project_for(name, task, answer, work)
    own_tests = sorted(p.name for p in (root / "tests").glob("test_*.py"))
    if not own_tests:
        return Result(name, False, "tests", "the answer brings no tests (tests/test_*.py)")
    done = _run([sys.executable, "-c", VALIDATE, str(root), *ENVS], root)
    if done.returncode != 0:
        return Result(name, False, "validate", _tail(done))
    pytest = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "--rootdir", str(root)]
    done = _run([*pytest, *(f"tests/{t}" for t in own_tests)], root)
    if done.returncode != 0:
        return Result(name, False, "tests", _tail(done))
    shutil.copyfile(HERE / "accept.py", root / "tests" / "benchmark_accept.py")
    shutil.copyfile(TASKS / name / "test_acceptance.py", root / "tests" / "test_benchmark_acceptance.py")
    done = _run([*pytest, "tests/test_benchmark_acceptance.py"], root)
    if done.returncode != 0:
        return Result(name, False, "acceptance", _tail(done))
    return Result(name, True, "passed")


def score(answers: Path, only: list[str] | None = None, jobs: int | None = None) -> list[Result]:
    """Score each task's answer, `jobs` tasks at a time (each on its own copy of its template)."""
    chosen = tasks(only)
    with (
        tempfile.TemporaryDirectory(prefix="feather-g2-") as tmp,
        ThreadPoolExecutor(jobs or _jobs()) as pool,
    ):
        return list(pool.map(lambda item: score_task(item[0], item[1], answers / item[0], Path(tmp)), chosen))


def _jobs() -> int:
    return max(1, min(8, (os.cpu_count() or 2) // 2))


def summary(results: list[Result], target: float = TARGET) -> dict[str, Any]:
    passed = sum(r.passed for r in results)
    rate = passed / len(results) if results else 0.0
    return {
        "tasks": len(results),
        "passed": passed,
        "pass_rate": round(rate, 4),
        "target": target,
        "met": rate >= target,
        "results": [asdict(r) for r in results],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    prompts = commands.add_parser("prompts", help="write one prompt per task")
    prompts.add_argument("out", type=Path)
    prompts.add_argument("--task", action="append", dest="only")
    scoring = commands.add_parser("score", help="score answers, one directory per task")
    scoring.add_argument("answers", type=Path, nargs="?")
    scoring.add_argument("--reference", action="store_true", help="score the reference answers")
    scoring.add_argument("--task", action="append", dest="only")
    scoring.add_argument("--report", type=Path, help="write the results here as JSON")
    scoring.add_argument("--target", type=float, default=TARGET)
    scoring.add_argument("--jobs", type=int, help="tasks scored at once (half the CPUs, up to 8, by default)")
    args = parser.parse_args(argv)

    if args.command == "prompts":
        for path in write_prompts(args.out, args.only):
            print(path)
        return 0
    if args.reference == (args.answers is not None):
        parser.error("give ANSWERS_DIR or --reference")
    results = score(REFERENCE if args.reference else args.answers, args.only, args.jobs)
    report = summary(results, args.target)
    for r in results:
        print(f"{'pass' if r.passed else 'FAIL'}  {r.task:28} {r.stage}")
    print(
        f"{report['passed']}/{report['tasks']} passed ({report['pass_rate']:.0%}); target {args.target:.0%}"
    )
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["met"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
