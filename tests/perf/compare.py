"""Compare a run's performance numbers with the baseline of the machine they ran on (M3 plan 1 P1).

    uv run python tests/perf/compare.py RESULTS.jsonl [--baseline docs/06-release/perf-baseline.json] [--summary F]

A number fails when it misses its NFR's target, or when it is more than 20% worse than the baseline of the same
machine label (`FEATHER_PERF_MACHINE`, recorded with each number). One slow run on a shared runner is expected now and
then, so the nightly job runs the tests twice and gives each number its better result before comparing. A machine
with no baseline yet is compared with the targets only. `--update` writes the run's numbers as that machine's new
baseline, for after a change that is meant to move them.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REGRESSION = 0.20


def best_of(lines: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Each number's best result in the file, by name."""
    best: dict[str, dict[str, Any]] = {}
    for line in lines:
        held = best.get(line["name"])
        better = held is None or (
            line["value"] > held["value"] if line["higher_is_better"] else line["value"] < held["value"]
        )
        if better:
            best[line["name"]] = line
    return best


def compare(
    results: dict[str, dict[str, Any]], baseline: dict[str, dict[str, float]]
) -> tuple[list[str], list[str]]:
    """The table's rows, and what failed."""
    rows, failed = [], []
    for name, line in sorted(results.items()):
        value, target, up = line["value"], line["target"], line["higher_is_better"]
        machine = line["machine"]["label"]
        base = baseline.get(machine, {}).get(name)
        meets = value >= target if up else value <= target
        change = None if not base else (value - base) / base
        regressed = change is not None and (change < -REGRESSION if up else change > REGRESSION)
        if not meets:
            failed.append(f"{name}: {value} {line['unit']} misses the target of {target}")
        if regressed:
            failed.append(f"{name}: {value} {line['unit']} is more than 20% worse than {base} on {machine}")
        shown = "" if change is None else f"{change:+.0%}"
        verdict = "fail" if not meets or regressed else "ok"
        rows.append(
            f"| {line['nfr']} | {name} | {value:,} {line['unit']} | {target:,} | {base or '—'} | {shown} | {verdict} |"
        )
    return rows, failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("results", type=Path)
    parser.add_argument("--baseline", type=Path, default=Path("docs/06-release/perf-baseline.json"))
    parser.add_argument("--summary", type=Path, help="append the table, as Markdown, to this file")
    parser.add_argument("--update", action="store_true", help="make this run the machine's baseline")
    args = parser.parse_args(argv)

    lines = [json.loads(x) for x in args.results.read_text(encoding="utf-8").splitlines() if x.strip()]
    if not lines:
        print("no performance numbers were recorded", file=sys.stderr)
        return 1
    results = best_of(lines)
    baseline: dict[str, dict[str, float]] = (
        json.loads(args.baseline.read_text(encoding="utf-8")) if args.baseline.exists() else {}
    )
    rows, failed = compare(results, baseline)
    machine = next(iter(results.values()))["machine"]
    table = "\n".join(
        [
            f"Performance on `{machine['label']}` ({machine['os']}, {machine['cpus']} CPUs)",
            "",
            "| NFR | Number | This run | Target | Baseline | Change | |",
            "|---|---|---|---|---|---|---|",
            *rows,
        ]
    )
    print(table)
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as out:
            out.write(table + "\n")
    if args.update:
        baseline[machine["label"]] = {name: line["value"] for name, line in results.items()}
        args.baseline.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for line in failed:
        print(f"FAIL {line}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
