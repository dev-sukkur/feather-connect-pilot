"""The performance tests' numbers (M3 plan 1 P1): each one is printed, and kept as a JSON line in the file that
`FEATHER_PERF_RESULTS` names, with the machine it ran on, for `tests/perf/compare.py` and
docs/06-release/perf-results.md."""

import json
import os
import platform
from datetime import UTC, datetime
from pathlib import Path


def machine() -> dict[str, object]:
    """What the numbers are to be read against."""
    return {
        "label": os.environ.get("FEATHER_PERF_MACHINE", platform.node() or "unknown"),
        "os": platform.platform(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
    }


def record(nfr: str, name: str, value: float, unit: str, target: float, *, higher_is_better: bool) -> None:
    """Keep one number. `target` is the NFR's; the regression check compares with the machine's baseline."""
    line = {
        "nfr": nfr,
        "name": name,
        "value": round(value, 3),
        "unit": unit,
        "target": target,
        "higher_is_better": higher_is_better,
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": machine(),
    }
    print(f"\n{nfr} {name}: {line['value']} {unit} (target {target} {unit})")
    path = os.environ.get("FEATHER_PERF_RESULTS")
    if path:
        with Path(path).open("a", encoding="utf-8") as out:
            out.write(json.dumps(line) + "\n")
