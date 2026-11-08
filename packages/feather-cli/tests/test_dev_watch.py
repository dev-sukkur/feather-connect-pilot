"""`feather dev`'s reload on a change, when the file watcher misses one."""

import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from feather_cli.commands import dev as dev_command
from feather_connect.project import ProjectError


def test_a_change_the_watcher_did_not_report_is_reloaded_all_the_same(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On Linux, inotify sometimes loses an edit made just after a failed reload: the fix was never picked up."""
    (tmp_path / "flows").mkdir()
    flow = tmp_path / "flows" / "orders.py"
    flow.write_text("owner = 'demo'\n")
    (tmp_path / "feather.yaml").write_text("name: x\nmodules: []\n")
    stop = threading.Event()
    loads: list[str] = []
    ticks = 0

    def watch_reporting_nothing(*paths: Path, **options: Any) -> Iterator[set[Any]]:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            if ticks == 3:
                flow.write_text("owner = 'fixed, and longer'\n")  # an edit the watcher never reports
            if ticks > 20:
                stop.set()
            yield set()

    def load(root: Path, env: str) -> None:
        loads.append(flow.read_text())
        stop.set()
        raise ProjectError("stop here")

    monkeypatch.setattr(dev_command, "watch", watch_reporting_nothing)
    monkeypatch.setattr(dev_command, "load_project", load)
    dev_command._watch(tmp_path, "dev", 0, object(), stop, lambda: None)  # type: ignore[arg-type]
    assert loads == ["owner = 'fixed, and longer'\n"]
