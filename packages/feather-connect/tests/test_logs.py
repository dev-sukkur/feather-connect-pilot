import io
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest

import feather_connect as fc
from feather_connect import logs
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.engine.local import LocalEngine
from feather_connect.logs import TaskLogHandler, bind, configure_logging
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool


@pytest.fixture
def out() -> Iterator[io.StringIO]:
    """Route the JSON lines to a buffer, and put the root logger and the secrets back afterwards."""
    root = logging.getLogger()
    before = (list(root.handlers), root.level, set(logs._secrets), logs._handler)
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    yield stream
    root.handlers[:] = before[0]
    root.setLevel(before[1])
    logs._secrets.clear()
    logs._secrets.update(before[2])
    logs._handler = before[3]


def lines(stream: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_json_line_carries_bound_ids(out: io.StringIO) -> None:
    log = logging.getLogger("demo")
    with bind(tenant="default", flow="orders"):
        with bind(run="r1", partition=None):
            log.info("extracting %d rows", 3)
        log.info("outer")
    log.info("unbound")
    inner, outer, unbound = lines(out)
    assert inner["message"] == "extracting 3 rows" and inner["level"] == "INFO" and inner["logger"] == "demo"
    assert (inner["tenant"], inner["flow"], inner["run"]) == ("default", "orders", "r1")
    assert "partition" not in inner and "run" not in outer
    assert "flow" not in unbound
    with pytest.raises(TypeError), bind(colour="red"):
        pass


def test_secrets_are_redacted(out: io.StringIO) -> None:
    configure_logging("INFO", secrets=["hunter2-token"], stream=out)
    log = logging.getLogger("demo")
    log.info("calling with hunter2-token now")
    try:
        raise RuntimeError("bad key hunter2-token")
    except RuntimeError:
        log.exception("failed with hunter2-token")
    text = out.getvalue()
    assert "hunter2-token" not in text and "***" in text
    assert len(lines(out)) == 2  # configuring twice did not double the lines


def test_secrets_are_redacted_in_the_task_log(out: io.StringIO) -> None:
    store = MemoryStore(FrozenClock(datetime(2026, 10, 18, tzinfo=UTC)))
    configure_logging("INFO", secrets=["hunter2-token"], stream=out)
    handler = TaskLogHandler(store)
    logging.getLogger().addHandler(handler)
    try:
        with bind(task="t1"):
            logging.getLogger("demo").info("token is hunter2-token")
    finally:
        logging.getLogger().removeHandler(handler)
    assert store.runs.task_logs("t1") == ["INFO: token is ***"]


def test_task_log_is_capped_with_a_notice(out: io.StringIO) -> None:
    store = MemoryStore(FrozenClock(datetime(2026, 10, 18, tzinfo=UTC)))
    handler = TaskLogHandler(store)
    logging.getLogger().addHandler(handler)
    try:
        with bind(task="t1"):
            for _ in range(200):
                logging.getLogger("demo").info("x" * 1000)
        with bind(task="t2"):
            logging.getLogger("demo").info("small")
        logging.getLogger("demo").info("no task bound")
    finally:
        logging.getLogger().removeHandler(handler)
    saved = store.runs.task_logs("t1")
    assert sum(len(line) for line in saved) <= logs.TASK_LOG_LIMIT + 200
    assert saved[-1].startswith("WARNING: log truncated") and saved.count(saved[-1]) == 1
    assert len(saved) < 200
    assert store.runs.task_logs("t2") == ["INFO: small"]


class ChattySource(Source):
    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        ctx.log.info("reading %s", req.partition)
        yield from super().read(ctx, req)


def test_tasks_bind_their_ids(out: io.StringIO) -> None:
    clock = FrozenClock(datetime(2026, 10, 18, 10, tzinfo=UTC))
    store, engine = MemoryStore(clock), LocalEngine(clock)
    dataset = fc.Dataset("d", source=ChattySource(rows=[{"id": 1}]), key="id", strategy=fc.strategy.replace())
    flow = fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("a"))], triggers=[fc.every("15m")])
    runtime = Runtime([flow], store, MemorySpool(clock), engine, code_sha="sha", clock=clock)
    engine.register(runtime.tasks())
    handler = TaskLogHandler(store)
    logging.getLogger().addHandler(handler)
    try:
        run_id = runtime.start_run("f")
        engine.run_until_idle()
    finally:
        logging.getLogger().removeHandler(handler)
    mine = [line for line in lines(out) if line["logger"] == "memory"]
    assert mine and all(line["run"] == run_id and line["flow"] == "f" and line["task"] for line in mine)
    assert all(line["dataset"] == "d" for line in mine)
    extract = next(t for t in store.runs.list_tasks(run_id) if t.kind == "extract")
    assert any("reading" in line for line in store.runs.task_logs(extract.task_id))


def test_a_process_saves_each_task_line_once_however_many_install_the_handler(out: io.StringIO) -> None:  # M5
    store = MemoryStore(FrozenClock(datetime(2026, 10, 18, tzinfo=UTC)))
    remove_worker = logs.install_task_logs(
        store
    )  # a worker and a server in one process, as feather dev runs them
    remove_server = logs.install_task_logs(store)
    try:
        with bind(task="t1"):
            logging.getLogger("demo").info("once")
    finally:
        remove_server()
        remove_worker()
    assert store.runs.task_logs("t1") == ["INFO: once"]
    assert not any(isinstance(h, TaskLogHandler) for h in logging.getLogger().handlers)


def test_the_handler_forgets_old_tasks(out: io.StringIO) -> None:  # M5
    store = MemoryStore(FrozenClock(datetime(2026, 10, 18, tzinfo=UTC)))
    handler = TaskLogHandler(store)
    logging.getLogger().addHandler(handler)
    try:
        for n in range(logs.TASKS_REMEMBERED + 10):
            with bind(task=f"t{n}"):
                logging.getLogger("demo").info("x")
    finally:
        logging.getLogger().removeHandler(handler)
    assert len(handler._used) <= logs.TASKS_REMEMBERED  # pyright: ignore[reportPrivateUsage]
