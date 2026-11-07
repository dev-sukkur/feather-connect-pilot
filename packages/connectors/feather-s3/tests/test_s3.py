"""`s3.Files` and `s3.PartitionFiles` over `memory://`, `file://` and a moto S3 server: the kit, formats, encodings,
unreadable rows, stamps, stable files, atomic partitions and credentials."""

import io
import json
import logging
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import fsspec  # pyright: ignore[reportMissingTypeStubs]
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem  # pyright: ignore[reportMissingTypeStubs]

import feather_connect as fc
from feather_connect.connectors import s3  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ReadRequest, Unreadable
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.testing.conformance import SinkConformance, SourceConformance
from feather_connect.testing.readme import run_readme_examples

SECRET = "moto-secret-key-9f3a"


def context(now: datetime | None = None, **config: Any) -> Context:
    """A context an hour ahead of the wall clock, so that every object just written counts as stable."""
    moment = now or datetime.now(UTC) + timedelta(hours=1)
    return Context(config, logging.getLogger("s3-test"), moment, "UTC")


def read(
    source: Any, ctx: Context, req: ReadRequest | None = None
) -> tuple[list[dict[str, Any]], list[Unreadable]]:
    rows: list[dict[str, Any]] = []
    bad: list[Unreadable] = []
    for batch in source.read(ctx, req or ReadRequest("*", fc.partition.none())):
        items: list[Any] = batch.to_pylist() if isinstance(batch, pa.RecordBatch) else list(batch)
        rows.extend(item for item in items if not isinstance(item, Unreadable))
        bad.extend(item for item in items if isinstance(item, Unreadable))
    return rows, bad


def apply_batch(
    records: list[ApplyRecord],
    *,
    partition: str,
    version: str = "v1",
    chunk: int = 0,
    last: bool = True,
    mode: Any = "replace_partition",
) -> ApplyBatch:
    key = f"r:{version}:{partition}:{chunk}:0"
    return ApplyBatch("r", version, chunk, partition, mode, records, key, chunk == 0, last)


def up(key: str, **payload: Any) -> ApplyRecord:
    return ApplyRecord(key, "upsert", payload)


class Files:
    """A prefix of its own in the process-wide `memory://` store."""

    def __init__(self) -> None:
        self.fs = fsspec.filesystem("memory")
        self.prefix = f"/t-{uuid.uuid4().hex}"

    def url(self, path: str) -> str:
        return f"memory://{self.prefix}/{path}"

    def put(self, path: str, data: bytes | str, encoding: str = "utf-8") -> None:
        self.fs.pipe_file(f"{self.prefix}/{path}", data.encode(encoding) if isinstance(data, str) else data)


@pytest.fixture
def files() -> Iterator[Files]:
    store = Files()
    yield store
    if store.fs.exists(store.prefix):
        store.fs.rm(store.prefix, recursive=True)


def parquet(rows: list[dict[str, Any]]) -> bytes:
    out = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), out)
    return out.getvalue()


def encoded(rows: list[dict[str, Any]], fmt: str) -> bytes:
    if fmt == "parquet":
        return parquet(rows)
    if fmt == "json":
        return json.dumps(rows).encode()
    if fmt == "jsonl":
        return "".join(json.dumps(row) + "\n" for row in rows).encode()
    head = ",".join(rows[0])
    return (head + "\n" + "".join(",".join(str(v) for v in row.values()) + "\n" for row in rows)).encode()


class TestFiles(SourceConformance):
    @pytest.fixture
    def ctx(self) -> Context:
        return context()

    @pytest.fixture
    def source(self, files: Files) -> Any:
        return s3.Files("landing", path=files.url("items/*.parquet"), format="parquet")

    @pytest.fixture
    def seed(self, files: Files) -> Callable[[list[dict[str, Any]]], None]:
        def load(rows: list[dict[str, Any]]) -> None:
            files.put("items/part-0.parquet", parquet(rows))

        return load

    @pytest.fixture
    def permanent_fault(self, files: Files) -> Callable[[], None]:
        return lambda: files.put("items/part-1.parquet", b"not parquet at all")

    @pytest.fixture
    def transient_fault(self, monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
        def reset(*args: Any, **kwargs: Any) -> Any:
            raise ConnectionResetError("the connection was reset")

        return lambda: monkeypatch.setattr(MemoryFileSystem, "_open", reset)


class TestPartitionFiles(SinkConformance):
    deletes_supported = False  # a file holds no deletes: append turns them down
    default_mode = "append"

    @pytest.fixture
    def ctx(self) -> Context:
        return context()

    @pytest.fixture
    def sink(self, files: Files) -> Any:
        return s3.PartitionFiles(
            "lake", path_template=files.url("out/{partition}/data.jsonl"), format="jsonl"
        )

    @pytest.fixture
    def fresh_sink(self, files: Files) -> Callable[[], Any]:
        return lambda: s3.PartitionFiles(
            "lake", path_template=files.url("out/{partition}/data.jsonl"), format="jsonl"
        )

    @pytest.fixture
    def read_back(self, files: Files) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        def back() -> Mapping[Any, Mapping[str, Any]]:
            held: dict[Any, Mapping[str, Any]] = {}
            for path in files.fs.glob(f"{files.prefix}/out/*/*.jsonl"):
                for line in files.fs.cat_file(path).decode().splitlines():
                    row = json.loads(line)
                    held[row["id"]] = row
            return held

        return back

    @pytest.fixture
    def permanent_fault(self, monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
        def refuse(*args: Any, **kwargs: Any) -> Any:
            raise PermissionError("access denied")

        return lambda: monkeypatch.setattr(MemoryFileSystem, "pipe_file", refuse)

    @pytest.fixture
    def transient_fault(self, monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
        def reset(*args: Any, **kwargs: Any) -> Any:
            raise ConnectionResetError("the connection was reset")

        return lambda: monkeypatch.setattr(MemoryFileSystem, "pipe_file", reset)


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_each_format_reads(fmt: Any, files: Files):
    rows = [{"id": n, "name": f"row {n}"} for n in range(3)]
    files.put(f"in/a.{fmt}", encoded(rows, fmt))
    found, bad = read(s3.Files("landing", path=files.url(f"in/*.{fmt}"), format=fmt), context())
    assert [(r["id"], r["name"]) for r in found] == [(0, "row 0"), (1, "row 1"), (2, "row 2")] and not bad
    assert all(r["_object_path"].endswith(f"a.{fmt}") and r["_object_mtime"].tzinfo for r in found)


def test_csv_with_bom_and_latin1(files: Files):  # Review Focus 4
    files.put("bom/a.csv", "﻿id,name\n1,Müller\n")
    files.put("latin/a.csv", "id;name\n1;Müller\n", encoding="latin-1")
    rows, _ = read(s3.Files("landing", path=files.url("bom/*.csv"), format="csv"), context())
    assert list(rows[0])[:2] == ["id", "name"]
    assert rows[0]["name"] == "Müller"
    latin = s3.Files(
        "landing", path=files.url("latin/*.csv"), format="csv", encoding="latin-1", delimiter=";"
    )
    rows, _ = read(latin, context())
    assert rows[0]["name"] == "Müller"


def test_short_csv_row_is_quarantined_and_the_file_continues(files: Files):  # Review Focus 4
    files.put("in/a.csv", "id,name\n1,a\n2\n3,c\n")
    rows, bad = read(s3.Files("landing", path=files.url("in/*.csv"), format="csv"), context())
    assert [r["id"] for r in rows] == [1, 3]
    assert [b.raw for b in bad] == ["2"] and "expected 2 fields, found 1" in bad[0].reason


def test_a_jsonl_line_that_does_not_parse_is_unreadable(files: Files):
    files.put("in/a.jsonl", '{"id": 1}\n{"id": 2\n[3]\n{"id": 4}\n')
    rows, bad = read(s3.Files("landing", path=files.url("in/*.jsonl"), format="jsonl"), context())
    assert [r["id"] for r in rows] == [1, 4] and [b.raw for b in bad] == ['{"id": 2', "[3]"]


def test_by_stamp_reads_only_new_objects(files: Files):
    source = s3.Files("landing", path=files.url("in/*.jsonl"), format="jsonl")
    files.put("in/a.jsonl", '{"id": 1}\n')
    first, _ = read(source, context())
    since = max(r["_object_mtime"] for r in first) + timedelta(microseconds=1)
    time.sleep(0.01)
    files.put("in/b.jsonl", '{"id": 2}\n')
    req = ReadRequest("*", fc.partition.none(), since=since, stamp_field="_object_mtime")
    rows, _ = read(source, context(), req)
    assert [r["id"] for r in rows] == [2]
    again = ReadRequest("*", fc.partition.none(), since=since.isoformat(), stamp_field="_object_mtime")
    assert [r["id"] for r in read(source, context(), again)[0]] == [2]  # a watermark kept as text


def test_recently_modified_file_is_skipped_until_stable(files: Files):  # Review Focus 5
    source = s3.Files("landing", path=files.url("in/*.jsonl"), format="jsonl", stable_after="60s")
    files.put("in/a.jsonl", '{"id": 1}\n')
    ((_, written),) = source.objects(context(), ReadRequest("*", fc.partition.none()))
    assert read(source, context(now=written + timedelta(seconds=10)))[0] == []
    assert [r["id"] for r in read(source, context(now=written + timedelta(seconds=61)))[0]] == [1]


def test_no_matching_files_is_an_empty_read(
    files: Files,
):  # Review Focus 5; the empty-read guard then applies
    assert read(s3.Files("landing", path=files.url("none/*.csv"), format="csv"), context()) == ([], [])


def test_a_partition_read_keeps_its_rows(files: Files):
    files.put("in/a.jsonl", '{"id": 1, "day": "d1"}\n{"id": 2, "day": "d2"}\n')
    source = s3.Files("landing", path=files.url("in/*.jsonl"), format="jsonl")
    rows, _ = read(source, context(), ReadRequest("day=d2", fc.partition.by_fields("day")))
    assert [r["id"] for r in rows] == [2]


def test_partition_files_are_replaced_atomically(tmp_path: Path):  # write to a temp key, then move
    template = f"file://{tmp_path.as_posix()}/lake/day={{partition}}/data.parquet"
    sink = s3.PartitionFiles("lake", path_template=template, format="parquet")
    target = tmp_path / "lake" / "day=d1" / "data.parquet"
    ctx = context()
    sink.apply(ctx, apply_batch([up("a", id="a", v=1)], partition="d1"))
    sink.apply(ctx, apply_batch([up("b", id="b", v=2)], partition="d1", version="v2", last=False))
    assert [r["id"] for r in pq.read_table(target).to_pylist()] == ["a"]  # the old slice until the last chunk
    sink.apply(ctx, apply_batch([up("c", id="c", v=3)], partition="d1", version="v2", chunk=1))  # os.replace
    found = pq.read_table(target).to_pylist()
    assert [(r["id"], r["_fc_partition"], r["_fc_version_id"]) for r in found] == [
        ("b", "d1", "v2"),
        ("c", "d1", "v2"),
    ]
    left = [p.name for p in (tmp_path / "lake").rglob("*") if p.is_file() and p.suffix in (".tmp", ".jsonl")]
    assert left == []  # no temporary file, no staged chunk
    moved = ApplyRecord("b", "upsert", {"id": "b", "v": 4}, moved_from="d1")
    sink.apply(ctx, apply_batch([moved], partition="d2", version="v3"))  # B6: the key leaves d1
    assert [r["id"] for r in pq.read_table(target).to_pylist()] == ["c"]
    sink.apply(ctx, apply_batch([], partition="d2", version="v4"))  # the slice became empty
    assert not (tmp_path / "lake" / "day=d2" / "data.parquet").exists()


def test_a_chunk_sent_again_changes_nothing(files: Files):
    sink = s3.PartitionFiles("lake", path_template=files.url("out/{partition}.jsonl"), format="jsonl")
    batch = apply_batch([up("a", id="a")], partition="d1")
    sink.apply(context(), batch)
    files.put("out/d1.jsonl", '{"id": "changed"}\n')
    assert sink.apply(context(), batch).applied == 1
    assert files.fs.cat_file(f"{files.prefix}/out/d1.jsonl") == b'{"id": "changed"}\n'


@pytest.fixture(scope="module")
def moto_server() -> Iterator[str]:
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


def s3_config(endpoint: str) -> dict[str, Any]:
    return {
        "endpoint_url": endpoint,
        "region": "us-east-1",
        "access_key_id": "testing-key",
        "secret_access_key": SECRET,
    }


def test_works_against_moto_s3(moto_server: str):
    config = s3_config(moto_server)
    bucket = f"b-{uuid.uuid4().hex[:12]}"
    fs = fsspec.filesystem(
        "s3", key="testing-key", secret=SECRET, endpoint_url=moto_server, skip_instance_cache=True
    )
    fs.mkdir(bucket)
    fs.pipe_file(f"{bucket}/in/a.csv", b"id,name\n1,a\n2,b\n")
    source = s3.Files("landing", path=f"s3://{bucket}/in/*.csv", format="csv")
    assert source.check(context(**config)).ok
    rows, _ = read(source, context(**config))
    assert [r["id"] for r in rows] == [1, 2]
    fs.pipe_file(f"{bucket}/in/b.csv", b"id,name\n3,c\n")  # a listing after a write sees it: no cache
    assert [r["id"] for r in read(source, context(**config))[0]] == [1, 2, 3]
    sink = s3.PartitionFiles(
        "lake", path_template=f"s3://{bucket}/out/{{partition}}/data.parquet", format="parquet"
    )
    assert sink.check(context(**config)).ok
    sink.apply(context(**config), apply_batch([up("a", id="a")], partition="d1", last=False))
    sink.apply(context(**config), apply_batch([up("b", id="b")], partition="d1", chunk=1))
    out = s3.Files("lake", path=f"s3://{bucket}/out/*/data.parquet", format="parquet")
    assert [r["id"] for r in read(out, context(**config))[0]] == ["a", "b"]
    assert fs.find(f"{bucket}/out/_staging") == []


def test_credentials_stay_out_of_errors(moto_server: str):
    config = s3_config(moto_server)
    source = s3.Files("landing", path="s3://no-such-bucket-here/*.csv", format="csv")
    sink = s3.PartitionFiles("lake", path_template="s3://no-such-bucket-here/{partition}.csv", format="csv")
    result = sink.check(context(**config))
    assert not result.ok and SECRET not in result.message
    with pytest.raises(PermanentError) as raised:
        sink.apply(context(**config), apply_batch([up("a", id="a")], partition="d1"))
    assert SECRET not in str(raised.value)
    rows, _ = read(source, context(**config))
    assert rows == []  # a missing bucket lists nothing; no secret could show


def test_readme_example_runs(files: Files, tmp_path: Path):
    names = run_readme_examples(Path(__file__).resolve().parents[1] / "README.md", tmp_path)
    assert isinstance(names["orders"].source, s3.Files)


def test_unknown_format_is_refused():
    with pytest.raises(ValueError, match="format"):
        s3.Files("landing", path="memory://x/*", format="xml")  # pyright: ignore[reportArgumentType]
