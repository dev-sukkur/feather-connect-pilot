"""S3-compatible files connectors: `Files`, a source that reads the objects a path matches, and `PartitionFiles`, a
sink that writes one object per partition.

A path is a URL that fsspec opens: `s3://bucket/prefix/*.csv`, `file:///data/*.parquet` or `memory://test/*.jsonl`.
Azure Blob (`az://`) and GCS (`gs://`) work through fsspec's extras, given their settings in `storage_options`, and
stay experimental. The S3 settings come from `Context.config`, with the keys a secret under `auth`:

    connections:
      landing:
        connector: s3.files
        endpoint_url: https://s3.eu-central-1.amazonaws.com
        region: eu-central-1
        auth: {access_key_id: "${secret:env:S3_KEY}", secret_access_key: "${secret:env:S3_SECRET}"}

`Files` adds two columns to each row: `_object_path` and `_object_mtime`, the object's modified time, which a
`by_stamp` dataset uses as its stamp (B3: names starting `_fc_` are Feather's own). An object modified less than
`stable_after` ago may still be being written, so it is left for the next read. A CSV row with too few or too many
fields, and a JSON line that does not parse, comes back as `fc.Unreadable` with its text, and the file goes on.

`PartitionFiles` writes the `_fc_*` columns of ruling 1. `replace_partition` stages each chunk under
`<root>/_staging/` and, on the last chunk, writes the partition's object in one PUT (on `file://`, a temporary file
and `os.replace`), so a reader sees the old slice or the new one, never a part. `append` writes one object per chunk.
Each chunk's result is kept under `<root>/_feather_applied/`, so that a chunk sent again changes nothing (S21).
"""

import codecs
import hashlib
import io
import json
import os
import uuid
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal, cast

import fsspec  # pyright: ignore[reportMissingTypeStubs]
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, ValidationError

from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyRecord,
    ApplyResult,
    Batch,
    CheckResult,
    ConnectorSpec,
    Key,
    ReadRequest,
    Rejection,
    Secret,
    StreamSchema,
    Unreadable,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.partitions import none
from feather_connect.core.records import canonical_json
from feather_connect.core.strategies import parse_duration

__all__ = ["Files", "PartitionFiles", "S3Config"]

Format = Literal["csv", "json", "jsonl", "parquet"]
PATH_FIELD = "_object_path"
MTIME_FIELD = "_object_mtime"
STAGING = "_staging"
APPLIED = "_feather_applied"
DEFAULT_BATCH_ROWS = 10_000
_S3 = frozenset({"s3", "s3a"})
_MASK = "***"
_SUFFIX: dict[str, str] = {"csv": ".csv", "json": ".json", "jsonl": ".jsonl", "parquet": ".parquet"}


class S3Config(BaseModel):
    """The settings of a files connection. The keys come from the connection's `auth`; `storage_options` go to
    fsspec as they are, for a store other than S3."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    endpoint_url: str | None = None
    region: str | None = None
    anon: bool = False
    access_key_id: Secret | None = None
    secret_access_key: Secret | None = None
    session_token: Secret | None = None
    storage_options: dict[str, Any] = {}

    @classmethod
    def of(cls, ctx: Context, connection: str) -> "S3Config":
        try:
            return cls.model_validate(dict(ctx.config))
        except ValidationError as error:
            problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in error.errors())
            raise PermanentError(f"connection {connection!r} is not set up right: {problems}") from None

    def secrets(self) -> list[str]:
        held = (self.access_key_id, self.secret_access_key, self.session_token)
        return [
            secret.get_secret_value() for secret in held if secret is not None and secret.get_secret_value()
        ]

    def options(self, protocol: str) -> dict[str, Any]:
        """What fsspec is given for a URL of `protocol`. s3fs is told to skip its caches, so that a listing sees
        objects written since the last one."""
        options = dict(self.storage_options)
        if protocol not in _S3:
            return options
        options["skip_instance_cache"] = True
        options["use_listings_cache"] = False
        options["anon"] = self.anon
        if self.access_key_id is not None:
            options["key"] = self.access_key_id.get_secret_value()
        if self.secret_access_key is not None:
            options["secret"] = self.secret_access_key.get_secret_value()
        if self.session_token is not None:
            options["token"] = self.session_token.get_secret_value()
        if self.endpoint_url is not None:
            options["endpoint_url"] = self.endpoint_url
        if self.region is not None:
            options["client_kwargs"] = {**options.get("client_kwargs", {}), "region_name": self.region}
        return options


def _protocol(url: str) -> str:
    return url.split("://", 1)[0].lower() if "://" in url else "file"


def _key_text(key: Key) -> str:
    return key if isinstance(key, str) else canonical_json(list(key)).decode("utf-8")


def _utc(moment: Any) -> datetime:
    if isinstance(moment, str):
        moment = datetime.fromisoformat(moment)
    if isinstance(moment, (int, float)):
        return datetime.fromtimestamp(moment, UTC)
    if not isinstance(moment, datetime):
        raise PermanentError(f"{moment!r} is not a time an object was modified at")
    return moment.replace(tzinfo=UTC) if moment.utcoffset() is None else moment.astimezone(UTC)


def _hash(text: str, size: int = 20) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:size]


class _Store:
    """What both connectors share: opening the filesystem for a URL, and turning its errors into Feather's with the
    credentials masked."""

    spec: ClassVar[ConnectorSpec]
    connection: str

    def _fs(self, ctx: Context, url: str) -> tuple[Any, str, S3Config]:
        config = S3Config.of(ctx, self.connection)
        try:
            fs, path = fsspec.core.url_to_fs(url, **config.options(_protocol(url)))  # pyright: ignore
        except (ImportError, ValueError, TypeError) as error:
            raise self._error(error, config) from None
        if hasattr(fs, "invalidate_cache"):
            fs.invalidate_cache()
        return fs, cast(str, path), config

    def _error(self, error: BaseException, config: S3Config) -> Exception:
        message = f"{self.spec.name} on {self.connection!r}: {type(error).__name__}: {error}"
        for secret in config.secrets():
            message = message.replace(secret, _MASK)
        permanent = (
            PermissionError,
            FileNotFoundError,
            IsADirectoryError,
            NotADirectoryError,
            ValueError,
            TypeError,
            KeyError,
            ImportError,
            UnicodeError,
            pa.ArrowInvalid,
        )
        if isinstance(error, permanent):
            return PermanentError(message)
        if isinstance(error, (OSError, ConnectionError, TimeoutError)):
            return TransientError(message)
        return PermanentError(message)


class Files(_Store, connector.Source):
    """A source that reads every object `path` matches, in path order, as batches of `batch_rows`.

    `format` is `csv` (with `delimiter` and `encoding`; a UTF-8 file may start with a byte order mark), `json` (one
    array of objects), `jsonl` (an object a line) or `parquet`. A `by_stamp` read on `_object_mtime` reads only the
    objects modified at `since` or later. An object modified less than `stable_after` before the read is skipped.
    """

    spec = ConnectorSpec(
        name="s3.files",
        config=S3Config,
        strategies=frozenset({"replace", "by_stamp", "snapshot_diff"}),
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        path: str,
        format: Format,
        encoding: str = "utf-8",
        delimiter: str = ",",
        stable_after: str = "60s",
        batch_rows: int = DEFAULT_BATCH_ROWS,
    ) -> None:
        if format not in _SUFFIX:
            raise ValueError(f"format must be one of {sorted(_SUFFIX)}, not {format!r}")
        codecs.lookup(encoding)  # an unknown encoding fails here, not on the first read
        self.connection = connection
        self.path = path
        self.format: Format = format
        self.encoding = encoding
        self.delimiter = delimiter
        self.stable_after = parse_duration(stable_after)
        self.batch_rows = batch_rows

    def check(self, ctx: Context) -> CheckResult:
        try:
            fs, pattern, config = self._fs(ctx, self.path)
            try:
                fs.glob(pattern)
            except Exception as error:  # noqa: BLE001 - every failure is reported
                raise self._error(error, config) from None
        except (PermanentError, TransientError) as error:
            return CheckResult(ok=False, message=str(error))
        return CheckResult(ok=True)

    def discover(self, ctx: Context) -> list[StreamSchema]:
        req = ReadRequest("*", none())
        for batch in self.read(ctx, req):
            if isinstance(batch, pa.RecordBatch):
                return [StreamSchema(self.path, {f.name: str(f.type) for f in batch.schema})]
            rows = [row for row in batch if not isinstance(row, Unreadable)]
            if rows:
                return [StreamSchema(self.path, {k: type(v).__name__ for k, v in rows[0].items()})]
        return [StreamSchema(self.path, {PATH_FIELD: "string", MTIME_FIELD: "timestamp[us, tz=UTC]"})]

    def objects(self, ctx: Context, req: ReadRequest) -> list[tuple[str, datetime]]:
        """The objects a read takes, with their modified times, in path order."""
        fs, pattern, config = self._fs(ctx, self.path)
        try:
            found = cast(dict[str, dict[str, Any]], fs.glob(pattern, detail=True))
            listed = [
                (path, self._mtime(fs, path, info))
                for path, info in sorted(found.items())
                if info.get("type", "file") == "file" and not _bookkeeping(path)
            ]
        except Exception as error:  # noqa: BLE001 - mapped to Feather's errors
            raise self._error(error, config) from None
        latest = ctx.now - self.stable_after
        since = _utc(req.since) if req.since is not None and req.stamp_field == MTIME_FIELD else None
        return [(p, m) for p, m in listed if m <= latest and (since is None or m >= since)]

    @staticmethod
    def _mtime(fs: Any, path: str, info: Mapping[str, Any]) -> datetime:
        for name in ("LastModified", "mtime", "last_modified", "updated", "created"):
            if info.get(name) is not None:
                return _utc(info[name])
        return _utc(fs.modified(path))

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[Batch]:
        objects = self.objects(ctx, req)
        if not objects:
            return
        fs, _, config = self._fs(ctx, self.path)
        for path, mtime in objects:
            try:
                with fs.open(path, "rb") as stream:
                    for batch in self._batches(stream, path):
                        rows = _with_object(batch, path, mtime)
                        if req.partition == "*" or isinstance(rows, pa.RecordBatch) and not rows.num_rows:
                            yield rows
                        else:
                            yield _in_partition(rows, req)
            except (PermanentError, TransientError):
                raise
            except Exception as error:  # noqa: BLE001 - mapped to Feather's errors
                raise self._error(error, config) from None

    def _batches(self, stream: Any, path: str) -> Iterator[Batch]:
        if self.format == "parquet":
            yield from pq.ParquetFile(stream).iter_batches(batch_size=self.batch_rows)
        elif self.format == "csv":
            yield from self._csv(stream, path)
        elif self.format == "jsonl":
            yield from self._jsonl(stream, path)
        else:
            yield from self._json(stream, path)

    def _csv(self, stream: Any, path: str) -> Iterator[Batch]:
        bad: list[dict[str, Any] | Unreadable] = []

        def invalid(row: Any) -> Literal["skip"]:
            where = f"{path} line {row.number}" if row.number is not None else path
            reason = f"{where}: expected {row.expected_columns} fields, found {row.actual_columns}"
            bad.append(Unreadable(raw=row.text, reason=reason))
            return "skip"

        encoding = "utf-8-sig" if codecs.lookup(self.encoding).name == "utf-8" else self.encoding
        reader = pa_csv.open_csv(
            stream,
            read_options=pa_csv.ReadOptions(encoding=encoding),
            parse_options=pa_csv.ParseOptions(delimiter=self.delimiter, invalid_row_handler=invalid),
        )
        pending: list[pa.RecordBatch] = []
        size = 0
        for batch in reader:
            pending.append(batch)
            size += batch.num_rows
            if size >= self.batch_rows:
                yield from _split(pa.Table.from_batches(pending), self.batch_rows)
                pending, size = [], 0
            if bad:
                yield list(bad)
                bad.clear()
        if pending:
            yield from _split(pa.Table.from_batches(pending), self.batch_rows)
        if bad:
            yield list(bad)

    def _jsonl(self, stream: Any, path: str) -> Iterator[Batch]:
        text = io.TextIOWrapper(stream, encoding=self.encoding)
        rows: list[dict[str, Any] | Unreadable] = []
        for number, line in enumerate(text, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                rows.append(Unreadable(raw=line.rstrip("\r\n"), reason=f"{path} line {number}: {error.msg}"))
            else:
                if isinstance(row, dict):
                    rows.append(cast(dict[str, Any], row))
                else:
                    reason = f"{path} line {number}: not an object"
                    rows.append(Unreadable(raw=line.rstrip("\r\n"), reason=reason))
            if len(rows) >= self.batch_rows:
                yield rows
                rows = []
        if rows:
            yield rows

    def _json(self, stream: Any, path: str) -> Iterator[Batch]:
        try:
            document = json.load(io.TextIOWrapper(stream, encoding=self.encoding))
        except json.JSONDecodeError as error:
            raise PermanentError(f"{self.spec.name}: {path} is not JSON: {error}") from None
        if not isinstance(document, list):
            raise PermanentError(f"{self.spec.name}: {path} holds no array of objects")
        items = cast(list[Any], document)
        for start in range(0, len(items), self.batch_rows):
            chunk: list[dict[str, Any] | Unreadable] = []
            for n, item in enumerate(items[start : start + self.batch_rows], start=start):
                if isinstance(item, dict):
                    chunk.append(cast(dict[str, Any], item))
                else:
                    chunk.append(Unreadable(raw=json.dumps(item), reason=f"{path} item {n}: not an object"))
            yield chunk


def _bookkeeping(path: str) -> bool:
    parts = path.replace("\\", "/").split("/")
    return STAGING in parts or APPLIED in parts


def _split(table: pa.Table, rows: int) -> Iterator[pa.RecordBatch]:
    for batch in table.combine_chunks().to_batches(max_chunksize=rows):
        if batch.num_rows:
            yield batch


def _with_object(batch: Batch, path: str, mtime: datetime) -> Batch:
    if isinstance(batch, pa.RecordBatch):
        table = pa.Table.from_batches([batch])
        for name in (PATH_FIELD, MTIME_FIELD):
            if name in table.column_names:
                table = table.drop_columns([name])
        n = table.num_rows
        table = table.append_column(PATH_FIELD, pa.array([path] * n, pa.string()))
        table = table.append_column(MTIME_FIELD, pa.array([mtime] * n, pa.timestamp("us", tz="UTC")))
        return table.combine_chunks().to_batches()[0] if n else pa.RecordBatch.from_pylist([], table.schema)
    return [
        row if isinstance(row, Unreadable) else {**row, PATH_FIELD: path, MTIME_FIELD: mtime} for row in batch
    ]


def _in_partition(batch: Batch, req: ReadRequest) -> Batch:
    rows = batch.to_pylist() if isinstance(batch, pa.RecordBatch) else batch
    return [row for row in rows if isinstance(row, Unreadable) or req.contains(row)]


class PartitionFiles(_Store, connector.Sink):
    """A sink that writes one object per partition at `path_template`, whose `{partition}` is the partition's key
    (`_all` for a dataset without partitions).

    `replace_partition` replaces the partition's object with the version's records, and removes moved keys from the
    object of the partition they left (B6). A delete leaves the record out. `append` writes each chunk as an object of
    its own next to the partition's, and turns deletes down: files hold no deletes.
    """

    spec = ConnectorSpec(
        name="s3.partition_files",
        config=S3Config,
        modes=frozenset({"replace_partition", "append"}),
        maturity="beta",
    )

    def __init__(self, connection: str, *, path_template: str, format: Format) -> None:
        if "{partition}" not in path_template:
            raise ValueError(f"path_template {path_template!r} has no {{partition}}")
        if format not in _SUFFIX:
            raise ValueError(f"format must be one of {sorted(_SUFFIX)}, not {format!r}")
        self.connection = connection
        self.path_template = path_template
        self.format: Format = format
        self.root = path_template.split("{partition}", 1)[0].rsplit("/", 1)[0]

    def check(self, ctx: Context) -> CheckResult:
        try:
            fs, root, config = self._fs(ctx, self.root)
            try:
                if _protocol(self.root) in _S3:
                    bucket = root.split("/", 1)[0]
                    if not fs.exists(bucket):
                        raise FileNotFoundError(f"there is no bucket {bucket!r}")
                elif fs.exists(root):
                    fs.ls(root)
            except Exception as error:  # noqa: BLE001 - every failure is reported
                raise self._error(error, config) from None
        except (PermanentError, TransientError) as error:
            return CheckResult(ok=False, message=str(error))
        return CheckResult(ok=True)

    def url_of(self, partition: str) -> str:
        """The URL of a partition's object."""
        return self.path_template.replace("{partition}", "_all" if partition in ("*", "_") else partition)

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        if batch.mode not in self.spec.modes:
            raise PermanentError(f"{self.spec.name} does not apply mode {batch.mode!r}")
        fs, root, config = self._fs(ctx, self.root)
        marker = f"{root}/{APPLIED}/{_hash(batch.idempotency_key, 32)}.json"
        try:
            if fs.exists(marker):
                return _result_of(json.loads(fs.cat_file(marker)))
            if batch.mode == "append":
                result = self._append(ctx, fs, batch)
            else:
                result = self._replace(ctx, fs, root, batch)
            _put(fs, marker, json.dumps(_record_of(result)).encode("utf-8"))
        except (PermanentError, TransientError):
            raise
        except Exception as error:  # noqa: BLE001 - mapped to Feather's errors
            raise self._error(error, config) from None
        return result

    def _rows(self, batch: ApplyBatch, records: Sequence[ApplyRecord]) -> list[dict[str, Any]]:
        now = datetime.now(UTC).isoformat()
        return [
            {
                **(record.payload or {}),
                "_fc_key": _key_text(record.key),
                "_fc_partition": batch.partition,
                "_fc_version_id": batch.version_id,
                "_fc_op": record.op,
                "_fc_applied_at": now,
            }
            for record in records
        ]

    def _append(self, ctx: Context, fs: Any, batch: ApplyBatch) -> ApplyResult:
        deletes = [r for r in batch.records if r.op == "delete"]
        kept = [r for r in batch.records if r.op != "delete"]
        rejected = [
            Rejection(r.key, "DeleteRefused", "a files sink appends; it holds no deletes") for r in deletes
        ]
        if kept:
            url = self.url_of(batch.partition)
            stem, suffix = os.path.splitext(url)
            _, path = fsspec.core.url_to_fs(f"{stem}-{_hash(batch.idempotency_key, 12)}{suffix}")  # pyright: ignore
            _put(fs, cast(str, path), _encode(self._rows(batch, kept), self.format))
        return ApplyResult(len(kept), rejected)

    def _replace(self, ctx: Context, fs: Any, root: str, batch: ApplyBatch) -> ApplyResult:
        staging = f"{root}/{STAGING}/{_hash(f'{batch.route}:{batch.version_id}:{batch.partition}')}"
        kept = [r for r in batch.records if r.op != "delete"]
        rows = self._rows(batch, kept)
        for row, record in zip(rows, kept, strict=True):
            if record.moved_from is not None and record.moved_from != batch.partition:
                row["_fc_moved_from"] = record.moved_from
        _put(fs, f"{staging}/{batch.chunk_no:08d}.jsonl", _encode(rows, "jsonl"))
        if not batch.is_last:
            return ApplyResult(len(kept))
        staged: list[dict[str, Any]] = []
        for chunk in sorted(cast(list[str], fs.find(staging))):
            staged.extend(
                json.loads(line) for line in fs.cat_file(chunk).decode("utf-8").splitlines() if line
            )
        moved: dict[str, set[str]] = {}
        for row in staged:
            if "_fc_moved_from" in row:
                moved.setdefault(row.pop("_fc_moved_from"), set()).add(row["_fc_key"])
        _, target = fsspec.core.url_to_fs(self.url_of(batch.partition))  # pyright: ignore
        if staged:
            _put(fs, cast(str, target), _encode(staged, self.format))
        elif fs.exists(target):
            fs.rm(target)  # the slice became empty
        for partition, keys in moved.items():
            self._remove(fs, partition, keys)
        fs.rm(staging, recursive=True)
        return ApplyResult(len(kept))

    def _remove(self, fs: Any, partition: str, keys: set[str]) -> None:
        _, path = fsspec.core.url_to_fs(self.url_of(partition))  # pyright: ignore
        if not fs.exists(path):
            return
        with fs.open(path, "rb") as stream:
            rows = _decode(stream, self.format)
        left = [row for row in rows if str(row.get("_fc_key")) not in keys]
        if len(left) != len(rows):
            _put(fs, cast(str, path), _encode(left, self.format))


def _record_of(result: ApplyResult) -> dict[str, Any]:
    return {
        "applied": result.applied,
        "rejected": [
            {"key": r.key, "error_class": r.error_class, "message": r.message} for r in result.rejected
        ],
    }


def _result_of(record: Mapping[str, Any]) -> ApplyResult:
    rejected = [
        Rejection(r["key"] if isinstance(r["key"], str) else tuple(r["key"]), r["error_class"], r["message"])
        for r in record.get("rejected", [])
    ]
    return ApplyResult(int(record["applied"]), rejected)


def _put(fs: Any, path: str, data: bytes) -> None:
    """Write an object whole: one PUT on an object store, a temporary file and `os.replace` on a local disk."""
    if getattr(fs, "protocol", None) in ("file", ("file", "local")) or type(fs).__name__ == "LocalFileSystem":
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        temporary = os.path.join(directory, f".{os.path.basename(path)}.{uuid.uuid4().hex}.tmp")
        with open(temporary, "wb") as out:
            out.write(data)
        os.replace(temporary, path)
        return
    fs.pipe_file(path, data)


def _encode(rows: list[dict[str, Any]], format: Format) -> bytes:
    if format == "jsonl":
        return b"".join(json.dumps(row, default=str).encode("utf-8") + b"\n" for row in rows)
    if format == "json":
        return json.dumps(rows, default=str).encode("utf-8")
    table = pa.Table.from_pylist(rows)
    out = io.BytesIO()
    if format == "parquet":
        pq.write_table(table, out)
    else:
        pa_csv.write_csv(table, out)
    return out.getvalue()


def _decode(stream: Any, format: Format) -> list[dict[str, Any]]:
    if format == "parquet":
        return pq.read_table(stream).to_pylist()
    if format == "csv":
        return pa_csv.read_csv(stream).to_pylist()
    text = stream.read().decode("utf-8")
    if format == "json":
        return cast(list[dict[str, Any]], json.loads(text))
    return [json.loads(line) for line in text.splitlines() if line.strip()]
