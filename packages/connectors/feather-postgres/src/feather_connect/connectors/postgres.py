"""Postgres connectors: `Query`, a source that reads a table or a query, and `Table`, a sink that writes a table.

Both read their connection from `Context.config`: `dsn`, a secret under `auth`, and `schema`, the schema that
names are looked up in (`public` by default).

    connections:
      warehouse:
        connector: postgres.table
        schema: sales
        auth: {dsn: "${secret:env:WAREHOUSE_DSN}"}

Every table and column name goes to the server as a quoted identifier, never as text in the SQL, and every value as
a parameter. A connection that fails or drops raises `TransientError`; any other database error is permanent.

`Table` writes the record's fields and five columns of its own (ruling 1): `_fc_key`, `_fc_partition`,
`_fc_version_id`, `_fc_op` and `_fc_applied_at`, which it adds to the table when they are missing. It keeps each
chunk's result in `_feather_applied` in the same schema, in the same transaction as the write, so that a chunk sent
again, by this process or after a restart, changes nothing and gets the first answer back (S21).
"""

import hashlib
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar, LiteralString, cast

import psycopg
from psycopg import errors, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, ValidationError

from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyRecord,
    ApplyResult,
    CheckResult,
    ConnectorSpec,
    Key,
    ReadRequest,
    Rejection,
    Secret,
    StreamSchema,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.records import canonical_json

__all__ = ["FC_COLUMNS", "PostgresConfig", "Query", "Table"]

FC_COLUMNS: dict[str, LiteralString] = {
    "_fc_key": "text",
    "_fc_partition": "text",
    "_fc_version_id": "text",
    "_fc_op": "text",
    "_fc_applied_at": "timestamptz",
}
APPLIED_TABLE = "_feather_applied"
DEFAULT_BATCH_ROWS = 10_000
_CONNECT_TIMEOUT = 10
_ROW_ERRORS = (errors.IntegrityError, errors.DataError)  # what one bad row causes, and only that row


class PostgresConfig(BaseModel):
    """The settings of a Postgres connection. `dsn` comes from the connection's `auth`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dsn: Secret
    schema_name: str = "public"
    connect_timeout: int = _CONNECT_TIMEOUT

    @classmethod
    def of(cls, ctx: Context, connection: str) -> "PostgresConfig":
        raw = dict(ctx.config)
        if "schema" in raw:
            raw["schema_name"] = raw.pop("schema")
        try:
            return cls.model_validate(raw)
        except ValidationError as error:
            problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in error.errors())
            raise PermanentError(f"connection {connection!r} is not set up right: {problems}") from None


def _classified(error: psycopg.Error, what: str) -> Exception:
    """The connector's error for a database error: transient when the connection failed, else permanent."""
    if isinstance(error, psycopg.OperationalError | errors.AdminShutdown | errors.SerializationFailure):
        return TransientError(f"{what}: {type(error).__name__}: {str(error).strip()}")
    return PermanentError(f"{what}: {type(error).__name__}: {str(error).strip()}")


class _Postgres:
    spec: ClassVar[ConnectorSpec]
    connection: str

    @contextmanager
    def _connect(self, ctx: Context) -> Iterator[psycopg.Connection[Any]]:
        config = PostgresConfig.of(ctx, self.connection)
        try:
            conn = psycopg.connect(config.dsn.get_secret_value(), connect_timeout=config.connect_timeout)
        except psycopg.Error as error:
            raise _classified(error, f"connecting to {self.connection!r}") from None
        try:
            yield conn
        except psycopg.Error as error:
            raise _classified(error, f"{self.spec.name} on {self.connection!r}") from None
        finally:
            conn.close()

    def _schema(self, ctx: Context) -> str:
        return PostgresConfig.of(ctx, self.connection).schema_name

    def check(self, ctx: Context) -> CheckResult:
        try:
            with self._connect(ctx) as conn:
                conn.execute("select 1")
        except (PermanentError, TransientError) as error:
            return CheckResult(ok=False, message=str(error))
        return CheckResult(ok=True)


class Query(_Postgres, connector.Source):
    """A source that reads `table` (in the connection's schema) or the rows of `sql`, through a server-side cursor in
    batches of `batch_rows`.

    A read of one partition adds the partition's bounds as conditions, and a `by_stamp` read `stamp >= since`, both
    as parameters, so that the server does the filtering. `sql` is the flow's own code and is sent as it is.
    """

    spec = ConnectorSpec(
        name="postgres.query",
        config=PostgresConfig,
        strategies=frozenset({"by_stamp", "snapshot_diff", "replace"}),
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        sql: str | None = None,
        table: str | None = None,
        batch_rows: int = DEFAULT_BATCH_ROWS,
    ) -> None:
        if (sql is None) == (table is None):
            raise ValueError("give postgres.Query either sql or table")
        self.connection = connection
        self.sql = sql
        self.table = table
        self.batch_rows = batch_rows
        self.last_statement: tuple[str, list[Any]] | None = (
            None  # what the last read sent, for tests to look at
        )

    def _from(self, ctx: Context) -> sql.Composable:
        if self.sql is not None:
            return sql.SQL("({}) as q").format(sql.SQL(self.sql))  # pyright: ignore[reportArgumentType]
        assert self.table is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema(ctx)), sql.Identifier(self.table))

    def discover(self, ctx: Context) -> list[StreamSchema]:
        """Offer one stream, named by the table or `query`, with each column's Postgres type."""
        statement = sql.SQL("select * from {} limit 0").format(self._from(ctx))
        with self._connect(ctx) as conn, conn.cursor() as cursor:
            cursor.execute(statement)
            names = conn.adapters.types
            fields = {
                column.name: (names.get(column.type_code).name if names.get(column.type_code) else "unknown")  # pyright: ignore[reportOptionalMemberAccess]
                for column in cursor.description or ()
            }
        return [StreamSchema(self.table or "query", fields)]

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        conditions: list[sql.Composable] = []
        params: list[Any] = []
        if req.partition != "*":
            for field, bound in req.scheme.bounds(req.partition).items():
                if bound.low == bound.high:
                    conditions.append(sql.SQL("{} = %s").format(sql.Identifier(field)))
                    params.append(bound.low)
                else:
                    conditions.append(
                        sql.SQL("{} >= %s and {} < %s").format(sql.Identifier(field), sql.Identifier(field))
                    )
                    params.extend([bound.low, bound.high])
        if req.since is not None and req.stamp_field is not None:
            conditions.append(sql.SQL("{} >= %s").format(sql.Identifier(req.stamp_field)))
            params.append(req.since)
        statement = sql.SQL("select * from {}").format(self._from(ctx))
        if conditions:
            statement = sql.SQL("{} where {}").format(statement, sql.SQL(" and ").join(conditions))
        with self._connect(ctx) as conn:
            self.last_statement = (statement.as_string(conn), params)
            with conn.transaction(), conn.cursor(name="feather_read", row_factory=dict_row) as cursor:
                cursor.itersize = self.batch_rows
                cursor.execute(statement, params)
                while rows := cursor.fetchmany(self.batch_rows):
                    yield rows


def _key_text(key: Key) -> str:
    return key if isinstance(key, str) else canonical_json(list(key)).decode("utf-8")


def _value(value: Any) -> Any:
    return Jsonb(value) if isinstance(value, dict | list) else value


class Table(_Postgres, connector.Sink):
    """A sink that writes `table`, in the connection's schema, by `key_columns`.

    `upsert` inserts or updates by key (`insert ... on conflict do update`), and a delete removes the key. `append`
    inserts every record. `replace_partition` loads each chunk into a staging table of the version and, on
    the last chunk, swaps the slice in one transaction: the rows whose `_fc_partition` is the batch's are deleted, and
    so are the rows of keys that moved from another partition, then the staged rows go in (B6). A slice that became
    empty arrives as one batch with no records, and empties the partition. `partition_columns` is for the index on
    the table only.

    A record with a field the table lacks fails the chunk with an error naming the column. A row that breaks a
    constraint or doesn't fit a column's type is rejected on its own; the rest of the batch is written.
    """

    spec = ConnectorSpec(
        name="postgres.table",
        config=PostgresConfig,
        modes=frozenset({"upsert", "append", "replace_partition"}),
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        table: str,
        key_columns: Sequence[str],
        partition_columns: Sequence[str] = (),
    ) -> None:
        if not key_columns:
            raise ValueError("postgres.Table needs key_columns")
        self.connection = connection
        self.table = table
        self.key_columns = tuple(key_columns)
        self.partition_columns = tuple(partition_columns)

    # Names.

    def _name(self, schema: str, table: str) -> sql.Composable:
        return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))

    def _stage_name(self, batch: ApplyBatch) -> str:
        replays = batch.idempotency_key.rsplit(":", 1)[-1]
        digest = hashlib.sha256(f"{batch.route}\0{batch.version_id}\0{replays}".encode()).hexdigest()[:20]
        return f"_feather_stage_{digest}"

    # Apply.

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        if batch.mode not in self.spec.modes:
            raise PermanentError(f"{self.spec.name} can't apply in mode {batch.mode!r}")
        schema = self._schema(ctx)
        with self._connect(ctx) as conn:
            columns = self._prepare(conn, schema)
            with conn.transaction():
                done = self._applied_before(conn, schema, batch.idempotency_key)
                if done is not None:
                    return done
                if batch.mode == "replace_partition":
                    result = self._stage(conn, schema, columns, batch)
                else:
                    result = self._write(conn, schema, columns, batch)
                self._remember(conn, schema, batch.idempotency_key, result)
            return result

    def _prepare(self, conn: psycopg.Connection[Any], schema: str) -> dict[str, str]:
        """Make sure of `_feather_applied` and the table's `_fc_*` columns, and return the table's columns by name."""
        with conn.transaction():
            conn.execute(
                sql.SQL(
                    "create table if not exists {} (idempotency_key text primary key, applied int not null, "
                    "rejected jsonb not null, at timestamptz not null default now())"
                ).format(self._name(schema, APPLIED_TABLE))
            )
            columns = self._columns(conn, schema, self.table)
            if not columns:
                raise PermanentError(f"{self.spec.name}: there is no table {schema}.{self.table}")
            for name, kind in FC_COLUMNS.items():
                if name not in columns:
                    conn.execute(
                        sql.SQL("alter table {} add column if not exists {} {}").format(
                            self._name(schema, self.table), sql.Identifier(name), sql.SQL(kind)
                        )
                    )
                    columns[name] = kind
        return columns

    @staticmethod
    def _columns(conn: psycopg.Connection[Any], schema: str, table: str) -> dict[str, str]:
        rows = conn.execute(
            """select column_name, data_type from information_schema.columns
               where table_schema = %s and table_name = %s order by ordinal_position""",
            (schema, table),
        ).fetchall()
        return {str(name): str(kind) for name, kind in rows}

    def _applied_before(self, conn: psycopg.Connection[Any], schema: str, key: str) -> ApplyResult | None:
        row = conn.execute(
            sql.SQL("select applied, rejected from {} where idempotency_key = %s").format(
                self._name(schema, APPLIED_TABLE)
            ),
            (key,),
        ).fetchone()
        if row is None:
            return None
        rejected = [
            Rejection(r["key"], r["error_class"], r["message"]) for r in cast(list[dict[str, Any]], row[1])
        ]
        return ApplyResult(int(row[0]), rejected)

    def _remember(self, conn: psycopg.Connection[Any], schema: str, key: str, result: ApplyResult) -> None:
        rejected = [
            {
                "key": r.key if isinstance(r.key, str) else list(r.key),
                "error_class": r.error_class,
                "message": r.message,
            }
            for r in result.rejected
        ]
        conn.execute(
            sql.SQL("insert into {} (idempotency_key, applied, rejected) values (%s, %s, %s)").format(
                self._name(schema, APPLIED_TABLE)
            ),
            (key, result.applied, Jsonb(rejected)),
        )

    def _fields(self, record: ApplyRecord, columns: Mapping[str, str]) -> dict[str, Any]:
        payload = cast(Mapping[str, Any], record.payload)
        missing = [name for name in payload if name not in columns]
        if missing:
            raise PermanentError(
                f"{self.spec.name}: table {self.table} has no column {', '.join(missing)} "
                f"(record {_key_text(record.key)})"
            )
        return dict(payload)

    def _fc(self, batch: ApplyBatch, record: ApplyRecord, now: datetime) -> dict[str, Any]:
        return {
            "_fc_key": _key_text(record.key),
            "_fc_partition": batch.partition,
            "_fc_version_id": batch.version_id,
            "_fc_op": record.op,
            "_fc_applied_at": now,
        }

    def _key_values(self, record: ApplyRecord) -> list[Any]:
        if isinstance(record.key, tuple):
            values = list(record.key)
        else:
            values = [record.key]
        if len(values) != len(self.key_columns):
            raise PermanentError(f"record key {record.key!r} doesn't fit key_columns {self.key_columns}")
        return values

    def _write(
        self, conn: psycopg.Connection[Any], schema: str, columns: Mapping[str, str], batch: ApplyBatch
    ) -> ApplyResult:
        now = datetime.now(UTC)
        statements = [self._statement(schema, batch, record, columns, now) for record in batch.records]
        try:
            with conn.transaction(), conn.cursor() as cursor:  # the fast path: all of it at once
                for statement, params in statements:
                    cursor.execute(statement, params)
            return ApplyResult(len(statements))
        except _ROW_ERRORS:
            pass
        rejected: list[Rejection] = []  # one bad row somewhere: each row on its own, in a savepoint
        for record, (statement, params) in zip(batch.records, statements, strict=True):
            try:
                with conn.transaction():
                    conn.execute(statement, params)
            except _ROW_ERRORS as error:
                rejected.append(Rejection(record.key, type(error).__name__, str(error).strip()))
        return ApplyResult(len(statements) - len(rejected), rejected)

    def _statement(
        self,
        schema: str,
        batch: ApplyBatch,
        record: ApplyRecord,
        columns: Mapping[str, str],
        now: datetime,
    ) -> tuple[sql.Composed, list[Any]]:
        target = self._name(schema, self.table)
        if record.op == "delete" or record.payload is None:
            where = sql.SQL(" and ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in self.key_columns
            )
            return sql.SQL("delete from {} where {}").format(target, where), self._key_values(record)
        row = {**self._fields(record, columns), **self._fc(batch, record, now)}
        names = list(row)
        insert = sql.SQL("insert into {} ({}) values ({})").format(
            target,
            sql.SQL(", ").join(map(sql.Identifier, names)),
            sql.SQL(", ").join(sql.Placeholder() * len(names)),
        )
        if batch.mode == "upsert":
            updates = [n for n in names if n not in self.key_columns]
            insert = sql.SQL("{} on conflict ({}) do update set {}").format(
                insert,
                sql.SQL(", ").join(map(sql.Identifier, self.key_columns)),
                sql.SQL(", ").join(sql.SQL("{0} = excluded.{0}").format(sql.Identifier(n)) for n in updates),
            )
        return insert, [_value(row[n]) for n in names]

    def _stage(
        self, conn: psycopg.Connection[Any], schema: str, columns: Mapping[str, str], batch: ApplyBatch
    ) -> ApplyResult:
        target = self._name(schema, self.table)
        stage = self._name(schema, self._stage_name(batch))
        conn.execute(
            sql.SQL(
                "create table if not exists {} (like {} including defaults, "
                "_fc_chunk int not null, _fc_moved_from text)"
            ).format(stage, target)
        )
        conn.execute(sql.SQL("delete from {} where _fc_chunk = %s").format(stage), (batch.chunk_no,))
        now = datetime.now(UTC)
        upserts = [r for r in batch.records if r.op != "delete" and r.payload is not None]
        for record in upserts:
            row = {
                **self._fields(record, columns),
                **self._fc(batch, record, now),
                "_fc_chunk": batch.chunk_no,
                "_fc_moved_from": record.moved_from,
            }
            names = list(row)
            try:
                with conn.transaction():
                    conn.execute(
                        sql.SQL("insert into {} ({}) values ({})").format(
                            stage,
                            sql.SQL(", ").join(map(sql.Identifier, names)),
                            sql.SQL(", ").join(sql.Placeholder() * len(names)),
                        ),
                        [_value(row[n]) for n in names],
                    )
            except _ROW_ERRORS as error:  # a replace can't leave a record out
                raise PermanentError(
                    f"{self.spec.name}: record {_key_text(record.key)} can't be written, and replacing the slice "
                    f"can't leave it out: {type(error).__name__}: {str(error).strip()}"
                ) from None
        if batch.is_last:
            self._swap(conn, schema, columns, batch, stage)
        return ApplyResult(len(upserts))

    def _swap(
        self,
        conn: psycopg.Connection[Any],
        schema: str,
        columns: Mapping[str, str],
        batch: ApplyBatch,
        stage: sql.Composable,
    ) -> None:
        target = self._name(schema, self.table)
        keys = sql.SQL(" and ").join(
            sql.SQL("t.{0} = s.{0}").format(sql.Identifier(c)) for c in self.key_columns
        )
        conn.execute(sql.SQL("delete from {} where _fc_partition = %s").format(target), (batch.partition,))
        conn.execute(
            sql.SQL(
                "delete from {} t using {} s where s._fc_moved_from is not null "
                "and t._fc_partition = s._fc_moved_from and {}"
            ).format(target, stage, keys)
        )
        names = sql.SQL(", ").join(map(sql.Identifier, columns))
        conn.execute(sql.SQL("insert into {} ({}) select {} from {}").format(target, names, names, stage))
        conn.execute(sql.SQL("drop table {}").format(stage))
