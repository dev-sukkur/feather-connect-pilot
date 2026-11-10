"""The database role package hosts connect as (flow packages §11): `feather_host` by default.

A package host runs code of the package's authors, so what it can reach in the database is what that code can. The
host role can read and write what running flows needs, and nothing that holds a credential: no access at all to
`secret`, `secret_key`, `app_user`, `api_token` and `registry` (whose rows say how to reach a registry), and only
`insert` on `audit`, so a host can record what it did but neither read nor change the record. It owns nothing, so it
can't change the schema, nor make the partitions that a day's or a month's rows go to: the supervisor makes them ahead
of time as the owner (`ensure_partitions_ahead`). It gets the DBOS system schema's tables as DBOS grants them to an application role.

    python -m feather_connect.storage.postgres.host_role --database-url OWNER_URL [--role feather_host]

creates the role if it isn't there (with no login: give it one with `alter role feather_host login password '...'`)
and grants it all this. A supervisor whose `FEATHER_HOST_DATABASE_URL` is set grants it again each time it starts,
after the migrations, so a table that a migration adds is covered.
"""

import argparse
from collections.abc import Sequence
from typing import Any
from urllib.parse import unquote, urlsplit

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

__all__ = ["DENIED", "INSERT_ONLY", "ensure_partitions_ahead", "grant_host_role", "role_of"]

SCHEMA = "feather"
DENIED = ("secret", "secret_key", "app_user", "api_token", "registry", "alembic_version")
INSERT_ONLY = ("audit",)


def role_of(database_url: str) -> str:
    """The role a URL or keyword DSN connects as."""
    if "://" in database_url:
        user = urlsplit(database_url).username
    else:
        user = conninfo_to_dict(database_url).get("user")
    if not user:
        raise ValueError("the host database URL names no user: it must connect as the host role")
    return unquote(str(user))


def grant_host_role(owner_url: str, role: str = "feather_host", *, create: bool = False) -> None:
    """Give `role` what a package host needs and nothing more, connected as the owner of the schema. With `create`,
    make the role first if it isn't there, with no login."""
    from dbos import DBOS

    from feather_engine_dbos.engine import system_database_url

    who = sql.Identifier(role)
    with psycopg.connect(owner_url, autocommit=True) as conn:
        exists = conn.execute("select 1 from pg_roles where rolname = %s", (role,)).fetchone()
        if exists is None:
            if not create:
                raise LookupError(f"the role {role} doesn't exist: create it, as the Operating Guide says")
            conn.execute(sql.SQL("create role {} nologin").format(who))
        tables = [
            r[0]
            for r in conn.execute(
                "select tablename from pg_tables where schemaname = %s order by tablename", (SCHEMA,)
            ).fetchall()
        ]
        database = conn.execute("select current_database()").fetchone()
        assert database is not None
        with conn.transaction():
            conn.execute(
                sql.SQL("grant connect on database {} to {}").format(sql.Identifier(database[0]), who)
            )
            conn.execute(sql.SQL("grant usage on schema {} to {}").format(sql.Identifier(SCHEMA), who))
            for table in tables:
                name = sql.Identifier(SCHEMA, table)
                conn.execute(sql.SQL("revoke all on {} from {}").format(name, who))
                if table in DENIED:
                    continue
                rights = "insert" if table in INSERT_ONLY else "select, insert, update, delete"
                conn.execute(sql.SQL("grant {} on {} to {}").format(sql.SQL(rights), name, who))
            conn.execute(
                sql.SQL("grant usage, select on all sequences in schema {} to {}").format(
                    sql.Identifier(SCHEMA), who
                )
            )
    DBOS.migrate(system_database_url(owner_url), application_role=role)


def ensure_partitions_ahead(store: Any, spool: Any, days: int = 7) -> None:
    """Make, as the schema's owner, the partitions that hosts write to for the next `days` days: the spool's and the
    inbox's daily ones, and the task logs' and delivery attempts' monthly ones. A host role can't make them."""
    spool.ensure_partitions(days)
    store.ensure_partitions(days)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m feather_connect.storage.postgres.host_role", description=__doc__
    )
    parser.add_argument("--database-url", required=True, help="a URL that connects as the schema's owner")
    parser.add_argument("--role", default="feather_host")
    args = parser.parse_args(argv)
    grant_host_role(args.database_url, args.role, create=True)
    print(f"{args.role} can now run package hosts: give it a login and set FEATHER_HOST_DATABASE_URL")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
