"""The Alembic environment. `migrate` hands in the connection, so there is no offline mode."""

from alembic import context
from sqlalchemy import Connection, text

_LOCK = 7_262_410_001  # serialises concurrent migrations of one database


def run() -> None:
    connection: Connection = context.config.attributes["connection"]
    connection.execute(text("select pg_advisory_xact_lock(:key)"), {"key": _LOCK})
    connection.execute(text("create schema if not exists feather"))
    context.configure(connection=connection, version_table_schema="feather")
    with context.begin_transaction():
        context.run_migrations()


run()
