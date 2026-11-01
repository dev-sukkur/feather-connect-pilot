"""Create or upgrade the Postgres schema."""

from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    return config


def at_head(database_url: str) -> bool:
    """Return whether the schema is at the latest revision. Raises `psycopg.OperationalError` if the database is away."""
    head = ScriptDirectory.from_config(_config()).get_current_head()
    with psycopg.connect(database_url, connect_timeout=2) as conn:
        try:
            row = conn.execute("select version_num from feather.alembic_version").fetchone()
        except psycopg.errors.UndefinedTable:  # nothing has been migrated
            return False
    return row is not None and row[0] == head


def migrate(database_url: str) -> None:
    """Bring the database up to the latest revision, creating schema `feather` first. Safe to repeat."""
    config = _config()
    engine = create_engine(make_url(database_url).set(drivername="postgresql+psycopg"))
    try:
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()
