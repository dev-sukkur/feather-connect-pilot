"""Create or upgrade the Postgres schema."""

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url


def migrate(database_url: str) -> None:
    """Bring the database up to the latest revision, creating schema `feather` first. Safe to repeat."""
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    engine = create_engine(make_url(database_url).set(drivername="postgresql+psycopg"))
    try:
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()
