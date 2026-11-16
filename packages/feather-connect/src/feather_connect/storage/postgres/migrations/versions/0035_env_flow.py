"""An environment's own `flow` entries: an installed flow's schedule, or whether it runs, set per environment with
no new package version. Additive: the kinds there were stay as they are.

Revision ID: 0035
Revises: 0034
"""

from alembic import op

revision = "0035"
down_revision = "0034"


def upgrade() -> None:
    op.execute(
        """alter table feather.env_config drop constraint env_config_kind_check,
        add constraint env_config_kind_check check (kind in ('connection', 'budget', 'setting', 'flow'))"""
    )


def downgrade() -> None:
    op.execute("delete from feather.env_config where kind = 'flow'")
    op.execute(
        """alter table feather.env_config drop constraint env_config_kind_check,
        add constraint env_config_kind_check check (kind in ('connection', 'budget', 'setting'))"""
    )
