"""Where a package version came from and how it served (flow packages §6, §11; the P5 review): the install checks it
passed, the environment it was promoted from and by whom, the key that signed it, and, for each flow of a version, when
it last served, so a rollback goes back to the version that actually did.

Revision ID: 0025
Revises: 0024
"""

from alembic import op

revision = "0025"
down_revision = "0024"


def upgrade() -> None:
    op.execute(
        """alter table feather.package_version
               add column checks jsonb not null default '[]'::jsonb,
               add column promoted_from text,
               add column promoted_by text,
               add column signed_by text"""
    )
    op.execute("alter table feather.flow_activation add column last_active_at timestamptz")
    op.execute("update feather.flow_activation set last_active_at = changed_at where state = 'active'")


def downgrade() -> None:
    op.execute("alter table feather.flow_activation drop column last_active_at")
    op.execute(
        """alter table feather.package_version
               drop column checks, drop column promoted_from, drop column promoted_by, drop column signed_by"""
    )
