"""Uninstalling a package version (flow packages §6, task 10): its row stays, `uninstalled`, as its history does, but
its wheel is deleted when no installed version has the same sha256. So a version's sha256 no longer has to name a
wheel the store keeps: the foreign key from `package_version` to `package_wheel` goes, and the sha256 stays as the
record of what was installed.

Revision ID: 0024
Revises: 0023
"""

from alembic import op

revision = "0024"
down_revision = "0023"


def upgrade() -> None:
    op.execute(
        "alter table feather.package_version drop constraint if exists package_version_tenant_id_sha256_fkey"
    )


def downgrade() -> None:
    op.execute(
        """alter table feather.package_version add constraint package_version_tenant_id_sha256_fkey
           foreign key (tenant_id, sha256) references feather.package_wheel (tenant_id, sha256) not valid"""
    )
