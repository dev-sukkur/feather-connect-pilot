"""The names an M1 project's flows, datasets and routes had before the upgrade made it a package (flow packages §13,
task 12): each now carries the package prefix, `<package>/<name>`, and this table says what an old name became, so that
a link, a bookmark, an API call or a partner's webhook with the old name still finds it.

Revision ID: 0026
Revises: 0025
"""

from alembic import op

revision = "0026"
down_revision = "0025"


def upgrade() -> None:
    op.execute(
        """create table feather.renamed (
            tenant_id text not null,
            kind text not null check (kind in ('flow', 'dataset', 'route')),
            old_name text not null,
            new_name text not null,
            package text not null,
            renamed_at timestamptz not null,
            primary key (tenant_id, kind, old_name)
        )"""
    )


def downgrade() -> None:
    op.execute("drop table feather.renamed")
