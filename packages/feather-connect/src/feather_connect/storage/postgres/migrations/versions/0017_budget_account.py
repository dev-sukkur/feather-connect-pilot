"""A budget can belong to a vendor account wider than one tenant (ADR-0013 rule 8): `budget.account` names it, and
the account's windows are kept once in `budget_window` under the tenant id `*`, so that every tenant whose budget names
the account takes from them.

Revision ID: 0017
Revises: 0016
"""

from alembic import op

revision = "0017"
down_revision = "0016"


def upgrade() -> None:
    op.execute("alter table feather.budget add column account text")


def downgrade() -> None:
    op.execute("delete from feather.budget_window where tenant_id = '*'")
    op.execute("alter table feather.budget drop column account")
