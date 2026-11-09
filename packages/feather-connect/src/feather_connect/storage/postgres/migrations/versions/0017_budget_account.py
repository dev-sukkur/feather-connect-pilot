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
    # The budgets of an account are found by it when one is defined, to check they agree on its windows.
    op.execute("create index budget_account_idx on feather.budget (account) where account is not null")


def downgrade() -> None:
    # The account windows go, and with them what was used of them: the budgets that named an account have no windows
    # left, so they must be defined again (a project load does) and start full. Without `account`, the windows can't
    # be found any more, and a '*' row would be a tenant that doesn't exist.
    op.execute("drop index feather.budget_account_idx")
    op.execute("delete from feather.budget_window where tenant_id = '*'")
    op.execute("alter table feather.budget drop column account")
