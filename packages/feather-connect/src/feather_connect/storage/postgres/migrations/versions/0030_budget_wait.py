"""The tasks that wait for a budget (M2 plan 2 O1), for the Budgets page: one row a task, kept a day past its wait.

Revision ID: 0030
Revises: 0029
"""

from alembic import op

revision = "0030"
down_revision = "0029"


def upgrade() -> None:
    op.execute(
        """create table feather.budget_wait (
            tenant_id text not null,
            budget text not null,
            task_ref text not null,
            run_id text,
            first_at timestamptz not null,
            until timestamptz not null,
            waits int not null,
            primary key (tenant_id, budget, task_ref)
        )"""
    )
    op.execute("create index budget_wait_until_idx on feather.budget_wait (tenant_id, until)")


def downgrade() -> None:
    op.execute("drop table feather.budget_wait")
