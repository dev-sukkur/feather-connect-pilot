"""Package hosts and the runs they look for (flow packages §7.1, tasks 6 and 9): how each worker's host of a version
stands, crash loops included, so the Packages page can show them; and an index on the package version a queued or
running run is tagged with, which a supervisor and a host read every few seconds. `tenant_id` leads both (ADR-0013).

Revision ID: 0023
Revises: 0022
"""

from alembic import op

revision = "0023"
down_revision = "0022"


def upgrade() -> None:
    op.execute(
        """create table feather.package_host (
            tenant_id text not null,
            worker_id text not null,
            package text not null,
            version text not null,
            state text not null check (state in ('starting', 'ready', 'crash_loop', 'failed', 'stopped')),
            restarts int not null,
            error text,
            changed_at timestamptz not null,
            primary key (tenant_id, worker_id, package, version)
        )"""
    )
    op.execute(
        """create index run_tagged_idx on feather.run
               (tenant_id, (params->>'package'), (params->>'version'), created_at)
           where status in ('queued', 'running')"""
    )


def downgrade() -> None:
    op.execute("drop index feather.run_tagged_idx")
    op.execute("drop table feather.package_host")
