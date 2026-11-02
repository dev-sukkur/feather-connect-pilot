"""Bounded reads of a run. A stream run gathers thousands of tasks and versions a day, so nothing done per tick, per apply
or per sweep may walk them. Each index here serves one such read.

`ref` is indexed in the C collation because a prefix can only be looked up as a range in byte order.

Revision ID: 0008
Revises: 0007
"""

from alembic import op

revision = "0008"
down_revision = "0007"

_STATEMENTS = [
    """create index version_run_sealed_idx on feather.version (tenant_id, run_id, dataset, seal_seq)
        where status = 'sealed'""",
    "create index version_staged_idx on feather.version (tenant_id, stage_seq) where status = 'staged'",
    'create index task_ref_prefix_idx on feather.task (tenant_id, run_id, ref collate "C")',
    "create index task_kind_idx on feather.task (tenant_id, run_id, kind, seq)",
    "create index task_running_idx on feather.task (tenant_id, run_id, kind, seq) where status = 'running'",
    "create index task_failed_idx on feather.task (tenant_id, run_id, kind, seq) where status = 'failed'",
    "create index task_retried_idx on feather.task (tenant_id, run_id) where attempt > 1",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)
