"""What daily stream runs need: one run per flow and day, and versions that know their tick.

A day that two racing replicas gave two runs keeps both. The older one takes the day's key and the other stays a plain
run with the versions attached to it.

Revision ID: 0007
Revises: 0006
"""

from alembic import op

revision = "0007"
down_revision = "0006"

_STATEMENTS = [
    "alter table feather.run add column stream_day date",
    """update feather.run r set stream_day = (r.params->>'day')::date
       where r.trigger = 'stream' and r.run_id = (
           select min(o.run_id) from feather.run o
           where o.tenant_id = r.tenant_id and o.flow = r.flow and o.trigger = 'stream'
             and o.params->>'day' = r.params->>'day')""",
    """create unique index run_stream_day_idx on feather.run (tenant_id, flow, stream_day)
        where stream_day is not null""",
    "alter table feather.version add column tick text",
    """create index version_tick_idx on feather.version (tenant_id, run_id, dataset, tick)
        where tick is not null""",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)
