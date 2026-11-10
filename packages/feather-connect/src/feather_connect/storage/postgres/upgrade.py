"""The rename of the upgrade from the project mode (flow packages §13, task 12): an M1 project's flows, datasets and
routes become its package's, `<package>/<name>`, and so does everything the store keeps under those names, in one
transaction.

It renames runs, the schedule's ticks, read versions and slices, watermarks, deliveries with their attempts and
responses, the ledger's partition status, record state and keys, quarantine, the webhook inbox, the task journal and
the pauses (`override`), and records each old name in `feather.renamed`, so an old link or webhook still finds it. Runs
that aren't over (queued, running, or with a delivery not yet applied) are tagged with the package and version, so
that version's host takes them on (`take_queued`, `resubmit_lost`). The audit log is left as it was: it says what was
done, under the names of the time.

The schedule and the runs are locked first, so no tick or run is written under an old name meanwhile: the scheduler
waits for the commit. A crash before the commit leaves every old name as it was, and running it again after a commit
finds nothing left to rename, so it is safe twice.
"""

from collections.abc import Mapping
from datetime import datetime

import psycopg
from psycopg import sql

__all__ = ["NAMED", "UpgradeError", "rename_to_package"]

# (table, column, kind): every column that holds a flow's, a dataset's or a route's name.
NAMED: tuple[tuple[str, str, str], ...] = (
    ("run", "flow", "flow"),
    ("schedule", "flow", "flow"),
    ("version", "dataset", "dataset"),
    ("run_slice", "dataset", "dataset"),
    ("dataset_watermark", "dataset", "dataset"),
    ("delivery", "dataset", "dataset"),
    ("delivery", "route", "route"),
    ("delivery_attempt", "route", "route"),
    ("delivery_response", "route", "route"),
    ("partition_status", "dataset", "dataset"),
    ("partition_status", "route", "route"),
    ("record_state", "dataset", "dataset"),
    ("record_key", "dataset", "dataset"),
    ("quarantine", "dataset", "dataset"),
    ("quarantine", "route", "route"),
    ("inbox", "dataset", "dataset"),
    ("inbox_event_id", "dataset", "dataset"),
    ("task", "dataset", "dataset"),
    ("task", "route", "route"),
)
_KINDS = ("flow", "dataset", "route")


class UpgradeError(Exception):
    """The rename can't be done: the message says why. Nothing was renamed."""


def rename_to_package(
    url: str,
    package: str,
    version: str,
    names: Mapping[str, Mapping[str, str]],
    at: datetime,
    *,
    tenant: str = "default",
    lock_timeout_s: float = 30,
) -> dict[str, int]:
    """Rename, in one transaction, every old name in `names` (by kind: `flow`, `dataset`, `route`; old to new) to
    its new one, and tag the runs that aren't over with `package` and `version`. Returns how many rows each table had
    renamed (`table.column`), with `tagged` for the runs tagged; all zero when it was done before."""
    pairs = {kind: dict(names.get(kind, {})) for kind in _KINDS}
    counts: dict[str, int] = {}
    try:
        with psycopg.connect(url) as conn:
            conn.execute(
                sql.SQL("set local lock_timeout = {}").format(sql.Literal(f"{int(lock_timeout_s * 1000)}ms"))
            )
            # The scheduler and anything that starts a run wait for the commit: no tick or run under an old name.
            conn.execute("lock table feather.schedule, feather.run in exclusive mode")
            for table, column, kind in NAMED:
                olds, news = list(pairs[kind]), list(pairs[kind].values())
                if not olds:
                    continue
                done = conn.execute(
                    sql.SQL(
                        """update feather.{table} t set {column} = m.new
                           from unnest(%s::text[], %s::text[]) as m(old, new)
                           where t.tenant_id = %s and t.{column} = m.old"""
                    ).format(table=sql.Identifier(table), column=sql.Identifier(column)),
                    (olds, news, tenant),
                )
                counts[f"{table}.{column}"] = done.rowcount
            targets = {
                f"{kind}:{old}": f"{kind}:{new}" for kind in _KINDS for old, new in pairs[kind].items()
            }
            if targets:
                done = conn.execute(
                    """update feather.override o set target = m.new
                       from unnest(%s::text[], %s::text[]) as m(old, new)
                       where o.tenant_id = %s and o.target = m.old""",
                    (list(targets), list(targets.values()), tenant),
                )
                counts["override.target"] = done.rowcount
            flows = list(pairs["flow"].values())
            tagged = conn.execute(
                """update feather.run r
                   set params = r.params || jsonb_build_object('package', %s::text, 'version', %s::text)
                   where r.tenant_id = %s and r.flow = any(%s) and r.params->>'package' is null
                     and (r.status in ('queued', 'running')
                          or exists (select 1 from feather.version v
                                     join feather.delivery d on d.tenant_id = v.tenant_id and d.version_id = v.version_id
                                     where v.tenant_id = r.tenant_id and v.run_id = r.run_id
                                       and d.status <> 'applied'))""",
                (package, version, tenant, flows),
            )
            counts["tagged"] = tagged.rowcount
            for kind in _KINDS:
                for old, new in pairs[kind].items():
                    conn.execute(
                        """insert into feather.renamed (tenant_id, kind, old_name, new_name, package, renamed_at)
                           values (%s, %s, %s, %s, %s, %s) on conflict do nothing""",
                        (tenant, kind, old, new, package, at),
                    )
    except psycopg.errors.UniqueViolation as error:
        raise UpgradeError(
            "a renamed row would take the place of one the package has already under its new name: "
            f"{error.diag.message_detail or error}"
        ) from None
    except psycopg.errors.LockNotAvailable:
        raise UpgradeError(
            f"the schedule or the runs stayed locked for {lock_timeout_s:g} s: stop the server and the workers first"
        ) from None
    return counts
