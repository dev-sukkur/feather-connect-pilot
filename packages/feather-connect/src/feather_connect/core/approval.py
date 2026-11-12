"""Approvals on a route (design §5.3, §6.5; M2 plan 2 stage A1).

A route with an approval holds a delivery whose records need one before anything of it is applied:

    fc.to(sap_cpi.Post("sap"), approval=fc.approval.when(lambda r: r["total"] > 500_000))

An approver approves the delivery, which then applies as it is, or rejects it, which skips it. The whole delivery
waits, not single records: a posting is all or nothing for the business, and the partition's order holds (I2).
A delete record never needs approval: it carries no payload to judge.
"""

from collections.abc import Callable
from dataclasses import dataclass

from feather_connect.core.records import Record

__all__ = ["APPROVED", "Approval", "always", "when"]

APPROVED = "approved"  # the reason, at its start, of the move from held to pending that an approval makes


@dataclass(frozen=True)
class Approval:
    """Which records of a delivery need an approver's yes. `needs` is None when every record does."""

    needs: Callable[[Record], bool] | None = None
    reason: str | None = None

    def wanted(self, record: Record) -> bool:
        if record.op == "delete":
            return False
        return True if self.needs is None else bool(self.needs(record))

    def spec(self) -> dict[str, str]:
        return {
            "kind": "always" if self.needs is None else "when",
            **({"reason": self.reason} if self.reason else {}),
        }


def when(needs: Callable[[Record], bool], *, reason: str | None = None) -> Approval:
    """An approval for the deliveries with a record for which `needs` is true, such as a total above a limit.
    `reason` is shown on the hold."""
    return Approval(needs, reason)


def always(*, reason: str | None = None) -> Approval:
    """An approval for every delivery with a record to apply."""
    return Approval(None, reason)
