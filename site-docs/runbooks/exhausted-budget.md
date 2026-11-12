# An exhausted budget

**You see**: an alert `budget_high` or `budget_exhausted`, the Budgets page near or at 100%, and reads or deliveries
that wait with "deferred by budget".

A budget caps the calls a connection makes, across every flow and worker, such as `["90/minute", "1400/day"]` for a
vendor API. Work that would go over it waits for the window to refill; nothing fails because of it, and a read bigger
than the whole budget fails at once, naming the budget.

1. **See who spends it.** The Budgets page shows each budget's use per window and which flows used it.
2. **If the spend is expected**, wait: the work resumes as the window refills, high priority first
   (`fc.to(..., priority="high")`).
3. **If one flow spends too much**, pause it, or move its schedule to a quieter hour.
4. **If the vendor gave you more**, raise the limits for the environment: `budgets:` in its environment file, imported
   with `feather config import`, with a reason. The account's real limit is the vendor's; Feather's must stay under it.
5. **If a single read can never fit**, make it smaller: partition the dataset (by day or by store), or read by
   `by_stamp` instead of in full.
