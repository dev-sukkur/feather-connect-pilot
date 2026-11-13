# An engine upgrade

Feather runs its durable work on DBOS. A new Feather release can bring a new DBOS, and with it a change to DBOS's own
system schema. This is how to take it without losing a task.

1. **Read the release notes** for "engine": a new DBOS version, and whether its system schema changes.
2. **Check it before it reaches production.** The engine conformance suite (`tests/engine_conformance`) runs the
   release's engine through every guarantee Feather depends on; it passed for the release, or the release says
   otherwise. Run the new release in qa first, then uat.
3. **Drain the workers**: stop them with SIGTERM (`docker compose stop feather-worker`). Each stops taking work and
   finishes its running tasks, for up to 30 seconds; anything still running is recovered after the upgrade.
4. **Migrate**: start the new release's `migrate` (Compose runs it before the server). It migrates Feather's schema,
   and DBOS migrates its own when it starts.
5. **Start the server and the workers** of the new release. Recovered workflows resume on them.
6. **Check** the Overview: workers alive, nothing failed, runs going. The Overview's code versions show two only
   during the rollout.

To roll back, stop the new release and start the old one: a migration of Feather's schema is backward compatible for
one release, as each release's notes confirm.
