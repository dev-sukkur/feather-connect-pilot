# The Zakya demo

Invoices from Zakya go to an ERP, one POST each, every minute. Both ends are small simulators on this machine:

- `sims/zakya.py` (port 29100) plays Zakya (Zoho Inventory's API): an OAuth2 refresh-token sign-in, a paged invoice
  list filtered by `last_modified_time`, and each invoice on its own with its line items. New invoices appear every
  20 seconds or so and some open ones are paid. About one call in 25 answers 429 or 503.
- `sims/receiver.py` (port 29101) plays the ERP: it keeps every request it takes in `.data/receiver.sqlite`
  (method, path, headers without credentials, body, idempotency key, time), answers `{"id": "ERP-..."}`, stores a
  repeat of an idempotency key only once, and answers 503 to about one request in 33. Its page at
  <http://127.0.0.1:29101/> lists the requests, newest first.

The flow, `flows/invoices.py`, calls the library template `zakya_invoices_to_erp` from `feather-retail`. Each run
reads the invoices changed since the last one through `http.Rest` and posts each through `http.Call`; the ERP's id
for each invoice shows on the run's delivery page on the dashboard.

## Run it

From the repository root:

```
uv run feather dev --project examples/zakya-demo
```

That starts both simulators (declared under `dev.processes` in `feather.yaml`), Postgres, the server and a
worker; Ctrl+C stops them all. Their output shows in the same console, prefixed `[zakya-sim]` and `[erp-sim]`. Sign
in to the dashboard with the password it prints, and watch the runs of `zakya-invoices`. Each delivery's page lists
the ERP id each invoice got.

The credentials in `secrets/` are fake, and only the simulators take them. Their paths in `env/dev.yaml` are read
from the directory feather runs in, so run it from the repository root.

To run the simulators alone, from this directory: `python sims/zakya.py --port 29100` and
`python sims/receiver.py --port 29101`.
