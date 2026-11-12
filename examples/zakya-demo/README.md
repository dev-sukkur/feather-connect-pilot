# The Zakya demo

Invoices from Zakya go to an ERP, one `PUT /invoices/{invoice_id}` each, every minute. Both ends are small simulators on this machine:

- `sims/zakya.py` (port 29100) plays Zakya with the `feather-zakya` package's simulator: an OAuth2 refresh-token
  sign-in, a paged invoice list filtered by `last_modified_time`, and each invoice on its own with its line items. New
  invoices appear every 20 seconds or so and some open ones are paid. About one call in 25 answers 429 or 503.
- `sims/receiver.py` (port 29101) plays the ERP: it keeps every request it takes in `.data/receiver.sqlite`
  (method, path, headers without credentials, body, idempotency key, time) and answers `{"id": "ERP-..."}`. A PUT is
  an upsert by invoice id: the first for an invoice makes its ERP id, and a later one (the invoice was paid, say) is
  stored as a second request for the same ERP id. It stores a repeat of an idempotency key only once, and answers 503 to about one request in 33. Its page at
  <http://127.0.0.1:29101/> lists the requests, newest first.

The flow, `flows/invoices.py`, calls the library template `zakya_invoices_to_erp` from `feather-retail`. Each run
reads the invoices changed since the last one through the `zakya` connector, paging by `last_modified_time` so that
an invoice changed during a read is not skipped, and upserts each through `http.Call`; the ERP's id for each invoice shows on the
run's delivery page on the dashboard.

## The chain to SAP

The same run upserts each invoice into a staging table too, `zakya_demo.invoices` in the Postgres that `feather dev`
runs (`sims/staging.py` makes it at startup). That is the first link of the reference chain, Zakya → Postgres → SAP
through CPI, in `flows/chain.py`:

- once a run's invoices are applied to staging, `invoices-to-sap` starts (`fc.on_ready(..., delivered_to="staging")`),
  at most every 5 minutes. It reads the staging rows changed since its last run and posts each, as a billing
  document, through `sap_cpi.Post`: an OAuth2 client-credentials token, a CSRF token per chunk, and an
  `Idempotency-Key` on each post, so that a resend after a lost answer posts nothing twice.
- a delivery with an invoice over 500,000 is held for an approver. It waits on the dashboard's **Holds** page (or
  `feather holds list`); approved, it posts as it was, and rejected, it is skipped.
- an applied delivery can be reversed: **Compensate** on its delivery page (or `feather deliveries compensate`) posts
  each document to the iFlow's reversal path, once.

`sims/cpi.py` (port 29102) plays the CPI tenant with the `feather-sap-cpi` package's simulator. It keeps what it is
sent in memory, so a restart forgets it.

`env/dev.yaml` raises the `zoho-api` budget to 20000 calls a day: the simulator makes about 8 calls a minute, which
would spend Zoho's 1400 a day in about three hours and leave the demo looking stalled.

## Run it

From the repository root:

```
uv run feather dev --project examples/zakya-demo
```

That starts the simulators (declared under `dev.processes` in `feather.yaml`), Postgres, the server and a
worker; Ctrl+C stops them all. Their output shows in the same console, prefixed `[zakya-sim]`, `[erp-sim]` and `[cpi-sim]`. Sign
in to the dashboard with the password it prints, and watch the runs of `zakya-invoices` and `invoices-to-sap`. Each delivery's page lists
the ERP id each invoice got.

The credentials in `secrets/` are fake, and only the simulators take them. Their paths in `env/dev.yaml` are read
against the project directory, so the demo runs from any directory.

To run the simulators alone, from this directory: `python sims/zakya.py --port 29100 --state .data/zakya.json` and
`python sims/receiver.py --port 29101` and `python sims/cpi.py --port 29102`.
