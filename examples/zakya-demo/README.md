# The Zakya demo

The pilot's reference chain against simulators on this machine: Zakya's sales and returns go to Postgres staging, and
from there to SAP through CPI. B2B sales go one document per invoice, and B2C sales are summed per day and material.
Every invoice also goes to a plain ERP, one `PUT /invoices/{invoice_id}` each, every minute: the first demo leg.

## Run it

From the repository root:

```
uv run feather dev --project examples/zakya-demo
```

That starts the simulators (declared under `dev.processes` in `feather.yaml`), Postgres, the server and a worker;
Ctrl+C stops them all. Their output shows in the same console, prefixed `[zakya-sim]`, `[erp-sim]`, `[sap-sim]` and
`[staging]`. Sign in to the dashboard with the password it prints. Then open:

| Page | What it is |
| --- | --- |
| <http://127.0.0.1:29100/> | Zakya: the manual counter, the invoices and credit notes, and the auto/manual switch |
| <http://127.0.0.1:29102/> | SAP through CPI: the views by invoice and by material per day, the parked postings in manual mode, and a form to enter a document by hand |
| <http://127.0.0.1:29101/> | The plain ERP: every request it took, newest first |

The credentials in `secrets/` are fake, and only the simulators take them. Their paths in `env/dev.yaml` are read
against the project directory, so the demo runs from any directory.

To run a simulator alone, from this directory: `python sims/zakya.py --port 29100 --state .data/zakya.json` (add
`--manual` to start in manual mode), `python sims/sap.py --port 29102 --db .data/sap.sqlite` (add `--manual` to park
postings), and `python sims/receiver.py --port 29101`.

After upgrading from an older version of the demo, the Zakya simulator sets its old state file aside
(`.data/zakya.old.json`), since those invoices have no barcodes or GST treatment, and starts again. Delete `.data/` to
start everything afresh.

## Master data: materials and barcodes

`sims/master.json` is the master data that Zakya, the ERP and the POS share:

- **Materials.** Each has the ERP's material number (`MAT-1001`), its **EAN-13** (the GS1 barcode on the pack, which
  the POS scans: `8901234500101`) and its **POS code** (Zakya's SKU: `TEA-250`), with a description, unit and list
  price. A Zakya line carries both barcodes; the integration finds the material by the EAN in SAP's material master,
  which `sap-materials` reads from the ERP every 15 minutes.
- **Customers.** A customer's `gst_treatment` says which kind of sale it makes: `business_gst` (with a GSTIN) is B2B,
  `consumer` is B2C. That is Zoho's own field for Indian GST.

## The simulators

**Zakya** (`sims/zakya.py`, port 29100) is the `feather-zakya` package's simulator (OAuth2 refresh-token sign-in, paged
lists by `last_modified_time`, Zoho's rate limits, and about one call in 25 answered 429 or 503), with the demo's sales:

- *Auto mode* raises one or two invoices every 20 seconds, about 60% of them B2C counter sales. A B2C invoice is paid as
  it is made; a B2B one is raised `sent` and some are paid later. About one B2B invoice in 20 is a bulk order over 5
  lakh, which the chain holds for approval. Now and then goods come back: a credit note for one unit of an earlier
  invoice. New sales come when Feather lists the invoices, or when the page refreshes.
- *Manual mode* raises nothing on its own. On the page, pick a customer (its type shows as B2B or B2C), scan or type
  either barcode for each line, change the price if you want, and **Raise invoice**. **Return goods** on an invoice
  makes a credit note for what you enter, no more than was sold and not yet returned. **Mark paid** pays a B2B
  invoice.

With `--state`, the invoices and credit notes are kept in a JSON file, so a restart carries on where it stopped.

**SAP through CPI** (`sims/sap.py`, port 29102) answers as the `feather-sap-cpi` package's simulator does (a client
credentials token, a CSRF token per chunk, an `Idempotency-Key` on each post, a reversal path), and keeps what it posts
in `.data/sap.sqlite`. Its iFlows:

| iFlow | Takes | Keyed by |
| --- | --- | --- |
| `GET /http/materials` | The material master, as `d.results` | |
| `POST /http/invoices` | A B2B invoice: a billing document (F2) with its items | `invoice_id` |
| `POST /http/creditmemos` | A B2B return: a credit memo (G2) against `ReferenceInvoice` | `creditnote_id` |
| `POST /http/salessummary` | One material's B2C sales, or returns, on one day | `document_id` |
| `POST /http/<iflow>/reverse` | Takes a posted document back, once | as above |

A document posted again under the same key with another idempotency key answers 409 with the document the ERP has: it
never posts a business document twice. A material it doesn't know is turned down (422), and so is a credit memo that
gives back more of a material than the invoice sold, less earlier credit memos. A credit memo whose invoice isn't in
the ERP yet answers 424 (Failed Dependency): try again later.

- *Auto mode* books each posting as it arrives.
- *Manual mode* parks it: it answers with a document number and `Status: parked`, and the posting counts in the views
  only once someone presses **Post** on the page (**Reject** drops it), as an accountant checking each posting would.
- **Enter a document** books a sale or a return keyed in the ERP by hand, for any material by its number or either
  barcode. It shows in both views, marked as entered by hand.

Its two views count posted documents only:

- **By invoice**: each document, newest first, with its kind (invoice, credit memo, B2C summary, B2C returns
  summary, manual), its type, posting date, customer, the Zakya document it came from (and the invoice a credit memo
  is against), its items and its status.
- **By material per day**: per day and material, the quantity sold, returned and net, the net amount, the B2B and B2C
  quantity, how many documents, and the net quantity and amount to date. Each posting of a day adds to the day's row,
  so the row is the day so far. A **Day** filter narrows both views to one day.

**The plain ERP** (`sims/receiver.py`, port 29101) keeps every request it takes in `.data/receiver.sqlite` (method, path,
headers without credentials, body, idempotency key, time) and answers `{"id": "ERP-..."}`. A PUT is an upsert by
invoice id: the first for an invoice makes its ERP id, and a later one (the invoice was paid, say) is stored as a
second request for the same ERP id. It stores a repeat of an idempotency key only once, and answers 503 to about one
request in 33.

## The flows

`env/dev.yaml` raises the `zoho-api` budget to 20000 calls a day: the simulator makes about 8 calls a minute, which
would spend Zoho's 1400 a day in about three hours and leave the demo looking stalled.

**Into staging** (`zakya_demo` in the Postgres of `feather dev`, made by `sims/staging.py`):

- `zakya-invoices` (`flows/invoices.py`) calls the library template `zakya_invoices_to_erp` from `feather-retail`. Each
  run reads the invoices changed since the last one, paging by `last_modified_time`, and upserts each into the plain
  ERP through `http.Call` and into `zakya_demo.invoices`, with its channel (B2B or B2C) and its lines, both barcodes
  on each, as JSON.
- `zakya-credit-notes` (`flows/chain.py`) does the same for Zakya's credit notes, into `zakya_demo.credit_notes`; each
  names the invoice it is against.
- `sap-materials` reads SAP's material master every 15 minutes into `zakya_demo.materials`. Its dataset keeps its
  state (`keep_state=True`), which is what lets the flows to SAP look a material up by EAN: `ctx.lookup("materials",
  ean)` in Python, `{{ dataset('materials') }}` in SQL. Until it has run once, a posting that needs it waits and
  retries.

**To SAP**, each once a run's documents are in staging (`fc.on_ready(..., delivered_to=...)`), at most every 5 minutes:

| Flow | Reads | Posts |
| --- | --- | --- |
| `invoices-to-sap` | B2B invoices in staging | One billing document per invoice, its lines as items with their SAP material |
| `b2b-returns-to-sap` | B2B credit notes in staging | One credit memo per credit note, against the invoice it names |
| `b2c-to-sap` (`flows/b2c.py`) | Every line of the B2C invoices and credit notes in staging | One summary per day, material and direction (sales or returns) of the run's new lines |

- **B2B is posted at invoice level.** A billing document or credit memo over 500,000 holds its delivery for an
  approver: it waits on the dashboard's **Holds** page (or `feather holds list`); approved, it posts as it was, and
  rejected, it is skipped. A line whose EAN SAP doesn't know turns the document down to the quarantine, with the line
  named, rather than posting it without a material.
- **B2C is posted at material level.** `b2c-lines` is one record per line of the last 35 days, a return's quantity
  and amount negative, read in full each time (`snapshot_diff`), so each run gets only the lines new since the last
  one. The route's SQL (`fc.sql`) sums them per day, material and direction, the material found by EAN in
  `{{ dataset('materials') }}`, and posts each sum. The ERP adds up a day's summaries as they come, so its view by
  material per day shows the day's B2C sales so far, beside the B2B ones.
- Every post carries an `Idempotency-Key`, so a resend after a lost answer posts nothing twice. An applied delivery
  can be reversed: **Compensate** on its delivery page (or `feather deliveries compensate`) posts each document to the
  iFlow's reversal path, once.

Each delivery's page on the dashboard lists the document number SAP gave each record (`sap_document`).

## Goods returns

**This is a proposal, for finance to confirm.** Whether a change after a posting is reversed and posted again, or
posted as the difference, is open (discovery Q8, D8 in the pilot plan). The demo builds the difference, as below, and
the other way is possible with what Feather has; the comparison follows.

A return is a credit note in Zakya, against the invoice the goods were sold on. Zakya's simulator never lets more
come back than was sold.

- **B2B (proposed): a credit memo per credit note, against its invoice.** `b2b-returns-to-sap` posts it with
  `ReferenceInvoice`, the Zakya invoice id, and SAP finds its own billing document by that id. SAP checks the return
  against the invoice: no more of a material than the invoice sold, less earlier credit memos. It shows in the views
  as a negative line on the day of the return. The approval rule is the invoices': over 500,000 is held. A credit
  memo that reaches SAP before its invoice fails its delivery (SAP answers 424); retry it once the invoice is there.
- **B2C (proposed): summed into the day's returns, on the day the goods came back.** A B2C credit note's lines join
  `b2c-lines` with negative quantities, and each run posts them as a returns summary per material (`B2C_RETURNS`),
  apart from the sales summary. The by-material view nets them: sold, returned and net per day. A return isn't booked
  against the day of the sale.

The two ways, side by side:

| | Post the difference (built) | Reverse and post again |
| --- | --- | --- |
| B2B | A credit memo (G2) for what came back; the invoice stays as posted | Reverse the invoice's billing document and post it again, less what came back |
| B2C | A returns summary on the day of the return | Reverse the day's summary for that material and post it again with the return netted in |
| How, in Feather | As built: the credit note is its own record, posted once with its own idempotency key | B2B: **Compensate** reverses a whole delivery (every invoice in it), so a reversal per invoice needs a route that posts each invoice's reversal itself, from the credit note. B2C: one summary per material per closed day (read the day once it is complete, `fc.on_ready(..., each="day")`), so that a day has one document to reverse; a late return then reruns the day (`on_late="rerun"`) |
| The books | The original invoice, its payment matching and its GST return stay as filed; the return is visible as its own document | The corrected document replaces the original; a filed GST return for the original month needs an amendment |
| Within a day | Many small documents | One document per day and material, posted after the day closes |

Why the proposal is the difference: a credit memo is how SAP records goods coming back; reversing a B2B invoice
undoes its payment matching and GST record; and a B2C summary holds many customers' sales, so reversing it for one
returned pack touches all of them. Feather's **Compensate** stays for a posting that was wrong, not for goods that came
back.

Other choices, also open to change:

- *Returns apart from sales in the B2C summary.* Netting them into one document would hide returns in the books; one
  summary per direction keeps returns visible and the net still shows in the view.
- *Per run, not once a day.* The ERP's view adds up the day's postings as they come, as asked for; a summary per
  closed day is the other way (one posting per material per day, after midnight), and is a change of the read and
  the trigger, not of the SQL.

### What is left open

- **A credit memo before its invoice.** When a B2B credit note reaches SAP before its invoice (the invoice is held for
  approval, say), SAP answers 424, which the route lists in `fail_statuses`: the delivery fails, and later credit
  notes wait behind it, in order. **Retry** it on the Failures page once the invoice is posted. Nothing retries it by
  itself; a readiness rule that holds returns until their invoices are applied would, and the demo doesn't have one.
- **A B2C invoice changed after it was summed.** A POS sale isn't edited: a correction is a return. If a B2C line
  were changed anyway, the new values would be summed and posted again, and the old ones would stay. Posting the
  difference needs the line's last posted state, which the read doesn't keep.
- **B2C returns without the original invoice.** The simulator always takes goods back against an invoice. A real
  counter also takes them back on a receipt alone; the summary doesn't need the invoice, so that works as it is, but
  Zakya's credit note must then still carry the EAN.
- **Taxes.** Lines go to SAP at their net amount, with one GST rate on the invoice total. Tax by line, HSN codes and
  place of supply are the iFlow's or SAP's to work out.
- **One summary per run and material.** A busy day makes many small summaries. Posting once a day after the day
  closes would make one per material.
- **Q8 / D8: the difference or reverse and post again.** Proposed as above; finance confirms. If finance picks
  reversal, the B2C flow moves to one summary per closed day, and B2B returns get a reversal route.
- **Which ERP mode is meant.** The ERP's manual mode parks postings for a person to post, and its form books a
  document by hand; its auto mode books what arrives. Whether "manual" should mean something else on the ERP side
  is for the sponsor to say.
