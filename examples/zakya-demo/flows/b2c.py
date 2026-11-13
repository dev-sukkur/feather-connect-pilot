"""B2C sales and returns to SAP, summarised per day and material (the reference chain's B2C half; flows/chain.py
has the rest).

`b2c-lines` is every line of the B2C invoices and credit notes in staging, one record per line, a return's
quantity and amount negative, dated by its own document: a return counts on the day the goods came back, not on the
day they were sold. It is read in full (`snapshot_diff`), so each run gets only the lines that are new since the last
one, and a line seen before gives nothing. Only the last 35 days are read: an older line leaving the read is a
delete, which the sink skips (`on_delete="skip"`).

The route to SAP sums those new lines with SQL (`fc.sql`) per day, material and direction, the material found by its
EAN in SAP's material master (`{{ dataset('materials') }}`), and posts each sum as one summary document. SAP adds
up the summaries of a day as they come, so its view by material per day is the day's B2C sales and returns so far.
Each summary carries the lowest key of its lines as its own `_fc_key`, so the delivery's idempotency key and
answers follow it, and a document id made from that key: a resend posts nothing twice.
"""

import feather_connect as fc
from feather_connect.connectors import postgres, sap_cpi  # pyright: ignore[reportAttributeAccessIssue]
from flows.chain import EVERY_FIVE_MINUTES

LINES_DAYS = 35

B2C_LINES = f"""
select i.invoice_id as document_id, 'sale' as kind, i.invoice_number as document_number,
       i.invoice_date as business_date, (l.value->>'line_no')::int as line_no, l.value->>'ean' as ean,
       (l.value->>'quantity')::float8 as quantity, (l.value->>'amount')::float8 as amount
from zakya_demo.invoices i cross join lateral jsonb_array_elements(i.lines) as l(value)
where i.channel = 'B2C' and i.invoice_date::date >= current_date - {LINES_DAYS}
union all
select c.creditnote_id, 'return', c.creditnote_number, c.note_date, (l.value->>'line_no')::int, l.value->>'ean',
       -(l.value->>'quantity')::float8, -(l.value->>'amount')::float8
from zakya_demo.credit_notes c cross join lateral jsonb_array_elements(c.lines) as l(value)
where c.channel = 'B2C' and c.note_date::date >= current_date - {LINES_DAYS}
"""

b2c_lines = fc.Dataset(
    "b2c-lines",
    source=postgres.Query("staging_read", sql=B2C_LINES),
    key=("document_id", "line_no"),
    strategy=fc.strategy.snapshot_diff(),
)

# One row per day, material and direction of the batch's new lines. `_fc_key` is the lowest line key of the group,
# which only this group of this batch has.
SUMMARY = """
select min(b._fc_key) as _fc_key,
       'B2C-' || replace(b.business_date, '-', '') || '-' || coalesce(m.material, 'EAN' || b.ean) || '-'
           || case b.kind when 'sale' then 'S' else 'R' end || '-' || left(md5(min(b._fc_key)), 8) as document_id,
       case b.kind when 'sale' then 'B2C_SALES' else 'B2C_RETURNS' end as SummaryType,
       b.business_date as PostingDate,
       coalesce(m.material, '') as Material,
       b.ean as EAN,
       sum(b.quantity) as Quantity,
       round(sum(b.amount), 2) as NetAmount,
       count(*) as Lines,
       string_agg(distinct b.document_number, ', ') as Documents
from batch b left join {{ dataset('materials') }} m on m.ean = b.ean
group by b.business_date, b.kind, b.ean, m.material
"""

b2c_to_sap = fc.Flow(
    "b2c-to-sap",
    reads=b2c_lines,
    deliver=[
        fc.to(
            sap_cpi.Post(
                "sap",
                path="/http/salessummary",
                reversal_path="/http/salessummary/reverse",
                response_map={"sap_document": "d.BillingDocument"},
                on_delete="skip",
            ),
            name="sap-b2c-summary",
            map=fc.sql(SUMMARY),
        )
    ],
    triggers=[
        fc.on_ready(
            fc.ready("invoices", delivered_to="staging"), each="run", at_most_every=EVERY_FIVE_MINUTES
        ),
        fc.on_ready(
            fc.ready("credit_notes", delivered_to="staging-returns"),
            each="run",
            at_most_every=EVERY_FIVE_MINUTES,
        ),
    ],
)
