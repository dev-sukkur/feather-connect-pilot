"""The demo flow: Zakya's new and changed invoices, posted to the ERP every minute.

It is the library template, called with the demo's names; a project that wants other fields to reach its ERP passes
its own `map`, and one that wants full control copies the template in with `feather new flow --template`.
"""

from feather_connect.templates.retail import zakya_invoices_to_erp  # pyright: ignore[reportMissingImports]

invoices = zakya_invoices_to_erp(source="zakya", destination="erp", name="zakya-invoices", page_size=25)
