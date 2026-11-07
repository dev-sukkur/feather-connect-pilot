"""Partner orders to the ERP: the `webhook_to_api` template with its `partner_order` map."""

from feather_connect.templates.generic import webhook_to_api  # pyright: ignore[reportMissingImports]

orders = webhook_to_api(source="partner", destination="erp", url="/orders/{order_id}")
