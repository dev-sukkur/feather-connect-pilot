# feather-sap-cpi

SAP through SAP Integration Suite, Cloud Integration (CPI): Feather calls an iFlow's HTTPS endpoint, and the iFlow
talks to SAP. No on-prem agent is needed, since CPI is reached over the internet.

```yaml
connections:
  sap:
    connector: sap_cpi.post
    base_url: https://acme.it-cpi018-rt.cfapps.eu10-003.hana.ondemand.com
    token_url: https://acme.authentication.eu10.hana.ondemand.com/oauth/token
    csrf: true                       # when the iFlow's HTTPS sender is CSRF protected
    auth:
      client_id: ${secret:env:CPI_CLIENT_ID}
      client_secret: ${secret:env:CPI_CLIENT_SECRET}
```

```python
from feather_connect.connectors import sap_cpi

to_sap = fc.to(
    sap_cpi.Post("sap", path="/http/invoices", reversal_path="/http/invoices/reverse"),
    approval=fc.approval.when(lambda r: r["total"] > 500_000),
)
```

- `sap_cpi.Source(connection, path=..., records_path=..., stamp_param=...)` reads an iFlow that answers JSON.
- `sap_cpi.Post` sends each record once, with an `Idempotency-Key` header the iFlow can dedupe on. It is a
  side-effect sink: `compensate` posts the same records to `reversal_path`, for a reversal posting.
- Auth is the OAuth2 client credentials of the Process Integration Runtime service key (`auth_scheme` defaults to
  `oauth2_client_credentials`); basic auth works too, with `auth_scheme: basic`.
