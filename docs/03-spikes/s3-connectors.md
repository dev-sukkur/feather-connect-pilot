# SP3 — Connector desk research (30 Oct 2026)

**Scope:** public documentation only. Live probes need sandbox access (discovery Q2–Q4). Each finding links its source. Third-party claims are marked as such.

## Zoho Zakya (Zoho POS)

| Topic | Finding | Source |
|---|---|---|
| Endpoints | Hosts are `api.zakya.com`, or `api.zakya.in` for India. Paths look like `/inventory/v1/invoices`. | [Zoho POS API](https://www.zoho.com/pos/resources/api/) |
| Auth | OAuth 2.0 with scopes `ZohoPOSAPI.invoices.{CREATE,READ,UPDATE,DELETE}`. The access token goes **only** in a header. | [Zoho POS OAuth](https://www.zoho.com/pos/resources/api/oauth.html) |
| Limits | A daily request cap per plan (published: Free/Trial 1,500, Standard 2,500), **plus 100 requests per minute** across all APIs | [Zoho POS API](https://www.zoho.com/pos/resources/api/) |
| Webhooks | No native POS webhook found. Zoho Flow (Zoho's no-code tool) has a "new invoice created" trigger. | [Zoho Flow: Zoho POS](https://www.zohoflow.com/apps/zoho-pos/integrations/) |

**Design impact:**
- The `zoho-api` budget needs **two windows**, per minute and per day. The ADR-0005/§6.6 budget design already supports this.
- Invoices are polled with `by_stamp` (list, then detail).
- A Zoho Flow → `feather` webhook bridge is an optional latency improvement, not a dependency.
- The reference organisation's enterprise cap (25,000/day per the reference evaluation) is far above the published plan caps, so Q4 must confirm it in writing.

## GoFrugal

| Topic | Finding | Source |
|---|---|---|
| Public API docs | **None found.** | — |
| Third-party claims | A limited REST API served by a local "WebReporter" service next to RetailEasy, with hosted HQ variants and an account key in a header. **Unverified**: the same vendor's docs page calls itself an analysis of what an API "could look like". | [Supergood report card](https://supergood.ai/api-report-card/gofrugal), [Supergood docs](https://supergood.ai/docs/gofrugal-api) |
| Existing integrations | Partners such as Unicommerce integrate with GoFrugal, so some access path exists | [Unicommerce KB](https://support.unicommerce.com/index.php/wp-json/wp/v2/ht-kb/7136) |

**Design impact:**
- Risk R6 is confirmed. The GoFrugal connector **can't be scoped** until GoFrugal provides API documentation and sandbox access (Q2).
- Recommendation: ask GoFrugal now. Plan the connector as "M2 if the docs arrive by M1's end, otherwise M3".
- Fallback: file exchange through S3-compatible storage.

## SAP S/4HANA

| Topic | Finding | Source |
|---|---|---|
| Pricing API | `API_SLSPRICINGCONDITIONRECORD_SRV`. No delta or change-tracking token is documented. Filters on the record and validity entities can't be combined in one `$expand` request; filter the parent on `LastChangeDateTime` instead. | [SAP KBA 3790186](https://userapps.support.sap.com/sap/support/knowledge/en/3790186) |
| Stamp reliability | On-premise CDS views were reported **not updating** `lastchangeddatetime` | [SAP KBA 3751328](https://userapps.support.sap.com/sap/support/knowledge/en/3751328) |
| RFC from Python | SAP's **PyRFC is archived and deprecated** (read-only repository). A third-party `pyrfcx` (Apache-2.0, Rust core) is a drop-in client. It still needs SAP's NetWeaver RFC SDK, which requires an S-user download. | [SAP-archive/PyRFC](https://github.com/SAP-archive/PyRFC), [pyrfcx](https://pypi.org/project/pyrfcx/), [community.sap_libs](https://github.com/sap-linuxlab/community.sap_libs) |

**Design impact:**
- `snapshot_diff` as the default for SAP prices is **validated**: change stamps can't be fully trusted. Use `by_stamp` on `LastChangeDateTime` only together with `reconcile`.
- OData first is **validated**. BAPI/RFC stays an optional extra, and the docs should name `pyrfcx` as the binding and flag its third-party status.

## Magento 2 / Adobe Commerce

| Topic | Finding | Source |
|---|---|---|
| Auth | Since 2.4.4, integration access tokens are **not** accepted as standalone bearer tokens by default (a version-dependent config flag changes this). Admin tokens expire after 4 h and don't work with 2FA. | [Adobe: token authentication](https://developer.adobe.com/commerce/webapi/get-started/authentication/gs-authentication-token), [Hyvä POS config](https://docs.hyva.io/hyva-pos/getting-started/magento-configuration.html) |
| Bulk writes | `/async/bulk/V1/...`, with a different path on SaaS. It needs RabbitMQ and the `async.operations.all` consumer running. Each request returns a `bulk_uuid` whose status must be polled. GET isn't supported. | [Bulk endpoints](https://developer.adobe.com/commerce/webapi/rest/use-rest/bulk-endpoints/), [Async endpoints](https://developer.adobe.com/commerce/webapi/rest/use-rest/asynchronous-web-endpoints), [Consumers](https://experienceleague.adobe.com/en/docs/commerce-operations/configuration-guide/message-queues/consumers) |

**Design impact:**
- The connector signs requests with **OAuth 1.0a** (integration credentials) by default, which avoids the bearer-token flag and admin-token expiry.
- **New design point:** a bulk write is *accepted* first and *confirmed* later. The sink protocol needs an "accepted, confirm later" result, and the delivery stays `running` until the bulk status confirms success per record. Without this, a delivery would be marked `applied` before the shop processed it. Proposed: `ApplyResult(pending_confirmation=token)`, with the runtime polling `sink.confirm(token)` using durable sleeps. Add this to design §8.1.
