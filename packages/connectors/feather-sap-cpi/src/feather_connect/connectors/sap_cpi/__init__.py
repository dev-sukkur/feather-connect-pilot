"""SAP through SAP Integration Suite, Cloud Integration (CPI) (M2 plan 2 stage C1).

Feather calls an iFlow's HTTPS sender endpoint, `/http/<path>` on the tenant's runtime host, and the iFlow does the
SAP side (OData, IDoc, RFC). Two connectors, built on the HTTP ones:

- `Source`, a GET of an iFlow that answers JSON, page by page, by stamp or as a whole;
- `Post`, a side-effect sink that POSTs each record with an idempotency key, and reverses an applied delivery by
  POSTing the same records to `reversal_path` (`compensate`).

Auth is the Process Integration Runtime service key's OAuth2 client credentials by default, or basic auth. With
`csrf: true`, each chunk first fetches a CSRF token (`X-CSRF-Token: Fetch`) on the endpoint, as a CSRF protected
HTTPS sender wants, and sends it, with the session cookie, on each POST.
"""

from collections.abc import Collection, Mapping
from typing import ClassVar, Literal

from pydantic import ValidationError

from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ApplyBatch, ApplyResult, ConnectorSpec
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.runtime.http import FeatherHttp

__all__ = ["CSRF_HEADER", "CpiConfig", "Post", "Source"]

CSRF_HEADER = "X-CSRF-Token"


class CpiConfig(http.HttpConfig):
    """A CPI connection: the HTTP settings, with OAuth2 client credentials by default, and `csrf` for an iFlow whose
    HTTPS sender is CSRF protected."""

    auth_scheme: http.AuthScheme = "oauth2_client_credentials"
    csrf: bool = False


def _config_of(connection: str, ctx: Context) -> CpiConfig:
    try:
        return CpiConfig.model_validate(dict(ctx.config))
    except ValidationError as error:
        problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in error.errors())
        raise PermanentError(f"connection {connection!r} is not set up right: {problems}") from None


def _http(config: CpiConfig) -> http.HttpConfig:
    fields = {name: getattr(config, name) for name in http.HttpConfig.model_fields}
    return http.HttpConfig(**fields)


class Source(http.Rest):
    """A source that reads an iFlow answering JSON: `path` (`/http/prices`), with `records_path` where the records
    are (`d.results` for an OData pass-through), `pagination`, and `stamp_param` for by_stamp, as `http.Rest`."""

    spec: ClassVar[ConnectorSpec] = ConnectorSpec(
        secrets=http.SECRET_SETTINGS,
        name="sap_cpi.source",
        config=CpiConfig,
        strategies=frozenset({"by_stamp", "replace", "snapshot_diff"}),
        cost={"default": 1, "detail": 1},
        resumable=True,
        maturity="experimental",
    )

    def _config(self, ctx: Context) -> http.HttpConfig:
        return _http(_config_of(self.connection, ctx))


class Post(http.Call):
    """A side-effect sink that POSTs each record to the iFlow at `path` (a template filled from the record), and
    reverses an applied delivery by POSTing its records to `reversal_path` (design §8.4: a reversal posting).

    Each request carries `Idempotency-Key`, the same for a record sent again, for the iFlow to dedupe on, so that a
    resend after a crash posts nothing twice (H9). A 409 counts as applied already. `body` and `response_map` are as
    `http.Call`'s, such as `{"sap_document": "d.BillingDocument"}` to keep the document number SAP gave.
    """

    spec: ClassVar[ConnectorSpec] = ConnectorSpec(
        secrets=http.SECRET_SETTINGS,
        name="sap_cpi.post",
        config=CpiConfig,
        modes=frozenset({"upsert", "append"}),
        cost={"default": 1},
        maturity="experimental",
        side_effect=True,
    )

    def __init__(
        self,
        connection: str,
        *,
        path: http.Template,
        reversal_path: http.Template | None = None,
        body: http.BodyTemplate | None = None,
        headers: Mapping[str, str] | None = None,
        response_map: Mapping[str, str] | None = None,
        on_delete: Literal["reject", "skip"] = "reject",
        fail_statuses: Collection[int] = http.DEFAULT_FAIL_STATUSES,
    ) -> None:
        super().__init__(
            connection,
            method="POST",
            url=path,
            body=body,
            headers=headers,
            response_map=response_map,
            on_delete=on_delete,
            fail_statuses=fail_statuses,
        )
        self.reversal_path = reversal_path

    def _config(self, ctx: Context) -> http.HttpConfig:
        return _http(_config_of(self.connection, ctx))

    def _chunk_headers(self, ctx: Context, client: FeatherHttp, url: http.Template) -> dict[str, str]:
        if not _config_of(self.connection, ctx).csrf:
            return {}
        where = url if isinstance(url, str) and "{" not in url else "/"
        answer = client.request("GET", where, headers={CSRF_HEADER: "Fetch"}, budget=ctx.budget)
        token = answer.headers.get(CSRF_HEADER)
        if not token or token.lower() == "required":
            raise PermanentError(
                f"CPI gave no CSRF token for {where}: is the iFlow's HTTPS sender CSRF protected?"
            )
        return {CSRF_HEADER: token}

    def compensate(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        if self.reversal_path is None:
            raise PermanentError(f"sap_cpi.Post to {self.url!r} has no reversal_path, so it can't compensate")
        return self._send(ctx, batch, self.reversal_path, f"reverse:{batch.idempotency_key}")
