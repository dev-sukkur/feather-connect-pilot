"""Signed webhooks in through `http.Webhook`, each sent on with `PUT /events/{id}` through `http.Call`."""

import feather_connect as fc
from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]

hooks = fc.Dataset(
    "perf_hooks",
    source=http.Webhook("perf_hooks", signature_secret="signature_secret", event_id_path="event_id"),
    key="id",
    strategy=fc.strategy.push(),
)
flow = fc.Flow(
    "hooks_to_http",
    reads=hooks,
    deliver=[fc.to(http.Call("echo", method="PUT", url="/events/{id}"), name="echo")],
    triggers=[fc.on_event(hooks)],
)
