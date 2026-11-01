"""The picture of a flow: its datasets, the routes they feed and the destinations those reach, as inline SVG.

It is computed here and needs no JavaScript. Every name from the project goes through `escape`, and each box has a
`<title>` that says what it is, for a screen reader or a hover.
"""

from markupsafe import Markup, escape

from feather_server.services.reads import FlowDetail

_BOX_W, _BOX_H, _GAP = 200, 46, 18
_COLUMNS = (20, 300, 580)  # left edge of datasets, routes and destinations
_WIDTH = _COLUMNS[2] + _BOX_W + 20
_MAX_CHARS = 26


def _short(text: str) -> str:
    return text if len(text) <= _MAX_CHARS else text[: _MAX_CHARS - 1] + "…"


def _box(x: int, y: int, css: str, title: str, label: str, detail: str) -> str:
    return (
        f"<g><title>{escape(title)}</title>"
        f'<rect class="node {css}" x="{x}" y="{y}" width="{_BOX_W}" height="{_BOX_H}" rx="8"/>'
        f'<text x="{x + 12}" y="{y + 19}" font-weight="600">{escape(_short(label))}</text>'
        f'<text x="{x + 12}" y="{y + 36}" opacity="0.8">{escape(_short(detail))}</text></g>'
    )


def _edge(x1: int, y1: float, x2: int, y2: float) -> str:
    mid = (x1 + x2) / 2
    return f'<path class="edge" d="M{x1} {y1:.0f} C{mid:.0f} {y1:.0f} {mid:.0f} {y2:.0f} {x2} {y2:.0f}"/>'


def flow_graph(detail: FlowDetail) -> Markup:
    """An SVG of datasets → routes → destinations. Paused routes are dashed and say so in their title."""
    flow = detail.summary
    destinations = list(dict.fromkeys(r.connection for r in flow.routes))
    rows = max(len(flow.datasets), len(flow.routes), len(destinations), 1)
    height = rows * (_BOX_H + _GAP) + _GAP

    def y_of(index: int, count: int) -> int:
        """Centre a column of `count` boxes against the tallest one."""
        offset = (rows - count) * (_BOX_H + _GAP) // 2
        return _GAP + offset + index * (_BOX_H + _GAP)

    parts: list[str] = []
    centres = _BOX_H / 2
    for i, route in enumerate(flow.routes):
        ry = y_of(i, len(flow.routes))
        for j, _ in enumerate(flow.datasets):
            parts.append(
                _edge(_COLUMNS[0] + _BOX_W, y_of(j, len(flow.datasets)) + centres, _COLUMNS[1], ry + centres)
            )
        d = destinations.index(route.connection)
        parts.append(
            _edge(_COLUMNS[1] + _BOX_W, ry + centres, _COLUMNS[2], y_of(d, len(destinations)) + centres)
        )
    for j, dataset in enumerate(flow.datasets):
        parts.append(
            _box(
                _COLUMNS[0],
                y_of(j, len(flow.datasets)),
                "node-dataset",
                f"Dataset {dataset.name}: read from {dataset.connection}, strategy {dataset.strategy}",
                dataset.name,
                f"from {dataset.connection}",
            )
        )
    for i, route in enumerate(flow.routes):
        parts.append(
            _box(
                _COLUMNS[1],
                y_of(i, len(flow.routes)),
                "node-route node-paused" if route.paused else "node-route",
                f"Route {route.name}" + (" (paused)" if route.paused else "") + f" to {route.connection}",
                route.name,
                "paused" if route.paused else "delivering",
            )
        )
    for d, destination in enumerate(destinations):
        parts.append(
            _box(
                _COLUMNS[2],
                y_of(d, len(destinations)),
                "node-destination",
                f"Destination {destination}",
                destination,
                "destination",
            )
        )
    summary = escape(
        f"Flow {flow.name}: {len(flow.datasets)} dataset(s) feed {len(flow.routes)} route(s) "
        f"to {len(destinations)} destination(s)."
    )
    return Markup(
        f'<svg class="graph" viewBox="0 0 {_WIDTH} {height}" role="img" aria-labelledby="graph-title">'
        f'<title id="graph-title">{summary}</title>{"".join(parts)}</svg>'
    )
