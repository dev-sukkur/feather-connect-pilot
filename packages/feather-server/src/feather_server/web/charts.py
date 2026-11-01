"""Small inline SVG charts, returned as `Markup`. Their text is built from numbers alone, so no value from the data can
reach the page as anything but a digit."""

import math
from collections.abc import Sequence

from markupsafe import Markup

_WIDTH, _HEIGHT, _PAD = 120, 28, 3


def _finite(values: Sequence[float]) -> list[float]:
    return [float(v) for v in values if math.isfinite(v)]


def sparkline(values: Sequence[float]) -> Markup:
    """A line through `values`, oldest first, drawn to fit its own range. No values draw nothing."""
    points = _finite(values)
    if not points:
        return Markup("")
    low, high = min(points), max(points)
    span = (high - low) or 1.0
    line = points * 2 if len(points) == 1 else points  # a line needs two points
    step = (_WIDTH - 2 * _PAD) / (len(line) - 1)
    drawn = " ".join(
        f"{_PAD + i * step:.1f},{_HEIGHT - _PAD - (v - low) / span * (_HEIGHT - 2 * _PAD):.1f}"
        for i, v in enumerate(line)
    )
    return Markup(
        f'<svg class="sparkline" viewBox="0 0 {_WIDTH} {_HEIGHT}" width="{_WIDTH}" height="{_HEIGHT}" '
        f'role="img" aria-label="trend of the last {len(points)} values: from {low:g} to {high:g}">'
        f'<polyline points="{drawn}" fill="none" stroke="currentColor" stroke-width="2" '
        'stroke-linejoin="round" stroke-linecap="round"/></svg>'
    )


def meter(value: float, limit: float) -> Markup:
    """A bar of `value` against `limit`, with the numbers beside it. Past the limit the bar is full and marked."""
    used = value if math.isfinite(value) else 0.0
    cap = limit if math.isfinite(limit) else 0.0
    share = 0.0 if cap <= 0 else min(max(used / cap, 0.0), 1.0)
    over = cap > 0 and used > cap
    label = f"{used:g} of {cap:g}"
    return Markup(
        f'<span class="meter{" meter-over" if over else ""}" role="img" aria-label="{label}">'
        f'<svg viewBox="0 0 100 8" width="100" height="8" aria-hidden="true">'
        f'<rect class="meter-track" width="100" height="8" rx="4"/>'
        f'<rect class="meter-fill" width="{share * 100:.1f}" height="8" rx="4"/></svg>'
        f'<span class="meter-text">{label}{" (over the limit)" if over else ""}</span></span>'
    )
