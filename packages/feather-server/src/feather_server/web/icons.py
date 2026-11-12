"""The dashboard's icons: simple strokes of our own on a 24-unit grid, drawn in the text's colour. Each is
decoration (`aria-hidden`), so a label beside it or on its control says what it is. Their markup is constant, so
nothing from the data can reach the page through them."""

from markupsafe import Markup

ICONS: dict[str, str] = {
    "overview": (
        '<rect x="3" y="3" width="7" height="9" rx="1.5"/><rect x="14" y="3" width="7" height="5" rx="1.5"/><rect x="14" y="12" width="7" height="9" rx="1.5"/><rect x="3" y="16" width="7" height="5" rx="1.5"/>'
    ),
    "flows": (
        '<circle cx="5" cy="6" r="2.5"/><circle cx="5" cy="18" r="2.5"/><circle cx="19" cy="12" r="2.5"/><path d="M7.5 6h3a3 3 0 0 1 3 3v0a3 3 0 0 0 3 3M7.5 18h3a3 3 0 0 0 3-3v0a3 3 0 0 1 3-3"/>'
    ),
    "runs": ('<circle cx="12" cy="12" r="9"/><path d="M10 8.5v7l5.5-3.5z"/>'),
    "datasets": (
        '<ellipse cx="12" cy="5.5" rx="8" ry="2.5"/><path d="M4 5.5v13c0 1.4 3.6 2.5 8 2.5s8-1.1 8-2.5v-13M4 12c0 1.4 3.6 2.5 8 2.5s8-1.1 8-2.5"/>'
    ),
    "deliveries": ('<path d="M3 11.5 21 3l-6.5 18-3-7.5z"/><path d="m11.5 13.5 3.5-3.5"/>'),
    "failures": ('<path d="M12 3.5 21.5 20h-19z"/><path d="M12 10v4.5M12 17.2v.3"/>'),
    "quarantine": ('<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M3 13h5l1.5 2.5h5L16 13h5"/>'),
    "connections": ('<path d="M9 7V3M15 7V3M6.5 7h11v4a5.5 5.5 0 0 1-11 0zM12 16.5V21"/>'),
    "secrets": ('<circle cx="8" cy="15" r="4.5"/><path d="m11.2 11.8 8.3-8.3M16.5 6.5l2.5 2.5M14 9l2 2"/>'),
    "users": (
        '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0 1 13 0M16 4.5a3.5 3.5 0 0 1 0 7M18 14.5a6.5 6.5 0 0 1 3.5 5.5"/>'
    ),
    "readiness": ('<path d="M4 12.5 9 17.5 20 6.5"/><path d="M4 6.5h7M4 20h16"/>'),
    "holds": (
        '<rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/>'
    ),
    "trace": ('<circle cx="10.5" cy="10.5" r="6.5"/><path d="m15.5 15.5 5 5M8 10.5h5"/>'),
    "packages": (
        '<path d="m12 2.8 8.5 4.6v9.2L12 21.2l-8.5-4.6V7.4z"/><path d="m3.5 7.4 8.5 4.6 8.5-4.6M12 12v9.2"/>'
    ),
    "registries": (
        '<rect x="3" y="4" width="18" height="6.5" rx="1.5"/><rect x="3" y="13.5" width="18" height="6.5" rx="1.5"/><path d="M7 7.2h.01M7 16.7h.01M11 7.2h6M11 16.7h6"/>'
    ),
    "search": ('<circle cx="10.5" cy="10.5" r="6.5"/><path d="m15.5 15.5 5 5"/>'),
    "help": (
        '<circle cx="12" cy="12" r="9"/><path d="M9.5 9.3a2.6 2.6 0 0 1 5 .9c0 1.8-2.5 2.3-2.5 3.8M12 17v.3"/>'
    ),
    "sun": (
        '<circle cx="12" cy="12" r="4"/><path d="M12 2.5v2M12 19.5v2M2.5 12h2M19.5 12h2M5.3 5.3l1.4 1.4M17.3 17.3l1.4 1.4M5.3 18.7l1.4-1.4M17.3 6.7l1.4-1.4"/>'
    ),
    "moon": ('<path d="M20 14.5A8.5 8.5 0 0 1 9.5 4a8.5 8.5 0 1 0 10.5 10.5z"/>'),
    "user": ('<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>'),
    "menu": ('<path d="M4 6h16M4 12h16M4 18h16"/>'),
    "chevron": ('<path d="m9 6 6 6-6 6"/>'),
    "play": ('<path d="M7 4.5v15l12-7.5z"/>'),
    "pause": ('<path d="M8 5v14M16 5v14"/>'),
    "stop": ('<rect x="6" y="6" width="12" height="12" rx="1.5"/>'),
    "more": (
        '<circle cx="12" cy="5.5" r="1.2"/><circle cx="12" cy="12" r="1.2"/><circle cx="12" cy="18.5" r="1.2"/>'
    ),
    "chart": ('<path d="M3 20h18M6 16l4-5 3.5 3L19 7"/>'),
    "copy": (
        '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V5.5A1.5 1.5 0 0 0 14.5 4h-9A1.5 1.5 0 0 0 4 5.5v9A1.5 1.5 0 0 0 5.5 16H8"/>'
    ),
    "replay": ('<path d="M4 12a8 8 0 1 0 2.3-5.7M4 4v4.5h4.5"/>'),
    "columns": ('<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16M15 4v16"/>'),
    "logo": ('<path d="M5 19c7 0 14-5.5 14-15C11 4 5 9.5 5 19zM5 19l7.5-7.5"/>'),
}


def icon(name: str) -> Markup:
    """The icon `name` as inline SVG. An unknown name draws an empty box, never an error on the page."""
    return Markup(
        '<svg class="ic" viewBox="0 0 24 24" width="20" height="20" aria-hidden="true" focusable="false" '
        'fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
        f"{ICONS.get(name, '')}</svg>"
    )
