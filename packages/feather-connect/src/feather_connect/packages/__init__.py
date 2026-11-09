"""Flow packages (flow packages design §4): a project built into a Python wheel with a manifest, published to a registry.

`manifest` says what a package holds and checks it against the JSON Schema next to it, `build` turns a project into a
reproducible wheel, `wheel` reads one back, and `index` reads a Python package index (PEP 503).
"""
