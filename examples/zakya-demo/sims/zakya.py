"""The demo's stand-in for Zakya: the simulator of the feather-zakya package, raising new invoices as time passes and
paying some, with a few calls answering 429 or 503 on purpose. With `--state`, the invoices are kept in a JSON file, so
a restart carries on where it stopped.

    python sims/zakya.py --port 29100 --state .data/zakya.json
"""

from feather_connect.connectors.zakya.testing import ZakyaSim, main, running

__all__ = ["ZakyaSim", "main", "running"]

if __name__ == "__main__":
    main()
