"""A small stand-in for SAP through CPI, for the demo: the `feather-sap-cpi` package's simulated tenant as an HTTP
server. It takes client-credentials tokens, hands out CSRF tokens, keeps each posted invoice once by its
`Idempotency-Key` and answers a billing document number, and takes a posting back on `/http/invoices/reverse`.

    python sims/cpi.py --port 29102
"""

import argparse
import threading

from feather_connect.connectors.sap_cpi.testing import CpiSim, serve  # pyright: ignore[reportMissingImports]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=29102)
    args = parser.parse_args()
    serve(CpiSim(key="invoice_id", csrf=True), args.port)
    print(f"CPI simulator on http://127.0.0.1:{args.port}", flush=True)
    threading.Event().wait()


if __name__ == "__main__":
    main()
