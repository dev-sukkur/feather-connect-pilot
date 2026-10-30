"""Worker process entry point. Tests start and kill these processes."""

import argparse
import json


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--engine", required=True, choices=["dbos"])
    p.add_argument("--name", required=True)
    p.add_argument("--labels", default="{}")
    p.add_argument("--slots", type=int, default=4)
    p.add_argument("--version", default="v1")
    p.add_argument("--auto-upgrade", action="store_true")
    a = p.parse_args()
    from spike.dbos_engine import serve

    serve(a.name, json.loads(a.labels), a.slots, a.version, auto_upgrade=a.auto_upgrade)


if __name__ == "__main__":
    main()
