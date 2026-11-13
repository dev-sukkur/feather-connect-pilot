"""A durable engine pickles a step's error: Zakya's comes back with its code and status."""

import pickle

from feather_connect.connectors.zakya.config import ZakyaError  # pyright: ignore[reportMissingImports]


def test_a_zakya_error_survives_pickling():
    error = pickle.loads(pickle.dumps(ZakyaError("GET /items: code 57", 57, 401)))
    assert (str(error), error.code, error.status) == ("GET /items: code 57", 57, 401)
