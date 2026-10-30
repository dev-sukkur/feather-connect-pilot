import os

# Local-only throwaway container from compose.yaml.
PG_URL = os.environ.get(
    "SPIKE_DB_URL", "postgresql://feather:feather_spike@127.0.0.1:25432/feather_spike"
)
