# feather-s3

S3-compatible files connectors for Feather Connect: `s3.Files`, a source that reads the objects a path matches, and
`s3.PartitionFiles`, a sink that writes one object per partition. Paths are fsspec URLs: `s3://`, `file://` and
`memory://`. Azure Blob and GCS work through fsspec's extras and `storage_options`, and stay experimental.

## A connection

The keys are secrets under `auth`; the endpoint and region stand at the connection's top level.

```yaml
connections:
  landing:
    connector: s3.files
    endpoint_url: https://s3.eu-central-1.amazonaws.com
    region: eu-central-1
    auth:
      access_key_id: "${secret:env:S3_KEY}"
      secret_access_key: "${secret:env:S3_SECRET}"
```

## A dataset

```python
import feather_connect as fc
from feather_connect.connectors import memory, s3

orders = fc.Dataset(
    "orders",
    source=s3.Files("landing", path="s3://landing/orders/*.csv", format="csv", stable_after="2m"),
    key="order_id",
    strategy=fc.strategy.by_stamp("_object_mtime"),
)

flow = fc.Flow(
    "orders-from-files",
    reads=orders,
    deliver=[fc.to(memory.Sink("warehouse"))],
    triggers=[fc.every("15m")],
)
```

`Files` reads `csv` (with `delimiter` and `encoding`; a UTF-8 file may start with a byte order mark), `json` (one
array of objects), `jsonl` and `parquet`, in batches of `batch_rows`. Each row gets `_object_path` and
`_object_mtime`, the object's modified time, which a `by_stamp` dataset uses as its stamp: a read then takes only the
objects modified since the watermark. An object modified less than `stable_after` ago is left for the next read, as
it may still be being written. A CSV row with the wrong number of fields, or a JSON line that does not parse, goes to
quarantine with its text, and the rest of the file is read. CSV types are inferred from the first block; a column
whose later value does not fit fails the file naming it, and `column_types={"qty": "string"}` fixes its type. No
matching object is an empty read, and an object gone since the listing is a transient error.

`PartitionFiles(path_template="s3://lake/sales/day={partition}/data.parquet", format="parquet")` supports
`replace_partition`, which stages a version's chunks under `_staging/` and writes the partition's object in one PUT
on the last chunk, and `append`, which writes each chunk as an object of its own. It writes the `_fc_key`,
`_fc_partition`, `_fc_version_id`, `_fc_op` and `_fc_applied_at` columns and keeps each chunk's result under
`_feather_applied/`, so that a chunk sent again changes nothing. Credentials never appear in its errors.
