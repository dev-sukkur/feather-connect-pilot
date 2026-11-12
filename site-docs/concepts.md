# Concepts

| Term | What it is |
|---|---|
| **Flow** | Python that says what to read, where to deliver it, and when: `fc.Flow(name, reads=dataset, deliver=[routes], triggers=[...])` |
| **Dataset** | What a source reads, with a key and a strategy: `replace`, `by_stamp` (rows changed since a watermark), `snapshot_diff`, or pushed by a webhook |
| **Partition** | A business slice of a dataset, such as a day or a store: the unit you re-run, replay or close |
| **Version** | What one read of a partition produced: the rows added, changed and deleted since the last version, sealed in the spool |
| **Route** | One destination of a flow: a sink, a map from record to payload, a mode (`upsert`, `append`, `replace_partition`) and its own policies |
| **Delivery** | One version on one route: `pending`, `running`, `applied`, `failed`, `held`, `skipped` or `compensated`, in the route's ledger |
| **Spool** | Where versions wait in Postgres, in chunks, until every route has applied them |
| **Environment** | dev, qa, uat or prod: its own connections, secrets, budgets and settings. A *protected* one needs reasons and a second person |
| **Package** | A versioned wheel of flows, installed from a registry into an environment and activated flow by flow |

## What Feather guarantees

- A route applies each version **once**, and the versions of a partition **in order**. A crash mid-apply resends the
  same chunk with the same idempotency key, and the sink drops the repeat.
- Routes are **independent**: one that fails or is held blocks only its own partition.
- Nothing is lost when a worker, a server or Postgres restarts: the work is in durable workflows (DBOS) and the spool.
- Every change by a person, from the dashboard, the API or the CLI, is **audited** with who, what and why.
