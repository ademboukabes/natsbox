# Relay Strategies

`nats-outbox` is designed with pluggable strategies for how the Relay extracts events from the database. 

Currently, the V1 architecture relies on a highly optimized Polling strategy. A WAL (Write-Ahead Log) Tailing strategy is planned for V2.

## V1 — The Polling Strategy

To start the relay with the polling strategy (currently the only available strategy and the default):

```bash
nats-outbox relay start --strategy polling
```

### How it works: Drain-then-Sleep

The polling strategy uses a single asynchronous event loop to query the `outbox_events` table. However, it does not sleep blindly. It uses a **Drain-then-Sleep** pattern to optimize for both high throughput and low database load:

1. The relay queries for a batch of `pending` events (limited by `OUTBOX_BATCH_SIZE`).
2. If it finds events, it processes them, marks them as `published`, and **immediately** loops to query again (zero sleep). It drains the queue as fast as Postgres and NATS can handle.
3. Only when a query returns `0` events does the relay go to sleep for `OUTBOX_POLLING_INTERVAL` seconds.

### Concurrency: `SELECT FOR UPDATE SKIP LOCKED`

The hot query executed by the relay looks like this:
```sql
SELECT * FROM outbox_events
WHERE status = 'pending' AND scheduled_at <= now()
ORDER BY scheduled_at, id
LIMIT batch_size
FOR UPDATE SKIP LOCKED
```

The `SKIP LOCKED` clause is what allows you to run multiple instances of the Relay concurrently. When an instance selects a batch of rows, it acquires a row-level write lock on them. If another relay instance executes the query at the exact same moment, it simply skips the locked rows and fetches the next available batch.

### Summary of V1 (Polling)
| Feature | Details |
|---|---|
| **Latency** | Up to `OUTBOX_POLLING_INTERVAL` (default 1s) when idle. Near-zero when under load. |
| **DB load** | Constant light read load. Strongly mitigated by the partial B-tree index on `(status) WHERE status='pending'`. |
| **Ordering** | FIFO within a single relay. Weakened under concurrent relay instances due to `SKIP LOCKED`. |
| **Complexity** | Very Low. Does not require Postgres superuser or replication slots. |

---

## V2 — WAL Tailing (Planned)

The V2 strategy will use PostgreSQL's Logical Replication (`pgoutput` via `psycopg3`). 

Instead of querying the table, the relay will stream the PostgreSQL Write-Ahead Log (WAL) directly. When a `COMMIT` happens in your application, the relay will receive the event instantly without polling.

```bash
nats-outbox relay start --strategy wal # NotImplementedError
```

### Summary of V2 (WAL)
| Feature | Details |
|---|---|
| **Latency** | Sub-millisecond. Reacts to the WAL `INSERT` in real-time. |
| **DB load** | Zero read polling. The replication slot streams data directly. |
| **Ordering** | Strict total order by LSN (Log Sequence Number). |
| **Complexity** | High. Requires `wal_level=logical`, a `REPLICATION` user privilege, and management of replication slots. |
