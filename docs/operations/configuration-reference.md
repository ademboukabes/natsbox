# Configuration Reference

The `nats-outbox` Relay is configured entirely through Environment Variables. 

All environment variables must be prefixed with `OUTBOX_`.

## Database and Broker Connection

| Variable | Type | Default | Description |
|---|---|---|---|
| `OUTBOX_DATABASE_URL` | `string` | *(Required)* | The SQLAlchemy connection string for the database holding the outbox table. Must use an asynchronous driver (e.g. `postgresql+asyncpg://...`). |
| `OUTBOX_NATS_URL` | `string` | *(Required)* | The NATS connection string (e.g. `nats://localhost:4222`). Can be a comma-separated list of servers. |

## Relay Tuning (Polling Strategy)

| Variable | Type | Default | Description |
|---|---|---|---|
| `OUTBOX_POLLING_INTERVAL` | `float` | `1.0` | The number of seconds the Relay will sleep when the outbox is completely empty. Lowering this reduces idle latency but increases database read load. |
| `OUTBOX_BATCH_SIZE` | `int` | `100` | The maximum number of events fetched from the database in a single tick. Increase for higher throughput, but avoid excessively large batches that hold DB locks too long. |

## Reliability and Retries

| Variable | Type | Default | Description |
|---|---|---|---|
| `OUTBOX_MAX_RETRIES` | `int` | `5` | The number of times the Relay will attempt to publish an event before giving up and setting its status to `failed` (dead-lettering). |
| `OUTBOX_JETSTREAM_DEDUP_WINDOW` | `int` | `120` | The time window (in seconds) for JetStream deduplication. The Relay passes this hint to NATS. It **must** match the `DuplicateWindow` configured on your target JetStream Stream. |

## Data Retention

| Variable | Type | Default | Description |
|---|---|---|---|
| `OUTBOX_RETENTION_DAYS` | `int` | `7` | Events marked as `published` are kept in the table for debugging. A background cleanup task periodically deletes published events older than this many days. |

## Observability

| Variable | Type | Default | Description |
|---|---|---|---|
| `OUTBOX_METRICS_PORT` | `int` | `0` | The port on which the Relay will expose a Prometheus `/metrics` endpoint. If set to `0`, the metrics server is completely disabled. |
