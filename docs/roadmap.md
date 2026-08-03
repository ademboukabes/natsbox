# Roadmap

`nats-outbox` is actively maintained and used in production. Here is what is planned for future releases.

## V1 — Stabilization and Ecosystem (Current Focus)

- **FastAPI / Dependency Injection helpers:** Provide out-of-the-box `Depends()` injectables to make using `outbox_transaction` even cleaner in FastAPI routes.
- **Relay CLI improvements:** Add commands to inspect the dead-letter queue interactively, rather than just requeuing everything.
- **OpenTelemetry Tracing:** Inject trace IDs directly into the NATS headers so that an event can be traced from the HTTP request all the way to the consuming service.

## V2 — WAL Tailing

The major feature for V2 is replacing the polling mechanism with **Logical Replication (WAL Tailing)** using `pgoutput` via `psycopg3`.

### Why WAL Tailing?
1. **Sub-millisecond latency:** Instead of waiting for the next polling tick (e.g. 1 second), the relay streams events the moment the `COMMIT` happens in PostgreSQL.
2. **Zero read load:** Eliminates the continuous `SELECT FOR UPDATE SKIP LOCKED` polling, which saves CPU and DB connections on highly loaded databases.
3. **Strict Global Ordering:** Events are streamed strictly in the order of their Log Sequence Number (LSN). This guarantees perfect chronological ordering without sacrificing horizontal scalability, resolving the trade-off present in V1.

### Requirements for V2
To prepare for V2, ensure that your PostgreSQL instance has `wal_level = logical` configured, and that the database user running the Relay will have the `REPLICATION` privilege.
