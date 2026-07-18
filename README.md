# nats-outbox

**Transactional Outbox Pattern for Python — PostgreSQL + NATS JetStream**

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## What problem does this solve?

In event-driven architectures, a service typically needs to do two things atomically:

1. **Persist** a domain state change to PostgreSQL (e.g. insert a `Photo` row)
2. **Publish** a corresponding event to NATS JetStream (e.g. `photo.created`)

These two operations touch two different systems with no shared distributed transaction. This creates silent failure scenarios — the **dual-write problem**:

- DB commit succeeds → service crashes → event **lost** (silent inconsistency)
- Event published → DB rollback → **phantom event** (consumers react to nothing)
- NATS temporarily down → event lost or duplicated without a retry mechanism

**nats-outbox** solves this with the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html):

> Write events to a dedicated `outbox_events` table **in the same SQL transaction** as your domain data. A separate relay process reads this table and publishes events reliably to NATS JetStream.

## Features

- **Atomic writes** — event and domain data commit together (ACID guaranteed by Postgres)
- **At-least-once delivery** — events are never lost, even across relay crashes
- **Idempotent publishes** — `Nats-Msg-Id = event_id` enables JetStream dedup on retry
- **Exponential backoff** — failed publishes are retried with backoff via `scheduled_at`
- **Dead-letter queue** — events that fail `max_retries` times are flagged (not silently dropped)
- **Inbox pattern** — optional consumer-side deduplication via `INSERT ... ON CONFLICT`
- **Delayed publish** — schedule events in the future with `scheduled_at`
- **Horizontal scaling** — multiple relay instances are safe via `SELECT FOR UPDATE SKIP LOCKED`
- **Prometheus metrics** — pending count, publish latency, failure rate
- **Framework-agnostic core** — works with any SQLAlchemy async session

## Quick Start

```bash
pip install nats-outbox[cli,all]
```

```python
from sqlalchemy.ext.asyncio import AsyncSession
from nats_outbox.core.outbox import outbox_transaction

async def create_photo(session: AsyncSession, url: str, user_id: int):
    async with outbox_transaction(session) as tx:
        photo = Photo(url=url, user_id=user_id)
        tx.add(photo)
        await session.flush()  # get photo.id before commit

        tx.publish_event(
            subject="photo.created",
            payload={"photo_id": str(photo.id), "user_id": user_id},
            aggregate_id=str(photo.id),
            aggregate_type="Photo",
        )
    # Both photo row AND outbox event are committed atomically
```

Start the relay:

```bash
OUTBOX_DATABASE_URL=postgresql+asyncpg://... \
OUTBOX_NATS_URL=nats://localhost:4222 \
nats-outbox relay start
```

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │           Your Application               │
                    │                                          │
  HTTP Request ────►│  async with outbox_transaction(db) as tx:│
                    │      tx.add(Photo(...))                  │
                    │      tx.publish_event("photo.created")   │
                    │                           │              │
                    └───────────────────────────┼──────────────┘
                                                │ SQL COMMIT (atomic)
                                                ▼
                    ┌─────────────────────────────────────────┐
                    │         PostgreSQL                       │
                    │                                          │
                    │  photos          outbox_events           │
                    │  ┌──────────┐   ┌─────────────────────┐ │
                    │  │ id  url  │   │ status  subject  ... │ │
                    │  │ 1   ...  │   │ pending photo.created│ │
                    │  └──────────┘   └─────────────────────┘ │
                    └───────────────────────────┬──────────────┘
                                                │ SELECT FOR UPDATE SKIP LOCKED
                                                ▼
                    ┌─────────────────────────────────────────┐
                    │         Relay (separate process)         │
                    │                                          │
                    │  PollingRelay.start()                    │
                    │    ├── poll every 1s                     │
                    │    ├── publish to JetStream              │
                    │    └── mark status=published             │
                    └───────────────────────────┬──────────────┘
                                                │ publish (Nats-Msg-Id=event_id)
                                                ▼
                    ┌─────────────────────────────────────────┐
                    │         NATS JetStream                   │
                    │                                          │
                    │  Stream: PHOTOS                          │
                    │  Subject: photo.created                  │
                    │  Dedup window: 2 min (Nats-Msg-Id)       │
                    └─────────────────────────────────────────┘
```

## Table Schema

The `outbox_events` table is the heart of the pattern:

| Column | Type | Purpose |
|---|---|---|
| `id` | BIGSERIAL | Sequential PK — no B-tree fragmentation |
| `event_id` | UUID | Business ID — used as `Nats-Msg-Id` header (stable across retries) |
| `subject` | TEXT | NATS subject |
| `headers` | JSONB | NATS headers (always includes `Nats-Msg-Id`) |
| `payload` | JSONB | Event body |
| `status` | TEXT | `pending` → `published` or `failed` |
| `retry_count` | SMALLINT | Failed publish attempts |
| `last_error` | TEXT | Last exception for debugging |
| `aggregate_id` | TEXT | Source entity ID (for ordering & observability) |
| `aggregate_type` | TEXT | Source entity class name |
| `created_at` | TIMESTAMPTZ | When event was written to outbox |
| `updated_at` | TIMESTAMPTZ | Last relay modification |
| `scheduled_at` | TIMESTAMPTZ | Earliest publish time (backoff pushes this forward) |
| `published_at` | TIMESTAMPTZ | When JetStream ACK was received |

## Relay Strategies

### V1 — Polling (available now)

```bash
nats-outbox relay start --strategy polling
```

| | |
|---|---|
| **Latency** | Up to `OUTBOX_POLLING_INTERVAL` (default 1s) |
| **DB load** | Constant light read load (mitigated by partial index) |
| **Ordering** | FIFO within single relay; weakened under concurrent instances |
| **Complexity** | Low — no Postgres superuser, no replication slot |

### V2 — WAL Tailing (planned)

```bash
nats-outbox relay start --strategy wal  # NotImplementedError — coming soon
```

| | |
|---|---|
| **Latency** | Sub-millisecond (reacts to WAL INSERT in real time) |
| **DB load** | Zero read polling |
| **Ordering** | Strict by LSN (total order) |
| **Complexity** | Requires `wal_level=logical`, REPLICATION privilege, replication slot |

## Configuration

All configuration is via environment variables (prefix: `OUTBOX_`):

```bash
OUTBOX_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/myapp
OUTBOX_NATS_URL=nats://localhost:4222
OUTBOX_POLLING_INTERVAL=1.0       # seconds between idle polls
OUTBOX_BATCH_SIZE=100             # events per tick
OUTBOX_MAX_RETRIES=5              # before dead-lettering
OUTBOX_RETENTION_DAYS=7           # cleanup window for published events
OUTBOX_JETSTREAM_DEDUP_WINDOW=120 # match your stream's DuplicateWindow
OUTBOX_METRICS_PORT=9090          # 0 = disabled
```

## Examples

See the `examples/` directory for a complete working FastAPI integration:
- [examples/fastapi_app.py](examples/fastapi_app.py)

## Running Integration Tests

Tests use real Docker containers via [testcontainers](https://testcontainers-python.readthedocs.io/):

```bash
make test
```

## Roadmap

**V2 — WAL Tailing (Logical Replication)**
We are planning to implement a sub-millisecond latency relay using Postgres logical replication (`pgoutput`) via `psycopg3`. This will replace the polling mechanism for high-throughput systems.
See the full technical roadmap in [implementation_plan.md](implementation_plan.md).

## License

MIT
