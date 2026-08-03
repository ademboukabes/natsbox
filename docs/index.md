# natsbox

**Transactional Outbox Pattern for Python — PostgreSQL + NATS JetStream**

[![CI](https://github.com/ademboukabes/natsbox/actions/workflows/ci.yml/badge.svg)](https://github.com/ademboukabes/natsbox/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/nats-outbox.svg)](https://pypi.org/project/nats-outbox/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Typed: strict](https://img.shields.io/badge/typing-strict%20mypy%20%2B%20pyright-informational)](pyrightconfig.json)

## What problem does this solve?

In event-driven architectures, a service typically needs to do two things atomically:

1. **Persist** a domain state change to PostgreSQL (e.g. insert a `Photo` row)
2. **Publish** a corresponding event to NATS JetStream (e.g. `photo.created`)

These two operations touch two different systems with no shared distributed transaction. This creates silent failure scenarios — the **dual-write problem**:

- DB commit succeeds → service crashes → event **lost** (silent inconsistency)
- Event published → DB rollback → **phantom event** (consumers react to nothing)
- NATS temporarily down → event lost or duplicated without a retry mechanism

**natsbox** solves this with the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html):

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

## Integration Guide

### Step 1 — Install

```bash
pip install nats-outbox[cli,all]
```

### Step 2 — Create the database table

**Option A — Raw SQL (fastest)**

```bash
psql $DATABASE_URL -f https://raw.githubusercontent.com/ademboukabes/natsbox/main/migrations/001_create_outbox_events.sql
```

Or run the local file if you have the repo cloned:

```bash
psql $DATABASE_URL -f migrations/001_create_outbox_events.sql
```

**Option B — SQLAlchemy (programmatic, e.g. in a FastAPI lifespan)**

```python
from nats_outbox.core.models import create_tables
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("postgresql+asyncpg://...")
await create_tables(engine)  # creates outbox_events + inbox_events + trigger
```

**Option C — Alembic** — see [docs/alembic.md](docs/alembic.md).

### Step 3 — Write events in your business logic

Replace direct `session.commit()` with `outbox_transaction`:

```python
from nats_outbox.core.outbox import outbox_transaction
from sqlalchemy.ext.asyncio import AsyncSession

async def create_order(session: AsyncSession, user_id: int, amount: float):
    async with outbox_transaction(session) as tx:
        order = Order(user_id=user_id, amount=amount)
        tx.add(order)
        await session.flush()  # needed to get order.id before commit

        tx.publish_event(
            subject="order.created",
            payload={"order_id": str(order.id), "amount": amount},
            aggregate_id=str(order.id),
            aggregate_type="Order",
        )
    # order row + outbox event committed atomically — relay takes it from here
```

> **Note:** Do **not** call `session.commit()` yourself — `outbox_transaction` does it. If an exception occurs inside the block, both the order row and the event are rolled back.

### Step 4 — Configure the NATS JetStream stream

The relay publishes to NATS subjects. You must have a stream that covers those subjects:

```bash
# Using the nats CLI
nats stream add ORDERS \
  --subjects "order.*" \
  --storage file \
  --retention limits \
  --max-age 24h \
  --dupe-window 2m   # must match OUTBOX_JETSTREAM_DEDUP_WINDOW
```

### Step 5 — Start the relay process

The relay is a separate long-running process. Run it alongside your application:

```bash
OUTBOX_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/myapp \
OUTBOX_NATS_URL=nats://localhost:4222 \
nats-outbox relay start
```

In **Docker Compose**, add a relay service:

```yaml
services:
  relay:
    image: python:3.12-slim
    command: nats-outbox relay start
    environment:
      OUTBOX_DATABASE_URL: postgresql+asyncpg://user:pass@db:5432/myapp
      OUTBOX_NATS_URL: nats://nats:4222
```

In **Kubernetes**, deploy it as a separate `Deployment` (1 replica minimum, scale horizontally for throughput — `SELECT FOR UPDATE SKIP LOCKED` handles concurrent relay instances safely).

### Step 6 — (Optional) Consumer-side deduplication

If a consumer receives the same event twice (e.g. relay crash after publish but before marking as published), use the Inbox Pattern to deduplicate:

```python
from nats_outbox.core.inbox import InboxDeduplicator
import uuid

async def handle_order_created(session: AsyncSession, event_id: uuid.UUID, payload: dict):
    async with session.begin():
        dedup = InboxDeduplicator(session, consumer_group="billing-service")
        if await dedup.is_duplicate(event_id):
            return  # already processed — skip safely

        # ... your business logic here ...
        # The inbox row and your business effect commit atomically
```

### Step 7 — (Optional) Recover dead-lettered events

If an event fails `OUTBOX_MAX_RETRIES` times (e.g. a bad NATS subject with no stream), it is dead-lettered (`status=failed`). Recover with:

```bash
# Preview first
nats-outbox relay requeue --dry-run

# Requeue all failed events
nats-outbox relay requeue

# Requeue a specific event
nats-outbox relay requeue --event-id 550e8400-e29b-41d4-a716-446655440000
```

### Step 8 — (Optional) Prometheus metrics

```bash
OUTBOX_METRICS_PORT=9090 nats-outbox relay start
```

Metrics exposed at `http://localhost:9090/metrics`:

| Metric | Type | Alert when |
|---|---|---|
| `outbox_events_pending` | Gauge | > threshold for > N minutes |
| `outbox_events_published_total` | Counter | — |
| `outbox_events_failed_total` | Counter | > 0 |
| `outbox_publish_latency_seconds` | Histogram | p99 > SLA |

---

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

## License

MIT
