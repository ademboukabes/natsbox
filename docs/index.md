# nats-outbox

**Transactional Outbox Pattern for Python — PostgreSQL + NATS JetStream**

## The Problem

In event-driven architectures, you often need to save data to a database (e.g. `INSERT INTO orders`) and notify other services via a message broker (e.g. publish `order.created` to NATS).
If you do this without a distributed transaction, you risk **silent inconsistencies**: the database commit might succeed, but the application crashes before the message is published.

## The Solution

`nats-outbox` ensures these two actions are **atomic**. You stage your event alongside your database changes. They are saved in a single PostgreSQL transaction. A background relay then guarantees the event is published to NATS JetStream *at least once*.

---

## Getting Started in 2 Minutes

### 1. Install

```bash
pip install nats-outbox[cli,all]
```

### 2. Prepare the Database

Your PostgreSQL database needs the `outbox_events` table. For a quick start without Alembic, run:

```python
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from nats_outbox.core.models import create_tables

async def setup():
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost:5432/myapp")
    await create_tables(engine)

asyncio.run(setup())
```

### 3. Write Atomic Events

Wrap your database operations in the `outbox_transaction` context manager. It automatically commits the session and stages the event atomically.

```python
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from nats_outbox.core.outbox import outbox_transaction

async def main():
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost:5432/myapp")
    
    async with AsyncSession(engine) as session:
        async with outbox_transaction(session) as tx:
            
            # 1. Your domain logic
            # tx.add(Order(user_id=1, amount=100.0))
            
            # 2. Stage the event
            tx.publish_event(
                subject="order.created",
                payload={"order_id": "123", "amount": 100.0},
                aggregate_id="123",
                aggregate_type="Order"
            )
        # Session is automatically committed here! 
        # Event is safely in the outbox_events table.

asyncio.run(main())
```

### 4. Start the Relay

Run the Relay as a background process. It will automatically detect pending events in PostgreSQL and publish them to NATS JetStream.

```bash
export OUTBOX_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/myapp
export OUTBOX_NATS_URL=nats://localhost:4222

nats-outbox relay start
```

*That's it! Your events are now guaranteed to be delivered.*

---

## Next Steps
- Head to the [Integration Guide](guides/integration-guide.md) to see how to use `nats-outbox` in a real application.
- Learn about the [Architecture](concepts/architecture.md) and the [Outbox Pattern](concepts/outbox-pattern.md).
