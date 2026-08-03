# Integration Guide

This guide will walk you through integrating `nats-outbox` into a new or existing Python application, from installation to publishing your first reliable event.

## Step 1: Install the package

Install the package via `pip`. It is recommended to include the `cli` and `all` extras if you plan to run the relay process and expose Prometheus metrics.

```bash
pip install nats-outbox[cli,all]
```

## Step 2: Set up the Database

You need to create the `outbox_events` and `inbox_events` tables in your PostgreSQL database.

If you use Alembic, please read the dedicated [Alembic Integration](alembic-integration.md) guide.

If you want to create them programmatically (for example, in a FastAPI lifespan event):

```python
from sqlalchemy.ext.asyncio import create_async_engine
from nats_outbox.core.models import create_tables

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost:5432/myapp")
await create_tables(engine)
```

## Step 3: Write events in your business logic

The core principle of the outbox pattern is to wrap your database operations in the `outbox_transaction` context manager. Replace your manual `session.commit()` with this block:

```python
from nats_outbox.core.outbox import outbox_transaction
from sqlalchemy.ext.asyncio import AsyncSession

async def create_order(session: AsyncSession, user_id: int, amount: float):
    async with outbox_transaction(session) as tx:
        # 1. Modify your domain state
        order = Order(user_id=user_id, amount=amount)
        tx.add(order)
        
        # Flush to get the generated order.id before commit
        await session.flush() 

        # 2. Publish the event
        tx.publish_event(
            subject="order.created",
            payload={"order_id": str(order.id), "amount": amount},
            aggregate_id=str(order.id),
            aggregate_type="Order",
        )
    # The order row and the outbox event are now committed atomically!
```

> [!WARNING]
> Do **not** call `session.commit()` yourself inside or after the `outbox_transaction` block. The context manager handles the commit automatically upon successful exit, or rolls back if an exception occurs.

## Step 4: Configure the NATS JetStream stream

The relay will publish messages to specific NATS subjects. You must configure a JetStream Stream that listens to these subjects and has a duplicate window configured.

```bash
# Using the NATS CLI
nats stream add ORDERS \
  --subjects "order.*" \
  --storage file \
  --retention limits \
  --max-age 24h \
  --dupe-window 2m   # This must match OUTBOX_JETSTREAM_DEDUP_WINDOW
```

## Step 5: Start the Relay process

The Relay is a separate long-running worker that polls the database and pushes to NATS. It must run continuously alongside your application.

```bash
export OUTBOX_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/myapp
export OUTBOX_NATS_URL=nats://localhost:4222

nats-outbox relay start
```

For more details on how to deploy this relay in production, see [Deploying the Relay](deploying-the-relay.md).
