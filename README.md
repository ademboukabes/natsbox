# natsbox

**Transactional Outbox Pattern for Python — PostgreSQL + NATS JetStream**

[![CI](https://github.com/ademboukabes/natsbox/actions/workflows/ci.yml/badge.svg)](https://github.com/ademboukabes/natsbox/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/nats-outbox.svg)](https://pypi.org/project/nats-outbox/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Typed: strict](https://img.shields.io/badge/typing-strict%20mypy%20%2B%20pyright-informational)](pyrightconfig.json)

> **📚 Full Documentation is available at [https://ademboukabes.github.io/natsbox/](https://ademboukabes.github.io/natsbox/)**

## What problem does this solve?

In event-driven architectures, a service typically needs to do two things atomically:
1. **Persist** a domain state change to PostgreSQL (e.g. insert an `Order` row).
2. **Publish** a corresponding event to NATS JetStream (e.g. `order.created`).

Doing this without a distributed transaction exposes your system to **silent inconsistencies** (events lost if the app crashes before publishing, or phantom events sent if the DB rolls back).

`nats-outbox` solves this with the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html). It writes events to a dedicated `outbox_events` table **in the same SQL transaction** as your domain data. A separate relay process reads this table and publishes events reliably to NATS JetStream, guaranteeing **at-least-once delivery**.

## Quick Start

```bash
pip install nats-outbox[cli,all]
```

```python
from sqlalchemy.ext.asyncio import AsyncSession
from nats_outbox.core.outbox import outbox_transaction

async def create_order(session: AsyncSession, user_id: int, amount: float):
    # This block automatically commits your session on success!
    async with outbox_transaction(session) as tx:
        order = Order(user_id=user_id, amount=amount)
        tx.add(order)
        await session.flush()  # get order.id before commit

        # Stage the event. It is saved in PostgreSQL atomically with your Order.
        tx.publish_event(
            subject="order.created",
            payload={"order_id": str(order.id), "amount": amount},
            aggregate_id=str(order.id),
            aggregate_type="Order",
        )
```

Start the background relay process to push events to NATS:

```bash
OUTBOX_DATABASE_URL=postgresql+asyncpg://... \
OUTBOX_NATS_URL=nats://localhost:4222 \
nats-outbox relay start
```

## Documentation

Please visit the [official documentation](https://ademboukabes.github.io/natsbox/) for the complete Integration Guide, Architecture details, Schema definitions, and Relay Configuration Reference.

## License
MIT
