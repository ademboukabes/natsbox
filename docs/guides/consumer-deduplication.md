# Consumer Deduplication (Inbox Pattern)

The Transactional Outbox pattern guarantees that a message will be delivered *at least once*. This implies that your message broker (NATS JetStream) may sometimes deliver the exact same event multiple times.

This can happen if:
- The Relay publishes the message to NATS, but crashes before it can receive the ACK and update the database.
- A network partition occurs between the Relay and NATS.
- NATS JetStream delivers a message to your consumer, but your consumer crashes before acknowledging it.

To safely process events without corrupting your data, your consumers must be **idempotent**. 
`nats-outbox` provides a built-in solution for this: the **Inbox Pattern**.

## Using the `InboxDeduplicator`

The `inbox_events` table stores a record of every event processed by a specific consumer group. By checking this table before processing an event, you can skip duplicates.

```python
import uuid
from nats_outbox.core.inbox import InboxDeduplicator
from sqlalchemy.ext.asyncio import AsyncSession

async def handle_order_created(session: AsyncSession, event_id: uuid.UUID, payload: dict):
    # Open a transaction for your consumer logic
    async with session.begin():
        
        # 1. Initialize the deduplicator with a logical consumer group name
        dedup = InboxDeduplicator(session, consumer_group="billing-service")
        
        # 2. Check if this event was already processed
        if await dedup.is_duplicate(event_id):
            return  # The event was already processed safely. Skip it.

        # 3. Perform your business logic
        # (e.g. generate an invoice based on the order)
        invoice = Invoice(order_id=payload["order_id"], amount=payload["amount"])
        session.add(invoice)

    # The inbox record and the new invoice are committed atomically!
```

## How it works

When `is_duplicate(event_id)` is called, it attempts to `INSERT` a row into the `inbox_events` table using the `(event_id, consumer_group)` composite key. 

- If the insert succeeds, it returns `False` (not a duplicate) and the row remains locked in the current transaction.
- If the insert fails due to a unique constraint violation, it means the event has already been processed by this consumer group. It returns `True` (duplicate).

Since this check occurs inside your consumer's database transaction, the inbox record is only persisted if your consumer successfully completes its business logic and commits.

## Why `consumer_group`?

In a microservices architecture, an event like `order.created` might be consumed by multiple distinct services (e.g., a Billing Service and a Shipping Service). 

By passing `consumer_group="billing-service"`, you ensure that the deduplication is isolated to that specific service. The Shipping Service will use `consumer_group="shipping-service"` and will maintain its own record of processed events.
