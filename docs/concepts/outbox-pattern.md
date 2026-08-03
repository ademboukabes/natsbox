# The Transactional Outbox Pattern

In a distributed architecture, an application frequently needs to modify its internal state (e.g., insert an order into a database) and notify other systems of this change (e.g., publish an `order.created` event on a message broker).

This operation raises the **Dual-Write Problem**.

## The Dual-Write Problem

If you write to the database and then publish to the broker without a distributed transaction, you are exposed to silent failures:

1. **Broker unavailability:** The SQL transaction succeeded, but the message cannot be sent. The event is lost.
2. **Application crash:** The application crashes exactly after the SQL `COMMIT` but before the line of code that sends the message could execute.
3. **Reversed order (Phantom Event):** If you send the message *before* committing the database, and the `COMMIT` fails (constraint violation, deadlock), the broker has received an event for data that does not exist.

The "2-Phase Commit" (2PC) is often not supported (NATS does not support 2PC) and strongly couples the availability of your database to that of your broker.

## The Solution: The Transactional Outbox

The **Transactional Outbox** pattern solves this problem by using a single local atomic database transaction.

Instead of sending the message directly to the broker, the application stores the event in a dedicated table (`outbox_events`) **within the same SQL transaction** as the business logic modification.

Since this is a standard SQL transaction (ACID):
- If the business logic fails, the event is not inserted.
- If the event insertion fails, the business logic is rolled back.
- Success guarantees that both the event and the business data are saved together.

An independent **Relay** then polls this `outbox_events` table asynchronously and publishes the messages to the broker.

## Guarantees provided by `nats-outbox`

### Atomicity
`nats-outbox` guarantees the atomicity of your local operations. Thanks to the `outbox_transaction` context manager, committing your domain entity and inserting the event into the outbox are executed in a single SQLAlchemy transactional block.

### At-Least-Once Delivery
If the application process crashes, the events are safely stored in Postgres. If the Relay process crashes before receiving an ACK from NATS JetStream, the event is not marked as published. Upon restart, the Relay will resend the event. You are guaranteed that a stored event will be delivered at least once.

### Limits and Trade-offs: Nesting and Flushing
In its current version, `outbox_transaction` always commits the session upon exiting the `async with` block. It does not support nested transactions.
If you need to stage multiple independent logical units and manage the commit yourself, you can instantiate `OutboxTransaction` directly and manually call the internal `tx._flush_events()` method before your own `session.commit()`.
