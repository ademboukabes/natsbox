# Comparisons and Trade-offs

When choosing an architecture for reliable event publishing, it is crucial to understand the available alternatives and the specific trade-offs each solution makes.

## `nats-outbox` vs Debezium / Kafka Connect

Debezium is the industry standard for Change Data Capture (CDC). It tails the database WAL and publishes to Apache Kafka.

- **Complexity:** Debezium requires running a Kafka Connect cluster, managing Zookeeper/KRaft, and deeply understanding Kafka partitions. `nats-outbox` is a lightweight Python process that you can deploy alongside your app using the tools you already know.
- **Payload:** Debezium typically publishes raw database row changes (`{"before": {...}, "after": {...}}`). `nats-outbox` publishes explicit business events (`{"order_id": "123", "amount": 50}`), completely decoupling your public event schema from your internal database schema.
- **Broker:** Debezium targets Kafka. `nats-outbox` targets NATS JetStream.

## `nats-outbox` vs "Rolling your own"

It's tempting to write a quick `asyncio.sleep(1)` loop that reads a table and publishes to NATS. However, `nats-outbox` solves several hard distributed systems problems for you:

1. **Idempotence & Duplicate Windows:** Native integration with `Nats-Msg-Id` guarantees that a lost ACK does not result in duplicate processing.
2. **Exponential Backoff:** Bad payloads don't block the queue. They are scheduled in the future and eventually dead-lettered.
3. **Safe Horizontal Scaling:** `SELECT FOR UPDATE SKIP LOCKED` ensures you can run 5 replicas of the relay without duplicating publishes or causing deadlocks.

---

## The Ordering Trade-off (Important)

The canonical definition of the Transactional Outbox pattern (as popularized by microservices.io) guarantees that messages are published to the broker in the exact order they were inserted.

**`nats-outbox` makes a deliberate design choice to relax strict global ordering in exchange for horizontal scalability.**

### Why?
To support horizontal scaling without partitioning, `nats-outbox` uses PostgreSQL's `SKIP LOCKED`.
If Relay A locks rows 1-100, and Relay B polls immediately after, Relay B will skip rows 1-100 and process rows 101-200.
If Relay B finishes publishing before Relay A, events 101-200 will arrive at NATS JetStream *before* events 1-100.

### The Impact
If your business logic absolutely requires that `T1` is processed before `T2` for the *same aggregate* (e.g., `user.created` must arrive before `user.updated`), you have two choices with the V1 Polling strategy:

1. **Run only one instance of the Relay.** A single relay polling sequentially guarantees strict FIFO ordering.
2. **Design commutative consumers.** Build consumers that do not strictly depend on order, or use the `aggregate_id` and a version number in the payload to handle out-of-order events.

> [!NOTE]
> **V2 (WAL Tailing)** will resolve this trade-off. By tailing the PostgreSQL WAL via logical replication, the V2 relay will guarantee strict total ordering based on the Log Sequence Number (LSN), even under massive scale.
