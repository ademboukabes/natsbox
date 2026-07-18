"""
WALRelay — V2 relay stub (NOT YET IMPLEMENTED).

⚠️  This module is a documented placeholder. Implementation begins after V1
    (PollingRelay) is validated. Do not use in production.

Overview of the planned V2 approach
-------------------------------------
WAL (Write-Ahead Log) tailing via Postgres logical replication allows the relay
to react to INSERT events on outbox_events in real time, without polling.

Architecture
~~~~~~~~~~~~
Postgres logical replication emits a stream of change events (INSERT, UPDATE,
DELETE) from the WAL. Using a logical replication slot with the `pgoutput`
plugin (built into Postgres 10+), we can subscribe to changes on the
outbox_events table without superuser privileges and without external tools.

The relay consumes this stream and publishes INSERT events (new pending rows)
to NATS JetStream immediately, without a SELECT query.

Latency vs polling
~~~~~~~~~~~~~~~~~~
- Polling: up to polling_interval per event (default 1s).
- WAL tailing: sub-millisecond (bounded by network RTT to Postgres).

Prerequisites (operator checklist)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. Postgres must be configured for logical replication:
   ``wal_level = logical`` in postgresql.conf (requires restart)
2. The relay's DB user needs REPLICATION privilege:
   ``GRANT REPLICATION ON DATABASE myapp TO outbox_user;``
   or create the user with ``CREATE USER outbox_user REPLICATION;``
3. A replication slot must be created (once, idempotently):
   ``SELECT pg_create_logical_replication_slot('nats_outbox_slot', 'pgoutput');``
4. A publication must be created:
   ``CREATE PUBLICATION nats_outbox_pub FOR TABLE outbox_events;``

⚠️  Replication slot risk: if the relay is offline for an extended period,
Postgres will hold WAL files that the slot hasn't consumed. This can cause
disk exhaustion. Monitor ``pg_replication_slots.lag_bytes`` and set
``max_slot_wal_keep_size`` (Postgres 13+).

Planned implementation
~~~~~~~~~~~~~~~~~~~~~~~
- Use ``psycopg`` (psycopg3) in async mode with:
  ``conn.set_session(autocommit=True)``
  ``conn.start_replication(slot_name=..., decode=True)``
- Parse ``pgoutput`` protocol messages to extract INSERT events.
- Use LSN (Log Sequence Number) for crash recovery: persist the last
  processed LSN in a ``outbox_wal_checkpoint`` table and pass it to
  ``start_replication(start_lsn=...)`` on reconnection.
- Handle reconnects with exponential backoff.
- Fall back to polling on replication lag > threshold (configurable).

Ordering guarantees
~~~~~~~~~~~~~~~~~~~~
- WAL tailing provides strict ordering by LSN — stronger than polling.
- Events for the same aggregate are always processed in insertion order
  because Postgres WAL is a total ordered log.
- This is the primary ordering advantage over the polling relay.

References
~~~~~~~~~~~
- https://www.postgresql.org/docs/current/logical-replication.html
- https://www.postgresql.org/docs/current/protocol-replication.html
- psycopg3 logical replication: https://www.psycopg.org/psycopg3/docs/advanced/async.html
"""

from .base import BaseRelay


class WALRelay(BaseRelay):
    """
    V2 relay: reacts to Postgres WAL INSERT events in real time.

    NOT IMPLEMENTED — see module docstring for planned design.
    Raise NotImplementedError to make misuse obvious.
    """

    async def start(self) -> None:
        raise NotImplementedError(
            "WALRelay is not yet implemented. "
            "Use PollingRelay for V1. "
            "WAL tailing (V2) will be implemented after V1 is validated."
        )

    async def stop(self) -> None:
        raise NotImplementedError("WALRelay is not yet implemented.")
