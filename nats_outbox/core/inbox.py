"""
Inbox Pattern — consumer-side event deduplication.

Why this is needed
------------------
The outbox relay provides at-least-once delivery: an event will eventually be
published, but may be published more than once (e.g. relay crash after publish
but before marking as published). JetStream's Nats-Msg-Id dedup window handles
duplicates within the dedup window (default 2 minutes), but consumers must
handle duplicates that arrive outside that window or via different streams.

Strategy
--------
INSERT INTO inbox_events (event_id, consumer_group) ON CONFLICT DO NOTHING

This is an atomic check-and-insert: if the row already exists, the INSERT
returns zero rows (conflict), indicating the event is a duplicate. No SELECT
is needed — avoids the TOCTOU race condition of SELECT then INSERT.

The consumer_group column lets multiple consumers (e.g. "billing", "analytics")
independently deduplicate events with the same event_id — each group tracks its
own processed events.

Usage
-----
::

    async with AsyncSession(engine) as session:
        async with session.begin():
            deduplicator = InboxDeduplicator(session, consumer_group="billing")
            if await deduplicator.is_duplicate(event_id):
                return  # already processed, skip

            # ... process event business logic here ...
            # The inbox row + business effect commit together atomically.

Table schema (run create_inbox_table() or use the provided migration)
::

    CREATE TABLE inbox_events (
        id              BIGSERIAL    PRIMARY KEY,
        event_id        UUID         NOT NULL,
        consumer_group  TEXT         NOT NULL,
        processed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
        CONSTRAINT uq_inbox_event UNIQUE (event_id, consumer_group)
    );
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from .models import Base


class InboxEvent(Base):
    """
    Records events that have been successfully processed by a consumer group.
    Enables idempotent consumers without application-level locking.
    """

    __tablename__ = "inbox_events"
    __table_args__ = (
        UniqueConstraint(
            "event_id",
            "consumer_group",
            name="uq_inbox_event_consumer",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
        comment="Matches OutboxEvent.event_id from the publisher.",
    )
    consumer_group: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment=(
            "Logical consumer name (e.g. 'billing-service', 'notification-worker'). "
            "Allows multiple consumers to independently deduplicate the same event."
        ),
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return f"<InboxEvent event_id={self.event_id} consumer_group={self.consumer_group!r}>"


class InboxDeduplicator:
    """
    Stateless helper for consumer-side event deduplication.

    Thread-safety: one instance per request/task — do not share across
    concurrent coroutines with the same session.
    """

    def __init__(self, session: AsyncSession, consumer_group: str) -> None:
        self._session = session
        self._consumer_group = consumer_group

    async def is_duplicate(self, event_id: uuid.UUID) -> bool:
        """
        Check whether event_id has already been processed by this consumer group.

        Uses INSERT ... ON CONFLICT DO NOTHING — atomic, no SELECT needed.
        Returns True if the event is a duplicate (already in inbox_events).
        Returns False and inserts the row if the event is new.

        IMPORTANT: This must be called within an open transaction. The inbox row
        and the business effect must commit together, otherwise a crash between
        the two commits can leave the inbox row without the business effect
        (or vice versa).
        """
        result = await self._session.execute(
            text("""
                INSERT INTO inbox_events (event_id, consumer_group)
                VALUES (:event_id, :consumer_group)
                ON CONFLICT (event_id, consumer_group) DO NOTHING
                RETURNING id
            """),
            {
                "event_id": str(event_id),
                "consumer_group": self._consumer_group,
            },
        )
        row = result.fetchone()
        # row is None ↔ conflict (already processed) → duplicate
        # row is not None ↔ inserted successfully → new event
        return row is None


async def create_inbox_table(engine: Any) -> None:
    """Create the inbox_events table. For tests and quick-start scripts."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
