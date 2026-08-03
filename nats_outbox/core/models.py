"""
SQLAlchemy ORM model for the `outbox_events` table.

Schema design decisions (full rationale in the project README):

- BIGSERIAL PK: sequential → zero B-tree fragmentation at high insert rates.
  UUID is present as a *business* key (event_id), not the structural PK.

- JSONB (not JSON): stored as binary → faster reads, supports GIN indexes
  for observability queries (e.g. payload @> '{"user_id": 42}').

- status TEXT + CHECK (not ENUM): adding a new status value with ENUM requires
  ALTER TYPE which takes AccessExclusiveLock. TEXT + CHECK supports
  ALTER TABLE ... ADD CONSTRAINT NOT VALID + VALIDATE in two lock-free steps.

- updated_at: maintained via SQLAlchemy's onupdate mechanism (client-side,
  evaluated on every ORM UPDATE). Useful for debugging stuck events without
  parsing last_error timestamps.

- scheduled_at separate from created_at: enables delayed/scheduled publish
  at zero extra implementation cost in the relay (just check scheduled_at <= now()).

- Three partial indexes (pending, published, aggregate) keep index size small
  because the majority of rows are in status='published'.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DDL,
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    event,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base. Import this into your own Base if you want
    outbox_events co-located with your application tables."""


class OutboxEvent(Base):
    """
    Represents a single event staged in the transactional outbox.

    State machine:
        pending ──(publish ok)──► published
        pending ──(retry exhausted)──► failed

    The relay only reads rows in status='pending'.
    Published rows are kept for `retention_days` then purged by the cleanup task.
    Failed rows are kept indefinitely (alert on them, then resolve manually).
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'published', 'failed')",
            name="chk_outbox_status",
        ),
        UniqueConstraint("event_id", name="uq_outbox_event_id"),
        # ── Partial index: polling relay query ──────────────────────────────
        # The relay's hot path is:
        #   WHERE status = 'pending' AND scheduled_at <= now()
        #   ORDER BY scheduled_at, id
        #   LIMIT batch_size
        #   FOR UPDATE SKIP LOCKED
        #
        # The partial index covers status='pending' only → much smaller than
        # a full-table index once most rows are published.
        # Postgres applies the `scheduled_at <= now()` filter as a post-scan
        # predicate on the (already-tiny) partial index — verified acceptable
        # via EXPLAIN ANALYZE at production volumes.
        Index(
            "idx_outbox_pending",
            "scheduled_at",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
        # ── Partial index: cleanup / retention ──────────────────────────────
        # DELETE WHERE status = 'published' AND published_at < cutoff
        Index(
            "idx_outbox_cleanup",
            "published_at",
            postgresql_where=text("status = 'published'"),
        ),
        # ── Partial index: aggregate ordering ───────────────────────────────
        # Retrieve pending events for a specific aggregate in insertion order.
        # Used by observability queries and WAL tailing (V2).
        Index(
            "idx_outbox_aggregate",
            "aggregate_type",
            "aggregate_id",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
    )

    # ── Identity ─────────────────────────────────────────────────────────────
    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment="Internal sequential PK. Used for ordering and FOR UPDATE SKIP LOCKED.",
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
        default=uuid.uuid4,
        comment=(
            "Business identifier. Exposed to consumers for idempotence (Inbox Pattern). "
            "Also used verbatim as the NATS Nats-Msg-Id header — stable across retries "
            "so JetStream dedup prevents duplicates even on ACK loss."
        ),
    )

    # ── NATS routing ─────────────────────────────────────────────────────────
    subject: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="NATS subject (e.g. 'photo.created', 'org.{id}.user.invited').",
    )
    headers: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        comment=(
            "NATS message headers. Always includes Nats-Msg-Id=event_id. "
            "Extend with tracing headers (traceparent, X-Correlation-Id) as needed."
        ),
    )

    # ── Content ──────────────────────────────────────────────────────────────
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment="Event payload. JSONB for binary storage and GIN-indexable queries.",
    )

    # ── Lifecycle ────────────────────────────────────────────────────────────
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="pending",
        comment="State machine: pending → published | failed.",
    )
    retry_count: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        default=0,
        comment=(
            "Number of failed publish attempts so far. "
            "V1 trade-off: max_retries is a global setting (OutboxSettings), "
            "not per-event. Per-event max_retries deferred to V2."
        ),
    )
    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Truncated repr of the last exception. Aids debugging without log-diving.",
    )

    # ── Correlation & ordering ────────────────────────────────────────────────
    aggregate_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            "ID of the source aggregate (e.g. photo_id, user_id). TEXT covers UUIDs, slugs, ints."
        ),
    )
    aggregate_type: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Type name of the source aggregate (e.g. 'Photo', 'User').",
    )

    # ── Timestamps ───────────────────────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When the event was written to the outbox (within the business transaction).",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment=(
            "Last time this row was modified by the relay (set on every ORM UPDATE). "
            "Useful for detecting stuck events: a row that stays pending for a long time "
            "with a recent updated_at is actively being retried; an old updated_at means "
            "the relay is not reaching this event."
        ),
    )
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment=(
            "Earliest timestamp at which the relay should attempt to publish. "
            "Default = now() (immediate). Set to a future time for delayed publish. "
            "The relay pushes this forward on retry using exponential backoff."
        ),
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment=(
            "When the event was successfully ACK'd by JetStream. "
            "NULL until published. Used to compute publish latency metrics "
            "and to identify rows eligible for retention cleanup."
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<OutboxEvent id={self.id} event_id={self.event_id} "
            f"subject={self.subject!r} status={self.status} retries={self.retry_count}>"
        )


# ── updated_at trigger ────────────────────────────────────────────────────────
# Keep test environments (which use create_tables()) consistent with production
# (which uses migrations/001_create_outbox_events.sql). Without this, tests
# would not exercise the server-side trigger, masking bugs where direct SQL
# UPDATEs (e.g. from psql) don't update updated_at via SQLAlchemy's onupdate.

_DDL_CREATE_FUNCTION = DDL(  # type: ignore[no-untyped-call]
    """
    CREATE OR REPLACE FUNCTION _outbox_set_updated_at()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        NEW.updated_at = now();
        RETURN NEW;
    END;
    $$
    """
)
_DDL_DROP_TRIGGER = DDL(  # type: ignore[no-untyped-call]
    "DROP TRIGGER IF EXISTS trg_outbox_events_updated_at ON outbox_events"
)
_DDL_CREATE_TRIGGER = DDL(  # type: ignore[no-untyped-call]
    """
    CREATE TRIGGER trg_outbox_events_updated_at
        BEFORE UPDATE ON outbox_events
        FOR EACH ROW
        EXECUTE FUNCTION _outbox_set_updated_at()
    """
)

# execute_if(dialect="postgresql") is a no-op on other dialects (e.g. SQLite
# used in lightweight unit tests), so this never breaks non-Postgres setups.
# asyncpg requires one statement per DDL call — hence three separate listeners.
for _ddl in (_DDL_CREATE_FUNCTION, _DDL_DROP_TRIGGER, _DDL_CREATE_TRIGGER):
    event.listen(
        OutboxEvent.__table__,
        "after_create",
        _ddl.execute_if(dialect="postgresql"),
    )



async def create_tables(engine: Any) -> None:
    """
    Create all tables defined in this module and in core.inbox.

    Imports InboxEvent here to ensure its table is registered with
    Base.metadata before create_all() is called.

    Convenience helper for tests and quick-start scripts.
    For production, prefer Alembic migrations generated from this model.
    """
    # Import InboxEvent so it registers with Base.metadata
    from nats_outbox.core.inbox import InboxEvent  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_tables(engine: Any) -> None:
    """Drop all tables. For tests only — never call in production."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
