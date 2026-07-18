"""
outbox_transaction — core context manager for the Transactional Outbox Pattern.

Design principles
-----------------
1. **Framework-agnostic**: this module has zero FastAPI dependency. It only
   requires a SQLAlchemy AsyncSession. FastAPI integration is a thin layer
   on top (see examples/fastapi_integration.py).

2. **Single transaction**: both domain objects and outbox events are committed
   atomically in one SQL COMMIT. If the commit fails, neither is persisted —
   no partial state, no phantom events.

3. **Nats-Msg-Id frozen at write time**: the event_id (which becomes the
   Nats-Msg-Id NATS header) is generated when publish_event() is called, not
   when the relay publishes. This is the critical invariant:
     - Retry 1: relay publishes with Nats-Msg-Id = event_id → ACK received → OK
     - Retry 2 (after crash): same Nats-Msg-Id → JetStream dedup silently drops
       the duplicate within the dedup window.
   If we generated a new UUID per publish attempt, a lost ACK would produce
   a duplicate message in the stream — defeating the at-least-once guarantee.

4. **Session lifecycle**: outbox_transaction commits the session on __aexit__.
   The caller must NOT commit the session afterward. For FastAPI dependencies
   that normally handle commit/rollback, exclude the session from auto-commit
   when wrapping with outbox_transaction.

   If you need to use outbox_transaction inside an existing
   `async with session.begin():` block, call tx._flush_events() manually
   and let the outer transaction commit.

Trade-offs
----------
- outbox_transaction always commits: simpler API, but means you can't stage
  multiple independent logical units then commit them all at once. For that
  use case, instantiate OutboxTransaction directly and call _flush_events()
  before your own commit.
- No nested transaction support in V1: using outbox_transaction inside an
  already-begun SQLAlchemy transaction will cause a double-commit. Documented
  as a known limitation.
"""

import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from .models import OutboxEvent


class OutboxTransaction:
    """
    A thin wrapper around AsyncSession that adds outbox-aware event staging.

    Call publish_event() to stage events. They are written to the database
    (within the same transaction as your domain objects) when the surrounding
    outbox_transaction context manager exits successfully.

    The session object is still accessible via .session for advanced use cases
    (raw SQL, bulk inserts, etc.).
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._pending_events: list[OutboxEvent] = []

    # ── Domain object proxy ──────────────────────────────────────────────────

    def add(self, instance: Any) -> None:
        """
        Add a domain object to the SQLAlchemy session.

        Mirrors session.add() — provided as a convenience so callers don't
        need to hold a reference to both tx and session separately.
        """
        self._session.add(instance)

    def add_all(self, instances: list[Any]) -> None:
        """Add multiple domain objects in one call."""
        for instance in instances:
            self._session.add(instance)

    # ── Event staging ────────────────────────────────────────────────────────

    def publish_event(
        self,
        subject: str,
        payload: dict[str, Any],
        *,
        aggregate_id: str | None = None,
        aggregate_type: str | None = None,
        scheduled_at: datetime | None = None,
        headers: dict[str, str] | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """
        Stage an outbox event to be persisted within this transaction.

        Parameters
        ----------
        subject:
            NATS subject (e.g. "photo.created", "org.42.user.invited").
        payload:
            Event body. Must be JSON-serializable.
        aggregate_id:
            ID of the source entity (e.g. str(photo.id)). Used for ordering
            guarantees and observability. Optional but strongly recommended.
        aggregate_type:
            Class/type name of the source entity (e.g. "Photo"). Optional.
        scheduled_at:
            Earliest publish time. Defaults to now() for immediate delivery.
            Pass a future datetime for delayed publish.
        headers:
            Extra NATS headers to include. Nats-Msg-Id is always set to
            event_id and cannot be overridden here.
        event_id:
            Override the auto-generated UUID. Useful when you need the event_id
            before calling publish_event (e.g. to store it as a FK in a domain
            object). If omitted, a UUID4 is generated.

        Returns
        -------
        uuid.UUID
            The event_id that was assigned. Store this if you need to correlate
            the outbox row with consumer-side inbox deduplication.

        Nats-Msg-Id invariant
        ---------------------
        The Nats-Msg-Id header is set here, not in the publisher. The publisher
        reads it from OutboxEvent.headers and enforces it (no override). This
        guarantees the same ID is used across all retry attempts, enabling
        JetStream's dedup window to absorb retries transparently.
        """
        _event_id = event_id if event_id is not None else uuid.uuid4()

        _headers: dict[str, str] = dict(headers or {})

        # ── Critical invariant: Nats-Msg-Id = event_id, set at write time ──
        # Do NOT allow callers to override this via the headers param.
        # The publisher also enforces this as a defense-in-depth safeguard.
        _headers["Nats-Msg-Id"] = str(_event_id)

        event = OutboxEvent(
            event_id=_event_id,
            subject=subject,
            payload=payload,
            headers=_headers,
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            scheduled_at=scheduled_at or datetime.now(tz=UTC),
            status="pending",
            retry_count=0,
        )
        self._pending_events.append(event)
        return _event_id

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _flush_events(self) -> None:
        """
        Add all staged OutboxEvent instances to the session.

        Called by the context manager before commit. Can be called manually
        if you manage the transaction yourself (e.g. inside an outer begin()).
        """
        for event in self._pending_events:
            self._session.add(event)
        # Keep the list in case of a re-flush after a savepoint rollback.
        # The session deduplicates by identity map.

    @property
    def session(self) -> AsyncSession:
        """
        Escape hatch: the underlying SQLAlchemy session.

        Use for operations not covered by the OutboxTransaction proxy
        (e.g. raw SQL, bulk operations, SQLAlchemy-specific features).
        """
        return self._session

    @property
    def staged_events(self) -> list[OutboxEvent]:
        """Read-only view of staged (not yet committed) events. Useful for testing."""
        return list(self._pending_events)


@asynccontextmanager
async def outbox_transaction(
    session: AsyncSession,
) -> AsyncGenerator[OutboxTransaction, None]:
    """
    Async context manager that wraps a SQLAlchemy session with outbox support.

    On successful exit (__aexit__ with no exception):
        1. All staged OutboxEvent rows are added to the session.
        2. session.commit() is called — domain objects AND events are persisted atomically.

    On exception:
        session.rollback() is called — neither domain objects nor events are persisted.

    Usage (framework-agnostic):
    ::

        engine = create_async_engine(settings.database_url)
        async_session = async_sessionmaker(engine)

        async with async_session() as session:
            async with outbox_transaction(session) as tx:
                photo = Photo(url="https://...", user_id=42)
                tx.add(photo)
                tx.publish_event(
                    subject="photo.created",
                    payload={"photo_id": str(photo.id)},
                    aggregate_id=str(photo.id),
                    aggregate_type="Photo",
                )
            # session is committed (and closed by the outer async with)

    FastAPI dependency injection:
    ::

        async def get_session() -> AsyncGenerator[AsyncSession, None]:
            async with AsyncSession(engine) as session:
                yield session
                # NOTE: do NOT commit here — outbox_transaction does it.

        @router.post("/photos")
        async def create_photo(session: AsyncSession = Depends(get_session)):
            async with outbox_transaction(session) as tx:
                photo = Photo(url=request.url)
                tx.add(photo)
                await session.flush()  # flush to get photo.id if needed
                tx.publish_event(
                    subject="photo.created",
                    payload={"photo_id": str(photo.id)},
                )

    Known limitation (V1):
        Do NOT use inside an existing `async with session.begin():` block.
        outbox_transaction will issue a commit that also commits the outer
        transaction. If you need composable transactions, call
        `await tx._flush_events()` and manage the commit yourself.
    """
    tx = OutboxTransaction(session)
    try:
        yield tx
        await tx._flush_events()
        await session.commit()
    except Exception:
        await session.rollback()
        raise
