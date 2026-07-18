"""
PollingRelay — V1 relay implementation.

Architecture
------------
The polling relay is a single asyncio event loop that periodically queries
Postgres for pending outbox events and publishes them to NATS JetStream.

Key design decisions
---------------------

1. SELECT FOR UPDATE SKIP LOCKED
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   The hot query is:
       SELECT * FROM outbox_events
       WHERE status = 'pending' AND scheduled_at <= now()
       ORDER BY scheduled_at, id
       LIMIT batch_size
       FOR UPDATE SKIP LOCKED

   FOR UPDATE: acquires a row-level write lock on the selected rows.
   SKIP LOCKED: if a row is already locked by another relay instance, skip it
   instead of blocking. This makes it safe to run multiple relay instances in
   parallel (horizontal scaling) without duplicating work or creating deadlocks.

   Trade-off: SKIP LOCKED means events can be processed out-of-order if one
   relay instance holds a lock and another skips to the next batch. For
   strict ordering within a single aggregate, use one relay instance or
   implement aggregate-level partitioning (deferred to V2).

2. Drain-then-sleep pattern
   ~~~~~~~~~~~~~~~~~~~~~~~~~~
   If a tick processes events, the relay immediately loops (no sleep) to drain
   any remaining queue. Only when a tick returns zero events does the relay
   sleep for polling_interval seconds. This gives near-zero latency when the
   queue is non-empty, while avoiding busy-waiting when idle.

3. Exponential backoff via scheduled_at
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   On publish failure, the relay does NOT sleep or busy-retry. Instead, it:
   - Increments retry_count
   - Pushes scheduled_at forward by backoff_seconds
   - Commits and moves on to the next event

   The event will re-appear in the polling query naturally once scheduled_at
   is in the past again. This approach:
   - Never blocks the relay from processing other events
   - Survives relay restarts (state is in the DB, not in memory)
   - Provides natural jitter if backoff_seconds varies per event

4. Dead-lettering
   ~~~~~~~~~~~~~~~~
   After max_retries failed attempts, status is set to 'failed'. The event
   remains in the table for audit/alerting. Set up a Prometheus alert on:
       outbox_events_failed_total > 0

   V1 trade-off: dead-lettered events require manual intervention (requeue
   by resetting status='pending' and retry_count=0). A requeue CLI command
   is planned for V2.

5. Session-per-tick
   ~~~~~~~~~~~~~~~~~~
   Each tick creates and destroys its own AsyncSession (via the session factory).
   This avoids connection leaks from long-running sessions and ensures the
   SQLAlchemy identity map is always fresh (no stale cached state).

Trade-offs vs WAL tailing (V2)
--------------------------------
Polling:
  + Simple: no Postgres superuser, no replication slot setup.
  + Survives Postgres restarts transparently.
  - Latency: up to polling_interval per event (default 1s).
  - DB load: constant read query even when outbox is empty. Mitigated by the
    partial index (cheap scan) but nonzero at scale.
  - Ordering: SKIP LOCKED weakens strict global ordering under concurrent relays.

WAL tailing:
  + Sub-millisecond latency (reacts to INSERT in real-time).
  + Zero read load (no SELECT polling).
  + Strong ordering via LSN sequence.
  - Requires a replication slot and REPLICATION privilege.
  - Reconnection and LSN tracking add implementation complexity.
  - Replication slots can block Postgres WAL cleanup if the relay is offline.
"""

import asyncio
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..core.models import OutboxEvent
from ..publishers.nats_publisher import NatsPublisher
from ..settings import OutboxSettings
from .base import BaseRelay

if TYPE_CHECKING:
    from ..observability.metrics import OutboxMetrics
else:
    try:
        from ..observability.metrics import OutboxMetrics

        _METRICS_AVAILABLE = True
    except ImportError:
        OutboxMetrics = None
        _METRICS_AVAILABLE = False

logger = logging.getLogger(__name__)


class PollingRelay(BaseRelay):
    """
    V1 relay: polls outbox_events periodically, publishes to NATS JetStream.

    Horizontally scalable: multiple instances can run simultaneously without
    duplicate work, thanks to SELECT FOR UPDATE SKIP LOCKED.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: NatsPublisher,
        settings: OutboxSettings,
        *,
        metrics: Optional["OutboxMetrics"] = None,
    ) -> None:
        """
        Parameters
        ----------
        session_factory:
            SQLAlchemy async_sessionmaker. Used to create a fresh session
            per polling tick.
        publisher:
            NatsPublisher instance connected to JetStream.
        settings:
            OutboxSettings loaded from environment.
        metrics:
            Optional OutboxMetrics for Prometheus instrumentation.
            If None, metrics are silently skipped.
        """
        self._session_factory = session_factory
        self._publisher = publisher
        self._settings = settings
        self._metrics = metrics
        self._running = False

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the polling loop. Blocks until stop() is called.

        Drain-then-sleep: loops immediately if events were found, sleeps
        polling_interval only when the outbox is empty.
        """
        self._running = True
        logger.info(
            "PollingRelay started — interval=%.1fs batch=%d max_retries=%d",
            self._settings.polling_interval,
            self._settings.batch_size,
            self._settings.max_retries,
        )

        while self._running:
            try:
                processed = await self._tick()
                if processed == 0:
                    # Outbox empty — sleep before next poll
                    await asyncio.sleep(self._settings.polling_interval)
                # else: events found — loop immediately to drain
            except asyncio.CancelledError:
                logger.info("PollingRelay received cancellation, shutting down")
                break
            except Exception:
                logger.exception(
                    "Unhandled error in relay tick. Sleeping %.1fs before retrying.",
                    self._settings.polling_interval,
                )
                await asyncio.sleep(self._settings.polling_interval)

        logger.info("PollingRelay stopped")

    async def stop(self) -> None:
        """Signal the relay to stop after the current tick completes."""
        self._running = False
        logger.info("PollingRelay stop requested — will exit after current tick")

    # ── Core tick ─────────────────────────────────────────────────────────

    async def _tick(self) -> int:
        """
        Single relay iteration.

        Fetches a batch of pending events, processes each one (publish + update
        status), and commits all changes atomically.

        Returns the number of events processed in this tick.
        """
        async with self._session_factory() as session, session.begin():
            now = datetime.now(tz=UTC)

            # ── Fetch a locked batch of pending events ─────────────────
            # FOR UPDATE SKIP LOCKED: safe for concurrent relay instances.
            # ORDER BY scheduled_at, id: respects delayed publish order
            # and FIFO within the same scheduled time.
            stmt = (
                select(OutboxEvent)
                .where(
                    OutboxEvent.status == "pending",
                    OutboxEvent.scheduled_at <= now,
                )
                .order_by(
                    OutboxEvent.scheduled_at.asc(),
                    OutboxEvent.id.asc(),
                )
                .limit(self._settings.batch_size)
                .with_for_update(skip_locked=True)
            )
            result = await session.execute(stmt)
            events: Sequence[OutboxEvent] = result.scalars().all()

            if not events:
                return 0

            logger.debug("Tick: processing %d event(s)", len(events))

            # ── Process each event within the same transaction ─────────
            # All status updates (published, retry backoff, failed) are
            # committed together at the end of the `async with session.begin()`
            # block. This means:
            # - Partial failures don't leave some events published and others not.
            # - The DB commit and NATS publish are still two separate operations
            #   (unavoidable without a distributed transaction), but the relay
            #   handles this correctly via idempotent re-publish.
            for event in events:
                await self._process_event(event)

            # Update metrics for pending count
            if self._metrics:
                await self._update_pending_gauge(session)

            return len(events)

    async def _process_event(self, event: OutboxEvent) -> None:
        """
        Attempt to publish a single event. Update its status in place.

        The session.begin() context in _tick() will commit all mutations
        together after all events in the batch are processed.

        Success path:
            status → published, published_at → now()

        Transient failure (retry_count < max_retries):
            retry_count += 1, scheduled_at pushed forward by backoff

        Permanent failure (retry_count >= max_retries):
            status → failed (dead-lettered)
        """
        try:
            await self._publisher.publish(event)

            # ── Success ────────────────────────────────────────────────────
            now = datetime.now(tz=UTC)
            event.status = "published"
            event.published_at = now

            # Publish latency metric
            if self._metrics:
                latency = (now - event.created_at).total_seconds()
                self._metrics.publish_latency_seconds.observe(latency)
                self._metrics.events_published_total.labels(subject=event.subject).inc()

            logger.info(
                "Published event_id=%s subject=%r (attempt %d)",
                event.event_id,
                event.subject,
                event.retry_count + 1,
            )

        except Exception as exc:
            # ── Failure ────────────────────────────────────────────────────
            event.retry_count += 1
            event.last_error = _truncate(repr(exc), max_len=2000)

            if event.retry_count >= self._settings.max_retries:
                # Dead-letter: stop retrying, alert on this
                event.status = "failed"
                if self._metrics:
                    self._metrics.events_failed_total.labels(subject=event.subject).inc()
                logger.error(
                    "DEAD-LETTERED event_id=%s subject=%r after %d attempt(s). last_error=%r",
                    event.event_id,
                    event.subject,
                    event.retry_count,
                    event.last_error,
                )
            else:
                # Transient failure: exponential backoff via scheduled_at
                backoff = _backoff(event.retry_count)
                event.scheduled_at = datetime.now(tz=UTC) + timedelta(seconds=backoff)
                logger.warning(
                    "Publish failed for event_id=%s subject=%r "
                    "(attempt %d/%d). Retrying in %.1fs. error=%r",
                    event.event_id,
                    event.subject,
                    event.retry_count,
                    self._settings.max_retries,
                    backoff,
                    exc,
                )

    # ── Cleanup ────────────────────────────────────────────────────────────

    async def run_cleanup(self) -> int:
        """
        Delete published events older than retention_days.

        Returns the number of rows deleted.

        This can be called periodically (e.g. daily via cron or a scheduler)
        or triggered manually. It runs in its own transaction, separate from
        the polling loop.
        """
        from typing import Any, cast

        from sqlalchemy import delete
        from sqlalchemy.engine import CursorResult

        cutoff = datetime.now(tz=UTC) - timedelta(days=self._settings.retention_days)
        async with self._session_factory() as session, session.begin():
            stmt = delete(OutboxEvent).where(
                OutboxEvent.status == "published",
                OutboxEvent.published_at < cutoff,
            )
            result = await session.execute(stmt)
            deleted = cast(CursorResult[Any], result).rowcount
            logger.info(
                "Cleanup: deleted %d published event(s) older than %d days",
                deleted,
                self._settings.retention_days,
            )
            return deleted

    # ── Internal helpers ───────────────────────────────────────────────────

    async def _update_pending_gauge(self, session: AsyncSession) -> None:
        """Update the Prometheus pending events gauge."""
        from sqlalchemy import func
        from sqlalchemy import select as sa_select

        if not self._metrics:
            return
        try:
            result = await session.execute(
                sa_select(func.count()).where(OutboxEvent.status == "pending")
            )
            count = result.scalar_one()
            self._metrics.events_pending.set(count)
        except Exception:
            logger.debug("Failed to update pending gauge", exc_info=True)


# ── Pure helper functions ──────────────────────────────────────────────────────


def _backoff(retry_count: int, base: float = 2.0, cap: float = 300.0) -> float:
    """
    Exponential backoff with a hard cap.

    retry_count=1 →   2s
    retry_count=2 →   4s
    retry_count=3 →   8s
    retry_count=4 →  16s
    retry_count=5 →  32s
    ...
    retry_count=8 → 256s
    retry_count=9 → 300s (capped)

    No jitter is added here. If you run multiple relay instances, SKIP LOCKED
    provides natural distribution. For single-instance deployments with burst
    failures, consider adding ±10% jitter.
    """
    return min(base**retry_count, cap)


def _truncate(s: str, max_len: int = 2000) -> str:
    """Truncate a string to max_len characters, adding '...' suffix if needed."""
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."
