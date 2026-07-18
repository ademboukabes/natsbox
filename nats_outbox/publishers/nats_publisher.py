"""
NATS JetStream publisher.

Nats-Msg-Id strategy (critical invariant)
------------------------------------------
JetStream supports message-level deduplication via the `Nats-Msg-Id` header.
When a message is published with a Nats-Msg-Id that matches a recently published
message (within the stream's DuplicateWindow), JetStream silently discards it
and returns the original ACK — *without* the message appearing in the stream again.

Our relay leverages this by always setting Nats-Msg-Id to OutboxEvent.event_id:
- event_id is generated at *write* time (inside outbox_transaction), not at
  publish time.
- This means every retry attempt uses the *same* Nats-Msg-Id.
- Scenario: relay publishes → JetStream receives → ACK is lost → relay crashes
  → relay restarts → relay re-publishes same event_id → JetStream deduplicates
  → ACK returned → relay marks row as published.
  Result: consumers see the event exactly once (within the dedup window).

This publisher enforces Nats-Msg-Id = event_id as a defense-in-depth measure,
even though outbox_transaction already sets it. If someone tampers with
OutboxEvent.headers between write and publish, this code corrects it silently.

Publish timeout and error handling
------------------------------------
A publish is considered failed if:
  - The NATS connection is lost (nats.errors.ConnectionClosedError, etc.)
  - JetStream returns no ACK within `publish_timeout` seconds
  - JetStream returns an error ACK (NoStreamResponseError, etc.)

The publisher raises the exception. The relay catches it, increments retry_count,
schedules a backoff, and will retry on the next tick. The relay is responsible
for the retry loop — the publisher is stateless and single-attempt.
"""

import json
import logging

from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext
from nats.js.errors import APIError, NoStreamResponseError

from ..core.models import OutboxEvent

logger = logging.getLogger(__name__)


class NatsPublisher:
    """
    Stateless NATS JetStream publisher for OutboxEvent rows.

    The publisher is intentionally simple: it publishes exactly once per call
    and raises on any failure. Retry logic lives in the relay, not here.

    Design trade-off: keeping retry logic out of the publisher means the relay
    can implement sophisticated strategies (exponential backoff, dead-lettering,
    metrics) without the publisher needing to know about them.
    """

    def __init__(
        self,
        js: JetStreamContext,
        *,
        publish_timeout: float = 5.0,
    ) -> None:
        """
        Parameters
        ----------
        js:
            A connected JetStream context (nc.jetstream()).
        publish_timeout:
            Seconds to wait for a JetStream ACK. After this, a TimeoutError
            is raised and the relay will retry with backoff.
        """
        self._js = js
        self._publish_timeout = publish_timeout

    async def publish(self, event: OutboxEvent) -> None:
        """
        Publish a single outbox event to NATS JetStream and wait for ACK.

        Raises
        ------
        Exception
            Any NATS or network exception. The relay handles all exceptions
            uniformly (retry with backoff → dead-letter after max_retries).

        Notes
        -----
        - Payload is serialized as UTF-8 JSON bytes.
        - Nats-Msg-Id header is always overwritten to str(event.event_id),
          even if already present in event.headers. This is intentional.
        """
        # Serialize payload to bytes
        payload_bytes = json.dumps(event.payload, ensure_ascii=False).encode("utf-8")

        # Build headers: start from stored headers, enforce Nats-Msg-Id
        headers: dict[str, str] = {k: str(v) for k, v in (event.headers or {}).items()}
        # ── Defense-in-depth: always enforce event_id as Nats-Msg-Id ────────
        # outbox_transaction already sets this, but we enforce it here too.
        # A mismatch would break JetStream dedup; catch it early.
        stored_msg_id = headers.get("Nats-Msg-Id")
        canonical_msg_id = str(event.event_id)
        if stored_msg_id and stored_msg_id != canonical_msg_id:
            logger.warning(
                "Nats-Msg-Id mismatch for event_id=%s: stored=%r, expected=%r. "
                "Correcting to event_id. This should not happen — check your "
                "outbox_transaction usage.",
                event.event_id,
                stored_msg_id,
                canonical_msg_id,
            )
        headers["Nats-Msg-Id"] = canonical_msg_id

        logger.debug(
            "Publishing event_id=%s subject=%s attempt=%d",
            event.event_id,
            event.subject,
            event.retry_count + 1,
        )

        try:
            ack = await self._js.publish(
                event.subject,
                payload_bytes,
                headers=headers,
                timeout=self._publish_timeout,
            )
        except NoStreamResponseError as exc:
            # No JetStream stream configured for this subject
            raise RuntimeError(
                f"No JetStream stream found for subject {event.subject!r}. "
                "Ensure a stream is configured that covers this subject."
            ) from exc
        except APIError as exc:
            raise RuntimeError(
                f"JetStream API error publishing to {event.subject!r}: {exc}"
            ) from exc

        logger.debug(
            "ACK received: event_id=%s stream=%s seq=%d duplicate=%s",
            event.event_id,
            ack.stream,
            ack.seq,
            getattr(ack, "duplicate", False),
        )

    @classmethod
    async def from_nats_client(
        cls,
        nc: NatsClient,
        *,
        publish_timeout: float = 5.0,
    ) -> "NatsPublisher":
        """
        Convenience constructor: build a NatsPublisher from a connected NatsClient.

        Usage::

            nc = await nats.connect("nats://localhost:4222")
            publisher = await NatsPublisher.from_nats_client(nc)
        """
        js = nc.jetstream()
        return cls(js, publish_timeout=publish_timeout)
