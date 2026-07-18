"""Abstract base interface for outbox relay implementations."""

from abc import ABC, abstractmethod


class BaseRelay(ABC):
    """
    Common interface for all relay implementations.

    A relay is a long-running worker that reads pending events from the
    outbox_events table and publishes them to NATS JetStream.

    Implementations
    ---------------
    - PollingRelay (V1): periodic SELECT polling with FOR UPDATE SKIP LOCKED.
    - WALRelay (V2): logical replication stream from Postgres WAL.

    Both implementations must be safe to run as multiple concurrent instances
    (horizontal scaling). Exclusive access to a batch of events must be
    coordinated via the database (e.g. SKIP LOCKED for polling,
    replication slot for WAL).
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Start the relay loop.

        Blocks until stop() is called or an unrecoverable error occurs.
        Should be run as a separate asyncio task or process.
        """

    @abstractmethod
    async def stop(self) -> None:
        """
        Signal the relay to stop gracefully.

        Should allow the current in-progress tick/batch to complete,
        then exit the loop. Non-blocking: returns immediately.
        """
