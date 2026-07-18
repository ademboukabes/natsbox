"""
nats-outbox — Transactional Outbox Pattern for PostgreSQL + NATS JetStream.

Public API surface:
    from nats_outbox.core.outbox import outbox_transaction
    from nats_outbox.core.models import OutboxEvent, Base
    from nats_outbox.core.inbox import InboxDeduplicator
    from nats_outbox.relay.polling import PollingRelay
    from nats_outbox.publishers.nats_publisher import NatsPublisher
    from nats_outbox.settings import OutboxSettings
"""

__version__ = "0.1.0"
__all__ = ["__version__"]
