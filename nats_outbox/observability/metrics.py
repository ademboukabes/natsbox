"""
Prometheus metrics for nats-outbox.

This module is optional — the library works without prometheus-client installed.
Install it with: pip install nats-outbox[metrics]

Metrics exposed
---------------
outbox_events_pending (Gauge):
    Current number of events in status='pending'.
    Updated after each relay tick.
    Alert: outbox_events_pending > threshold for > N minutes indicates
    the relay is not running or NATS is down.

outbox_events_published_total (Counter):
    Cumulative count of successfully published events.
    Labels: subject

outbox_events_failed_total (Counter):
    Cumulative count of dead-lettered events (status='failed').
    Labels: subject
    Alert: outbox_events_failed_total > 0 requires manual intervention.

outbox_publish_latency_seconds (Histogram):
    Time (seconds) between event creation (created_at) and publication
    (published_at). This measures end-to-end outbox latency including
    polling sleep time, publish attempt, and JetStream ACK.
    Labels: subject

Prometheus HTTP server
-----------------------
If settings.metrics_port > 0, the relay CLI starts a lightweight HTTP server
on that port serving /metrics in the Prometheus text exposition format.

Usage (programmatic, without the CLI):
::

    from nats_outbox.observability.metrics import OutboxMetrics, start_metrics_server

    metrics = OutboxMetrics()
    await start_metrics_server(port=9090)

    relay = PollingRelay(session_factory, publisher, settings, metrics=metrics)
"""

import asyncio
import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import (  # pyright: ignore[reportMissingImports]
        Counter,
        Gauge,
        Histogram,
        start_http_server,
    )

    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

    from typing import Any

    class _DummyMetric:
        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            return self

        def labels(self, *args: Any, **kwargs: Any) -> Any:
            return self

        def inc(self, *args: Any, **kwargs: Any) -> None:
            pass

        def set(self, *args: Any, **kwargs: Any) -> None:
            pass

        def observe(self, *args: Any, **kwargs: Any) -> None:
            pass

    Counter = Gauge = Histogram = start_http_server = _DummyMetric()  # type: ignore


class OutboxMetrics:
    """
    Container for all Prometheus metric objects.

    Instantiate once and pass to PollingRelay. Creating multiple instances
    with the same metric names will raise a ValueError from prometheus-client.
    Use a module-level singleton or a DI container.
    """

    def __init__(self, namespace: str = "outbox") -> None:
        if not _PROMETHEUS_AVAILABLE:
            raise ImportError(
                "prometheus-client is not installed. "
                "Install it with: pip install nats-outbox[metrics]"
            )

        self.events_pending = Gauge(
            f"{namespace}_events_pending",
            "Number of outbox events in status=pending",
        )

        self.events_published_total = Counter(
            f"{namespace}_events_published_total",
            "Total number of events successfully published to NATS JetStream",
            ["subject"],
        )

        self.events_failed_total = Counter(
            f"{namespace}_events_failed_total",
            "Total number of events dead-lettered (status=failed) after max retries",
            ["subject"],
        )

        self.publish_latency_seconds = Histogram(
            f"{namespace}_publish_latency_seconds",
            "Time in seconds from event creation (created_at) to publication (published_at). "
            "Includes polling sleep time, retry delays, and JetStream ACK wait.",
            ["subject"],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
        )

        logger.info(
            "OutboxMetrics initialized (namespace=%r). "
            "Expose via start_metrics_server() or an existing Prometheus HTTP endpoint.",
            namespace,
        )


async def start_metrics_server(port: int, addr: str = "") -> None:
    """
    Start a Prometheus HTTP server in a background thread.

    This is a thin wrapper around prometheus_client.start_http_server().
    The server runs in a daemon thread and serves /metrics at the given port.

    Parameters
    ----------
    port:
        TCP port to listen on. Use 0 to disable.
    addr:
        Interface to bind to. Default is all interfaces.
    """
    if not _PROMETHEUS_AVAILABLE:
        logger.warning(
            "prometheus-client not installed; metrics server not started. "
            "Install with: pip install nats-outbox[metrics]"
        )
        return

    if port == 0:
        logger.debug("Metrics port=0, Prometheus metrics server disabled")
        return

    # start_http_server is synchronous but spawns a daemon thread — safe to call from async
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: start_http_server(port, addr=addr))
    logger.info("Prometheus metrics server started on :%d/metrics", port)
