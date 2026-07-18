"""
CLI entry point: `nats-outbox relay start`

Requires: pip install nats-outbox[cli]
"""

import asyncio
import contextlib
import logging
import signal
import sys

try:
    import typer
except ImportError:
    print(
        "typer is not installed. Install the CLI extra: pip install nats-outbox[cli]",
        file=sys.stderr,
    )
    sys.exit(1)

import nats
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .publishers.nats_publisher import NatsPublisher
from .relay.polling import PollingRelay
from .settings import OutboxSettings

app = typer.Typer(
    name="nats-outbox",
    help="nats-outbox — Transactional Outbox relay for PostgreSQL + NATS JetStream",
    no_args_is_help=True,
)
relay_app = typer.Typer(help="Relay worker commands")
app.add_typer(relay_app, name="relay")


@relay_app.command("start")
def start(
    strategy: str = typer.Option(
        "polling",
        "--strategy",
        "-s",
        help="Relay strategy: 'polling' (V1, stable) or 'wal' (V2, not yet implemented).",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Logging level: DEBUG, INFO, WARNING, ERROR.",
    ),
    env_file: str | None = typer.Option(
        None,
        "--env-file",
        help="Path to a .env file. Overrides default .env lookup.",
    ),
) -> None:
    """
    Start the outbox relay worker.

    Reads configuration from environment variables (OUTBOX_* prefix) or a .env file.
    Runs until SIGTERM or SIGINT is received, then shuts down gracefully.

    Example:
        OUTBOX_DATABASE_URL=postgresql+asyncpg://... \\
        OUTBOX_NATS_URL=nats://localhost:4222 \\
        nats-outbox relay start --strategy polling
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if strategy not in ("polling", "wal"):
        typer.echo(f"Unknown strategy: {strategy!r}. Choose 'polling' or 'wal'.", err=True)
        raise typer.Exit(code=1)

    if strategy == "wal":
        typer.echo(
            "WAL tailing (V2) is not yet implemented. Use --strategy polling.",
            err=True,
        )
        raise typer.Exit(code=1)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_start_polling_relay(env_file=env_file))


@relay_app.command("cleanup")
def cleanup(
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """
    Delete published events older than retention_days (one-shot, then exits).

    Useful as a cron job:
        0 3 * * * nats-outbox relay cleanup
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    deleted = asyncio.run(_run_cleanup())
    typer.echo(f"Cleanup complete: {deleted} row(s) deleted.")


# ── Async implementations ──────────────────────────────────────────────────────


async def _start_polling_relay(env_file: str | None = None) -> None:
    """Build and run the polling relay until stopped."""
    settings = OutboxSettings(_env_file=env_file) if env_file else OutboxSettings()  # type: ignore[call-arg]  # type: ignore[call-arg]

    engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    nc = await nats.connect(settings.nats_url)
    publisher = await NatsPublisher.from_nats_client(nc)

    # Optional metrics
    metrics = None
    if settings.metrics_port > 0:
        try:
            from .observability.metrics import OutboxMetrics, start_metrics_server

            metrics = OutboxMetrics()
            await start_metrics_server(settings.metrics_port)
        except ImportError:
            logging.getLogger(__name__).warning(
                "prometheus-client not installed; metrics disabled. "
                "Install with: pip install nats-outbox[metrics]"
            )

    relay = PollingRelay(session_factory, publisher, settings, metrics=metrics)

    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        asyncio.create_task(relay.stop())

    loop.add_signal_handler(signal.SIGTERM, _request_stop)
    loop.add_signal_handler(signal.SIGINT, _request_stop)

    try:
        await relay.start()
    finally:
        await nc.drain()
        await nc.close()
        await engine.dispose()
        logging.getLogger(__name__).info("Relay shutdown complete")


async def _run_cleanup() -> int:
    settings = OutboxSettings()  # type: ignore[call-arg]
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    nc = await nats.connect(settings.nats_url)
    publisher = await NatsPublisher.from_nats_client(nc)
    relay = PollingRelay(session_factory, publisher, settings)
    try:
        return await relay.run_cleanup()
    finally:
        await nc.close()
        await engine.dispose()


if __name__ == "__main__":
    app()
