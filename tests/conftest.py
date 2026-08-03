"""
Shared pytest fixtures for nats-outbox integration tests.

Event loop strategy (critical)
-------------------------------
pytest-asyncio 1.4.0 introduced `asyncio_default_test_loop_scope`. We set both:
  asyncio_default_fixture_loop_scope = "session"
  asyncio_default_test_loop_scope    = "session"

This means ALL async fixtures AND all tests share the SAME event loop for the
entire session. Without this, session-scoped async fixtures (engine, nc) are
created on the session loop, but tests run on their own function-scoped loops →
asyncpg raises "Future attached to a different loop".

Container strategy
------------------
- Postgres + NATS containers: session-scoped (started once, ~15s total startup).
- engine, session_factory, nc, js, publisher: session-scoped (created once,
  reused across all tests on the same event loop).
- clean_outbox: function-scoped autouse → TRUNCATE between each test for isolation.
  Streams per test are created lazily via ensure_stream() (idempotent).
"""

import contextlib
from collections.abc import AsyncGenerator, Generator
from typing import Any

import nats
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from nats_outbox.core.models import create_tables
from nats_outbox.publishers.nats_publisher import NatsPublisher
from nats_outbox.settings import OutboxSettings

try:
    from testcontainers.core.waiting_utils import (  # type: ignore[attr-defined, unused-ignore]
        LogMessageWaitStrategy,  # pyright: ignore
    )

    _HAS_LOG_WAIT_STRATEGY = True
except ImportError:
    _HAS_LOG_WAIT_STRATEGY = False
    LogMessageWaitStrategy = None  # pyright: ignore[reportConstantRedefinition]


# ── Infrastructure containers (session-scoped, started once) ────────────────


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start a real PostgreSQL 16 container. Pulled once per test session."""
    with PostgresContainer(
        image="postgres:16-alpine",
        username="outbox",
        password="outbox",
        dbname="outbox_test",
    ) as pg:
        yield pg


@pytest.fixture(scope="session")
def nats_container() -> Generator[DockerContainer, None, None]:
    """
    Start a NATS 2.10 container with JetStream enabled (-js flag).

    Uses DockerContainer directly because testcontainers has no first-party
    NATS image. Waits for the "Server is ready" log line before yielding.
    """
    container = DockerContainer("nats:2.10-alpine").with_command("-js").with_exposed_ports(4222)
    container.start()
    # Wait until NATS is ready — use structured strategy if available,
    # fall back to string-based wait_for_logs for older testcontainers versions
    if _HAS_LOG_WAIT_STRATEGY:
        assert LogMessageWaitStrategy is not None
        container.waiting_for(LogMessageWaitStrategy("Server is ready", timeout=30))
    else:
        wait_for_logs(container, "Server is ready", timeout=30)
    yield container
    container.stop()


# ── Connection strings ────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def db_url(postgres_container: PostgresContainer) -> str:
    """
    Async asyncpg DSN from the Postgres container.
    testcontainers returns a psycopg2 URL; we convert scheme to asyncpg.
    """
    sync_url = postgres_container.get_connection_url()
    # "postgresql+psycopg2://..." → "postgresql+asyncpg://..."
    url = sync_url.replace("psycopg2", "asyncpg")
    # Guard: avoid double-prefix if already correct
    url = url.replace("postgresql+asyncpg+asyncpg://", "postgresql+asyncpg://")
    if not url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://")
    return str(url)


@pytest.fixture(scope="session")
def nats_url(nats_container: DockerContainer) -> str:
    """NATS URL built from the container's dynamically assigned port."""
    host = nats_container.get_container_host_ip()
    port = nats_container.get_exposed_port(4222)
    return f"nats://{host}:{port}"


# ── SQLAlchemy engine & session factory (session-scoped) ─────────────────────


@pytest.fixture(scope="session")
async def engine(db_url: str) -> AsyncGenerator[AsyncEngine, None]:
    """
    Create the async SQLAlchemy engine and run table migrations once per session.

    Session-scoped so asyncpg connections live on the same event loop as tests
    (see "Event loop strategy" in module docstring).
    """
    _engine = create_async_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )
    await create_tables(_engine)
    yield _engine
    await _engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory reused across all tests."""
    return async_sessionmaker(engine, expire_on_commit=False)


# ── Per-test isolation ────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
async def clean_outbox(engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """
    TRUNCATE outbox_events and inbox_events after each test.

    autouse=True → runs automatically for every test in this package.
    Runs *after* the test (yield first) so failures show the pre-cleanup state.

    Note: TRUNCATE ... RESTART IDENTITY resets the BIGSERIAL counters,
    giving each test a clean slate of IDs starting from 1.
    """
    yield
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE outbox_events RESTART IDENTITY CASCADE"))
        await conn.execute(text("TRUNCATE inbox_events RESTART IDENTITY CASCADE"))


# ── NATS client & JetStream (session-scoped) ──────────────────────────────────
# Session-scoped so the connection lives on the same loop as the engine.
# Test isolation for NATS: each test creates its own stream (via ensure_stream)
# and subscriber. Streams are not deleted between tests — they accumulate but
# don't interfere because subjects are unique per test.


@pytest.fixture(scope="session")
async def nc(nats_url: str) -> AsyncGenerator[Any, None]:
    """
    Persistent NATS client for the test session.
    Session-scoped to share the event loop with the async engine.
    """
    client = await nats.connect(nats_url)
    yield client
    await client.drain()
    await client.close()


@pytest.fixture(scope="session")
async def js(nc: Any) -> Any:
    """JetStream context for the test session."""
    return nc.jetstream()


@pytest.fixture(scope="session")
async def publisher(js: Any) -> NatsPublisher:
    """
    NatsPublisher wired to the session JetStream context.
    Session-scoped: all tests that need a publisher share this instance.
    Tests that need a custom publisher (e.g. flaky) create their own inline.
    """
    return NatsPublisher(js)


# ── OutboxSettings (session-scoped) ──────────────────────────────────────────


@pytest.fixture(scope="session")
def settings(db_url: str, nats_url: str) -> OutboxSettings:
    """
    OutboxSettings built from container URLs, bypassing .env file lookup.
    polling_interval=0.05s for fast test runs.
    """
    return OutboxSettings(
        database_url=db_url,
        nats_url=nats_url,
        polling_interval=0.05,
        batch_size=50,
        max_retries=3,
        retention_days=7,
        jetstream_dedup_window=120,
        metrics_port=0,
    )


# ── JetStream stream helper ───────────────────────────────────────────────────


async def ensure_stream(js: Any, name: str, subjects: list[str]) -> None:
    """
    Create a JetStream stream if it doesn't exist. Idempotent.

    Sets duplicate_window=120.0 (seconds) so Nats-Msg-Id deduplication works
    in test_relay_idempotent_on_lost_ack. nats-py StreamConfig.duplicate_window
    takes seconds as a float; it converts to nanoseconds internally.
    """
    from nats.js.api import StreamConfig
    from nats.js.errors import BadRequestError, NotFoundError

    try:
        await js.stream_info(name)
        # Stream exists — nothing to do
    except NotFoundError:
        with contextlib.suppress(BadRequestError):
            await js.add_stream(
                StreamConfig(
                    name=name,
                    subjects=subjects,
                    duplicate_window=120.0,  # seconds (nats-py converts to ns)
                )
            )
    except Exception:
        # stream_info failed for another reason — try to create anyway
        with contextlib.suppress(BadRequestError):
            await js.add_stream(
                StreamConfig(
                    name=name,
                    subjects=subjects,
                    duplicate_window=120.0,
                )
            )
