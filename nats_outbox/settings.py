"""
Central configuration for nats-outbox.

All settings are read from environment variables or a .env file.
No values are ever hard-coded — this is a non-negotiable architectural constraint.

Variable naming: all env vars use the OUTBOX_ prefix to avoid collisions in
multi-library environments.
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class OutboxSettings(BaseSettings):
    """
    Runtime configuration for the outbox relay and publisher.

    Loaded from environment variables or a .env file.
    Every field has a sensible default except database_url and nats_url,
    which must be explicitly set.
    """

    model_config = SettingsConfigDict(
        env_prefix="OUTBOX_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ── Database ────────────────────────────────────────────────────────────
    database_url: str = Field(
        ...,
        description=(
            "SQLAlchemy async DSN. Must use the asyncpg driver: "
            "postgresql+asyncpg://user:pass@host:port/dbname"
        ),
    )

    # ── NATS ────────────────────────────────────────────────────────────────
    nats_url: str = Field(
        "nats://localhost:4222",
        description="NATS server URL. Supports nats://, tls://, and ws:// schemes.",
    )

    # ── Relay — Polling ─────────────────────────────────────────────────────
    polling_interval: float = Field(
        1.0,
        gt=0,
        description=(
            "Seconds to sleep between polling ticks when the outbox is empty. "
            "When events are found the relay loops immediately (no sleep) to "
            "drain the queue as fast as possible."
        ),
    )
    batch_size: int = Field(
        100,
        gt=0,
        le=1000,
        description=(
            "Maximum number of events fetched per polling tick. "
            "Bounded to 1000 to limit lock contention with concurrent relay instances."
        ),
    )
    max_retries: int = Field(
        5,
        ge=1,
        description=(
            "Maximum number of publish attempts before an event is dead-lettered "
            "(status='failed'). The relay uses exponential backoff between attempts. "
            "V1 trade-off: this limit is global, not per-event. A per-event "
            "max_retries column is deferred to V2."
        ),
    )

    # ── Retention ───────────────────────────────────────────────────────────
    retention_days: int = Field(
        7,
        ge=1,
        description=(
            "Published events older than this many days are eligible for deletion "
            "by the cleanup task. Does not affect pending or failed events."
        ),
    )

    # ── NATS JetStream deduplication ─────────────────────────────────────────
    jetstream_dedup_window: int = Field(
        120,
        ge=1,
        description=(
            "Duration (seconds) of the JetStream deduplication window. "
            "Must match MaxAge / DuplicateWindow configured on your stream. "
            "The relay sets Nats-Msg-Id = event_id (stable across retries), "
            "so any retry within this window is silently deduplicated by JetStream "
            "— even if the first publish succeeded but the ACK was lost."
        ),
    )

    # ── Observability ────────────────────────────────────────────────────────
    metrics_port: int = Field(
        0,
        ge=0,
        le=65535,
        description=(
            "TCP port to expose a Prometheus /metrics HTTP endpoint. "
            "Set to 0 (default) to disable the metrics server."
        ),
    )

    @field_validator("database_url")
    @classmethod
    def _validate_db_url(cls, v: str) -> str:
        if not v.startswith("postgresql+asyncpg://"):
            raise ValueError(
                "database_url must use the asyncpg driver: "
                "postgresql+asyncpg://user:pass@host:port/dbname. "
                f"Got: {v!r}"
            )
        return v
