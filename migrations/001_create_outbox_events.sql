-- nats-outbox migration 001: create outbox_events and inbox_events tables
--
-- Apply with:
--   psql $DATABASE_URL -f migrations/001_create_outbox_events.sql
--
-- For Alembic, generate from the SQLAlchemy models:
--   alembic revision --autogenerate -m "create outbox tables"

BEGIN;

-- ── outbox_events ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS outbox_events (
    -- Identity
    id              BIGSERIAL       NOT NULL,
    event_id        UUID            NOT NULL DEFAULT gen_random_uuid(),

    -- NATS routing
    subject         TEXT            NOT NULL,
    headers         JSONB           NOT NULL DEFAULT '{}',

    -- Content
    payload         JSONB           NOT NULL,

    -- Lifecycle
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending',
    retry_count     SMALLINT        NOT NULL DEFAULT 0,
    last_error      TEXT,

    -- Correlation & ordering
    aggregate_id    TEXT,
    aggregate_type  TEXT,

    -- Timestamps
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    scheduled_at    TIMESTAMPTZ     NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,

    CONSTRAINT pk_outbox_events         PRIMARY KEY (id),
    CONSTRAINT uq_outbox_event_id       UNIQUE (event_id),
    CONSTRAINT chk_outbox_status        CHECK (status IN ('pending', 'published', 'failed'))
);

COMMENT ON TABLE  outbox_events IS
    'Transactional outbox: events staged atomically with domain data, '
    'published asynchronously to NATS JetStream by the relay worker.';

COMMENT ON COLUMN outbox_events.event_id IS
    'Business identifier. Used as NATS Nats-Msg-Id header — stable across '
    'retries, enabling JetStream deduplication on ACK loss.';

COMMENT ON COLUMN outbox_events.updated_at IS
    'Last modified timestamp. Updated by the relay on every status change. '
    'Useful for detecting stuck events: old updated_at + status=pending = relay issue.';

COMMENT ON COLUMN outbox_events.scheduled_at IS
    'Earliest publish time. Default = now() for immediate delivery. '
    'Set to future for delayed publish. Relay pushes this forward on retry (backoff).';

-- Polling relay hot path: pending events ready to publish
-- Partial (status=''pending'' only) keeps index small once rows are published.
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events (scheduled_at ASC, id ASC)
    WHERE status = 'pending';

-- Cleanup / retention: DELETE WHERE published_at < cutoff
CREATE INDEX IF NOT EXISTS idx_outbox_cleanup
    ON outbox_events (published_at)
    WHERE status = 'published';

-- Aggregate ordering & observability
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox_events (aggregate_type, aggregate_id, id ASC)
    WHERE status = 'pending';

-- ── updated_at trigger (optional — keeps updated_at accurate on direct SQL updates) ──
-- Remove if you prefer to let SQLAlchemy manage updated_at client-side.

CREATE OR REPLACE FUNCTION _outbox_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_outbox_events_updated_at ON outbox_events;

CREATE TRIGGER trg_outbox_events_updated_at
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW
    EXECUTE FUNCTION _outbox_set_updated_at();

-- ── inbox_events ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS inbox_events (
    id              BIGSERIAL       NOT NULL,
    event_id        UUID            NOT NULL,
    consumer_group  TEXT            NOT NULL,
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_inbox_events              PRIMARY KEY (id),
    CONSTRAINT uq_inbox_event_consumer      UNIQUE (event_id, consumer_group)
);

COMMENT ON TABLE inbox_events IS
    'Consumer-side deduplication. Tracks which event_ids have been processed '
    'per consumer group. Use INSERT ... ON CONFLICT DO NOTHING for atomic dedup.';

COMMIT;
