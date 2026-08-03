# Alembic Integration

This guide explains how to manage the `outbox_events` and `inbox_events` tables
with Alembic when `nats-outbox` is used as a library in your application.

## Why Alembic and not the raw SQL file?

The `migrations/001_create_outbox_events.sql` file is provided for teams that
run plain `psql` scripts. If your project already uses Alembic, the recommended
approach is to let Alembic generate and manage the outbox migrations alongside
your own application tables.

## Step 1 — Import nats-outbox models in `alembic/env.py`

Open your project's `alembic/env.py` and add the following imports **before**
the `target_metadata` assignment:

```python
# Import nats-outbox models so Alembic knows about them.
# The import has a side-effect: it registers OutboxEvent and InboxEvent
# with their SQLAlchemy metadata so autogenerate detects them.
import nats_outbox.core.models  # noqa: F401
import nats_outbox.core.inbox   # noqa: F401

from nats_outbox.core.models import Base as OutboxBase
```

## Step 2 — Merge metadata targets

Alembic's `autogenerate` command needs to know about all tables. If your
application already has its own `Base`, you must pass both metadata objects:

```python
from your_app.database import Base as AppBase
from nats_outbox.core.models import Base as OutboxBase

# Pass a list to include all tables from both bases
target_metadata = [AppBase.metadata, OutboxBase.metadata]
```

If your application has no `Base` of its own (you only use nats-outbox):

```python
from nats_outbox.core.models import Base as OutboxBase
target_metadata = OutboxBase.metadata
```

## Step 3 — Generate the migration

```bash
alembic revision --autogenerate -m "create outbox and inbox tables"
```

Review the generated file in `alembic/versions/`. You should see `CREATE TABLE`
statements for `outbox_events` and `inbox_events` along with all three partial
indexes.

## Step 4 — Add the updated_at trigger

Alembic does not generate triggers automatically. Add the trigger manually at
the bottom of the generated migration's `upgrade()` function:

```python
def upgrade() -> None:
    # ... auto-generated CREATE TABLE statements ...

    # updated_at trigger — keeps updated_at accurate on direct SQL UPDATEs.
    op.execute("""
        CREATE OR REPLACE FUNCTION _outbox_set_updated_at()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$;

        CREATE TRIGGER trg_outbox_events_updated_at
            BEFORE UPDATE ON outbox_events
            FOR EACH ROW
            EXECUTE FUNCTION _outbox_set_updated_at();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_outbox_events_updated_at ON outbox_events;")
    op.execute("DROP FUNCTION IF EXISTS _outbox_set_updated_at;")
    op.drop_table("outbox_events")
    op.drop_table("inbox_events")
```

## Step 5 — Apply the migration

```bash
alembic upgrade head
```

## Notes

- **Do not apply** `migrations/001_create_outbox_events.sql` if you use Alembic
  — you will end up with duplicate tables or conflicts.
- If you add the `updated_at` trigger via the DDL event listener in
  `nats_outbox.core.models` (available in `nats-outbox >= 0.2.0`), the trigger
  is automatically created when `create_tables()` is called (useful in tests).
  In production, Alembic controls the schema, so the trigger must be in the
  migration as shown above.
