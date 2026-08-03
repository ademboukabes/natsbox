# Recovering Dead Letters

When the Relay fails to publish an event to NATS JetStream (e.g., due to network issues or a missing stream configuration), it will retry exponentially. 

However, to prevent infinitely blocking faulty events, an event that fails `OUTBOX_MAX_RETRIES` times is considered a **Dead Letter**. Its status is updated to `failed`, and the Relay will stop attempting to process it.

This guide explains how to safely investigate and recover these failed events using the `nats-outbox` CLI.

## Step 1: Identify the failure

When an event transitions to `failed`, the error is recorded directly in the database.
You can query the `outbox_events` table manually:

```sql
SELECT id, event_id, subject, last_error 
FROM outbox_events 
WHERE status = 'failed' 
ORDER BY updated_at DESC;
```

This will reveal why the event failed (e.g. `nats.errors.NoRespondersError` if the JetStream stream does not exist for this subject).

## Step 2: Fix the root cause

Before requeuing the event, ensure that the underlying issue has been resolved. For example:
- Did NATS run out of disk space? 
- Did someone delete the target Stream?
- Are the NATS credentials expired?

If you requeue without fixing the root cause, the events will simply fail again and return to the dead letter state.

## Step 3: Requeue (Dry Run)

Before modifying any data, you can run the `requeue` command with the `--dry-run` flag. This will output exactly which events are eligible to be recovered without actually altering the database.

```bash
nats-outbox relay requeue --dry-run
```

## Step 4: Requeue all failed events

Once you are confident, run the command without `--dry-run`. 

This operation will reset the `status` of all `failed` events back to `pending`, and reset their `retry_count` to `0`. 

```bash
nats-outbox relay requeue
```

The Relay process (if running) will immediately pick up these newly pending events and attempt to publish them again.

## Requeuing a specific event

If you only want to recover a single specific event, you can provide its `event_id` (UUID):

```bash
nats-outbox relay requeue --event-id 550e8400-e29b-41d4-a716-446655440000
```
