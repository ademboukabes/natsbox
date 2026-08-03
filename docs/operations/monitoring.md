# Monitoring

If you configure `OUTBOX_METRICS_PORT` (e.g. `9090`), the `nats-outbox` Relay will start a lightweight HTTP server on that port and expose Prometheus metrics at the `/metrics` endpoint.

These metrics allow you to monitor the health, throughput, and latency of your outbox relay.

## Exposed Metrics

| Metric | Type | Description |
|---|---|---|
| `outbox_events_pending` | `Gauge` | The current number of events in the database with `status = 'pending'`. A constantly rising number indicates the Relay cannot keep up with the Application. |
| `outbox_events_published_total` | `Counter` | The total number of events successfully published and ACKed by JetStream since the Relay started. |
| `outbox_events_failed_total` | `Counter` | The total number of events that have reached `OUTBOX_MAX_RETRIES` and were dead-lettered (`status = 'failed'`). |
| `outbox_publish_latency_seconds` | `Histogram` | The time it takes for an event to be published, measured from `created_at` to the moment JetStream returns the ACK. This tracks end-to-end latency. |

## Recommended Alerting Rules

When integrating `nats-outbox` metrics into Prometheus/Alertmanager, we recommend setting up the following alerts:

### 1. High Latency / Backlog
Alert if the number of pending events remains high for too long. This means your application is writing to the database much faster than the relay can publish to NATS, or the relay is down.

```yaml
- alert: OutboxRelayLagging
  expr: outbox_events_pending > 1000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Outbox relay is lagging behind"
    description: "There are more than 1000 pending events in the outbox table."
```

### 2. Dead Letters
Alert immediately if any event fails its maximum retries. Dead letters require manual intervention.

```yaml
- alert: OutboxDeadLetters
  expr: rate(outbox_events_failed_total[1m]) > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Outbox events are failing"
    description: "Events are being dead-lettered. Check NATS connectivity or stream configuration."
```
