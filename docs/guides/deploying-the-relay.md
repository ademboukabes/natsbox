# Deploying the Relay

The `nats-outbox` Relay is designed to run as a long-running background worker process, entirely decoupled from your web API threads. This decoupling ensures that heavy web traffic does not impact the polling loop, and vice versa.

This guide provides examples on how to deploy this relay process in production environments.

## Docker Compose

If you are using Docker Compose, you should define a specific service for the relay alongside your API and infrastructure containers.

Since `nats-outbox` provides a CLI, you can use a standard Python image, install your application (which includes `nats-outbox` as a dependency), and run the CLI command.

```yaml
version: '3.8'

services:
  # Your main web API
  api:
    build: .
    command: uvicorn myapp.main:app --host 0.0.0.0 --port 8000
    environment:
      - DATABASE_URL=postgresql+asyncpg://user:pass@db:5432/myapp
      - NATS_URL=nats://nats:4222
    depends_on:
      - db
      - nats

  # The nats-outbox Relay Worker
  relay:
    build: .
    command: nats-outbox relay start
    environment:
      - OUTBOX_DATABASE_URL=postgresql+asyncpg://user:pass@db:5432/myapp
      - OUTBOX_NATS_URL=nats://nats:4222
      - OUTBOX_METRICS_PORT=9090
    ports:
      - "9090:9090" # Expose Prometheus metrics
    depends_on:
      - db
      - nats

  db:
    image: postgres:16
    # ...

  nats:
    image: nats:latest
    command: -js
    # ...
```

## Kubernetes Deployment

In a Kubernetes cluster, the Relay should be deployed as a standard `Deployment` object. It does not require persistent volumes or stateful sets, as all state is stored safely in PostgreSQL.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outbox-relay
  labels:
    app: outbox-relay
spec:
  replicas: 2
  selector:
    matchLabels:
      app: outbox-relay
  template:
    metadata:
      labels:
        app: outbox-relay
    spec:
      containers:
      - name: relay
        image: my-registry/my-app:latest
        command: ["nats-outbox", "relay", "start"]
        env:
        - name: OUTBOX_DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        - name: OUTBOX_NATS_URL
          value: "nats://nats-cluster:4222"
        - name: OUTBOX_METRICS_PORT
          value: "9090"
        ports:
        - containerPort: 9090
          name: metrics
```

### Horizontal Scaling and Concurrency

In the Kubernetes manifest above, we set `replicas: 2`. 

**Is it safe to run multiple relay instances concurrently?**

Yes. The `nats-outbox` Relay utilizes PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED` mechanism. This ensures that:
1. When Instance A picks up a batch of pending events, it places a row-level lock on those specific rows.
2. If Instance B polls at the exact same time, it will automatically "skip" the rows locked by Instance A and pick up the next available batch.

This allows you to scale the Relay horizontally to increase throughput without causing deadlocks or duplicate publishes. 

> [!NOTE]
> While horizontal scaling increases raw throughput, it weakens the strict FIFO ordering guarantee. If strict chronological ordering of events is absolutely critical to your system, you should restrict the Relay to a single instance (`replicas: 1`), or wait for the V2 release (WAL tailing), which will maintain strict LSN ordering globally.
