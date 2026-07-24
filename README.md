<div align="center">

# natsaga

**Orchestration-based Saga Pattern for async Python microservices**

NATS JetStream + PostgreSQL · built on [`nats-outbox`](https://github.com/ademboukabes/natsbox)

Distributed transactions across microservices, without a distributed transaction.

[![CI](https://github.com/ademboukabes/natsaga/actions/workflows/ci.yml/badge.svg)](https://github.com/ademboukabes/natsaga/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/natsaga.svg)](https://pypi.org/project/natsaga/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Typed: strict](https://img.shields.io/badge/typing-strict%20mypy%20%2B%20pyright-informational)](pyrightconfig.json)

[The Problem](#the-problem) · [Quick Start](#quick-start) · [Architecture](#architecture) · [Guarantees](#guarantees) · [Integration Contract](#integration-contract) · [Roadmap](#roadmap)

</div>

---

## The Problem

Modern systems decompose business operations across multiple services, each owning its own database. Placing an order might require:

```
1. Payment Service    →  reserve the charge
2. Inventory Service  →  reserve the stock
3. Shipping Service   →  schedule delivery
```

There is no ACID transaction that spans all three. If step 2 fails, step 1 must be undone — the customer should never be charged for an item that's out of stock. This is the classic distributed-transactions problem, and the industry-standard answer is the **Saga Pattern**: break the operation into local transactions, each paired with a **compensating action**, coordinated by an orchestrator that walks forward on success and rolls back in reverse order on failure.

### What's missing in Python today

| | Status |
|---|---|
| Async-native | ❌ Existing Python saga libraries predate `async`/`await` |
| NATS JetStream support | ❌ Built for Celery/Kafka, not NATS |
| Actively maintained | ❌ Last meaningful updates years old |
| Crash-safe by construction | ❌ Relies on application-level discipline, not atomic guarantees |

`natsaga` fills this gap: an async-first, NATS JetStream-native saga orchestrator with atomic state transitions, deterministic idempotency, mandatory timeouts, and a complete audit trail — the properties a production system actually needs from this pattern.

## Why not just call the services in order?

```python
# The naive approach — looks fine until it isn't
await payment_service.reserve(order)
await inventory_service.reserve(order)   # crash here?
await shipping_service.schedule(order)   # payment is now orphaned
```

If the process crashes between calls, or a service is briefly unreachable, you're left with a payment that has no matching inventory reservation — and nothing recorded anywhere to tell you that. `natsaga` replaces this sequence with declared steps, persisted state, and automatic compensation, so a crash mid-flight is a recoverable event, not a silent data-integrity bug.

## Built on `nats-outbox`

An orchestrator faces the exact same dual-write problem that `nats-outbox` was built to solve: it must, in one atomic step, (a) update the saga's state in Postgres and (b) publish the next command to NATS. `natsaga` doesn't reimplement this — it uses `outbox_transaction()` from `nats-outbox` for every state transition.

```
┌──────────────────────────┐        ┌───────────────────────────┐
│         natsaga           │  uses  │        nats-outbox         │
│  Saga orchestration,      │───────►│  Atomic Postgres write +   │
│  compensation, timeouts   │        │  reliable NATS publish     │
└──────────────────────────┘        └───────────────────────────┘
```

This is a deliberate layering decision, not an implementation detail: reliable delivery is a solved problem one layer down, so `natsaga`'s own code stays focused on saga semantics — step ordering, compensation, timeouts — instead of re-solving message delivery.

## Quick Start

```bash
pip install natsaga
```

Define a saga declaratively:

```python
from natsaga.core.saga import Saga
from natsaga.core.step import Step

class OrderSaga(Saga):
    saga_type = "order_creation"

    @classmethod
    def steps(cls) -> list[Step]:
        return [
            Step(
                name="reserve_payment",
                action_subject="payment.reserve",
                compensation_subject="payment.release",
                timeout_seconds=30,
            ),
            Step(
                name="reserve_inventory",
                action_subject="inventory.reserve",
                compensation_subject="inventory.release",
                timeout_seconds=30,
            ),
            Step(
                name="schedule_shipping",
                action_subject="shipping.schedule",
                compensation_subject="shipping.cancel",
                timeout_seconds=60,
            ),
        ]
```

Start it from your FastAPI endpoint:

```python
from natsaga.orchestrator import SagaOrchestrator

@app.post("/orders")
async def create_order(order: OrderIn, session: AsyncSession = Depends(get_session)):
    saga_id = await orchestrator.start(
        OrderSaga,
        payload={"order_id": order.id, "amount": order.amount},
        session=session,
    )
    return {"saga_id": saga_id, "status": "processing"}
```

Run the two background workers:

```bash
natsaga listener start          # routes NATS responses to the orchestrator
natsaga timeout-checker start   # detects and compensates stuck steps
```

If `reserve_inventory` fails, `natsaga` automatically publishes to `payment.release` — no manual rollback code required.

## Architecture

### End-to-end flow

```
 HTTP Request
      │
      ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                    SagaOrchestrator.start()                        │
 │                                                                     │
 │   async with outbox_transaction(session) as tx:                    │
 │       saga_state.status = "running"                                │
 │       saga_state.step_deadline = now() + step.timeout_seconds      │
 │       tx.publish_event("payment.reserve", event_id=uuid5(...))     │
 └────────────────────────────┬────────────────────────────────────┘
                               │ atomic commit (Postgres ACID)
                               ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                          PostgreSQL                                 │
 │  saga_state                        saga_step_log                   │
 │  ┌─────────────────────────┐      ┌───────────────────────────┐   │
 │  │ id      order-saga-1     │      │ step │ direction │ outcome│   │
 │  │ status  running          │      │ 0    │ forward   │ -      │   │
 │  │ current 0                │      └───────────────────────────┘   │
 │  │ deadline now()+30s       │                                      │
 │  └─────────────────────────┘                                      │
 └────────────────────────────┬────────────────────────────────────┘
                               │ relayed by nats-outbox
                               ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                        NATS JetStream                               │
 │  subject: payment.reserve                                            │
 └────────────────────────────┬────────────────────────────────────┘
                               │
                               ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                        Payment Service                              │
 │  processes the reservation, then publishes its result to:          │
 │  payment.reserve.response.{saga_id}.{step_index}                    │
 └────────────────────────────┬────────────────────────────────────┘
                               ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                          SagaListener                               │
 │  subscribes to *.response.>, routes outcome to the orchestrator     │
 └────────────────────────────┬────────────────────────────────────┘
                success ──────┴────── failure / timeout
                   │                          │
                   ▼                          ▼
       advance to next step          compensate completed steps
       (same atomic pattern)         in reverse order
```

### Failure path — compensation in reverse order

```
Step 1: reserve_payment    ✅ succeeded
Step 2: reserve_inventory  ✅ succeeded
Step 3: schedule_shipping  ❌ FAILED
                │
                ▼
     compensation begins, reverse order
                │
    ┌───────────┴────────────┐
    ▼                         ▼
inventory.release        payment.release
(compensate step 2)      (compensate step 1)
    │                         │
    ▼                         ▼
saga_step_log:            saga_step_log:
step=1, compensate,       step=0, compensate,
outcome=success           outcome=success
                │
                ▼
      saga_state.status = "failed"
```

### The three workers

| Component | Role | Trigger |
|---|---|---|
| `SagaOrchestrator` | Persists saga state, dispatches the next command, drives compensation | Called directly on `start()`; called by the listener and timeout checker on response/expiry |
| `SagaListener` | Subscribes to `*.response.>` on JetStream, routes outcomes | Every microservice response |
| `TimeoutChecker` | Polls `saga_state` for expired `step_deadline`, triggers compensation | Every `poll_interval` seconds (default 5s) |

## Data Model

### `saga_state` — the state machine

| Column | Type | Purpose |
|---|---|---|
| `id` | `UUID` | Saga identity — UUID rather than a sequence, since sagas are created from many concurrent API processes with no natural single point of contention |
| `saga_type` | `TEXT` | Which `Saga` subclass this row belongs to |
| `current_step` | `SMALLINT` | Zero-indexed pointer to the active step |
| `status` | `TEXT` + `CHECK` | `running` → `completed` \| `compensating` → `failed` \| `compensation_failed` |
| `payload` | `JSONB` | The original, immutable input — never overwritten |
| `context` | `JSONB` | Mutable accumulator for step outputs (e.g. a payment reference a later step needs) — deliberately separate from `payload` so "what was asked" and "what happened" never blur together |
| `step_deadline` | `TIMESTAMPTZ` | Absolute deadline for the current step; the source of truth for `TimeoutChecker` |
| `last_error` | `TEXT` | Latest failure, for a fast glance without joining `saga_step_log` |
| `created_at` / `updated_at` | `TIMESTAMPTZ` | Lifecycle timestamps; `updated_at` also powers crash-recovery scans |

### `saga_step_log` — the audit trail

Append-only, one row per attempted transition (forward or compensating):

```sql
SELECT step_name, direction, outcome, error, finished_at
FROM saga_step_log
WHERE saga_id = '...'
ORDER BY id;
```

This is what turns a 3 a.m. incident from guesswork into a five-second query: exactly which step ran, in which direction, with what outcome, and why.

## Guarantees

Stated precisely — not as marketing claims, but as what is actually tested:

- **At-least-once command delivery**, inherited from `nats-outbox`, deduplicated within JetStream's configured dedup window via a deterministic `Nats-Msg-Id`.
- **No duplicate step execution on orchestrator crash-restart.** Each command's `event_id` is a UUID v5 derived from `(saga_id, step_index, direction)` — deterministic and stable across processes, so a restarted orchestrator recognizes work already in flight instead of re-dispatching it.
- **No double state advancement on concurrent or duplicate responses.** State transitions are guarded by `SELECT ... FOR UPDATE` followed by a status/step check inside the same transaction — a second, duplicate, or racing response is a verified no-op, not a best-effort one.
- **No silently stuck sagas.** Every step declares a mandatory `timeout_seconds`; there is no default that lets a step wait forever.
- **No silent compensation failures.** If a compensating action itself fails, the saga enters a terminal `compensation_failed` state and increments a Prometheus counter — it does not retry silently forever, and it does not pretend to have succeeded.

What `natsaga` does **not** guarantee: exactly-once execution of the domain action itself (a step can theoretically execute twice if a microservice fails to be idempotent on message redelivery — that responsibility sits with the microservice, as it does in any at-least-once system, not with the orchestrator).

## Integration Contract

Every microservice participating in a saga owns two responsibilities:

**1. Consume the action subject and do the work:**

```python
@nats_subscribe("payment.reserve")
async def handle_reserve_payment(msg: Msg) -> None:
    data = json.loads(msg.data)
    saga_id = data["saga_id"]
    step_index = data["step_index"]

    try:
        reservation = await payment_gateway.reserve(data["amount"])
        outcome = {"outcome": "success", "result": {"payment_ref": reservation.id}}
    except PaymentDeclined as exc:
        outcome = {"outcome": "failure", "error": str(exc)}

    await nc.publish(
        f"payment.reserve.response.{saga_id}.{step_index}",
        json.dumps(outcome).encode(),
    )
```

**2. Publish the result to the exact response subject** — `{action_subject}.response.{saga_id}.{step_index}` — with an `outcome` field of `success` or `failure`. This convention is load-bearing: it's what lets `SagaListener` route a response to the correct saga and step without ambiguity. Getting this subject format wrong is the single most common integration mistake — test it against a real NATS instance, not a mock, before deploying a new saga participant.

## Configuration

```bash
NATSAGA_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/myapp
NATSAGA_NATS_URL=nats://localhost:4222
NATSAGA_TIMEOUT_POLL_INTERVAL=5.0   # seconds between timeout scans
NATSAGA_METRICS_PORT=9091           # 0 disables the Prometheus exporter
```

## Observability

| Metric | Type | Meaning |
|---|---|---|
| `saga_started_total{saga_type}` | Counter | Sagas started |
| `saga_completed_total{saga_type}` | Counter | Sagas that reached `completed` |
| `saga_failed_total{saga_type}` | Counter | Sagas that reached `failed` after full compensation |
| `saga_compensation_failed_total{saga_type}` | Counter | **Page someone** — a compensation itself failed |
| `saga_step_duration_seconds{saga_type,step_name,direction}` | Histogram | Time spent per step, forward or compensating |

## Schema Setup

`natsaga` does not ship its own Alembic migrations — as a library, embedding one would conflict with your application's own migration history. It exposes the SQLAlchemy `Base` (`SagaState`, `SagaStepLog`), so your own `alembic revision --autogenerate` detects the tables automatically. A plain [`migrations/schema.sql`](migrations/schema.sql) is provided for non-SQLAlchemy setups.

## What's explicitly out of scope (V1)

| Not included | Why |
|---|---|
| Choreography-based sagas | Different coordination model entirely; orchestration covers the majority of real-world needs first |
| Parallel steps (fan-out/fan-in) | Meaningful added complexity in failure semantics; sequential steps first |
| Automatic compensation retries | A failed compensation needs a human decision, not a silent retry loop, until proven otherwise |
| Web UI | Prometheus metrics are the V1 answer to "what's happening" |
| Brokers other than NATS JetStream | Scope stays tight and matches `nats-outbox` |

## Testing

```bash
git clone https://github.com/ademboukabes/natsaga.git
cd natsaga
make install
make test   # real Postgres + NATS via testcontainers
make lint   # ruff + mypy --strict + pyright
```

Integration coverage includes: happy path, failure-triggered compensation, timeout-triggered compensation, orchestrator crash-and-restart recovery, compensation failure (terminal state, no retry loop), and duplicate/concurrent response delivery (verified against a real NATS redelivery, not a mock).

## Roadmap

- [x] **V1** — Orchestration-based sagas, atomic transitions via `nats-outbox`, mandatory timeouts, audit log, Prometheus metrics
- [ ] LISTEN/NOTIFY-based timeout detection (sub-second, vs. today's 5s poll)
- [ ] Automatic, bounded retry of failed compensations
- [ ] Parallel step groups (fan-out/fan-in)
- [ ] Choreography-based saga variant

## License

MIT — see [LICENSE](LICENSE).
