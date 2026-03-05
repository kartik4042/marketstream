# 🛒 Real-Time MarketStream Event Processing Platform

> Simulates how MarketStream (eBay-scale) processes orders, inventory, and payments using Apache Kafka event streams. Built to demonstrate production-grade distributed systems engineering.

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │         Client / Browser         │
                    └──────────────┬──────────────────┘
                                   │ POST /api/v1/orders
                                   │ Idempotency-Key header
                                   ▼
                    ┌─────────────────────────────────┐
                    │     Order API  (Java/Spring)     │
                    │  • Idempotency via Redis         │
                    │  • Validates & persists order    │
                    │  • Partitions events by userId   │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │   Kafka Topic: order_events      │
                    │   6 partitions, key=userId       │
                    │   Retention: 7 days              │
                    └──────────────┬──────────────────┘
                                   │
              ┌────────────────────▼──────────────────────┐
              │         Inventory Service  (Python)        │
              │  • Consumer Group (2 replicas)             │
              │  • SELECT FOR UPDATE (race-free reserve)   │
              │  • Idempotency via Redis TTL               │
              │  • Retry + exponential backoff             │
              └────────────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │  Kafka Topic: inventory_events   │
                    └──────────────┬──────────────────┘
                                   │
              ┌────────────────────▼──────────────────────┐
              │         Payment Service  (Java/Spring)     │
              │  • Consumer Group (3 concurrent threads)   │
              │  • Idempotent payment processing           │
              │  • @RetryableTopic for non-blocking retry  │
              │  • 85% simulated success rate              │
              └────────────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │   Kafka Topic: payment_events    │
                    └──────────────┬──────────────────┘
                                   │
              ┌────────────────────▼──────────────────────┐
              │       Notification Worker  (Python)        │
              │  • Email/SMS/Push simulation               │
              │  • Full audit trail                        │
              │  • DLQ for failed notifications            │
              └───────────────────────────────────────────┘

Dead Letter Queues:
  order_events_dlq       ← failed order publishes
  inventory_events_dlq   ← failed inventory events  
  payment_events_dlq     ← failed payment events
  notification_events_dlq← failed notifications
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Order API | Java 17, Spring Boot 3.2, Spring Kafka |
| Inventory Service | Python 3.12, aiokafka, asyncpg |
| Payment Service | Java 17, Spring Boot 3.2, Spring Kafka |
| Notification Worker | Python 3.12, aiokafka |
| Message Broker | Apache Kafka 7.5 (6 partitions) |
| Database | PostgreSQL 15 |
| Cache / Idempotency | Redis 7 |
| Metrics | Prometheus + Grafana |
| Containerization | Docker + Docker Compose |

---

## Features Implemented

### 1. Kafka Consumer Groups
Each service registers a dedicated consumer group. Multiple instances of the inventory service share the 6 partitions of `order_events`, so adding more replicas linearly scales throughput.

### 2. Dead Letter Queue (DLQ)
All services implement a DLQ pattern. Failed events are written to `{topic}_dlq` with full context: original partition, offset, payload, and error message. This enables:
- Zero data loss on transient failures
- Offline inspection and replay
- Alerting on DLQ accumulation

### 3. Idempotent Consumers
Two layers of idempotency:
- **Redis TTL cache** (`order:idempotency:{key}` → orderId, TTL 24h) — fast path deduplication
- **HTTP header** (`Idempotency-Key`) — clients can safely retry without duplicate orders
- **Idempotent Kafka producer** (`enable.idempotence=true`) — prevents broker-side duplicates

### 4. Retry with Exponential Backoff
- Python services: manual async retry loop — 1s, 2s, 4s delays
- Java services: Spring's `@RetryableTopic` — non-blocking retry topics with configurable backoff
- Maximum 3 attempts before DLQ routing

### 5. Partitioning Strategy
All Kafka producers use `userId` as the partition key:
- Guarantees **ordering** for all events from the same user
- Ensures **affinity** — same consumer handles the full lifecycle of one user's orders
- Even distribution across 6 partitions via hash(userId) % 6

### 6. Prometheus Metrics
Exposed at each service's `/metrics` endpoint:

| Metric | Description |
|---|---|
| `marketstream.orders.created` | Total orders successfully created |
| `marketstream.orders.duplicate` | Blocked duplicate submissions |
| `marketstream.orders.publish.latency` | P99 Kafka publish time |
| `inventory_events_processed_total` | Inventory events by status |
| `inventory_reserve_duration_seconds` | Reservation latency histogram |
| `inventory_stock_level{item_id}` | Live stock gauge per item |
| `marketstream.payments.processed` | Successful payments |
| `marketstream.payments.duration` | Payment processing P50/P99 |
| `notifications_sent_total{channel,type}` | Notifications by channel |

---

## Quick Start

### Prerequisites
- Docker Desktop (or Docker Engine + Compose)
- Python 3.12+ (for load generator only)

### 1. Start Everything
```bash
docker compose up -d
```

Wait ~60 seconds for all services to be healthy.

### 2. Verify Health
```bash
# Order API
curl http://localhost:8080/api/v1/orders/health

# All containers
docker compose ps
```

### 3. Place Your First Order
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "userId":   "user-0001",
    "itemId":   "ITEM-002",
    "quantity": 1,
    "price":    999.99
  }'
```

### 4. Run the Load Generator
```bash
pip install aiohttp
python load_generator.py --orders 20 --rate 2
```

### 5. Demo All Features
```bash
# Idempotency demo (same key, 3 requests → same orderId)
python load_generator.py --idempotency-test

# DLQ demo (invalid items → routed to DLQ)
python load_generator.py --dlq-test

# Full concurrent burst
python load_generator.py --orders 50 --concurrent

# Run everything
python load_generator.py --all
```

---

## Observability

| Dashboard | URL |
|---|---|
| Kafka UI (topics, consumers, lag) | http://localhost:8090 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Order API Actuator | http://localhost:8081/actuator |
| Order API Metrics | http://localhost:8081/actuator/prometheus |

---

## Interview Talking Points

**"Walk me through what happens when a user places an order."**
> An HTTP POST hits Order API with an `Idempotency-Key` header. We check Redis first — if the key exists, we return the cached result immediately without any side effects. New orders are persisted to PostgreSQL, then an `ORDER_CREATED` event is published to `order_events` with `userId` as the partition key. The Inventory Service (Python) picks it up via consumer group, uses `SELECT FOR UPDATE` to atomically reserve stock, and publishes `INVENTORY_RESERVED`. Payment Service then processes the payment and publishes the result. Finally, the Notification Worker sends a confirmation email.

**"How do you prevent duplicate order processing?"**
> Three layers: (1) Idempotency-Key HTTP header with 24h Redis TTL prevents API-level duplication, (2) `enable.idempotence=true` on the Kafka producer prevents broker-level duplicates via sequence numbers, (3) each consumer checks a Redis key before processing any event.

**"What happens when the Payment Service crashes mid-processing?"**
> Kafka offsets are committed manually only after successful processing. On restart, the consumer replays from the last committed offset. Duplicate events are caught by the idempotency check in `PaymentProcessingService` (`findByOrderId`). After 3 retries, the event goes to `payment_events_dlq` for inspection.

**"How do you scale this system?"**
> Horizontally: add more inventory-service replicas — Kafka rebalances the 6 partitions automatically. Vertically: `concurrency=3` on `@KafkaListener` gives each instance 3 threads. The partition count (6) is the ceiling for parallel consumers per group.

---

## Project Structure

```
marketstream/
├── docker-compose.yml              # Full stack orchestration
├── load_generator.py               # Traffic simulation tool
├── infrastructure/
│   ├── init.sql                    # DB schema + seed data
│   └── prometheus/prometheus.yml   # Scrape config
├── order-api/                      # Java Spring Boot
│   ├── src/main/java/com/marketstream/
│   │   ├── OrderApiApplication.java
│   │   ├── controller/OrderController.java
│   │   ├── service/OrderService.java       ← idempotency, partitioning, retry
│   │   ├── model/{Order,OrderEvent}.java
│   │   ├── kafka/InventoryEventConsumer.java
│   │   └── config/KafkaProducerConfig.java ← idempotent producer setup
│   └── Dockerfile
├── inventory-service/              # Python async worker
│   ├── main.py                     ← consumer group, DLQ, retry, metrics
│   ├── requirements.txt
│   └── Dockerfile
├── payment-service/                # Java Spring Boot
│   ├── src/main/java/com/marketstream/
│   │   ├── kafka/PaymentEventConsumer.java ← @RetryableTopic, DLQ
│   │   └── service/PaymentProcessingService.java
│   └── Dockerfile
└── notification-worker/            # Python async worker
    ├── main.py                     ← final stage, audit trail
    └── Dockerfile
```
