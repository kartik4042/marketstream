"""
Inventory Service - Python Kafka Worker
=======================================
Consumes order_events, checks/reserves inventory,
publishes inventory_events.

Features demonstrated:
  #1  Consumer Groups (multiple workers share partitions)
  #2  Dead Letter Queue
  #3  Idempotent processing via Redis
  #4  Retry with exponential backoff
  #5  Partitioned consumption by user_id
  #6  Prometheus metrics
"""

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("inventory-service")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_URL     = os.getenv("POSTGRES_URL", "postgresql://marketstream:marketstream_secret@localhost:5432/marketstream")
REDIS_HOST       = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT", "6379"))
CONSUMER_GROUP   = "inventory-service-group"
IDEMPOTENCY_TTL  = 86400  # 24 hours
MAX_RETRIES      = 3
INITIAL_BACKOFF  = 1.0   # seconds

# ── Topics ────────────────────────────────────────────────────────────────────
TOPIC_ORDER_EVENTS     = "order_events"
TOPIC_INVENTORY_EVENTS = "inventory_events"
TOPIC_INVENTORY_DLQ    = "inventory_events_dlq"

# ── Prometheus Metrics (Feature #6) ──────────────────────────────────────────
EVENTS_PROCESSED = Counter(
    "inventory_events_processed_total",
    "Total inventory events processed",
    ["status"]  # success, duplicate, failed
)
RESERVE_LATENCY = Histogram(
    "inventory_reserve_duration_seconds",
    "Time to reserve inventory",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
)
INVENTORY_LEVEL = Gauge(
    "inventory_stock_level",
    "Current stock level per item",
    ["item_id"]
)
DLQ_EVENTS = Counter(
    "inventory_dlq_events_total",
    "Events sent to DLQ"
)


# ── Event Models ──────────────────────────────────────────────────────────────

@dataclass
class InventoryReservedEvent:
    orderId: str
    userId: str
    itemId: str
    quantity: int
    eventType: str = "INVENTORY_RESERVED"
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


@dataclass
class InventoryInsufficientEvent:
    orderId: str
    userId: str
    itemId: str
    requestedQuantity: int
    availableQuantity: int
    reason: str
    eventType: str = "INVENTORY_INSUFFICIENT"
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


# ── Inventory Service ─────────────────────────────────────────────────────────

class InventoryService:

    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[aioredis.Redis] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._running = True

    async def start(self):
        log.info("Starting Inventory Service...")

        # Start Prometheus metrics endpoint
        start_http_server(8000)
        log.info("Prometheus metrics available at :8000/metrics")

        # Connect to infrastructure
        self.db_pool = await asyncpg.create_pool(POSTGRES_URL, min_size=2, max_size=10)
        self.redis   = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        # Kafka Producer - idempotent
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
            acks="all",
            enable_idempotent=True,   # Feature #3: Idempotent producer
            max_in_flight_requests_per_connection=5,
        )
        await self.producer.start()

        # Kafka Consumer - Consumer Group (Feature #1)
        # Multiple instances of this service share the 6 partitions of order_events
        self.consumer = AIOKafkaConsumer(
            TOPIC_ORDER_EVENTS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,   # Feature #1: Consumer Groups
            auto_offset_reset="earliest",
            enable_auto_commit=False,   # Manual commit for reliability
            value_deserializer=lambda v: json.loads(v.decode()),
            key_deserializer=lambda k: k.decode() if k else None,
            max_poll_records=50,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        await self.consumer.start()

        log.info("Inventory Service started. Consuming from '%s' (group: %s)",
                 TOPIC_ORDER_EVENTS, CONSUMER_GROUP)
        await self._consume_loop()

    async def _consume_loop(self):
        """Main consumption loop with manual offset management."""
        try:
            async for msg in self.consumer:
                if not self._running:
                    break

                log.debug("Received message. partition=%d offset=%d key=%s",
                          msg.partition, msg.offset, msg.key)

                await self._process_with_retry(msg)

                # Manual commit only after successful processing
                await self.consumer.commit()

        finally:
            await self._shutdown()

    async def _process_with_retry(self, msg):
        """
        Retry with exponential backoff (Feature #4).
        On exhaustion, route to DLQ (Feature #2).
        """
        event = msg.value
        attempt = 0

        while attempt < MAX_RETRIES:
            try:
                await self._process_event(event)
                return
            except Exception as e:
                attempt += 1
                if attempt >= MAX_RETRIES:
                    log.error("Max retries reached. Routing to DLQ. orderId=%s error=%s",
                              event.get("orderId"), str(e))
                    await self._send_to_dlq(msg, str(e))
                    DLQ_EVENTS.inc()
                    EVENTS_PROCESSED.labels(status="failed").inc()
                    return

                # Exponential backoff: 1s, 2s, 4s
                backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
                log.warning("Retry %d/%d in %.1fs. orderId=%s error=%s",
                            attempt, MAX_RETRIES, backoff,
                            event.get("orderId"), str(e))
                await asyncio.sleep(backoff)

    async def _process_event(self, event: dict):
        """Core event processing logic."""
        event_type = event.get("eventType")

        if event_type != "ORDER_CREATED":
            log.debug("Skipping non-order event: %s", event_type)
            return

        order_id  = event["orderId"]
        user_id   = event["userId"]
        item_id   = event["itemId"]
        quantity  = event["quantity"]

        # ── Idempotency Check (Feature #3) ───────────────────────────────────
        idempotency_key = f"inventory:processed:{order_id}"
        already_processed = await self.redis.get(idempotency_key)
        if already_processed:
            log.info("Duplicate event skipped. orderId=%s", order_id)
            EVENTS_PROCESSED.labels(status="duplicate").inc()
            return

        # ── Reserve Inventory ─────────────────────────────────────────────────
        with RESERVE_LATENCY.time():
            reserved = await self._try_reserve_inventory(order_id, item_id, quantity)

        if reserved:
            # Mark as processed in Redis (TTL = 24h)
            await self.redis.setex(idempotency_key, IDEMPOTENCY_TTL, "1")

            # Update Prometheus gauge
            stock = await self._get_stock_level(item_id)
            INVENTORY_LEVEL.labels(item_id=item_id).set(stock)

            # Publish success event
            out_event = InventoryReservedEvent(
                orderId=order_id,
                userId=user_id,
                itemId=item_id,
                quantity=quantity
            )
            await self.producer.send_and_wait(
                TOPIC_INVENTORY_EVENTS,
                key=user_id,   # Feature #5: Partition by userId
                value=asdict(out_event)
            )
            EVENTS_PROCESSED.labels(status="success").inc()
            log.info("Inventory reserved. orderId=%s itemId=%s qty=%d", order_id, item_id, quantity)
        else:
            # Publish failure event
            available = await self._get_stock_level(item_id)
            out_event = InventoryInsufficientEvent(
                orderId=order_id,
                userId=user_id,
                itemId=item_id,
                requestedQuantity=quantity,
                availableQuantity=available,
                reason="Insufficient stock"
            )
            await self.producer.send_and_wait(
                TOPIC_INVENTORY_EVENTS,
                key=user_id,
                value=asdict(out_event)
            )
            EVENTS_PROCESSED.labels(status="failed").inc()
            log.warning("Insufficient inventory. orderId=%s itemId=%s requested=%d available=%d",
                        order_id, item_id, quantity, available)

    async def _try_reserve_inventory(self, order_id: str, item_id: str, quantity: int) -> bool:
        """
        Atomic inventory reservation using PostgreSQL row-level locking.
        SELECT FOR UPDATE prevents race conditions between concurrent workers.
        """
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT quantity, reserved FROM inventory WHERE item_id = $1 FOR UPDATE",
                    item_id
                )
                if not row:
                    log.error("Item not found in inventory: %s", item_id)
                    return False

                available = row["quantity"] - row["reserved"]
                if available < quantity:
                    return False

                # Reserve the stock
                await conn.execute(
                    """UPDATE inventory
                       SET reserved = reserved + $1, updated_at = NOW()
                       WHERE item_id = $2""",
                    quantity, item_id
                )
                return True

    async def _get_stock_level(self, item_id: str) -> int:
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT quantity - reserved AS available FROM inventory WHERE item_id = $1",
                item_id
            )
            return row["available"] if row else 0

    async def _send_to_dlq(self, msg, error: str):
        """Dead Letter Queue (Feature #2) - preserve failed events."""
        dlq_payload = {
            "originalTopic": msg.topic,
            "originalPartition": msg.partition,
            "originalOffset": msg.offset,
            "originalKey": msg.key,
            "originalValue": msg.value,
            "errorMessage": error,
            "failedAt": datetime.now(timezone.utc).isoformat(),
        }
        try:
            await self.producer.send_and_wait(
                TOPIC_INVENTORY_DLQ,
                key=msg.key,
                value=dlq_payload
            )
            log.warning("Event sent to DLQ. topic=%s offset=%d", msg.topic, msg.offset)
        except Exception as e:
            log.error("CRITICAL: Failed to write to DLQ! %s", str(e))

    async def _shutdown(self):
        log.info("Shutting down Inventory Service...")
        self._running = False
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        if self.db_pool:
            await self.db_pool.close()
        if self.redis:
            await self.redis.close()

    def handle_signal(self):
        log.info("Shutdown signal received")
        self._running = False


async def main():
    service = InventoryService()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, service.handle_signal)

    await service.start()


if __name__ == "__main__":
    asyncio.run(main())
