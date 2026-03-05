"""
Notification Worker - Python Kafka Consumer
============================================
Consumes payment_events and sends notifications to users.
The final stage in the marketstream event pipeline.

In production this would integrate with:
  - Email (SendGrid / SES)
  - Push notifications (FCM / APNs)
  - SMS (Twilio)

Demonstrates:
  #1  Consumer Groups (notification-worker-group)
  #2  DLQ for failed notifications
  #4  Retry with exponential backoff
  #6  Prometheus metrics
"""

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import Counter, Histogram, start_http_server

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("notification-worker")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP  = "notification-worker-group"
MAX_RETRIES     = 3
INITIAL_BACKOFF = 1.0

TOPIC_PAYMENT_EVENTS       = "payment_events"
TOPIC_NOTIFICATION_EVENTS  = "notification_events"
TOPIC_NOTIFICATION_DLQ     = "notification_events_dlq"

# ── Prometheus Metrics ────────────────────────────────────────────────────────
NOTIFICATIONS_SENT = Counter(
    "notifications_sent_total",
    "Notifications dispatched",
    ["channel", "type"]   # channel: email/sms/push, type: success/failure
)
NOTIFICATION_LATENCY = Histogram(
    "notification_dispatch_seconds",
    "Time to dispatch a notification",
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
)
DLQ_EVENTS = Counter("notification_dlq_total", "Events sent to DLQ")


# ── Notification Templates ────────────────────────────────────────────────────

@dataclass
class Notification:
    user_id: str
    order_id: str
    channel: str
    subject: str
    body: str
    notification_type: str


def build_notification(event: dict) -> Optional[Notification]:
    event_type = event.get("eventType", "")
    order_id   = event.get("orderId", "unknown")
    user_id    = event.get("userId", "unknown")
    amount     = event.get("amount", 0)

    if event_type == "PAYMENT_COMPLETED":
        return Notification(
            user_id=user_id,
            order_id=order_id,
            channel="email",
            subject=f"Order {order_id} Confirmed! 🎉",
            body=(
                f"Hi {user_id},\n\n"
                f"Great news! Your order {order_id} has been confirmed.\n"
                f"Amount charged: ${amount:.2f}\n\n"
                f"Your item will be shipped within 2-3 business days.\n\n"
                f"Thanks for shopping with us!\n"
                f"— MarketStream Team"
            ),
            notification_type="ORDER_CONFIRMED"
        )
    elif event_type == "PAYMENT_FAILED":
        return Notification(
            user_id=user_id,
            order_id=order_id,
            channel="email",
            subject=f"Action Required: Order {order_id} Payment Failed",
            body=(
                f"Hi {user_id},\n\n"
                f"Unfortunately, your payment for order {order_id} could not be processed.\n"
                f"Please update your payment method and try again.\n\n"
                f"Order ID: {order_id}\n"
                f"Amount: ${amount:.2f}\n\n"
                f"If you need help, contact our support team.\n"
                f"— MarketStream Team"
            ),
            notification_type="PAYMENT_FAILED"
        )
    return None


class NotificationWorker:

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._running = True

    async def start(self):
        log.info("Starting Notification Worker...")

        start_http_server(8001)
        log.info("Prometheus metrics at :8001/metrics")

        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
            acks="all",
            enable_idempotent=True,
        )
        await self.producer.start()

        # Feature #1: Consumer Group - multiple workers can run in parallel
        self.consumer = AIOKafkaConsumer(
            TOPIC_PAYMENT_EVENTS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode()),
            key_deserializer=lambda k: k.decode() if k else None,
        )
        await self.consumer.start()

        log.info("Notification Worker consuming '%s' (group: %s)",
                 TOPIC_PAYMENT_EVENTS, CONSUMER_GROUP)

        await self._consume_loop()

    async def _consume_loop(self):
        try:
            async for msg in self.consumer:
                if not self._running:
                    break

                await self._process_with_retry(msg)
                await self.consumer.commit()
        finally:
            await self._shutdown()

    async def _process_with_retry(self, msg):
        """Retry with exponential backoff (Feature #4)."""
        event = msg.value
        attempt = 0

        while attempt < MAX_RETRIES:
            try:
                await self._dispatch_notification(event)
                return
            except Exception as e:
                attempt += 1
                if attempt >= MAX_RETRIES:
                    log.error("Max retries reached. Routing to DLQ. error=%s", str(e))
                    await self._send_to_dlq(msg, str(e))
                    DLQ_EVENTS.inc()
                    return

                backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
                log.warning("Retry %d/%d in %.1fs. error=%s", attempt, MAX_RETRIES, backoff, str(e))
                await asyncio.sleep(backoff)

    async def _dispatch_notification(self, event: dict):
        """Build and send notification, emit metrics."""
        notification = build_notification(event)
        if not notification:
            log.debug("No notification handler for event type: %s", event.get("eventType"))
            return

        start = time.time()

        # Simulate sending email (in production: call SendGrid API, etc.)
        await self._simulate_send(notification)

        elapsed = time.time() - start
        NOTIFICATION_LATENCY.observe(elapsed)
        NOTIFICATIONS_SENT.labels(
            channel=notification.channel,
            type=notification.notification_type
        ).inc()

        # Publish notification event for audit trail
        audit_event = {
            "eventType": "NOTIFICATION_SENT",
            "userId": notification.user_id,
            "orderId": notification.order_id,
            "channel": notification.channel,
            "notificationType": notification.notification_type,
            "sentAt": datetime.now(timezone.utc).isoformat(),
        }
        await self.producer.send_and_wait(
            TOPIC_NOTIFICATION_EVENTS,
            key=notification.user_id,
            value=audit_event
        )

        log.info(
            "Notification dispatched. userId=%s orderId=%s type=%s channel=%s latency=%.3fs",
            notification.user_id,
            notification.order_id,
            notification.notification_type,
            notification.channel,
            elapsed
        )

    async def _simulate_send(self, notification: Notification):
        """
        Simulates email/SMS/push dispatch.
        In production: integrate with SendGrid, Twilio, FCM, etc.
        """
        await asyncio.sleep(0.02)  # Simulate ~20ms network call
        log.debug("📧 [%s → %s] %s", notification.channel, notification.user_id, notification.subject)
        log.debug("   %s", notification.body[:100] + "...")

    async def _send_to_dlq(self, msg, error: str):
        """Dead Letter Queue (Feature #2)."""
        dlq_payload = {
            "originalTopic": msg.topic,
            "originalOffset": msg.offset,
            "originalPartition": msg.partition,
            "originalValue": msg.value,
            "errorMessage": error,
            "failedAt": datetime.now(timezone.utc).isoformat(),
        }
        try:
            await self.producer.send_and_wait(
                TOPIC_NOTIFICATION_DLQ,
                key=msg.key,
                value=dlq_payload
            )
        except Exception as e:
            log.error("CRITICAL: Failed to write notification to DLQ: %s", str(e))

    async def _shutdown(self):
        log.info("Shutting down Notification Worker...")
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    def handle_signal(self):
        self._running = False


async def main():
    worker = NotificationWorker()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.handle_signal)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
