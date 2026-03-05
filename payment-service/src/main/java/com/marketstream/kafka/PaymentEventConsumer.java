package com.marketstream.kafka;

import com.marketstream.model.Payment;
import com.marketstream.service.PaymentProcessingService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Payment Event Consumer
 *
 * Consumes inventory_events of type INVENTORY_RESERVED,
 * processes payments, and publishes payment_events.
 *
 * Features #1 (Consumer Groups), #2 (DLQ), #4 (Retry), #6 (Metrics)
 */
@Component
@Slf4j
public class PaymentEventConsumer {

    private final PaymentProcessingService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Counter paymentsProcessed;
    private final Counter paymentsFailed;
    private final Timer paymentLatency;

    public PaymentEventConsumer(
        PaymentProcessingService paymentService,
        KafkaTemplate<String, Object> kafkaTemplate,
        MeterRegistry meterRegistry
    ) {
        this.paymentService  = paymentService;
        this.kafkaTemplate   = kafkaTemplate;
        this.paymentsProcessed = Counter.builder("marketstream.payments.processed")
            .tag("status", "success").register(meterRegistry);
        this.paymentsFailed  = Counter.builder("marketstream.payments.failed")
            .register(meterRegistry);
        this.paymentLatency  = Timer.builder("marketstream.payments.duration")
            .description("Payment processing latency")
            .register(meterRegistry);
    }

    /**
     * Non-blocking retry: events go to retry topics instead of blocking the partition.
     * inventory_events → inventory_events-retry-0 → inventory_events-retry-1 → payment_events_dlq
     */
    @RetryableTopic(
        attempts     = "3",
        backoff      = @Backoff(delay = 3000, multiplier = 2.0, maxDelay = 60000),
        dltStrategy  = DltStrategy.FAIL_ON_ERROR,
        dltTopicSuffix = "_dlq",
        autoCreateTopics = "false"
    )
    @KafkaListener(
        topics      = "inventory_events",
        groupId     = "payment-service-group",
        concurrency = "3"   // Feature #1: 3 concurrent consumer threads
    )
    @Transactional
    public void onInventoryEvent(ConsumerRecord<String, Map<String, Object>> record) {
        Map<String, Object> event = record.value();
        String eventType = (String) event.get("eventType");

        if (!"INVENTORY_RESERVED".equals(eventType)) {
            return; // Only process successful inventory reservations
        }

        String orderId = (String) event.get("orderId");
        String userId  = (String) event.get("userId");
        String itemId  = (String) event.get("itemId");
        int    qty     = (Integer) event.get("quantity");
        double rawPrice = event.get("price") instanceof Number n ? n.doubleValue() : 0.0;
        BigDecimal amount = BigDecimal.valueOf(rawPrice * qty);

        log.info("Processing payment. orderId={} userId={} amount={}", orderId, userId, amount);

        Timer.Sample sample = Timer.start();
        try {
            Payment payment = paymentService.processPayment(orderId, userId, itemId, amount);
            sample.stop(paymentLatency);

            // Publish result
            Map<String, Object> paymentEvent = Map.of(
                "eventType",    payment.getStatus() == Payment.PaymentStatus.COMPLETED
                                  ? "PAYMENT_COMPLETED" : "PAYMENT_FAILED",
                "paymentId",    payment.getPaymentId(),
                "orderId",      orderId,
                "userId",       userId,
                "amount",       amount,
                "status",       payment.getStatus().name(),
                "timestamp",    java.time.Instant.now().toString()
            );

            kafkaTemplate.send("payment_events", userId, paymentEvent);

            if (payment.getStatus() == Payment.PaymentStatus.COMPLETED) {
                paymentsProcessed.increment();
                log.info("Payment completed. orderId={} paymentId={}", orderId, payment.getPaymentId());
            } else {
                paymentsFailed.increment();
                log.warn("Payment failed. orderId={} reason=DECLINED", orderId);
            }

        } catch (Exception e) {
            sample.stop(paymentLatency);
            paymentsFailed.increment();
            log.error("Payment processing error. orderId={}", orderId, e);
            throw e; // Let @RetryableTopic handle the retry
        }
    }
}
