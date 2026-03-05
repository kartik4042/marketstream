package com.marketstream.kafka;

import com.marketstream.model.Order;
import com.marketstream.service.OrderRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consumes inventory_events and updates order status.
 *
 * Feature #1: Consumer Groups - multiple instances share load
 * Feature #2: DLQ via @RetryableTopic
 * Feature #4: Exponential backoff via @RetryableTopic
 */
@Component
@Slf4j
public class InventoryEventConsumer {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Counter inventoryConfirmedCounter;
    private final Counter inventoryFailedCounter;

    public InventoryEventConsumer(
        OrderRepository orderRepository,
        KafkaTemplate<String, Object> kafkaTemplate,
        MeterRegistry meterRegistry
    ) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate   = kafkaTemplate;
        this.inventoryConfirmedCounter = Counter.builder("marketstream.inventory.confirmed")
            .register(meterRegistry);
        this.inventoryFailedCounter = Counter.builder("marketstream.inventory.failed")
            .register(meterRegistry);
    }

    /**
     * @RetryableTopic handles:
     * - Automatic retry topics: inventory_events-retry-0, inventory_events-retry-1
     * - DLQ: inventory_events_dlq
     * - Exponential backoff between retries
     * - Non-blocking retries (doesn't hold up the partition)
     */
    @RetryableTopic(
        attempts         = "3",
        backoff          = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30000),
        dltStrategy      = DltStrategy.FAIL_ON_ERROR,
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltTopicSuffix   = "_dlq"
    )
    @KafkaListener(
        topics          = "inventory_events",
        groupId         = "order-api-inventory-group",
        concurrency     = "3"  // 3 concurrent consumers (Feature #1: Consumer Groups)
    )
    public void consumeInventoryEvent(ConsumerRecord<String, Map<String, Object>> record) {
        Map<String, Object> event = record.value();
        String eventType = (String) event.get("eventType");
        String orderId   = (String) event.get("orderId");

        log.info("Processing inventory event. type={}, orderId={}, partition={}, offset={}",
            eventType, orderId, record.partition(), record.offset());

        switch (eventType) {
            case "INVENTORY_RESERVED" -> handleInventoryReserved(orderId, event);
            case "INVENTORY_INSUFFICIENT" -> handleInventoryInsufficient(orderId, event);
            default -> log.warn("Unknown inventory event type: {}", eventType);
        }
    }

    private void handleInventoryReserved(String orderId, Map<String, Object> event) {
        orderRepository.findByOrderId(orderId).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.INVENTORY_RESERVED);
            orderRepository.save(order);
            inventoryConfirmedCounter.increment();
            log.info("Order status updated to INVENTORY_RESERVED. orderId={}", orderId);
        }, () -> log.warn("Order not found for inventory event. orderId={}", orderId));
    }

    private void handleInventoryInsufficient(String orderId, Map<String, Object> event) {
        orderRepository.findByOrderId(orderId).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.FAILED);
            orderRepository.save(order);
            inventoryFailedCounter.increment();
            log.warn("Order failed: insufficient inventory. orderId={}", orderId);
        }, () -> log.warn("Order not found. orderId={}", orderId));
    }
}
