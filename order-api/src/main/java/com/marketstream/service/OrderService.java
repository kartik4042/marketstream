package com.marketstream.service;

import com.marketstream.model.Order;
import com.marketstream.model.OrderEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * OrderService - Core business logic for order creation.
 *
 * Key patterns implemented:
 *  1. Idempotent processing via Redis cache (Feature #3)
 *  2. Partitioning by user_id (Feature #5)
 *  3. Retry with exponential backoff (Feature #4)
 *  4. Prometheus metrics (Feature #6)
 *  5. DLQ on failure (Feature #2)
 */
@Service
@Slf4j
public class OrderService {

    private static final String IDEMPOTENCY_PREFIX = "order:idempotency:";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final OrderRepository orderRepository;

    // Metrics
    private final Counter ordersCreatedCounter;
    private final Counter ordersDuplicateCounter;
    private final Counter ordersFailedCounter;
    private final Timer orderPublishTimer;

    @Value("${app.kafka.topics.order-events}")
    private String orderEventsTopic;

    @Value("${app.kafka.topics.order-dlq}")
    private String orderDlqTopic;

    @Value("${app.idempotency.ttl-hours:24}")
    private long idempotencyTtlHours;

    public OrderService(
        KafkaTemplate<String, Object> kafkaTemplate,
        RedisTemplate<String, String> redisTemplate,
        OrderRepository orderRepository,
        MeterRegistry meterRegistry
    ) {
        this.kafkaTemplate   = kafkaTemplate;
        this.redisTemplate   = redisTemplate;
        this.orderRepository = orderRepository;

        // Register Prometheus metrics
        this.ordersCreatedCounter   = Counter.builder("marketstream.orders.created")
            .description("Total orders successfully created")
            .register(meterRegistry);
        this.ordersDuplicateCounter = Counter.builder("marketstream.orders.duplicate")
            .description("Duplicate order attempts blocked")
            .register(meterRegistry);
        this.ordersFailedCounter    = Counter.builder("marketstream.orders.failed")
            .description("Orders that failed processing")
            .register(meterRegistry);
        this.orderPublishTimer      = Timer.builder("marketstream.orders.publish.latency")
            .description("Latency of publishing order events to Kafka")
            .register(meterRegistry);
    }

    /**
     * Creates an order with full idempotency guarantee.
     * Same idempotency key = same result, no duplicate processing.
     */
    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        // ── Step 1: Idempotency Check (Feature #3) ─────────────────────────
        String idempotencyKey = IDEMPOTENCY_PREFIX + request.idempotencyKey();
        String existingOrderId = redisTemplate.opsForValue().get(idempotencyKey);

        if (existingOrderId != null) {
            log.info("Duplicate order request detected. idempotencyKey={}, orderId={}",
                request.idempotencyKey(), existingOrderId);
            ordersDuplicateCounter.increment();
            return orderRepository.findByOrderId(existingOrderId)
                .orElseThrow(() -> new IllegalStateException("Order not found: " + existingOrderId));
        }

        // ── Step 2: Create & Persist Order ─────────────────────────────────
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        Order order = Order.builder()
            .orderId(orderId)
            .userId(request.userId())
            .itemId(request.itemId())
            .quantity(request.quantity())
            .price(request.price())
            .idempotencyKey(request.idempotencyKey())
            .status(Order.OrderStatus.PENDING)
            .build();

        orderRepository.save(order);

        // ── Step 3: Cache for idempotency (TTL = 24h) ──────────────────────
        redisTemplate.opsForValue().set(idempotencyKey, orderId, Duration.ofHours(idempotencyTtlHours));

        // ── Step 4: Publish to Kafka (partitioned by userId) ───────────────
        // Feature #5: Partition key = userId → all orders from same user go to same partition
        // This guarantees ordering for per-user events
        publishOrderEvent(order);

        ordersCreatedCounter.increment();
        log.info("Order created. orderId={}, userId={}, itemId={}", orderId, request.userId(), request.itemId());
        return order;
    }

    /**
     * Publish order event with retry + exponential backoff (Feature #4).
     * On final failure, route to DLQ (Feature #2).
     */
    @Retryable(
        retryFor  = { Exception.class },
        maxAttempts = 3,
        backoff   = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000)
    )
    private void publishOrderEvent(Order order) {
        Timer.Sample sample = Timer.start();

        OrderEvent.OrderCreated event = OrderEvent.OrderCreated.builder()
            .orderId(order.getOrderId())
            .userId(order.getUserId())
            .itemId(order.getItemId())
            .quantity(order.getQuantity())
            .price(order.getPrice())
            .idempotencyKey(order.getIdempotencyKey())
            .build();

        try {
            // Partition key = userId (Feature #5)
            CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(orderEventsTopic, order.getUserId(), event);

            future.whenComplete((result, ex) -> {
                sample.stop(orderPublishTimer);
                if (ex != null) {
                    log.error("Failed to publish order event. orderId={}", order.getOrderId(), ex);
                    routeToDlq(order, ex.getMessage());
                    ordersFailedCounter.increment();
                } else {
                    log.info("Order event published. orderId={}, partition={}, offset={}",
                        order.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            sample.stop(orderPublishTimer);
            log.error("Critical failure publishing order. Routing to DLQ. orderId={}", order.getOrderId(), e);
            routeToDlq(order, e.getMessage());
            throw e;
        }
    }

    /**
     * Dead Letter Queue routing (Feature #2).
     * Failed events preserved for inspection and reprocessing.
     */
    private void routeToDlq(Order order, String errorMessage) {
        try {
            OrderEvent.OrderFailed dlqEvent = OrderEvent.OrderFailed.builder()
                .orderId(order.getOrderId())
                .userId(order.getUserId())
                .reason("PUBLISH_FAILURE: " + errorMessage)
                .idempotencyKey(order.getIdempotencyKey())
                .build();

            kafkaTemplate.send(orderDlqTopic, order.getUserId(), dlqEvent);
            log.warn("Order routed to DLQ. orderId={}, reason={}", order.getOrderId(), errorMessage);
        } catch (Exception dlqEx) {
            log.error("CRITICAL: Failed to write to DLQ! orderId={}", order.getOrderId(), dlqEx);
        }
    }

    public record CreateOrderRequest(
        String userId,
        String itemId,
        Integer quantity,
        BigDecimal price,
        String idempotencyKey
    ) {}
}
