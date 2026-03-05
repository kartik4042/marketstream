package com.marketstream.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Base event for all marketstream events flowing through Kafka.
 * Uses Jackson polymorphism for clean deserialization.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderEvent.OrderCreated.class,   name = "ORDER_CREATED"),
    @JsonSubTypes.Type(value = OrderEvent.OrderConfirmed.class, name = "ORDER_CONFIRMED"),
    @JsonSubTypes.Type(value = OrderEvent.OrderFailed.class,    name = "ORDER_FAILED"),
})
public sealed interface OrderEvent permits
    OrderEvent.OrderCreated,
    OrderEvent.OrderConfirmed,
    OrderEvent.OrderFailed {

    String orderId();
    String userId();
    String idempotencyKey();
    Instant timestamp();

    // ── Events ──────────────────────────────────────────────────────────

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    final class OrderCreated implements OrderEvent {
        private String orderId;
        private String userId;
        private String itemId;
        private Integer quantity;
        private BigDecimal price;
        private String idempotencyKey;
        @Builder.Default
        private Instant timestamp = Instant.now();
        private String eventType = "ORDER_CREATED";
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    final class OrderConfirmed implements OrderEvent {
        private String orderId;
        private String userId;
        private String itemId;
        private Integer quantity;
        private BigDecimal totalAmount;
        private String idempotencyKey;
        @Builder.Default
        private Instant timestamp = Instant.now();
        private String eventType = "ORDER_CONFIRMED";
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    final class OrderFailed implements OrderEvent {
        private String orderId;
        private String userId;
        private String reason;
        private String idempotencyKey;
        @Builder.Default
        private Instant timestamp = Instant.now();
        private String eventType = "ORDER_FAILED";
    }
}
