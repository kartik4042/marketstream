package com.marketstream.controller;

import com.marketstream.model.Order;
import com.marketstream.service.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderService orderService;

    /**
     * POST /api/v1/orders
     *
     * Idempotency-Key header prevents duplicate orders on network retries.
     * This is exactly how eBay/Amazon handle order submission.
     */
    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(
        @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKeyHeader,
        @Valid @RequestBody CreateOrderRequest request
    ) {
        // Use provided idempotency key or generate one
        String idempotencyKey = idempotencyKeyHeader != null
            ? idempotencyKeyHeader
            : UUID.randomUUID().toString();

        log.info("Received order request. userId={}, itemId={}, idempotencyKey={}",
            request.userId(), request.itemId(), idempotencyKey);

        Order order = orderService.createOrder(new OrderService.CreateOrderRequest(
            request.userId(),
            request.itemId(),
            request.quantity(),
            request.price(),
            idempotencyKey
        ));

        return ResponseEntity
            .status(HttpStatus.CREATED)
            .body(OrderResponse.from(order));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
            "status", "UP",
            "service", "order-api",
            "version", "1.0.0"
        ));
    }

    // ── DTOs ──────────────────────────────────────────────────────────────

    public record CreateOrderRequest(
        @NotBlank String userId,
        @NotBlank String itemId,
        @Min(1) @Max(100) Integer quantity,
        @NotNull @DecimalMin("0.01") BigDecimal price
    ) {}

    public record OrderResponse(
        String orderId,
        String userId,
        String itemId,
        Integer quantity,
        BigDecimal price,
        String status,
        String createdAt
    ) {
        static OrderResponse from(Order order) {
            return new OrderResponse(
                order.getOrderId(),
                order.getUserId(),
                order.getItemId(),
                order.getQuantity(),
                order.getPrice(),
                order.getStatus().name(),
                order.getCreatedAt() != null ? order.getCreatedAt().toString() : null
            );
        }
    }
}
