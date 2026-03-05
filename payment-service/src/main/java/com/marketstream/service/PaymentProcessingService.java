package com.marketstream.service;

import com.marketstream.model.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

/**
 * PaymentProcessingService
 *
 * Simulates a payment gateway with configurable success rate.
 * Real-world: this would call Stripe, Braintree, etc.
 *
 * Idempotency: same orderId always returns same Payment record.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentProcessingService {

    private final PaymentRepository paymentRepository;

    @Value("${app.payment.success-rate:0.85}")
    private double successRate;

    private final Random random = new Random();

    @Transactional
    public Payment processPayment(
        String orderId,
        String userId,
        String itemId,
        BigDecimal amount
    ) {
        // ── Idempotency: Return existing payment if already processed ──────
        return paymentRepository.findByOrderId(orderId).orElseGet(() -> {
            String paymentId     = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            String idempotencyKey = "payment-" + orderId;

            // Simulate payment gateway response
            boolean approved = random.nextDouble() < successRate;
            Payment.PaymentStatus status = approved
                ? Payment.PaymentStatus.COMPLETED
                : Payment.PaymentStatus.FAILED;

            Payment payment = Payment.builder()
                .paymentId(paymentId)
                .orderId(orderId)
                .userId(userId)
                .amount(amount)
                .status(status)
                .idempotencyKey(idempotencyKey)
                .processedAt(Instant.now())
                .build();

            return paymentRepository.save(payment);
        });
    }
}
