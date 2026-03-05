package com.marketstream.service;

import com.marketstream.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {
    Optional<Payment> findByOrderId(String orderId);
    Optional<Payment> findByIdempotencyKey(String key);
}
