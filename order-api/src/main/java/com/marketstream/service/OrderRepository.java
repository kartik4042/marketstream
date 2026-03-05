package com.marketstream.service;

import com.marketstream.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {
    Optional<Order> findByOrderId(String orderId);
    Optional<Order> findByIdempotencyKey(String idempotencyKey);
}
