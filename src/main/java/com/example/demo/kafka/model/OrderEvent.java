package com.example.demo.kafka.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Produced with {@code orderId} as the Kafka message key so all events
 * for one order land on the same partition and stay ordered.
 */
public record OrderEvent(
        String eventId,
        String orderId,
        String type,
        BigDecimal amount,
        Instant createdAt) {
}
