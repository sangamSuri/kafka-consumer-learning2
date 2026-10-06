package com.example.demo.kafka.idempotency;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dedup marker store: tracks (consumerGroup, eventId) pairs a handler has already processed.
 * In-memory for this demo; swap for a {@code processed_event} table (guide §8.1-8.2) backed by
 * your transactional datastore so the business write and the dedup marker commit atomically.
 */
@Component
public class ProcessedEventStore {

    private final Set<String> processed = ConcurrentHashMap.newKeySet();

    public boolean alreadyProcessed(String consumerGroup, String eventId) {
        return processed.contains(key(consumerGroup, eventId));
    }

    public void markProcessed(String consumerGroup, String eventId) {
        processed.add(key(consumerGroup, eventId));
    }

    private String key(String consumerGroup, String eventId) {
        return consumerGroup + ":" + eventId;
    }
}
