package com.example.demo.kafka.analytics;

import com.example.demo.kafka.idempotency.ProcessedEventStore;
import com.example.demo.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/** Bulk/batch group: high throughput, replayable; retry the batch, then record-by-record DLT. */
@Service
public class AnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);
    private static final String GROUP = "analytics-service";

    private final ProcessedEventStore processed;

    public AnalyticsService(ProcessedEventStore processed) {
        this.processed = processed;
    }

    public void bulkInsert(List<OrderEvent> events) {
        List<OrderEvent> toInsert = events.stream()
                .filter(e -> !processed.alreadyProcessed(GROUP, e.eventId()))
                .toList();
        if (toInsert.isEmpty()) {
            return; // whole batch already applied (e.g. redelivered after a rebalance)
        }
        log.info("Analytics: bulk-inserting {} events ({} duplicates skipped)",
                toInsert.size(), events.size() - toInsert.size());
        // one bulk, idempotent (upsert) write per poll would go here
        toInsert.forEach(e -> processed.markProcessed(GROUP, e.eventId()));
    }
}
