package com.example.demo.kafka.notification;

import com.example.demo.kafka.idempotency.ProcessedEventStore;
import com.example.demo.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Best-effort group: low latency, drop to DLT quickly rather than stall. */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);
    private static final String GROUP = "notification-service";

    private final ProcessedEventStore processed;

    public NotificationService(ProcessedEventStore processed) {
        this.processed = processed;
    }

    public void send(OrderEvent event) {
        if (processed.alreadyProcessed(GROUP, event.eventId())) {
            return; // duplicate: avoid double-sending the same alert
        }
        log.info("Notification: sending alert for orderId={} type={}", event.orderId(), event.type());
        // outbound HTTP call with a timeout well under a second would go here
        processed.markProcessed(GROUP, event.eventId());
    }
}
