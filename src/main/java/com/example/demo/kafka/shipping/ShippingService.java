package com.example.demo.kafka.shipping;

import com.example.demo.kafka.idempotency.ProcessedEventStore;
import com.example.demo.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Important group: no loss, DB + HTTP side effects, non-blocking retries via retry topics. */
@Service
public class ShippingService {

    private static final Logger log = LoggerFactory.getLogger(ShippingService.class);
    private static final String GROUP = "shipping-service";

    private final ProcessedEventStore processed;

    public ShippingService(ProcessedEventStore processed) {
        this.processed = processed;
    }

    public void handle(OrderEvent event) {
        if (processed.alreadyProcessed(GROUP, event.eventId())) {
            return; // duplicate: retry topic or rebalance redelivery, already applied
        }
        log.info("Shipping: updating shipment for orderId={} type={}", event.orderId(), event.type());
        // DB write + downstream HTTP call would go here, each with an idempotency key (event.eventId())
        processed.markProcessed(GROUP, event.eventId());
    }
}
