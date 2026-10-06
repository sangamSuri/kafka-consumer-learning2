package com.example.demo.kafka.billing;

import com.example.demo.kafka.idempotency.ProcessedEventStore;
import com.example.demo.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Critical group: no loss, no duplicate side effects. */
@Service
public class BillingService {

    private static final Logger log = LoggerFactory.getLogger(BillingService.class);
    private static final String GROUP = "billing-service";

    private final ProcessedEventStore processed;

    public BillingService(ProcessedEventStore processed) {
        this.processed = processed;
    }

    public void handle(OrderEvent event) {
        if (processed.alreadyProcessed(GROUP, event.eventId())) {
            return; // duplicate: already handled, skip safely
        }
        log.info("Billing: generating invoice for orderId={} amount={}", event.orderId(), event.amount());
        // business side effect (e.g. invoices.save(Invoice.from(event))) would go here,
        // in the same transaction as marking the event processed (guide §8.3)
        processed.markProcessed(GROUP, event.eventId());
    }
}
