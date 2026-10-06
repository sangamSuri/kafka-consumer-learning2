package com.example.demo.kafka.billing;

import com.example.demo.kafka.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Critical group: blocking retries, record ack mode, idempotent handler. */
@Component
public class BillingListener {

    private final BillingService billingService;

    public BillingListener(BillingService billingService) {
        this.billingService = billingService;
    }

    @KafkaListener(
            id = "billing-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "billingFactory")
    public void billing(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        MDC.put("eventId", event.eventId());
        try {
            billingService.handle(event);
        } finally {
            MDC.clear();
        }
    }
}
