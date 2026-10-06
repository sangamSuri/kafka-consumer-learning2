package com.example.demo.kafka.analytics;

import com.example.demo.kafka.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/** Bulk/batch group: batch listener for throughput, per the production guide §9. */
@Component
public class AnalyticsListener {

    private final AnalyticsService analyticsService;

    public AnalyticsListener(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @KafkaListener(
            id = "analytics-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "analyticsFactory")
    public void analytics(List<ConsumerRecord<String, OrderEvent>> records) {
        List<OrderEvent> events = records.stream().map(ConsumerRecord::value).toList();
        analyticsService.bulkInsert(events);
    }
}
