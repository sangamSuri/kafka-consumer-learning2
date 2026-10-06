package com.example.demo.kafka.notification;

import com.example.demo.kafka.model.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Best-effort group: a few quick retries, then DLT; never block the partition for long. */
@Component
public class NotificationListener {

    private final NotificationService notificationService;

    public NotificationListener(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(
            id = "notification-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "notificationFactory")
    public void notification(OrderEvent event) {
        notificationService.send(event);
    }
}
