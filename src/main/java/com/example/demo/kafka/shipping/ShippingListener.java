package com.example.demo.kafka.shipping;

import com.example.demo.kafka.exception.ValidationException;
import com.example.demo.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** Important group: a slow/failing record must not stall the partition, so retries happen on separate topics. */
@Component
public class ShippingListener {

    private static final Logger log = LoggerFactory.getLogger(ShippingListener.class);

    private final ShippingService shippingService;

    public ShippingListener(ShippingService shippingService) {
        this.shippingService = shippingService;
    }

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 5_000, multiplier = 3.0, maxDelay = 120_000),
            dltTopicSuffix = ".shipping.DLT",
            retryTopicSuffix = ".shipping.retry",
            autoCreateTopics = "false",
            numPartitions = "12",
            replicationFactor = "3",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            exclude = {ValidationException.class},
            kafkaTemplate = "dltKafkaTemplate")
    @KafkaListener(
            id = "shipping-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "shippingFactory")
    public void shipping(OrderEvent event) {
        shippingService.handle(event);
    }

    @DltHandler
    public void shippingDlt(OrderEvent event,
                             @Header(KafkaHeaders.ORIGINAL_OFFSET) byte[] offset,
                             @Header(KafkaHeaders.EXCEPTION_MESSAGE) String reason) {
        log.error("Shipping DLT eventId={} reason={}", event.eventId(), reason);
    }
}
