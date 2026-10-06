package com.example.demo.kafka.config;

import com.example.demo.kafka.model.OrderEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

/** One listener container factory per consumer group, each with its own concurrency, polling, and error policy. */
@Configuration
@EnableKafka
public class KafkaGroupFactoryConfig {

    private final KafkaConsumerConfig consumerConfig;

    public KafkaGroupFactoryConfig(KafkaConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> billingFactory(
            @Value("${app.kafka.groups.billing.group-id}") String groupId,
            @Value("${app.kafka.groups.billing.concurrency}") int concurrency,
            @Value("${app.kafka.groups.billing.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.billing.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("blockingRetryErrorHandler") DefaultErrorHandler errorHandler) {
        return consumerConfig.containerFactory(
                consumerConfig.consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.RECORD, errorHandler, false);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> shippingFactory(
            @Value("${app.kafka.groups.shipping.group-id}") String groupId,
            @Value("${app.kafka.groups.shipping.concurrency}") int concurrency,
            @Value("${app.kafka.groups.shipping.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.shipping.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("nonBlockingErrorHandler") DefaultErrorHandler errorHandler) {
        // @RetryableTopic on the listener does the retrying; this handler only hands failures to it.
        return consumerConfig.containerFactory(
                consumerConfig.consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.RECORD, errorHandler, false);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> notificationFactory(
            @Value("${app.kafka.groups.notification.group-id}") String groupId,
            @Value("${app.kafka.groups.notification.concurrency}") int concurrency,
            @Value("${app.kafka.groups.notification.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.notification.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("bestEffortErrorHandler") DefaultErrorHandler errorHandler) {
        return consumerConfig.containerFactory(
                consumerConfig.consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.RECORD, errorHandler, false);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> analyticsFactory(
            @Value("${app.kafka.groups.analytics.group-id}") String groupId,
            @Value("${app.kafka.groups.analytics.concurrency}") int concurrency,
            @Value("${app.kafka.groups.analytics.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.analytics.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("batchErrorHandler") DefaultErrorHandler errorHandler) {
        return consumerConfig.containerFactory(
                consumerConfig.consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.BATCH, errorHandler, true);
    }
}
