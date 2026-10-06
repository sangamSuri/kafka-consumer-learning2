package com.example.demo.kafka.config;

import com.example.demo.kafka.model.OrderEvent;
import com.example.demo.kafka.security.KafkaSecurityConfigurer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the {@link ConsumerFactory} and listener container factory shared by every
 * consumer group. Each group's {@code @Bean} factory (see {@link KafkaGroupFactoryConfig})
 * calls into this builder with its own group id, concurrency, and error handler.
 */
@Component
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final MeterRegistry meterRegistry;
    private final KafkaSecurityConfigurer securityConfigurer;

    @Value("${app.kafka.pod-id}")
    private String podId;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, MeterRegistry meterRegistry,
                                KafkaSecurityConfigurer securityConfigurer) {
        this.kafkaProperties = kafkaProperties;
        this.meterRegistry = meterRegistry;
        this.securityConfigurer = securityConfigurer;
    }

    ConsumerFactory<String, OrderEvent> consumerFactory(
            String groupId, int maxPollRecords, int maxPollIntervalMs) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + podId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupId + "-" + podId);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10_000);

        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());

        // Poison-pill protection: a bad payload becomes a handled failure, not an infinite loop.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.demo.kafka.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        securityConfigurer.apply(props);

        DefaultKafkaConsumerFactory<String, OrderEvent> factory = new DefaultKafkaConsumerFactory<>(props);
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> containerFactory(
            ConsumerFactory<String, OrderEvent> cf,
            int concurrency,
            ContainerProperties.AckMode ackMode,
            CommonErrorHandler errorHandler,
            boolean batch) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(concurrency);
        factory.setBatchListener(batch);
        factory.setCommonErrorHandler(errorHandler);

        ContainerProperties cp = factory.getContainerProperties();
        cp.setAckMode(ackMode);
        cp.setShutdownTimeout(30_000L);
        cp.setPollTimeout(1_000L);
        cp.setObservationEnabled(true);
        cp.setIdleEventInterval(60_000L);
        return factory;
    }
}
