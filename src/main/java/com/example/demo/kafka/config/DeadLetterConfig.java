package com.example.demo.kafka.config;

import com.example.demo.kafka.model.OrderEvent;
import com.example.demo.kafka.security.KafkaSecurityConfigurer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dead-letter publishing: a DLT producer that can serialize both a normal
 * {@link OrderEvent} and the raw bytes of a payload that failed deserialization,
 * plus the recoverer every error handler delegates to.
 */
@Configuration
public class DeadLetterConfig {

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(KafkaProperties kafkaProperties,
                                                            KafkaSecurityConfigurer securityConfigurer) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        securityConfigurer.apply(props);

        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(OrderEvent.class, new JacksonJsonSerializer<OrderEvent>());

        var pf = new DefaultKafkaProducerFactory<String, Object>(
                props, new StringSerializer(), new DelegatingByTypeSerializer(delegates));
        return new KafkaTemplate<>(pf);
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterRecoverer(
            KafkaTemplate<String, Object> dltKafkaTemplate,
            @Value("${app.kafka.dlt-topic}") String dltTopic) {
        // Partition -1 lets the producer choose, so the DLT may have fewer partitions than the source.
        return new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, ex) -> new TopicPartition(dltTopic, -1));
    }
}
