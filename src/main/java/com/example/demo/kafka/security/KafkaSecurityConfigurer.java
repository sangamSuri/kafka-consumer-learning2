package com.example.demo.kafka.security;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Map;

/**
 * Applies {@code app.kafka.security.*} (bound via {@link KafkaSecurityProperties}) onto a
 * client's property map. Both the consumer factory ({@code KafkaConsumerConfig}) and the DLT
 * producer ({@code DeadLetterConfig}) call {@link #apply} after building their base properties,
 * so TLS/SASL is a single place to configure instead of duplicated per client.
 *
 * <p>No-op when {@code app.kafka.security.enabled=false} (the default), so local/dev PLAINTEXT
 * setups are unaffected.
 */
@Component
@EnableConfigurationProperties(KafkaSecurityProperties.class)
public class KafkaSecurityConfigurer {

    private final KafkaSecurityProperties properties;

    public KafkaSecurityConfigurer(KafkaSecurityProperties properties) {
        this.properties = properties;
    }

    public void apply(Map<String, Object> props) {
        if (!properties.isEnabled()) {
            return;
        }

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, properties.getProtocol());

        if (isSasl(properties.getProtocol())) {
            applySasl(props);
        }
        if (isSsl(properties.getProtocol())) {
            applySsl(props);
        }
    }

    private void applySasl(Map<String, Object> props) {
        String mechanism = properties.getSaslMechanism();
        props.put(SaslConfigs.SASL_MECHANISM, mechanism);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig(mechanism));
    }

    private void applySsl(Map<String, Object> props) {
        KafkaSecurityProperties.Ssl ssl = properties.getSsl();
        putIfPresent(props, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ssl.getTrustStoreLocation());
        putIfPresent(props, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.getTrustStorePassword());
        putIfPresent(props, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ssl.getKeyStoreLocation());
        putIfPresent(props, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ssl.getKeyStorePassword());
        putIfPresent(props, SslConfigs.SSL_KEY_PASSWORD_CONFIG, ssl.getKeyPassword());
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                ssl.getEndpointIdentificationAlgorithm());
    }

    private String jaasConfig(String mechanism) {
        String username = requireNonBlank(properties.getUsername(), "app.kafka.security.username");
        String password = requireNonBlank(properties.getPassword(), "app.kafka.security.password");
        String loginModule = switch (mechanism.toUpperCase(Locale.ROOT)) {
            case "PLAIN" -> "org.apache.kafka.common.security.plain.PlainLoginModule";
            case "SCRAM-SHA-256", "SCRAM-SHA-512" -> "org.apache.kafka.common.security.scram.ScramLoginModule";
            default -> throw new IllegalStateException("Unsupported SASL mechanism: " + mechanism);
        };
        return loginModule + " required username=\"" + escape(username) + "\" password=\"" + escape(password) + "\";";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String requireNonBlank(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(property + " must be set when app.kafka.security.enabled=true and protocol is SASL_*");
        }
        return value;
    }

    private static boolean isSasl(String protocol) {
        return protocol != null && protocol.toUpperCase(Locale.ROOT).startsWith("SASL");
    }

    private static boolean isSsl(String protocol) {
        return protocol != null && protocol.toUpperCase(Locale.ROOT).endsWith("SSL");
    }

    private static void putIfPresent(Map<String, Object> props, String key, String value) {
        if (value != null && !value.isBlank()) {
            props.put(key, value);
        }
    }
}
