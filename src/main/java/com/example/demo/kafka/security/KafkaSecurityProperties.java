package com.example.demo.kafka.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code app.kafka.security.*} (see {@code application-security.yml}).
 * Disabled by default so local/dev (PLAINTEXT) is unaffected; enable per-environment.
 */
@ConfigurationProperties(prefix = "app.kafka.security")
public class KafkaSecurityProperties {

    /** Master switch; when false, {@link KafkaSecurityConfigurer} leaves client props untouched. */
    private boolean enabled = false;

    /** e.g. SASL_SSL, SSL, SASL_PLAINTEXT. */
    private String protocol = "PLAINTEXT";

    /** e.g. SCRAM-SHA-512, PLAIN. Only used when protocol starts with SASL. */
    private String saslMechanism;

    private String username;
    private String password;

    private final Ssl ssl = new Ssl();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Ssl getSsl() {
        return ssl;
    }

    public static class Ssl {
        private String trustStoreLocation;
        private String trustStorePassword;
        private String keyStoreLocation;
        private String keyStorePassword;
        private String keyPassword;
        private String endpointIdentificationAlgorithm = "https";

        public String getTrustStoreLocation() {
            return trustStoreLocation;
        }

        public void setTrustStoreLocation(String trustStoreLocation) {
            this.trustStoreLocation = trustStoreLocation;
        }

        public String getTrustStorePassword() {
            return trustStorePassword;
        }

        public void setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
        }

        public String getKeyStoreLocation() {
            return keyStoreLocation;
        }

        public void setKeyStoreLocation(String keyStoreLocation) {
            this.keyStoreLocation = keyStoreLocation;
        }

        public String getKeyStorePassword() {
            return keyStorePassword;
        }

        public void setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
        }

        public String getKeyPassword() {
            return keyPassword;
        }

        public void setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
        }

        public String getEndpointIdentificationAlgorithm() {
            return endpointIdentificationAlgorithm;
        }

        public void setEndpointIdentificationAlgorithm(String endpointIdentificationAlgorithm) {
            this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
        }
    }
}
