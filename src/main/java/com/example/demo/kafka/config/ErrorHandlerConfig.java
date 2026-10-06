package com.example.demo.kafka.config;

import com.example.demo.kafka.exception.ValidationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

/** One error-handling policy per consumer group, per the production guide §7.2. */
@Configuration
public class ErrorHandlerConfig {

    /** Critical (billing): retry transient failures with exponential backoff, then dead-letter. */
    @Bean
    public DefaultErrorHandler blockingRetryErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        var backoff = new ExponentialBackOffWithMaxRetries(4);
        backoff.setInitialInterval(1_000);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(10_000);

        var handler = new DefaultErrorHandler(recoverer, backoff);
        handler.addNotRetryableExceptions(
                ValidationException.class,
                IllegalArgumentException.class,
                JsonProcessingException.class);
        handler.setCommitRecovered(true);
        handler.setResetStateOnRecoveryFailure(false);
        return handler;
    }

    /** Shipping: failures are handed to {@code @RetryableTopic}. Keep the in-memory retry minimal. */
    @Bean
    public DefaultErrorHandler nonBlockingErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(0L, 0L));
    }

    /** Notification: best effort. Two quick retries, then DLT. Never stall the partition for long. */
    @Bean
    public DefaultErrorHandler bestEffortErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2));
    }

    /** Analytics (batch): retry the batch, then fall back to record-by-record DLT. */
    @Bean
    public DefaultErrorHandler batchErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        return new DefaultErrorHandler(recoverer, new FixedBackOff(2_000L, 3));
    }
}
