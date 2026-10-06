package com.example.demo.kafka.admin;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Pauses/resumes a listener container by id, e.g. to back off during a downstream outage.
 * Pausing keeps the consumer in the group (it still polls and heartbeats), so no rebalance occurs.
 * Wire this to a circuit breaker's state transitions in production (guide §11).
 */
@RestController
@RequestMapping("/admin/kafka/listeners")
public class ConsumerPauseController {

    private final KafkaListenerEndpointRegistry registry;

    public ConsumerPauseController(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @PostMapping("/{listenerId}/pause")
    public ResponseEntity<String> pause(@PathVariable String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c == null) {
            return ResponseEntity.notFound().build();
        }
        if (!c.isContainerPaused()) {
            c.pause();
        }
        return ResponseEntity.ok("paused: " + listenerId);
    }

    @PostMapping("/{listenerId}/resume")
    public ResponseEntity<String> resume(@PathVariable String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c == null) {
            return ResponseEntity.notFound().build();
        }
        if (c.isContainerPaused()) {
            c.resume();
        }
        return ResponseEntity.ok("resumed: " + listenerId);
    }
}
