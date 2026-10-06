package com.example.demo.kafka.admin;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Replays a message from the DLT back onto its original topic. Because handlers are
 * idempotent (see {@code idempotency} package), replay is safe to repeat.
 *
 * Protect this endpoint with authentication and rate-limit replays in production (guide §14).
 */
@RestController
@RequestMapping("/admin/kafka/dlt")
public class DltReplayController {

    private final KafkaTemplate<String, Object> dltKafkaTemplate;

    public DltReplayController(KafkaTemplate<String, Object> dltKafkaTemplate) {
        this.dltKafkaTemplate = dltKafkaTemplate;
    }

    @PostMapping("/replay")
    public ResponseEntity<String> replay(
            @RequestParam String originalTopic,
            @RequestParam String key,
            @RequestParam String value) {
        dltKafkaTemplate.send(originalTopic, key, value);
        return ResponseEntity.ok("replayed to " + originalTopic);
    }
}
