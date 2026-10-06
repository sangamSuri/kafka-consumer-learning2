package com.example.demo.kafka.exception;

/** Thrown by handlers when a message's payload is structurally valid but semantically wrong. Not retryable. */
public class ValidationException extends RuntimeException {
    public ValidationException(String message) {
        super(message);
    }
}
