# Kafka Consumers in Spring Boot: Production Guide (ASD-STE100 Edition)

> This document uses Simplified Technical English (ASD-STE100) writing rules. Sentences are short. Sentences use the active voice. Sentences give one instruction at a time. This document follows the content of `kafka-consumer-production-guide.md`.

> **Load:** The system must handle a load between 100 TPS and 500 TPS. 100 TPS is the off-peak load. 500 TPS is the peak load.
>
> **Software:** Use Java 17 or a later version. Use Spring Boot 3.x. Use Spring for Apache Kafka 3.x. Use Kafka 3.x brokers.

---

## 1. Target Architecture

The system has one topic. The name of the topic is `orders`. Four consumer groups read the topic. Each group receives all messages. Each group has its own scaling rules. Each group has its own retry policy. A failure in one group does not affect the other groups.

```
                         +---------------------+
   producers  ---->      |   topic: orders     |   36 partitions, RF=3, min.insync.replicas=2
                         +----------+----------+
                                    |
      +-------------+---------------+---------------+-----------------+
      |             |                               |                 |
 billing-service  shipping-service         notification-service  analytics-service
 (critical,       (important,              (best effort,         (bulk, batch
  strict, DB)      DB + HTTP)               high throughput)      listener)
      |             |                               |                 |
   orders.DLT   orders.DLT                    orders.DLT         orders.DLT
```

| Group | Criticality | Delivery Goal | Listener Type | Retry Policy |
|---|---|---|---|---|
| `billing-service` | Critical | No loss. No duplicate actions. | Record. Manual offset control. | Blocking retries. Then send to the DLT. |
| `shipping-service` | High | No loss. | Record. | Non-blocking retry topics. Then send to the DLT. |
| `notification-service` | Medium | Best effort. Low latency. | Record. | Few retries. Then send to the DLT. |
| `analytics-service` | Low | High throughput. Replay is possible. | Batch. | Retry the batch. Then send to the DLT. |

---

## 2. Capacity Sizing

### The Formula

```
threads needed = TPS x average processing time (seconds)
partitions     >= threads needed x headroom (1.5x to 2x)
```

One thread reads one partition. The number of partitions limits the number of threads. Each thread processes about `1 / processing_time` messages per second.

### Worked Example (50 ms per message)

| Load | Threads Needed | Threads With Headroom | Partitions to Create |
|---|---|---|---|
| 100 TPS | 5 | ~8 | 12 |
| 300 TPS | 15 | ~24 | 24 |
| 500 TPS | 25 | ~36 | **36** |

**Recommendation:** Create the topic with **36 partitions** now. You can add partitions later. You cannot remove partitions later. An increase in partitions changes the key-to-partition map. This change can break the order of messages with the same key during the change. Extra partitions have a low cost at this scale.

### Pod Layout

- Set 12 threads per pod. The topic has 36 partitions in total.
- **Off-peak load (100 TPS):** Run 2 pods. This is the minimum number for high availability. The pods give 24 threads. This is more than enough.
- **Peak load (500 TPS):** Run 3 pods. The pods give 36 threads. Each thread reads one partition.
- Do not run more threads than partitions. Extra threads stay idle.
- Scale the pods based on consumer lag. Do not scale the pods based on CPU use. CPU use stays low when a consumer waits for input or output.

### Procedure If Processing Is Slower Than Expected

If one message takes 200 ms, 500 TPS needs 100 threads. This number is more than 36 partitions. Do these steps in this order:
1. Make the processing step faster. Use bulk database writes. Use a cache. Use asynchronous HTTP calls.
2. Use a batch listener. Refer to Section 9, the analytics group.
3. Add partitions. Plan the key map again before you do this step.
4. Send work to a worker pool inside the listener. Do this step only if you accept a weaker order of messages. You must manage the offsets with care. Do not use this step for critical groups.

---

## 3. Topic Creation

```bash
kafka-topics.sh --bootstrap-server broker1:9092 --create \
  --topic orders \
  --partitions 36 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000          # 7 days: your replay window

kafka-topics.sh --bootstrap-server broker1:9092 --create \
  --topic orders.DLT \
  --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000         # 30 days: time to investigate
```

The retention period on the main topic is the disaster recovery window. A consumer that stops for one day can only catch up if the data is still in the topic.

---

## 4. Project Dependencies

```xml
<dependencies>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
  </dependency>
  <dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
  </dependency>
</dependencies>
```

---

## 5. Configuration

### 5.1 `application.yml`

```yaml
server:
  shutdown: graceful                       # finish in-flight HTTP work on SIGTERM

spring:
  lifecycle:
    timeout-per-shutdown-phase: 45s        # must exceed container shutdown timeout below
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:broker1:9092,broker2:9092,broker3:9092}
    properties:
      security.protocol: ${KAFKA_SECURITY_PROTOCOL:SASL_SSL}
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";
    ssl:
      trust-store-location: file:${KAFKA_TRUSTSTORE}
      trust-store-password: ${KAFKA_TRUSTSTORE_PASSWORD}
    listener:
      observation-enabled: true            # tracing/metrics per record (Boot 3.2+)

app:
  kafka:
    topic: orders
    dlt-topic: orders.DLT
    pod-id: ${HOSTNAME:local}              # used for static membership
    groups:
      billing:
        group-id: billing-service
        concurrency: 12
        max-poll-records: 50
        max-poll-interval-ms: 300000
      shipping:
        group-id: shipping-service
        concurrency: 12
        max-poll-records: 100
        max-poll-interval-ms: 300000
      notification:
        group-id: notification-service
        concurrency: 12
        max-poll-records: 200
        max-poll-interval-ms: 120000
      analytics:
        group-id: analytics-service
        concurrency: 6
        max-poll-records: 1000
        max-poll-interval-ms: 600000

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,info
  endpoint:
    health:
      probes:
        enabled: true
  metrics:
    tags:
      application: order-consumers
```

### 5.2 Reason for Each Consumer Property

| Property | Value | Reason |
|---|---|---|
| `enable.auto.commit` | `false` | Spring commits the offset only after the listener completes the work. Auto-commit can commit the offset before the work is complete. This action can cause a loss of messages after a crash. |
| `auto.offset.reset` | `earliest` for new groups | A new group reads all past messages. The group does not skip the past messages. For a group that monitors live events only, use `latest`. |
| `isolation.level` | `read_committed` | The consumer skips records from an aborted producer transaction. This setting has no effect if the producer does not use transactions. |
| `max.poll.records` | 50 to 1000 per group | This value limits the time between calls to `poll()`. Rule: `max.poll.records x worst-case time per record` must stay below `max.poll.interval.ms`. |
| `max.poll.interval.ms` | per group | If the consumer exceeds this time, the group removes the consumer. The group starts a rebalance. The group repeats the uncommitted work. |
| `session.timeout.ms` | 30000 | This value is fast enough to find a failed pod. This value is slow enough to survive a pause for garbage collection. |
| `heartbeat.interval.ms` | 10000 | This value must not exceed one third of the session timeout. |
| `partition.assignment.strategy` | `CooperativeStickyAssignor` | A rebalance moves only the partitions that must move. The other consumers continue their work. This behavior is important during autoscaling. |
| `group.instance.id` | pod name | This setting enables static membership. A pod that restarts within the session timeout gets its own partitions again. A rebalance does not occur. This setting needs stable pod names from a StatefulSet. |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 KB / 200 ms | At 100 to 500 TPS, a large value for `fetch.min.bytes` adds latency with little gain. Keep this value small. Raise this value only for the analytics group. |
| `client.id` | unique per group and pod | This setting lets you find a specific group and pod in broker logs and metrics. |

---

## 6. Core Java Configuration

### 6.1 Event Model

```java
public record OrderEvent(
        String eventId,       // globally unique, used for idempotency
        String orderId,       // Kafka message key, so one order's events stay ordered
        String type,
        java.math.BigDecimal amount,
        java.time.Instant createdAt) {}
```

**Key choice:** Produce each message with `orderId` as the key. All events for one order go to the same partition. The consumer processes these events in order. A random key or a null key gives a better balance across partitions, but it gives no order.

### 6.2 Shared Consumer Factory Builder

```java
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final MeterRegistry meterRegistry;

    @Value("${app.kafka.pod-id}")
    private String podId;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, MeterRegistry meterRegistry) {
        this.kafkaProperties = kafkaProperties;
        this.meterRegistry = meterRegistry;
    }

    /** Builds a ConsumerFactory with settings shared by every group. */
    private ConsumerFactory<String, OrderEvent> consumerFactory(
            String groupId, int maxPollRecords, int maxPollIntervalMs) {

        // Start from Spring Boot's properties so SSL/SASL settings are inherited.
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + podId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupId + "-" + podId); // static membership

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
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.orders");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        DefaultKafkaConsumerFactory<String, OrderEvent> factory = new DefaultKafkaConsumerFactory<>(props);
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry)); // exposes lag and fetch metrics
        return factory;
    }

    /** Builds a listener container factory with common container settings. */
    private ConcurrentKafkaListenerContainerFactory<String, OrderEvent> containerFactory(
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
        cp.setShutdownTimeout(30_000L);          // let in-flight records finish on SIGTERM
        cp.setPollTimeout(1_000L);
        cp.setObservationEnabled(true);
        cp.setIdleEventInterval(60_000L);        // emits an idle event if a group sees no data
        return factory;
    }

    // ----- one factory per consumer group -----

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> billingFactory(
            @Value("${app.kafka.groups.billing.group-id}") String groupId,
            @Value("${app.kafka.groups.billing.concurrency}") int concurrency,
            @Value("${app.kafka.groups.billing.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.billing.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("blockingRetryErrorHandler") DefaultErrorHandler errorHandler) {
        return containerFactory(consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.RECORD, errorHandler, false);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> shippingFactory(
            @Value("${app.kafka.groups.shipping.group-id}") String groupId,
            @Value("${app.kafka.groups.shipping.concurrency}") int concurrency,
            @Value("${app.kafka.groups.shipping.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.shipping.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("nonBlockingErrorHandler") DefaultErrorHandler errorHandler) {
        // @RetryableTopic does the retrying; this handler only needs to hand failures to it.
        return containerFactory(consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.RECORD, errorHandler, false);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> notificationFactory(
            @Value("${app.kafka.groups.notification.group-id}") String groupId,
            @Value("${app.kafka.groups.notification.concurrency}") int concurrency,
            @Value("${app.kafka.groups.notification.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.notification.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("bestEffortErrorHandler") DefaultErrorHandler errorHandler) {
        return containerFactory(consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.RECORD, errorHandler, false);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> analyticsFactory(
            @Value("${app.kafka.groups.analytics.group-id}") String groupId,
            @Value("${app.kafka.groups.analytics.concurrency}") int concurrency,
            @Value("${app.kafka.groups.analytics.max-poll-records}") int maxPollRecords,
            @Value("${app.kafka.groups.analytics.max-poll-interval-ms}") int maxPollInterval,
            @Qualifier("batchErrorHandler") DefaultErrorHandler errorHandler) {
        return containerFactory(consumerFactory(groupId, maxPollRecords, maxPollInterval),
                concurrency, ContainerProperties.AckMode.BATCH, errorHandler, true);
    }
}
```

> **Note on `group.instance.id`:** If `concurrency` is more than 1, Spring Kafka adds a suffix for each thread. Each consumer thread gets its own instance ID. If your pods have random names from a plain Deployment, static membership gives no benefit. A restarted pod gets a new ID in this case. Use a StatefulSet, or remove this property.

---

## 7. Dead-Letter Publishing and Error Handlers

### 7.1 DLT Producer

A dead-letter producer must publish two types of data. It must publish a normal `OrderEvent`. It must also publish the raw `byte[]` of a payload that failed during deserialization. `DelegatingByTypeSerializer` handles both types.

```java
@Configuration
public class DeadLetterConfig {

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        props.put(ProducerConfig.ACKS_CONFIG, "all");                       // do not lose DLT records
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());             // failed deserialization
        delegates.put(OrderEvent.class, new JsonSerializer<OrderEvent>());

        var pf = new DefaultKafkaProducerFactory<String, Object>(
                props, new StringSerializer(), new DelegatingByTypeSerializer(delegates));
        return new KafkaTemplate<>(pf);
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterRecoverer(
            KafkaTemplate<String, Object> dltKafkaTemplate,
            @Value("${app.kafka.dlt-topic}") String dltTopic) {
        // Partition -1 lets the producer choose, so the DLT may have fewer partitions than the source.
        // Without this, the recoverer targets the SAME partition number and fails if the DLT is smaller.
        return new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, ex) -> new TopicPartition(dltTopic, -1));
    }
}
```

The recoverer adds headers to the record. The headers show the original topic, the original partition, the original offset, the exception class, the exception message, and the stack trace. You need this data for diagnosis and replay.

### 7.2 Error Handlers: One Policy for Each Group

```java
@Configuration
public class ErrorHandlerConfig {

    /** Critical (billing): retry transient failures with exponential backoff, then dead-letter. */
    @Bean
    public DefaultErrorHandler blockingRetryErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        // Waits: 1s, 2s, 4s, 8s (about 15s total), capped at 10s per attempt.
        var backoff = new ExponentialBackOffWithMaxRetries(4);
        backoff.setInitialInterval(1_000);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(10_000);

        var handler = new DefaultErrorHandler(recoverer, backoff);
        // Retrying cannot fix these. Send straight to the DLT.
        handler.addNotRetryableExceptions(
                ValidationException.class,
                IllegalArgumentException.class,
                JsonProcessingException.class);
        handler.setCommitRecovered(true);       // commit offset once the record is safely in the DLT
        handler.setResetStateOnRecoveryFailure(false);
        return handler;
    }

    /** Shipping: failures are handed to @RetryableTopic. Keep the in-memory retry minimal. */
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
```

### 7.3 Blocking Retries Compared With Non-Blocking Retries

| | Blocking Retry (`DefaultErrorHandler` Backoff) | Non-Blocking Retry (`@RetryableTopic`) |
|---|---|---|
| Partition during the retry | The partition **stops** until success or until the DLT receives the record | The partition **continues to flow**. The failed record moves to a retry topic. |
| Order of messages with the same key | The order stays correct | The order **does not stay correct** for retried records |
| Best use | Critical groups. Order and correctness are more important than throughput. | Groups where one slow record must not delay many other records. |
| Retry delay budget | Keep the total time below `max.poll.interval.ms` | The total time can be minutes or hours |

At 500 TPS across 36 partitions, each partition sees about 14 TPS. A 15-second blocking retry delays about 200 records on that partition. This delay is acceptable for billing. A downstream outage of several minutes would make the backlog grow fast. For a long outage, use the circuit-breaker method in Section 11.

---

## 8. Idempotency: the Most Important Production Safeguard

Kafka delivery is **at-least-once**. Duplicate messages will occur during a rebalance. Duplicate messages will occur if a crash happens after processing but before the commit. Duplicate messages will occur during a retry. Every handler with a side effect must tolerate the same event twice.

### 8.1 Table

```sql
CREATE TABLE processed_event (
    consumer_group  VARCHAR(100) NOT NULL,
    event_id        VARCHAR(100) NOT NULL,
    processed_at    TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);
```

### 8.2 Entity and Repository

```java
@Entity
@Table(name = "processed_event")
@IdClass(ProcessedEventId.class)
public class ProcessedEvent {
    @Id private String consumerGroup;
    @Id private String eventId;
    private Instant processedAt = Instant.now();

    protected ProcessedEvent() {}
    public ProcessedEvent(String consumerGroup, String eventId) {
        this.consumerGroup = consumerGroup;
        this.eventId = eventId;
    }
}

public record ProcessedEventId(String consumerGroup, String eventId) implements Serializable {}

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, ProcessedEventId> {}
```

### 8.3 Service: the Business Write and the Dedup Marker in ONE Transaction

```java
@Service
public class BillingService {

    private static final String GROUP = "billing-service";
    private final ProcessedEventRepository processed;
    private final InvoiceRepository invoices;

    public BillingService(ProcessedEventRepository processed, InvoiceRepository invoices) {
        this.processed = processed;
        this.invoices = invoices;
    }

    @Transactional
    public void handle(OrderEvent event) {
        if (processed.existsById(new ProcessedEventId(GROUP, event.eventId()))) {
            return;                                    // duplicate: already handled, skip safely
        }
        invoices.save(Invoice.from(event));            // business side effect
        processed.save(new ProcessedEvent(GROUP, event.eventId()));
        // Both rows commit or roll back together.
    }
}
```

If two threads try to process the same event at the same time, the primary key makes the second commit fail. The exception starts a retry. During the retry, `existsById` returns true, so the handler skips the record. The handler does not apply the event twice.

**For external side effects** such as an HTTP call, an email, or a payment: send an **idempotency key** (the `eventId`) to the downstream system. The downstream system must remove duplicates on its own side. A database transaction cannot make an HTTP call atomic.

Add a scheduled job to delete rows in `processed_event` that are older than your topic retention period. In this guide, this period is 7 days.

---

## 9. Listeners: One for Each Group

```java
@Component
public class OrderListeners {

    private static final Logger log = LoggerFactory.getLogger(OrderListeners.class);

    private final BillingService billingService;
    private final ShippingService shippingService;
    private final NotificationService notificationService;
    private final AnalyticsService analyticsService;

    // constructor omitted for brevity

    // ---------- Billing: critical, blocking retries, idempotent ----------
    @KafkaListener(
            id = "billing-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "billingFactory")
    public void billing(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        MDC.put("eventId", event.eventId());
        try {
            billingService.handle(event);
        } finally {
            MDC.clear();
        }
    }

    // ---------- Shipping: non-blocking retry topics ----------
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 5_000, multiplier = 3.0, maxDelay = 120_000),
            dltTopicSuffix = ".shipping.DLT",
            retryTopicSuffix = ".shipping.retry",
            autoCreateTopics = "false",                    // create topics via IaC in production
            numPartitions = "12",
            replicationFactor = "3",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            exclude = { ValidationException.class },       // skip retries, go straight to DLT
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
        // alert, persist for manual review, or push to a ticketing system
    }

    // ---------- Notification: best effort, never block ----------
    @KafkaListener(
            id = "notification-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "notificationFactory")
    public void notification(OrderEvent event) {
        notificationService.send(event);       // use an HTTP timeout well under a second
    }

    // ---------- Analytics: batch for throughput ----------
    @KafkaListener(
            id = "analytics-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "analyticsFactory")
    public void analytics(List<ConsumerRecord<String, OrderEvent>> records) {
        List<OrderEvent> events = records.stream().map(ConsumerRecord::value).toList();
        analyticsService.bulkInsert(events);   // one bulk write per poll, idempotent via upsert
    }
}
```

> **Use of `@RetryableTopic`:** The listener group must have permission to read the retry topics. If `autoCreateTopics` is false, the topics must exist before use. A retried record can arrive out of order compared to a newer event with the same key. The shipping handler must tolerate this condition. For example, compare an event version or a timestamp before you apply the event.

---

## 10. Production Issue Checklist: Problem, Symptom, and Fix

| # | Problem | Symptom | Fix in This Guide |
|---|---|---|---|
| 1 | **Poison pill** (a malformed message) | The consumer repeats the same offset forever. Lag grows on one partition. | `ErrorHandlingDeserializer`, non-retryable exceptions, and the DLT |
| 2 | **Duplicate processing** | Double invoices. Double emails. | The idempotency table and idempotency keys on external calls (Section 8) |
| 3 | **Loss of a message after a crash** | The system acknowledges records that it never processed. | `enable.auto.commit=false`, `AckMode.RECORD` or `AckMode.BATCH`, commit only after success |
| 4 | **Rebalance storms** | Throughput drops at intervals. `CommitFailedException` occurs. "Member was fenced" occurs. | `CooperativeStickyAssignor`, static membership, tuned `max.poll.records` and `max.poll.interval.ms` |
| 5 | **A slow consumer is evicted** | A rebalance occurs when a slow record appears. | Lower `max.poll.records`. Raise `max.poll.interval.ms`. Set a timeout on every downstream call. |
| 6 | **A downstream outage** (the database or an API is down) | Retries build up. Lag increases fast. The DLT fills up. | Retry topics with a long backoff, and a circuit breaker with a container pause (Section 11) |
| 7 | **Unbalanced partitions** (a hot key) | One consumer is busy. The other consumers are idle. | Choose a key with high cardinality. Monitor the lag of each partition. |
| 8 | **The DLT has fewer partitions than the source topic** | `DeadLetterPublishingRecoverer` fails. The system retries the record forever. | A resolver that returns partition `-1` (Section 7.1) |
| 9 | **A mismatch in the deserialization class** | `ClassNotFoundException` occurs. An untrusted package error occurs. | Set `USE_TYPE_INFO_HEADERS` to false. Set a fixed default type. State the trusted packages. |
| 10 | **An unclean shutdown** | Duplicate records or incomplete work occur after each deploy. | Graceful shutdown, `ContainerProperties.shutdownTimeout`, and a `terminationGracePeriodSeconds` value that is more than the shutdown timeout |
| 11 | **A schema change breaks the consumers** | Deserialization errors occur after a producer deploy. | Add only optional fields. Use `@JsonIgnoreProperties(ignoreUnknown = true)`. Or use a schema registry. |
| 12 | **The number of threads is more than the number of partitions** | Threads stay idle. No increase in speed occurs. | `concurrency <= partitions / pods`, or accept idle spare capacity for failover |
| 13 | **No visibility into the system** | Users find the problems first. | Lag metrics, DLT alerts, and structured logs with `eventId` (Section 12) |
| 14 | **A broker failure** | Consumers pause or show errors during the failover. | RF=3, `min.insync.replicas=2`, multiple bootstrap servers, and client reconnect settings |
| 15 | **Credentials in the source code** | Leaked secrets occur. | Use environment variables or a secrets manager. Do not use plain text in a YAML file. |
| 16 | **Lost data from a short retention period** | You cannot replay data after a long outage. | Set the topic retention to at least 7 days. Set the DLT retention to 30 days. |
| 17 | **Out-of-order retried events** | An older update overwrites a newer update. | Add a version check or a timestamp check in the handler |
| 18 | **Memory pressure** | An out-of-memory error occurs with large batches. | Keep `max.poll.records x average message size` well below the heap size. Tune `fetch.max.bytes`. |

---

## 11. Backpressure: How to Pause Consumers During a Downstream Outage

When a dependency is down, a retry of every record wastes CPU time and fills the DLT. Instead, pause the consumption. Wait for the dependency to recover. Then resume the consumption. A paused consumer stays in the group, because it still polls and sends heartbeats. A rebalance does not occur.

```java
@Service
public class ConsumerPauseController {

    private final KafkaListenerEndpointRegistry registry;

    public ConsumerPauseController(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void pause(String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c != null && !c.isContainerPaused()) {
            c.pause();
        }
    }

    public void resume(String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c != null && c.isContainerPaused()) {
            c.resume();
        }
    }
}
```

Connect this controller to a circuit breaker, for example Resilience4j:

```java
circuitBreaker.getEventPublisher()
    .onStateTransition(e -> {
        switch (e.getStateTransition().getToState()) {
            case OPEN      -> pauseController.pause("billing-listener");
            case HALF_OPEN -> pauseController.resume("billing-listener");   // probe with live traffic
            case CLOSED    -> pauseController.resume("billing-listener");
            default        -> { }
        }
    });
```

The pause takes effect at the next poll. A small number of in-flight records can still finish. Because the group continues to send heartbeats and poll, a pause of several minutes does not cause an eviction. The uncommitted lag grows during the pause. The lag drains after the resume. Your topic retention period must be longer than the longest outage that you want to survive.

---

## 12. Observability

### 12.1 Metrics to Monitor

| Metric | Source | Reason to Monitor |
|---|---|---|
| Consumer lag for each group and partition | `kafka_consumer_fetch_manager_records_lag_max` (Micrometer), or a Kafka exporter, or Burrow | This is the primary health signal |
| Rate of records consumed | `kafka_consumer_fetch_manager_records_consumed_total` | Compare this rate with the expected TPS |
| Rebalance rate | `kafka_consumer_coordinator_rebalance_total` | A spike shows instability |
| Time between polls | `kafka_consumer_last_poll_seconds_ago` | A value close to `max.poll.interval.ms` shows a risk |
| Listener duration | `spring_kafka_listener_seconds` (with observation enabled) | This shows the latency for each record, and success compared to exception |
| DLT message count | Growth of the end offset on the DLT topic | Review any increase |
| JVM and database pool | Actuator | Pool exhaustion often occurs before lag |

### 12.2 Alert Suggestions (Prometheus Style)

```yaml
groups:
  - name: kafka-consumers
    rules:
      - alert: ConsumerLagHigh
        expr: max by (group) (kafka_consumergroup_lag) > 5000
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Lag above 5000 for {{ $labels.group }}"

      - alert: ConsumerLagGrowing
        expr: deriv(kafka_consumergroup_lag[10m]) > 0
        for: 15m
        labels: { severity: critical }

      - alert: DeadLetterTopicReceivingMessages
        expr: increase(kafka_topic_partition_current_offset{topic="orders.DLT"}[10m]) > 0
        labels: { severity: critical }

      - alert: FrequentRebalances
        expr: increase(kafka_consumer_coordinator_rebalance_total[15m]) > 3
        labels: { severity: warning }
```

Set the thresholds from observed numbers. At 500 TPS, a lag of 5000 is about 10 seconds of backlog. For billing, you may want a tighter threshold than for analytics.

### 12.3 Logging

Log the `eventId`, the `orderId`, the `topic`, the `partition`, and the `offset` for every failure. Put the `eventId` in the MDC. Do not log a full payload if it contains personal data or payment data.

### 12.4 Kubernetes Autoscaling Signal

Scale the pods based on lag, not CPU. Use KEDA, for example:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: order-consumers
spec:
  scaleTargetRef:
    name: order-consumers
  minReplicaCount: 2        # HA floor (100 TPS)
  maxReplicaCount: 3        # 3 pods x 12 threads = 36 = partition count
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: broker1:9092
        consumerGroup: billing-service
        topic: orders
        lagThreshold: "500"
```

Do not set `maxReplicaCount` higher than `partitions / concurrency`. Additional pods would receive no partitions.

---

## 13. Kubernetes Deployment Essentials

```yaml
apiVersion: apps/v1
kind: StatefulSet               # stable pod names make static membership effective
metadata:
  name: order-consumers
spec:
  serviceName: order-consumers
  replicas: 2
  podManagementPolicy: Parallel
  template:
    spec:
      terminationGracePeriodSeconds: 60     # > spring shutdown phase (45s) > container shutdown (30s)
      containers:
        - name: app
          image: registry.example.com/order-consumers:1.0.0
          env:
            - name: JAVA_TOOL_OPTIONS
              value: "-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
            - name: KAFKA_USER
              valueFrom: { secretKeyRef: { name: kafka-creds, key: user } }
            - name: KAFKA_PASSWORD
              valueFrom: { secretKeyRef: { name: kafka-creds, key: password } }
          resources:
            requests: { cpu: "1", memory: 1Gi }
            limits:   { memory: 1Gi }
          readinessProbe:
            httpGet: { path: /actuator/health/readiness, port: 8080 }
          livenessProbe:
            httpGet: { path: /actuator/health/liveness, port: 8080 }
            failureThreshold: 6
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: order-consumers
spec:
  minAvailable: 1               # node drains cannot take all consumers down at once
  selector:
    matchLabels: { app: order-consumers }
```

A rolling update restarts the pods one at a time. With static membership and `CooperativeStickyAssignor`, a restart within the session timeout causes little or no rebalance.

---

## 14. How to Replay the Dead-Letter Topic

A DLT record is not complete work. It is an item on a to-do list. Define the replay process before you need it.

1. **Triage the records:** Group the DLT records by the `kafka_dlt-exception-fqcn` header. Bad data and a transient outage need different handling.
2. **Fix the cause.** The cause can be a code bug, a downstream outage, or bad data from the producer.
3. **Replay the records** through a controlled tool. Do not replay the records by hand. The handlers are idempotent, so a replay is safe.

```java
@Service
public class DltReplayService {

    private final KafkaTemplate<String, Object> template;

    public DltReplayService(KafkaTemplate<String, Object> template) {
        this.template = template;
    }

    /** Re-publishes one DLT record to its original topic. Call from an admin endpoint. */
    public void replay(ConsumerRecord<String, byte[]> dltRecord) {
        String originalTopic = new String(
                dltRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value(),
                StandardCharsets.UTF_8);
        template.send(originalTopic, dltRecord.key(), dltRecord.value());
    }
}
```

Protect the replay endpoint with authentication. Set a rate limit on the replay function, so a large DLT does not overload the live traffic.

---

## 15. Tests to Run Before Go-Live

| Test | Method | Pass Criteria |
|---|---|---|
| Functional test | `@EmbeddedKafka` or Testcontainers Kafka | Each group receives each message one time |
| Poison pill test | Publish invalid JSON | The record goes to the DLT. The partition keeps flowing. |
| Duplicate delivery test | Publish the same `eventId` two times | The system creates one invoice |
| Crash recovery test | Stop the pod during processing | The system reprocesses the message. No duplicate side effect occurs. |
| Rebalance test | Scale the pods from 2, to 3, to 2, during the load | The lag recovers. No errors occur. |
| Downstream outage test | Stop the database for 5 minutes under load | The consumers pause or back off. The lag drains after recovery. No loss occurs. |
| Load test | Run 100 TPS. Increase to 500 TPS. Hold for 30 minutes. Drop to 100 TPS. | The lag stays near zero. The p99 latency stays within the target. |
| Soak test | Run for 24 hours at a mixed load | No memory growth occurs. No rebalance storms occur. |

Minimal Testcontainers example:

```java
@SpringBootTest
@Testcontainers
class BillingConsumerIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired KafkaTemplate<String, OrderEvent> template;
    @Autowired InvoiceRepository invoices;

    @Test
    void duplicateEventCreatesOneInvoice() {
        OrderEvent e = new OrderEvent("evt-1", "order-1", "CREATED", BigDecimal.TEN, Instant.now());
        template.send("orders", e.orderId(), e);
        template.send("orders", e.orderId(), e);

        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertThat(invoices.countByOrderId("order-1")).isEqualTo(1));
    }
}
```

---

## 16. Go-Live Checklist

**Kafka**
- [ ] The `orders` topic has 36 partitions, RF=3, `min.insync.replicas=2`, and a retention period of 7 days
- [ ] The infrastructure-as-code tool creates the `orders.DLT` topic and the retry topics. These topics are not auto-created.
- [ ] The ACLs give each group READ access to `orders` and to its retry topics. The ACLs give the application WRITE access to the DLT topics.

**Application**
- [ ] `enable.auto.commit=false`. The ack mode is set for each group.
- [ ] `ErrorHandlingDeserializer` is set on the key and on the value
- [ ] Non-retryable exceptions are classified
- [ ] Every handler is idempotent. Every external call carries an idempotency key.
- [ ] A timeout is set on every downstream call. Each timeout is shorter than `max.poll.interval.ms / max.poll.records`.
- [ ] The graceful shutdown timeouts match the `terminationGracePeriodSeconds` value
- [ ] Secrets come from an environment variable or a vault

**Operations**
- [ ] Alerts for lag, rebalance, and the DLT are active and routed to on-call staff
- [ ] A dashboard exists for each group. The dashboard shows lag, rate, errors, and latency.
- [ ] A runbook exists for DLT triage and replay
- [ ] The autoscaling bounds match `partitions / concurrency`
- [ ] A load test ran at 1.5 times the expected peak load (750 TPS)
- [ ] A rollback plan exists. The plan states how to reset offsets if a bad deploy corrupted the data:

```bash
# Stop the group first, then reset (dry run first with --dry-run):
kafka-consumer-groups.sh --bootstrap-server broker1:9092 \
  --group billing-service --topic orders \
  --reset-offsets --to-datetime 2026-10-06T08:00:00.000 --execute
```

---

## 17. Summary of Key Decisions

1. **36 partitions** cover a load of 100 to 500 TPS, at about 50 ms per message, with headroom.
2. **One consumer factory for each group** gives each group its own concurrency, polling, retry, and ack behavior. This design isolates failures between groups.
3. **At-least-once delivery, plus idempotency,** is the realistic and reliable model. Exactly-once delivery across a database and Kafka has a high cost. Make duplicate messages harmless instead.
4. Use **blocking retries** for critical, ordered work. Use **retry topics** for work that must not stall.
5. **Every failure must end at a visible place:** the DLT, with headers for diagnosis, an alert, and a replay path.
6. **Scale based on lag. Set the replica limit at the partition count. Keep the threads per pod, times the number of pods, at or below the partition count.**
