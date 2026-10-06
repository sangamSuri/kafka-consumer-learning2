# Kafka Consumers in Spring Boot: Production Guide (100 to 500 TPS, Multiple Consumer Groups)

> **Assumption:** "500 to 100 TPS" is read as a workload that varies between **100 TPS (off-peak) and 500 TPS (peak)**. If you meant 100K TPS, the architecture holds but partition counts, batching, and instance counts need to be re-sized (see section 2).
>
> **Stack:** Java 17+, Spring Boot 3.x, Spring for Apache Kafka 3.x, Kafka 3.x brokers.

---

## 1. Target architecture

One topic (`orders`), consumed independently by four consumer groups. Each group gets every message and has its own scaling, retry policy, and failure isolation.

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

| Group | Criticality | Delivery goal | Listener type | Retry policy |
|---|---|---|---|---|
| `billing-service` | Critical | No loss, no duplicate side effects | Record, manual control | Blocking retries, then DLT |
| `shipping-service` | High | No loss | Record | Non-blocking retry topics, then DLT |
| `notification-service` | Medium | Best effort, low latency | Record | Few retries, drop to DLT |
| `analytics-service` | Low | High throughput, replayable | Batch | Retry batch, then DLT |

---

## 2. Capacity sizing

### The formula

```
threads needed = TPS x average processing time (seconds)
partitions     >= threads needed x headroom (1.5x to 2x)
```

A partition is consumed by exactly one thread per group, so **partitions cap your parallelism**. Each consumer thread handles about `1 / processing_time` messages per second.

### Worked example (50 ms per message, e.g. one DB write)

| Load | Threads needed | With headroom | Partitions to provision |
|---|---|---|---|
| 100 TPS | 5 | ~8 | 12 |
| 300 TPS | 15 | ~24 | 24 |
| 500 TPS | 25 | ~36 | **36** |

**Recommendation:** create the topic with **36 partitions** now. You can add partitions later but never remove them, and adding partitions changes key-to-partition mapping (breaking per-key ordering during the transition). Over-provisioning slightly is cheap at this scale.

### Pod layout

- `concurrency = 12` threads per pod, 36 partitions total.
- **Off-peak (100 TPS):** 2 pods (minimum for HA). 24 threads, plenty of room.
- **Peak (500 TPS):** 3 pods. 36 threads, one partition per thread.
- Never run more threads than partitions. Extra threads sit idle.
- Autoscale on **consumer lag**, not CPU. CPU stays low when consumers block on I/O.

### If processing is slower than expected

If one message takes 200 ms, 500 TPS needs 100 threads, which exceeds 36 partitions. Options, in order of preference:
1. Make the work faster (bulk DB writes, cache lookups, async HTTP).
2. Use a batch listener (section 9, analytics group).
3. Add partitions (re-plan the key mapping first).
4. Hand off to a worker pool inside the listener (only if you accept weaker ordering and manage offsets carefully; not recommended for critical groups).

---

## 3. Topic creation

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

Retention on the main topic is your **disaster recovery window**. A consumer that was broken for a day can only catch up if the data is still there.

---

## 4. Project dependencies

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

### 5.2 Why each consumer property is set the way it is

| Property | Value | Production reason |
|---|---|---|
| `enable.auto.commit` | `false` | Offsets are committed by Spring only after the listener succeeds. Auto-commit can commit before processing finishes and lose messages on crash. |
| `auto.offset.reset` | `earliest` for new groups | A new group replays history instead of silently skipping it. For live-only alerting groups use `latest`. |
| `isolation.level` | `read_committed` | Skips records from aborted producer transactions. Harmless if producers do not use transactions. |
| `max.poll.records` | 50 to 1000 per group | Bounds the time between `poll()` calls. Rule: `max.poll.records x worst-case time per record < max.poll.interval.ms`. |
| `max.poll.interval.ms` | per group | If exceeded, the consumer is evicted, a rebalance starts, and uncommitted work is redone. |
| `session.timeout.ms` | 30000 | Fast enough to detect dead pods, slow enough to survive a GC pause. |
| `heartbeat.interval.ms` | 10000 | At most one third of the session timeout. |
| `partition.assignment.strategy` | `CooperativeStickyAssignor` | Rebalances move only the partitions that must move, so the other consumers keep working. Critical during autoscaling. |
| `group.instance.id` | pod name | Static membership. A pod that restarts within `session.timeout.ms` reclaims its partitions with no rebalance. Needs stable pod names (StatefulSet) to pay off. |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 KB / 200 ms | At 100 to 500 TPS, a large `fetch.min.bytes` adds latency for little gain. Keep it small. Raise it only for the analytics group. |
| `client.id` | unique per group + pod | Makes broker logs and metrics attributable. |

---

## 6. Core Java configuration

### 6.1 Event model

```java
public record OrderEvent(
        String eventId,       // globally unique, used for idempotency
        String orderId,       // Kafka message key, so one order's events stay ordered
        String type,
        java.math.BigDecimal amount,
        java.time.Instant createdAt) {}
```

**Key choice:** produce with `orderId` as the key. All events for one order land on the same partition and are processed in order. A random or null key gives better balance but no ordering.

### 6.2 Shared consumer factory builder

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

> **Note on `group.instance.id`:** with `concurrency > 1`, Spring Kafka appends a per-thread suffix so each consumer thread has a unique instance ID. If your pods have random names (a plain Deployment), static membership gives no benefit because a restarted pod gets a new ID. Use a StatefulSet or drop this property.

---

## 7. Dead-letter publishing and error handlers

### 7.1 DLT producer

A dead-letter producer must be able to publish **both** a normal `OrderEvent` and the raw `byte[]` of a payload that failed deserialization. `DelegatingByTypeSerializer` handles both.

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

The recoverer automatically adds headers with the original topic, partition, offset, exception class, message, and stack trace, which you need for diagnosis and replay.

### 7.2 Error handlers (one policy per group)

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

### 7.3 Blocking vs non-blocking retries

| | Blocking (`DefaultErrorHandler` backoff) | Non-blocking (`@RetryableTopic`) |
|---|---|---|
| Partition during retry | **Stalled** until success or DLT | **Keeps flowing**; failed record moves to a retry topic |
| Ordering per key | Preserved | **Not preserved** for retried records |
| Best for | Critical groups where order and correctness beat throughput | Groups where one slow record must not delay thousands of others |
| Retry delay budget | Keep total under `max.poll.interval.ms` | Can be minutes or hours |

At 500 TPS across 36 partitions, each partition sees about 14 TPS. A 15-second blocking retry delays about 200 records on that partition. That is acceptable for billing, but a multi-minute outage downstream would make the backlog grow quickly. For long outages, use the circuit-breaker pattern in section 11.

---

## 8. Idempotency (the most important production safeguard)

Kafka consumption is **at-least-once**. Duplicates will happen on rebalance, crash-after-process-before-commit, and retries. Every handler with a side effect must tolerate seeing the same event twice.

### 8.1 Table

```sql
CREATE TABLE processed_event (
    consumer_group  VARCHAR(100) NOT NULL,
    event_id        VARCHAR(100) NOT NULL,
    processed_at    TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);
```

### 8.2 Entity and repository

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

### 8.3 Service: business write and dedup marker in ONE transaction

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

If two threads race on the same event, the primary key makes the second commit fail. The exception triggers a retry, and on retry `existsById` returns true so the record is skipped. Nothing is double-applied.

**For external side effects** (HTTP calls, emails, payments): pass an **idempotency key** (the `eventId`) to the downstream API so the remote side deduplicates too. A database transaction cannot make an HTTP call atomic.

Add a scheduled job to purge `processed_event` rows older than your topic retention (7 days here).

---

## 9. Listeners (one per group)

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

> **Using `@RetryableTopic`:** for the retry topics to be consumed, the listener's group must be allowed to read them (ACLs), and the topics must exist if `autoCreateTopics` is false. Retried records can arrive out of order relative to newer events for the same key, so the shipping handler must tolerate that (for example, compare an event version or timestamp before applying).

---

## 10. Production issue checklist: problem, symptom, and fix

| # | Problem | Symptom | Fix in this guide |
|---|---|---|---|
| 1 | **Poison pill** (malformed message) | Consumer loops on the same offset forever, lag grows on one partition | `ErrorHandlingDeserializer` + non-retryable exceptions + DLT |
| 2 | **Duplicate processing** | Double invoices, double emails | Idempotency table, idempotency keys on external calls (section 8) |
| 3 | **Message loss on crash** | Records acknowledged but never processed | `enable.auto.commit=false`, `AckMode.RECORD`/`BATCH`, commit only after success |
| 4 | **Rebalance storms** | Throughput drops periodically, `CommitFailedException`, "member was fenced" | `CooperativeStickyAssignor`, static membership, tuned `max.poll.records` and `max.poll.interval.ms` |
| 5 | **Slow consumer evicted** | Rebalance whenever a slow record appears | Lower `max.poll.records`, raise `max.poll.interval.ms`, enforce timeouts on every downstream call |
| 6 | **Downstream outage** (DB or API down) | Retries pile up, lag explodes, DLT floods | Retry topics with long backoff, circuit breaker with container pause (section 11) |
| 7 | **Unbalanced partitions** (hot key) | One consumer busy, others idle | Choose a high-cardinality key, monitor per-partition lag |
| 8 | **DLT smaller than source topic** | `DeadLetterPublishingRecoverer` fails, record retried forever | Resolver returning partition `-1` (section 7.1) |
| 9 | **Deserialization class mismatch** | `ClassNotFoundException`, untrusted package errors | `USE_TYPE_INFO_HEADERS=false`, fixed default type, explicit trusted packages |
| 10 | **Unclean shutdown** | Duplicates or partial work after every deploy | Graceful shutdown, `ContainerProperties.shutdownTimeout`, `terminationGracePeriodSeconds` > shutdown timeout |
| 11 | **Schema evolution breaks consumers** | Deserialization errors after a producer deploy | Only add optional fields, use `@JsonIgnoreProperties(ignoreUnknown = true)`, or adopt a schema registry |
| 12 | **Threads exceed partitions** | Idle threads, no speedup | `concurrency <= partitions / pods` (or accept idle spare capacity for failover) |
| 13 | **No visibility** | Problems found by users | Lag metrics, DLT alerts, structured logs with `eventId` (section 12) |
| 14 | **Broker failure** | Consumers pause or error during failover | RF=3, `min.insync.replicas=2`, multiple bootstrap servers, client reconnect settings |
| 15 | **Credentials in source control** | Leaked secrets | Environment variables or a secrets manager, never plain text in YAML |
| 16 | **Lost data due to short retention** | Cannot replay after a long outage | Topic retention of at least 7 days, DLT retention of 30 days |
| 17 | **Out-of-order retried events** | Older update overwrites newer one | Version or timestamp check in handler (last-write-wins guard) |
| 18 | **Memory pressure** | OOM with large batches | Keep `max.poll.records x avg message size` well below heap; tune `fetch.max.bytes` |

---

## 11. Backpressure: pausing consumers during a downstream outage

When a dependency is down, retrying every record just burns CPU and floods the DLT. Instead, pause consumption, wait for recovery, then resume. Pausing keeps the consumer in the group (it still polls and heartbeats), so no rebalance occurs.

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

Wire it to a circuit breaker (for example Resilience4j):

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

Pause is applied at the next poll, so a few in-flight records may still finish. Because the group keeps heartbeating and polling, a pause of several minutes does not trigger an eviction. The uncommitted lag simply accumulates and drains after resume. Your topic retention must be longer than the longest outage you want to survive.

---

## 12. Observability

### 12.1 Metrics to watch

| Metric | Source | Why |
|---|---|---|
| Consumer lag per group and partition | `kafka_consumer_fetch_manager_records_lag_max` (Micrometer) or Kafka exporter / Burrow | The primary health signal |
| Records consumed rate | `kafka_consumer_fetch_manager_records_consumed_total` | Compare with expected TPS |
| Rebalance rate | `kafka_consumer_coordinator_rebalance_total` | Spikes indicate instability |
| Time between polls | `kafka_consumer_last_poll_seconds_ago` | Approaching `max.poll.interval.ms` means trouble |
| Listener duration | `spring_kafka_listener_seconds` (with observation enabled) | Latency per record, success vs exception |
| DLT message count | DLT topic end offset growth | Any increase deserves a look |
| JVM and DB pool | Actuator | Pool exhaustion often precedes lag |

### 12.2 Alert suggestions (Prometheus-style)

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

Tune thresholds from observed numbers. At 500 TPS, 5000 lag is about 10 seconds of backlog. For billing you may want a tighter threshold than for analytics.

### 12.3 Logging

Log `eventId`, `orderId`, `topic`, `partition`, and `offset` on every failure, and put `eventId` in the MDC. Never log full payloads if they contain personal or payment data.

### 12.4 Kubernetes autoscaling signal

Scale on lag, not CPU. With KEDA:

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

Never set `maxReplicaCount` higher than `partitions / concurrency`. Additional pods would receive no partitions.

---

## 13. Kubernetes deployment essentials

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

Rolling updates restart pods one at a time. With static membership and `CooperativeStickyAssignor`, a restart under the session timeout causes little or no rebalance.

---

## 14. Replaying the dead-letter topic

DLT records are not "done". They are a to-do list. Define the process before you need it.

1. **Triage:** group DLT records by `kafka_dlt-exception-fqcn` header. Bad data and transient outages need different handling.
2. **Fix the cause** (code bug, downstream outage, bad producer).
3. **Replay** through a controlled tool, not by hand. Because handlers are idempotent, replay is safe.

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

Protect any replay endpoint with authentication, and rate-limit replays so a large DLT does not swamp live traffic.

---

## 15. Testing before go-live

| Test | How | Pass criteria |
|---|---|---|
| Functional | `@EmbeddedKafka` or Testcontainers Kafka | Each group receives each message once |
| Poison pill | Publish invalid JSON | Lands in DLT, partition keeps flowing |
| Duplicate delivery | Publish the same `eventId` twice | One invoice created |
| Crash recovery | Kill the pod mid-processing | Message reprocessed, no duplicate side effect |
| Rebalance | Scale pods 2 to 3 to 2 during load | Lag recovers, no errors |
| Downstream outage | Stop the DB for 5 minutes under load | Consumers pause or back off, drain after recovery, no loss |
| Load | 100 TPS, ramp to 500, hold 30 min, drop to 100 | Lag stays near zero, p99 latency within target |
| Soak | 24 hours at mixed load | No memory growth, no rebalance storms |

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

## 16. Go-live checklist

**Kafka**
- [ ] `orders`: 36 partitions, RF=3, `min.insync.replicas=2`, retention 7 days
- [ ] `orders.DLT` and retry topics created via IaC (not auto-created)
- [ ] ACLs grant each group READ on `orders` and its retry topics, and the app WRITE on DLT topics

**Application**
- [ ] `enable.auto.commit=false`, ack mode set per group
- [ ] `ErrorHandlingDeserializer` on key and value
- [ ] Non-retryable exceptions classified
- [ ] Every handler idempotent, external calls carry idempotency keys
- [ ] Timeouts on every downstream call (shorter than `max.poll.interval.ms / max.poll.records`)
- [ ] Graceful shutdown timeouts consistent with `terminationGracePeriodSeconds`
- [ ] Secrets from environment or vault

**Operations**
- [ ] Lag, rebalance, and DLT alerts live and routed to on-call
- [ ] Dashboard per group (lag, rate, errors, latency)
- [ ] Runbook: DLT triage and replay
- [ ] Autoscaling bounds match partitions / concurrency
- [ ] Load test completed at 1.5x expected peak (750 TPS)
- [ ] Rollback plan, including how to rewind offsets if a bad deploy corrupted data:

```bash
# Stop the group first, then reset (dry run first with --dry-run):
kafka-consumer-groups.sh --bootstrap-server broker1:9092 \
  --group billing-service --topic orders \
  --reset-offsets --to-datetime 2026-10-06T08:00:00.000 --execute
```

---

## 17. Summary of key decisions

1. **36 partitions** cover 100 to 500 TPS at about 50 ms per message, with headroom.
2. **One consumer factory per group** lets each group have its own concurrency, polling, retry, and ack behavior, and isolates failures.
3. **At-least-once plus idempotency** is the realistic, reliable model. Exactly-once across a DB and Kafka is not free, so make duplicates harmless instead.
4. **Blocking retries for critical, ordered work; retry topics for everything that must not stall.**
5. **Every failure ends somewhere visible:** the DLT, with headers for diagnosis, an alert, and a replay path.
6. **Scale on lag, cap replicas at the partition count, and keep threads per pod times pods at or below partitions.**
