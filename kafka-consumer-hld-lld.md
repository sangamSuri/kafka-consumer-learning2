# Order Events Kafka Consumer — HLD & LLD

Reflects the code actually present in this repo (not aspirational/future state).

## 1. High-Level Design (HLD)

### 1.1 Purpose
Fan out `OrderEvent` messages published on topic `orders` to four independent downstream
consumer groups, each processing at its own pace and failure-handling policy.

### 1.2 Components

```
                        ┌──────────────────┐
                        │   Topic: orders   │  (partitions: provisioned
                        │   (key = orderId) │   out-of-band, no NewTopic
                        └─────────┬─────────┘   bean in code)
                                  │
        ┌─────────────┬──────────┼──────────┬──────────────┐
        ▼             ▼          ▼          ▼              │
  billing-service  shipping-  notification- analytics-      │
   (group, cc=3)   service    service        service        │
                   (cc=3,     (cc=3,         (cc=2, BATCH    │
                   RetryTopic) RECORD ack)    ack mode)       │
        │             │          │             │
        ▼             ▼          ▼             ▼
  BillingListener ShippingListener NotificationListener AnalyticsListener
        │             │
        │             ▼
        │       @RetryableTopic (4 attempts,
        │        orders.shipping.retry,
        │        orders.shipping.DLT)
        ▼
  DefaultErrorHandler (blocking retry) ──on exhaustion──▶ DeadLetterConfig
                                                            dltKafkaTemplate
                                                                 │
                                                                 ▼
                                                          Topic: orders.DLT
```

Each group is a **separate Spring `ConcurrentKafkaListenerContainerFactory`**
(`KafkaGroupFactoryConfig.java`), built from a shared factory method in
`KafkaConsumerConfig.java`. Independent group IDs mean each group gets its own
full copy of every partition's data (standard pub/sub fan-out via consumer groups).

### 1.3 Scaling model
- Horizontal scaling unit = **pods**, each running `concurrency` threads per group.
- Max useful parallelism per group = **partition count of `orders`** (fixed, provisioned
  externally — no `NewTopic` bean exists in code).
- `CooperativeStickyAssignor` + static group membership (`group.instance.id =
  <groupId>-<podId>`) keep rebalances incremental and let restarting pods reclaim
  their partitions, provided pod identity is stable (StatefulSet-style).

### 1.4 Reliability
- Manual offset commit (`enable.auto.commit=false`), `RECORD` ack mode for
  billing/shipping/notification, `BATCH` for analytics.
- `read_committed` isolation level (works correctly only if the producer is
  transactional/idempotent — not present in this repo).
- Poison-pill protection via `ErrorHandlingDeserializer`.
- Blocking retry (`DefaultErrorHandler`) + topic-level retry/DLT only on
  the shipping path (`@RetryableTopic`); other groups fall back to the shared
  `dltKafkaTemplate` on exhaustion.
- `ConsumerPauseController` allows pausing/resuming a listener without leaving
  the group (backpressure, not scaling).

### 1.5 Known gaps (relevant to the scaling question)
- No `NewTopic`/`KafkaAdmin` bean — partition count for `orders` is undeclared in
  code; a producer-side partition increase is invisible to this app until someone
  manually bumps `app.kafka.groups.*.concurrency`.
- No actual producer code for `orders` exists in this repo — only a DLT/replay
  producer (`DeadLetterConfig`, `DltReplayController`).
- No HPA/KEDA manifest in-repo (guide documents it conceptually only).

---

## 2. Low-Level Design (LLD)

### 2.1 Configuration (`application.yml`)
| Key | Value |
|---|---|
| `app.kafka.topic` | `orders` |
| `app.kafka.dlt-topic` | `orders.DLT` |
| `app.kafka.pod-id` | `${HOSTNAME:local}` |
| `groups.billing` | group-id=`billing-service`, concurrency=3, max-poll-records=50, max-poll-interval-ms=300000 |
| `groups.shipping` | group-id=`shipping-service`, concurrency=3, max-poll-records=100, max-poll-interval-ms=300000 |
| `groups.notification` | group-id=`notification-service`, concurrency=3, max-poll-records=200, max-poll-interval-ms=120000 |
| `groups.analytics` | group-id=`analytics-service`, concurrency=2, max-poll-records=1000, max-poll-interval-ms=600000 |

### 2.2 `KafkaConsumerConfig.java`
- `consumerFactory(groupId, maxPollRecords, maxPollInterval)`:
  - `ENABLE_AUTO_COMMIT_CONFIG = false`
  - `AUTO_OFFSET_RESET_CONFIG = earliest`
  - `ISOLATION_LEVEL_CONFIG = read_committed`
  - `PARTITION_ASSIGNMENT_STRATEGY_CONFIG = CooperativeStickyAssignor`
  - `SESSION_TIMEOUT_MS_CONFIG = 30000`, `HEARTBEAT_INTERVAL_MS_CONFIG = 10000`
  - `FETCH_MIN_BYTES_CONFIG = 1024`, `FETCH_MAX_WAIT_MS_CONFIG = 200`
  - `GROUP_INSTANCE_ID_CONFIG = groupId + "-" + podId` (static membership)
  - Key/value deserializers wrapped in `ErrorHandlingDeserializer`
    (delegate: `StringDeserializer` / `JsonDeserializer<OrderEvent>`)
  - `MicrometerConsumerListener` registered on the factory
- `containerFactory(consumerFactory, concurrency, ackMode, errorHandler, batch)`:
  - sets `setConcurrency(concurrency)`, `getContainerProperties().setAckMode(ackMode)`,
    `setBatchListener(batch)`, `setCommonErrorHandler(errorHandler)`

### 2.3 `KafkaGroupFactoryConfig.java`
Four `@Bean` factory methods (`billingFactory`, `shippingFactory`,
`notificationFactory`, `analyticsFactory`), each injecting its group's
`group-id`/`concurrency`/`max-poll-*` from config and the shared
`blockingRetryErrorHandler`. `analyticsFactory` sets `AckMode.BATCH` + batch
listener; the other three use `AckMode.RECORD`.

### 2.4 Listeners
| Listener | Group | Container factory | Notes |
|---|---|---|---|
| `BillingListener` | billing-service | billingFactory | single-record |
| `ShippingListener` | shipping-service | shippingFactory | `@RetryableTopic(attempts=4, numPartitions=12, replicationFactor=3, autoCreateTopics=false, retryTopicSuffix=".shipping.retry", dltTopicSuffix=".shipping.DLT")` |
| `NotificationListener` | notification-service | notificationFactory | single-record |
| `AnalyticsListener` | analytics-service | analyticsFactory | batch: `List<ConsumerRecord<String, OrderEvent>>` |

### 2.5 Dead-letter path (`DeadLetterConfig.java`)
- `dltKafkaTemplate`: key=`StringSerializer`, value=`DelegatingByTypeSerializer`
  (`byte[]` → `ByteArraySerializer`, `OrderEvent` → `JacksonJsonSerializer`)
- Producer hardening: `acks=all`, `enable.idempotence=true`,
  `delivery.timeout.ms=60000`, `max.in.flight.requests.per.connection=5`
- Target resolved per-record to `app.kafka.dlt-topic` (`orders.DLT`), partition `-1`

### 2.6 Admin / ops
- `ConsumerPauseController`: `/admin/kafka/listeners/{listenerId}/pause|resume`
  via `KafkaListenerEndpointRegistry` — pause/resume without rebalance.
- `DltReplayController`: replays a `String` payload from `orders.DLT` back to an
  arbitrary `originalTopic` via `dltKafkaTemplate`.

### 2.7 Sequence — scaling + partition-count-change interaction
1. Ops increases partitions on `orders` out-of-band (`kafka-topics.sh --alter`), or
   increases pod replica count.
2. If replicas increased: new pod starts, gets `group.instance.id =
   <groupId>-<new-podId>`; `CooperativeStickyAssignor` incrementally rebalances,
   handing the new consumer idle/under-subscribed partitions without revoking
   others' active partitions.
3. If partition count increased: existing consumers' next rebalance (triggered by
   metadata refresh) picks up new partitions. No code path auto-adjusts
   `concurrency` — operator must bump `app.kafka.groups.*.concurrency` and
   redeploy to use the added parallelism; otherwise new partitions are still
   consumed, just not with extra thread parallelism within a pod.
4. Key-based ordering (`orderId`) is only guaranteed for messages produced after
   the partition increase — pre-increase keys may now map differently going
   forward, but historical ordering per key up to that point is unaffected since
   partitions are append-only logs.

### 2.8 Multiple consumer groups on the same topic

Example: `order-group1` (concurrency=3) consuming `orders` (3 partitions), and a
second, independent `order-group2` added later on the same topic.

- Partition assignment is scoped **per group**. `order-group1` gets all 3
  partitions (1 thread each). `order-group2` performs its own independent
  assignment over the same 3 partitions — unaffected by what `order-group1` is
  doing.
- Each group commits its **own offsets**, keyed by `(topic, partition,
  group.id)` in `__consumer_offsets`. So both groups read the *same* messages
  independently — this is pub/sub fan-out, not load-sharing between groups
  (this is exactly how `billing-service` / `shipping-service` /
  `notification-service` / `analytics-service` already coexist on `orders` in
  this repo).
- If `order-group2`'s concurrency exceeds the partition count, the extra
  threads sit idle — same per-group ceiling as any single group.
- A rebalance inside one group (e.g. a pod restart in `order-group1`) has
  **zero effect** on `order-group2`'s assignment — groups are fully isolated.
- To add this in code: a new entry under `app.kafka.groups.*` in
  `application.yml`, a new container factory bean in
  `KafkaGroupFactoryConfig.java`, and a new `@KafkaListener` pointed at
  `${app.kafka.topic}` — no changes required to the existing four groups or to
  the topic/partitions.
- **Caveat:** two group IDs give you duplicate processing, not horizontal
  scaling of one logical consumer. If `order-group1`/`order-group2` are meant
  to be replicas of the *same* logical consumer, they should share one
  `group-id` instead (see §2.7) — a second group ID is only correct when it's
  a genuinely different downstream consumer.
