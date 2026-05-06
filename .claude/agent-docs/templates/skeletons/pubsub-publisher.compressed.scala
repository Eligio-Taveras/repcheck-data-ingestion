<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/pubsub-publisher.scala -->

# RepCheck Skeleton: Pub/Sub Publisher

**Purpose:** PipelineEvent[T] envelope for all inter-service messages + tagless-final publisher wrapping Google Pub/Sub Java SDK.

**Key Design:** EventId, eventType, timestamp, source, retryCount, maxRetries, ResourceRequirements, and payload travel together. ResourceRequirements enable orchestrator capacity checks without deserializing payload. Circe codecs auto-derived. Google Pub/Sub Java SDK wrapped in Sync[F].blocking with centralized retry wrapper.

---

## ResourceRequirements

Capacity hints for orchestrator scheduling.

```scala
final case class ResourceRequirements(
    maxCpu: String = "1",        // e.g., "1", "2", "4"
    maxMemory: String = "512Mi"  // e.g., "512Mi", "1Gi", "4Gi"
)

object ResourceRequirements {
  given Encoder[ResourceRequirements] = deriveEncoder
  given Decoder[ResourceRequirements] = deriveDecoder
}
```

---

## PipelineEvent[T] Envelope

Typed event envelope for all pipeline messages.

| Field | Purpose |
|-------|---------|
| eventId | Unique ID for idempotency and tracing (UUID) |
| eventType | Discriminator, e.g., "bill.text.available" |
| timestamp | When event was created (Instant) |
| source | Originating service, e.g., "bill-ingestion" |
| retryCount | Times orchestrator has re-enqueued (default: 0) |
| maxRetries | Ceiling; retryCount >= maxRetries → dead-letter (default: 5) |
| resources | Capacity hints for Cloud Run scheduling |
| payload | Business data of type T (requires Encoder/Decoder) |

```scala
final case class PipelineEvent[T](
    eventId: UUID,
    eventType: String,
    timestamp: Instant,
    source: String,
    retryCount: Int = 0,
    maxRetries: Int = 5,
    resources: ResourceRequirements = ResourceRequirements(),
    payload: T
)

object PipelineEvent {
  given [T: Encoder]: Encoder[PipelineEvent[T]] = deriveEncoder
  given [T: Decoder]: Decoder[PipelineEvent[T]] = deriveDecoder

  def create[T](
      eventType: String,
      source: String,
      payload: T,
      resources: ResourceRequirements = ResourceRequirements(),
      maxRetries: Int = 5
  ): PipelineEvent[T] =
    PipelineEvent(
      eventId = UUID.randomUUID(),
      eventType = eventType,
      timestamp = Instant.now(),
      source = source,
      retryCount = 0,
      maxRetries = maxRetries,
      resources = resources,
      payload = payload
    )
}
```

---

## PubSubPublisher Trait

Tagless-final publisher interface (one instance per topic).

```scala
trait PubSubPublisher[F[_]] {
  def publish[T: Encoder](event: PipelineEvent[T]): F[String]
}
```

**publish** — Returns Pub/Sub message ID on success.

---

## Google Pub/Sub SDK Implementation

Config + lifecycle-managed Resource.

```scala
object PubSubPublisher {

  final case class PubSubPublisherConfig(
      projectId: String,
      topicId: String,
      retry: RetryConfig = RetryConfig()
  )

  def make[F[_]: Sync](
      config: PubSubPublisherConfig,
      classifier: ErrorClassifier
  ): Resource[F, PubSubPublisher[F]] =
    Resource
      .make(
        Sync[F].blocking {
          // TODO: val topicName = TopicName.of(config.projectId, config.topicId)
          // TODO: Publisher.newBuilder(topicName).build()
          ???: com.google.cloud.pubsub.v1.Publisher
        }
      )(publisher =>
        Sync[F].blocking {
          // TODO: publisher.shutdown()
          // TODO: publisher.awaitTermination(30, TimeUnit.SECONDS)
        }
      )
      .map { sdkPublisher =>
        new PubSubPublisher[F] {
          def publish[T: Encoder](event: PipelineEvent[T]): F[String] = {
            val operation: F[String] = Sync[F].blocking {
              val json = event.asJson.noSpaces
              val data =
                com.google.protobuf.ByteString.copyFromUtf8(json)
              val message = com.google.pubsub.v1.PubsubMessage
                .newBuilder()
                .setData(data)
                .putAttributes("eventType", event.eventType)
                .putAttributes("source", event.source)
                .putAttributes("retryCount", event.retryCount.toString)
                .build()

              // TODO: sdkPublisher.publish(message).get()
              ???
            }

            RetryWrapper.withRetry[F, String](
              config.retry,
              classifier,
              s"pubsub-publish(${config.topicId})"
            )(operation)
          }
        }
      }
}
```

**How to Create:** Wrap with Resource.use { publisher => publisher.publish(event) }. Blocks on Sync[F]. Serializes event to JSON, wraps with eventType/source/retryCount attributes. Retries via centralized RetryWrapper.