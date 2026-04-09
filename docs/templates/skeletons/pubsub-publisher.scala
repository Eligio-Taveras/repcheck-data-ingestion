// =============================================================================
// RepCheck Skeleton: Pub/Sub Publisher
// Repo: repcheck-pipeline-models (shared library)
// =============================================================================
//
// PURPOSE: Defines the PipelineEvent[T] envelope used by ALL inter-service
// messages, plus a tagless-final publisher wrapping Google Pub/Sub Java SDK.
//
// KEY DECISIONS (from Q&A):
// - PipelineEvent envelope carries: eventId, eventType, timestamp, source,
//   retryCount, maxRetries, ResourceRequirements, payload
// - ResourceRequirements (maxCpu, maxMemory) travel with the message so the
//   orchestrator can check Cloud Run capacity without deserializing payload
// - Circe codecs auto-derived for envelope; payload T needs its own codecs
// - Google Pub/Sub Java SDK wrapped in Sync[F].blocking
// - Uses centralized retry wrapper
// =============================================================================

package repcheck.pipeline.models.pubsub

import cats.effect.{Resource, Sync}
import cats.syntax.all.*

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*
import io.circe.syntax.*

import java.util.UUID
import java.time.Instant

import repcheck.pipeline.models.retry.{ErrorClassifier, RetryConfig, RetryWrapper}

// ---------------------------------------------------------------------------
// Resource Requirements — capacity hints for the orchestrator
// ---------------------------------------------------------------------------

final case class ResourceRequirements(
    maxCpu: String = "1",        // e.g., "1", "2", "4"
    maxMemory: String = "512Mi"  // e.g., "512Mi", "1Gi", "4Gi"
)

object ResourceRequirements {
  given Encoder[ResourceRequirements] = deriveEncoder
  given Decoder[ResourceRequirements] = deriveDecoder
}

// ---------------------------------------------------------------------------
// Pipeline Event Envelope
// ---------------------------------------------------------------------------

/** Typed event envelope for ALL pipeline messages.
  *
  * @tparam T         Business payload type (must have Circe Encoder/Decoder)
  * @param eventId    Unique ID for idempotency and tracing
  * @param eventType  Discriminator, e.g., "bill.text.available"
  * @param timestamp  When the event was created
  * @param source     Originating service, e.g., "bill-ingestion"
  * @param retryCount Times the orchestrator has re-enqueued this event
  * @param maxRetries Ceiling — when retryCount >= maxRetries → dead-letter
  * @param resources  Capacity hints for Cloud Run job scheduling
  * @param payload    The actual business data
  */
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

  /** Convenience constructor — generates eventId and timestamp. */
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

// ---------------------------------------------------------------------------
// Publisher Trait
// ---------------------------------------------------------------------------

/** Tagless-final publisher. One instance per topic. */
trait PubSubPublisher[F[_]] {
  /** Publish a typed event. Returns the Pub/Sub message ID on success. */
  def publish[T: Encoder](event: PipelineEvent[T]): F[String]
}

// ---------------------------------------------------------------------------
// Google Pub/Sub SDK Implementation
// ---------------------------------------------------------------------------

object PubSubPublisher {

  final case class PubSubPublisherConfig(
      projectId: String,
      topicId: String,
      retry: RetryConfig = RetryConfig()
  )

  /** Create a publisher Resource managing the underlying SDK Publisher lifecycle.
    *
    * Usage:
    *   PubSubPublisher.make[IO](config, PubSubErrorClassifier).use { publisher =>
    *     publisher.publish(event)
    *   }
    */
  def make[F[_]: Sync](
      config: PubSubPublisherConfig,
      classifier: ErrorClassifier
  ): Resource[F, PubSubPublisher[F]] =
    // TODO: Acquire com.google.cloud.pubsub.v1.Publisher:
    //   val topicName = TopicName.of(config.projectId, config.topicId)
    //   val publisher = Publisher.newBuilder(topicName).build()
    // Release with:
    //   publisher.shutdown()
    //   publisher.awaitTermination(30, TimeUnit.SECONDS)
    Resource
      .make(
        Sync[F].blocking {
          // TODO: Replace with real Publisher.newBuilder(...).build()
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
              // Returns the message ID string
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
