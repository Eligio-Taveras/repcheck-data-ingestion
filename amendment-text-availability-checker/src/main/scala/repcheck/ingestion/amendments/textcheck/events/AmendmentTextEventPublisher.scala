package repcheck.ingestion.amendments.textcheck.events

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import io.circe.syntax._

import repcheck.ingestion.amendments.textcheck.errors.AmendmentTextEventPublishErrorClassifier
import repcheck.ingestion.common.events.{EventPublishFailed, PubSubEventPublisher}
import repcheck.pipeline.models.events.{AmendmentTextAvailableEvent, EventTypes, PipelineEvent}

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

/**
 * Tagless-final publisher for `AmendmentTextAvailableEvent` on the `amendment.text.available` topic. The shared
 * `IngestionEventPublisher` trait in `ingestion-common 0.1.28` does not have an `amendmentTextAvailable` method, so we
 * own this thin adapter inside the checker subproject. Same wiring shape as `DefaultIngestionEventPublisher`: serialize
 * via `PipelineEvent.create`, publish via `PubSubEventPublisher.publish`, retry via `RetryWrapper` with a transient
 * (network-aware) classifier.
 */
trait AmendmentTextEventPublisher[F[_]] {
  def publish(event: AmendmentTextAvailableEvent, correlationId: UUID): F[String]
}

class DefaultAmendmentTextEventPublisher[F[_]: Async](
  publisher: PubSubEventPublisher[F],
  topicName: String,
  source: String,
  retryWrapper: RetryWrapper[F],
  retryConfig: RetryConfig,
) extends AmendmentTextEventPublisher[F] {

  override def publish(event: AmendmentTextAvailableEvent, correlationId: UUID): F[String] = {
    val operation = for {
      envelope <- PipelineEvent.create[F, AmendmentTextAvailableEvent](
        EventTypes.AmendmentTextAvailable,
        event,
        correlationId,
        source,
      )
      json = envelope.asJson(using PipelineEvent.encoder[AmendmentTextAvailableEvent]).noSpaces
      messageId <- publisher.publish(topicName, json, Map("eventType" -> EventTypes.AmendmentTextAvailable))
    } yield messageId

    retryWrapper.withRetry(
      operation = operation,
      config = retryConfig,
      classifier = AmendmentTextEventPublishErrorClassifier,
      errorFactory = (msg, cause) => EventPublishFailed(topicName, msg, Some(cause)),
      correlationId = correlationId,
    )
  }

}
