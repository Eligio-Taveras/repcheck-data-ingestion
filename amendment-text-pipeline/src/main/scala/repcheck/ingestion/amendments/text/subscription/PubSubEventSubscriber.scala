package repcheck.ingestion.amendments.text.subscription

import repcheck.pipeline.models.events.{AmendmentTextAvailableEvent, PipelineEvent}

/**
 * Pulls messages from a Pub/Sub subscription, deserializes the `PipelineEvent[AmendmentTextAvailableEvent]` envelope,
 * and returns them with their ack IDs for downstream processing. Mirror of the bill-side `PubSubEventSubscriber`.
 */
trait PubSubEventSubscriber[F[_]] {

  /** Pull up to `maxMessages` from the subscription and deserialize each into a PipelineEvent. */
  def pull(maxMessages: Int): F[List[ReceivedEvent]]

  /** Acknowledge successfully processed messages so they are not redelivered. */
  def acknowledge(ackIds: List[String]): F[Unit]

  /**
   * Negative-acknowledge: explicitly redeliver by setting the ack deadline to 0. Bounded by the subscription's
   * `max_delivery_attempts` + dead-letter topic. Used by the embedder on known failures (UPSERT error, embed error,
   * trim error, markFetched error).
   */
  def nack(ackIds: List[String]): F[Unit]

}

/** A deserialized event paired with the Pub/Sub ack ID needed to acknowledge it after processing. */
final case class ReceivedEvent(
  event: PipelineEvent[AmendmentTextAvailableEvent],
  ackId: String,
)
