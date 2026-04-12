package com.repcheck.bills.text.subscription

import repcheck.pipeline.models.events.{BillTextAvailableEvent, PipelineEvent}

/**
 * Pulls messages from a Pub/Sub subscription, deserializes the `PipelineEvent[BillTextAvailableEvent]` envelope, and
 * returns them with their ack IDs for downstream processing.
 *
 * Uses synchronous pull (not streaming) — Cloud Run Jobs are short-lived batch processors that pull a finite batch of
 * messages, process them, and exit.
 */
trait PubSubEventSubscriber[F[_]] {

  /** Pull up to `maxMessages` from the subscription and deserialize each into a PipelineEvent. */
  def pull(maxMessages: Int): F[List[ReceivedEvent]]

  /** Acknowledge successfully processed messages so they are not redelivered. */
  def acknowledge(ackIds: List[String]): F[Unit]

}

/** A deserialized event paired with the Pub/Sub ack ID needed to acknowledge it after processing. */
final case class ReceivedEvent(
  event: PipelineEvent[BillTextAvailableEvent],
  ackId: String,
)
