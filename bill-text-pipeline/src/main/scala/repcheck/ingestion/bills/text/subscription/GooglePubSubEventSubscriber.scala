package repcheck.ingestion.bills.text.subscription

import scala.jdk.CollectionConverters._

import cats.effect.Async
import cats.syntax.all._

import io.circe.parser

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.{AcknowledgeRequest, PullRequest}

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, PipelineEvent}

/**
 * Google Cloud Pub/Sub synchronous pull subscriber.
 *
 * Wraps the low-level `SubscriberStub` SDK to pull messages, deserialize `PipelineEvent[BillTextAvailableEvent]`
 * envelopes, and acknowledge processed messages. Messages that fail deserialization are logged and skipped (not acked,
 * so they will be redelivered until the ack deadline expires or they hit the dead-letter topic).
 */
class GooglePubSubEventSubscriber[F[_]: Async] private[subscription] (
  stub: SubscriberStub,
  subscriptionName: String,
  logger: PipelineLogger[F],
) extends PubSubEventSubscriber[F] {

  private val logCtx = LogContext(runId = "subscriber", stepName = "pubsub-pull")

  override def pull(maxMessages: Int): F[List[ReceivedEvent]] = {
    val pullRequest = PullRequest
      .newBuilder()
      .setSubscription(subscriptionName)
      .setMaxMessages(maxMessages)
      .build()

    for {
      response <- Async[F].blocking(stub.pullCallable().call(pullRequest))
      messages = response.getReceivedMessagesList.asScala.toList
      _ <- logger.info(logCtx, s"Pulled ${messages.size.toString} messages from subscription")
      events <- messages.traverse { receivedMessage =>
        val json  = receivedMessage.getMessage.getData.toStringUtf8
        val ackId = receivedMessage.getAckId
        deserializeEvent(json, ackId)
      }
    } yield events.flatten
  }

  override def acknowledge(ackIds: List[String]): F[Unit] =
    if (ackIds.isEmpty) {
      Async[F].unit
    } else {
      val ackRequest = AcknowledgeRequest
        .newBuilder()
        .setSubscription(subscriptionName)
        .addAllAckIds(ackIds.asJava)
        .build()

      Async[F].blocking(stub.acknowledgeCallable().call(ackRequest)).void
    }

  private[subscription] def deserializeEvent(
    json: String,
    ackId: String,
  ): F[Option[ReceivedEvent]] =
    parser.decode[PipelineEvent[BillTextAvailableEvent]](json) match {
      case Right(event) =>
        Async[F].pure(Some(ReceivedEvent(event, ackId)))
      case Left(error) =>
        logger.warn(
          logCtx,
          s"Failed to deserialize Pub/Sub message (will not ack): ${error.getMessage}",
        ) *> Async[F].pure(None)
    }

}
