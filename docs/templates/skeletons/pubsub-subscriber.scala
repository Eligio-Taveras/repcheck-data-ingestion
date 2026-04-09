// =============================================================================
// RepCheck Skeleton: Pub/Sub Subscriber
// Repo: repcheck-pipeline-models (shared library)
// =============================================================================
//
// PURPOSE: Pulls messages from a Pub/Sub subscription, deserializes the
// PipelineEvent envelope, and passes the typed payload to a handler function.
//
// KEY DECISIONS (from Q&A):
// - Synchronous pull (not streaming) — Cloud Run Jobs are short-lived batch
// - Messages that fail deserialization are nacked
// - Uses retry wrapper for the pull + ack cycle
// =============================================================================

package repcheck.pipeline.models.pubsub

import cats.effect.{Resource, Sync}
import cats.syntax.all.*

import io.circe.{Decoder, parser}

import scala.jdk.CollectionConverters.*

import repcheck.pipeline.models.retry.{ErrorClassifier, RetryConfig, RetryWrapper}

// ---------------------------------------------------------------------------
// Subscriber Trait
// ---------------------------------------------------------------------------

trait PubSubSubscriber[F[_]] {
  /** Pull up to maxMessages, deserialize, invoke handler, ack on success.
    * Returns the number of messages successfully processed.
    */
  def pullAndProcess[T: Decoder](
      maxMessages: Int,
      handler: PipelineEvent[T] => F[Unit]
  ): F[Int]
}

// ---------------------------------------------------------------------------
// Google Pub/Sub SDK Implementation
// ---------------------------------------------------------------------------

object PubSubSubscriber {

  final case class PubSubSubscriberConfig(
      projectId: String,
      subscriptionId: String,
      retry: RetryConfig = RetryConfig(),
      maxMessages: Int = 100
  )

  def make[F[_]: Sync](
      config: PubSubSubscriberConfig,
      classifier: ErrorClassifier
  ): Resource[F, PubSubSubscriber[F]] =
    // TODO: Acquire SubscriberStub via GrpcSubscriberStub.create(...)
    // Release with stub.close()
    Resource
      .make(
        Sync[F].blocking {
          ???: com.google.cloud.pubsub.v1.stub.SubscriberStub
        }
      )(stub => Sync[F].blocking { /* stub.close() */ })
      .map { stub =>
        new PubSubSubscriber[F] {
          def pullAndProcess[T: Decoder](
              maxMessages: Int,
              handler: PipelineEvent[T] => F[Unit]
          ): F[Int] = {
            val operation: F[Int] = Sync[F]
              .blocking {
                // TODO: Build PullRequest and call stub.pullCallable().call(...)
                // val pullRequest = PullRequest.newBuilder()
                //   .setSubscription(SubscriptionName.of(...).toString)
                //   .setMaxMessages(maxMessages)
                //   .build()
                // stub.pullCallable().call(pullRequest).getReceivedMessagesList.asScala.toList
                ???: List[com.google.pubsub.v1.ReceivedMessage]
              }
              .flatMap { messages =>
                messages.traverse { receivedMessage =>
                  val json = receivedMessage.getMessage.getData.toStringUtf8

                  parser.decode[PipelineEvent[T]](json) match {
                    case Right(event) =>
                      handler(event) *>
                        ackMessage(stub, receivedMessage.getAckId).as(1)

                    case Left(err) =>
                      // TODO: Log WARN: deserialization failure
                      nackMessage(stub, receivedMessage.getAckId).as(0)
                  }
                }.map(_.sum)
              }

            RetryWrapper.withRetry[F, Int](
              config.retry,
              classifier,
              s"pubsub-pull(${config.subscriptionId})"
            )(operation)
          }

          private def ackMessage(
              stub: com.google.cloud.pubsub.v1.stub.SubscriberStub,
              ackId: String
          ): F[Unit] =
            Sync[F].blocking {
              // TODO: stub.acknowledgeCallable().call(
              //   AcknowledgeRequest.newBuilder()
              //     .setSubscription(...)
              //     .addAckIds(ackId)
              //     .build())
            }

          private def nackMessage(
              stub: com.google.cloud.pubsub.v1.stub.SubscriberStub,
              ackId: String
          ): F[Unit] =
            Sync[F].blocking {
              // TODO: stub.modifyAckDeadlineCallable().call(
              //   ModifyAckDeadlineRequest.newBuilder()
              //     .setSubscription(...)
              //     .addAckIds(ackId)
              //     .setAckDeadlineSeconds(0)  // nack = deadline 0
              //     .build())
            }
        }
      }
}
