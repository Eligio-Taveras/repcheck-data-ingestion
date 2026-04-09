<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/pubsub-subscriber.scala -->

```markdown
# RepCheck Skeleton: Pub/Sub Subscriber

**Repo:** repcheck-pipeline-models (shared library)

Pulls messages from Pub/Sub subscription, deserializes PipelineEvent envelope, passes typed payload to handler. Synchronous pull (not streaming) for Cloud Run Jobs. Failed deserializations are nacked. Uses retry wrapper for pull + ack cycle.

## Code

```scala
package repcheck.pipeline.models.pubsub

import cats.effect.{Resource, Sync}
import cats.syntax.all.*

import io.circe.{Decoder, parser}

import scala.jdk.CollectionConverters.*

import repcheck.pipeline.models.retry.{ErrorClassifier, RetryConfig, RetryWrapper}

trait PubSubSubscriber[F[_]] {
  /** Pull up to maxMessages, deserialize, invoke handler, ack on success.
    * Returns the number of messages successfully processed.
    */
  def pullAndProcess[T: Decoder](
      maxMessages: Int,
      handler: PipelineEvent[T] => F[Unit]
  ): F[Int]
}

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
```

## When To Use

- Consuming messages from Google Cloud Pub/Sub in batch/Cloud Run Job context
- Need typed deserialization via Circe decoders
- Require automatic retry on transient failures
- Want explicit success/failure (ack/nack) semantics
```