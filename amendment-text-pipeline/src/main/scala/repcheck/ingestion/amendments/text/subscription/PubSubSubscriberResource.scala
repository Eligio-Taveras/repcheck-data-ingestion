package repcheck.ingestion.amendments.text.subscription

import cats.effect.{Async, Resource}

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.SubscriptionName

import repcheck.ingestion.common.logging.PipelineLogger

/**
 * Creates a `Resource`-managed [[PubSubEventSubscriber]] backed by the Google Cloud Pub/Sub SDK. Mirror of the
 * bill-side `PubSubSubscriberResource`. The `SubscriberStub` is acquired on resource allocation and shut down on
 * release; for local development with the Pub/Sub emulator, set `PUBSUB_EMULATOR_HOST` and the SDK auto-detects it.
 */
object PubSubSubscriberResource {

  private[text] def make[F[_]: Async](
    config: EventSubscriberConfig,
    logger: PipelineLogger[F],
    stubFactory: () => SubscriberStub,
  ): Resource[F, PubSubEventSubscriber[F]] = {
    val subscriptionName = SubscriptionName.of(config.projectId, config.subscriptionId).toString

    Resource
      .make(
        Async[F].blocking(stubFactory())
      )(stub => Async[F].blocking(stub.close()))
      .map(stub => new GooglePubSubEventSubscriber[F](stub, subscriptionName, logger))
  }

}
