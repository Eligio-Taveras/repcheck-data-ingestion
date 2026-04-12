package com.repcheck.bills.text.subscription

import cats.effect.{Async, Resource}

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.SubscriptionName

import repcheck.ingestion.common.logging.PipelineLogger

/**
 * Creates a `Resource`-managed `PubSubEventSubscriber` backed by the Google Cloud Pub/Sub SDK.
 *
 * The `SubscriberStub` is acquired on resource allocation and shut down on release, ensuring clean gRPC channel
 * teardown. For local development with the Pub/Sub emulator, set the `PUBSUB_EMULATOR_HOST` environment variable — the
 * SDK auto-detects it and routes traffic to the emulator.
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
