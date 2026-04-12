package com.repcheck.bills.text.app

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.PipelineLoggerFactory

import com.repcheck.bills.text.app.BillTextPipelinePipeline.AppConfig
import com.repcheck.bills.text.subscription.PubSubSubscriberResource

object BillTextPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    BillTextPipelinePipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        BillTextPipelinePipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          EmberClientBuilder.default[IO].build,
          PubSubPublisherResource.make[IO](_),
          (subConfig, log) =>
            PubSubSubscriberResource.make[IO](
              subConfig,
              log,
              () => {
                val settings = SubscriberStubSettings.newBuilder().build()
                GrpcSubscriberStub.create(settings)
              },
            ),
        ),
      processorFactory = BillTextPipelinePipeline.buildProcessor[IO],
      streamFactory = BillTextPipelinePipeline.buildStream[IO],
      workflowStateUpdaterFactory = (xa, cfg) => Some(new WorkflowStateUpdater[IO](xa, cfg)),
    )
  }

}
