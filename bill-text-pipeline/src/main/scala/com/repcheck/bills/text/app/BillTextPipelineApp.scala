package com.repcheck.bills.text.app

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory

import com.repcheck.bills.text.app.BillTextPipelinePipeline.AppConfig

object BillTextPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    BillTextPipelinePipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config: AppConfig) =>
        for {
          xa              <- TransactorResource.make[IO](config.database)
          httpClient      <- EmberClientBuilder.default[IO].build
          pubSubPublisher <- PubSubPublisherResource.make[IO](config.eventPublisher)
        } yield (xa, httpClient, pubSubPublisher),
      processorFactory = BillTextPipelinePipeline.buildProcessor[IO],
    )
  }

}
