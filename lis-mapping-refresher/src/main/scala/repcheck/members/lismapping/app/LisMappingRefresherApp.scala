package repcheck.members.lismapping.app

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.members.lismapping.app.LisMappingRefresherPipeline.AppConfig

object LisMappingRefresherApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    LisMappingRefresherPipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        LisMappingRefresherPipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          EmberClientBuilder.default[IO].build,
          PubSubPublisherResource.make[IO](_),
        ),
      processorFactory = LisMappingRefresherPipeline.buildProcessor[IO],
    )
  }

}
