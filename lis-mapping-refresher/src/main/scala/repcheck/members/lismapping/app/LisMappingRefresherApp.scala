package repcheck.members.lismapping.app

import cats.effect.{ExitCode, IO, IOApp}

import org.http4s.ember.client.EmberClientBuilder

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.members.lismapping.app.LisMappingRefresherPipeline.AppConfig

object LisMappingRefresherApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      runId    <- PipelineBootstrap.extractRunId[IO](args)
      _        <- PipelineBootstrap.extractStepRunId[IO](args)
      exitCode <- runPipeline(args, runId)
    } yield exitCode

  private[app] def runPipeline(args: List[String], runId: Long): IO[ExitCode] =
    LisMappingRefresherPipeline.runWithFactories[IO](
      configLoader = PipelineBootstrap.loadConfig[IO, AppConfig](args),
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
      runId = runId,
    )

}
