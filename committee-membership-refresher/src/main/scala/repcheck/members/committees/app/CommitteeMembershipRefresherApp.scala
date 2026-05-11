package repcheck.members.committees.app

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.members.committees.app.CommitteeMembershipRefresherPipeline.AppConfig

object CommitteeMembershipRefresherApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args
    CommitteeMembershipRefresherPipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        CommitteeMembershipRefresherPipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          EmberClientBuilder.default[IO].build,
        ),
      processorFactory = CommitteeMembershipRefresherPipeline.buildProcessor[IO],
    )
  }

}
