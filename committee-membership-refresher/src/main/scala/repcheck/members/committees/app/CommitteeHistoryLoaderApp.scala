package repcheck.members.committees.app

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.members.committees.app.CommitteeHistoryLoaderPipeline.{AppConfig, LoaderResources}

/**
 * One-time entry point for backfilling historical committee membership from the GovInfo Congressional Directory. Runs
 * in the refresher image; invoke with the loader main class, e.g.: java -cp /app/app.jar
 * repcheck.members.committees.app.CommitteeHistoryLoaderApp cfg-unused <runId>
 */
object CommitteeHistoryLoaderApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    CommitteeHistoryLoaderPipeline.runWithFactories[IO](
      args = args,
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config: AppConfig) =>
        for {
          xa         <- TransactorResource.make[IO](config.database)
          httpClient <- EmberClientBuilder.default[IO].build
        } yield LoaderResources(xa, httpClient),
      loaderFactory = CommitteeHistoryLoaderPipeline.buildLoader[IO],
    )

}
