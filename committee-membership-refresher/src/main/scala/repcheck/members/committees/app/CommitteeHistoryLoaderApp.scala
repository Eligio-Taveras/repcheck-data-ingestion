package repcheck.members.committees.app

import cats.effect.{ExitCode, IO, IOApp, Sync}

import fs2.io.file.{Files, Path}

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.members.committees.app.CommitteeHistoryLoaderPipeline.AppConfig

/**
 * One-time entry point for backfilling historical committee membership from the canonical TSV. Runs in the same image
 * as the refresher; invoke with the loader main class, e.g.: java -cp /app/app.jar
 * repcheck.members.committees.app.CommitteeHistoryLoaderApp cfg-unused <runId>
 */
object CommitteeHistoryLoaderApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    CommitteeHistoryLoaderPipeline.runWithFactories[IO](
      args = args,
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      transactorFactory = TransactorResource.make[IO](_),
      linesFactory = (path: String) => Files[IO].readUtf8Lines(Path(path)),
      loaderFactory = CommitteeHistoryLoaderPipeline.buildLoader[IO],
    )

}
