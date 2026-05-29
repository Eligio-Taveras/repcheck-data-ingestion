package repcheck.members.committees.app

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.config.HistoricalLoaderConfig
import repcheck.members.committees.persistence.{DoobieCommitteeMemberRepository, DoobieCommitteeRepository}
import repcheck.members.committees.pipeline.CommitteeHistoryLoader
import repcheck.members.common.persistence.DoobieMemberRepository

private[app] object CommitteeHistoryLoaderPipeline {

  private val PipelineName = "committee-history-loader"

  final case class AppConfig(
    database: DatabaseConfig,
    historical: HistoricalLoaderConfig,
  ) derives pureconfig.ConfigReader

  private[app] def runWithFactories[F[_]: Async](
    args: List[String],
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    linesFactory: String => Stream[F, String],
    loaderFactory: (Transactor[F], HistoricalLoaderConfig, PipelineLogger[F]) => CommitteeHistoryLoader[F],
  ): F[ExitCode] =
    for {
      config <- configLoader
      runId  <- PipelineBootstrap.extractRunId[F](args)
      logger <- loggerFactory(PipelineName)
      exitCode <- transactorFactory(config.database).use { xa =>
        val loader = loaderFactory(xa, config.historical, logger)
        val logCtx = LogContext(runId = runId, stepName = PipelineName)
        for {
          result <- loader.load(linesFactory(config.historical.filePath), runId.toLongOption.getOrElse(0L))
          _ <- logger.info(
            logCtx,
            s"Historical load complete: rowsRead=${result.rowsRead.toString} " +
              s"upserted=${result.upserted.toString} skippedNoMember=${result.skippedNoMember.toString} " +
              s"parseErrors=${result.parseErrors.toString}",
          )
        } yield ExitCode.Success
      }
    } yield exitCode

  private[app] def buildLoader[F[_]: Async](
    xa: Transactor[F],
    config: HistoricalLoaderConfig,
    logger: PipelineLogger[F],
  ): CommitteeHistoryLoader[F] =
    new CommitteeHistoryLoader[F](
      committeeRepo = new DoobieCommitteeRepository,
      committeeMemberRepo = new DoobieCommitteeMemberRepository,
      memberRepo = new DoobieMemberRepository,
      xa = xa,
      config = config,
      logger = logger,
    )

}
