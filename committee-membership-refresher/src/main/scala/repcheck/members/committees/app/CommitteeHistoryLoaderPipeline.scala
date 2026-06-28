package repcheck.members.committees.app

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.GovInfoCdirClient
import repcheck.members.committees.config.{GovInfoConfig, HistoricalLoaderConfig}
import repcheck.members.committees.model.UsStateNames
import repcheck.members.committees.persistence.{
  DoobieCommitteeMemberRepository,
  DoobieCommitteeRepository,
  DoobieHistoricalMemberRepository,
}
import repcheck.members.committees.pipeline.CommitteeHistoryLoader

private[app] object CommitteeHistoryLoaderPipeline {

  private val PipelineName = "committee-history-loader"

  final case class AppConfig(
    database: DatabaseConfig,
    historical: HistoricalLoaderConfig,
    govinfo: GovInfoConfig,
  ) derives pureconfig.ConfigReader

  final case class LoaderResources[F[_]](
    xa: Transactor[F],
    httpClient: Client[F],
  )

  private[app] def runWithFactories[F[_]: Async](
    args: List[String],
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: AppConfig => Resource[F, LoaderResources[F]],
    loaderFactory: (LoaderResources[F], AppConfig, PipelineLogger[F]) => CommitteeHistoryLoader[F],
  ): F[ExitCode] =
    for {
      config <- configLoader
      runId  <- PipelineBootstrap.extractRunId[F](args)
      _      <- PipelineBootstrap.extractStepRunId[F](args)
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config).use { resources =>
        val loader = loaderFactory(resources, config, logger)
        val logCtx = LogContext(runId = runId.value.toString, stepName = PipelineName)
        for {
          result <- loader.load(runId.value)
          _ <- logger.info(
            logCtx,
            s"Pipeline completed: seen=${result.assignmentsSeen.toString} upserted=${result.upserted.toString} " +
              s"noMember=${result.skippedNoMember.toString} noCommittee=${result.skippedNoCommittee.toString}",
          )
        } yield ExitCode.Success
      }
    } yield exitCode

  private[app] def buildLoader[F[_]: Async](
    resources: LoaderResources[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): CommitteeHistoryLoader[F] = {
    val source = new GovInfoCdirClient[F](resources.httpClient, config.govinfo, logger)
    new CommitteeHistoryLoader[F](
      source = source,
      committeeRepo = new DoobieCommitteeRepository,
      committeeMemberRepo = new DoobieCommitteeMemberRepository,
      memberRepo = new DoobieHistoricalMemberRepository,
      xa = resources.xa,
      config = config.historical,
      stateNames = UsStateNames.all,
      logger = logger,
    )
  }

}
