package repcheck.members.committees.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.committees.client.{
  CommitteeMetadataApiClient,
  HouseMemberDataXmlClient,
  SenateCommitteeMembershipXmlClient,
  SenateIdentityXmlClient,
}
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.persistence.{DoobieCommitteeMemberRepository, DoobieCommitteeRepository}
import repcheck.members.committees.pipeline.CommitteeMembershipProcessor
import repcheck.members.common.persistence.DoobieMemberRepository
import repcheck.pipeline.models.errors.{ErrorClass, RetryConfig, RetryWrapper}

private[app] object CommitteeMembershipRefresherPipeline {

  private val PipelineName = "committee-membership-refresher"

  final case class AppConfig(
    database: DatabaseConfig,
    pipeline: CommitteeMembershipConfig,
    congressGov: CongressGovClientConfig,
    fetchRetry: RetryConfig,
  ) derives pureconfig.ConfigReader

  final case class RefresherResources[F[_]](
    xa: Transactor[F],
    httpClient: Client[F],
  )

  private[app] def runWithFactories[F[_]: Async](
    args: List[String],
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: (AppConfig, PipelineLogger[F]) => Resource[F, RefresherResources[F]],
    processorFactory: (
      Client[F],
      Transactor[F],
      AppConfig,
      PipelineLogger[F],
    ) => CommitteeMembershipProcessor[F],
  ): F[ExitCode] =
    for {
      config <- configLoader
      runId  <- PipelineBootstrap.extractRunId[F](args)
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config, logger).use { resources =>
        val processor = processorFactory(resources.httpClient, resources.xa, config, logger)
        val logCtx    = LogContext(runId = runId, stepName = PipelineName)
        for {
          result <- processor.refreshAll(runId.toLongOption.getOrElse(0L))
          _ <- logger.info(
            logCtx,
            s"Pipeline completed: committees=${result.committeesUpserted.toString} " +
              s"distinctMembers=${result.distinctMembersProcessed.toString} " +
              s"membershipRows=${result.membershipRowsUpserted.toString}",
          )
        } yield ExitCode.Success
      }
    } yield exitCode

  private[app] def buildProcessor[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): CommitteeMembershipProcessor[F] = {
    given org.typelevel.log4cats.Logger[F] =
      org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F](PipelineName)
    val retryLogger: (Int, Int, Long, ErrorClass, String, UUID) => F[Unit] =
      (attempt, maxRetries, backoffMs, errorClass, message, correlationId) => {
        val logCtx = LogContext(runId = "retry", stepName = PipelineName)
        logger.warn(
          logCtx,
          s"Retry $attempt/$maxRetries backoff=${backoffMs.toString}ms class=$errorClass " +
            s"correlationId=${correlationId.toString}: $message",
        )
      }
    val retryWrapper        = new RetryWrapper[F](retryLogger)
    val xmlFeedClient       = XmlFeedClient.make[F](httpClient, config.fetchRetry)
    val houseXmlClient      = new HouseMemberDataXmlClient[F](xmlFeedClient, config.pipeline, logger)
    val senateIdClient      = new SenateIdentityXmlClient[F](xmlFeedClient, config.pipeline, logger)
    val senateCommClient    = new SenateCommitteeMembershipXmlClient[F](xmlFeedClient, config.pipeline, logger)
    val committeeApiClient  = CommitteeMetadataApiClient[F](config.congressGov, httpClient, retryWrapper)
    val committeeRepo       = new DoobieCommitteeRepository
    val committeeMemberRepo = new DoobieCommitteeMemberRepository
    val memberRepo          = new DoobieMemberRepository

    new CommitteeMembershipProcessor[F](
      committeeApiClient = committeeApiClient,
      houseXmlClient = houseXmlClient,
      senateIdentityClient = senateIdClient,
      senateCommitteeClient = senateCommClient,
      committeeRepo = committeeRepo,
      committeeMemberRepo = committeeMemberRepo,
      memberRepo = memberRepo,
      xa = xa,
      config = config.pipeline,
      logger = logger,
    )
  }

  private[app] def buildResources[F[_]](
    config: AppConfig,
    logger: PipelineLogger[F],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
  ): Resource[F, RefresherResources[F]] = {
    val _ = logger
    for {
      xa         <- transactorFactory(config.database)
      httpClient <- httpClientFactory
    } yield RefresherResources(xa, httpClient)
  }

}
