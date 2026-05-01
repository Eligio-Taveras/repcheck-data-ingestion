package repcheck.ingestion.bills.metadata.app

import scala.concurrent.duration._

import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.bills.common.persistence.{
  DoobieBillCosponsorRepository,
  DoobieBillHistoryArchiver,
  DoobieBillRepository,
  DoobieBillSubjectRepository,
}
import repcheck.ingestion.bills.metadata.api.BillsApiClient
import repcheck.ingestion.bills.metadata.config.BillMetadataConfig
import repcheck.ingestion.bills.metadata.pipeline.BillMetadataProcessor
import repcheck.ingestion.common.api.{CongressGovClientConfig, RateLimitedHttpClient}
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.logging.{LogContext, PipelineLoggerFactory}
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{DoobieMemberRepository, MemberWriteInstances}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dos.member.MemberDO

private[app] object BillMetadataPipeline {

  private val PipelineName = "bill-metadata-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: BillMetadataConfig,
  ) derives pureconfig.ConfigReader

  import MemberWriteInstances._

  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    for {
      config <- Sync[F].delay {
        ConfigSource.default.loadOrThrow[AppConfig]
      }
      logger <- PipelineLoggerFactory.make[F](PipelineName)
      exitCode <- buildResources[F](config).use {
        case (xa, httpClient) =>
          val billRepo         = new DoobieBillRepository
          val cosponsorRepo    = new DoobieBillCosponsorRepository
          val subjectRepo      = new DoobieBillSubjectRepository
          val historyArchiver  = new DoobieBillHistoryArchiver
          val memberRepo       = new DoobieMemberRepository
          val placeholder      = new DefaultPlaceholderCreator[F]
          val memberEntityRepo = new DoobieEntityRepository[F, MemberDO](xa, MemberInsertSql.value)
          // Surface every retry attempt as a structured WARN log. Without this, an extended Ember-
          // timeout-and-back-off cycle (e.g., a Congress.gov page that hangs into the 30s timeout,
          // then retries with up to ~330s of cumulative backoff) presents to operators as a frozen
          // pipeline — no log line is emitted between attempts, so it's indistinguishable from a
          // genuine deadlock. The previous `(_, _, _, _, _, _) => Async[F].unit` callback silently
          // swallowed retry signals; this version preserves the correlationId so a single retried
          // request's lifecycle is traceable across attempts.
          val retryLogCtx = LogContext("0", "bill-metadata")
          val retryWrapper = new RetryWrapper[F]((attempt, maxRetries, delayMs, errorClass, message, correlationId) =>
            logger.warn(
              retryLogCtx.copy(correlationId = Some(correlationId)),
              s"Retry $attempt/$maxRetries scheduled in ${delayMs.toString}ms " +
                s"(errorClass=${errorClass.toString}): $message",
            )
          )
          val apiClient = BillsApiClient[F](config.congressApi, httpClient, retryWrapper, logger)

          val processor = new BillMetadataProcessor[F](
            apiClient = apiClient,
            billRepo = billRepo,
            cosponsorRepo = cosponsorRepo,
            subjectRepo = subjectRepo,
            historyArchiver = historyArchiver,
            memberRepo = memberRepo,
            placeholderCreator = placeholder,
            memberEntityRepo = memberEntityRepo,
            xa = xa,
            config = config.pipeline,
            logger = logger,
          )

          // TODO: replace 0L with the Long run ID obtained from workflow_runs DB registration
          // once PipelineBootstrap.extractRunId (ingestion-common §3.7) is implemented.
          val runId        = 0L
          val resultStream = processor.streamAll(runId)
          PipelineExecutor.execute[F](resultStream, logger, PipelineName, runId)
      }
    } yield exitCode
  }

  private def buildResources[F[_]: Async: Network](
    config: AppConfig
  ): Resource[F, (Transactor[F], Client[F])] =
    for {
      xa <- TransactorResource.make[F](config.database)
      // withTimeout(30s) + withIdleConnectionTime(60s): without these, Ember's connection pool
      // happily reuses a TCP connection that Congress.gov has already closed (server-side keep-
      // alive expiry, ~30-90s idle). The next request through the half-closed socket gets
      // ReachedEndOfStream, which the fail-loud path then surfaces as an aborted run. Mirroring
      // bill-text-availability-checker (PR #99): 30s on a stuck request, 60s pool eviction so
      // we never reuse a connection past the upstream's close window.
      rawClient <- EmberClientBuilder
        .default[F]
        .withTimeout(30.seconds)
        .withIdleConnectionTime(60.seconds)
        .build
      throttledClient <- RateLimitedHttpClient.make[F](
        rawClient,
        pageDelay = config.congressApi.pageDelay,
        permits = 1L,
      )
    } yield (xa, throttledClient)

}
