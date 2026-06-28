package repcheck.ingestion.bills.metadata.app

import scala.concurrent.duration._

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

import doobie.util.transactor.Transactor

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
import repcheck.ingestion.common.execution.{PipelineBootstrap, PipelineExecutor, RetryWrapperFactory}
import repcheck.ingestion.common.logging.{LogContext, PipelineLoggerFactory}
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{DoobieMemberRepository, MemberWriteInstances}
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
    val bootCtx = LogContext("0", "bill-metadata-boot")
    for {
      config    <- PipelineBootstrap.loadConfig[F, AppConfig](args)
      runId     <- PipelineBootstrap.extractRunId[F](args)
      stepRunId <- PipelineBootstrap.extractStepRunId[F](args)
      logger    <- PipelineLoggerFactory.make[F](PipelineName)
      _ <- logger.info(
        bootCtx,
        s"App boot: db=${config.database.host}:${config.database.port.toString}/${config.database.database} " +
          s"congressBaseUrl=${config.congressApi.baseUrl} pageSize=${config.congressApi.pageSize.toString} " +
          s"pageDelay=${config.congressApi.pageDelay.toString} " +
          s"retry.max=${config.congressApi.retry.maxRetries.toString} " +
          s"pipeline.lookbackDays=${config.pipeline.lookbackDays.toString} " +
          s"pipeline.parallelism=${config.pipeline.parallelism.toString} " +
          s"pipeline.minCongress=${config.pipeline.minCongress.fold("none")(_.toString)}",
      )
      _ <- logger.info(bootCtx, s"App boot: building resources (transactor + ember client)")
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
          // genuine deadlock.
          val retryWrapper = RetryWrapperFactory.logging[F](logger, PipelineName)
          val apiClient    = BillsApiClient[F](config.congressApi, httpClient, retryWrapper, logger)

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

          val resultStream = processor.streamAll(runId.value)
          logger.info(
            bootCtx,
            s"App boot: resources built, processor wired, handing off to PipelineExecutor (runId=${runId.value.toString})",
          ) *> PipelineExecutor.execute[F](resultStream, logger, PipelineName, runId, stepRunId)
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
